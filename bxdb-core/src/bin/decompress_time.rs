/// Compares `pread` vs `mmap` blob read + zstd decompression time on a
/// real bxdb index.
///
/// Usage: decompress-time <path-to-bxdb-dir> [num_samples]
///
/// Phases:
///   1. mmap index.bxdb, sample Full chunks
///   2. pread  blob read + zstd decompress  (baseline — original code)
///   3. mmap   blob read + copy + zstd      (proposed change)
///   4. BtreeDb::load_page() using mmap BlobReaders (full path with cache)

use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::ptr;
use std::time::Instant;

use bxdb::BtreeDb;
use bxdb::cache::SharedCache;
use bxdb::chunk::*;
use bxdb::btree;

const DEFAULT_NUM_SAMPLES: usize = 10_000;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: decompress-time <path-to-bxdb-dir> [num_samples]");
        std::process::exit(1);
    }
    let dir = Path::new(&args[1]);
    let num_samples: usize = args
        .get(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_NUM_SAMPLES);

    let index_path = dir.join("index.bxdb");
    if !index_path.exists() {
        eprintln!("index.bxdb not found in {}", dir.display());
        std::process::exit(1);
    }

    // ── Phase 1: mmap the index, enumerate Full chunk records ───────────────
    eprintln!("Opening index: {}", index_path.display());
    let (full_records, num_records) = scan_index_for_full(&index_path, num_samples);
    eprintln!(
        "  index has {} total records, sampled {} Full chunks",
        num_records,
        full_records.len(),
    );
    if full_records.is_empty() {
        eprintln!("No Full chunks found. Exiting.");
        std::process::exit(1);
    }

    let blob_dir = dir.join("blobs");
    let mut decomp = zstd::bulk::Decompressor::new().expect("zstd decompressor init");
    let mut sizes: Vec<u32> = Vec::with_capacity(full_records.len());

    // ── Phase 2: pread + zstd (baseline) ────────────────────────────────────
    eprintln!(
        "Phase 2: pread blob read + zstd decompress  ({:>4} chunks)...",
        full_records.len()
    );
    let (pread_read_ns, pread_decomp_ns) =
        benchmark_pread(&full_records, &blob_dir, &mut decomp, &mut sizes);

    // ── Phase 3: mmap + copy + zstd (proposed) ──────────────────────────────
    eprintln!(
        "Phase 3: mmap  blob read + copy + zstd      ({:>4} chunks)...",
        full_records.len()
    );
    let (mmap_read_ns, mmap_decomp_ns) =
        benchmark_mmap(&full_records, &blob_dir, &mut decomp);

    // ── Phase 4: BtreeDb::load_page() (now uses mmap BlobReaders) ───────────
    eprintln!("Phase 4: BtreeDb::load_page() (full path)...");
    let load_ns = benchmark_load_page(dir, &full_records);

    // ── Report ──────────────────────────────────────────────────────────────
    println!();
    println!("╔══════════════════════════════════════════════════════════════════════╗");
    println!("║  pread vs mmap — {} Full chunks sampled                        ║", full_records.len());
    println!("╠══════════════════════════════════════════════════════════════════════╣");

    print_phase("pread — blob read            ", &pread_read_ns);
    print_phase("mmap  — blob read (slice+copy)", &mmap_read_ns);
    println!("  ---");
    print_phase("zstd decompress (shared)     ", &pread_decomp_ns);
    println!("  ---");
    {
        let pc: Vec<u64> = pread_read_ns.iter().zip(&pread_decomp_ns).map(|(r, d)| r + d).collect();
        let mc: Vec<u64> = mmap_read_ns.iter().zip(&mmap_decomp_ns).map(|(r, d)| r + d).collect();
        print_phase("pread + zstd (no cache)      ", &pc);
        print_phase("mmap  + zstd (no cache)      ", &mc);
    }
    if !load_ns.is_empty() {
        print_phase("BtreeDb::load_page() (mmap)  ", &load_ns);
    }

    println!();
    println!("  Compressed size stats:");
    print_size_stats(&sizes);

    println!();
    println!("  Summary (avg):");
    let pr_avg = mean(&pread_read_ns) / 1_000.0;
    let mr_avg = mean(&mmap_read_ns) / 1_000.0;
    let zd_avg = mean(&pread_decomp_ns) / 1_000.0;
    let pc_avg = pr_avg + zd_avg;
    let mc_avg = mr_avg + zd_avg;
    let ld_avg = if load_ns.is_empty() { 0.0 } else { mean(&load_ns) / 1_000.0 };
    println!("    pread  blob read:        {:>8.1} us", pr_avg);
    println!("    mmap   blob read:        {:>8.1} us  ({}x faster than pread)", mr_avg,
        if mr_avg > 0.0 { pr_avg / mr_avg } else { 0.0 });
    println!("    zstd decompress:         {:>8.1} us", zd_avg);
    println!("    pread + zstd:            {:>8.1} us  (baseline)", pc_avg);
    println!("    mmap  + zstd:            {:>8.1} us  (proposed)", mc_avg);
    if !load_ns.is_empty() {
        println!("    load_page() full:        {:>8.1} us", ld_avg);
        println!("    load_page overhead:      {:>8.1} us  (floor + cache + memcpy)", ld_avg - mc_avg);
    }

    let _ = &full_records;
}

// ─── Phase 2: pread ───────────────────────────────────────────────────────────

fn benchmark_pread(
    records: &[ChunkRecord],
    blob_dir: &Path,
    decomp: &mut zstd::bulk::Decompressor<'static>,
    sizes: &mut Vec<u32>,
) -> (Vec<u64>, Vec<u64>) {
    let mut blob_files: HashMap<u8, File> = HashMap::new();
    let mut buf = Vec::new();
    let mut out = [0u8; PAGE_SIZE];

    let mut read_ns = Vec::with_capacity(records.len());
    let mut decomp_ns = Vec::with_capacity(records.len());
    sizes.clear();

    for rec in records {
        let blob = blob_files.entry(rec.worker_id).or_insert_with(|| {
            File::open(blob_dir.join(format!("worker_{}.blob", rec.worker_id))).unwrap()
        });

        buf.resize(rec.len as usize, 0);
        let t0 = Instant::now();
        blob.read_exact_at(&mut buf, rec.offset)
            .expect("pread failed");
        let r_ns = t0.elapsed().as_nanos() as u64;

        let t0 = Instant::now();
        let result = decomp.decompress(&buf, PAGE_SIZE + 1)
            .expect("zstd decompress failed");
        let d_ns = t0.elapsed().as_nanos() as u64;
        assert_eq!(result.len(), PAGE_SIZE);
        out.copy_from_slice(&result);

        read_ns.push(r_ns);
        decomp_ns.push(d_ns);
        sizes.push(rec.len);
    }
    (read_ns, decomp_ns)
}

// ─── Phase 3: mmap blob files ─────────────────────────────────────────────────

struct BlobMmap {
    base: *const u8,
    len: usize,
    _file: File,
}

impl BlobMmap {
    fn open(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        let len = file.metadata()?.len() as usize;
        let base = unsafe {
            libc::mmap(ptr::null_mut(), len, libc::PROT_READ,
                       libc::MAP_SHARED | libc::MAP_POPULATE,
                       file.as_raw_fd(), 0)
        };
        if base == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        Ok(Self { base: base as *const u8, len, _file: file })
    }

    fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.base, self.len) }
    }
}

impl Drop for BlobMmap {
    fn drop(&mut self) {
        if !self.base.is_null() {
            unsafe { libc::munmap(self.base as *mut libc::c_void, self.len); }
        }
    }
}

fn benchmark_mmap(
    records: &[ChunkRecord],
    blob_dir: &Path,
    decomp: &mut zstd::bulk::Decompressor<'static>,
) -> (Vec<u64>, Vec<u64>) {
    let mut blob_mmaps: HashMap<u8, BlobMmap> = HashMap::new();
    let mut buf = Vec::new();
    let mut out = [0u8; PAGE_SIZE];

    let mut read_ns = Vec::with_capacity(records.len());
    let mut decomp_ns = Vec::with_capacity(records.len());

    for rec in records {
        let mmap = blob_mmaps.entry(rec.worker_id).or_insert_with(|| {
            BlobMmap::open(&blob_dir.join(format!("worker_{}.blob", rec.worker_id))).unwrap()
        });

        let start = rec.offset as usize;
        let end = start + rec.len as usize;

        let t0 = Instant::now();
        // Copy from mmap (triggers page faults on first access, then memcpy)
        buf.clear();
        buf.extend_from_slice(&mmap.as_slice()[start..end]);
        let r_ns = t0.elapsed().as_nanos() as u64;

        let t0 = Instant::now();
        let result = decomp.decompress(&buf, PAGE_SIZE + 1)
            .expect("zstd decompress failed");
        let d_ns = t0.elapsed().as_nanos() as u64;
        assert_eq!(result.len(), PAGE_SIZE);
        out.copy_from_slice(&result);

        read_ns.push(r_ns);
        decomp_ns.push(d_ns);
    }
    (read_ns, decomp_ns)
}

// ─── Phase 4: BtreeDb::load_page() ────────────────────────────────────────────

fn benchmark_load_page(dir: &Path, records: &[ChunkRecord]) -> Vec<u64> {
    // Ensure shared-memory cache exists before opening.
    let canonical = dir.canonicalize().unwrap_or_else(|_| dir.to_path_buf());
    let shm_path = match btree::shm_cache_path(&canonical) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("  WARNING: shm_cache_path failed: {e}. Skipping load_page.");
            return Vec::new();
        }
    };
    if !shm_path.exists() {
        eprintln!("  Creating shared-memory cache at {} ...", shm_path.display());
        if let Err(e) = SharedCache::create(&shm_path, &canonical) {
            eprintln!("  WARNING: SharedCache::create failed: {e}. Skipping load_page.");
            return Vec::new();
        }
    }

    let db = match BtreeDb::open(dir) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("  WARNING: BtreeDb::open failed: {e}. Skipping load_page.");
            return Vec::new();
        }
    };

    let mut load_ns = Vec::with_capacity(records.len());
    for rec in records {
        let pa = pa_of(rec.key);
        let snap = snapshot_of(rec.key);
        let mut page = [0u8; PAGE_SIZE];
        let t0 = Instant::now();
        let found = db
            .load_page(&mut page, pa, snap)
            .expect("load_page failed");
        assert!(found, "load_page returned false");
        load_ns.push(t0.elapsed().as_nanos() as u64);
    }
    load_ns
}

// ─── Scan helpers ──────────────────────────────────────────────────────────────

fn scan_index_for_full(index_path: &Path, num_samples: usize)
    -> (Vec<ChunkRecord>, usize)
{
    let file = File::open(index_path).expect("open index.bxdb");
    let len = file.metadata().expect("stat index.bxdb").len() as usize;

    let base = unsafe {
        libc::mmap(
            ptr::null_mut(), len,
            libc::PROT_READ, libc::MAP_SHARED, file.as_raw_fd(), 0,
        )
    };
    if base == libc::MAP_FAILED {
        panic!("mmap index.bxdb failed: {}", io::Error::last_os_error());
    }

    let bytes: &[u8] = unsafe { std::slice::from_raw_parts(base as *const u8, len) };

    if bytes.len() < HEADER_SIZE || bytes[0..8] != MAGIC_IDX {
        eprintln!("WARNING: index.bxdb has invalid header magic");
        unsafe { libc::munmap(base, len); }
        return (Vec::new(), 0);
    }

    let body = &bytes[HEADER_SIZE..];
    let num_records = body.len() / FIXED_RECORD_SIZE;
    let step = (num_records / num_samples).max(1);

    let mut records = Vec::with_capacity(num_samples);

    for i in (0..num_records).step_by(step) {
        let off = i * FIXED_RECORD_SIZE;
        let rec_bytes: &[u8; FIXED_RECORD_SIZE] =
            body[off..off + FIXED_RECORD_SIZE].try_into().unwrap();
        if let Ok(rec) = ChunkRecord::decode_fixed(rec_bytes) {
            if rec.kind == ChunkKind::Full {
                records.push(rec);
                if records.len() >= num_samples {
                    break;
                }
            }
        }
    }

    // Leak the index mmap — needed for ChunkRecord references in benchmarks.
    // SAFETY: process exit cleans up.
    struct MmapGuard(*mut libc::c_void, usize);
    impl Drop for MmapGuard { fn drop(&mut self) { unsafe { libc::munmap(self.0, self.1); } } }
    std::mem::forget(MmapGuard(base, len));

    (records, num_records)
}

// ─── Statistics ────────────────────────────────────────────────────────────────

fn mean(v: &[u64]) -> f64 {
    if v.is_empty() { 0.0 } else { v.iter().sum::<u64>() as f64 / v.len() as f64 }
}

fn percentile(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() { 0.0 }
    else { sorted[((p / 100.0) * (sorted.len() - 1) as f64) as usize] as f64 }
}

fn print_phase(label: &str, ns: &[u64]) {
    if ns.is_empty() {
        println!("  {} — no data", label);
        return;
    }
    let mut sorted = ns.to_vec();
    sorted.sort_unstable();
    let avg = mean(&sorted) / 1_000.0;
    let p50 = percentile(&sorted, 50.0) / 1_000.0;
    let p90 = percentile(&sorted, 90.0) / 1_000.0;
    let p99 = percentile(&sorted, 99.0) / 1_000.0;
    println!(
        "  {} avg {:>8.1} us  p50 {:>8.1} us  p90 {:>8.1} us  p99 {:>8.1} us",
        label, avg, p50, p90, p99
    );
}

fn print_size_stats(sizes: &[u32]) {
    if sizes.is_empty() { return; }
    let mut s = sizes.to_vec();
    s.sort_unstable();
    let sum: u64 = s.iter().map(|&x| x as u64).sum();
    let avg = sum as f64 / s.len() as f64;
    let p50 = s[s.len() * 50 / 100];
    let p90 = s[s.len() * 90 / 100];
    let p99 = s[s.len() * 99 / 100];
    println!(
        "    avg {:>6.0} B  p50 {:>5} B  p90 {:>5} B  p99 {:>5} B  min {:>5} B  max {:>5} B",
        avg, p50, p90, p99, s[0], s.last().unwrap()
    );
}
