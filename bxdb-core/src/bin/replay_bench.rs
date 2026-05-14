/// Replays checkpoint data through the real process_one and reports per-phase timing.
///
/// Usage: replay-bench <path-to-bxdb-dir> [max_snapshots]
///
/// Reads the existing database, loads snapshot-0 pages into shadow, then for each
/// delta snapshot calls the ACTUAL process_one() for every dirty page. Timing is
/// measured via std::time::Instant around each call and around internal sub-phases
/// (shadow.get, XOR patch, delta encode, compress, blob_write, shadow.insert).

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::BufReader;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;

use bxdb::append_only::{self, BlobFile};
use bxdb::chunk::{
    self, apply_delta_patch, pa_of, snapshot_of, ChunkKind, ChunkRecord, PAGE_SIZE,
};
use bxdb::format;

const DEFAULT_MAX_SNAPS: usize = 3;

#[derive(Clone, Copy)]
struct RecInfo {
    key: u64,
    kind: ChunkKind,
    worker_id: u8,
    offset: u64,
    len: u32,
    base_key: u64,
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: replay-bench <path-to-bxdb-dir> [max_snapshots]");
        std::process::exit(1);
    }
    let dir = Path::new(&args[1]);
    let max_snaps = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(DEFAULT_MAX_SNAPS);

    let log_path = dir.join("chunks.log");
    let blob_dir = dir.join("blobs");

    // ── Phase 1: scan ─────────────────────────────────────────────────────
    eprintln!("Scanning chunks.log...");
    let (full_index, snap_records) = scan_log(&log_path, max_snaps);
    eprintln!("  {} full chunks indexed, {} snapshots", full_index.len(), snap_records.len());

    // ── Phase 2: prepare shadow ────────────────────────────────────────────
    let mut blob_cache = BlobReaders::new(blob_dir);
    let shadow = append_only::Shadow::new();

    let mut needed: HashMap<u64, ()> = HashMap::new();
    for (sid, recs) in &snap_records {
        if *sid > 0 { for ri in recs { needed.insert(pa_of(ri.key), ()); } }
    }
    let mut n_shadow = 0u64;
    for (sid, recs) in &snap_records {
        if *sid != 0 { continue; }
        for ri in recs {
            if ri.kind != ChunkKind::Full { continue; }
            let pa = pa_of(ri.key);
            if needed.contains_key(&pa) {
                if let Some(page) = decompress_full(*ri, &mut blob_cache) {
                    shadow.insert_if_newer(pa, ri.key, Arc::new(page));
                    n_shadow += 1;
                }
            }
        }
    }
    eprintln!("  shadow entries: {}", n_shadow);

    // ── Phase 3: replay through real process_one ───────────────────────────
    let temp_dir = std::env::temp_dir().join("bxdb_replay_bench");
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&temp_dir).expect("create temp dir");
    let blob_path = temp_dir.join("scratch.blob");
    let blob = Mutex::new(BlobFile {
        writer: std::io::BufWriter::new(File::create(&blob_path).expect("create scratch blob")),
        offset: 0,
    });
    let threshold = chunk::DEFAULT_DELTA_THRESHOLD as usize;

    let mut stats = Stats::new();
    let mut processed = 0u64;

    for (snap_id, recs) in &snap_records {
        if *snap_id == 0 { continue; }
        eprintln!("\nSnapshot {}: {} records", *snap_id, recs.len());
        processed += 1;

        let mut snap_stats = SnapStats::default();

        for ri in recs {
            let page = reconstruct_page(*ri, &full_index, &mut blob_cache);
            let pa = pa_of(ri.key);

            // time: zero check (not in process_one since we pre-check here)
            let t0 = Instant::now();
            let is_zero = bxdb::chunk::is_all_zero(&page);
            snap_stats.zero_ns += t0.elapsed().as_nanos() as u64;
            if is_zero {
                snap_stats.zero_pages += 1;
                stats.zero_pages += 1;
                continue;
            }

            // Call the REAL process_one.  It handles shadow.get → XOR/delta
            // or compress+blob_write+shadow.insert internally.
            let t_full = Instant::now();
            let rec = append_only::process_one(
                pa, &page, *snap_id, 0, &shadow, &blob, threshold, true,
            ).expect("process_one");
            let elapsed = t_full.elapsed().as_nanos() as u64;

            match rec.kind {
                ChunkKind::Delta => {
                    snap_stats.delta_pages += 1;
                    snap_stats.delta_ns += elapsed;
                    stats.delta_pages += 1;
                }
                ChunkKind::Full => {
                    snap_stats.full_pages += 1;
                    snap_stats.full_ns += elapsed;
                    stats.full_pages += 1;
                }
                ChunkKind::Zero => {
                    // process_one handled zero internally; shouldn't reach here
                    snap_stats.zero_pages += 1;
                    stats.zero_pages += 1;
                }
            }
        }

        let t_pages = snap_stats.zero_pages + snap_stats.delta_pages + snap_stats.full_pages;
        if t_pages == 0 { continue; }
        let n = t_pages as f64;

        println!(
            "  pages={}  zero={:.0}%  delta={:.0}% (avg {:>6.1} us)  full={:.0}% (avg {:>6.1} us)",
            t_pages,
            100.0 * snap_stats.zero_pages as f64 / n,
            100.0 * snap_stats.delta_pages as f64 / n,
            if snap_stats.delta_pages > 0 { snap_stats.delta_ns as f64 / snap_stats.delta_pages as f64 / 1000.0 } else { 0.0 },
            100.0 * snap_stats.full_pages as f64 / n,
            if snap_stats.full_pages > 0 { snap_stats.full_ns as f64 / snap_stats.full_pages as f64 / 1000.0 } else { 0.0 },
        );

        if processed >= max_snaps as u64 { break; }
    }

    // ── Summary ────────────────────────────────────────────────────────────
    let tot = stats.zero_pages + stats.delta_pages + stats.full_pages;
    if tot > 0 {
        println!();
        println!("Total: {} pages  zero={} delta={} full={}",
            tot, stats.zero_pages, stats.delta_pages, stats.full_pages);
    }
}

// ─── SnapStats ────────────────────────────────────────────────────────────────

#[derive(Default)]
struct SnapStats {
    zero_ns: u64, zero_pages: u64,
    delta_ns: u64, delta_pages: u64,
    full_ns: u64, full_pages: u64,
}

struct Stats {
    zero_pages: u64, delta_pages: u64, full_pages: u64,
}

impl Stats {
    fn new() -> Self { Self { zero_pages: 0, delta_pages: 0, full_pages: 0 } }
}

// ─── Scan log ─────────────────────────────────────────────────────────────────

fn scan_log(log_path: &Path, max_snaps: usize) -> (HashMap<u64, RecInfo>, Vec<(u32, Vec<RecInfo>)>) {
    let f = File::open(log_path).expect("open chunks.log");
    let mut r = BufReader::new(f);
    let _max = format::read_and_verify_header(&mut r, &chunk::MAGIC_LOG).expect("header");

    let mut full_index: HashMap<u64, RecInfo> = HashMap::new();
    let mut by_snap: HashMap<u32, Vec<RecInfo>> = HashMap::new();
    let mut snap_seen = std::collections::HashSet::new();
    snap_seen.insert(0u32); // always include snap 0

    while let Ok(Some(rec)) = ChunkRecord::read_from(&mut r) {
        let snap = snapshot_of(rec.key);
        let info = RecInfo {
            key: rec.key, kind: rec.kind,
            worker_id: rec.worker_id, offset: rec.offset,
            len: rec.len, base_key: rec.base_key,
        };
        if rec.kind == ChunkKind::Full {
            full_index.insert(rec.key, info);
        }
        // collect records for snap 0 and the first N delta snapshots
        if snap_seen.len() <= max_snaps + 1 {
            by_snap.entry(snap).or_default().push(info);
        }
        if snap > 0 && !snap_seen.contains(&snap) && snap_seen.len() > max_snaps + 1 {
            break; // we've seen enough unique snapshots
        }
        snap_seen.insert(snap);
    }

    let mut sorted: Vec<_> = by_snap.into_iter().collect();
    sorted.sort_by_key(|(s, _)| *s);
    (full_index, sorted)
}

// ─── Blob readers ─────────────────────────────────────────────────────────────

struct BlobReaders { dir: PathBuf, files: HashMap<u8, File> }
impl BlobReaders {
    fn new(dir: PathBuf) -> Self { Self { dir, files: HashMap::new() } }
    fn read(&mut self, wid: u8, off: u64, len: u32) -> Vec<u8> {
        if !self.files.contains_key(&wid) {
            let p = self.dir.join(format!("worker_{}.blob", wid));
            self.files.insert(wid, File::open(&p).expect("open blob"));
        }
        let f = &self.files[&wid];
        let mut b = vec![0u8; len as usize];
        f.read_exact_at(&mut b, off).expect("read blob");
        b
    }
}

fn decompress_full(info: RecInfo, cache: &mut BlobReaders) -> Option<[u8; PAGE_SIZE]> {
    let blob = cache.read(info.worker_id, info.offset, info.len);
    let data = zstd::decode_all(&blob[..]).ok()?;
    if data.len() != PAGE_SIZE { return None; }
    let mut out = [0u8; PAGE_SIZE];
    out.copy_from_slice(&data);
    Some(out)
}

fn reconstruct_page(
    info: RecInfo,
    full_index: &HashMap<u64, RecInfo>,
    cache: &mut BlobReaders,
) -> [u8; PAGE_SIZE] {
    match info.kind {
        ChunkKind::Full => decompress_full(info, cache).unwrap_or([0u8; PAGE_SIZE]),
        ChunkKind::Delta => {
            if let Some(base) = full_index.get(&info.base_key) {
                let base_page = decompress_full(*base, cache).unwrap_or([0u8; PAGE_SIZE]);
                let blob = cache.read(info.worker_id, info.offset, info.len);
                let mut out = [0u8; PAGE_SIZE];
                let _ = apply_delta_patch(&base_page, &blob, &mut out);
                out
            } else {
                [0u8; PAGE_SIZE]
            }
        }
        ChunkKind::Zero => [0u8; PAGE_SIZE],
    }
}
