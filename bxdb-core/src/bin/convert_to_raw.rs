//! Convert a bxdb database to a raw-format checkpoint chain.
//!
//! Usage:
//!   convert-to-raw <bxdb_db_dir> <output_name> <memory_size_bytes>
//!
//! Streams chunks.log in natural (snapshot_id, PA) order — no index scan,
//! no per-snapshot bulk loads.  Produces <output_name>.rawmem-test/ chain.
use std::env;
use std::fs::{self, File};
use std::io::{self, BufReader, Read, Write, Seek};
use std::process;

use bxdb::chunk::{
    self, ChunkKind, ChunkRecord, PAGE_SIZE, MAGIC_LOG, HEADER_SIZE,
    apply_delta_patch,
};
use bxdb::btree::PageStore;

const ALIGN: u64 = 4096;

/// Page-aligned mmap'd buffer.
struct MmapBuf { ptr: *mut u8, len: usize }
impl MmapBuf {
    fn new(size: usize) -> Self {
        if size == 0 { return Self { ptr: std::ptr::null_mut(), len: 0 }; }
        let ptr = unsafe {
            libc::mmap(std::ptr::null_mut(), size,
                       libc::PROT_READ | libc::PROT_WRITE,
                       libc::MAP_PRIVATE | libc::MAP_ANONYMOUS, -1, 0)
        };
        if ptr == libc::MAP_FAILED { panic!("mmap({size}) failed"); }
        Self { ptr: ptr as *mut u8, len: size }
    }
    fn as_mut(&mut self) -> &mut [u8] {
        if self.ptr.is_null() { &mut [] }
        else { unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) } }
    }
    fn as_ref(&self) -> &[u8] {
        if self.ptr.is_null() { &[] }
        else { unsafe { std::slice::from_raw_parts(self.ptr, self.len) } }
    }
}
impl Drop for MmapBuf {
    fn drop(&mut self) {
        if !self.ptr.is_null() { unsafe { libc::munmap(self.ptr as *mut _, self.len); } }
    }
}
unsafe impl Send for MmapBuf {}

fn collect_addrs(buf: &[u8], page_count: u64) -> Vec<u64> {
    let mut addrs = Vec::with_capacity(page_count as usize);
    for i in 0..page_count {
        let off = (i * PAGE_SIZE as u64) as usize;
        if !buf[off..off + PAGE_SIZE].iter().all(|&b| b == 0) {
            addrs.push(i * PAGE_SIZE as u64);
        }
    }
    addrs
}

fn write_raw_file(path: &str, addrs: &[u64], memory: &[u8]) -> io::Result<()> {
    let num = addrs.len() as u64;
    let data_off = ((8 + num * 8).wrapping_add(ALIGN - 1)) & !(ALIGN - 1);
    let mut f = File::create(path)?;
    f.write_all(&num.to_le_bytes())?;
    for &a in addrs { f.write_all(&a.to_le_bytes())?; }
    let pos = f.stream_position()?;
    let pad = data_off - pos;
    if pad > 0 { f.write_all(&vec![0u8; pad as usize])?; }
    for &a in addrs {
        f.write_all(&memory[a as usize..a as usize + PAGE_SIZE])?;
    }
    Ok(())
}

fn flush_snapshot(chain_dir: &str, snap_id: u32, buf: &[u8], page_count: u64) {
    let addrs = collect_addrs(buf, page_count);
    let path = format!("{chain_dir}/{snap_id}");
    write_raw_file(&path, &addrs, buf).expect("write failed");
    eprintln!("  [{snap_id}] {nz} non-zero pages → {path}", nz = addrs.len());
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 4 {
        eprintln!("Usage: {} <bxdb_db_dir> <output_name> <memory_size_bytes>", args[0]);
        process::exit(1);
    }
    let db_dir = &args[1];
    let output_name = &args[2];
    let memory_size: u64 = args[3].parse().expect("invalid memory_size");
    if memory_size == 0 || memory_size % PAGE_SIZE as u64 != 0 {
        eprintln!("memory_size must be a positive multiple of {PAGE_SIZE}");
        process::exit(1);
    }
    let page_count = memory_size / PAGE_SIZE as u64;

    // 1. Open the page store to get blob readers
    let store = PageStore::open(std::path::Path::new(db_dir))
        .expect("failed to open bxdb page store");

    // 2. Open chunks.log and read header
    let log_path = format!("{db_dir}/chunks.log");
    let log_file = File::open(&log_path)
        .expect(&format!("cannot open {log_path}"));
    let mut reader = BufReader::new(log_file);

    // Verify header (16 bytes: magic + version + max_snap_id)
    let mut header = [0u8; HEADER_SIZE];
    reader.read_exact(&mut header).expect("failed to read log header");
    if header[0..8] != MAGIC_LOG {
        panic!("bad chunks.log magic");
    }

    // 3. Working buffer for current snapshot state
    let mut state = MmapBuf::new(memory_size as usize);
    let chain_dir = format!("{output_name}.rawmem-test");
    fs::create_dir_all(&chain_dir).expect("mkdir failed");

    // 4. Stream chunks.log, building state and flushing at snapshot boundaries
    let mut current_snap_id: u32 = 0;
    let mut record_count: u64 = 0;
    let mut last_pa: u64 = 0; // for monotonicity check within a snapshot

    eprintln!("Streaming chunks.log → {chain_dir}/ ({} GB)", memory_size as f64 / 1e9);

    let mut truncated = false;
    loop {
        let rec = match ChunkRecord::read_from(&mut reader) {
            Ok(Some(r)) => r,
            Ok(None) => break,  // EOF
            Err(e) => panic!("read error at record {record_count}: {e}"),
        };
        record_count += 1;

        let pa = chunk::pa_of(rec.key);
        let snap = chunk::snapshot_of(rec.key);

        if snap < current_snap_id {
            panic!("rec {record_count}: snap {snap} < current {current_snap_id}");
        }

        if snap != current_snap_id {
            flush_snapshot(&chain_dir, current_snap_id, state.as_ref(), page_count);
            current_snap_id = snap;
            last_pa = pa;
        } else {
            if pa < last_pa {
                panic!("rec {record_count}: snap {snap} PA 0x{pa:x} < 0x{last_pa:x}");
            }
            last_pa = pa;
        }

        // Resolve the chunk into the state buffer
        let dst = &mut state.as_mut()[pa as usize..pa as usize + PAGE_SIZE];
        match rec.kind {
            ChunkKind::Full => {
                if let Err(e) = store.decompress_full_into(&rec, dst.try_into().unwrap()) {
                    let blob_path = format!("{db_dir}/blobs/worker_{}.blob", rec.worker_id);
                    let blob_len = std::fs::metadata(&blob_path)
                        .map(|m| m.len()).unwrap_or(0);
                    eprintln!("Warning: rec {record_count} (snap {snap}, pa=0x{pa:x}) Full decompress failed: {e}");
                    eprintln!("  worker_id={}, offset={}, len={}, blob_file_size={}, end={}",
                              rec.worker_id, rec.offset, rec.len, blob_len,
                              rec.offset as u64 + rec.len as u64);
                    truncated = true; break;
                }
            }
            ChunkKind::Delta => {
                let mut base = [0u8; PAGE_SIZE];
                let base_rec = match store.exact_lookup(rec.base_key) {
                    Ok(Some(r)) => r,
                    _ => {
                        eprintln!("Warning: rec {record_count} delta base not found — truncating");
                        truncated = true; break;
                    }
                };
                if base_rec.kind != ChunkKind::Full {
                    eprintln!("Warning: rec {record_count} delta base is not Full — truncating");
                    truncated = true; break;
                }
                if let Err(e) = store.decompress_full_into(&base_rec, &mut base) {
                    eprintln!("Warning: rec {record_count} delta base decompress failed: {e} — truncating");
                    truncated = true; break;
                }
                let delta_blob = match store.blob_readers.read(rec.worker_id, rec.offset, rec.len) {
                    Ok(b) => b,
                    Err(e) => {
                        // Fetch blob file size for diagnosis
                        let blob_path = format!("{db_dir}/blobs/worker_{}.blob", rec.worker_id);
                        let blob_len = std::fs::metadata(&blob_path)
                            .map(|m| m.len()).unwrap_or(0);
                        eprintln!("Warning: rec {record_count} (snap {snap}, pa=0x{pa:x}) delta blob read failed: {e}");
                        eprintln!("  worker_id={}, offset={}, len={}, blob_file_size={}, end={}",
                                  rec.worker_id, rec.offset, rec.len, blob_len,
                                  rec.offset as u64 + rec.len as u64);
                        eprintln!("  key=0x{:016x}, base_key=0x{:016x}",
                                  rec.key, rec.base_key);
                        truncated = true; break;
                    }
                };
                if let Err(e) = apply_delta_patch(&base, &delta_blob, dst.try_into().unwrap()) {
                    eprintln!("Warning: rec {record_count} delta patch apply failed: {e} — truncating");
                    truncated = true; break;
                }
            }
            ChunkKind::Zero => {
                dst.fill(0);
            }
        }
    }

    // Flush final snapshot only if we didn't truncate mid-stream
    if !truncated {
        flush_snapshot(&chain_dir, current_snap_id, state.as_ref(), page_count);
    }
    let max_snap = if truncated { current_snap_id.saturating_sub(1) } else { current_snap_id };

    fs::write(format!("{chain_dir}/raw-meta"),
              format!("current_snap_id={max_snap}\n"))
        .expect("write raw-meta");

    eprintln!("Done. {} records, {} snapshots flushed → {}",
              record_count, max_snap + 1, chain_dir);
    if truncated {
        eprintln!("  (snapshot {current_snap_id} was incomplete — chain truncated)");
    }
}
