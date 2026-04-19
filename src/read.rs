use std::fs::{File, OpenOptions};
use std::io::{self, BufReader};
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::{Arc, OnceLock};
use std::thread;

use parking_lot::RwLock;
use rustc_hash::FxHashMap;

use crate::cache::SharedCache;
use crate::chunk::{
    ChunkKind, ChunkRecord, FIXED_RECORD_SIZE, HEADER_SIZE, LOG_RECORD_BASE_SIZE, MAGIC_IDX,
    MAGIC_LOG, MAX_SNAPSHOT_ID, PAGE_SIZE, apply_delta_patch, encode_key, pa_of, snapshot_of,
};
use crate::format::{peek_magic, read_and_verify_header};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexMode {
    BTree,
    AppendOnly,
}

pub struct ReadDb {
    _dir: PathBuf,
    source: IndexSource,
    blob_readers: BlobReaders,
    cache: SharedCache,
    mode: IndexMode,
}

enum IndexSource {
    // B-tree: index.bxdb is mmap'd read-only. Records are fixed-size and
    // sorted by key. The OS page cache handles hot pages; we never load the
    // whole index into our address space as a Vec or BTreeMap.
    BTree { mmap: Mmap, num_records: usize },

    // Append-only: chunks.log is variable-length and unsorted. load_all_pages
    // scans it sequentially per §9. load_page builds a sorted in-memory index
    // lazily on first call (most bulk readers never touch this path).
    AppendOnly {
        log_path: PathBuf,
        lazy_index: OnceLock<LazyIndex>,
    },
}

struct LazyIndex {
    keys: Vec<u64>,
    records: Vec<ChunkRecord>,
}

struct Mmap {
    base: *const u8,
    len: usize,
    _file: File,
}

unsafe impl Send for Mmap {}
unsafe impl Sync for Mmap {}

impl Mmap {
    fn open(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        let len = file.metadata()?.len() as usize;
        if len == 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "empty index file"));
        }
        let fd = file.as_raw_fd();
        let raw = unsafe {
            libc::mmap(
                ptr::null_mut(),
                len,
                libc::PROT_READ,
                libc::MAP_SHARED,
                fd,
                0,
            )
        };
        if raw == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        Ok(Self { base: raw as *const u8, len, _file: file })
    }

    fn as_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.base, self.len) }
    }
}

impl Drop for Mmap {
    fn drop(&mut self) {
        if !self.base.is_null() {
            unsafe { libc::munmap(self.base as *mut libc::c_void, self.len) };
        }
    }
}

struct BlobReaders {
    dir: PathBuf,
    files: RwLock<FxHashMap<u8, Arc<File>>>,
}

impl BlobReaders {
    fn new(dir: &Path) -> Self {
        Self { dir: dir.to_path_buf(), files: RwLock::new(FxHashMap::default()) }
    }

    fn read(&self, worker_id: u8, offset: u64, len: u32) -> io::Result<Vec<u8>> {
        let f = self.get(worker_id)?;
        let mut buf = vec![0u8; len as usize];
        f.read_exact_at(&mut buf, offset)?;
        Ok(buf)
    }

    fn get(&self, worker_id: u8) -> io::Result<Arc<File>> {
        if let Some(f) = self.files.read().get(&worker_id) {
            return Ok(Arc::clone(f));
        }
        let path = self.dir.join("blobs").join(format!("worker_{worker_id}.blob"));
        let file = Arc::new(OpenOptions::new().read(true).open(&path)?);
        let mut w = self.files.write();
        if let Some(f) = w.get(&worker_id) {
            return Ok(Arc::clone(f));
        }
        w.insert(worker_id, Arc::clone(&file));
        Ok(file)
    }
}

impl ReadDb {
    pub fn open(name: impl AsRef<Path>) -> io::Result<Self> {
        let dir = name.as_ref().to_path_buf();
        let idx_path = dir.join("index.bxdb");
        let log_path = dir.join("chunks.log");

        let (source, mode) = if idx_path.exists() {
            let mmap = Mmap::open(&idx_path)?;
            verify_index_header(&mmap)?;
            let body_len = mmap.len - HEADER_SIZE;
            if body_len % FIXED_RECORD_SIZE != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "index.bxdb body size not aligned to record size",
                ));
            }
            let num_records = body_len / FIXED_RECORD_SIZE;
            (IndexSource::BTree { mmap, num_records }, IndexMode::BTree)
        } else if log_path.exists() {
            verify_log_header(&log_path)?;
            (
                IndexSource::AppendOnly {
                    log_path,
                    lazy_index: OnceLock::new(),
                },
                IndexMode::AppendOnly,
            )
        } else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "neither index.bxdb nor chunks.log found",
            ));
        };

        let cache = SharedCache::open(&dir.join("cache.shm"))?;

        Ok(Self {
            blob_readers: BlobReaders::new(&dir),
            _dir: dir,
            source,
            cache,
            mode,
        })
    }

    pub fn mode(&self) -> IndexMode {
        self.mode
    }

    pub fn load_page(&self, pa: u64, snapshot_id: u32) -> io::Result<Option<[u8; PAGE_SIZE]>> {
        if snapshot_id > MAX_SNAPSHOT_ID {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "snapshot_id exceeds 19-bit range",
            ));
        }
        let rec = match self.floor(pa, snapshot_id)? {
            Some(r) => r,
            None => return Ok(None),
        };
        let mut out = [0u8; PAGE_SIZE];
        self.resolve(&rec, &mut out)?;
        Ok(Some(out))
    }

    pub fn load_all_pages(
        &self,
        out: &mut [u8],
        pa_offset: u64,
        total_page_count: u64,
        snapshot_id: u32,
        worker_count: usize,
    ) -> io::Result<bool> {
        if snapshot_id > MAX_SNAPSHOT_ID {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "snapshot_id exceeds 19-bit range",
            ));
        }
        if out.len() as u128 != (total_page_count as u128) * (PAGE_SIZE as u128) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "output length != total_page_count * 4096",
            ));
        }
        let worker_count = worker_count.max(1);

        match &self.source {
            IndexSource::BTree { .. } => {
                self.load_all_btree(out, pa_offset, total_page_count, snapshot_id, worker_count)
            }
            IndexSource::AppendOnly { log_path, .. } => self.load_all_scan(
                out,
                pa_offset,
                total_page_count,
                snapshot_id,
                worker_count,
                log_path,
            ),
        }
    }

    fn load_all_btree(
        &self,
        out: &mut [u8],
        pa_offset: u64,
        total_page_count: u64,
        snapshot_id: u32,
        worker_count: usize,
    ) -> io::Result<bool> {
        let total = total_page_count as usize;
        let pages_per_worker = (total + worker_count - 1) / worker_count;
        let success = std::sync::atomic::AtomicBool::new(true);

        thread::scope(|s| -> io::Result<()> {
            let mut starts = Vec::with_capacity(worker_count);
            let mut cursor = 0usize;
            let mut remaining = out;
            for _ in 0..worker_count {
                if cursor >= total {
                    break;
                }
                let take = pages_per_worker.min(total - cursor);
                let split = take * PAGE_SIZE;
                let (head, tail) = remaining.split_at_mut(split);
                starts.push((cursor, head));
                remaining = tail;
                cursor += take;
            }
            let mut handles = Vec::with_capacity(starts.len());
            for (start_idx, slice) in starts {
                let success_ref = &success;
                handles.push(s.spawn(move || -> io::Result<()> {
                    for (i, slot) in slice.chunks_exact_mut(PAGE_SIZE).enumerate() {
                        let pa = pa_offset + (start_idx + i) as u64;
                        let slot_arr: &mut [u8; PAGE_SIZE] = slot.try_into().unwrap();
                        match self.floor(pa, snapshot_id)? {
                            Some(rec) => self.resolve(&rec, slot_arr)?,
                            None => {
                                slot_arr.fill(0);
                                success_ref
                                    .store(false, std::sync::atomic::Ordering::Relaxed);
                            }
                        }
                    }
                    Ok(())
                }));
            }
            for h in handles {
                h.join().map_err(|_| io::Error::other("worker panicked"))??;
            }
            Ok(())
        })?;

        Ok(success.load(std::sync::atomic::Ordering::Relaxed))
    }

    fn load_all_scan(
        &self,
        out: &mut [u8],
        pa_offset: u64,
        total_page_count: u64,
        snapshot_id: u32,
        worker_count: usize,
        log_path: &Path,
    ) -> io::Result<bool> {
        // Single linear scan per architecture §9: for each PA in range, retain
        // the record with the highest snap ≤ snapshot_id.
        let total = total_page_count as usize;
        let mut latest: Vec<Option<ChunkRecord>> = vec![None; total];
        let end_pa = pa_offset + total_page_count;

        let file = File::open(log_path)?;
        let mut r = BufReader::new(file);
        read_and_verify_header(&mut r, &MAGIC_LOG)?;
        while let Some(rec) = ChunkRecord::read_from(&mut r)? {
            let pa = pa_of(rec.key);
            if pa < pa_offset || pa >= end_pa {
                continue;
            }
            let snap = snapshot_of(rec.key);
            if snap > snapshot_id {
                continue;
            }
            let slot = (pa - pa_offset) as usize;
            match &latest[slot] {
                None => latest[slot] = Some(rec),
                Some(prev) if snapshot_of(prev.key) < snap => latest[slot] = Some(rec),
                _ => {}
            }
        }

        let success = std::sync::atomic::AtomicBool::new(true);
        let pages_per_worker = (total + worker_count - 1) / worker_count;

        thread::scope(|s| -> io::Result<()> {
            let mut cursor = 0usize;
            let mut remaining = out;
            let mut handles = Vec::with_capacity(worker_count);
            let latest = &latest;
            for _ in 0..worker_count {
                if cursor >= total {
                    break;
                }
                let take = pages_per_worker.min(total - cursor);
                let split = take * PAGE_SIZE;
                let (head, tail) = remaining.split_at_mut(split);
                let start_idx = cursor;
                let success_ref = &success;
                cursor += take;
                remaining = tail;
                handles.push(s.spawn(move || -> io::Result<()> {
                    for (i, slot) in head.chunks_exact_mut(PAGE_SIZE).enumerate() {
                        let slot_arr: &mut [u8; PAGE_SIZE] = slot.try_into().unwrap();
                        match &latest[start_idx + i] {
                            Some(rec) => self.resolve(rec, slot_arr)?,
                            None => {
                                slot_arr.fill(0);
                                success_ref
                                    .store(false, std::sync::atomic::Ordering::Relaxed);
                            }
                        }
                    }
                    Ok(())
                }));
            }
            for h in handles {
                h.join().map_err(|_| io::Error::other("worker panicked"))??;
            }
            Ok(())
        })?;

        Ok(success.load(std::sync::atomic::Ordering::Relaxed))
    }

    fn floor(&self, pa: u64, snapshot_id: u32) -> io::Result<Option<ChunkRecord>> {
        let key = encode_key(pa, snapshot_id);
        match &self.source {
            IndexSource::BTree { mmap, num_records } => {
                Ok(floor_mmap(mmap.as_bytes(), *num_records, key, pa))
            }
            IndexSource::AppendOnly { log_path, lazy_index } => {
                let idx = self.ensure_lazy_index(log_path, lazy_index)?;
                Ok(floor_sorted(&idx.keys, &idx.records, key, pa))
            }
        }
    }

    fn ensure_lazy_index<'a>(
        &self,
        log_path: &Path,
        slot: &'a OnceLock<LazyIndex>,
    ) -> io::Result<&'a LazyIndex> {
        if let Some(idx) = slot.get() {
            return Ok(idx);
        }
        let built = build_lazy_index(log_path)?;
        Ok(slot.get_or_init(|| built))
    }

    fn resolve(&self, rec: &ChunkRecord, out: &mut [u8; PAGE_SIZE]) -> io::Result<()> {
        match rec.kind {
            ChunkKind::Zero => {
                out.fill(0);
                Ok(())
            }
            ChunkKind::Full => {
                if self.cache.get(rec.key, out) {
                    return Ok(());
                }
                self.decompress_full_into(rec, out)?;
                self.cache.put(rec.key, out);
                Ok(())
            }
            ChunkKind::Delta => {
                let mut base = [0u8; PAGE_SIZE];
                self.load_full_cached(rec.base_key, &mut base)?;
                let delta_blob = self.blob_readers.read(rec.worker_id, rec.offset, rec.len)?;
                apply_delta_patch(&base, &delta_blob, out)
            }
        }
    }

    fn decompress_full_into(&self, rec: &ChunkRecord, out: &mut [u8; PAGE_SIZE]) -> io::Result<()> {
        let blob = self.blob_readers.read(rec.worker_id, rec.offset, rec.len)?;
        let decompressed = zstd::decode_all(&blob[..])?;
        if decompressed.len() != PAGE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Full chunk decompressed size != 4096",
            ));
        }
        out.copy_from_slice(&decompressed);
        Ok(())
    }

    fn load_full_cached(&self, key: u64, out: &mut [u8; PAGE_SIZE]) -> io::Result<()> {
        if self.cache.get(key, out) {
            return Ok(());
        }
        let rec = self.exact_lookup(key)?.ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "delta base chunk missing from index")
        })?;
        if rec.kind != ChunkKind::Full {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "delta base is not a Full chunk",
            ));
        }
        self.decompress_full_into(&rec, out)?;
        self.cache.put(key, out);
        Ok(())
    }

    fn exact_lookup(&self, key: u64) -> io::Result<Option<ChunkRecord>> {
        match &self.source {
            IndexSource::BTree { mmap, num_records } => {
                Ok(exact_mmap(mmap.as_bytes(), *num_records, key))
            }
            IndexSource::AppendOnly { log_path, lazy_index } => {
                let idx = self.ensure_lazy_index(log_path, lazy_index)?;
                Ok(exact_sorted(&idx.keys, &idx.records, key))
            }
        }
    }
}

fn verify_index_header(mmap: &Mmap) -> io::Result<()> {
    let bytes = mmap.as_bytes();
    if bytes.len() < HEADER_SIZE {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "index.bxdb too short"));
    }
    if bytes[0..8] != MAGIC_IDX {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad index magic"));
    }
    Ok(())
}

fn verify_log_header(path: &Path) -> io::Result<()> {
    let file = File::open(path)?;
    let mut r = BufReader::new(file);
    read_and_verify_header(&mut r, &MAGIC_LOG)
}

fn build_lazy_index(path: &Path) -> io::Result<LazyIndex> {
    let file = File::open(path)?;
    let meta = file.metadata()?;
    let mut r = BufReader::new(file);
    read_and_verify_header(&mut r, &MAGIC_LOG)?;
    let capacity = (meta.len().saturating_sub(HEADER_SIZE as u64)
        / LOG_RECORD_BASE_SIZE as u64) as usize;
    let mut records: Vec<ChunkRecord> = Vec::with_capacity(capacity);
    while let Some(rec) = ChunkRecord::read_from(&mut r)? {
        records.push(rec);
    }
    records.sort_by_key(|r| r.key);
    records.dedup_by_key(|r| r.key);
    let keys: Vec<u64> = records.iter().map(|r| r.key).collect();
    Ok(LazyIndex { keys, records })
}

fn floor_mmap(bytes: &[u8], n: usize, key: u64, pa: u64) -> Option<ChunkRecord> {
    let body = &bytes[HEADER_SIZE..];
    // Partition point: smallest idx with record_key(idx) > key.
    let mut lo = 0usize;
    let mut hi = n;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let k = read_key_at(body, mid);
        if k <= key {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if lo == 0 {
        return None;
    }
    let idx = lo - 1;
    let rec = read_record_at(body, idx).ok()?;
    if pa_of(rec.key) == pa { Some(rec) } else { None }
}

fn exact_mmap(bytes: &[u8], n: usize, key: u64) -> Option<ChunkRecord> {
    let body = &bytes[HEADER_SIZE..];
    let mut lo = 0usize;
    let mut hi = n;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let k = read_key_at(body, mid);
        if k == key {
            return read_record_at(body, mid).ok();
        } else if k < key {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    None
}

fn floor_sorted(keys: &[u64], records: &[ChunkRecord], key: u64, pa: u64) -> Option<ChunkRecord> {
    let idx = partition_point_le(keys, key);
    if idx == 0 {
        return None;
    }
    let rec = records[idx - 1];
    if pa_of(rec.key) == pa { Some(rec) } else { None }
}

fn exact_sorted(keys: &[u64], records: &[ChunkRecord], key: u64) -> Option<ChunkRecord> {
    match keys.binary_search(&key) {
        Ok(i) => Some(records[i]),
        Err(_) => None,
    }
}

fn partition_point_le(keys: &[u64], key: u64) -> usize {
    keys.partition_point(|&k| k <= key)
}

#[inline]
fn read_key_at(body: &[u8], idx: usize) -> u64 {
    let off = idx * FIXED_RECORD_SIZE;
    u64::from_le_bytes(body[off..off + 8].try_into().unwrap())
}

#[inline]
fn read_record_at(body: &[u8], idx: usize) -> io::Result<ChunkRecord> {
    let off = idx * FIXED_RECORD_SIZE;
    let buf: &[u8; FIXED_RECORD_SIZE] =
        body[off..off + FIXED_RECORD_SIZE].try_into().unwrap();
    ChunkRecord::decode_fixed(buf)
}

pub fn detect_mode(dir: &Path) -> io::Result<IndexMode> {
    if dir.join("index.bxdb").exists() {
        let mut r = File::open(dir.join("index.bxdb"))?;
        let magic = peek_magic(&mut r)?;
        if magic != MAGIC_IDX {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "bad index magic"));
        }
        Ok(IndexMode::BTree)
    } else if dir.join("chunks.log").exists() {
        let mut r = File::open(dir.join("chunks.log"))?;
        let magic = peek_magic(&mut r)?;
        if magic != MAGIC_LOG {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "bad log magic"));
        }
        Ok(IndexMode::AppendOnly)
    } else {
        Err(io::Error::new(io::ErrorKind::NotFound, "no index file"))
    }
}
