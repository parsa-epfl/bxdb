use std::cell::RefCell;
use std::fs::File;
use std::io::{self, BufReader};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use rustc_hash::FxHashMap;

use crate::cache::SharedCache;
use crate::chunk::{
    ChunkKind, ChunkRecord, FIXED_RECORD_SIZE, HEADER_SIZE, LOG_RECORD_BASE_SIZE, MAGIC_IDX,
    MAGIC_LOG, MAX_SNAPSHOT_ID, PAGE_SIZE, apply_delta_patch_in_place, encode_key, pa_of,
};
use crate::format::{peek_magic, read_and_verify_header};

thread_local! {
    // Compressor uses WindowLog=12, use matching decompressor window
    // to reduce memory footprint and improve cache locality.
    static ZSTD_DEC: RefCell<zstd::bulk::Decompressor<'static>> =
        RefCell::new({
            let mut d = zstd::bulk::Decompressor::new().expect("zstd decompressor init");
            d.set_parameter(zstd::zstd_safe::DParameter::WindowLogMax(12))
                .expect("zstd window log");
            d
        });
}
use crate::timing;

/// Compile-time flag to bypass the shared-memory page cache.
/// When `true`, `BtreeDb` will not create or consult the
/// [`SharedCache`](crate::cache::SharedCache) at `/dev/shm`.
///
/// Build with `--features disable-shared-cache` to activate,
/// or flip this constant to `true` / `false` for ad-hoc testing.
///
/// By default, we set it to true, based on the empirical study.
pub const DISABLE_SHARED_CACHE: bool = true;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexMode {
    BTree,
    AppendOnly,
}

// Cache-less reader primitives shared by BtreeDb (single-page, cached) and
// AppendOnlyDb::load_all_pages (bulk load, uncached: each PA is resolved exactly once
// per call so caching only adds overhead).
pub struct PageStore {
    pub(crate) source: IndexSource,
    pub blob_readers: BlobReaders,
    pub(crate) mode: IndexMode,
}

pub(crate) enum IndexSource {
    // B-tree: index.bxdb is mmap'd read-only. Records are fixed-size and
    // sorted by key. The OS page cache handles hot pages; we never load the
    // whole index into our address space as a Vec or BTreeMap.
    BTree {
        mmap: Mmap,
        num_records: usize,
    },

    // Append-only: chunks.log is variable-length and unsorted. Bulk loads
    // scan it sequentially per §9; single-page lookups build a sorted
    // in-memory index lazily on first call.
    AppendOnly {
        log_path: PathBuf,
        lazy_index: OnceLock<LazyIndex>,
    },
}

pub(crate) struct LazyIndex {
    keys: Vec<u64>,
    records: Vec<ChunkRecord>,
}

pub struct Mmap {
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
            return Ok(Self {
                base: ptr::NonNull::dangling().as_ptr(),
                len: 0,
                _file: file,
            });
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
        Ok(Self {
            base: raw as *const u8,
            len,
            _file: file,
        })
    }

    pub fn as_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.base, self.len) }
    }
}

impl Drop for Mmap {
    fn drop(&mut self) {
        if self.len > 0 && !self.base.is_null() {
            unsafe { libc::munmap(self.base as *mut libc::c_void, self.len) };
        }
    }
}

pub struct BlobReaders {
    files: Vec<Mmap>,
    /// Map worker_id → index into `files`. All blob files as found on disk.
    by_id: FxHashMap<u8, usize>,
}

impl BlobReaders {
    pub fn new(dir: &Path) -> io::Result<Self> {
        let blobs_dir = dir.join("blobs");
        let mut paths: Vec<(u8, PathBuf)> = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&blobs_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if let Some(rest) = name.strip_prefix("worker_") {
                    if let Some(digits) = rest.strip_suffix(".blob") {
                        if let Ok(id) = digits.parse::<u8>() {
                            paths.push((id, entry.path()));
                        }
                    }
                }
            }
        }
        paths.sort_by_key(|(id, _)| *id);
        let mut files: Vec<Mmap> = Vec::with_capacity(paths.len());
        let mut by_id: FxHashMap<u8, usize> = FxHashMap::default();
        for (id, path) in paths {
            let idx = files.len();
            files.push(Mmap::open(&path)?);
            by_id.insert(id, idx);
        }
        Ok(Self { files, by_id })
    }

    pub fn mmap(&self, worker_id: u8) -> &Mmap {
        let idx = self.by_id.get(&worker_id).expect("worker blob not found");
        &self.files[*idx]
    }
}

impl PageStore {
    pub fn open(dir: &Path) -> io::Result<Self> {
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

        Ok(Self {
            blob_readers: BlobReaders::new(dir)?,
            source,
            mode,
        })
    }

    pub(crate) fn mode(&self) -> IndexMode {
        self.mode
    }

    pub(crate) fn log_path(&self) -> Option<&Path> {
        match &self.source {
            IndexSource::AppendOnly { log_path, .. } => Some(log_path.as_path()),
            _ => None,
        }
    }

    pub fn floor(&self, pa: u64, snapshot_id: u32) -> io::Result<Option<ChunkRecord>> {
        let key = encode_key(pa, snapshot_id);
        match &self.source {
            IndexSource::BTree { mmap, num_records } => {
                Ok(floor_mmap(mmap.as_bytes(), *num_records, key, pa))
            }
            IndexSource::AppendOnly {
                log_path,
                lazy_index,
            } => {
                let idx = self.ensure_lazy_index(log_path, lazy_index)?;
                Ok(floor_sorted(&idx.keys, &idx.records, key, pa))
            }
        }
    }

    pub fn exact_lookup(&self, key: u64) -> io::Result<Option<ChunkRecord>> {
        match &self.source {
            IndexSource::BTree { mmap, num_records } => {
                Ok(exact_mmap(mmap.as_bytes(), *num_records, key))
            }
            IndexSource::AppendOnly {
                log_path,
                lazy_index,
            } => {
                let idx = self.ensure_lazy_index(log_path, lazy_index)?;
                Ok(exact_sorted(&idx.keys, &idx.records, key))
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

    pub fn idx_mmap(&self) -> &Mmap {
        match &self.source {
            IndexSource::BTree { mmap, .. } => mmap,
            _ => panic!("idx_mmap called on non-BTree PageStore"),
        }
    }

    pub fn decompress_blob_into(
        &self,
        rec: &ChunkRecord,
        out: &mut [u8; PAGE_SIZE],
    ) -> io::Result<()> {
        let _t_blob = Instant::now();
        let mmap = self.blob_readers.mmap(rec.worker_id);
        let start = rec.offset as usize;
        let end = start.checked_add(rec.len as usize).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "blob offset+len overflow")
        })?;
        let bytes = mmap.as_bytes();
        if end > bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "blob read out of range",
            ));
        }
        let blob = &bytes[start..end];
        let _ = blob.first();
        let _ = blob.get(blob.len().saturating_sub(1));
        timing::LOAD_TIMING
            .blob_read_ns
            .add(_t_blob.elapsed().as_nanos() as u64);
        timing::LOAD_TIMING.blob_read_calls.inc();

        let _t_zstd = Instant::now();
        let written = ZSTD_DEC.with(|d| d.borrow_mut().decompress_to_buffer(blob, out))?;
        timing::LOAD_TIMING
            .zstd_decompress_ns
            .add(_t_zstd.elapsed().as_nanos() as u64);
        timing::LOAD_TIMING.zstd_decompress_calls.inc();

        if written != PAGE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "decompressed size != 4096",
            ));
        }
        Ok(())
    }

    // Resolve any chunk kind into `out` without touching a read cache.
    // Intended for bulk loads where each PA is touched exactly once per call.
    pub(crate) fn resolve_uncached(
        &self,
        rec: &ChunkRecord,
        out: &mut [u8; PAGE_SIZE],
    ) -> io::Result<()> {
        match rec.kind {
            ChunkKind::Zero => Ok(()),
            ChunkKind::Full => self.decompress_blob_into(rec, out),
            ChunkKind::Delta => {
                let base_rec = self.exact_lookup(rec.base_key)?.ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "delta base chunk missing from index",
                    )
                })?;
                if base_rec.kind != ChunkKind::Full {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "delta base is not a Full chunk",
                    ));
                }
                self.decompress_blob_into(&base_rec, out)?;
                let mmap = self.blob_readers.mmap(rec.worker_id);
                let start = rec.offset as usize;
                let end = start + rec.len as usize;
                let delta_blob = mmap.as_bytes().get(start..end).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::UnexpectedEof, "delta blob out of range")
                })?;
                apply_delta_patch_in_place(delta_blob, out)
            }
        }
    }
}

pub struct BtreeDb {
    pub(crate) _dir: PathBuf,
    pub store: PageStore,
    cache: Option<SharedCache>,
}

pub fn shm_cache_path(dir: &Path) -> io::Result<PathBuf> {
    use std::hash::{Hash, Hasher};
    let mut hasher = rustc_hash::FxHasher::default();
    // Mix (dev, ino) with the path bytes: filesystem identity prevents
    // collision across databases, path salt guards against reuse on
    // ephemeral filesystems where inode numbers cycle fast.
    match std::fs::metadata(dir) {
        Ok(meta) => {
            use std::os::unix::fs::MetadataExt;
            meta.dev().hash(&mut hasher);
            meta.ino().hash(&mut hasher);
        }
        Err(_) => {}
    }
    dir.as_os_str().hash(&mut hasher);
    let hash = hasher.finish();
    let shm_dir = PathBuf::from("/dev/shm/bxdb").join(format!("{:016x}", hash));
    std::fs::create_dir_all(&shm_dir)?;
    Ok(shm_dir.join("cache.shm"))
}

impl BtreeDb {
    pub fn open(name: impl AsRef<Path>) -> io::Result<Self> {
        let dir = name.as_ref().to_path_buf();
        let canonical = dir.canonicalize().unwrap_or_else(|_| dir.clone());
        let store = PageStore::open(&dir)?;
        let cache = if DISABLE_SHARED_CACHE {
            None
        } else {
            Some(SharedCache::open(&shm_cache_path(&canonical)?, &canonical)?)
        };
        timing::init_timing();
        Ok(Self {
            _dir: dir,
            store,
            cache,
        })
    }

    pub fn mode(&self) -> IndexMode {
        self.store.mode()
    }

    pub fn load_page(
        &self,
        out: &mut [u8; PAGE_SIZE],
        pa: u64,
        snapshot_id: u32,
    ) -> io::Result<bool> {
        let _t_total = Instant::now();
        if snapshot_id > MAX_SNAPSHOT_ID {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "snapshot_id exceeds 19-bit range",
            ));
        }
        let _t_floor = Instant::now();
        let rec = match self.store.floor(pa, snapshot_id)? {
            Some(r) => r,
            None => return Ok(false),
        };
        timing::LOAD_TIMING
            .floor_ns
            .add(_t_floor.elapsed().as_nanos() as u64);
        self.resolve_cached(&rec, out)?;
        timing::LOAD_TIMING
            .total_ns
            .add(_t_total.elapsed().as_nanos() as u64);
        timing::LOAD_TIMING.total_calls.inc();
        Ok(true)
    }

    fn resolve_cached(&self, rec: &ChunkRecord, out: &mut [u8; PAGE_SIZE]) -> io::Result<()> {
        match rec.kind {
            ChunkKind::Zero => {
                out.fill(0);
                timing::LOAD_TIMING.zero_calls.inc();
                Ok(())
            }
            ChunkKind::Full => {
                let _t_cache_get = Instant::now();
                if let Some(ref cache) = self.cache {
                    if cache.get(rec.key, out) {
                        timing::LOAD_TIMING
                            .full_cache_get_ns
                            .add(_t_cache_get.elapsed().as_nanos() as u64);
                        timing::LOAD_TIMING.full_cache_hits.inc();
                        timing::LOAD_TIMING.full_calls.inc();
                        return Ok(());
                    }
                }
                timing::LOAD_TIMING
                    .full_cache_get_ns
                    .add(_t_cache_get.elapsed().as_nanos() as u64);

                let _t_decompress = Instant::now();
                self.store.decompress_blob_into(rec, out)?;
                timing::LOAD_TIMING
                    .full_decompress_ns
                    .add(_t_decompress.elapsed().as_nanos() as u64);

                let _t_cache_put = Instant::now();
                if let Some(ref cache) = self.cache {
                    cache.put(rec.key, out);
                }
                timing::LOAD_TIMING
                    .full_cache_put_ns
                    .add(_t_cache_put.elapsed().as_nanos() as u64);
                timing::LOAD_TIMING.full_calls.inc();
                Ok(())
            }
            ChunkKind::Delta => {
                self.load_full_cached(rec.base_key, out)?;

                let _t_delta_blob = Instant::now();
                let mmap = self.store.blob_readers.mmap(rec.worker_id);
                let start = rec.offset as usize;
                let end = start + rec.len as usize;
                let delta_blob = mmap.as_bytes().get(start..end).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::UnexpectedEof, "delta blob out of range")
                })?;
                timing::LOAD_TIMING
                    .delta_blob_read_ns
                    .add(_t_delta_blob.elapsed().as_nanos() as u64);

                let _t_patch = Instant::now();
                let r = apply_delta_patch_in_place(delta_blob, out);
                timing::LOAD_TIMING
                    .delta_patch_ns
                    .add(_t_patch.elapsed().as_nanos() as u64);
                timing::LOAD_TIMING.delta_calls.inc();
                r
            }
        }
    }

    fn load_full_cached(&self, key: u64, out: &mut [u8; PAGE_SIZE]) -> io::Result<()> {
        let _t_cache_get = Instant::now();
        if let Some(ref cache) = self.cache {
            if cache.get(key, out) {
                timing::LOAD_TIMING
                    .delta_base_cache_get_ns
                    .add(_t_cache_get.elapsed().as_nanos() as u64);
                return Ok(());
            }
        }
        timing::LOAD_TIMING
            .delta_base_cache_get_ns
            .add(_t_cache_get.elapsed().as_nanos() as u64);

        let _t_lookup = Instant::now();
        let rec = self.store.exact_lookup(key)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "delta base chunk missing from index",
            )
        })?;
        if rec.kind != ChunkKind::Full {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "delta base is not a Full chunk",
            ));
        }
        timing::LOAD_TIMING
            .delta_base_lookup_ns
            .add(_t_lookup.elapsed().as_nanos() as u64);

        let _t_decompress = Instant::now();
        self.store.decompress_blob_into(&rec, out)?;
        timing::LOAD_TIMING
            .delta_base_decompress_ns
            .add(_t_decompress.elapsed().as_nanos() as u64);

        let _t_cache_put = Instant::now();
        if let Some(ref cache) = self.cache {
            cache.put(key, out);
        }
        timing::LOAD_TIMING
            .delta_base_cache_put_ns
            .add(_t_cache_put.elapsed().as_nanos() as u64);
        Ok(())
    }
}

fn verify_index_header(mmap: &Mmap) -> io::Result<()> {
    let bytes = mmap.as_bytes();
    if bytes.len() < HEADER_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "index.bxdb too short",
        ));
    }
    if bytes[0..8] != MAGIC_IDX {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bad index magic",
        ));
    }
    Ok(())
}

fn verify_log_header(path: &Path) -> io::Result<()> {
    let file = File::open(path)?;
    let mut r = BufReader::new(file);
    read_and_verify_header(&mut r, &MAGIC_LOG)?;
    Ok(())
}

fn build_lazy_index(path: &Path) -> io::Result<LazyIndex> {
    let file = File::open(path)?;
    let meta = file.metadata()?;
    let mut r = BufReader::new(file);
    read_and_verify_header(&mut r, &MAGIC_LOG)?;
    let capacity =
        (meta.len().saturating_sub(HEADER_SIZE as u64) / LOG_RECORD_BASE_SIZE as u64) as usize;
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
    let mut jumps = 0u64;
    while lo < hi {
        jumps += 1;
        let mid = lo + (hi - lo) / 2;
        let k = read_key_at(body, mid);
        if k <= key {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    timing::LOAD_TIMING.floor_jumps.add(jumps);
    if lo == 0 {
        return None;
    }
    let idx = lo - 1;
    let rec = read_record_at(body, idx).ok()?;
    if pa_of(rec.key) == pa {
        Some(rec)
    } else {
        None
    }
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
    if pa_of(rec.key) == pa {
        Some(rec)
    } else {
        None
    }
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
    let buf: &[u8; FIXED_RECORD_SIZE] = body[off..off + FIXED_RECORD_SIZE].try_into().unwrap();
    ChunkRecord::decode_fixed(buf)
}

pub fn detect_mode(dir: &Path) -> io::Result<IndexMode> {
    if dir.join("index.bxdb").exists() {
        let mut r = File::open(dir.join("index.bxdb"))?;
        let magic = peek_magic(&mut r)?;
        if magic != MAGIC_IDX {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bad index magic",
            ));
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
