use libc;
use std::cell::RefCell;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

thread_local! {
    // Per-thread zstd context tuned for 4 KB pages:
    //   Level 1:  strategy=fast (no lazy matching — wasted on 4 KB)
    //   WindowLog=12:  4 KB search window (exact page size)
    //   HashLog=12:    4096 hash buckets (~1 per byte position)
    //   ChainLog=10:   1024 chain slots (no collisions on 4 KB)
    //   SearchLog=0:   take first match (no alternatives to check)
    // This shrinks the context from ~2 MB (default zstd-3) to ~30 KB.
    static ZSTD_CTX: RefCell<zstd::bulk::Compressor<'static>> =
        RefCell::new({
            let mut c = zstd::bulk::Compressor::new(1).expect("zstd init");
            c.set_parameter(zstd::zstd_safe::CParameter::WindowLog(12)).expect("wl");
            c.set_parameter(zstd::zstd_safe::CParameter::HashLog(12)).expect("hl");
            c.set_parameter(zstd::zstd_safe::CParameter::ChainLog(10)).expect("cl");
            c.set_parameter(zstd::zstd_safe::CParameter::SearchLog(0)).expect("sl");
            c
        });

    // Reusable output buffer — avoids a Vec allocation per page.
    // 8 KB is more than enough for any 4 KB input.
    static COMPRESS_BUF: RefCell<Vec<u8>> =
        RefCell::new(Vec::with_capacity(8192));
}
use std::thread;

use parking_lot::{Mutex, RwLock};
use rayon::ThreadPool;
use rustc_hash::FxHashMap;

use crate::btree::{IndexMode, PageStore};
use crate::chunk::{
    ChunkRecord, DEFAULT_DELTA_THRESHOLD, MAGIC_LOG, MAX_SNAPSHOT_ID, PAGE_SIZE, compute_xor_patch,
    encode_delta_patch, encode_key, is_all_zero, pa_of, snapshot_of,
};
use crate::format::{read_and_verify_header, write_log_header};

const SHADOW_SHARDS: usize = 2048;
const SHADOW_SHARDS_MASK: u64 = (SHADOW_SHARDS as u64) - 1;
const FIB_MUL: u64 = 0x9e3779b97f4a7c15;

// Pass-2 task granularity: each rayon task processes this many bitmap words
// (= WORDS_PER_GROUP * 64 pages of address space). Small enough for rayon's
// work stealer to balance dense vs sparse regions; large enough that per-task
// overhead is negligible next to zstd.
const WORDS_PER_GROUP: usize = 64;
const PAGES_PER_GROUP: usize = WORDS_PER_GROUP * 64; // 4096

type ShadowEntry = (u64, Arc<[u8; PAGE_SIZE]>);
type ShadowShard = RwLock<FxHashMap<u64, ShadowEntry>>;

#[doc(hidden)]
pub struct Shadow {
    shards: Box<[ShadowShard]>,
}

impl Shadow {
    pub fn new() -> Self {
        let v: Vec<ShadowShard> = (0..SHADOW_SHARDS)
            .map(|_| RwLock::new(FxHashMap::default()))
            .collect();
        Self {
            shards: v.into_boxed_slice(),
        }
    }

    #[inline]
    fn shard(&self, pa: u64) -> &ShadowShard {
        let idx = (pa.wrapping_mul(FIB_MUL) & SHADOW_SHARDS_MASK) as usize;
        &self.shards[idx]
    }

    pub fn get(&self, pa: u64) -> Option<ShadowEntry> {
        self.shard(pa)
            .read()
            .get(&pa)
            .map(|(k, p)| (*k, Arc::clone(p)))
    }

    pub fn insert_if_newer(&self, pa: u64, key: u64, page: Arc<[u8; PAGE_SIZE]>) {
        let mut w = self.shard(pa).write();
        let install = match w.get(&pa) {
            None => true,
            Some((k, _)) => key > *k,
        };
        if install {
            w.insert(pa, (key, page));
        }
    }
}

pub struct AppendOnlyDb {
    dir: PathBuf,
    delta_threshold: u16,
    use_shadow: bool,
    shadow: Shadow,
    log_file: BufWriter<File>,
    blob_files: Vec<Mutex<BlobFile>>,
    pool: ThreadPool,
    // Reusable scratch buffers, sized once and kept across save_pages calls.
    records_buf: Vec<ChunkRecord>,
    // Exclusive prefix-sum of per-group dirty counts; len = num_groups + 1.
    offsets_buf: Vec<u32>,
    // Group indices (u32) of non-empty bitmap groups; built during pass 1b.
    active_groups: Vec<u32>,
    // Tracks the last snapshot_id written; enforces monotonic growth.
    last_snapshot: Option<u32>,
}

pub struct BlobFile {
    pub writer: BufWriter<File>,
    pub offset: u64,
}

// Raw pointer wrapper so rayon tasks can write to disjoint indices of
// records_buf in parallel. Safety contract: each task writes to a distinct
// index range derived from offsets_buf (an exclusive prefix sum), so no two
// tasks ever touch the same slot.
struct RecordsPtr {
    ptr: *mut ChunkRecord,
    len: usize,
}

unsafe impl Send for RecordsPtr {}
unsafe impl Sync for RecordsPtr {}

impl RecordsPtr {
    #[inline]
    unsafe fn write(&self, idx: usize, value: ChunkRecord) {
        debug_assert!(idx < self.len);
        unsafe { self.ptr.add(idx).write(value) }
    }
}

impl AppendOnlyDb {
    pub fn open(
        name: impl AsRef<Path>,
        worker_count: usize,
        delta_threshold: u16,
        use_shadow: bool,
    ) -> io::Result<Self> {
        if worker_count == 0 || worker_count > 255 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "worker_count must be in 1..=255",
            ));
        }
        let threshold = if delta_threshold == 0 {
            DEFAULT_DELTA_THRESHOLD
        } else {
            delta_threshold
        };
        let dir = name.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        fs::create_dir_all(dir.join("blobs"))?;

        let log_path = dir.join("chunks.log");
        let mut log_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&log_path)?;
        let size = log_file.metadata()?.len();
        if size == 0 {
            write_log_header(&mut log_file, 0)?;
        } else {
            log_file.seek(SeekFrom::Start(0))?;
            read_and_verify_header(&mut log_file, &MAGIC_LOG)?;
            log_file.seek(SeekFrom::End(0))?;
        }
        let log_file = BufWriter::with_capacity(1 << 20, log_file);

        let mut blob_files = Vec::with_capacity(worker_count);
        for i in 0..worker_count {
            let path = dir.join(format!("blobs/worker_{i}.blob"));
            let file = OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open(&path)?;
            let offset = file.metadata()?.len();
            let bf = BlobFile {
                writer: BufWriter::new(file),
                offset,
            };
            blob_files.push(Mutex::new(bf));
        }

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(worker_count)
            .thread_name(|i| format!("bxdb-writer-{i}"))
            .build()
            .map_err(io::Error::other)?;

        Ok(Self {
            dir,
            delta_threshold: threshold,
            use_shadow,
            shadow: Shadow::new(),
            log_file,
            blob_files,
            pool,
            records_buf: Vec::new(),
            offsets_buf: Vec::new(),
            active_groups: Vec::with_capacity(8192),
            last_snapshot: None,
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn save_all_pages(
        &mut self,
        memory: &[u8],
        total_page_count: u64,
        snapshot_id: u32,
    ) -> io::Result<()> {
        if snapshot_id > MAX_SNAPSHOT_ID {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "snapshot_id exceeds 19-bit range",
            ));
        }
        if let Some(last) = self.last_snapshot {
            if snapshot_id <= last {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "snapshot_id must increase monotonically: got {snapshot_id} after {last}"
                    ),
                ));
            }
        }
        self.last_snapshot = Some(snapshot_id);
        let total = total_page_count as usize;
        let expected_mem = (total as u128) * (PAGE_SIZE as u128);
        if memory.len() as u128 != expected_mem {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "memory length != total_page_count * 4096",
            ));
        }
        if total == 0 {
            return Ok(());
        }

        let num_groups = (total + PAGES_PER_GROUP - 1) / PAGES_PER_GROUP;

        self.records_buf.clear();
        self.records_buf.reserve(total);
        let buf_ptr = RecordsPtr {
            ptr: self.records_buf.as_mut_ptr(),
            len: total,
        };

        let shadow = &self.shadow;
        let blob_files = &self.blob_files;
        let threshold = self.delta_threshold as usize;
        let blob_count = blob_files.len();
        let use_shadow = self.use_shadow;

        self.pool.install(|| -> io::Result<()> {
            use rayon::prelude::*;
            (0..num_groups)
                .into_par_iter()
                .try_for_each(|group_idx| -> io::Result<()> {
                    let write_base = group_idx * PAGES_PER_GROUP;
                    let group_pa = (group_idx * PAGES_PER_GROUP) as u64;
                    let group_pages = if group_idx + 1 == num_groups {
                        total - write_base
                    } else {
                        PAGES_PER_GROUP
                    };

                    for i in 0..group_pages {
                        let pa = group_pa + i as u64;
                        let mem_off = (pa as usize) * PAGE_SIZE;
                        let page: &[u8; PAGE_SIZE] =
                            memory[mem_off..mem_off + PAGE_SIZE].try_into().unwrap();
                        let wid = (pa as usize) % blob_count;
                        let rec = process_one(
                            pa,
                            page,
                            snapshot_id,
                            wid as u8,
                            shadow,
                            &blob_files[wid],
                            threshold,
                            use_shadow,
                        )?;
                        unsafe { buf_ptr.write(write_base + i, rec) };
                    }
                    Ok(())
                })
        })?;

        unsafe { self.records_buf.set_len(total) };

        for r in &self.records_buf {
            r.write_to(&mut self.log_file)?;
        }

        {
            self.log_file.seek(SeekFrom::Start(9))?;
            self.log_file.write_all(&snapshot_id.to_le_bytes())?;
            self.log_file.seek(SeekFrom::End(0))?;
        }
        Ok(())
    }

    pub fn save_pages_with_bitmap(
        &mut self,
        memory: &[u8],
        dirty_bitmap: &[u64],
        total_page_count: u64,
        snapshot_id: u32,
    ) -> io::Result<()> {
        if snapshot_id > MAX_SNAPSHOT_ID {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "snapshot_id exceeds 19-bit range",
            ));
        }
        if let Some(last) = self.last_snapshot {
            if snapshot_id <= last {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "snapshot_id must increase monotonically: got {snapshot_id} after {last}"
                    ),
                ));
            }
        }
        self.last_snapshot = Some(snapshot_id);
        let expected_mem = (total_page_count as u128) * (PAGE_SIZE as u128);
        if memory.len() as u128 != expected_mem {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "memory length != total_page_count * 4096",
            ));
        }
        let expected_words = ((total_page_count + 63) / 64) as usize;
        if dirty_bitmap.len() < expected_words {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "dirty_bitmap too short",
            ));
        }
        if expected_words == 0 {
            return Ok(());
        }

        let bitmap = &dirty_bitmap[..expected_words];
        let num_groups = (expected_words + WORDS_PER_GROUP - 1) / WORDS_PER_GROUP;

        // Pass 1a (parallel): per-group popcount into offsets_buf[0..num_groups].
        // Pass 1b (serial):   in-place exclusive prefix sum, then total in slot [num_groups].
        // The prefix sum is O(num_groups) serial adds (~100 µs for 128K groups);
        // a log-depth parallel scan isn't worth the complexity at this size.
        self.offsets_buf.clear();
        self.offsets_buf.resize(num_groups + 1, 0);

        let pool = &self.pool;
        {
            let counts = &mut self.offsets_buf[..num_groups];
            pool.install(|| {
                use rayon::prelude::*;
                counts
                    .par_iter_mut()
                    .enumerate()
                    .for_each(|(group_idx, slot)| {
                        let start = group_idx * WORDS_PER_GROUP;
                        let end = ((group_idx + 1) * WORDS_PER_GROUP).min(expected_words);
                        let mut count: u32 = 0;
                        for wi in start..end {
                            let w =
                                mask_last_word(bitmap[wi], wi, expected_words, total_page_count);
                            count += w.count_ones();
                        }
                        *slot = count;
                    });
            });
        }

        let mut acc: u32 = 0;
        self.active_groups.clear();
        for (g, slot) in self.offsets_buf[..num_groups].iter_mut().enumerate() {
            let c = *slot;
            if c > 0 {
                self.active_groups.push(g as u32);
            }
            *slot = acc;
            acc = acc.checked_add(c).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "dirty count exceeds u32")
            })?;
        }
        self.offsets_buf[num_groups] = acc;
        let total_dirty = acc as usize;

        if total_dirty == 0 {
            return Ok(());
        }

        // Pre-size records_buf without zeroing — pass 2 writes every slot
        // exactly once via raw pointer. ChunkRecord is Copy (no Drop), so
        // briefly leaving uninit memory in the Vec is sound as long as we
        // don't read it before all writes complete.
        self.records_buf.clear();
        self.records_buf.reserve(total_dirty);
        let buf_ptr = RecordsPtr {
            ptr: self.records_buf.as_mut_ptr(),
            len: total_dirty,
        };

        let offsets = &self.offsets_buf;
        let active = &self.active_groups;
        let shadow = &self.shadow;
        let blob_files = &self.blob_files;
        let threshold = self.delta_threshold as usize;
        let blob_count = blob_files.len();
        let use_shadow = self.use_shadow;

        self.pool.install(|| -> io::Result<()> {
            use rayon::prelude::*;
            active
                .par_iter()
                .try_for_each(|&group_idx| -> io::Result<()> {
                    let g = group_idx as usize;
                    let mut write_idx = offsets[g] as usize;
                    let group_end = offsets[g + 1] as usize;
                    let start = g * WORDS_PER_GROUP;
                    let end = ((g + 1) * WORDS_PER_GROUP).min(expected_words);

                    for wi in start..end {
                        let mut w =
                            mask_last_word(bitmap[wi], wi, expected_words, total_page_count);
                        let base = (wi as u64) * 64;
                        while w != 0 {
                            let b = w.trailing_zeros() as u64;
                            let pa = base + b;
                            let mem_off = (pa * PAGE_SIZE as u64) as usize;
                            let page: &[u8; PAGE_SIZE] =
                                memory[mem_off..mem_off + PAGE_SIZE].try_into().unwrap();
                            let wid = (pa as usize) % blob_count;
                            let rec = process_one(
                                pa,
                                page,
                                snapshot_id,
                                wid as u8,
                                shadow,
                                &blob_files[wid],
                                threshold,
                                use_shadow,
                            )?;
                            unsafe { buf_ptr.write(write_idx, rec) };
                            write_idx += 1;
                            w &= w - 1;
                        }
                    }
                    debug_assert_eq!(write_idx, group_end);
                    let _ = group_end;
                    Ok(())
                })
        })?;

        // All total_dirty slots have been written exactly once.
        unsafe { self.records_buf.set_len(total_dirty) };

        for r in &self.records_buf {
            r.write_to(&mut self.log_file)?;
        }

        // Update max_snapshot_id in the on-disk header.
        {
            self.log_file.seek(SeekFrom::Start(9))?;
            self.log_file.write_all(&snapshot_id.to_le_bytes())?;
            self.log_file.seek(SeekFrom::End(0))?;
        }
        Ok(())
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
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize };
        if !(out.as_ptr() as usize).is_multiple_of(page_size) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "output buffer address not page-aligned",
            ));
        }
        if !out.is_empty() {
            unsafe {
                libc::madvise(
                    out.as_mut_ptr() as *mut libc::c_void,
                    out.len(),
                    libc::MADV_DONTNEED,
                );
            }
        }
        let worker_count = worker_count.max(1);

        // A bulk load resolves each PA exactly once, so there is no reuse to
        // amortize over the SharedCache; open a cache-less PageStore instead.
        let store = PageStore::open(&self.dir)?;

        match store.mode() {
            IndexMode::BTree => load_all_btree(
                &store,
                out,
                pa_offset,
                total_page_count,
                snapshot_id,
                worker_count,
            ),
            IndexMode::AppendOnly => {
                let log_path = store
                    .log_path()
                    .expect("append-only mode must expose log path");
                load_all_scan(
                    &store,
                    out,
                    pa_offset,
                    total_page_count,
                    snapshot_id,
                    worker_count,
                    log_path,
                )
            }
        }
    }
}

fn load_all_btree(
    store: &PageStore,
    out: &mut [u8],
    pa_offset: u64,
    total_page_count: u64,
    snapshot_id: u32,
    worker_count: usize,
) -> io::Result<bool> {
    let total = total_page_count as usize;
    let pages_per_worker = (total + worker_count - 1) / worker_count;
    let success = AtomicBool::new(true);

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
                    match store.floor(pa, snapshot_id)? {
                        Some(rec) => store.resolve_uncached(&rec, slot_arr)?,
                        None => {
                            success_ref.store(false, Ordering::Relaxed);
                        }
                    }
                }
                Ok(())
            }));
        }
        for h in handles {
            h.join()
                .map_err(|_| io::Error::other("worker panicked"))??;
        }
        Ok(())
    })?;

    Ok(success.load(Ordering::Relaxed))
}

fn load_all_scan(
    store: &PageStore,
    out: &mut [u8],
    pa_offset: u64,
    total_page_count: u64,
    snapshot_id: u32,
    worker_count: usize,
    log_path: &Path,
) -> io::Result<bool> {
    // Architecture §9: a single linear scan of chunks.log keeps the record
    // with the highest snap ≤ snapshot_id per PA in range.
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

    let success = AtomicBool::new(true);
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
                        Some(rec) => store.resolve_uncached(rec, slot_arr)?,
                        None => {
                            success_ref.store(false, Ordering::Relaxed);
                        }
                    }
                }
                Ok(())
            }));
        }
        for h in handles {
            h.join()
                .map_err(|_| io::Error::other("worker panicked"))??;
        }
        Ok(())
    })?;

    Ok(success.load(Ordering::Relaxed))
}

#[inline]
fn mask_last_word(w: u64, wi: usize, expected_words: usize, total_page_count: u64) -> u64 {
    if wi + 1 == expected_words {
        let valid = total_page_count - (wi as u64) * 64;
        if valid < 64 {
            return w & ((1u64 << valid) - 1);
        }
    }
    w
}

pub fn process_one(
    pa: u64,
    page: &[u8; PAGE_SIZE],
    snapshot_id: u32,
    worker_id: u8,
    shadow: &Shadow,
    blob: &Mutex<BlobFile>,
    threshold: usize,
    use_shadow: bool,
) -> io::Result<ChunkRecord> {
    let key = encode_key(pa, snapshot_id);

    if is_all_zero(page) {
        return Ok(ChunkRecord::new_zero(key));
    }

    if use_shadow {
        if let Some((base_key, base_page)) = shadow.get(pa) {
            let patch = compute_xor_patch(page, &base_page);
            if patch.len() <= threshold {
                let data = encode_delta_patch(&patch);
                let (offset, len) = append_blob(blob, &data)?;
                return Ok(ChunkRecord::new_delta(
                    key, worker_id, offset, len, base_key,
                ));
            }
        }
    }

    let (offset, len) = COMPRESS_BUF.with(|buf_cell| {
        let mut buf = buf_cell.borrow_mut();
        buf.clear();
        ZSTD_CTX.with(|c| c.borrow_mut().compress_to_buffer(&page[..], &mut *buf))?;
        let result = append_blob(blob, &buf)?;
        Ok::<_, io::Error>(result)
    })?;

    let rec = ChunkRecord::new_full(key, worker_id, offset, len);

    if use_shadow {
        shadow.insert_if_newer(pa, key, Arc::new(*page));
    }

    Ok(rec)
}

pub fn append_blob(blob: &Mutex<BlobFile>, data: &[u8]) -> io::Result<(u64, u32)> {
    let mut g = blob.lock();
    let offset = g.offset;
    g.writer.write_all(data)?;
    g.offset += data.len() as u64;
    let len: u32 = data
        .len()
        .try_into()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "blob too large for u32 len"))?;
    Ok((offset, len))
}
