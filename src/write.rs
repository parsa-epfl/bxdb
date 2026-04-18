use std::fs::{self, File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;

use parking_lot::{Mutex, RwLock};
use rustc_hash::FxHashMap;

use crate::chunk::{
    ChunkRecord, DEFAULT_DELTA_THRESHOLD, MAX_SNAPSHOT_ID, MAGIC_LOG, PAGE_SIZE,
    compute_xor_patch, encode_delta_patch, encode_key, is_all_zero,
};
use crate::format::{read_and_verify_header, write_log_header};

const SHADOW_SHARDS: usize = 2048;
const SHADOW_SHARDS_MASK: u64 = (SHADOW_SHARDS as u64) - 1;
const FIB_MUL: u64 = 0x9e3779b97f4a7c15;

type ShadowEntry = (u64, Arc<[u8; PAGE_SIZE]>);
type ShadowShard = RwLock<FxHashMap<u64, ShadowEntry>>;

struct Shadow {
    shards: Box<[ShadowShard]>,
}

impl Shadow {
    fn new() -> Self {
        let v: Vec<ShadowShard> = (0..SHADOW_SHARDS)
            .map(|_| RwLock::new(FxHashMap::default()))
            .collect();
        Self { shards: v.into_boxed_slice() }
    }

    #[inline]
    fn shard(&self, pa: u64) -> &ShadowShard {
        let idx = (pa.wrapping_mul(FIB_MUL) & SHADOW_SHARDS_MASK) as usize;
        &self.shards[idx]
    }

    fn get(&self, pa: u64) -> Option<ShadowEntry> {
        self.shard(pa).read().get(&pa).map(|(k, p)| (*k, Arc::clone(p)))
    }

    fn insert_if_newer(&self, pa: u64, key: u64, page: Arc<[u8; PAGE_SIZE]>) {
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

pub struct WriteDb {
    dir: PathBuf,
    worker_count: usize,
    delta_threshold: u16,
    shadow: Shadow,
    log_file: Mutex<File>,
    blob_files: Vec<Mutex<BlobFile>>,
}

struct BlobFile {
    file: File,
    offset: u64,
}

struct Job<'a> {
    pa: u64,
    page: &'a [u8; PAGE_SIZE],
    snapshot_id: u32,
}

impl WriteDb {
    pub fn open(name: impl AsRef<Path>, worker_count: usize, delta_threshold: u16) -> io::Result<Self> {
        if worker_count == 0 || worker_count > 255 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "worker_count must be in 1..=255",
            ));
        }
        let threshold = if delta_threshold == 0 { DEFAULT_DELTA_THRESHOLD } else { delta_threshold };
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
            write_log_header(&mut log_file)?;
        } else {
            log_file.seek(SeekFrom::Start(0))?;
            read_and_verify_header(&mut log_file, &MAGIC_LOG)?;
            log_file.seek(SeekFrom::End(0))?;
        }

        let mut blob_files = Vec::with_capacity(worker_count);
        for i in 0..worker_count {
            let path = dir.join(format!("blobs/worker_{i}.blob"));
            let file = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .open(&path)?;
            let offset = file.metadata()?.len();
            let mut bf = BlobFile { file, offset };
            bf.file.seek(SeekFrom::End(0))?;
            blob_files.push(Mutex::new(bf));
        }

        Ok(Self {
            dir,
            worker_count,
            delta_threshold: threshold,
            shadow: Shadow::new(),
            log_file: Mutex::new(log_file),
            blob_files,
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn save_pages(
        &self,
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

        let (tx, rx) = crossbeam_channel::bounded::<Job>(self.worker_count * 8);
        let records: Mutex<Vec<ChunkRecord>> = Mutex::new(Vec::new());

        thread::scope(|s| -> io::Result<()> {
            let mut handles = Vec::with_capacity(self.worker_count);
            for wid in 0..self.worker_count {
                let rx = rx.clone();
                let records = &records;
                let shadow = &self.shadow;
                let blob = &self.blob_files[wid];
                let threshold = self.delta_threshold as usize;
                handles.push(s.spawn(move || -> io::Result<()> {
                    let mut local = Vec::new();
                    while let Ok(job) = rx.recv() {
                        process_job(job, wid as u8, shadow, blob, threshold, &mut local)?;
                    }
                    records.lock().extend(local);
                    Ok(())
                }));
            }
            drop(rx);

            for i in 0..total_page_count {
                let word_idx = (i / 64) as usize;
                let bit = (dirty_bitmap[word_idx] >> (i % 64)) & 1;
                if bit == 0 {
                    continue;
                }
                let start = (i * PAGE_SIZE as u64) as usize;
                let page: &[u8; PAGE_SIZE] = memory[start..start + PAGE_SIZE].try_into().unwrap();
                tx.send(Job { pa: i, page, snapshot_id }).unwrap();
            }
            drop(tx);

            for h in handles {
                h.join().map_err(|_| io::Error::other("worker panicked"))??;
            }
            Ok(())
        })?;

        let records = records.into_inner();

        {
            let mut log = self.log_file.lock();
            for r in &records {
                r.write_to(&mut *log)?;
            }
            log.sync_all()?;
        }
        for bf in &self.blob_files {
            bf.lock().file.sync_all()?;
        }
        Ok(())
    }
}

fn process_job(
    job: Job<'_>,
    worker_id: u8,
    shadow: &Shadow,
    blob: &Mutex<BlobFile>,
    threshold: usize,
    records: &mut Vec<ChunkRecord>,
) -> io::Result<()> {
    let key = encode_key(job.pa, job.snapshot_id);

    if is_all_zero(job.page) {
        records.push(ChunkRecord::new_zero(key));
        return Ok(());
    }

    if let Some((base_key, base_page)) = shadow.get(job.pa) {
        let patch = compute_xor_patch(job.page, &base_page);
        if patch.len() <= threshold {
            let data = encode_delta_patch(&patch);
            let (offset, len) = append_blob(blob, &data)?;
            records.push(ChunkRecord::new_delta(key, worker_id, offset, len, base_key));
            return Ok(());
        }
    }

    let compressed = zstd::encode_all(&job.page[..], 3)?;
    let (offset, len) = append_blob(blob, &compressed)?;
    records.push(ChunkRecord::new_full(key, worker_id, offset, len));

    let mut owned = Box::new([0u8; PAGE_SIZE]);
    owned.copy_from_slice(job.page);
    shadow.insert_if_newer(job.pa, key, Arc::from(owned));
    Ok(())
}

fn append_blob(blob: &Mutex<BlobFile>, data: &[u8]) -> io::Result<(u64, u32)> {
    let mut g = blob.lock();
    let offset = g.offset;
    g.file.write_all(data)?;
    g.offset += data.len() as u64;
    let len: u32 = data.len().try_into().map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidData, "blob too large for u32 len")
    })?;
    Ok((offset, len))
}

