use std::fs;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::process::Command;

use bxdb::cache::{SharedCache, TOTAL_BYTES};
use bxdb::chunk::PAGE_SIZE;
use bxdb::purge;
use bxdb::{AppendOnlyDb, IndexMode, BtreeDb};
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use tempfile::TempDir;

/// Thin wrapper that deletes the shared-memory cache file on drop.
struct TestReader {
    db: BtreeDb,
    cache_path: PathBuf,
}

impl Drop for TestReader {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.cache_path);
        // Try to remove the parent hash dir (ok if not empty).
        if let Some(parent) = self.cache_path.parent() {
            let _ = fs::remove_dir(parent);
        }
    }
}

impl std::ops::Deref for TestReader {
    type Target = BtreeDb;
    fn deref(&self) -> &BtreeDb {
        &self.db
    }
}

struct MmapBuf {
    ptr: *mut u8,
    len: usize,
}

impl MmapBuf {
    fn new(n_pages: usize) -> Self {
        let len = n_pages * PAGE_SIZE;
        if len == 0 {
            return Self { ptr: std::ptr::null_mut(), len: 0 };
        }
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        assert!(ptr != libc::MAP_FAILED, "mmap failed for {len} bytes");
        Self { ptr: ptr as *mut u8, len }
    }
}

impl Deref for MmapBuf {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        if self.len == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
        }
    }
}

impl DerefMut for MmapBuf {
    fn deref_mut(&mut self) -> &mut [u8] {
        if self.len == 0 {
            &mut []
        } else {
            unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
        }
    }
}

impl Drop for MmapBuf {
    fn drop(&mut self) {
        if self.len > 0 {
            unsafe { libc::munmap(self.ptr as *mut libc::c_void, self.len); }
        }
    }
}

fn make_memory(n_pages: usize) -> MmapBuf {
    MmapBuf::new(n_pages)
}

fn set_dirty(bitmap: &mut [u64], i: u64) {
    bitmap[(i / 64) as usize] |= 1 << (i % 64);
}

fn random_page(rng: &mut StdRng) -> [u8; PAGE_SIZE] {
    let mut p = [0u8; PAGE_SIZE];
    rng.fill(&mut p[..]);
    p
}

fn set_page(memory: &mut [u8], i: usize, page: &[u8; PAGE_SIZE]) {
    let start = i * PAGE_SIZE;
    memory[start..start + PAGE_SIZE].copy_from_slice(page);
}

fn get_page(memory: &[u8], i: usize) -> [u8; PAGE_SIZE] {
    let start = i * PAGE_SIZE;
    memory[start..start + PAGE_SIZE].try_into().unwrap()
}

fn open_read(dir: &Path) -> TestReader {
    let canonical = dir.canonicalize().unwrap_or_else(|_| dir.to_path_buf());
    let cache_path = bxdb::shm_cache_path(&canonical).unwrap();
    if !cache_path.exists() {
        bxdb::cache::SharedCache::create(&cache_path, &canonical).expect("cache create");
    }
    TestReader {
        db: BtreeDb::open(dir).expect("open btree"),
        cache_path,
    }
}

fn convert_to_btree(dir: &Path) {
    bxdb::convert::to_btree(dir).expect("to-btree");
}

fn convert_to_log(dir: &Path) {
    bxdb::convert::to_log(dir).expect("to-log");
}

#[test]
fn write_read_single_snapshot_btree() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().join("db");
    let n_pages = 16u64;
    let mut rng = StdRng::seed_from_u64(42);

    let mut memory = make_memory(n_pages as usize);
    let mut bitmap = vec![0u64; 1];
    let mut pages = Vec::with_capacity(n_pages as usize);
    for i in 0..n_pages {
        let p = random_page(&mut rng);
        set_page(&mut memory, i as usize, &p);
        set_dirty(&mut bitmap, i);
        pages.push(p);
    }

    let mut wdb = AppendOnlyDb::open(&dir, 4, 0, true).unwrap();
    wdb.save_pages_with_bitmap(&memory, &bitmap, n_pages, 7).unwrap();
    drop(wdb);

    convert_to_btree(&dir);

    let rdb = open_read(&dir);
    assert_eq!(rdb.mode(), IndexMode::BTree);

    for i in 0..n_pages {
        let got = rdb.load_page(i, 7).unwrap().expect("page present");
        assert_eq!(got, pages[i as usize], "page {i} mismatch");
    }

    let missing = rdb.load_page(n_pages, 7).unwrap();
    assert!(missing.is_none(), "out of range PA should return None");
}

#[test]
fn zero_pages_and_reads_without_blob_io() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().join("db");

    let n_pages = 8u64;
    let memory = make_memory(n_pages as usize);
    let mut bitmap = vec![0u64; 1];
    for i in 0..n_pages {
        set_dirty(&mut bitmap, i);
    }

    let mut wdb = AppendOnlyDb::open(&dir, 2, 0, true).unwrap();
    wdb.save_pages_with_bitmap(&memory, &bitmap, n_pages, 1).unwrap();
    drop(wdb);

    convert_to_btree(&dir);

    // All blob files should be empty because every page was zero.
    for w in 0..2 {
        let path = dir.join(format!("blobs/worker_{w}.blob"));
        let meta = std::fs::metadata(&path).unwrap();
        assert_eq!(meta.len(), 0, "worker {w} blob should be empty for all-zero snapshot");
    }

    let rdb = open_read(&dir);
    for i in 0..n_pages {
        let p = rdb.load_page(i, 1).unwrap().unwrap();
        assert!(p.iter().all(|&b| b == 0));
    }
}

#[test]
fn delta_chain_floor_query_multiple_snapshots() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().join("db");

    let n_pages = 4u64;
    let mut rng = StdRng::seed_from_u64(7);
    let mut memory = make_memory(n_pages as usize);
    let mut bitmap = vec![0u64; 1];

    let base: Vec<[u8; PAGE_SIZE]> = (0..n_pages).map(|_| random_page(&mut rng)).collect();
    for (i, p) in base.iter().enumerate() {
        set_page(&mut memory, i, p);
        set_dirty(&mut bitmap, i as u64);
    }

    let mut wdb = AppendOnlyDb::open(&dir, 2, 256, true).unwrap();
    wdb.save_pages_with_bitmap(&memory, &bitmap, n_pages, 10).unwrap();

    // Snapshot 20: modify a few bytes in each page so xor patch is tiny.
    let mut snapshots_20: Vec<[u8; PAGE_SIZE]> = base.clone();
    for (i, p) in snapshots_20.iter_mut().enumerate() {
        p[i * 2] ^= 0xAB;
        p[i * 2 + 1] ^= 0xCD;
        set_page(&mut memory, i, p);
    }
    wdb.save_pages_with_bitmap(&memory, &bitmap, n_pages, 20).unwrap();

    // Snapshot 40: modify further, still small.
    let mut snapshots_40: Vec<[u8; PAGE_SIZE]> = snapshots_20.clone();
    for (i, p) in snapshots_40.iter_mut().enumerate() {
        p[100 + i] ^= 0xEE;
        set_page(&mut memory, i, p);
    }
    wdb.save_pages_with_bitmap(&memory, &bitmap, n_pages, 40).unwrap();
    drop(wdb);

    convert_to_btree(&dir);
    let rdb = open_read(&dir);

    for i in 0..n_pages {
        // Floor of 10: base.
        assert_eq!(rdb.load_page(i, 10).unwrap().unwrap(), base[i as usize]);
        // Floor of 15: should still resolve to snapshot 10.
        assert_eq!(rdb.load_page(i, 15).unwrap().unwrap(), base[i as usize]);
        // Floor of 20: snapshot 20.
        assert_eq!(rdb.load_page(i, 20).unwrap().unwrap(), snapshots_20[i as usize]);
        // Floor of 39: still snapshot 20.
        assert_eq!(rdb.load_page(i, 39).unwrap().unwrap(), snapshots_20[i as usize]);
        // Floor of 40: snapshot 40.
        assert_eq!(rdb.load_page(i, 40).unwrap().unwrap(), snapshots_40[i as usize]);
        // Floor of 524287 (max): snapshot 40.
        assert_eq!(rdb.load_page(i, 524287).unwrap().unwrap(), snapshots_40[i as usize]);
        // Floor of 0: below the lowest snapshot for this PA → None (no version ≤ 0).
        assert!(rdb.load_page(i, 0).unwrap().is_none());
    }
}

#[test]
fn threshold_forces_new_full_and_reads_stay_correct() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().join("db");

    let n_pages = 2u64;
    let mut rng = StdRng::seed_from_u64(9);
    let mut memory = make_memory(n_pages as usize);
    let mut bitmap = vec![0u64; 1];

    let base: Vec<[u8; PAGE_SIZE]> = (0..n_pages).map(|_| random_page(&mut rng)).collect();
    for (i, p) in base.iter().enumerate() {
        set_page(&mut memory, i, p);
        set_dirty(&mut bitmap, i as u64);
    }

    // Force small threshold so any non-trivial diff produces a new Full.
    let mut wdb = AppendOnlyDb::open(&dir, 1, 4, true).unwrap();
    wdb.save_pages_with_bitmap(&memory, &bitmap, n_pages, 1).unwrap();

    // Snapshot 2: change many bytes → exceeds threshold → new Full base.
    let mut snap2 = base.clone();
    for (i, p) in snap2.iter_mut().enumerate() {
        for k in 0..200 {
            p[k * 8] ^= (i as u8).wrapping_add(k as u8 + 1);
        }
        set_page(&mut memory, i, p);
    }
    wdb.save_pages_with_bitmap(&memory, &bitmap, n_pages, 2).unwrap();

    // Snapshot 3: small tweak → Delta against the new Full base (from snap 2).
    let mut snap3 = snap2.clone();
    for (i, p) in snap3.iter_mut().enumerate() {
        p[0] ^= 0xFF;
        p[1] = (i as u8).wrapping_add(9);
        set_page(&mut memory, i, p);
    }
    wdb.save_pages_with_bitmap(&memory, &bitmap, n_pages, 3).unwrap();
    drop(wdb);

    convert_to_btree(&dir);
    let rdb = open_read(&dir);

    for i in 0..n_pages {
        assert_eq!(rdb.load_page(i, 1).unwrap().unwrap(), base[i as usize]);
        assert_eq!(rdb.load_page(i, 2).unwrap().unwrap(), snap2[i as usize]);
        assert_eq!(rdb.load_page(i, 3).unwrap().unwrap(), snap3[i as usize]);
    }
}

#[test]
fn load_all_pages_mixed_kinds() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().join("db");

    let n_pages = 32u64;
    let mut rng = StdRng::seed_from_u64(123);

    // Initial snapshot: random, every even page, plus zero pages for odd.
    let mut memory = make_memory(n_pages as usize);
    let mut bitmap = vec![0u64; 1];
    let mut expected_s1: Vec<[u8; PAGE_SIZE]> = vec![[0u8; PAGE_SIZE]; n_pages as usize];
    for i in 0..n_pages as usize {
        if i % 2 == 0 {
            let p = random_page(&mut rng);
            set_page(&mut memory, i, &p);
            expected_s1[i] = p;
        }
        set_dirty(&mut bitmap, i as u64);
    }

    let mut wdb = AppendOnlyDb::open(&dir, 3, 128, true).unwrap();
    wdb.save_pages_with_bitmap(&memory, &bitmap, n_pages, 100).unwrap();

    // Snapshot 200: only modify half the pages (mix of small delta / new random zeroed).
    let mut bitmap2 = vec![0u64; 1];
    let mut expected_s2 = expected_s1.clone();
    for i in 0..n_pages as usize {
        if i % 3 == 0 {
            let mut p = expected_s1[i];
            p[0] ^= 0x55;
            set_page(&mut memory, i, &p);
            expected_s2[i] = p;
            set_dirty(&mut bitmap2, i as u64);
        }
    }
    wdb.save_pages_with_bitmap(&memory, &bitmap2, n_pages, 200).unwrap();
    drop(wdb);

    convert_to_btree(&dir);
    let append_db = AppendOnlyDb::open(&dir, 3, 128, true).unwrap();

    let mut out = make_memory(n_pages as usize);
    let ok = append_db
        .load_all_pages(&mut out, 0, n_pages, 200, 4)
        .unwrap();
    assert!(ok);
    for i in 0..n_pages as usize {
        assert_eq!(get_page(&out, i), expected_s2[i], "page {i} after snap 200");
    }

    // Subrange with pa_offset.
    let start: u64 = 4;
    let count: u64 = 10;
    let mut sub = make_memory(count as usize);
    let ok = append_db
        .load_all_pages(&mut sub, start, count, 200, 2)
        .unwrap();
    assert!(ok);
    for i in 0..count as usize {
        assert_eq!(get_page(&sub, i), expected_s2[start as usize + i]);
    }
}

#[test]
fn append_only_scan_mode_read_without_conversion() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().join("db");

    let n_pages = 8u64;
    let mut rng = StdRng::seed_from_u64(11);
    let mut memory = make_memory(n_pages as usize);
    let mut bitmap = vec![0u64; 1];
    let pages: Vec<[u8; PAGE_SIZE]> = (0..n_pages).map(|_| random_page(&mut rng)).collect();
    for (i, p) in pages.iter().enumerate() {
        set_page(&mut memory, i, p);
        set_dirty(&mut bitmap, i as u64);
    }

    let mut wdb = AppendOnlyDb::open(&dir, 2, 256, true).unwrap();
    wdb.save_pages_with_bitmap(&memory, &bitmap, n_pages, 5).unwrap();
    drop(wdb);

    // Do not run convert — read directly from chunks.log.
    let rdb = open_read(&dir);
    assert_eq!(rdb.mode(), IndexMode::AppendOnly);

    for i in 0..n_pages {
        let got = rdb.load_page(i, 5).unwrap().unwrap();
        assert_eq!(got, pages[i as usize]);
    }
}

#[test]
fn conversion_is_roundtrippable() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().join("db");

    let n_pages = 4u64;
    let mut rng = StdRng::seed_from_u64(2);
    let mut memory = make_memory(n_pages as usize);
    let mut bitmap = vec![0u64; 1];
    let pages: Vec<[u8; PAGE_SIZE]> = (0..n_pages).map(|_| random_page(&mut rng)).collect();
    for (i, p) in pages.iter().enumerate() {
        set_page(&mut memory, i, p);
        set_dirty(&mut bitmap, i as u64);
    }

    let mut wdb = AppendOnlyDb::open(&dir, 2, 256, true).unwrap();
    wdb.save_pages_with_bitmap(&memory, &bitmap, n_pages, 42).unwrap();
    drop(wdb);

    convert_to_btree(&dir);
    assert!(dir.join("index.bxdb").exists());

    // After to-btree, append-only log still exists as source of truth.
    assert!(dir.join("chunks.log").exists());

    // Round-trip: delete log, produce a new one via to-log.
    std::fs::remove_file(dir.join("chunks.log")).unwrap();
    convert_to_log(&dir);
    assert!(dir.join("chunks.log").exists());

    // Delete the B-tree and read via scan mode over the regenerated log.
    std::fs::remove_file(dir.join("index.bxdb")).unwrap();
    let rdb = open_read(&dir);
    assert_eq!(rdb.mode(), IndexMode::AppendOnly);
    for i in 0..n_pages {
        assert_eq!(rdb.load_page(i, 42).unwrap().unwrap(), pages[i as usize]);
    }
}

#[test]
fn bxdb_convert_binary_runs() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().join("db");

    let n_pages = 2u64;
    let mut rng = StdRng::seed_from_u64(33);
    let mut memory = make_memory(n_pages as usize);
    let mut bitmap = vec![0u64; 1];
    for i in 0..n_pages as usize {
        let p = random_page(&mut rng);
        set_page(&mut memory, i, &p);
    }
    for i in 0..n_pages {
        set_dirty(&mut bitmap, i);
    }

    let mut wdb = AppendOnlyDb::open(&dir, 1, 256, true).unwrap();
    wdb.save_pages_with_bitmap(&memory, &bitmap, n_pages, 1).unwrap();
    drop(wdb);

    let bin = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("bxdb-convert");
    let status = Command::new(&bin)
        .args(["to-btree", dir.to_str().unwrap()])
        .status()
        .unwrap();
    assert!(status.success());
    assert!(dir.join("index.bxdb").exists());

    let status = Command::new(&bin)
        .args(["to-log", dir.to_str().unwrap()])
        .status()
        .unwrap();
    assert!(status.success());
}

#[test]
fn shared_cache_basic_hit_and_evict() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("c.shm");
    SharedCache::create(&path, tmp.path()).unwrap();
    let cache = SharedCache::open(&path, tmp.path()).unwrap();

    let mut out = [0u8; PAGE_SIZE];
    assert!(!cache.get(42, &mut out), "empty cache shouldn't hit");

    let data = [0xAB; PAGE_SIZE];
    cache.put(42, &data);
    assert!(cache.get(42, &mut out));
    assert_eq!(out, data);

    // Overwrite same key
    let data2 = [0xCD; PAGE_SIZE];
    cache.put(42, &data2);
    assert!(cache.get(42, &mut out));
    assert_eq!(out, data2);

    // Fill one set (16 ways) with distinct keys that collide to the same set, then
    // evict. With Fibonacci hashing collisions are rare; instead just confirm many
    // distinct puts + gets round-trip.
    for i in 0..1000u64 {
        let mut page = [0u8; PAGE_SIZE];
        page[0..8].copy_from_slice(&i.to_le_bytes());
        cache.put(i, &page);
    }
    // Key 42 may or may not survive eviction; any successful get must match.
    if cache.get(42, &mut out) {
        // Either original data or 42's encoded 8 bytes
        let expected = {
            let mut p = [0u8; PAGE_SIZE];
            p[0..8].copy_from_slice(&42u64.to_le_bytes());
            p
        };
        assert!(out == data2 || out == expected);
    }
}

#[test]
fn shared_cache_file_sized_and_reattaches() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("c.shm");
    SharedCache::create(&path, tmp.path()).unwrap();
    {
        let c = SharedCache::open(&path, tmp.path()).unwrap();
        let page = [1u8; PAGE_SIZE];
        c.put(999, &page);
    }
    let meta = std::fs::metadata(&path).unwrap();
    assert_eq!(meta.len() as usize, TOTAL_BYTES, "cache file size mismatch");

    // Re-attach; existing entry should survive because the file backs the cache.
    let c = SharedCache::open(&path, tmp.path()).unwrap();
    let mut out = [0u8; PAGE_SIZE];
    assert!(c.get(999, &mut out));
    assert!(out.iter().all(|&b| b == 1));
}

#[test]
fn readdb_uses_shared_cache_file() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().join("db");

    let n_pages = 2u64;
    let mut rng = StdRng::seed_from_u64(88);
    let mut memory = make_memory(n_pages as usize);
    let mut bitmap = vec![0u64; 1];
    let pages: Vec<[u8; PAGE_SIZE]> = (0..n_pages).map(|_| random_page(&mut rng)).collect();
    for (i, p) in pages.iter().enumerate() {
        set_page(&mut memory, i, p);
        set_dirty(&mut bitmap, i as u64);
    }
    let mut wdb = AppendOnlyDb::open(&dir, 1, 256, true).unwrap();
    wdb.save_pages_with_bitmap(&memory, &bitmap, n_pages, 1).unwrap();
    drop(wdb);
    convert_to_btree(&dir);

    {
        let cache_path = bxdb::shm_cache_path(&dir).unwrap();
        let rdb = open_read(&dir);
        for i in 0..n_pages {
            assert_eq!(rdb.load_page(i, 1).unwrap().unwrap(), pages[i as usize]);
        }
        // Cache file exists and is the expected size while reader is alive.
        let meta = fs::metadata(&cache_path).unwrap();
        assert_eq!(meta.len() as usize, TOTAL_BYTES);

        // Reopen — reuses existing cache file.
        let rdb2 = open_read(&dir);
        for i in 0..n_pages {
            assert_eq!(rdb2.load_page(i, 1).unwrap().unwrap(), pages[i as usize]);
        }
    }
}

#[test]
fn key_packing_limits() {
    // Confirm encoding respects 45+19 split.
    let pa_max = (1u64 << 45) - 1;
    let snap_max = (1u32 << 19) - 1;
    let key = bxdb::encode_key(pa_max, snap_max);
    assert_eq!(key >> 19, pa_max);
    assert_eq!(key & ((1u64 << 19) - 1), snap_max as u64);
}

#[test]
fn purge_deletes_high_snapshot_records_and_blobs() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().join("db");
    let n_pages = 16u64;
    let mut rng = StdRng::seed_from_u64(99);

    // Write different pages at each snapshot so we can distinguish them.
    let snap1_page0 = random_page(&mut rng);
    let snap2_page0 = random_page(&mut rng);
    let snap3_page0 = random_page(&mut rng);

    // Snapshot 1: write our known page at PA 0, plus other pages.
    {
        let mut memory = make_memory(n_pages as usize);
        let mut bitmap = vec![0u64; 1];
        set_page(&mut memory, 0, &snap1_page0);
        set_dirty(&mut bitmap, 0);
        for i in 1..n_pages {
            let p = random_page(&mut rng);
            set_page(&mut memory, i as usize, &p);
            set_dirty(&mut bitmap, i);
        }
        let mut append_db = AppendOnlyDb::open(&dir, 4, 0, false).unwrap();
        append_db.save_pages_with_bitmap(&memory, &bitmap, n_pages, 1).unwrap();
    }

    // Snapshot 2: change page 0.
    {
        let mut memory = make_memory(n_pages as usize);
        let mut bitmap = vec![0u64; 1];
        set_page(&mut memory, 0, &snap2_page0);
        set_dirty(&mut bitmap, 0);
        let mut append_db = AppendOnlyDb::open(&dir, 4, 0, false).unwrap();
        append_db.save_pages_with_bitmap(&memory, &bitmap, n_pages, 2).unwrap();
    }

    // Snapshot 3: change page 0 again.
    {
        let mut memory = make_memory(n_pages as usize);
        let mut bitmap = vec![0u64; 1];
        set_page(&mut memory, 0, &snap3_page0);
        set_dirty(&mut bitmap, 0);
        let mut append_db = AppendOnlyDb::open(&dir, 4, 0, false).unwrap();
        append_db.save_pages_with_bitmap(&memory, &bitmap, n_pages, 3).unwrap();
    }

    convert_to_btree(&dir);

    // Before purge: floor queries return the latest page at or below each snapshot.
    let rdb = open_read(&dir);
    assert_eq!(rdb.load_page(0, 1).unwrap().unwrap(), snap1_page0);
    assert_eq!(rdb.load_page(0, 2).unwrap().unwrap(), snap2_page0);
    assert_eq!(rdb.load_page(0, 3).unwrap().unwrap(), snap3_page0);
    drop(rdb);

    // Purge records with snapshot > 1.
    let removed = purge::purge(&dir, 1).unwrap();
    assert!(removed > 0);

    // After purge: snap 2 and 3 records are gone.
    // load_page uses floor: querying snap 2 or 3 falls back to snap 1.
    let rdb = open_read(&dir);
    assert_eq!(rdb.load_page(0, 1).unwrap().unwrap(), snap1_page0);
    assert_eq!(rdb.load_page(0, 2).unwrap().unwrap(), snap1_page0);
    assert_eq!(rdb.load_page(0, 3).unwrap().unwrap(), snap1_page0);

    // Purge again with same threshold removes nothing.
    assert_eq!(purge::purge(&dir, 1).unwrap(), 0);

    // Purge everything.
    let removed2 = purge::purge(&dir, 0).unwrap();
    assert!(removed2 > 0);
    let rdb = open_read(&dir);
    assert!(rdb.load_page(0, 3).unwrap().is_none());
}

#[test]
fn purge_deletes_high_snapshot_from_log_format() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().join("db");
    let n_pages = 16u64;
    let mut rng = StdRng::seed_from_u64(99);

    let snap1_page0 = random_page(&mut rng);
    let snap2_page0 = random_page(&mut rng);
    let snap3_page0 = random_page(&mut rng);

    // Snapshot 1.
    {
        let mut memory = make_memory(n_pages as usize);
        let mut bitmap = vec![0u64; 1];
        set_page(&mut memory, 0, &snap1_page0);
        set_dirty(&mut bitmap, 0);
        for i in 1..n_pages {
            let p = random_page(&mut rng);
            set_page(&mut memory, i as usize, &p);
            set_dirty(&mut bitmap, i);
        }
        let mut append_db = AppendOnlyDb::open(&dir, 4, 0, false).unwrap();
        append_db.save_pages_with_bitmap(&memory, &bitmap, n_pages, 1).unwrap();
    }

    // Snapshot 2.
    {
        let mut memory = make_memory(n_pages as usize);
        let mut bitmap = vec![0u64; 1];
        set_page(&mut memory, 0, &snap2_page0);
        set_dirty(&mut bitmap, 0);
        let mut append_db = AppendOnlyDb::open(&dir, 4, 0, false).unwrap();
        append_db.save_pages_with_bitmap(&memory, &bitmap, n_pages, 2).unwrap();
    }

    // Snapshot 3.
    {
        let mut memory = make_memory(n_pages as usize);
        let mut bitmap = vec![0u64; 1];
        set_page(&mut memory, 0, &snap3_page0);
        set_dirty(&mut bitmap, 0);
        let mut append_db = AppendOnlyDb::open(&dir, 4, 0, false).unwrap();
        append_db.save_pages_with_bitmap(&memory, &bitmap, n_pages, 3).unwrap();
    }

    // Purge directly from chunks.log (no prior conversion).
    assert!(dir.join("chunks.log").exists());
    assert!(!dir.join("index.bxdb").exists());
    let removed = purge::purge(&dir, 1).unwrap();
    assert!(removed > 0);

    // After purge, chunks.log is preserved (not converted to index.bxdb).
    assert!(dir.join("chunks.log").exists());
    assert!(!dir.join("index.bxdb").exists());

    // Floor queries should fall back to snap 1.
    let rdb = open_read(&dir);
    assert_eq!(rdb.load_page(0, 1).unwrap().unwrap(), snap1_page0);
    assert_eq!(rdb.load_page(0, 2).unwrap().unwrap(), snap1_page0);
    assert_eq!(rdb.load_page(0, 3).unwrap().unwrap(), snap1_page0);
}

#[test]
fn save_pages_with_bitmap_rejects_non_monotonic_snapshots() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().join("db");
    let n_pages = 16u64;

    let mut append_db = AppendOnlyDb::open(&dir, 4, 0, false).unwrap();
    let memory = make_memory(n_pages as usize);
    let mut bitmap = vec![0u64; 1];
    for i in 0..n_pages {
        set_dirty(&mut bitmap, i);
    }

    // First save at snap 5 works.
    append_db.save_pages_with_bitmap(&memory, &bitmap, n_pages, 5).unwrap();

    // Same snapshot is rejected.
    assert!(append_db.save_pages_with_bitmap(&memory, &bitmap, n_pages, 5).is_err());

    // Lower snapshot is rejected.
    assert!(append_db.save_pages_with_bitmap(&memory, &bitmap, n_pages, 4).is_err());

    // Higher snapshot still works.
    append_db.save_pages_with_bitmap(&memory, &bitmap, n_pages, 6).unwrap();
}

#[test]
fn batch_load_zero_pages_recovered_without_write() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().join("db");

    let n_pages = 16u64;
    let mut rng = StdRng::seed_from_u64(42);

    // Prepare: pages 0, 3, 7, 12 are non-zero; the rest are zero.
    let mut memory = make_memory(n_pages as usize);
    let mut bitmap = vec![0u64; 1];
    let non_zero_indices: [usize; 4] = [0, 3, 7, 12];
    let mut expected: Vec<[u8; PAGE_SIZE]> = vec![[0u8; PAGE_SIZE]; n_pages as usize];
    for &i in &non_zero_indices {
        let p = random_page(&mut rng);
        set_page(&mut memory, i, &p);
        expected[i] = p;
        set_dirty(&mut bitmap, i as u64);
    }
    // Also mark zero pages as dirty so they are saved explicitly.
    for i in 0..n_pages as usize {
        set_dirty(&mut bitmap, i as u64);
    }

    let mut wdb = AppendOnlyDb::open(&dir, 2, 128, true).unwrap();
    wdb.save_pages_with_bitmap(&memory, &bitmap, n_pages, 10).unwrap();
    drop(wdb);

    // --- Scan mode (append-only, no conversion) ---
    {
        let db = AppendOnlyDb::open(&dir, 2, 128, true).unwrap();
        let mut out = make_memory(n_pages as usize);
        let ok = db.load_all_pages(&mut out, 0, n_pages, 10, 2).unwrap();
        assert!(ok);
        for i in 0..n_pages as usize {
            assert_eq!(
                get_page(&out, i),
                expected[i],
                "scan mode: page {i}"
            );
        }
    }

    // --- Btree mode (after conversion) ---
    convert_to_btree(&dir);
    {
        let db = AppendOnlyDb::open(&dir, 2, 128, true).unwrap();
        let mut out = make_memory(n_pages as usize);
        let ok = db.load_all_pages(&mut out, 0, n_pages, 10, 2).unwrap();
        assert!(ok);
        for i in 0..n_pages as usize {
            assert_eq!(
                get_page(&out, i),
                expected[i],
                "btree mode: page {i}"
            );
        }
    }

    // --- Subrange with pa_offset, includes a zero page at the start ---
    {
        let db = AppendOnlyDb::open(&dir, 2, 128, true).unwrap();
        let start: u64 = 1;
        let count: u64 = 5;
        let mut sub = make_memory(count as usize);
        let ok = db.load_all_pages(&mut sub, start, count, 10, 2).unwrap();
        assert!(ok);
        for i in 0..count as usize {
            let pa = start as usize + i;
            assert_eq!(
                get_page(&sub, i),
                expected[pa],
                "subrange: pa={pa}"
            );
        }
    }
}
