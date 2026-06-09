use std::cell::RefCell;
use std::path::Path;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use bxdb::BtreeDb;
use bxdb::chunk::*;

const ITER: usize = 10_000;

thread_local! {
    static ZSTD_DEC: RefCell<zstd::bulk::Decompressor<'static>> =
        RefCell::new({
            let mut d = zstd::bulk::Decompressor::new().unwrap();
            d.set_parameter(zstd::zstd_safe::DParameter::WindowLogMax(12)).unwrap();
            d
        });
}

fn bench_shared(db: &Arc<BtreeDb>, n_workers: usize) {
    let store = &db.store;

    let idx = store.idx_mmap();
    let body = &idx.as_bytes()[HEADER_SIZE..];
    let total = body.len() / FIXED_RECORD_SIZE;
    let step = (total / ITER).max(1);
    let mut recs = Vec::new();
    for i in (0..total).step_by(step) {
        let off = i * FIXED_RECORD_SIZE;
        if off + FIXED_RECORD_SIZE > body.len() { break; }
        if let Ok(rec) = ChunkRecord::decode_fixed(&body[off..off+FIXED_RECORD_SIZE].try_into().unwrap()) {
            if rec.kind == ChunkKind::Full {
                recs.push(rec);
            }
        }
        if recs.len() >= ITER { break; }
    }
    let n = recs.len();
    eprintln!("{} Full records, {} concurrent workers (shared store)", n, n_workers);

    // Warmup: single-thread to ramp CPU frequency and warm caches
    eprint!("  warming up... ");
    let mut out = [0u8; PAGE_SIZE];
    for _ in 0..2 {
        for rec in &recs {
            store.decompress_blob_into(rec, &mut out).ok();
        }
    }
    eprintln!("done.");

    let recs = Arc::new(recs);
    let per_zstd_ns: Arc<Vec<AtomicU64>> = Arc::new((0..n_workers).map(|_| AtomicU64::new(0)).collect());
    let per_blob_ns: Arc<Vec<AtomicU64>> = Arc::new((0..n_workers).map(|_| AtomicU64::new(0)).collect());
    let barrier = Arc::new(Barrier::new(n_workers + 1));

    let mut handles = Vec::new();
    for t in 0..n_workers {
        let r = recs.clone();
        let b = barrier.clone();
        let pz = per_zstd_ns.clone();
        let pb = per_blob_ns.clone();
        let db2 = db.clone();
        let handle = std::thread::spawn(move || {
            let store = &db2.store;
            let mut out = [0u8; PAGE_SIZE];
            b.wait();
            let mut zsum = 0u64;
            let mut bsum = 0u64;
            for rec in r.iter() {
                let mmap = store.blob_readers.mmap(rec.worker_id);
                let s = rec.offset as usize;
                let e = s + rec.len as usize;
                let tb0 = Instant::now();
                let blob = &mmap.as_bytes()[s..e];
                let _ = blob.first();
                let _ = blob.get(blob.len().saturating_sub(1));
                bsum += tb0.elapsed().as_nanos() as u64;
                let tz0 = Instant::now();
                ZSTD_DEC.with(|d| {
                    let n_out = d.borrow_mut().decompress_to_buffer(blob, &mut out).unwrap();
                    assert_eq!(n_out, PAGE_SIZE);
                });
                zsum += tz0.elapsed().as_nanos() as u64;
            }
            pz[t].store(zsum, Ordering::Relaxed);
            pb[t].store(bsum, Ordering::Relaxed);
        });
        handles.push(handle);
    }

    barrier.wait();
    for h in handles {
        h.join().unwrap();
    }

    let mut all_zstd: Vec<f64> = (0..n_workers).map(|t| per_zstd_ns[t].load(Ordering::Relaxed) as f64 / n as f64 / 1000.0).collect();
    let mut all_blob: Vec<f64> = (0..n_workers).map(|t| per_blob_ns[t].load(Ordering::Relaxed) as f64 / n as f64 / 1000.0).collect();
    let avg_zstd = all_zstd.iter().sum::<f64>() / n_workers as f64;
    let avg_blob = all_blob.iter().sum::<f64>() / n_workers as f64;
    all_zstd.sort_by(|a, b| a.partial_cmp(b).unwrap());
    all_blob.sort_by(|a, b| a.partial_cmp(b).unwrap());
    println!(
        "   zstd {:.1} us  |  blob_read {:.1} us  ({} workers × {} calls)",
        avg_zstd, avg_blob, n_workers, n
    );
    if n_workers > 1 {
        fn pct(v: &[f64], idx: usize) -> f64 { v[idx] }
        println!("   per-worker zstd:  min {:.1}  p50 {:.1}  p99 {:.1}  max {:.1}",
            pct(&all_zstd, 0), pct(&all_zstd, n_workers/2), pct(&all_zstd, n_workers*99/100), pct(&all_zstd, n_workers-1));
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let path = Path::new(&args[1]);
    let n_workers: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(1);

    // Cold open triggers self-test
    println!("=== cold open + self-test ===");
    let db = Arc::new(BtreeDb::open(path).unwrap());

    println!();
    bench_shared(&db, n_workers);

    // Detailed breakdown (single-threaded, warm)
    println!();
    println!("=== single-threaded detailed breakdown ===");
    let store = &db.store;

    let idx = store.idx_mmap();
    let body = &idx.as_bytes()[HEADER_SIZE..];
    let total = body.len() / FIXED_RECORD_SIZE;
    let step = (total / ITER).max(1);
    let mut recs = Vec::new();
    for i in (0..total).step_by(step) {
        let off = i * FIXED_RECORD_SIZE;
        if off + FIXED_RECORD_SIZE > body.len() { break; }
        if let Ok(rec) = ChunkRecord::decode_fixed(&body[off..off+FIXED_RECORD_SIZE].try_into().unwrap()) {
            if rec.kind == ChunkKind::Full {
                recs.push(rec);
            }
        }
        if recs.len() >= ITER { break; }
    }
    let n = recs.len();

    let mut out = [0u8; PAGE_SIZE];

    let mut dec = zstd::bulk::Decompressor::new().unwrap();
    let t0 = Instant::now();
    for rec in &recs {
        let mmap = store.blob_readers.mmap(rec.worker_id);
        let s = rec.offset as usize;
        let e = s + rec.len as usize;
        let blob = &mmap.as_bytes()[s..e];
        let _ = blob.first();
        let _ = blob.get(blob.len().saturating_sub(1));
        let n_out = dec.decompress_to_buffer(blob, &mut out).unwrap();
        assert_eq!(n_out, PAGE_SIZE);
    }
    let ta = t0.elapsed().as_nanos() as f64 / n as f64 / 1000.0;
    println!("A  local decompressor          {ta:.1} us/call");

    let t0 = Instant::now();
    for rec in &recs {
        let mmap = store.blob_readers.mmap(rec.worker_id);
        let s = rec.offset as usize;
        let e = s + rec.len as usize;
        let blob = &mmap.as_bytes()[s..e];
        let _ = blob.first();
        let _ = blob.get(blob.len().saturating_sub(1));
        ZSTD_DEC.with(|d| {
            let n_out = d.borrow_mut().decompress_to_buffer(blob, &mut out).unwrap();
            assert_eq!(n_out, PAGE_SIZE);
        });
    }
    let tb = t0.elapsed().as_nanos() as f64 / n as f64 / 1000.0;
    println!("B  thread_local! + RefCell      {tb:.1} us/call  (+{:.1} us overhead)", tb - ta);

    let t0 = Instant::now();
    for rec in &recs {
        store.decompress_blob_into(rec, &mut out).unwrap();
    }
    let tc = t0.elapsed().as_nanos() as f64 / n as f64 / 1000.0;
    println!("C  decompress_blob_into()       {tc:.1} us/call");

    let t0 = Instant::now();
    for rec in &recs {
        let pa = pa_of(rec.key);
        let snap = snapshot_of(rec.key);
        let mut page = [0u8; PAGE_SIZE];
        let found = db.load_page(&mut page, pa, snap).unwrap();
        assert!(found);
        std::hint::black_box(page);
    }
    let td = t0.elapsed().as_nanos() as f64 / n as f64 / 1000.0;
    println!("D  load_page() (full uffd path) {td:.1} us/call");
}
