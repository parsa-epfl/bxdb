/// Compares compression algorithms on full chunks from an existing bxdb database.
///
/// Usage: compress-compare <path-to-bxdb-dir> [max_train_pages] [max_test_pages]
///
/// 1. Scans chunks.log for all Full-chunk records.
/// 2. Decompresses snapshot-0 pages (up to max_train_pages) and trains a zstd
///    dictionary on them.
/// 3. For every Full chunk NOT in snapshot 0 (sampled up to max_test_pages),
///    decompresses the original page and re-compresses it with four
///    configurations, measuring wall time and compressed size:
///    a) lz4
///    b) zstd level 1 + WindowLog(14)
///    c) zstd level 3 + WindowLog(14)
///    d) zstd level 3 + WindowLog(14) + snapshot-0-trained dictionary
/// 4. Prints a summary table.

use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::time::Instant;

use bxdb::chunk::{self, ChunkKind, ChunkRecord, PAGE_SIZE};
use bxdb::format;

const DEFAULT_MAX_TRAIN_PAGES: usize = 100_000;
const DEFAULT_MAX_TEST_PAGES: usize = 100_000;
const DICT_MAX_SIZE: usize = 110_000;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: compress-compare <path-to-bxdb-dir> [max_train_pages] [max_test_pages]");
        std::process::exit(1);
    }
    let dir = Path::new(&args[1]);
    let max_train = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(DEFAULT_MAX_TRAIN_PAGES);
    let max_test  = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(DEFAULT_MAX_TEST_PAGES);

    if !dir.join("chunks.log").exists() {
        eprintln!("chunks.log not found in {}. Is this a bxdb directory?", dir.display());
        std::process::exit(1);
    }

    eprintln!("Scanning chunks.log in {} ...", dir.display());
    let (snap0, test, blob_dir) = scan_log(dir, max_train);

    eprintln!("  snapshot-0 full chunks (training): {}", snap0.len());
    eprintln!("  other-snapshot full chunks (test) : {}", test.len());

    if snap0.is_empty() && test.is_empty() {
        eprintln!("No chunks found.");
        std::process::exit(1);
    }

    // ── Phase 2: train dictionary ──────────────────────────────────────────
    let dictionary = if snap0.is_empty() {
        eprintln!("  WARNING: no snapshot-0 pages → dictionary will be empty.");
        Vec::new()
    } else {
        let pages = decompress_pages(&snap0, &blob_dir);
        eprintln!("  decompressed {} training pages, training dictionary (max {} KB)...", pages.len(), DICT_MAX_SIZE / 1024);
        train_dictionary(&pages)
    };

    // ── Phase 3: sample test set ───────────────────────────────────────────
    let test_sample = if test.len() > max_test {
        eprintln!("  sampling {} / {} test pages (uniform stride)", max_test, test.len());
        let step = test.len() / max_test;
        (0..max_test).map(|i| test[i * step].clone()).collect()
    } else {
        test.clone()
    };
    eprintln!("Benchmarking {} test pages...", test_sample.len());

    // ── Phase 4: benchmark ─────────────────────────────────────────────────
    let results = benchmark(&test_sample, &blob_dir, &dictionary);

    // ── Phase 5: report ────────────────────────────────────────────────────
    print_report(&results, test_sample.len());
}

// ─── Data structures ───────────────────────────────────────────────────────────

#[derive(Clone)]
struct ChunkInfo {
    worker_id: u8,
    offset: u64,
    len: u32,
}

struct AlgoResult {
    total_original: u64,
    total_compressed: u64,
    total_time_ns: u64,
}

// ─── Phase 1: scan chunks.log ──────────────────────────────────────────────────

fn scan_log(dir: &Path, max_train: usize) -> (Vec<ChunkInfo>, Vec<ChunkInfo>, PathBuf) {
    let log_path = dir.join("chunks.log");
    let blob_dir = dir.join("blobs");
    let mut f = BufReader::new(File::open(&log_path).expect("open chunks.log"));

    let _max_snap = format::read_and_verify_header(&mut f, &chunk::MAGIC_LOG)
        .expect("read chunks.log header");

    let mut snap0  = Vec::new();
    let mut others = Vec::new();
    let mut snap0_count = 0u64;

    while let Ok(Some(rec)) = ChunkRecord::read_from(&mut f) {
        if rec.kind != ChunkKind::Full {
            continue;
        }
        let info = ChunkInfo {
            worker_id: rec.worker_id,
            offset:    rec.offset,
            len:       rec.len,
        };
        if chunk::snapshot_of(rec.key) == 0 {
            snap0_count += 1;
            if snap0.len() < max_train {
                snap0.push(info);
            }
        } else {
            others.push(info);
        }
    }

    eprintln!("  raw scan: {} total full, {} snap-0 full", snap0_count + others.len() as u64, snap0_count);
    (snap0, others, blob_dir)
}

// ─── Blob cache ────────────────────────────────────────────────────────────────

struct BlobCache {
    dir: PathBuf,
    files: HashMap<u8, File>,
}

impl BlobCache {
    fn new(dir: PathBuf) -> Self {
        Self { dir, files: HashMap::new() }
    }

    fn read(&mut self, worker_id: u8, offset: u64, len: u32) -> Vec<u8> {
        if !self.files.contains_key(&worker_id) {
            let path = self.dir.join(format!("worker_{}.blob", worker_id));
            self.files.insert(worker_id, File::open(&path).expect("open blob file"));
        }
        let f = &self.files[&worker_id];
        let mut buf = vec![0u8; len as usize];
        f.read_exact_at(&mut buf, offset).expect("read blob");
        buf
    }
}

// ─── Decompress helpers ────────────────────────────────────────────────────────

fn decompress_pages(chunks: &[ChunkInfo], blob_dir: &Path) -> Vec<Vec<u8>> {
    let mut cache = BlobCache::new(blob_dir.to_path_buf());
    let mut pages = Vec::with_capacity(chunks.len());
    for ci in chunks {
        let blob = cache.read(ci.worker_id, ci.offset, ci.len);
        let page = zstd::decode_all(&blob[..]).expect("zstd decompress");
        assert_eq!(page.len(), PAGE_SIZE);
        pages.push(page);
    }
    pages
}

fn decompress_one(ci: &ChunkInfo, cache: &mut BlobCache) -> Vec<u8> {
    let blob = cache.read(ci.worker_id, ci.offset, ci.len);
    let page = zstd::decode_all(&blob[..]).expect("zstd decompress");
    assert_eq!(page.len(), PAGE_SIZE);
    page
}

// ─── Dictionary training ──────────────────────────────────────────────────────

fn train_dictionary(pages: &[Vec<u8>]) -> Vec<u8> {
    let total_bytes: usize = pages.iter().map(|p| p.len()).sum();
    let mut data = Vec::with_capacity(total_bytes);
    for p in pages {
        data.extend_from_slice(p);
    }
    let sizes: Vec<usize> = pages.iter().map(|p| p.len()).collect();
    zstd::dict::from_continuous(&data, &sizes, DICT_MAX_SIZE).expect("train dictionary")
}

// ─── Compressor factory functions ─────────────────────────────────────────────

fn make_lz4() -> Box<dyn FnMut(&[u8]) -> Vec<u8>> {
    Box::new(|page: &[u8]| lz4_flex::compress_prepend_size(page))
}

fn make_zstd1_wl14() -> Box<dyn FnMut(&[u8]) -> Vec<u8>> {
    let mut c = zstd::bulk::Compressor::new(1).expect("zstd init");
    c.set_parameter(zstd::zstd_safe::CParameter::WindowLog(14)).expect("set windowLog");
    Box::new(move |page: &[u8]| c.compress(page).expect("zstd compress"))
}

fn make_zstd3_wl14() -> Box<dyn FnMut(&[u8]) -> Vec<u8>> {
    let mut c = zstd::bulk::Compressor::new(3).expect("zstd init");
    c.set_parameter(zstd::zstd_safe::CParameter::WindowLog(14)).expect("set windowLog");
    Box::new(move |page: &[u8]| c.compress(page).expect("zstd compress"))
}

fn make_zstd3_wl14_dict(dict: &[u8]) -> Box<dyn FnMut(&[u8]) -> Vec<u8>> {
    let mut c = if dict.is_empty() {
        zstd::bulk::Compressor::new(3).expect("zstd init")
    } else {
        zstd::bulk::Compressor::with_dictionary(3, dict).expect("zstd with dict")
    };
    c.set_parameter(zstd::zstd_safe::CParameter::WindowLog(14)).expect("set windowLog");
    Box::new(move |page: &[u8]| c.compress(page).expect("zstd compress"))
}

fn make_zstd1_wl12_minimal() -> Box<dyn FnMut(&[u8]) -> Vec<u8>> {
    let mut c = zstd::bulk::Compressor::new(1).expect("zstd init");
    c.set_parameter(zstd::zstd_safe::CParameter::WindowLog(12)).expect("set windowLog");
    c.set_parameter(zstd::zstd_safe::CParameter::HashLog(12)).expect("set hashLog");
    c.set_parameter(zstd::zstd_safe::CParameter::ChainLog(10)).expect("set chainLog");
    c.set_parameter(zstd::zstd_safe::CParameter::SearchLog(0)).expect("set searchLog");
    Box::new(move |page: &[u8]| c.compress(page).expect("zstd compress"))
}

fn benchmark(chunks: &[ChunkInfo], blob_dir: &Path, dictionary: &[u8]) -> Vec<AlgoResult> {
    let mut cache = BlobCache::new(blob_dir.to_path_buf());
    let dict_vec = dictionary.to_vec();

    // Create compressors ONCE, outside the per-page loop.
    let mut lz4_c        = make_lz4();
    let mut zstd1_c      = make_zstd1_wl14();
    let mut zstd3_c      = make_zstd3_wl14();
    let mut zstd3dict_c  = make_zstd3_wl14_dict(&dict_vec);
    let mut zstd1min_c   = make_zstd1_wl12_minimal();

    let mut results = vec![
        AlgoResult { total_original: 0, total_compressed: 0, total_time_ns: 0 },
        AlgoResult { total_original: 0, total_compressed: 0, total_time_ns: 0 },
        AlgoResult { total_original: 0, total_compressed: 0, total_time_ns: 0 },
        AlgoResult { total_original: 0, total_compressed: 0, total_time_ns: 0 },
        AlgoResult { total_original: 0, total_compressed: 0, total_time_ns: 0 },
    ];

    let report_every = (chunks.len() / 10).max(1);
    for (i, ci) in chunks.iter().enumerate() {
        if i % report_every == 0 {
            eprintln!("  progress: {}/{} ({}%)", i, chunks.len(), (i * 100) / chunks.len());
        }
        let page = decompress_one(ci, &mut cache);
        let orig_len = page.len() as u64;

        {
            let t0 = Instant::now();
            let c = lz4_c(&page);
            results[0].total_original   += orig_len;
            results[0].total_compressed += c.len() as u64;
            results[0].total_time_ns    += t0.elapsed().as_nanos() as u64;
        }
        {
            let t0 = Instant::now();
            let c = zstd1_c(&page);
            results[1].total_original   += orig_len;
            results[1].total_compressed += c.len() as u64;
            results[1].total_time_ns    += t0.elapsed().as_nanos() as u64;
        }
        {
            let t0 = Instant::now();
            let c = zstd3_c(&page);
            results[2].total_original   += orig_len;
            results[2].total_compressed += c.len() as u64;
            results[2].total_time_ns    += t0.elapsed().as_nanos() as u64;
        }
        {
            let t0 = Instant::now();
            let c = zstd3dict_c(&page);
            results[3].total_original   += orig_len;
            results[3].total_compressed += c.len() as u64;
            results[3].total_time_ns    += t0.elapsed().as_nanos() as u64;
        }
        {
            let t0 = Instant::now();
            let c = zstd1min_c(&page);
            results[4].total_original   += orig_len;
            results[4].total_compressed += c.len() as u64;
            results[4].total_time_ns    += t0.elapsed().as_nanos() as u64;
        }
    }
    results
}

// ─── Report ────────────────────────────────────────────────────────────────────

fn print_report(results: &[AlgoResult], num_pages: usize) {
    let labels = ["lz4", "zstd-1+wl14", "zstd-3+wl14", "zstd-3+wl14+dict", "zstd-1+wl12-min"];

    println!();
    println!("Benchmarked {} pages", num_pages);
    println!();
    println!(" {:<24} {:>11} {:>11} {:>11} {:>13}", "Algorithm", "Orig(MB)", "Comp(MB)", "Ratio", "MB/s");
    println!("{}", "-".repeat(74));

    for (i, r) in results.iter().enumerate() {
        let orig_mb = r.total_original as f64 / (1u64 << 20) as f64;
        let comp_mb = r.total_compressed as f64 / (1u64 << 20) as f64;
        let ratio = if r.total_compressed > 0 { r.total_original as f64 / r.total_compressed as f64 } else { 0.0 };
        let total_s = r.total_time_ns as f64 / 1e9;
        let throughput = if total_s > 0.0 { orig_mb / total_s } else { 0.0 };

        println!(
            " {labels:<24} {orig_mb:>10.1}  {comp_mb:>10.1}  {ratio:>10.2}x  {throughput:>10.0}",
            labels = labels[i], orig_mb = orig_mb, comp_mb = comp_mb, ratio = ratio, throughput = throughput,
        );
    }

    println!();
    println!("Ratio = original / compressed (higher is better)");
    println!("MB/s  = original size / wall time (single-threaded sequential)");
}
