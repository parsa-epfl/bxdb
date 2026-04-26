/// Microbenchmark: compression algorithm × block size sweep using real page data.
///
/// Usage: compress-bench [path-to-raw-file]
/// Defaults to ./stress-test-data.raw
///
/// Reads 1 GiB from the raw file, then for each (algorithm, block_size) pair:
///   - compresses all blocks, records throughput + ratio
///   - decompresses all blocks, records throughput
///
/// Block sizes tested: 4 KB (current), 16 KB, 64 KB, 256 KB, 1 MB
/// Algorithms: none, zstd-1, zstd-3, zstd-7, lz4, snap
use std::{
    fs::File,
    io::Write,
    os::unix::io::AsRawFd,
    time::Instant,
};

const BENCH_BYTES: usize = 1 << 30; // 1 GiB
const BLOCK_SIZES: &[usize] = &[4096, 16384, 65536, 262144, 1048576];
const RAW_FILE_DEFAULT: &str = "./stress-test-data.raw";

fn mmap_file(path: &str) -> &'static [u8] {
    let file = File::open(path).unwrap_or_else(|_| panic!("cannot open {path}"));
    let meta = file.metadata().unwrap();
    let len = (meta.len() as usize).min(BENCH_BYTES);
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ,
            libc::MAP_SHARED,
            file.as_raw_fd(),
            0,
        )
    };
    assert_ne!(ptr, libc::MAP_FAILED, "mmap failed");
    unsafe { std::slice::from_raw_parts(ptr as *const u8, len) }
}

enum Algo {
    None,
    Zstd(i32),
    Lz4,
    Snap,
}

impl Algo {
    fn name(&self) -> &'static str {
        match self {
            Algo::None => "none",
            Algo::Zstd(1) => "zstd-1",
            Algo::Zstd(3) => "zstd-3",
            Algo::Zstd(7) => "zstd-7",
            Algo::Lz4 => "lz4",
            Algo::Snap => "snap",
            Algo::Zstd(l) => unreachable!("level {l}"),
        }
    }

    fn compress(&self, src: &[u8]) -> Vec<u8> {
        match self {
            Algo::None => src.to_vec(),
            Algo::Zstd(level) => zstd::encode_all(src, *level).unwrap(),
            Algo::Lz4 => lz4_flex::compress_prepend_size(src),
            Algo::Snap => {
                let mut enc = snap::raw::Encoder::new();
                enc.compress_vec(src).unwrap()
            }
        }
    }

    fn decompress(&self, src: &[u8], original_len: usize) -> Vec<u8> {
        match self {
            Algo::None => src.to_vec(),
            Algo::Zstd(_) => zstd::decode_all(src).unwrap(),
            Algo::Lz4 => lz4_flex::decompress_size_prepended(src).unwrap(),
            Algo::Snap => {
                let mut dec = snap::raw::Decoder::new();
                let mut out = vec![0u8; original_len];
                dec.decompress(src, &mut out).unwrap();
                out
            }
        }
    }
}

fn bench_one(data: &[u8], algo: &Algo, block_size: usize) {
    let blocks: Vec<&[u8]> = data.chunks(block_size).collect();
    let n = blocks.len();

    // --- compress ---
    let t0 = Instant::now();
    let compressed: Vec<Vec<u8>> = blocks.iter().map(|b| algo.compress(b)).collect();
    let compress_secs = t0.elapsed().as_secs_f64();

    let total_in: usize = blocks.iter().map(|b| b.len()).sum();
    let total_out: usize = compressed.iter().map(|b| b.len()).sum();
    let compress_mb_s = (total_in as f64 / (1 << 20) as f64) / compress_secs;
    let ratio = total_in as f64 / total_out as f64;

    // --- decompress ---
    let t1 = Instant::now();
    let mut sink: usize = 0;
    for (i, c) in compressed.iter().enumerate() {
        let d = algo.decompress(c, blocks[i].len());
        sink += d.len(); // prevent optimisation
    }
    let decompress_secs = t1.elapsed().as_secs_f64();
    let decompress_mb_s = (sink as f64 / (1 << 20) as f64) / decompress_secs;

    println!(
        "{:<8} {:>7} KB  {:>6} blocks  compress {:>8.1} MB/s  decompress {:>8.1} MB/s  ratio {:.2}x",
        algo.name(),
        block_size / 1024,
        n,
        compress_mb_s,
        decompress_mb_s,
        ratio,
    );
}

fn main() {
    let path = std::env::args().nth(1).unwrap_or_else(|| RAW_FILE_DEFAULT.to_string());
    eprintln!("Mapping {BENCH_BYTES} bytes from {path}...");
    let data = mmap_file(&path);
    eprintln!("Mapped {} MiB. Starting benchmark...\n", data.len() >> 20);

    let algos: &[Algo] = &[
        Algo::None,
        Algo::Zstd(1),
        Algo::Zstd(3),
        Algo::Zstd(7),
        Algo::Lz4,
        Algo::Snap,
    ];

    println!(
        "{:<8} {:>10}  {:>13}  {:>25}  {:>27}  {:>8}",
        "algo", "block", "blocks", "compress", "decompress", "ratio"
    );
    println!("{}", "-".repeat(100));

    for block_size in BLOCK_SIZES {
        for algo in algos {
            bench_one(data, algo, *block_size);
        }
        println!();
    }

    // Suppress unused import warning
    let _ = std::io::stdout().flush();
}
