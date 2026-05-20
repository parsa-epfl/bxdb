//! Temporary per-component timing counters for `BtreeDb::load_page`.
//!
//! Sequential access is guaranteed (single uffd thread).

use std::cell::UnsafeCell;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};

// ── Counter ───────────────────────────────────────────────────────────────────

/// Zero-overhead sequential counter backed by `UnsafeCell<u64>`.
/// `Sync` is manually implemented because the caller guarantees
/// single-threaded access (UFFD handler).
pub(crate) struct Counter(UnsafeCell<u64>);

// SAFETY: the UFFD handler thread is the sole writer/reader.
unsafe impl Sync for Counter {}

impl Counter {
    pub(crate) const fn new() -> Self { Self(UnsafeCell::new(0)) }
    pub(crate) fn add(&self, ns: u64) { unsafe { *self.0.get() += ns; } }
    pub(crate) fn inc(&self) { unsafe { *self.0.get() += 1; } }
    pub(crate) fn get(&self) -> u64 { unsafe { *self.0.get() } }
}

// ── Timing buckets ────────────────────────────────────────────────────────────

pub(crate) struct LoadTiming {
    // ── Top-level ────────────────────────────────────────────────────────
    pub total_ns:   Counter,
    pub total_calls: Counter,

    // ── Index lookup ─────────────────────────────────────────────────────
    pub floor_ns:   Counter,

    // ── Chunk-kind counters ──────────────────────────────────────────────
    pub full_calls:  Counter,
    pub zero_calls:  Counter,
    pub delta_calls: Counter,

    // ── Full-chunk path ──────────────────────────────────────────────────
    pub full_decompress_ns: Counter,   // blob read + zstd in resolve_cached
    pub full_cache_get_ns:  Counter,
    pub full_cache_hits:    Counter,
    pub full_cache_put_ns:  Counter,

    // ── Delta-chunk path ─────────────────────────────────────────────────
    pub delta_base_cache_get_ns: Counter,
    pub delta_base_lookup_ns:    Counter,   // exact_lookup for base key
    pub delta_base_decompress_ns: Counter,  // decompress base page
    pub delta_base_cache_put_ns: Counter,
    pub delta_blob_read_ns:      Counter,   // blob read of delta chunk
    pub delta_patch_ns:          Counter,   // apply_delta_patch

    // ── Aggregated I/O ───────────────────────────────────────────────────
    pub blob_read_ns:       Counter,   // sum over Full + Delta
    pub blob_read_calls:    Counter,
    pub zstd_decompress_ns: Counter,
    pub zstd_decompress_calls: Counter,
}

impl LoadTiming {
    const fn new() -> Self {
        Self {
            total_ns:            Counter::new(),
            total_calls:         Counter::new(),
            floor_ns:            Counter::new(),
            full_calls:          Counter::new(),
            zero_calls:          Counter::new(),
            delta_calls:         Counter::new(),
            full_decompress_ns:  Counter::new(),
            full_cache_get_ns:   Counter::new(),
            full_cache_hits:     Counter::new(),
            full_cache_put_ns:   Counter::new(),
            delta_base_cache_get_ns: Counter::new(),
            delta_base_lookup_ns:    Counter::new(),
            delta_base_decompress_ns:Counter::new(),
            delta_base_cache_put_ns: Counter::new(),
            delta_blob_read_ns:      Counter::new(),
            delta_patch_ns:          Counter::new(),
            blob_read_ns:        Counter::new(),
            blob_read_calls:     Counter::new(),
            zstd_decompress_ns:  Counter::new(),
            zstd_decompress_calls:Counter::new(),
        }
    }
}

pub(crate) static LOAD_TIMING: LoadTiming = LoadTiming::new();

// ── Exit reporter ─────────────────────────────────────────────────────────────

fn log_path() -> PathBuf {
    std::env::var("BXDB_TIMING_LOG")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            let mut p = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("/tmp"));
            p.push("bxdb_ckpt_time.json");
            p
        })
}

static REGISTERED: AtomicBool = AtomicBool::new(false);

extern "C" fn on_exit_report() {
    let path = log_path();
    let pid = unsafe { libc::getpid() };

    // ── snapshot all counters ────────────────────────────────────────────
    let total_calls = LOAD_TIMING.total_calls.get();
    let total_ns    = LOAD_TIMING.total_ns.get();

    let floor_ns    = LOAD_TIMING.floor_ns.get();

    let full_calls  = LOAD_TIMING.full_calls.get();
    let zero_calls  = LOAD_TIMING.zero_calls.get();
    let delta_calls = LOAD_TIMING.delta_calls.get();

    let fc_get_ns     = LOAD_TIMING.full_cache_get_ns.get();
    let fc_hits       = LOAD_TIMING.full_cache_hits.get();
    let fc_decomp_ns  = LOAD_TIMING.full_decompress_ns.get();
    let fc_decomp_calls= full_calls.saturating_sub(fc_hits);
    let fc_put_ns     = LOAD_TIMING.full_cache_put_ns.get();

    let dc_base_get_ns   = LOAD_TIMING.delta_base_cache_get_ns.get();
    let dc_base_lookup_ns= LOAD_TIMING.delta_base_lookup_ns.get();
    let dc_base_decomp_ns= LOAD_TIMING.delta_base_decompress_ns.get();
    let dc_base_put_ns   = LOAD_TIMING.delta_base_cache_put_ns.get();
    let dc_blob_ns       = LOAD_TIMING.delta_blob_read_ns.get();
    let dc_patch_ns      = LOAD_TIMING.delta_patch_ns.get();

    let blob_read_ns     = LOAD_TIMING.blob_read_ns.get();
    let blob_read_calls  = LOAD_TIMING.blob_read_calls.get();
    let zstd_decomp_ns   = LOAD_TIMING.zstd_decompress_ns.get();
    let zstd_decomp_calls= LOAD_TIMING.zstd_decompress_calls.get();

    // ── build JSON ───────────────────────────────────────────────────────
    use std::fmt::Write;

    let mut j = String::with_capacity(2048);
    let _ = write!(j, "{{\"pid\":{pid},\"total_calls\":{total_calls},\"total_ns\":{total_ns},");
    let _ = write!(j, "\"phases\":{{");

    // floor
    let _ = write!(j, "\"floor\":{{\"ns\":{floor_ns},\"calls\":{total_calls}}},");

    // full
    let _ = write!(j, "\"full\":{{\"calls\":{full_calls},");
    let _ = write!(j, "\"cache_get_ns\":{fc_get_ns},\"cache_get_calls\":{total_calls},");
    let _ = write!(j, "\"cache_hits\":{fc_hits},");
    let _ = write!(j, "\"decompress_ns\":{fc_decomp_ns},\"decompress_calls\":{fc_decomp_calls},");
    let _ = write!(j, "\"cache_put_ns\":{fc_put_ns},\"cache_put_calls\":{fc_decomp_calls}}},");

    // delta
    let _ = write!(j, "\"delta\":{{\"calls\":{delta_calls},");
    let _ = write!(j, "\"base_cache_get_ns\":{dc_base_get_ns},\"base_cache_get_calls\":{delta_calls},");
    let _ = write!(j, "\"base_lookup_ns\":{dc_base_lookup_ns},\"base_lookup_calls\":{delta_calls},");
    let _ = write!(j, "\"base_decompress_ns\":{dc_base_decomp_ns},\"base_decompress_calls\":{delta_calls},");
    let _ = write!(j, "\"base_cache_put_ns\":{dc_base_put_ns},\"base_cache_put_calls\":{delta_calls},");
    let _ = write!(j, "\"blob_read_ns\":{dc_blob_ns},\"blob_read_calls\":{delta_calls},");
    let _ = write!(j, "\"patch_ns\":{dc_patch_ns},\"patch_calls\":{delta_calls}}},");

    // zero
    let _ = write!(j, "\"zero\":{{\"calls\":{zero_calls}}},");

    // io (aggregated)
    let _ = write!(j, "\"io\":{{\"blob_read_ns\":{blob_read_ns},\"blob_read_calls\":{blob_read_calls},");
    let _ = write!(j, "\"zstd_decompress_ns\":{zstd_decomp_ns},\"zstd_decompress_calls\":{zstd_decomp_calls}}}");

    let _ = write!(j, "}}"); // close phases
    let _ = write!(j, "}}"); // close root

    match std::fs::write(&path, j) {
        Ok(()) => {}
        Err(e) => {
            let msg = format!("bxdb timing: cannot write {}: {e}\n", path.display());
            let _ = unsafe { libc::write(2, msg.as_ptr() as *const libc::c_void, msg.len()) };
        }
    }
}

/// Register the atexit handler once.
pub(crate) fn init_timing() {
    if !REGISTERED.swap(true, Ordering::SeqCst) {
        unsafe { libc::atexit(on_exit_report as extern "C" fn()); }
    }
}
