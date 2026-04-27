// Enable per-phase timing output by building with `--features timing`
// (e.g. `cargo build --release --features timing`).
// All [TIMING] eprintln instrumentation is compiled out by default.
#[macro_export]
macro_rules! timeit {
    ($($tt:tt)*) => {
        #[cfg(feature = "timing")]
        { $($tt)* }
    };
}

pub mod c_api;
pub mod cache;
pub mod chunk;
pub mod convert;
pub mod format;
pub mod purge;
pub mod append_only;
pub mod btree;

pub use btree::{IndexMode, BtreeDb, shm_cache_path};
pub use append_only::AppendOnlyDb;
pub use chunk::{DEFAULT_DELTA_THRESHOLD, MAX_SNAPSHOT_ID, PAGE_SIZE, encode_key};
