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
pub mod fw;
pub mod timing;

pub use timing::{IndexMode, TimingDb};
pub use fw::FwDb;
pub use chunk::{DEFAULT_DELTA_THRESHOLD, MAX_SNAPSHOT_ID, PAGE_SIZE, encode_key};
