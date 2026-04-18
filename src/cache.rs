use std::fs::{File, OpenOptions};
use std::io;
use std::mem;
use std::os::fd::AsRawFd;
use std::path::Path;
use std::ptr;

pub const CACHE_PAGE_SIZE: usize = 4096;
pub const METADATA_PAGES: usize = 512;
pub const SETS_PER_META: usize = 16;
pub const WAYS_PER_SET: usize = 16;
pub const TOTAL_SLOTS: usize = METADATA_PAGES * SETS_PER_META * WAYS_PER_SET;
pub const METADATA_BYTES: usize = METADATA_PAGES * CACHE_PAGE_SIZE;
pub const DATA_BYTES: usize = TOTAL_SLOTS * CACHE_PAGE_SIZE;
pub const TOTAL_BYTES: usize = METADATA_BYTES + DATA_BYTES;

const EMPTY_KEY: u64 = u64::MAX;
const FIB_MUL: u64 = 0x9e3779b97f4a7c15;

const MUTEX_OFFSET: usize = 0;
const SETS_OFFSET: usize = 64;
const SET_STRIDE: usize = 192;

#[repr(C)]
struct SetMetadata {
    page_ids: [u64; WAYS_PER_SET],
    timestamps: [u8; WAYS_PER_SET],
    _pad: [u8; 48],
}
const _: () = assert!(mem::size_of::<SetMetadata>() == SET_STRIDE);
const _: () = assert!(mem::size_of::<libc::pthread_mutex_t>() <= 64);

pub struct SharedCache {
    base: *mut u8,
    len: usize,
    // Holding the File keeps the flock (LOCK_SH) alive for this handle's lifetime.
    // Dropping releases the shared lock, allowing a future initializer to acquire
    // LOCK_EX once all attached handles are gone.
    _file: File,
}

unsafe impl Send for SharedCache {}
unsafe impl Sync for SharedCache {}

impl Drop for SharedCache {
    fn drop(&mut self) {
        if !self.base.is_null() {
            unsafe {
                libc::munmap(self.base as *mut libc::c_void, self.len);
            }
        }
    }
}

impl SharedCache {
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;
        let fd = file.as_raw_fd();

        // First-initializer vs. attacher coordination:
        //   - Try LOCK_EX non-blocking. If we win, we are the initializer.
        //   - If EX fails, another process holds EX (initializing) or SH
        //     (already attached). Either way, LOCK_SH (blocking) resolves both
        //     cases: it waits until the initializer downgrades.
        //   - The initializer downgrades to LOCK_SH at the end of open() so
        //     subsequent attachers proceed while we are still using the cache.
        let got_ex = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) } == 0;

        if got_ex {
            let size = file.metadata()?.len() as usize;
            if size < TOTAL_BYTES {
                if unsafe { libc::ftruncate(fd, TOTAL_BYTES as libc::off_t) } != 0 {
                    let e = io::Error::last_os_error();
                    unsafe { libc::flock(fd, libc::LOCK_UN) };
                    return Err(e);
                }
            }
            let base = unsafe { mmap_rw(fd)? };
            if size < TOTAL_BYTES {
                if let Err(e) = unsafe { init_metadata(base) } {
                    unsafe {
                        libc::munmap(base as *mut libc::c_void, TOTAL_BYTES);
                        libc::flock(fd, libc::LOCK_UN);
                    }
                    return Err(e);
                }
            }
            // Downgrade to shared; attachers waiting on LOCK_SH wake up.
            if unsafe { libc::flock(fd, libc::LOCK_SH) } != 0 {
                let e = io::Error::last_os_error();
                unsafe { libc::munmap(base as *mut libc::c_void, TOTAL_BYTES) };
                return Err(e);
            }
            Ok(Self {
                base,
                len: TOTAL_BYTES,
                _file: file,
            })
        } else {
            // Wait for SH — blocks until the current EX holder downgrades or exits.
            if unsafe { libc::flock(fd, libc::LOCK_SH) } != 0 {
                return Err(io::Error::last_os_error());
            }
            let size = file.metadata()?.len() as usize;
            if size < TOTAL_BYTES {
                // Prior initializer died before finishing. Surface the error;
                // the caller can retry the open.
                unsafe { libc::flock(fd, libc::LOCK_UN) };
                return Err(io::Error::other(
                    "shared cache file present but not fully initialized",
                ));
            }
            let base = unsafe { mmap_rw(fd)? };
            Ok(Self {
                base,
                len: TOTAL_BYTES,
                _file: file,
            })
        }
    }

    pub fn get(&self, key: u64, out: &mut [u8; CACHE_PAGE_SIZE]) -> bool {
        if key == EMPTY_KEY {
            return false;
        }
        let (meta_idx, set_idx) = locate(key);
        let page_base = unsafe { self.base.add(meta_idx * CACHE_PAGE_SIZE) };
        let mutex = page_base.wrapping_add(MUTEX_OFFSET) as *mut libc::pthread_mutex_t;
        let set = unsafe { set_ptr(page_base, set_idx) };

        lock_mutex(mutex);
        let hit;
        unsafe {
            hit = find_way(&(*set).page_ids, key);
            if let Some(w) = hit {
                let slot = meta_idx * (SETS_PER_META * WAYS_PER_SET) + set_idx * WAYS_PER_SET + w;
                let data = self.base.add(METADATA_BYTES + slot * CACHE_PAGE_SIZE);
                ptr::copy_nonoverlapping(data, out.as_mut_ptr(), CACHE_PAGE_SIZE);
                update_lru(&mut *set, w);
            }
        }
        unlock_mutex(mutex);
        hit.is_some()
    }

    pub fn put(&self, key: u64, page: &[u8; CACHE_PAGE_SIZE]) {
        if key == EMPTY_KEY {
            return;
        }
        let (meta_idx, set_idx) = locate(key);
        let page_base = unsafe { self.base.add(meta_idx * CACHE_PAGE_SIZE) };
        let mutex = page_base.wrapping_add(MUTEX_OFFSET) as *mut libc::pthread_mutex_t;
        let set = unsafe { set_ptr(page_base, set_idx) };

        lock_mutex(mutex);
        unsafe {
            let target =
                find_way(&(*set).page_ids, key).or_else(|| find_way(&(*set).page_ids, EMPTY_KEY));
            let way = target.unwrap_or_else(|| {
                let mut min_ts = u8::MAX;
                let mut min_w = 0;
                for w in 0..WAYS_PER_SET {
                    if (*set).timestamps[w] < min_ts {
                        min_ts = (*set).timestamps[w];
                        min_w = w;
                    }
                }
                min_w
            });
            let slot = meta_idx * (SETS_PER_META * WAYS_PER_SET) + set_idx * WAYS_PER_SET + way;
            let data = self.base.add(METADATA_BYTES + slot * CACHE_PAGE_SIZE);
            ptr::copy_nonoverlapping(page.as_ptr(), data, CACHE_PAGE_SIZE);
            (*set).page_ids[way] = key;
            update_lru(&mut *set, way);
        }
        unlock_mutex(mutex);
    }
}

fn locate(key: u64) -> (usize, usize) {
    let hash = key.wrapping_mul(FIB_MUL);
    let meta_idx = ((hash >> 4) & (METADATA_PAGES as u64 - 1)) as usize;
    let set_idx = (hash & (SETS_PER_META as u64 - 1)) as usize;
    (meta_idx, set_idx)
}

unsafe fn set_ptr(page_base: *mut u8, set_idx: usize) -> *mut SetMetadata {
    unsafe { page_base.add(SETS_OFFSET + set_idx * SET_STRIDE) as *mut SetMetadata }
}

// Find the way index whose page_id matches `key`, scanning all 16 ways in
// parallel where SIMD is available. Returns the lowest matching way.
#[inline]
fn find_way(page_ids: &[u64; WAYS_PER_SET], key: u64) -> Option<usize> {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("sse4.1") {
            return unsafe { find_way_sse41(page_ids, key) };
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        // NEON is mandatory in AArch64.
        return unsafe { find_way_neon(page_ids, key) };
    }
    #[allow(unreachable_code)]
    page_ids.iter().position(|&k| k == key)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.1")]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn find_way_sse41(page_ids: &[u64; WAYS_PER_SET], key: u64) -> Option<usize> {
    use core::arch::x86_64::*;
    let k = _mm_set1_epi64x(key as i64);
    let ptr = page_ids.as_ptr() as *const __m128i;
    let mut mask: u32 = 0;
    for i in 0..8 {
        let v = _mm_loadu_si128(ptr.add(i));
        let cmp = _mm_cmpeq_epi64(v, k);
        // 2 bits per 128-bit reg (one per u64 lane).
        let m = _mm_movemask_pd(_mm_castsi128_pd(cmp)) as u32;
        mask |= m << (i * 2);
    }
    if mask == 0 {
        None
    } else {
        Some(mask.trailing_zeros() as usize)
    }
}

#[cfg(target_arch = "aarch64")]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn find_way_neon(page_ids: &[u64; WAYS_PER_SET], key: u64) -> Option<usize> {
    use core::arch::aarch64::*;
    let k = vdupq_n_u64(key);
    let ptr = page_ids.as_ptr();
    let mut mask: u32 = 0;
    for i in 0..8 {
        let v = vld1q_u64(ptr.add(i * 2));
        let cmp = vceqq_u64(v, k);
        // Each lane is all-1 or all-0; reduce to the low bit per lane.
        let bits = vshrq_n_u64::<63>(cmp);
        let lo = vgetq_lane_u64::<0>(bits) as u32;
        let hi = vgetq_lane_u64::<1>(bits) as u32;
        mask |= (lo << (i * 2)) | (hi << (i * 2 + 1));
    }
    if mask == 0 {
        None
    } else {
        Some(mask.trailing_zeros() as usize)
    }
}

// Saturating-decrement all 16 timestamps, then stamp the accessed slot with 0xFF.
// Decrementing a just-accessed slot is harmless because the 0xFF write follows.
#[cfg(target_arch = "x86_64")]
fn update_lru(set: &mut SetMetadata, accessed: usize) {
    use core::arch::x86_64::{
        __m128i, _mm_loadu_si128, _mm_set1_epi8, _mm_storeu_si128, _mm_subs_epu8,
    };
    // SSE2 is baseline for x86_64; no runtime feature check needed.
    unsafe {
        let ptr = set.timestamps.as_mut_ptr() as *mut __m128i;
        let v = _mm_loadu_si128(ptr);
        let ones = _mm_set1_epi8(1);
        let v2 = _mm_subs_epu8(v, ones);
        _mm_storeu_si128(ptr, v2);
    }
    set.timestamps[accessed] = u8::MAX;
}

#[cfg(target_arch = "aarch64")]
fn update_lru(set: &mut SetMetadata, accessed: usize) {
    use core::arch::aarch64::{vdupq_n_u8, vld1q_u8, vqsubq_u8, vst1q_u8};
    unsafe {
        let ptr = set.timestamps.as_mut_ptr();
        let v = vld1q_u8(ptr);
        let ones = vdupq_n_u8(1);
        let v2 = vqsubq_u8(v, ones);
        vst1q_u8(ptr, v2);
    }
    set.timestamps[accessed] = u8::MAX;
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
fn update_lru(set: &mut SetMetadata, accessed: usize) {
    for w in 0..WAYS_PER_SET {
        set.timestamps[w] = set.timestamps[w].saturating_sub(1);
    }
    set.timestamps[accessed] = u8::MAX;
}

fn lock_mutex(m: *mut libc::pthread_mutex_t) {
    let rc = unsafe { libc::pthread_mutex_lock(m) };
    if rc == libc::EOWNERDEAD {
        unsafe { libc::pthread_mutex_consistent(m) };
    } else if rc != 0 {
        panic!("pthread_mutex_lock failed: {rc}");
    }
}

fn unlock_mutex(m: *mut libc::pthread_mutex_t) {
    let rc = unsafe { libc::pthread_mutex_unlock(m) };
    if rc != 0 {
        panic!("pthread_mutex_unlock failed: {rc}");
    }
}

unsafe fn mmap_rw(fd: i32) -> io::Result<*mut u8> {
    let raw = unsafe {
        libc::mmap(
            ptr::null_mut(),
            TOTAL_BYTES,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            fd,
            0,
        )
    };
    if raw == libc::MAP_FAILED {
        return Err(io::Error::last_os_error());
    }
    Ok(raw as *mut u8)
}

unsafe fn init_metadata(base: *mut u8) -> io::Result<()> {
    unsafe { ptr::write_bytes(base, 0, METADATA_BYTES) };

    let mut attr: libc::pthread_mutexattr_t = unsafe { mem::zeroed() };
    let rc = unsafe { libc::pthread_mutexattr_init(&mut attr) };
    if rc != 0 {
        return Err(io::Error::from_raw_os_error(rc));
    }
    let rc = unsafe { libc::pthread_mutexattr_setpshared(&mut attr, libc::PTHREAD_PROCESS_SHARED) };
    if rc != 0 {
        unsafe { libc::pthread_mutexattr_destroy(&mut attr) };
        return Err(io::Error::from_raw_os_error(rc));
    }
    let rc = unsafe { libc::pthread_mutexattr_setrobust(&mut attr, libc::PTHREAD_MUTEX_ROBUST) };
    if rc != 0 {
        unsafe { libc::pthread_mutexattr_destroy(&mut attr) };
        return Err(io::Error::from_raw_os_error(rc));
    }

    for idx in 0..METADATA_PAGES {
        let page_base = unsafe { base.add(idx * CACHE_PAGE_SIZE) };
        let mutex = page_base.wrapping_add(MUTEX_OFFSET) as *mut libc::pthread_mutex_t;
        let rc = unsafe { libc::pthread_mutex_init(mutex, &attr) };
        if rc != 0 {
            unsafe { libc::pthread_mutexattr_destroy(&mut attr) };
            return Err(io::Error::from_raw_os_error(rc));
        }
        for s in 0..SETS_PER_META {
            let sp = unsafe { set_ptr(page_base, s) };
            unsafe {
                (*sp).page_ids = [EMPTY_KEY; WAYS_PER_SET];
                (*sp).timestamps = [0; WAYS_PER_SET];
            }
        }
    }

    unsafe { libc::pthread_mutexattr_destroy(&mut attr) };
    Ok(())
}
