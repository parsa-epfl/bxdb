use std::ffi::{CStr, c_char, c_int};
use std::path::Path;
use std::ptr;
use std::slice;

use crate::append_only::AppendOnlyDb;
use crate::btree::{self, BtreeDb};
use crate::cache;
use crate::chunk::PAGE_SIZE;

pub enum BxdbHandle {
    Dummy,
    AppendOnly(AppendOnlyDb),
    Btree(BtreeDb),
}

#[unsafe(no_mangle)]
pub extern "C" fn bxdb_init() -> *mut BxdbHandle {
    Box::into_raw(Box::new(BxdbHandle::Dummy))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn bxdb_open_for_append_only(
    name: *const c_char,
    worker_count: c_int,
    delta_threshold: u16,
    use_shadow: bool,
) -> *mut BxdbHandle {
    if name.is_null() || worker_count <= 0 {
        return ptr::null_mut();
    }
    let name = match unsafe { CStr::from_ptr(name) }.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };
    match AppendOnlyDb::open(name, worker_count as usize, delta_threshold, use_shadow) {
        Ok(db) => Box::into_raw(Box::new(BxdbHandle::AppendOnly(db))),
        Err(_) => ptr::null_mut(),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn bxdb_open_for_btree(name: *const c_char) -> *mut BxdbHandle {
    if name.is_null() {
        return ptr::null_mut();
    }
    let name = match unsafe { CStr::from_ptr(name) }.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };
    match BtreeDb::open(name) {
        Ok(db) => Box::into_raw(Box::new(BxdbHandle::Btree(db))),
        Err(_) => ptr::null_mut(),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn bxdb_close(db: *mut BxdbHandle) {
    if db.is_null() {
        return;
    }
    drop(unsafe { Box::from_raw(db) });
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn bxdb_save_all_pages(
    db: *mut BxdbHandle,
    memory: *const c_char,
    total_page_count: u64,
    snapshot_id: u32,
) {
    if db.is_null() || memory.is_null() {
        return;
    }
    let handle = unsafe { &mut *db };
    let append_db = match handle {
        BxdbHandle::AppendOnly(w) => w,
        _ => return,
    };
    let mem_len = (total_page_count as usize).saturating_mul(PAGE_SIZE);
    let mem = unsafe { slice::from_raw_parts(memory as *const u8, mem_len) };
    let _ = append_db.save_all_pages(mem, total_page_count, snapshot_id);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn bxdb_save_pages_with_bitmap(
    db: *mut BxdbHandle,
    memory: *const c_char,
    dirty_bitmap: *const u64,
    total_page_count: u64,
    snapshot_id: u32,
) {
    if db.is_null() || memory.is_null() || dirty_bitmap.is_null() {
        return;
    }
    let handle = unsafe { &mut *db };
    let append_db = match handle {
        BxdbHandle::AppendOnly(w) => w,
        _ => return,
    };
    let mem_len = (total_page_count as usize).saturating_mul(PAGE_SIZE);
    let mem = unsafe { slice::from_raw_parts(memory as *const u8, mem_len) };
    let bitmap_words = ((total_page_count + 63) / 64) as usize;
    let bitmap = unsafe { slice::from_raw_parts(dirty_bitmap, bitmap_words) };
    let _ = append_db.save_pages_with_bitmap(mem, bitmap, total_page_count, snapshot_id);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn bxdb_load_page(
    db: *mut BxdbHandle,
    page: *mut c_char,
    pa: u64,
    snapshot_id: u32,
) -> bool {
    if db.is_null() || page.is_null() {
        return false;
    }
    let handle = unsafe { &*db };
    let btree_db = match handle {
        BxdbHandle::Btree(r) => r,
        _ => return false,
    };
    match btree_db.load_page(pa, snapshot_id) {
        Ok(Some(p)) => {
            let out = unsafe { slice::from_raw_parts_mut(page as *mut u8, PAGE_SIZE) };
            out.copy_from_slice(&p);
            true
        }
        _ => false,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn bxdb_load_all_pages(
    db: *mut BxdbHandle,
    pages: *mut c_char,
    pa_offset: u64,
    total_page_count: u64,
    snapshot_id: u32,
    worker_count: c_int,
) -> bool {
    if db.is_null() || pages.is_null() {
        return false;
    }
    let handle = unsafe { &*db };
    let append_db = match handle {
        BxdbHandle::AppendOnly(w) => w,
        _ => return false,
    };
    let len = (total_page_count as usize).saturating_mul(PAGE_SIZE);
    let out = unsafe { slice::from_raw_parts_mut(pages as *mut u8, len) };
    append_db
        .load_all_pages(
            out,
            pa_offset,
            total_page_count,
            snapshot_id,
            worker_count.max(1) as usize,
        )
        .unwrap_or(false)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn bxdb_cache_create(name: *const c_char) -> bool {
    if name.is_null() {
        return false;
    }
    let name = match unsafe { CStr::from_ptr(name) }.to_str() {
        Ok(s) => s,
        Err(_) => return false,
    };
    let dir = Path::new(name);
    let canonical = dir.canonicalize().unwrap_or_else(|_| dir.to_path_buf());
    let cache_path = match btree::shm_cache_path(&canonical) {
        Ok(p) => p,
        Err(_) => return false,
    };
    cache::SharedCache::create(&cache_path, &canonical).is_ok()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn bxdb_cache_delete(name: *const c_char) -> bool {
    if name.is_null() {
        return false;
    }
    let name = match unsafe { CStr::from_ptr(name) }.to_str() {
        Ok(s) => s,
        Err(_) => return false,
    };
    let dir = Path::new(name);
    let canonical = dir.canonicalize().unwrap_or_else(|_| dir.to_path_buf());
    let cache_path = match btree::shm_cache_path(&canonical) {
        Ok(p) => p,
        Err(_) => return false,
    };
    let ok = std::fs::remove_file(&cache_path).is_ok();
    // Also try to remove the parent hash directory.
    if let Some(parent) = cache_path.parent() {
        let _ = std::fs::remove_dir(parent);
    }
    ok
}
