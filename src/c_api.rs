use std::ffi::{CStr, c_char, c_int};
use std::ptr;
use std::slice;

use crate::chunk::PAGE_SIZE;
use crate::read::ReadDb;
use crate::write::WriteDb;

pub enum BxdbHandle {
    Dummy,
    Write(WriteDb),
    Read(ReadDb),
}

#[unsafe(no_mangle)]
pub extern "C" fn bxdb_init() -> *mut BxdbHandle {
    Box::into_raw(Box::new(BxdbHandle::Dummy))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn bxdb_open_for_write(
    name: *const c_char,
    worker_count: c_int,
    delta_threshold: u16,
) -> *mut BxdbHandle {
    if name.is_null() || worker_count <= 0 {
        return ptr::null_mut();
    }
    let name = match unsafe { CStr::from_ptr(name) }.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };
    match WriteDb::open(name, worker_count as usize, delta_threshold) {
        Ok(db) => Box::into_raw(Box::new(BxdbHandle::Write(db))),
        Err(_) => ptr::null_mut(),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn bxdb_open_for_read(name: *const c_char) -> *mut BxdbHandle {
    if name.is_null() {
        return ptr::null_mut();
    }
    let name = match unsafe { CStr::from_ptr(name) }.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };
    match ReadDb::open(name) {
        Ok(db) => Box::into_raw(Box::new(BxdbHandle::Read(db))),
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
pub unsafe extern "C" fn bxdb_save_pages(
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
    let write_db = match handle {
        BxdbHandle::Write(w) => w,
        _ => return,
    };
    let mem_len = (total_page_count as usize).saturating_mul(PAGE_SIZE);
    let mem = unsafe { slice::from_raw_parts(memory as *const u8, mem_len) };
    let bitmap_words = ((total_page_count + 63) / 64) as usize;
    let bitmap = unsafe { slice::from_raw_parts(dirty_bitmap, bitmap_words) };
    let _ = write_db.save_pages(mem, bitmap, total_page_count, snapshot_id);
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
    let read_db = match handle {
        BxdbHandle::Read(r) => r,
        _ => return false,
    };
    match read_db.load_page(pa, snapshot_id) {
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
    let read_db = match handle {
        BxdbHandle::Read(r) => r,
        _ => return false,
    };
    let len = (total_page_count as usize).saturating_mul(PAGE_SIZE);
    let out = unsafe { slice::from_raw_parts_mut(pages as *mut u8, len) };
    read_db
        .load_all_pages(out, pa_offset, total_page_count, snapshot_id, worker_count.max(1) as usize)
        .unwrap_or(false)
}
