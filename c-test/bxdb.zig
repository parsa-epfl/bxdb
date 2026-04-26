pub const BxdbHandle = opaque {};

pub extern fn bxdb_init() ?*BxdbHandle;
pub extern fn bxdb_open_for_append_only(name: [*:0]const u8, worker_count: i32, delta_threshold: u16, use_shadow: bool) ?*BxdbHandle;
pub extern fn bxdb_open_for_btree(name: [*:0]const u8) ?*BxdbHandle;
pub extern fn bxdb_close(db: ?*BxdbHandle) void;
pub extern fn bxdb_save_pages(
    db: ?*BxdbHandle,
    memory: [*]const u8,
    dirty_bitmap: [*]const u64,
    total_page_count: u64,
    snapshot_id: u32,
) void;
pub extern fn bxdb_load_page(db: ?*BxdbHandle, page: [*]u8, pa: u64, snapshot_id: u32) bool;
pub extern fn bxdb_load_all_pages(
    db: ?*BxdbHandle,
    pages: [*]u8,
    pa_offset: u64,
    total_page_count: u64,
    snapshot_id: u32,
    worker_count: i32,
) bool;
