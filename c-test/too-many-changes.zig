const std = @import("std");

const BxdbHandle = opaque {};

extern fn bxdb_init() ?*BxdbHandle;
extern fn bxdb_open_for_write(name: [*:0]const u8, worker_count: i32, delta_threshold: u16) ?*BxdbHandle;
extern fn bxdb_open_for_read(name: [*:0]const u8) ?*BxdbHandle;
extern fn bxdb_close(db: ?*BxdbHandle) void;
extern fn bxdb_save_pages(
    db: ?*BxdbHandle,
    memory: [*]const u8,
    dirty_bitmap: [*]const u64,
    total_page_count: u64,
    snapshot_id: u32,
) void;
extern fn bxdb_load_page(db: ?*BxdbHandle, page: [*]u8, pa: u64, snapshot_id: u32) bool;
extern fn bxdb_load_all_pages(
    db: ?*BxdbHandle,
    pages: [*]u8,
    pa_offset: u64,
    total_page_count: u64,
    snapshot_id: u32,
    worker_count: i32,
) bool;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    const file_name = @src().file;
    // const db_name: []const u8 = @ptrCast(file_name[0 .. file_name.len - 4]);
    const db_name = try allocator.dupeZ(u8, file_name[0 .. file_name.len - 4]);

    std.debug.print("DB name: {s} \n", .{db_name});

    // first, delete the old test folder if it exists.
    std.fs.cwd().deleteTree(db_name) catch |err| {
        if (err != error.FileNotFound) return err;
    };

    const db = bxdb_open_for_write(db_name.ptr, 1, 0) orelse return error.OpenFailed;
    defer bxdb_close(db);

    const memory: []u8 = try allocator.alloc(u8, 4096 * 64);
    defer allocator.free(memory);

    // initialize to zero.
    @memset(memory, 0);

    var dirty_bitmap: u64 = 0xffffffffffffffff;
    var snapshot_id: u32 = 0;

    // We first create this checkpoint.
    bxdb_save_pages(db, memory.ptr, @ptrCast(&dirty_bitmap), 64, snapshot_id);
    snapshot_id += 1;

    // Then, we update the page and save a second version.
    memory[1] = 10;
    memory[2] = 20;
    memory[1 * 4096 + 1] = 30;
    memory[2 * 4096 + 1] = 40;

    // only the first two pages are changed.
    dirty_bitmap = 0x7;

    bxdb_save_pages(db, memory.ptr, @ptrCast(&dirty_bitmap), 64, snapshot_id);
    snapshot_id += 1;

    // For the second snapshot, I would like to create a delta.
    memory[1] = 100;
    dirty_bitmap = 1;
    bxdb_save_pages(db, memory.ptr, @ptrCast(&dirty_bitmap), 64, snapshot_id);
    snapshot_id += 1;

    // For the third snapshot, I would like to create a big delta.
    for (0..2049) |i| {
        memory[i] = @intCast((2049 - i) & 0xff);
    }

    dirty_bitmap = 1;
    bxdb_save_pages(db, memory.ptr, @ptrCast(&dirty_bitmap), 64, snapshot_id);
    snapshot_id += 1;

    // Then, we have another delta.
    memory[32] = 70;

    dirty_bitmap = 1;
    bxdb_save_pages(db, memory.ptr, @ptrCast(&dirty_bitmap), 64, snapshot_id);
    snapshot_id += 1;
}
