const std = @import("std");
const bxdb = @import("./bxdb.zig");

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

    const db = bxdb.bxdb_open_for_fw(db_name.ptr, 1, 0) orelse return error.OpenFailed;

    const memory: []u8 = try allocator.alloc(u8, 4096 * 64);
    defer allocator.free(memory);

    // initialize to zero.
    @memset(memory, 0);

    var dirty_bitmap: u64 = 0xffffffffffffffff;
    var snapshot_id: u32 = 0;

    // We first create this checkpoint.
    bxdb.bxdb_save_pages(db, memory.ptr, @ptrCast(&dirty_bitmap), 64, snapshot_id);
    snapshot_id += 1;

    // Then, we update the page and save a second version.
    memory[1] = 10;
    memory[2] = 20;
    memory[1 * 4096 + 1] = 30;
    memory[2 * 4096 + 1] = 40;

    // only the first two pages are changed.
    dirty_bitmap = 0x7;

    bxdb.bxdb_save_pages(db, memory.ptr, @ptrCast(&dirty_bitmap), 64, snapshot_id);
    snapshot_id += 1;

    // Let's do a third-time changes, targeting page[2].
    memory[2 * 4096 + 10] = 55;
    dirty_bitmap = 1 << 2;
    bxdb.bxdb_save_pages(db, memory.ptr, @ptrCast(&dirty_bitmap), 64, snapshot_id);
    snapshot_id += 1;

    // Now, we close this DB.
    bxdb.bxdb_close(db);

    // Then, we read this DB again.
    const db2 = bxdb.bxdb_open_for_fw(db_name.ptr, 1, 0) orelse return error.OpenFailed;
    defer bxdb.bxdb_close(db2);

    memory[3] = 50;
    dirty_bitmap = 1;
    bxdb.bxdb_save_pages(db2, memory.ptr, @ptrCast(&dirty_bitmap), 64, snapshot_id);
    snapshot_id += 1;
}
