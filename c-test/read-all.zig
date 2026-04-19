const std = @import("std");
const bxdb = @import("./bxdb.zig");

fn populate_memory_and_save_db(memory: []u8, db_name: [:0]u8, mv: [][4096 * 3]u8) void {
    @memset(memory, 0);
    // We create some data.
    memory[0] = 100;
    memory[1] = 20;
    memory[2] = 30;
    memory[3] = 70;

    memory[4096 + 0] = 89;
    memory[4096 + 1] = 192;
    memory[4096 + 2] = 13;

    memory[8192 + 0] = 33;
    memory[8192 + 1] = 44;
    memory[8192 + 2] = 55;

    var dirty_bitmap: u64 = 0b111;
    var snapshot_id: u32 = 0;

    // create the db.
    const db = bxdb.bxdb_open_for_fw(db_name, 1, 0);
    defer bxdb.bxdb_close(db);

    // save a version.
    bxdb.bxdb_save_pages(db, memory.ptr, @ptrCast(&dirty_bitmap), 3, snapshot_id);
    @memcpy(mv[snapshot_id][0..], memory);
    snapshot_id += 1;

    // Now, I would like to make some changes.
    memory[0] = 32;
    memory[1] = 20;
    memory[2] = 75;

    memory[8192 + 0] = 34;
    dirty_bitmap = 0b101;
    bxdb.bxdb_save_pages(db, memory.ptr, @ptrCast(&dirty_bitmap), 3, snapshot_id);
    @memcpy(mv[snapshot_id][0..], memory);
    snapshot_id += 1;

    // Then, we create a bigger change on 0. We create 2000 changes on page 3.
    for (0..2000) |i| {
        memory[4096 + i] = @intCast(i & 0xff);
    }

    // Then, we also create some changes on page 0.
    memory[4] = 111;

    dirty_bitmap = 0b011;
    bxdb.bxdb_save_pages(db, memory.ptr, @ptrCast(&dirty_bitmap), 3, snapshot_id);
    @memcpy(mv[snapshot_id][0..], memory);
    snapshot_id += 1;

    // As the last change, we create  50 more changes at the second-half of the
    for (3000..3050) |i| {
        memory[4096 + i] = @intCast(i & 0xff);
    }
    memory[5] = 222;

    dirty_bitmap = 0b11;
    bxdb.bxdb_save_pages(db, memory.ptr, @ptrCast(&dirty_bitmap), 3, snapshot_id);
    @memcpy(mv[snapshot_id][0..], memory);
    snapshot_id += 1;
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    // Init DB.
    const file_name = @src().file;
    // const db_name: []const u8 = @ptrCast(file_name[0 .. file_name.len - 4]);
    const db_name = try allocator.dupeZ(u8, file_name[0 .. file_name.len - 4]);

    std.debug.print("DB name: {s} \n", .{db_name});

    // first, delete the old test folder if it exists.
    std.fs.cwd().deleteTree(db_name) catch |err| {
        if (err != error.FileNotFound) return err;
    };

    // Create a big memory chunk.
    // Add a few snapshots.
    // Load it back and compare.
    const memory: []u8 = try allocator.alloc(u8, 4096 * 3);
    const mv: [][4096 * 3]u8 = try allocator.alloc([4096 * 3]u8, 4);
    for (0..4) |i| {
        @memset(mv[i][0..], 0);
    }
    populate_memory_and_save_db(memory, db_name, mv);

    // Nice. Now, let's load the version one by one to understand it.
    for (0..4) |id| {
        // load the DB.
        const db = bxdb.bxdb_open_for_fw(db_name, 1, 0);

        const temporal_buf = try allocator.alloc(u8, 4096 * 3);
        defer allocator.free(temporal_buf);

        _ = bxdb.bxdb_load_all_pages(db, temporal_buf.ptr, 0, 3, @intCast(id), 1);

        // now compare this page with the corresponding page in version.
        for (0..4096 * 3) |byte_idx| {
            if (mv[id][byte_idx] != temporal_buf[byte_idx]) {
                std.debug.panic("Idx does not match: SNAPSHOT={d}, PA={d}, Offset={x}. MV={x}, LOADED={d}\n", .{ id, byte_idx / 4096, byte_idx % 4096, mv[id][byte_idx], temporal_buf[byte_idx] });
            }
        }
    }

    std.debug.print("Test passed! \n", .{});
}
