const std = @import("std");
const bxdb = @import("bxdb.zig");

pub fn main(init: std.process.Init.Minimal) !void {
    var gpa_state = std.heap.DebugAllocator(.{}){};
    const allocator = gpa_state.allocator();

    var it = std.process.Args.Iterator.init(init.args);
    _ = it.skip();
    const db_path = it.next() orelse return error.BadArgs;
    const snap_str = it.next() orelse return error.BadArgs;
    const snapshot_id = try std.fmt.parseInt(u32, snap_str, 10);

    const db_path_z = try allocator.dupeZ(u8, db_path);
    defer allocator.free(db_path_z);

    std.debug.print("Opening {s}...\n", .{db_path});
    const db = bxdb.bxdb_open_for_btree(db_path_z.ptr) orelse {
        std.debug.print("FAILED to open — cache likely missing\n", .{});
        return error.OpenFailed;
    };
    defer bxdb.bxdb_close(db);

    var page: [4096]u8 = undefined;
    std.debug.print("Loading page 0 at snapshot {d}...\n", .{snapshot_id});
    _ = bxdb.bxdb_load_page(db, &page, 0, snapshot_id);
    std.debug.print("Done.\n", .{});
}
