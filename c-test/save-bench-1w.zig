const std = @import("std");
const bxdb = @import("./bxdb.zig");
const posix = std.posix;
const linux = std.os.linux;

const PAGE_SIZE: usize = 4096;
const TOTAL_PAGES: usize = 8_388_608;
const TOTAL_BYTES: usize = TOTAL_PAGES * PAGE_SIZE;
const BITMAP_WORDS: usize = TOTAL_PAGES / 64;
const RAW_FILE: [*:0]const u8 = "./stress-test-data.raw";
const DB_1W: [*:0]const u8 = "./save-bench-1w-db";

fn nanotime() u64 {
    var ts: linux.timespec = undefined;
    _ = linux.clock_gettime(.MONOTONIC, &ts);
    return @as(u64, @intCast(ts.sec)) * 1_000_000_000 + @as(u64, @intCast(ts.nsec));
}

fn deleteDbDir(name: []const u8) void {
    const io = std.Io.Threaded.global_single_threaded.io();
    std.Io.Dir.cwd().deleteTree(io, name) catch {};
}

pub fn main() !void {
    const bitmap = try std.heap.page_allocator.alloc(u64, BITMAP_WORDS);
    defer std.heap.page_allocator.free(bitmap);
    @memset(bitmap, 0xFFFF_FFFF_FFFF_FFFF);

    const fd = try posix.openatZ(posix.AT.FDCWD, RAW_FILE, .{ .ACCMODE = .RDONLY }, 0);
    defer _ = linux.close(fd);
    const src = try posix.mmap(null, TOTAL_BYTES, .{ .READ = true }, .{ .TYPE = .SHARED }, fd, 0);
    defer posix.munmap(src);

    deleteDbDir("save-bench-1w-db");
    const db = bxdb.bxdb_open_for_append_only(DB_1W, 1, 256, false) orelse return error.OpenFailed;
    defer bxdb.bxdb_close(db);

    std.debug.print("Starting 1-worker save (32 GiB)...\n", .{});
    const t0 = nanotime();
    bxdb.bxdb_save_pages(db, src.ptr, bitmap.ptr, TOTAL_PAGES, 0);
    const elapsed = nanotime() - t0;
    std.debug.print("Total: {d} ns ({d:.2} s)\n", .{ elapsed, @as(f64, @floatFromInt(elapsed)) / 1e9 });
}
