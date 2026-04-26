const std = @import("std");
const bxdb = @import("./bxdb.zig");
const posix = std.posix;
const linux = std.os.linux;

const PAGE_SIZE: usize = 4096;
const TOTAL_PAGES: usize = 8_388_608; // 32 GiB
const TOTAL_BYTES: usize = TOTAL_PAGES * PAGE_SIZE;
const BITMAP_WORDS: usize = TOTAL_PAGES / 64;
const RAW_FILE: [*:0]const u8 = "./stress-test-data.raw";
const DB_64W: [*:0]const u8 = "./stress-save-64w";
const DB_1W: [*:0]const u8 = "./stress-save-1w";

fn nanotime() u64 {
    var ts: linux.timespec = undefined;
    _ = linux.clock_gettime(.MONOTONIC, &ts);
    return @as(u64, @intCast(ts.sec)) * 1_000_000_000 + @as(u64, @intCast(ts.nsec));
}

fn rawFileExists() bool {
    const fd = posix.openatZ(posix.AT.FDCWD, RAW_FILE, .{ .ACCMODE = .RDONLY }, 0) catch return false;
    _ = linux.close(fd);
    return true;
}

fn generateRawFile() !void {
    std.debug.print("Generating stress-test-data.raw ({d} GiB)...\n", .{TOTAL_BYTES / (1024 * 1024 * 1024)});
    const t0 = nanotime();

    const fd = try posix.openatZ(posix.AT.FDCWD, RAW_FILE, .{ .ACCMODE = .RDWR, .CREAT = true, .TRUNC = true }, 0o644);
    defer _ = linux.close(fd);

    const ret = linux.ftruncate(fd, @intCast(TOTAL_BYTES));
    if (linux.errno(ret) != .SUCCESS) return error.FtruncateFailed;

    const mapped = try posix.mmap(null, TOTAL_BYTES, .{ .READ = true, .WRITE = true }, .{ .TYPE = .SHARED }, fd, 0);
    defer posix.munmap(mapped);

    const BLOCK: usize = 64 * 1024 * 1024;
    const block_buf = try std.heap.page_allocator.alloc(u8, BLOCK);
    defer std.heap.page_allocator.free(block_buf);

    var rng = std.Random.DefaultPrng.init(0xdeadbeef_cafebabe);
    const rand = rng.random();

    var offset: usize = 0;
    while (offset < TOTAL_BYTES) {
        const n = @min(BLOCK, TOTAL_BYTES - offset);
        rand.bytes(block_buf[0..n]);
        @memcpy(mapped[offset..][0..n], block_buf[0..n]);
        offset += n;
    }

    const elapsed = nanotime() - t0;
    std.debug.print("Generated in {d:.2} s\n", .{@as(f64, @floatFromInt(elapsed)) / 1e9});
}

fn mmapReadOnly() ![]align(std.heap.page_size_min) u8 {
    const fd = try posix.openatZ(posix.AT.FDCWD, RAW_FILE, .{ .ACCMODE = .RDONLY }, 0);
    defer _ = linux.close(fd);
    return posix.mmap(null, TOTAL_BYTES, .{ .READ = true }, .{ .TYPE = .SHARED }, fd, 0);
}

fn deleteDbDir(name: []const u8) void {
    const io = std.Io.Threaded.global_single_threaded.io();
    std.Io.Dir.cwd().deleteTree(io, name) catch {};
}

pub fn main() !void {
    if (!rawFileExists()) {
        try generateRawFile();
    } else {
        std.debug.print("Reusing existing stress-test-data.raw\n", .{});
    }

    const bitmap = try std.heap.page_allocator.alloc(u64, BITMAP_WORDS);
    defer std.heap.page_allocator.free(bitmap);
    @memset(bitmap, 0xFFFF_FFFF_FFFF_FFFF);

    const src = try mmapReadOnly();
    defer posix.munmap(src);

    std.debug.print("Allocating 32 GiB load buffer...\n", .{});
    const load_buf = try posix.mmap(
        null,
        TOTAL_BYTES,
        .{ .READ = true, .WRITE = true },
        .{ .TYPE = .PRIVATE, .ANONYMOUS = true },
        -1,
        0,
    );
    defer posix.munmap(load_buf);

    // --- Save with 64 workers ---
    deleteDbDir("stress-save-64w");
    {
        const db = bxdb.bxdb_open_for_append_only(DB_64W, 64, 256, false) orelse return error.OpenFailed;
        defer bxdb.bxdb_close(db);
        const t0 = nanotime();
        bxdb.bxdb_save_pages(db, src.ptr, bitmap.ptr, TOTAL_PAGES, 0);
        const elapsed = nanotime() - t0;
        std.debug.print("Save 64w: {d} ns ({d:.2} s)\n", .{ elapsed, @as(f64, @floatFromInt(elapsed)) / 1e9 });
    }

    // --- Save with 1 worker ---
    deleteDbDir("stress-save-1w");
    {
        const db = bxdb.bxdb_open_for_append_only(DB_1W, 1, 256, false) orelse return error.OpenFailed;
        defer bxdb.bxdb_close(db);
        const t0 = nanotime();
        bxdb.bxdb_save_pages(db, src.ptr, bitmap.ptr, TOTAL_PAGES, 0);
        const elapsed = nanotime() - t0;
        std.debug.print("Save  1w: {d} ns ({d:.2} s)\n", .{ elapsed, @as(f64, @floatFromInt(elapsed)) / 1e9 });
    }

    // --- Load 64w DB with 1 worker, compare ---
    {
        const db = bxdb.bxdb_open_for_append_only(DB_64W, 1, 256, false) orelse return error.OpenFailed;
        defer bxdb.bxdb_close(db);
        const ok = bxdb.bxdb_load_all_pages(db, load_buf.ptr, 0, TOTAL_PAGES, 0, 1);
        if (!ok) return error.LoadFailed;
        if (!std.mem.eql(u8, load_buf, src)) {
            std.debug.panic("Load (64w DB, 1w load): MISMATCH\n", .{});
        }
        std.debug.print("Load (64w DB, 1w load): OK\n", .{});
        @memset(load_buf, 0);
    }

    // --- Load 1w DB with 64 workers, compare ---
    {
        const db = bxdb.bxdb_open_for_append_only(DB_1W, 64, 256, false) orelse return error.OpenFailed;
        defer bxdb.bxdb_close(db);
        const ok = bxdb.bxdb_load_all_pages(db, load_buf.ptr, 0, TOTAL_PAGES, 0, 64);
        if (!ok) return error.LoadFailed;
        if (!std.mem.eql(u8, load_buf, src)) {
            std.debug.panic("Load  (1w DB, 64w load): MISMATCH\n", .{});
        }
        std.debug.print("Load  (1w DB, 64w load): OK\n", .{});
    }

    std.debug.print("All checks passed.\n", .{});
}
