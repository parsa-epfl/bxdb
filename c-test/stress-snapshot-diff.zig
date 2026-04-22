const std = @import("std");
const bxdb = @import("./bxdb.zig");
const posix = std.posix;
const linux = std.os.linux;

const PAGE_SIZE: usize = 4096;
const TOTAL_PAGES: usize = 8_388_608; // 32 GiB
const TOTAL_BYTES: usize = TOTAL_PAGES * PAGE_SIZE;
const BITMAP_WORDS: usize = TOTAL_PAGES / 64;
const MODIFIED_PAGES: usize = TOTAL_PAGES / 4; // 25%

const RAW_FILE: [*:0]const u8 = "./stress-test-data.raw";
const DB_64W: [*:0]const u8 = "./stress-snapdiff-64w";
const DB_1W: [*:0]const u8 = "./stress-snapdiff-1w";

fn nanotime() u64 {
    var ts: linux.timespec = undefined;
    _ = linux.clock_gettime(.MONOTONIC, &ts);
    return @as(u64, @intCast(ts.sec)) * 1_000_000_000 + @as(u64, @intCast(ts.nsec));
}

fn modifiedByte(page_idx: usize) u8 {
    return @as(u8, @truncate(page_idx)) ^ 0xA5;
}

// Build sorted list of 25% random page indices via partial Fisher-Yates.
// Uses a hash map to track swapped positions without allocating the full index array.
fn buildModifiedSet(allocator: std.mem.Allocator) ![]usize {
    var rng = std.Random.Xoshiro256.init(0xcafebabe_12340000);
    const rand = rng.random();

    const selected = try allocator.alloc(usize, MODIFIED_PAGES);
    errdefer allocator.free(selected);

    var swapped = std.AutoHashMap(usize, usize).init(allocator);
    defer swapped.deinit();

    for (0..MODIFIED_PAGES) |i| {
        const j = i + rand.uintLessThan(usize, TOTAL_PAGES - i);
        const val_i = swapped.get(i) orelse i;
        const val_j = swapped.get(j) orelse j;
        try swapped.put(i, val_j);
        try swapped.put(j, val_i);
        selected[i] = val_j;
    }

    std.mem.sort(usize, selected, {}, std.sort.asc(usize));
    return selected;
}

fn mmapReadOnly() ![]align(std.heap.page_size_min) u8 {
    const fd = try posix.openatZ(posix.AT.FDCWD, RAW_FILE, .{ .ACCMODE = .RDONLY }, 0);
    defer _ = linux.close(fd);
    return posix.mmap(null, TOTAL_BYTES, .{ .READ = true }, .{ .TYPE = .SHARED }, fd, 0);
}

fn mmapPrivate() ![]align(std.heap.page_size_min) u8 {
    const fd = try posix.openatZ(posix.AT.FDCWD, RAW_FILE, .{ .ACCMODE = .RDWR }, 0);
    defer _ = linux.close(fd);
    return posix.mmap(null, TOTAL_BYTES, .{ .READ = true, .WRITE = true }, .{ .TYPE = .PRIVATE }, fd, 0);
}

fn deleteDbDir(name: []const u8) void {
    const io = std.Io.Threaded.global_single_threaded.io();
    std.Io.Dir.cwd().deleteTree(io, name) catch {};
}

fn verifyLoad(
    db: ?*bxdb.BxdbHandle,
    snapshot_id: u32,
    worker_count: i32,
    load_buf: []u8,
    expected: []const u8,
    label: []const u8,
) !void {
    const ok = bxdb.bxdb_load_all_pages(db, load_buf.ptr, 0, TOTAL_PAGES, snapshot_id, worker_count);
    if (!ok) {
        std.debug.print("Load FAILED: {s}\n", .{label});
        return error.LoadFailed;
    }
    if (!std.mem.eql(u8, load_buf, expected)) {
        for (load_buf, expected, 0..) |got, want, i| {
            if (got != want) {
                std.debug.panic("MISMATCH {s}: byte {d} (page {d}, offset {d}): got {d}, want {d}\n", .{
                    label, i, i / PAGE_SIZE, i % PAGE_SIZE, got, want,
                });
            }
        }
    }
    std.debug.print("OK: {s}\n", .{label});
    @memset(load_buf, 0);
}

pub fn main() !void {
    var gpa_state = std.heap.DebugAllocator(.{}){};
    const allocator = gpa_state.allocator();

    std.debug.print("Building modified page set (25% of {d} pages)...\n", .{TOTAL_PAGES});
    const modified_pages = try buildModifiedSet(allocator);
    defer allocator.free(modified_pages);

    // Dirty bitmap for snapshot 1 (only modified pages).
    const snap1_bitmap = try allocator.alloc(u64, BITMAP_WORDS);
    defer allocator.free(snap1_bitmap);
    @memset(snap1_bitmap, 0);
    for (modified_pages) |p| {
        snap1_bitmap[p / 64] |= @as(u64, 1) << @intCast(p % 64);
    }

    // All-ones bitmap for snapshot 0.
    const snap0_bitmap = try allocator.alloc(u64, BITMAP_WORDS);
    defer allocator.free(snap0_bitmap);
    @memset(snap0_bitmap, 0xFFFF_FFFF_FFFF_FFFF);

    std.debug.print("Mapping source file (snapshot 0 ground truth)...\n", .{});
    const src = try mmapReadOnly();
    defer posix.munmap(src);

    std.debug.print("Mapping snapshot-1 ground truth (MAP_PRIVATE, applying {d} modifications)...\n", .{MODIFIED_PAGES});
    const snap1_mem = try mmapPrivate();
    defer posix.munmap(snap1_mem);
    for (modified_pages) |p| {
        @memset(snap1_mem[p * PAGE_SIZE ..][0..PAGE_SIZE], modifiedByte(p));
    }

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

    // -------------------------------------------------------------------------
    // 64-worker database
    // -------------------------------------------------------------------------
    deleteDbDir("stress-snapdiff-64w");
    {
        const db = bxdb.bxdb_open_for_fw(DB_64W, 64, 256, true) orelse return error.OpenFailed;
        defer bxdb.bxdb_close(db);

        bxdb.bxdb_save_pages(db, src.ptr, snap0_bitmap.ptr, TOTAL_PAGES, 0);
        std.debug.print("Snap0 saved (64w)\n", .{});

        const t0 = nanotime();
        bxdb.bxdb_save_pages(db, snap1_mem.ptr, snap1_bitmap.ptr, TOTAL_PAGES, 1);
        const elapsed = nanotime() - t0;
        std.debug.print("Snap1 save 64w: {d} ns ({d:.2} s)\n", .{ elapsed, @as(f64, @floatFromInt(elapsed)) / 1e9 });
    }

    // -------------------------------------------------------------------------
    // 1-worker database
    // -------------------------------------------------------------------------
    deleteDbDir("stress-snapdiff-1w");
    {
        const db = bxdb.bxdb_open_for_fw(DB_1W, 1, 256, true) orelse return error.OpenFailed;
        defer bxdb.bxdb_close(db);

        bxdb.bxdb_save_pages(db, src.ptr, snap0_bitmap.ptr, TOTAL_PAGES, 0);
        std.debug.print("Snap0 saved (1w)\n", .{});

        const t0 = nanotime();
        bxdb.bxdb_save_pages(db, snap1_mem.ptr, snap1_bitmap.ptr, TOTAL_PAGES, 1);
        const elapsed = nanotime() - t0;
        std.debug.print("Snap1 save  1w: {d} ns ({d:.2} s)\n", .{ elapsed, @as(f64, @floatFromInt(elapsed)) / 1e9 });
    }

    // -------------------------------------------------------------------------
    // Verify all four load combinations
    // -------------------------------------------------------------------------
    {
        const db = bxdb.bxdb_open_for_fw(DB_64W, 64, 256, true) orelse return error.OpenFailed;
        defer bxdb.bxdb_close(db);
        try verifyLoad(db, 0, 64, load_buf, src, "64w DB snap=0 load 64w");
        try verifyLoad(db, 1, 64, load_buf, snap1_mem, "64w DB snap=1 load 64w");
    }
    {
        const db = bxdb.bxdb_open_for_fw(DB_1W, 1, 256, true) orelse return error.OpenFailed;
        defer bxdb.bxdb_close(db);
        try verifyLoad(db, 0, 1, load_buf, src, "1w DB snap=0 load 1w");
        try verifyLoad(db, 1, 1, load_buf, snap1_mem, "1w DB snap=1 load 1w");
    }

    std.debug.print("All snapshot comparisons passed.\n", .{});
}
