const std = @import("std");
const bxdb = @import("./bxdb.zig");
const linux = std.os.linux;

const PAGE_SIZE: usize = 4096;
const TOTAL_PAGES: u64 = 8_388_608;
const NUM_ACCESSES: usize = 1_000_000;

// λ chosen so 50% of accesses land in the hottest 0.5% of pages.
// CDF at 0.005*N = 0.5  =>  λ = ln(2) / (0.005 * N)
const LN2: f64 = 0.6931471805599453;
const LAMBDA: f64 = LN2 / (0.005 * @as(f64, @floatFromInt(TOTAL_PAGES)));

fn nanotime() u64 {
    var ts: linux.timespec = undefined;
    _ = linux.clock_gettime(.MONOTONIC, &ts);
    return @as(u64, @intCast(ts.sec)) * 1_000_000_000 + @as(u64, @intCast(ts.nsec));
}

fn samplePage(rand: std.Random) u64 {
    const u = rand.float(f64);
    const safe_u = if (u >= 1.0) 1.0 - 1e-15 else u;
    const raw: f64 = -@log(1.0 - safe_u) / LAMBDA;
    const page = @as(u64, @intFromFloat(@floor(raw)));
    return @min(page, TOTAL_PAGES - 1);
}

pub fn main(init: std.process.Init.Minimal) !void {
    var gpa_state = std.heap.DebugAllocator(.{}){};
    const allocator = gpa_state.allocator();

    var it = std.process.Args.Iterator.init(init.args);
    _ = it.skip(); // skip program name
    const db_path = it.next() orelse {
        std.debug.print("Usage: timing-serve <db_path> <snapshot_id> <process_id>\n", .{});
        return error.BadArgs;
    };
    const snap_str = it.next() orelse return error.BadArgs;
    const pid_str = it.next() orelse return error.BadArgs;

    const snapshot_id = try std.fmt.parseInt(u32, snap_str, 10);
    const process_id = try std.fmt.parseInt(u32, pid_str, 10);

    const db_path_z = try allocator.dupeZ(u8, db_path);
    defer allocator.free(db_path_z);

    const db = bxdb.bxdb_open_for_btree(db_path_z.ptr) orelse {
        std.debug.print(
            \\Failed to open DB at "{s}" for reading.
            \\The shared-memory page cache must be created first:
            \\  bxdb cache create "{s}"
            \\
        , .{ db_path, db_path });
        return error.OpenFailed;
    };
    defer bxdb.bxdb_close(db);

    var rng = std.Random.DefaultPrng.init(@as(u64, process_id) *% 0xdeadbeef + 1);
    const rand = rng.random();

    // Open CSV file for writing using the Io system
    const io = std.Io.Threaded.global_single_threaded.io();
    const csv_name = try std.fmt.allocPrint(allocator, "timing_latencies_{d}.csv", .{process_id});
    defer allocator.free(csv_name);

    const csv_file = try std.Io.Dir.cwd().createFile(io, csv_name, .{});
    defer csv_file.close(io);

    // Use a write buffer for the CSV writer
    const write_buf = try allocator.alloc(u8, 64 * 1024);
    defer allocator.free(write_buf);
    var fw = std.Io.File.Writer.initStreaming(csv_file, io, write_buf);
    defer fw.end() catch {};

    try fw.interface.writeAll("process_id,access_idx,page_idx,latency_ns\n");

    var page_buf: [PAGE_SIZE]u8 = undefined;

    for (0..NUM_ACCESSES) |access_idx| {
        const page_idx = samplePage(rand);
        const pa: u64 = page_idx * PAGE_SIZE;

        const t0 = nanotime();
        _ = bxdb.bxdb_load_page(db, &page_buf, pa, snapshot_id);
        const elapsed = nanotime() - t0;

        try fw.interface.print("{d},{d},{d},{d}\n", .{ process_id, access_idx, page_idx, elapsed });
    }

    std.debug.print("Process {d} done: {d} accesses written to {s}\n", .{ process_id, NUM_ACCESSES, csv_name });
}
