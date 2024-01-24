const std = @import("std");
const Allocator = std.mem.Allocator;
const GeneralPurposeAllocator = std.heap.GeneralPurposeAllocator;

const c_mod = @import("sqlite3/c.zig");
const c = c_mod.c;
const encodeSqliteVersion = c_mod.encodeVersionNumber;
const sqliteVersion = c_mod.versionNumber;
const sqlite = @import("sqlite3.zig");
const sqlite_errors = sqlite.errors;
const vtab = sqlite.vtab;
const Conn = sqlite.Conn;

const Table = @import("Table.zig");
const StanchionVTab = vtab.VirtualTable(Table);

const SegmentsFn = @import("functions/Segments.zig");
const SegmentsTabValFn = vtab.VirtualTable(SegmentsFn);
const SegmentInfoFn = @import("functions/SegmentInfo.zig");
const SegmentInfoTabValFn = vtab.VirtualTable(SegmentInfoFn);

var allocator: GeneralPurposeAllocator(.{}) = undefined;

const min_sqlite_version = encodeSqliteVersion(3, 26, 0);

pub export fn sqlite3_stanchion_init(
    db: *c.sqlite3,
    err_msg: [*c][*c]u8,
    api: *c.sqlite3_api_routines,
) callconv(.C) c_int {
    c.sqlite3_api = api;

    if (sqliteVersion() < min_sqlite_version) {
        setErrorMsg(err_msg, "stanchion requires sqlite version >= 3.26.0");
        return c.SQLITE_ERROR;
    }

    // To store data common to all table instances (global to the module), replace this allocator
    // with a struct containing the common data (see `ModuleContext` in `zig-sqlite`)
    allocator = GeneralPurposeAllocator(.{}){};

    var res = c.sqlite3_create_module_v2(
        db,
        "stanchion",
        &StanchionVTab.module,
        &allocator,
        null,
    );
    if (res != c.SQLITE_OK) {
        setErrorMsg(err_msg, "error creating stanchion module");
        return res;
    }

    res = c.sqlite3_create_module_v2(
        db,
        "stanchion_segments",
        &SegmentsTabValFn.module,
        &allocator,
        null,
    );
    if (res != c.SQLITE_OK) {
        setErrorMsg(err_msg, "error creating stanchion_segments module");
        return res;
    }

    res = c.sqlite3_create_module_v2(
        db,
        "stanchion_segment_info",
        &SegmentInfoTabValFn.module,
        &allocator,
        null,
    );
    if (res != c.SQLITE_OK) {
        setErrorMsg(err_msg, "error creating stanchion_segment_info module");
        return res;
    }

    return c.SQLITE_OK;
}

fn setErrorMsg(err_msg: [*c][*c]u8, msg_text: []const u8) void {
    const msg_buf: [*c]u8 = @ptrCast(c.sqlite3_malloc(@intCast(msg_text.len)));
    @memcpy(msg_buf[0..msg_text.len], msg_text);
    err_msg.* = msg_buf;
}
