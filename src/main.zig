const std = @import("std");
const Allocator = std.mem.Allocator;
const GeneralPurposeAllocator = std.heap.GeneralPurposeAllocator;

const c = @import("./sqlite3/c.zig").c;
const sqlite = @import("sqlite3.zig");
const sqlite_errors = sqlite.errors;
const vtab = sqlite.vtab;
const Conn = sqlite.Conn;

const Table = @import("./Table.zig");
const StanchionVTab = vtab.VirtualTable(Table);

var allocator: GeneralPurposeAllocator(.{}) = undefined;

pub export fn sqlite3_stanchion_init(
    db: *c.sqlite3,
    err_msg: [*c][*c]u8,
    api: *c.sqlite3_api_routines,
) callconv(.C) c_int {
    c.sqlite3_api = api;

    // To store data common to all table instances (global to the module), replace this allocator
    // with a struct containing the common data (see `ModuleContext` in `zig-sqlite`)
    allocator = GeneralPurposeAllocator(.{}){};

    const res = c.sqlite3_create_module_v2(
        db,
        "stanchion",
        &StanchionVTab.module,
        &allocator,
        null,
    );
    if (res != c.SQLITE_OK) {
        err_msg.* = @constCast(@ptrCast("error creating module"));
        return res;
    }

    return c.SQLITE_OK;
}
