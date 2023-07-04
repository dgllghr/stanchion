const std = @import("std");
const Allocator = std.mem.Allocator;

const c = @import("./sqlite3/c.zig").c;
const vtab = @import("./sqlite3/vtab.zig");

const Table = @import("./Table.zig");
const StanchionVTab = vtab.VirtualTable(Table);

pub export fn sqlite3_stanchion_init(
    db: *c.sqlite3,
    err_msg: [*c][*c]u8,
    api: *c.sqlite3_api_routines,
) callconv(.C) c_int {
    _ = err_msg;

    c.sqlite3_api = api;

    // To store data common to all table instances (global to the module), replace this
    // allocator with a struct containing the common data (see `ModuleContext` in
    // `zig-sqlite`)
    var allocator = std.heap.GeneralPurposeAllocator(.{}){};

    const res = c.sqlite3_create_module_v2(
        db,
        "stanchion",
        &StanchionVTab.module,
        &allocator,
        null
    );
    if (res != c.SQLITE_OK) {
        // TODO return proper error
        //return errors.errorFromResultCode(result);
        return -1;
    }

    return c.SQLITE_OK;
}
