const std = @import("std");
const Allocator = std.mem.Allocator;

const c = @import("./sqlite3/c.zig").c;
const sqlite_errors = @import("./sqlite3/errors.zig");
const vtab = @import("./sqlite3/vtab.zig");

const Conn = @import("./sqlite3/Conn.zig");

const Migrations = @import("./db.zig").Migrations;
const Table = @import("./Table.zig");
const StanchionVTab = vtab.VirtualTable(Table);

pub export fn sqlite3_stanchion_init(
    db: *c.sqlite3,
    err_msg: [*c][*c]u8,
    api: *c.sqlite3_api_routines,
) callconv(.C) c_int {
    c.sqlite3_api = api;

    // To store data common to all table instances (global to the module), replace this
    // allocator with a struct containing the common data (see `ModuleContext` in
    // `zig-sqlite`)
    var allocator = std.heap.GeneralPurposeAllocator(.{}){};

    const conn = Conn.init(db);
    Migrations.apply(conn) catch {
        err_msg.* = @constCast(@ptrCast("error apply migrations"));
        return c.SQLITE_ERROR;
    };

    const res = c.sqlite3_create_module_v2(
        db,
        "stanchion",
        &StanchionVTab.module,
        &allocator,
        null
    );
    if (res != c.SQLITE_OK) {
        err_msg.* = @constCast(@ptrCast("error creating module"));
        return res;
    }

    return c.SQLITE_OK;
}
