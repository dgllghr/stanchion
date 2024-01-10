//! A thin wrapper over a sqlite connection. Since this is a struct that only contains a
//! pointer, values of type `Conn` should be passed by value rather than by pointer.
//! Almost all queries and dml should happen through a `Stmt`, which is created with the
//! `prepare` function. In cases where no parameters are passed and no results are
//! returned, use the `exec` shortcut function.

const std = @import("std");
const mem = std.mem;

const c = @import("c.zig").c;
const errors = @import("errors.zig");
const Stmt = @import("Stmt.zig");

const Self = @This();

conn: *c.sqlite3,

pub fn init(conn: *c.sqlite3) Self {
    return .{ .conn = conn };
}

pub fn open(path: [*c]const u8) !Self {
    const flags = c.SQLITE_OPEN_READWRITE | c.SQLITE_OPEN_CREATE;
    var conn: ?*c.sqlite3 = null;
    const res = c.sqlite3_open_v2(path, &conn, flags, null);
    if (res != c.SQLITE_OK) {
        return errors.errorFromResultCode(res);
    }
    return .{ .conn = conn.? };
}

pub fn openInMemory() !Self {
    return open(":memory:");
}

pub fn close(self: Self) void {
    _ = c.sqlite3_close_v2(self.conn);
}

pub fn prepare(self: Self, sql: []const u8) !Stmt {
    var stmt: ?*c.sqlite3_stmt = null;
    const res = c.sqlite3_prepare_v3(
        self.conn,
        sql.ptr,
        @intCast(sql.len),
        c.SQLITE_PREPARE_PERSISTENT,
        &stmt,
        null,
    );
    if (res != c.SQLITE_OK) {
        return errors.errorFromResultCode(res);
    }

    return .{ .stmt = stmt.? };
}

pub fn exec(self: Self, sql: [*:0]const u8) !void {
    const res = c.sqlite3_exec(self.conn, sql, null, null, null);
    if (res != c.SQLITE_OK) {
        return errors.errorFromResultCode(res);
    }
}

pub fn lastInsertRowid(self: Self) i64 {
    return @intCast(c.sqlite3_last_insert_rowid(self.conn));
}

pub fn lastErrMsg(self: Self) []const u8 {
    const err_msg = c.sqlite3_errmsg(self.conn);
    return mem.sliceTo(err_msg, 0);
}
