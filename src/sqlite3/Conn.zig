const c = @import("./c.zig").c;

const errors = @import("./errors.zig");
const Stmt = @import("./Stmt.zig");

const Self = @This();

conn: *c.sqlite3,

pub fn init(conn: *c.sqlite3) Self {
    return .{ .conn = conn };
}

pub fn open(path: [*c]const u8) !Self {
    const flags = c.SQLITE_OPEN_READWRITE;
    var conn: ?*c.sqlite3 = null;
    const res = c.sqlite3_open_v2(path, &conn, flags, null);
    if (res != c.SQLITE_OK) {
        return errors.errorFromResultCode(res);
    }
    return .{.conn = conn.?};
}

pub fn openInMemory() !Self {
    return open(":memory:");
}

pub fn close(self: Self) void {
    _ = c.sqlite3_close_v2(self.conn);
}

pub fn prepare(self: Self, sql: []const u8) !Stmt {
    var stmt: ?*c.sqlite3_stmt = null;
    const res = c.sqlite3_prepare_v2(self.conn, sql.ptr, @intCast(sql.len), &stmt, null);
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
