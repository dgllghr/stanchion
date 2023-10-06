const std = @import("std");
const Allocator = std.mem.Allocator;

const Conn = @import("sqlite3/Conn.zig");
const Stmt = @import("sqlite3/Stmt.zig");

const Self = @This();

stmt: ?Stmt,
/// Not owned and therefore not deallocated by the `StmtCell`. Must be de-allocated by
/// the caller.
sql: [:0]const u8,

pub fn init(sql: [:0]const u8) Self {
    return .{
        .stmt = null,
        .sql = sql,
    };
}

pub fn deinit(self: *Self) void {
    if (self.stmt) |s| {
        s.deinit();
    }
}

pub fn reset(self: *Self) void {
    if (self.stmt) |s| {
        // TODO a failed reset can invalidate a transaction. How can this error be
        //      propagated?
        s.reset() catch |e| {
            std.log.err("failed to reset statement: {any}", .{e});
            s.deinit();
            self.stmt = null;
        };
    }
}

pub fn getStmt(self: *Self, conn: Conn) !Stmt {
    if (self.stmt == null) {
        self.stmt = try conn.prepare(self.sql);
    }
    return self.stmt.?;
}
