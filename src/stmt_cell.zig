const std = @import("std");
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const sqlite = @import("sqlite3.zig");
const Conn = sqlite.Conn;
const Stmt = sqlite.Stmt;

pub fn StmtCell(comptime Ctx: type) type {
    return struct {
        const Self = @This();

        /// Use an arena allocator so that the callback function can optionally allocate
        /// (or not) and deallocation remains consistent
        pub const InitSqlFn = fn (*const Ctx, *ArenaAllocator) Allocator.Error![]const u8;

        stmt: ?Stmt,
        initSql: *const InitSqlFn,

        pub fn init(comptime initSql: *const InitSqlFn) Self {
            return .{
                .stmt = null,
                .initSql = initSql,
            };
        }

        pub fn deinit(self: *Self) void {
            if (self.stmt) |s| {
                s.deinit();
            }
        }

        /// Clears bound parameters of the statement and resets the execution. Only call
        /// `reset` after `getStmt`
        pub fn reset(self: *Self) void {
            const s = self.stmt.?;
            s.clearBoundParams() catch {};
            // My reading of the documentation* is that reseting a statement can return
            // an error code from the previous execution of the statement but does not
            // indicate that the process of reseting has failed. Therefore, ignore any
            // errors returned by calling reset. My hope is that those errors will also
            // show up in a call to commit for dml/ddl and will be evident in any query.
            // Ignoring the error returned by reset is also what rusqlite does.
            //
            // * https://www.sqlite.org/c3ref/reset.html (The return code from
            //   sqlite3_reset(S) indicates whether or not the previous evaluation of
            //   prepared statement S completed successfully)
            s.resetExec() catch |e| {
                std.log.debug("error reseting statement: {any}", .{e});
            };
        }

        pub fn getStmt(self: *Self, arena: *ArenaAllocator, ctx: *const Ctx) !Stmt {
            if (self.stmt == null) {
                const sql = try self.initSql(ctx, arena);
                self.stmt = try ctx.conn.prepare(sql);
            }
            return self.stmt.?;
        }
    };
}
