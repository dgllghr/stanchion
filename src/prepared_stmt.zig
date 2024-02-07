const std = @import("std");
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;
const ArrayList = std.ArrayList;

const sqlite = @import("sqlite3.zig");
const Conn = sqlite.Conn;
const Stmt = sqlite.Stmt;

/// Use an arena allocator so that the callback function can optionally allocate (or not) and
/// deallocation remains consistent
pub fn InitSqlFn(comptime Ctx: type) type {
    return fn (Ctx, *ArenaAllocator) Allocator.Error![]const u8;
}

pub fn Cell(comptime Ctx: type) type {
    return struct {
        const Self = @This();

        stmt: ?Stmt,
        initSql: *const InitSqlFn(Ctx),

        pub fn init(comptime initSql: *const InitSqlFn(Ctx)) Self {
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

        /// Acquires the only statement from the cell, initializing the statement if it has not
        /// been initialized previously. Since there is only one statement, every call to `take`
        /// must be followed by `release` and concurrent access of the statement is not allowed.
        pub fn acquire(self: *Self, arena: *ArenaAllocator, ctx: Ctx) !Stmt {
            if (self.stmt == null) {
                const sql = try self.initSql(ctx, arena);
                self.stmt = try ctx.conn().prepare(sql);
            }
            return self.stmt.?;
        }

        /// Resets a statement and adds it back to the cell for future reuse. `release` should
        /// only be after `take` has been called on the same cell.
        pub fn release(self: *Self) void {
            reset(self.stmt.?);
        }
    };
}

pub fn Pool(comptime Ctx: type) type {
    return struct {
        stmts: ArrayList(Stmt),
        initSql: *const InitSqlFn(Ctx),

        const Self = @This();

        pub fn init(allocator: Allocator, comptime initSql: *const InitSqlFn(Ctx)) Self {
            return .{
                .stmts = ArrayList(Stmt).init(allocator),
                .initSql = initSql,
            };
        }

        pub fn deinit(self: *Self) void {
            for (self.stmts.items) |stmt| {
                stmt.deinit();
            }
            self.stmts.deinit();
        }

        /// Acquires a statement from the cell, initializing a new statement if one is not
        /// available. Every call to `take` must be followed by a call to `release` for the
        /// statement to ensure the statement is returned to the pool.
        pub fn acquire(self: *Self, arena: *ArenaAllocator, ctx: Ctx) !Stmt {
            const stmt_exst = self.stmts.popOrNull();
            if (stmt_exst) |s| {
                return s;
            }

            const sql = try self.initSql(ctx, arena);
            const stmt = try ctx.conn().prepare(sql);
            // Ensure there is capacity for the stmt so when `release` is called capacity does not
            // need to be expanded (and `release` does not return an error)
            try self.stmts.ensureUnusedCapacity(1);
            return stmt;
        }

        /// Resets a statement and adds it back to the pool for future reuse. `release` should
        /// only be called with a stmt that was acquired through `take` of the same pool. To
        /// preload the pool with statements, append them to the `stmts` field directly.
        pub fn release(self: *Self, stmt: Stmt) void {
            reset(stmt);
            self.stmts.appendAssumeCapacity(stmt);
        }
    };
}

/// Using `inline` does help here
inline fn reset(stmt: Stmt) void {
    stmt.clearBoundParams() catch {};
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
    stmt.resetExec() catch |e| {
        std.log.debug("error reseting statement: {any}", .{e});
    };
}
