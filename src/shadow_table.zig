const std = @import("std");
const fmt = std.fmt;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const sqlite = @import("sqlite3.zig");
const Conn = sqlite.Conn;

pub const Error = error{ShadowTableDoesNotExist};

pub fn ShadowTable(comptime VTabCtx: type, comptime ShadowTableDef: type) type {
    return struct {
        ctx: *const VTabCtx,

        const Self = @This();

        pub const suffix = ShadowTableDef.suffix;

        pub fn create(self: Self, tmp_arena: *ArenaAllocator) !void {
            const ddl = try ShadowTableDef.createTableDdl(self.ctx.*, tmp_arena.allocator());
            try self.ctx.conn().exec(ddl);
        }

        pub fn verifyExists(self: Self, tmp_arena: *ArenaAllocator) !void {
            const query = try fmt.allocPrintZ(tmp_arena.allocator(),
                \\SELECT 1
                \\FROM sqlite_master
                \\WHERE type = 'table' AND name = '{s}_{s}'
            , .{ self.ctx.vtabName(), ShadowTableDef.suffix });

            const stmt = try self.ctx.conn().prepare(query);
            defer stmt.deinit();
            const exists = try stmt.next();
            if (!exists) {
                return Error.ShadowTableDoesNotExist;
            }
        }

        pub fn checkExists(self: Self, tmp_arena: *ArenaAllocator) !bool {
            self.verifyExists(tmp_arena) catch |e| {
                if (e == Error.ShadowTableDoesNotExist) {
                    return false;
                }
                return e;
            };
            return true;
        }

        pub fn drop(self: Self, tmp_arena: *ArenaAllocator) !void {
            const ddl = try fmt.allocPrintZ(
                tmp_arena.allocator(),
                \\DROP TABLE "{s}_{s}"
            ,
                .{ self.ctx.vtabName(), ShadowTableDef.suffix },
            );
            try self.ctx.conn().exec(ddl);
        }
    };
}
