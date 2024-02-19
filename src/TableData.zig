const std = @import("std");
const fmt = std.fmt;
const testing = std.testing;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const sqlite = @import("sqlite3.zig");
const Conn = sqlite.Conn;

const prep_stmt = @import("prepared_stmt.zig");
const sql_fmt = @import("sql_fmt.zig");
const MakeShadowTable = @import("shadow_table.zig").ShadowTable;
const VtabCtxSchemaless = @import("ctx.zig").VtabCtxSchemaless;

ctx: *const VtabCtxSchemaless,

read_data: StmtCell,
write_data: StmtCell,

const Self = @This();

const StmtCell = prep_stmt.Cell(VtabCtxSchemaless);

pub const Key = enum(u8) {
    next_rowid = 1,
};

pub fn init(ctx: *const VtabCtxSchemaless) Self {
    return .{
        .ctx = ctx,
        .read_data = StmtCell.init(&readQuery),
        .write_data = StmtCell.init(&writeDml),
    };
}

pub fn deinit(self: *Self) void {
    self.read_data.deinit();
    self.write_data.deinit();
}

pub const ShadowTable = MakeShadowTable(VtabCtxSchemaless, struct {
    pub const suffix: []const u8 = "tabledata";

    pub fn createTableDdl(ctx: VtabCtxSchemaless, allocator: Allocator) ![:0]const u8 {
        return fmt.allocPrintZ(allocator,
            \\CREATE TABLE "{s}_tabledata" (
            \\  key INTEGER NOT NULL,
            \\  value ANY NOT NULL,
            \\  PRIMARY KEY (key)
            \\)
        , .{ctx.vtabName()});
    }
});

pub fn table(self: Self) ShadowTable {
    return .{ .ctx = self.ctx };
}

pub fn readInt(self: *Self, tmp_arena: *ArenaAllocator, key: Key) !?i64 {
    const stmt = try self.read_data.acquire(tmp_arena, self.ctx.*);
    defer self.read_data.release();

    try stmt.bind(.Int64, 1, @intFromEnum(key));

    const has_value = try stmt.next();
    if (!has_value) {
        return null;
    }

    return stmt.read(.Int64, false, 0);
}

fn readQuery(ctx: VtabCtxSchemaless, arena: *ArenaAllocator) ![]const u8 {
    return fmt.allocPrintZ(arena.allocator(),
        \\SELECT value
        \\FROM "{s}_tabledata"
        \\WHERE key = ?
    , .{ctx.vtabName()});
}

pub fn writeInt(self: *Self, tmp_arena: *ArenaAllocator, key: Key, value: i64) !void {
    const stmt = try self.write_data.acquire(tmp_arena, self.ctx.*);
    defer self.write_data.release();

    try stmt.bind(.Int64, 1, @intFromEnum(key));
    try stmt.bind(.Int64, 2, value);

    try stmt.exec();
}

fn writeDml(ctx: VtabCtxSchemaless, arena: *ArenaAllocator) ![]const u8 {
    return fmt.allocPrintZ(arena.allocator(),
        \\INSERT INTO "{s}_tabledata" (key, value)
        \\VALUES (?, ?)
        \\ON CONFLICT (key) DO UPDATE SET value = excluded.value
    , .{ctx.vtabName()});
}
