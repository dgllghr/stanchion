const std = @import("std");
const fmt = std.fmt;
const testing = std.testing;
const ArenaAllocator = std.heap.ArenaAllocator;

const sqlite = @import("sqlite3.zig");
const Conn = sqlite.Conn;

const prep_stmt = @import("prepared_stmt.zig");
const sql_fmt = @import("sql_fmt.zig");

conn: Conn,
vtab_table_name: []const u8,

read_data: StmtCell,
write_data: StmtCell,

const Self = @This();

const StmtCell = prep_stmt.Cell(Self);

pub const Key = enum(u8) {
    next_rowid = 1,
};

pub fn create(tmp_arena: *ArenaAllocator, conn: Conn, vtab_table_name: []const u8) !Self {
    const ddl = try fmt.allocPrintZ(tmp_arena.allocator(),
        \\CREATE TABLE "{s}_tabledata" (
        \\  key INTEGER NOT NULL,
        \\  value ANY NOT NULL,
        \\  PRIMARY KEY (key)
        \\)
    , .{vtab_table_name});
    try conn.exec(ddl);

    return .{
        .conn = conn,
        .vtab_table_name = vtab_table_name,
        .read_data = StmtCell.init(&readQuery),
        .write_data = StmtCell.init(&writeDml),
    };
}

pub fn open(conn: Conn, vtab_table_name: []const u8) !Self {
    return .{
        .conn = conn,
        .vtab_table_name = vtab_table_name,
        .read_data = StmtCell.init(&readQuery),
        .write_data = StmtCell.init(&writeDml),
    };
}

pub fn deinit(self: *Self) void {
    self.read_data.deinit();
    self.write_data.deinit();
}

pub fn drop(self: *Self, tmp_arena: *ArenaAllocator) !void {
    const query = try fmt.allocPrintZ(
        tmp_arena.allocator(),
        \\DROP TABLE "{s}_tabledata"
    ,
        .{self.vtab_table_name},
    );
    try self.conn.exec(query);
}

pub fn readInt(self: *Self, tmp_arena: *ArenaAllocator, key: Key) !?i64 {
    const stmt = try self.read_data.acquire(tmp_arena, self);
    defer self.read_data.release();

    try stmt.bind(.Int64, 1, @intFromEnum(key));

    const has_value = try stmt.next();
    if (!has_value) {
        return null;
    }

    return stmt.read(.Int64, false, 0);
}

fn readQuery(self: *const Self, arena: *ArenaAllocator) ![]const u8 {
    return fmt.allocPrintZ(arena.allocator(),
        \\SELECT value
        \\FROM "{s}_tabledata"
        \\WHERE key = ?
    , .{self.vtab_table_name});
}

pub fn writeInt(self: *Self, tmp_arena: *ArenaAllocator, key: Key, value: i64) !void {
    const stmt = try self.write_data.acquire(tmp_arena, self);
    defer self.write_data.release();

    try stmt.bind(.Int64, 1, @intFromEnum(key));
    try stmt.bind(.Int64, 2, value);

    try stmt.exec();
}

fn writeDml(self: *const Self, arena: *ArenaAllocator) ![]const u8 {
    return fmt.allocPrintZ(arena.allocator(),
        \\INSERT INTO "{s}_tabledata" (key, value)
        \\VALUES (?, ?)
        \\ON CONFLICT DO UPDATE SET value = excluded.value
    , .{self.vtab_table_name});
}
