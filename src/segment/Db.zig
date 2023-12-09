const std = @import("std");
const fmt = std.fmt;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const sqlite = @import("../sqlite3.zig");
const Blob = sqlite.Blob;
const Conn = sqlite.Conn;
const Stmt = sqlite.Stmt;

const stmt_cell = @import("../stmt_cell.zig");

const Self = @This();

const StmtCell = stmt_cell.StmtCell(Self);

conn: Conn,
table_name: [:0]const u8,
insert_segment: StmtCell,
delete_segment: StmtCell,

pub const Handle = struct {
    id: i64,
    blob: Blob,

    pub fn close(self: *@This()) void {
        self.blob.close() catch |e| {
            std.log.err("error closing segment blob {d}: {any}", .{ self.id, e });
        };
    }
};

const segment_column_name = "segment";

pub fn createTable(tmp_arena: *ArenaAllocator, conn: Conn, vtab_table_name: []const u8) !void {
    const query = try fmt.allocPrintZ(
        tmp_arena.allocator(),
        \\CREATE TABLE "{s}_segments" (
        \\  id INTEGER NOT NULL PRIMARY KEY,
        \\  segment BLOB NOT NULL
        \\) STRICT
    ,
        .{vtab_table_name},
    );
    try conn.exec(query);
}

pub fn init(arena: *ArenaAllocator, conn: Conn, vtab_table_name: []const u8) !Self {
    const table_name = try fmt.allocPrintZ(
        arena.allocator(),
        "{s}_segments",
        .{vtab_table_name},
    );
    return .{
        .conn = conn,
        .table_name = table_name,
        .insert_segment = StmtCell.init(&insertSegmentDml),
        .delete_segment = StmtCell.init(&deleteSegmentDml),
    };
}

pub fn deinit(self: *Self) void {
    self.insert_segment.deinit();
    self.delete_segment.deinit();
}

pub fn dropTable(self: *Self, tmp_arena: *ArenaAllocator) !void {
    const query = try fmt.allocPrintZ(
        tmp_arena.allocator(),
        \\DROP TABLE "{s}"
    ,
        .{self.table_name},
    );
    try self.conn.exec(query);
}

fn insertSegmentDml(self: *const Self, arena: *ArenaAllocator) ![]const u8 {
    return fmt.allocPrintZ(arena.allocator(),
        \\INSERT INTO "{s}" (segment)
        \\VALUES (ZEROBLOB(?))
        \\RETURNING id
    , .{self.table_name});
}

pub fn allocate(self: *Self, tmp_arena: *ArenaAllocator, size: usize) !Handle {
    const stmt = try self.insert_segment.getStmt(tmp_arena, self);
    defer self.insert_segment.reset();

    try stmt.bind(.Int64, 1, @as(i64, @intCast(size)));
    _ = try stmt.next();
    const id = stmt.read(.Int64, false, 0);

    const blob = try Blob.open(
        self.conn,
        self.table_name,
        segment_column_name,
        id,
    );
    return .{ .id = id, .blob = blob };
}

pub fn open(self: *Self, id: i64) !Handle {
    const blob = try Blob.open(
        self.conn,
        self.table_name,
        segment_column_name,
        id,
    );
    return .{
        .id = id,
        .blob = blob,
    };
}

fn deleteSegmentDml(self: *const Self, arena: *ArenaAllocator) ![]const u8 {
    return fmt.allocPrintZ(arena.allocator(),
        \\DELETE FROM "{s}"
        \\WHERE id = ?
    , .{self.table_name});
}

pub fn free(self: *Self, tmp_arena: *ArenaAllocator, handle: Handle) !void {
    const stmt = try self.delete_segment.getStmt(tmp_arena, self);
    defer self.delete_segment.reset();

    try stmt.bind(.Int64, 1, @as(i64, @intCast(handle.id)));
    try stmt.exec();
}
