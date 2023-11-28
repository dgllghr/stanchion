const std = @import("std");
const fmt = std.fmt;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;
const ArrayListUnmanaged = std.ArrayListUnmanaged;

const Conn = @import("../sqlite3/Conn.zig");
const Stmt = @import("../sqlite3/Stmt.zig");

const stmt_cell = @import("../stmt_cell.zig");

const Column = @import("./Column.zig");
const ColumnType = @import("./ColumnType.zig");

const Self = @This();

const StmtCell = stmt_cell.StmtCell(Self);

conn: Conn,
vtab_table_name: []const u8,
load_columns: StmtCell,
create_column: StmtCell,

pub fn createTable(tmp_arena: *ArenaAllocator, conn: Conn, vtab_table_name: []const u8) !void {
    const query = try fmt.allocPrintZ(
        tmp_arena.allocator(),
        \\CREATE TABLE "{s}_columns" (
        \\  rank INTEGER NOT NULL,
        \\  name TEXT NOT NULL COLLATE NOCASE,
        \\  column_type TEXT NOT NULL,
        \\  sk_rank INTEGER NULL,
        \\  PRIMARY KEY (rank),
        \\  UNIQUE (name)
        \\) STRICT, WITHOUT ROWID
    ,
        .{vtab_table_name},
    );
    try conn.exec(query);
}

pub fn init(conn: Conn, vtab_table_name: []const u8) Self {
    return .{
        .conn = conn,
        .vtab_table_name = vtab_table_name,
        .load_columns = StmtCell.init(&loadColumnsQuery),
        .create_column = StmtCell.init(&createColumnDml),
    };
}

pub fn deinit(self: *Self) void {
    self.load_columns.deinit();
    self.create_column.deinit();
}

pub fn dropTable(self: *Self, tmp_arena: *ArenaAllocator) !void {
    const query = try fmt.allocPrintZ(tmp_arena.allocator(),
        \\DROP TABLE "{s}_columns"
    , .{self.vtab_table_name});
    try self.conn.exec(query);
}

fn loadColumnsQuery(self: *const Self, arena: *ArenaAllocator) ![]const u8 {
    return fmt.allocPrintZ(arena.allocator(),
        \\SELECT rank, name, column_type, sk_rank
        \\FROM "{s}_columns"
        \\ORDER BY rank
    , .{self.vtab_table_name});
}

pub fn loadColumns(
    self: *Self,
    allocator: Allocator,
    tmp_arena: *ArenaAllocator,
    columns: *ArrayListUnmanaged(Column),
    sort_key_len: *usize,
) !void {
    const stmt = try self.load_columns.getStmt(tmp_arena, self);
    defer self.load_columns.reset();

    sort_key_len.* = 0;
    while (try stmt.next()) {
        const col = try readColumn(allocator, stmt);
        try columns.append(allocator, col);
        if (col.sk_rank) |_| {
            sort_key_len.* += 1;
        }
    }
}

fn readColumn(allocator: Allocator, stmt: Stmt) !Column {
    const rank = stmt.read(.Int32, false, 0);
    const name = try allocator.dupe(u8, stmt.read(.Text, false, 1));
    const column_type = ColumnType.read(stmt.read(.Text, false, 2));
    const sk_rank = stmt.read(.Int32, true, 3);
    return .{
        .rank = rank,
        .name = name,
        .column_type = column_type,
        .sk_rank = if (sk_rank) |r| @intCast(r) else null,
    };
}

fn createColumnDml(self: *const Self, arena: *ArenaAllocator) ![]const u8 {
    return fmt.allocPrintZ(arena.allocator(),
        \\INSERT INTO "{s}_columns" (
        \\  rank, name, column_type, sk_rank)
        \\VALUES (?, ?, ?, ?)
    , .{self.vtab_table_name});
}

pub fn createColumn(
    self: *Self,
    tmp_arena: *ArenaAllocator,
    column: *const Column,
) !void {
    const stmt = try self.create_column.getStmt(tmp_arena, self);
    defer self.create_column.reset();

    try stmt.bind(.Int32, 1, column.rank);
    try stmt.bind(.Text, 2, column.name);
    const column_type = try fmt.allocPrint(tmp_arena.allocator(), "{s}", .{column.column_type});
    try stmt.bind(.Text, 3, column_type);
    const sk_rank: ?i32 = if (column.sk_rank) |r| @intCast(r) else null;
    try stmt.bind(.Int32, 4, sk_rank);
    try stmt.exec();
}

test "create column" {
    const conn = try Conn.openInMemory();

    var arena = ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var column = Column{
        .rank = 0,
        .name = "first_col",
        .column_type = .{ .data_type = .Integer, .nullable = false },
        .sk_rank = 0,
    };
    try Self.createTable(&arena, conn, "test");
    var db = Self.init(conn, "test");

    try db.createColumn(&arena, &column);
}
