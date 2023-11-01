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
load_columns: StmtCell,
create_column: StmtCell,

pub fn init(conn: Conn) Self {
    return .{
        .conn = conn,
        .load_columns = StmtCell.init(&loadColumnsQuery),
        .create_column = StmtCell.init(&createColumnDml),
    };
}

pub fn deinit(self: *Self) void {
    self.load_columns.deinit();
    self.create_column.deinit();
}

fn loadColumnsQuery(_: *const Self, _: *ArenaAllocator) ![]const u8 {
    return
        \\SELECT rank, name, column_type, sk_rank
        \\FROM _stanchion_columns
        \\WHERE table_id = ?
        ;
}

pub fn loadColumns(
    self: *Self,
    allocator: Allocator,
    tmp_arena: *ArenaAllocator,
    table_id: i64,
    columns: *ArrayListUnmanaged(Column),
    sort_key_len: *usize,
) !void {
    const stmt = try self.load_columns.getStmt(tmp_arena, self);
    defer self.load_columns.reset();

    sort_key_len.* = 0;
    try stmt.bind(.Int64, 1, table_id);
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

fn createColumnDml(_: *const Self, _: *ArenaAllocator) ![]const u8 {
    return
        \\INSERT INTO _stanchion_columns (
        \\  table_id, rank, name, column_type, sk_rank)
        \\VALUES (?, ?, ?, ?, ?)
        ;
}

pub fn createColumn(
    self: *Self,
    allocator: Allocator,
    tmp_arena: *ArenaAllocator,
    table_id: i64,
    column: *const Column,
) !void {
    const stmt = try self.create_column.getStmt(tmp_arena, self);
    defer self.create_column.reset();

    try stmt.bind(.Int64, 1, table_id);
    try stmt.bind(.Int32, 2, column.rank);
    try stmt.bind(.Text, 3, column.name);
    const column_type = try fmt.allocPrint(allocator, "{s}", .{column.column_type});
    defer allocator.free(column_type);
    try stmt.bind(.Text, 4, column_type);
    const sk_rank: ?i32 = if (column.sk_rank) |r| @intCast(r) else null;
    try stmt.bind(.Int32, 5, sk_rank);
    try stmt.exec();
}

test "create column" {
    const conn = try Conn.openInMemory();
    try @import("../Db.zig").Migrations.apply(conn);

    var column = Column{
        .rank = 0,
        .name = "first_col",
        .column_type = .{ .data_type = .Integer, .nullable = false },
        .sk_rank = 0,
    };
    var db = Self.init(conn);

    var arena = ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    try db.createColumn(arena.allocator(), &arena, 100, &column);
}
