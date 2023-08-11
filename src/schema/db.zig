const std = @import("std");
const fmt = std.fmt;
const Allocator = std.mem.Allocator;
const ArrayListUnmanaged = std.ArrayListUnmanaged;

const Conn = @import("../sqlite3/Conn.zig");
const Stmt = @import("../sqlite3/Stmt.zig");

const Column = @import("./Column.zig");
const ColumnType = @import("./ColumnType.zig");

const Self = @This();

conn: Conn,
load_columns: ?Stmt,
create_column: ?Stmt,

pub fn init(conn: Conn) Self {
    return .{
        .conn = conn,
        .load_columns = null,
        .create_column = null,
    };
}

pub fn deinit(self: *Self) void {
    if (self.load_columns) |stmt| {
        stmt.deinit();
    }
    if (self.create_column) |stmt| {
        stmt.deinit();
    }
}

pub fn loadColumns(
    self: *Self,
    allocator: Allocator,
    table_id: i64,
    columns: *ArrayListUnmanaged(Column),
    sort_key_len: *usize,
) !void {
    if (self.load_columns == null) {
        self.load_columns = try self.conn.prepare(
            \\SELECT rank, name, column_type, sk_rank
            \\FROM _stanchion_columns
            \\WHERE table_id = ?
        );
    }

    sort_key_len.* = 0;
    const stmt = self.load_columns.?;
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

pub fn createColumn(
    self: *Self,
    allocator: Allocator,
    table_id: i64,
    column: *const Column,
) !void {
    if (self.create_column == null) {
        self.create_column = try self.conn.prepare(
            \\INSERT INTO _stanchion_columns (
            \\  table_id, rank, name, column_type, sk_rank)
            \\VALUES (?, ?, ?, ?, ?)
        );
    }

    const stmt = self.create_column.?;
    try stmt.bind(.Int64, 1, table_id);
    try stmt.bind(.Int32, 2, column.rank);
    try stmt.bind(.Text, 3, column.name);
    const column_type = try fmt.allocPrint(allocator, "{s}", .{column.column_type});
    defer allocator.free(column_type);
    try stmt.bind(.Text, 4, column_type);
    const sk_rank: ?i32 = if (column.sk_rank) |r| @intCast(r) else null;
    try stmt.bind(.Int32, 5, sk_rank);
    try stmt.exec();
    try stmt.reset();
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
    try db.createColumn(std.testing.allocator, 100, &column);
}
