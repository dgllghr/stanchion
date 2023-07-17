const std = @import("std");
const fmt = std.fmt;
const Allocator = std.mem.Allocator;
const ArrayListUnmanaged = std.ArrayListUnmanaged;

const Stmt = @import("../sqlite3/Stmt.zig");

const Column = @import("./Column.zig");
const ColumnType = @import("./ColumnType.zig");

pub const db_ctx_fields = [_][]const u8 {
    "create_column_stmt",
    "load_column_stmt",
};

pub fn loadColumns(
    allocator: Allocator,
    db_ctx: anytype,
    table_id: i64,
    columns: *ArrayListUnmanaged(Column),
    sort_key_len: *usize,
) !void {
    if (db_ctx.load_column_stmt == null) {
        db_ctx.load_column_stmt = try db_ctx.conn.prepare(
            \\SELECT rank, name, column_type, sk_rank
            \\FROM _stanchion_columns
            \\WHERE table_id = ?
        );
    }

    sort_key_len.* = 0;
    const stmt  = db_ctx.load_column_stmt.?;
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
    allocator: Allocator,
    db_ctx: anytype,
    table_id: i64,
    column: *const Column,
) !void {
    if (db_ctx.create_column_stmt == null) {
        db_ctx.create_column_stmt = try db_ctx.conn.prepare(
            \\INSERT INTO _stanchion_columns (
            \\  table_id, rank, name, column_type, sk_rank)
            \\VALUES (?, ?, ?, ?, ?)
        );
    }

    const stmt  = db_ctx.create_column_stmt.?;
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
    const DbCtx = @import("../db.zig").Ctx;

    const conn = try @import("../sqlite3/Conn.zig").openInMemory();
    try conn.exec(
        \\CREATE TABLE _stanchion_columns (
        \\  table_id INTEGER NOT NULL,
        \\  rank INTEGER NOT NULL,
        \\  name TEXT NOT NULL,
        \\  column_type TEXT NOT NULL,
        \\  sk_rank INTEGER NULL,
        \\  PRIMARY KEY (table_id, rank)
        \\)
    );

    var db_ctx = DbCtx(&db_ctx_fields) { .conn = conn, };

    var column = Column {
        .rank = 0,
        .name = "first_col",
        .column_type = .{ .data_type = .Integer, .nullable = false },
        .sk_rank = 0,
    };
    try createColumn(std.testing.allocator, &db_ctx, 100, &column);
}