const std = @import("std");
const fmt = std.fmt;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;
const ArrayListUnmanaged = std.ArrayListUnmanaged;

const sqlite = @import("../sqlite3.zig");
const Conn = sqlite.Conn;
const Stmt = sqlite.Stmt;

const stmt_cell = @import("../stmt_cell.zig");

const Column = @import("Column.zig");
const ColumnType = @import("ColumnType.zig");
const Schema = @import("Schema.zig");
const SchemaDef = @import("SchemaDef.zig");

conn: Conn,
vtab_table_name: []const u8,
load_columns: StmtCell,
create_column: StmtCell,

const Self = @This();

const StmtCell = stmt_cell.StmtCell(Self);

pub const Error = error{ SortKeyColumnNotFound, ExecReturnedData };

pub fn init(tmp_arena: *ArenaAllocator, conn: Conn, vtab_table_name: []const u8) !Self {
    try setup(tmp_arena, conn, vtab_table_name);

    return .{
        .conn = conn,
        .vtab_table_name = vtab_table_name,
        .load_columns = StmtCell.init(&loadColumnsQuery),
        .create_column = StmtCell.init(&createColumnDml),
    };
}

fn setup(tmp_arena: *ArenaAllocator, conn: Conn, vtab_table_name: []const u8) !void {
    const query = try fmt.allocPrintZ(tmp_arena.allocator(),
        \\CREATE TABLE IF NOT EXISTS "{s}_columns" (
        \\  rank INTEGER NOT NULL,
        \\  name TEXT NOT NULL COLLATE NOCASE,
        \\  column_type TEXT NOT NULL,
        \\  sk_rank INTEGER NULL,
        \\  PRIMARY KEY (rank),
        \\  UNIQUE (name)
        \\) STRICT, WITHOUT ROWID
    , .{vtab_table_name});
    try conn.exec(query);
}

pub fn deinit(self: *Self) void {
    self.load_columns.deinit();
    self.create_column.deinit();
}

pub fn destroy(self: *Self, tmp_arena: *ArenaAllocator) !void {
    const query = try fmt.allocPrintZ(tmp_arena.allocator(),
        \\DROP TABLE "{s}_columns"
    , .{self.vtab_table_name});
    try self.conn.exec(query);
}

pub fn load(self: *Self, table_static_arena: *ArenaAllocator, tmp_arena: *ArenaAllocator) !Schema {
    const stmt = try self.load_columns.getStmt(tmp_arena, self);
    defer self.load_columns.reset();

    var columns = ArrayListUnmanaged(Column){};
    var sort_key_len: usize = 0;
    while (try stmt.next()) {
        const col = try readColumn(table_static_arena.allocator(), stmt);
        try columns.append(tmp_arena.allocator(), col);
        if (col.sk_rank) |_| {
            sort_key_len += 1;
        }
    }

    var sort_key = try table_static_arena.allocator().alloc(usize, sort_key_len);
    for (columns.items, 0..) |*col, col_rank| {
        if (col.sk_rank) |sk_rank| {
            sort_key[sk_rank] = col_rank;
        }
    }

    return .{
        .columns = try columns.toOwnedSlice(table_static_arena.allocator()),
        .sort_key = sort_key,
    };
}

fn loadColumnsQuery(self: *const Self, arena: *ArenaAllocator) ![]const u8 {
    return fmt.allocPrintZ(arena.allocator(),
        \\SELECT rank, name, column_type, sk_rank
        \\FROM "{s}_columns"
        \\ORDER BY rank
    , .{self.vtab_table_name});
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

pub fn create(
    self: *Self,
    table_static_arena: *ArenaAllocator,
    tmp_arena: *ArenaAllocator,
    def: *const SchemaDef,
) !Schema {
    // Validate the sort key
    var sort_key = try table_static_arena.allocator().alloc(usize, def.sort_key.items.len);
    sk: for (def.sort_key.items, 0..) |name, sk_idx| {
        for (def.columns.items, 0..) |col, rank| {
            // TODO support unicode?
            if (std.ascii.eqlIgnoreCase(name, col.name)) {
                sort_key[sk_idx] = rank;
                continue :sk;
            }
        }
        return Error.SortKeyColumnNotFound;
    }

    var columns = try table_static_arena.allocator().alloc(Column, def.columns.items.len);
    for (def.columns.items, 0..) |col_def, rank| {
        const col_name = try table_static_arena.allocator().dupe(u8, col_def.name);
        var sk_rank: ?u16 = null;
        for (sort_key, 0..) |sr, r| {
            if (rank == sr) {
                sk_rank = @intCast(r);
                break;
            }
        }
        const col: Column = .{
            .rank = @intCast(rank),
            .name = col_name,
            .column_type = col_def.column_type,
            .sk_rank = sk_rank,
        };

        try self.createColumn(tmp_arena, &col);

        columns[rank] = col;
    }

    return .{
        .columns = columns,
        .sort_key = sort_key,
    };
}

pub fn createColumn(self: *Self, tmp_arena: *ArenaAllocator, column: *const Column) !void {
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

fn createColumnDml(self: *const Self, arena: *ArenaAllocator) ![]const u8 {
    return fmt.allocPrintZ(arena.allocator(),
        \\INSERT INTO "{s}_columns" (
        \\  rank, name, column_type, sk_rank)
        \\VALUES (?, ?, ?, ?)
    , .{self.vtab_table_name});
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

    var mgr = try Self.init(&arena, conn, "test");
    try mgr.createColumn(&arena, &column);
}
