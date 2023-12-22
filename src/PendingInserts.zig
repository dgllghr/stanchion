const std = @import("std");
const fmt = std.fmt;
const testing = std.testing;
const ArenaAllocator = std.heap.ArenaAllocator;

const sqlite = @import("sqlite3.zig");
const Conn = sqlite.Conn;
const Stmt = sqlite.Stmt;
const ValueRef = sqlite.ValueRef;

const stmt_cell = @import("stmt_cell.zig");

const schema_mod = @import("schema.zig");
const Column = schema_mod.Column;
const ColumnType = schema_mod.ColumnType;
const DataType = schema_mod.ColumnType.DataType;
const Schema = schema_mod.Schema;

const TableData = @import("TableData.zig");
const CursorRange = @import("CursorRange.zig");

const sql_fmt = @import("sql_fmt.zig");

conn: Conn,
vtab_table_name: []const u8,
sort_key: []const usize,
columns_len: usize,

table_data: *TableData,
next_rowid: i64,

insert_stmt: StmtCell,
cursor_from_key: StmtCell,
delete_from: StmtCell,
delete_range: StmtCell,

const Self = @This();

const StmtCell = stmt_cell.StmtCell(Self);

pub fn create(
    tmp_arena: *ArenaAllocator,
    conn: Conn,
    vtab_table_name: []const u8,
    schema: *const Schema,
    table_data: *TableData,
) !Self {
    const ddl_formatter = CreateTableDdlFormatter{
        .vtab_table_name = vtab_table_name,
        .schema = schema,
    };
    const ddl = try fmt.allocPrintZ(tmp_arena.allocator(), "{s}", .{ddl_formatter});
    try conn.exec(ddl);

    return .{
        .conn = conn,
        .vtab_table_name = vtab_table_name,
        .sort_key = schema.sort_key,
        .columns_len = schema.columns.len,
        .table_data = table_data,
        .next_rowid = 1,
        .insert_stmt = StmtCell.init(&insertDml),
        .cursor_from_key = StmtCell.init(&cursorFromKeyQuery),
        .delete_from = StmtCell.init(&deleteFromQuery),
        .delete_range = StmtCell.init(&deleteRangeQuery),
    };
}

const CreateTableDdlFormatter = struct {
    vtab_table_name: []const u8,
    schema: *const Schema,

    pub fn format(
        self: @This(),
        comptime _: []const u8,
        _: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        try writer.print(
            \\CREATE TABLE "{s}_pendinginserts" (
            \\rowid INTEGER NOT NULL,
        ,
            .{self.vtab_table_name},
        );
        for (self.schema.columns, 0..) |*col, rank| {
            const col_type = ColumnType.SqliteFormatter{
                .column_type = col.column_type,
            };
            try writer.print("col_{d} {},", .{ rank, col_type });
        }
        try writer.print(
            \\PRIMARY KEY (
        , .{});
        for (self.schema.sort_key) |col_rank| {
            try writer.print("col_{d},", .{col_rank});
        }
        try writer.print("rowid)", .{});
        try writer.print(") STRICT, WITHOUT ROWID", .{});
    }
};

test "pending inserts: format create table ddl" {
    const allocator = testing.allocator;
    const datasets = @import("testing/datasets.zig");

    const schema = datasets.planets.schema;

    const formatter = CreateTableDdlFormatter{ .vtab_table_name = "planets", .schema = &schema };
    const ddl = try fmt.allocPrintZ(allocator, "{}", .{formatter});
    defer allocator.free(ddl);

    const conn = try @import("sqlite3/Conn.zig").openInMemory();
    defer conn.close();

    conn.exec(ddl) catch |e| {
        std.log.err("sqlite error: {s}", .{conn.lastErrMsg()});
        return e;
    };
}

pub fn open(
    tmp_arena: *ArenaAllocator,
    conn: Conn,
    vtab_table_name: []const u8,
    schema: *const Schema,
    table_data: *TableData,
) !Self {
    var self = Self{
        .conn = conn,
        .vtab_table_name = vtab_table_name,
        .sort_key = schema.sort_key,
        .columns_len = schema.columns.len,
        .table_data = table_data,
        .next_rowid = 0,
        .insert_stmt = StmtCell.init(&insertDml),
        .cursor_from_key = StmtCell.init(&cursorFromKeyQuery),
        .delete_from = StmtCell.init(&deleteFromQuery),
        .delete_range = StmtCell.init(&deleteRangeQuery),
    };
    try self.loadNextRowid(tmp_arena);
    return self;
}

pub fn deinit(self: *Self) void {
    self.insert_stmt.deinit();
    self.cursor_from_key.deinit();
    self.delete_from.deinit();
    self.delete_range.deinit();
}

pub fn drop(self: *Self, tmp_arena: *ArenaAllocator) !void {
    const query = try fmt.allocPrintZ(
        tmp_arena.allocator(),
        \\DROP TABLE "{s}_pendinginserts"
    ,
        .{self.vtab_table_name},
    );
    try self.conn.exec(query);
}

pub fn insert(self: *Self, tmp_arena: *ArenaAllocator, values: anytype) !i64 {
    const rowid = self.next_rowid;
    self.next_rowid += 1;

    const stmt = try self.insert_stmt.getStmt(tmp_arena, self);
    defer self.insert_stmt.reset();

    try stmt.bind(.Int64, 1, rowid);
    for (0..self.columns_len) |idx| {
        const value = try values.readValue(idx);
        try value.bind(stmt, idx + 2);
    }

    try stmt.exec();

    return rowid;
}

fn insertDml(self: *const Self, arena: *ArenaAllocator) ![]const u8 {
    return fmt.allocPrintZ(arena.allocator(),
        \\INSERT INTO "{s}_pendinginserts" (rowid, {s})
        \\VALUES (?, {s})
    , .{
        self.vtab_table_name,
        sql_fmt.ColumnListLenFormatter("col_{d}"){ .len = self.columns_len },
        sql_fmt.ParameterListFormatter{ .len = self.columns_len },
    });
}

test "pending inserts: insert dml" {
    const table_name = "test";

    var arena = ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const conn = try Conn.openInMemory();
    defer conn.close();

    const self = Self{
        .conn = conn,
        .vtab_table_name = table_name,
        .sort_key = undefined,
        .columns_len = 5,
        .table_data = undefined,
        .next_rowid = undefined,
        .insert_stmt = undefined,
        .cursor_from_key = undefined,
        .delete_from = undefined,
        .delete_range = undefined,
    };

    const expected =
        \\INSERT INTO "test_pendinginserts" (rowid, col_0, col_1, col_2, col_3, col_4)
        \\VALUES (?, ?, ?, ?, ?, ?)
    ;
    const result = try self.insertDml(&arena);
    try testing.expectEqualSlices(u8, expected, result);
}

pub fn persistNextRowid(self: *Self, tmp_arena: *ArenaAllocator) !void {
    try self.table_data.writeInt(tmp_arena, .next_rowid, self.next_rowid);
}

pub fn loadNextRowid(self: *Self, tmp_arena: *ArenaAllocator) !void {
    self.next_rowid = (try self.table_data.readInt(tmp_arena, .next_rowid)) orelse 1;
}

pub const Cursor = struct {
    stmt: Stmt,
    cell: ?*StmtCell,
    is_eof: bool,

    pub fn deinit(self: *Cursor) void {
        if (self.cell) |*cell| {
            cell.*.reset();
        } else {
            self.stmt.deinit();
        }
    }

    pub fn next(self: *Cursor) !void {
        self.is_eof = !(try self.stmt.next());
    }

    pub fn eof(self: Cursor) bool {
        return self.is_eof;
    }

    pub fn rewind(self: *Cursor) !void {
        try self.stmt.resetExec();
        self.is_eof = !(try self.stmt.next());
    }

    pub fn readRowid(self: Cursor) !ValueRef {
        return self.stmt.readSqliteValue(0);
    }

    pub fn readValue(self: Cursor, col_idx: usize) !ValueRef {
        return self.stmt.readSqliteValue(col_idx + 1);
    }

    pub const SortKey = struct {
        stmt: Stmt,
        sort_key: []const usize,

        pub fn readValue(self: SortKey, index: usize) !ValueRef {
            const col_idx = self.sort_key[index];
            return self.stmt.readSqliteValue(col_idx + 1);
        }
    };

    pub fn sortKey(self: Cursor, sort_key: []const usize) SortKey {
        return .{ .stmt = self.stmt, .sort_key = sort_key };
    }
};

pub fn cursor(self: *Self, tmp_arena: *ArenaAllocator) !Cursor {
    const query = try self.cursorFromStartQuery(tmp_arena);
    const stmt = try self.conn.prepare(query);

    const eof = !(try stmt.next());

    return .{
        .stmt = stmt,
        .cell = null,
        .is_eof = eof,
    };
}

fn cursorFromStartQuery(self: *const Self, arena: *ArenaAllocator) ![]const u8 {
    return fmt.allocPrintZ(arena.allocator(),
        \\SELECT rowid, {}
        \\FROM "{s}_pendinginserts"
        \\ORDER BY {}, rowid
    , .{
        sql_fmt.ColumnListLenFormatter("col_{d}"){ .len = self.columns_len },
        self.vtab_table_name,
        sql_fmt.ColumnListIndicesFormatter("col_{d}"){ .indices = self.sort_key },
    });
}

/// Only 1 pending inserts cursor can be open at a time
/// TODO use a pool of stmts to allow for multiple cursors at once
pub fn cursorFrom(
    self: *Self,
    tmp_arena: *ArenaAllocator,
    start_sort_key: anytype,
    start_rowid: i64,
) !Cursor {
    const stmt = try self.cursor_from_key.getStmt(tmp_arena, self);

    for (0..self.sort_key.len) |idx| {
        const value = try start_sort_key.readValue(idx);
        try value.bind(stmt, idx + 1);
    }
    try stmt.bind(.Int64, self.sort_key.len + 1, start_rowid);

    const eof = !(try stmt.next());

    return .{
        .stmt = stmt,
        .cell = &self.cursor_from_key,
        .is_eof = eof,
    };
}

fn cursorFromKeyQuery(self: *const Self, arena: *ArenaAllocator) ![]const u8 {
    return fmt.allocPrintZ(arena.allocator(),
        \\SELECT rowid, {}
        \\FROM "{s}_pendinginserts"
        \\WHERE ({}, rowid) >= ({}, ?)
        \\ORDER BY {}, rowid
    , .{
        sql_fmt.ColumnListLenFormatter("col_{d}"){ .len = self.columns_len },
        self.vtab_table_name,
        sql_fmt.ColumnListIndicesFormatter("col_{d}"){ .indices = self.sort_key },
        sql_fmt.ParameterListFormatter{ .len = self.sort_key.len },
        sql_fmt.ColumnListIndicesFormatter("col_{d}"){ .indices = self.sort_key },
    });
}

pub fn cursorPartial(self: *Self, tmp_arena: *ArenaAllocator, range: CursorRange) !Cursor {
    const query = try self.cursorPartialQuery(tmp_arena, range);
    const stmt = try self.conn.prepare(query);

    for (0..range.key.valuesLen()) |idx| {
        const value = try range.key.readValue(idx);
        try value.bind(stmt, idx + 1);
    }

    const eof = !(try stmt.next());

    return .{
        .stmt = stmt,
        .cell = null,
        .is_eof = eof,
    };
}

fn cursorPartialQuery(self: *const Self, arena: *ArenaAllocator, range: CursorRange) ![]const u8 {
    const key_len = range.key.valuesLen();
    return fmt.allocPrintZ(arena.allocator(),
        \\SELECT rowid, {}
        \\FROM "{s}_pendinginserts"
        \\WHERE ({}) {s} ({})
        \\ORDER BY {}, rowid
    , .{
        sql_fmt.ColumnListLenFormatter("col_{d}"){ .len = self.columns_len },
        self.vtab_table_name,
        sql_fmt.ColumnListIndicesFormatter("col_{d}"){ .indices = self.sort_key[0..key_len] },
        lastOpSymbol(range.last_op),
        sql_fmt.ParameterListFormatter{ .len = key_len },
        sql_fmt.ColumnListIndicesFormatter("col_{d}"){ .indices = self.sort_key },
    });
}

fn lastOpSymbol(last_op: CursorRange.LastOp) []const u8 {
    return switch (last_op) {
        .lt => "<",
        .le => "<=",
        .eq => "=",
        .gt => ">",
        .ge => ">=",
    };
}

pub fn deleteFrom(
    self: *Self,
    tmp_arena: *ArenaAllocator,
    start_sort_key: anytype,
    start_rowid: i64,
) !void {
    const stmt = try self.delete_from.getStmt(tmp_arena, self);
    defer self.delete_from.reset();

    for (0..self.sort_key.len) |idx| {
        const value = try start_sort_key.readValue(idx);
        try value.bind(stmt, idx + 1);
    }
    try stmt.bind(.Int64, self.sort_key.len + 1, start_rowid);

    try stmt.exec();
}

fn deleteFromQuery(self: *const Self, arena: *ArenaAllocator) ![]const u8 {
    return fmt.allocPrintZ(arena.allocator(),
        \\DELETE FROM "{s}_pendinginserts"
        \\WHERE ({}, rowid) >= ({}, ?)
    , .{
        self.vtab_table_name,
        sql_fmt.ColumnListIndicesFormatter("col_{d}"){ .indices = self.sort_key },
        sql_fmt.ParameterListFormatter{ .len = self.sort_key.len },
    });
}

pub fn deleteRange(
    self: *Self,
    tmp_arena: *ArenaAllocator,
    start_sort_key: anytype,
    start_rowid: i64,
    end_sort_key: anytype,
    end_rowid: i64,
) !void {
    const stmt = try self.delete_range.getStmt(tmp_arena, self);
    defer self.delete_range.reset();

    for (0..self.sort_key.len) |idx| {
        const value = try start_sort_key.readValue(idx);
        try value.bind(stmt, idx + 1);
    }
    try stmt.bind(.Int64, self.sort_key.len + 1, start_rowid);
    for (0..self.sort_key.len) |idx| {
        const value = try end_sort_key.readValue(idx);
        try value.bind(stmt, self.sort_key.len + 2 + idx);
    }
    try stmt.bind(.Int64, self.sort_key.len * 2 + 2, end_rowid);

    try stmt.exec();
}

fn deleteRangeQuery(self: *const Self, arena: *ArenaAllocator) ![]const u8 {
    return fmt.allocPrintZ(arena.allocator(),
        \\DELETE FROM "{s}_pendinginserts"
        \\WHERE ({}, rowid) >= ({}, ?) AND ({}, rowid) < ({}, ?)
    , .{
        self.vtab_table_name,
        sql_fmt.ColumnListIndicesFormatter("col_{d}"){ .indices = self.sort_key },
        sql_fmt.ParameterListFormatter{ .len = self.sort_key.len },
        sql_fmt.ColumnListIndicesFormatter("col_{d}"){ .indices = self.sort_key },
        sql_fmt.ParameterListFormatter{ .len = self.sort_key.len },
    });
}
