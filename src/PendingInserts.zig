const std = @import("std");
const fmt = std.fmt;
const testing = std.testing;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const sqlite = @import("sqlite3.zig");
const Conn = sqlite.Conn;
const Stmt = sqlite.Stmt;
const ValueRef = sqlite.ValueRef;

const VtabCtx = @import("ctx.zig").VtabCtx;
const prep_stmt = @import("prepared_stmt.zig");
const sql_fmt = @import("sql_fmt.zig");

const schema_mod = @import("schema.zig");
const Column = schema_mod.Column;
const ColumnType = schema_mod.ColumnType;
const DataType = schema_mod.ColumnType.DataType;
const Schema = schema_mod.Schema;

const TableData = @import("TableData.zig");

const index_mod = @import("index.zig");
const RangeOp = index_mod.RangeOp;
const SortKeyRange = index_mod.SortKeyRange;

ctx: *const VtabCtx,

table_data: *TableData,
next_rowid: i64,

insert_stmt: StmtCell,
cursor_from_start: StmtPool,
cursor_from_key: StmtPool,
delete_from: StmtCell,
delete_range: StmtCell,

const Self = @This();

const StmtCell = prep_stmt.Cell(VtabCtx);
const StmtPool = prep_stmt.Pool(VtabCtx);

pub fn create(
    allocator: Allocator,
    tmp_arena: *ArenaAllocator,
    ctx: *const VtabCtx,
    table_data: *TableData,
) !Self {
    const ddl_formatter = CreateTableDdlFormatter{ .ctx = ctx.* };
    const ddl = try fmt.allocPrintZ(tmp_arena.allocator(), "{s}", .{ddl_formatter});
    try ctx.conn().exec(ddl);

    return .{
        .ctx = ctx,
        .table_data = table_data,
        .next_rowid = 1,
        .insert_stmt = StmtCell.init(&insertDml),
        .cursor_from_start = StmtPool.init(allocator, &cursorFromStartQuery),
        .cursor_from_key = StmtPool.init(allocator, &cursorFromKeyQuery),
        .delete_from = StmtCell.init(&deleteFromQuery),
        .delete_range = StmtCell.init(&deleteRangeQuery),
    };
}

const CreateTableDdlFormatter = struct {
    ctx: VtabCtx,

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
            .{self.ctx.vtabName()},
        );
        for (self.ctx.columns(), 0..) |*col, rank| {
            const col_type = ColumnType.SqliteFormatter{
                .column_type = col.column_type,
            };
            try writer.print("col_{d} {},", .{ rank, col_type });
        }
        try writer.print(
            \\PRIMARY KEY (
        , .{});
        for (self.ctx.sortKey()) |col_rank| {
            try writer.print("col_{d},", .{col_rank});
        }
        try writer.print("rowid)", .{});
        try writer.print(") WITHOUT ROWID", .{});
    }
};

test "pending inserts: format create table ddl" {
    const allocator = testing.allocator;
    const datasets = @import("testing/datasets.zig");

    const ctx = VtabCtx.init(undefined, "planets", datasets.planets.schema);

    const formatter = CreateTableDdlFormatter{ .ctx = ctx };
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
    allocator: Allocator,
    tmp_arena: *ArenaAllocator,
    ctx: *const VtabCtx,
    table_data: *TableData,
) !Self {
    var self = Self{
        .ctx = ctx,
        .table_data = table_data,
        .next_rowid = 0,
        .insert_stmt = StmtCell.init(&insertDml),
        .cursor_from_start = StmtPool.init(allocator, &cursorFromStartQuery),
        .cursor_from_key = StmtPool.init(allocator, &cursorFromKeyQuery),
        .delete_from = StmtCell.init(&deleteFromQuery),
        .delete_range = StmtCell.init(&deleteRangeQuery),
    };
    try self.loadNextRowid(tmp_arena);
    return self;
}

pub fn deinit(self: *Self) void {
    self.insert_stmt.deinit();
    self.cursor_from_start.deinit();
    self.cursor_from_key.deinit();
    self.delete_from.deinit();
    self.delete_range.deinit();
}

pub fn drop(self: *Self, tmp_arena: *ArenaAllocator) !void {
    const query = try fmt.allocPrintZ(
        tmp_arena.allocator(),
        \\DROP TABLE "{s}_pendinginserts"
    ,
        .{self.ctx.vtabName()},
    );
    try self.ctx.conn().exec(query);
}

pub fn insert(self: *Self, tmp_arena: *ArenaAllocator, values: anytype) !i64 {
    const rowid = self.next_rowid;
    self.next_rowid += 1;

    const stmt = try self.insert_stmt.acquire(tmp_arena, self.ctx.*);
    defer self.insert_stmt.release();

    try stmt.bind(.Int64, 1, rowid);
    for (0..self.ctx.columns().len) |idx| {
        const value = try values.readValue(idx);
        try value.bind(stmt, idx + 2);
    }

    try stmt.exec();

    return rowid;
}

fn insertDml(ctx: VtabCtx, arena: *ArenaAllocator) ![]const u8 {
    return fmt.allocPrintZ(arena.allocator(),
        \\INSERT INTO "{s}_pendinginserts" (rowid, {s})
        \\VALUES (?, {s})
    , .{
        ctx.vtabName(),
        sql_fmt.ColumnListLenFormatter("col_{d}"){ .len = ctx.columns().len },
        sql_fmt.ParameterListFormatter{ .len = ctx.columns().len },
    });
}

test "pending inserts: insert dml" {
    var arena = ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const conn = try Conn.openInMemory();
    defer conn.close();

    const columns: [5]Column = undefined;
    const ctx = VtabCtx.init(conn, "test", .{
        .sort_key = undefined,
        .columns = &columns,
    });

    const expected =
        \\INSERT INTO "test_pendinginserts" (rowid, col_0, col_1, col_2, col_3, col_4)
        \\VALUES (?, ?, ?, ?, ?, ?)
    ;
    const result = try insertDml(ctx, &arena);
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
    pool: ?*StmtPool,
    is_eof: bool,

    pub fn deinit(self: *Cursor) void {
        if (self.pool) |*pool| {
            pool.*.release(self.stmt);
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
        try self.next();
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
    const stmt = try self.cursor_from_start.acquire(tmp_arena, self.ctx.*);

    const eof = !(try stmt.next());

    return .{
        .stmt = stmt,
        .pool = &self.cursor_from_start,
        .is_eof = eof,
    };
}

fn cursorFromStartQuery(ctx: VtabCtx, arena: *ArenaAllocator) ![]const u8 {
    return fmt.allocPrintZ(arena.allocator(),
        \\SELECT rowid, {}
        \\FROM "{s}_pendinginserts"
        \\ORDER BY {}, rowid
    , .{
        sql_fmt.ColumnListLenFormatter("col_{d}"){ .len = ctx.columns().len },
        ctx.vtabName(),
        sql_fmt.ColumnListIndicesFormatter("col_{d}"){ .indices = ctx.sortKey() },
    });
}

pub fn cursorFrom(
    self: *Self,
    tmp_arena: *ArenaAllocator,
    start_sort_key: anytype,
    start_rowid: i64,
) !Cursor {
    const stmt = try self.cursor_from_key.acquire(tmp_arena, self.ctx.*);

    for (0..self.ctx.sortKey().len) |idx| {
        const value = try start_sort_key.readValue(idx);
        try value.bind(stmt, idx + 1);
    }
    try stmt.bind(.Int64, self.ctx.sortKey().len + 1, start_rowid);

    const eof = !(try stmt.next());

    return .{
        .stmt = stmt,
        .pool = &self.cursor_from_key,
        .is_eof = eof,
    };
}

fn cursorFromKeyQuery(ctx: VtabCtx, arena: *ArenaAllocator) ![]const u8 {
    return fmt.allocPrintZ(arena.allocator(),
        \\SELECT rowid, {}
        \\FROM "{s}_pendinginserts"
        \\WHERE ({}, rowid) >= ({}, ?)
        \\ORDER BY {}, rowid
    , .{
        sql_fmt.ColumnListLenFormatter("col_{d}"){ .len = ctx.columns().len },
        ctx.vtabName(),
        sql_fmt.ColumnListIndicesFormatter("col_{d}"){ .indices = ctx.sortKey() },
        sql_fmt.ParameterListFormatter{ .len = ctx.sortKey().len },
        sql_fmt.ColumnListIndicesFormatter("col_{d}"){ .indices = ctx.sortKey() },
    });
}

pub fn cursorPartial(self: *Self, tmp_arena: *ArenaAllocator, range: SortKeyRange) !Cursor {
    const query = try cursorPartialQuery(self.ctx.*, tmp_arena, range);
    const stmt = try self.ctx.conn().prepare(query);

    for (0..range.args.valuesLen()) |idx| {
        const value = try range.args.readValue(idx);
        try value.bind(stmt, idx + 1);
    }

    const eof = !(try stmt.next());

    return .{
        .stmt = stmt,
        .pool = null,
        .is_eof = eof,
    };
}

fn cursorPartialQuery(ctx: VtabCtx, arena: *ArenaAllocator, range: SortKeyRange) ![]const u8 {
    const args_len = range.args.valuesLen();
    switch (range.last_op) {
        .between => |between| {
            const upper_bound_indices = try arena.allocator()
                .alloc(usize, args_len - 1);
            for (0..(args_len - 2)) |i| {
                upper_bound_indices[i] = i;
            }
            upper_bound_indices[args_len - 2] = args_len - 1;

            return fmt.allocPrintZ(arena.allocator(),
                \\WITH args AS (SELECT {})
                \\SELECT rowid, {}
                \\FROM "{s}_pendinginserts"
                \\JOIN args a
                \\WHERE ({}) {s} ({}) AND ({}) {s} ({})
                \\ORDER BY {}, rowid
            , .{
                sql_fmt.ColumnListLenFormatter("? AS arg_{d}"){ .len = args_len },
                sql_fmt.ColumnListLenFormatter("col_{d}"){ .len = ctx.columns().len },
                ctx.vtabName(),
                sql_fmt.ColumnListIndicesFormatter("col_{d}"){
                    .indices = ctx.sortKey()[0..(args_len - 1)],
                },
                if (between.lower_inc) ">=" else ">",
                sql_fmt.ColumnListLenFormatter("a.arg_{d}"){ .len = args_len - 1 },
                sql_fmt.ColumnListIndicesFormatter("col_{d}"){
                    .indices = ctx.sortKey()[0..(args_len - 1)],
                },
                if (between.upper_inc) "<=" else "<",
                sql_fmt.ColumnListIndicesFormatter("a.arg_{d}"){ .indices = upper_bound_indices },
                sql_fmt.ColumnListIndicesFormatter("col_{d}"){ .indices = ctx.sortKey() },
            });
        },
        else => |op| {
            return fmt.allocPrintZ(arena.allocator(),
                \\SELECT rowid, {}
                \\FROM "{s}_pendinginserts"
                \\WHERE ({}) {s} ({})
                \\ORDER BY {}, rowid
            , .{
                sql_fmt.ColumnListLenFormatter("col_{d}"){ .len = ctx.columns().len },
                ctx.vtabName(),
                sql_fmt.ColumnListIndicesFormatter("col_{d}"){
                    .indices = ctx.sortKey()[0..args_len],
                },
                lastOpSymbol(op),
                sql_fmt.ParameterListFormatter{ .len = args_len },
                sql_fmt.ColumnListIndicesFormatter("col_{d}"){ .indices = ctx.sortKey() },
            });
        },
    }
}

fn lastOpSymbol(last_op: RangeOp) []const u8 {
    return switch (last_op) {
        .to_exc => "<",
        .to_inc => "<=",
        .eq => "=",
        .from_exc => ">",
        .from_inc => ">=",
        else => unreachable,
    };
}

pub fn deleteFrom(
    self: *Self,
    tmp_arena: *ArenaAllocator,
    start_sort_key: anytype,
    start_rowid: i64,
) !void {
    const stmt = try self.delete_from.acquire(tmp_arena, self.ctx.*);
    defer self.delete_from.release();

    for (0..self.ctx.sortKey().len) |idx| {
        const value = try start_sort_key.readValue(idx);
        try value.bind(stmt, idx + 1);
    }
    try stmt.bind(.Int64, self.ctx.sortKey().len + 1, start_rowid);

    try stmt.exec();
}

fn deleteFromQuery(ctx: VtabCtx, arena: *ArenaAllocator) ![]const u8 {
    return fmt.allocPrintZ(arena.allocator(),
        \\DELETE FROM "{s}_pendinginserts"
        \\WHERE ({}, rowid) >= ({}, ?)
    , .{
        ctx.vtabName(),
        sql_fmt.ColumnListIndicesFormatter("col_{d}"){ .indices = ctx.sortKey() },
        sql_fmt.ParameterListFormatter{ .len = ctx.sortKey().len },
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
    const stmt = try self.delete_range.acquire(tmp_arena, self.ctx.*);
    defer self.delete_range.release();

    for (0..self.ctx.sortKey().len) |idx| {
        const value = try start_sort_key.readValue(idx);
        try value.bind(stmt, idx + 1);
    }
    try stmt.bind(.Int64, self.ctx.sortKey().len + 1, start_rowid);
    for (0..self.ctx.sortKey().len) |idx| {
        const value = try end_sort_key.readValue(idx);
        try value.bind(stmt, self.ctx.sortKey().len + 2 + idx);
    }
    try stmt.bind(.Int64, self.ctx.sortKey().len * 2 + 2, end_rowid);

    try stmt.exec();
}

fn deleteRangeQuery(ctx: VtabCtx, arena: *ArenaAllocator) ![]const u8 {
    return fmt.allocPrintZ(arena.allocator(),
        \\DELETE FROM "{s}_pendinginserts"
        \\WHERE ({}, rowid) >= ({}, ?) AND ({}, rowid) < ({}, ?)
    , .{
        ctx.vtabName(),
        sql_fmt.ColumnListIndicesFormatter("col_{d}"){ .indices = ctx.sortKey() },
        sql_fmt.ParameterListFormatter{ .len = ctx.sortKey().len },
        sql_fmt.ColumnListIndicesFormatter("col_{d}"){ .indices = ctx.sortKey() },
        sql_fmt.ParameterListFormatter{ .len = ctx.sortKey().len },
    });
}
