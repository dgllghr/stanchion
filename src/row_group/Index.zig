const std = @import("std");
const fmt = std.fmt;
const testing = std.testing;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;
const ArrayListUnmanaged = std.ArrayListUnmanaged;
const DynamicBitSetUnmanaged = std.bit_set.DynamicBitSetUnmanaged;
const Order = std.math.Order;

const sqlite = @import("../sqlite3.zig");
const Conn = sqlite.Conn;
const Stmt = sqlite.Stmt;
const ValueRef = sqlite.ValueRef;

const prep_stmt = @import("../prepared_stmt.zig");
const sql_fmt = @import("../sql_fmt.zig");
const MakeShadowTable = @import("../shadow_table.zig").ShadowTable;
const VtabCtx = @import("../ctx.zig").VtabCtx;

const schema_mod = @import("../schema.zig");
const Column = schema_mod.Column;
const ColumnType = schema_mod.ColumnType;
const DataType = schema_mod.ColumnType.DataType;
const Schema = schema_mod.Schema;

const BlobManager = @import("../BlobManager.zig");
const BlobHandle = BlobManager.Handle;

const segment = @import("../segment.zig");
const SegmentHandle = segment.Storage.Handle;
const SegmentPlan = segment.Plan;
const SegmentPlanner = segment.Planner;
const SegmentReader = segment.Reader;
const SegmentStorage = segment.Storage;
const SegmentValue = segment.Value;
const SegmentWriter = segment.Writer;

const value_mod = @import("../value.zig");
const MemoryValue = value_mod.MemoryValue;
const MemoryTuple = value_mod.MemoryTuple;

const PendingInserts = @import("../PendingInserts.zig");
const PendingInsertsCursor = PendingInserts.Cursor;

const index_mod = @import("../index.zig");
const RangeOp = index_mod.RangeOp;
const SortKeyRange = index_mod.SortKeyRange;

ctx: *const VtabCtx,

insert: StmtCell,
delete: StmtCell,
merge_candidates: StmtCell,

const Self = @This();

const StmtCell = prep_stmt.Cell(VtabCtx);

pub const Entry = struct {
    rowid_segment_id: i64,
    column_segment_ids: []i64,
    record_count: u32,

    pub fn deinit(self: *Entry, allocator: Allocator) void {
        allocator.free(self.column_segment_ids);
    }
};

pub fn init(ctx: *const VtabCtx) Self {
    return .{
        .ctx = ctx,
        .insert = StmtCell.init(&insertDml),
        .delete = StmtCell.init(&deleteEntryDml),
        .merge_candidates = StmtCell.init(&mergeCandidatesQuery),
    };
}

pub fn deinit(self: *Self) void {
    self.insert.deinit();
    self.delete.deinit();
    self.merge_candidates.deinit();
}

pub const ShadowTable = MakeShadowTable(VtabCtx, struct {
    pub const suffix: []const u8 = "rowgroupindex";

    pub fn createTableDdl(ctx: VtabCtx, allocator: Allocator) ![:0]const u8 {
        const ddl_formatter = CreateTableDdlFormatter{ .ctx = ctx };
        return fmt.allocPrintZ(allocator, "{s}", .{ddl_formatter});
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
                \\CREATE TABLE "{s}_rowgroupindex" (
            ,
                .{self.ctx.vtabName()},
            );
            for (self.ctx.sortKey(), 0..) |sk_index, sk_rank| {
                const col = &self.ctx.columns()[sk_index];
                const data_type = DataType.SqliteFormatter{
                    .data_type = col.column_type.data_type,
                };
                try writer.print("start_sk_value_{d} {s} NOT NULL,", .{ sk_rank, data_type });
            }
            try writer.print("start_rowid_value INTEGER NOT NULL,", .{});
            // The `col_N` columns are uesd for storing values for insert entries and
            // segment IDs for row group entries
            for (0..self.ctx.columns().len) |rank| {
                try writer.print("col_{d}_segment_id INTEGER NULL,", .{rank});
            }
            try writer.print(
                \\rowid_segment_id INTEGER NOT NULL,
                \\record_count INTEGER NOT NULL,
                \\PRIMARY KEY (
            , .{});
            for (0..self.ctx.sortKey().len) |sk_rank| {
                try writer.print("start_sk_value_{d},", .{sk_rank});
            }
            try writer.print("start_rowid_value)", .{});
            // TODO partial unique index on table status entry?
            // TODO in tests it would be nice to add some check constraints to ensure
            //      data is populated correctly based on the entry_type
            try writer.print(") WITHOUT ROWID", .{});
        }
    };

    test "row group index: format create table ddl" {
        const datasets = @import("../testing/datasets.zig");
        const allocator = testing.allocator;

        const conn = try Conn.openInMemory();
        defer conn.close();

        inline for (.{ datasets.planets, datasets.all_column_types }) |s| {
            const ctx = VtabCtx.init(conn, s.name, s.schema);
            const formatter = CreateTableDdlFormatter{ .ctx = ctx };
            const ddl = try fmt.allocPrintZ(allocator, "{}", .{formatter});
            defer allocator.free(ddl);

            conn.exec(ddl) catch |e| {
                std.log.err("sqlite error: {s}", .{conn.lastErrMsg()});
                return e;
            };
        }
    }
});

test {
    _ = ShadowTable;
}

pub fn table(self: Self) ShadowTable {
    return .{ .ctx = self.ctx };
}

pub fn insertEntry(
    self: *Self,
    tmp_arena: *ArenaAllocator,
    start_sort_key: anytype,
    start_rowid: i64,
    entry: *const Entry,
) !void {
    const stmt = try self.insert.acquire(tmp_arena, self.ctx.*);
    defer self.insert.release();

    try stmt.bind(.Int64, 1, @intCast(entry.record_count));
    try stmt.bind(.Int64, 2, start_rowid);
    try stmt.bind(.Int64, 3, entry.rowid_segment_id);
    for (0..self.ctx.sortKey().len, 4..) |sk_idx, idx| {
        const sk_value = try start_sort_key.readValue(sk_idx);
        try sk_value.bind(stmt, idx);
    }
    const col_idx_start = self.ctx.sortKey().len + 4;
    for (0..self.ctx.columns().len, col_idx_start..) |rank, idx| {
        try stmt.bind(.Int64, idx, entry.column_segment_ids[rank]);
    }

    try stmt.exec();
}

fn insertDml(ctx: VtabCtx, arena: *ArenaAllocator) ![]const u8 {
    return fmt.allocPrintZ(arena.allocator(),
        \\INSERT INTO "{s}_rowgroupindex" (
        \\  record_count, start_rowid_value, rowid_segment_id, {s}, {s})
        \\VALUES (?, ?, ?, {s})
    , .{
        ctx.vtabName(),
        sql_fmt.ColumnListLenFormatter("start_sk_value_{d}"){ .len = ctx.sortKey().len },
        sql_fmt.ColumnListLenFormatter("col_{d}_segment_id"){ .len = ctx.columns().len },
        sql_fmt.ParameterListFormatter{ .len = ctx.columns().len + ctx.sortKey().len },
    });
}

test "row group index: insert dml" {
    const table_name = "test";

    var arena = ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const conn = try Conn.openInMemory();
    defer conn.close();

    const ctx = VtabCtx.init(conn, table_name, .{
        .columns = &[3]Column{ undefined, undefined, undefined },
        .sort_key = &[_]usize{ 0, 2 },
    });

    const expected =
        \\INSERT INTO "test_rowgroupindex" (
        ++
        "\n  " ++
        "record_count, start_rowid_value, rowid_segment_id, start_sk_value_0, " ++
        "start_sk_value_1, col_0_segment_id, col_1_segment_id, col_2_segment_id" ++
        \\)
        \\VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ;
    const result = try insertDml(ctx, &arena);
    try testing.expectEqualSlices(u8, expected, result);
}

pub fn deleteEntry(
    self: *Self,
    tmp_arena: *ArenaAllocator,
    start_sort_key: anytype,
    start_rowid: i64,
) !void {
    const stmt = try self.delete.acquire(tmp_arena, self.ctx.*);
    defer self.delete.release();

    for (0..self.ctx.sortKey().len) |idx| {
        const sk_value = try start_sort_key.readValue(idx);
        try sk_value.bind(stmt, idx + 1);
    }
    try stmt.bind(.Int64, self.ctx.sortKey().len + 1, start_rowid);

    try stmt.exec();
}

fn deleteEntryDml(ctx: VtabCtx, arena: *ArenaAllocator) ![]const u8 {
    return fmt.allocPrintZ(arena.allocator(),
        \\DELETE FROM "{s}_rowgroupindex"
        \\WHERE ({}, start_rowid_value) = ({}, ?)
    , .{
        ctx.vtabName(),
        sql_fmt.ColumnListLenFormatter("start_sk_value_{d}"){ .len = ctx.sortKey().len },
        sql_fmt.ParameterListFormatter{ .len = ctx.sortKey().len },
    });
}

pub const EntriesCursor = struct {
    stmt: Stmt,
    columns_len: usize,
    is_eof: bool,

    pub fn deinit(self: *EntriesCursor) void {
        self.stmt.deinit();
    }

    pub fn next(self: *EntriesCursor) !void {
        self.is_eof = !(try self.stmt.next());
    }

    pub fn eof(self: *const EntriesCursor) bool {
        return self.is_eof;
    }

    pub fn readEntry(self: *const EntriesCursor, entry: *Entry) void {
        entry.record_count = @intCast(self.stmt.read(.Int64, false, 0));
        entry.rowid_segment_id = self.stmt.read(.Int64, false, 1);
        for (entry.column_segment_ids, 2..) |*csid, idx| {
            csid.* = self.stmt.read(.Int64, false, idx);
        }
    }
};

pub fn cursor(self: *Self, tmp_arena: *ArenaAllocator) !EntriesCursor {
    const query = try cursorQuery(self.ctx.*, tmp_arena);
    const stmt = try self.ctx.conn().prepare(query);
    const eof = !(try stmt.next());
    return .{
        .stmt = stmt,
        .columns_len = self.ctx.columns().len,
        .is_eof = eof,
    };
}

fn cursorQuery(ctx: VtabCtx, arena: *ArenaAllocator) ![]const u8 {
    return fmt.allocPrintZ(arena.allocator(),
        \\SELECT record_count, rowid_segment_id, {}
        \\FROM "{s}_rowgroupindex"
        \\ORDER BY {}, start_rowid_value
    , .{
        sql_fmt.ColumnListLenFormatter("col_{d}_segment_id"){ .len = ctx.columns().len },
        ctx.vtabName(),
        sql_fmt.ColumnListLenFormatter("start_sk_value_{d}"){ .len = ctx.sortKey().len },
    });
}

pub fn cursorPartial(self: *Self, tmp_arena: *ArenaAllocator, range: SortKeyRange) !EntriesCursor {
    const query = try cursorPartialQuery(self.ctx.*, tmp_arena, range);
    const stmt = self.ctx.conn().prepare(query) catch |e| {
        std.log.debug("{s}", .{self.ctx.conn().lastErrMsg()});
        return e;
    };

    for (0..range.args.valuesLen()) |idx| {
        const value = try range.args.readValue(idx);
        try value.bind(stmt, idx + 1);
    }

    const eof = !(try stmt.next());

    return .{
        .stmt = stmt,
        .columns_len = self.ctx.columns().len,
        .is_eof = eof,
    };
}

fn cursorPartialQuery(ctx: VtabCtx, arena: *ArenaAllocator, range: SortKeyRange) ![]const u8 {
    // TODO these queries need to be dynamically generated to account for varying lengths of
    //      the key and the different comparison ops (and all combinations of those). However,
    //      these queries may benefit from a dynamic cache keyed by the sort key length and the
    //      last op. This is likely to help due to common user/application common access
    //      patterns
    return switch (range.last_op) {
        .to_inc, .to_exc => cursorPartialLtLe(ctx, arena, range),
        .eq, .from_inc, .from_exc => cursorPartialEqGtGe(ctx, arena, range),
        .between => cursorPartialBetween(ctx, arena, range),
    };
}

fn cursorPartialLtLe(ctx: VtabCtx, arena: *ArenaAllocator, range: SortKeyRange) ![]const u8 {
    return fmt.allocPrintZ(arena.allocator(),
        \\SELECT record_count, rowid_segment_id, {}, {}, start_rowid_value
        \\FROM "{s}_rowgroupindex"
        \\WHERE ({s}) {s} ({s})
        \\ORDER BY {}, start_rowid_value
    , .{
        sql_fmt.ColumnListLenFormatter("col_{d}_segment_id"){ .len = ctx.columns().len },
        sql_fmt.ColumnListLenFormatter("start_sk_value_{d}"){ .len = ctx.sortKey().len },
        ctx.vtabName(),
        sql_fmt.ColumnListLenFormatter("start_sk_value_{d}"){ .len = range.args.valuesLen() },
        lastOpSymbol(range.last_op),
        sql_fmt.ParameterListFormatter{ .len = range.args.valuesLen() },
        sql_fmt.ColumnListLenFormatter("start_sk_value_{d}"){ .len = ctx.sortKey().len },
    });
}

fn cursorPartialEqGtGe(ctx: VtabCtx, arena: *ArenaAllocator, range: SortKeyRange) ![]const u8 {
    // These filters need to consider that the minimum value that should be included in the
    // result set is in a row group that has a starting sort key that comes before the
    // minimum value.
    return fmt.allocPrintZ(arena.allocator(),
        \\WITH sort_key_values AS (
        \\  SELECT {s}
        \\), first_row_group AS (
        \\  SELECT record_count, rowid_segment_id, {}, {}, start_rowid_value
        \\  FROM "{s}_rowgroupindex" rgi
        \\  JOIN sort_key_values skv
        \\  WHERE ({s}) < ({s})
        \\  ORDER BY {}, rgi.start_rowid_value DESC
        \\  LIMIT 1
        \\)
        \\SELECT *
        \\FROM (
        \\  SELECT record_count, rowid_segment_id, {}, {}, start_rowid_value
        \\  FROM first_row_group
        \\
        \\  UNION ALL
        \\
        \\  SELECT record_count, rowid_segment_id, {}, {}, start_rowid_value
        \\  FROM "{s}_rowgroupindex" rgi
        \\  JOIN sort_key_values skv
        \\  WHERE ({}) {s} ({})
        \\  ORDER BY {}, start_rowid_value
        \\)
    , .{
        // sort_key_values CTE
        sql_fmt.ColumnListLenFormatter("? AS sk_value_{d}"){ .len = range.args.valuesLen() },
        // first_row_group CTE
        sql_fmt.ColumnListLenFormatter("col_{d}_segment_id"){ .len = ctx.columns().len },
        sql_fmt.ColumnListLenFormatter("start_sk_value_{d}"){ .len = ctx.sortKey().len },
        ctx.vtabName(),
        sql_fmt.ColumnListLenFormatter("rgi.start_sk_value_{d}"){ .len = range.args.valuesLen() },
        sql_fmt.ColumnListLenFormatter("skv.sk_value_{d}"){ .len = range.args.valuesLen() },
        sql_fmt.ColumnListLenFormatter("rgi.start_sk_value_{d} DESC"){ .len = ctx.sortKey().len },
        // main query
        sql_fmt.ColumnListLenFormatter("col_{d}_segment_id"){ .len = ctx.columns().len },
        sql_fmt.ColumnListLenFormatter("start_sk_value_{d}"){ .len = ctx.sortKey().len },
        sql_fmt.ColumnListLenFormatter("col_{d}_segment_id"){ .len = ctx.columns().len },
        sql_fmt.ColumnListLenFormatter("rgi.start_sk_value_{d}"){ .len = ctx.sortKey().len },
        ctx.vtabName(),
        sql_fmt.ColumnListLenFormatter("rgi.start_sk_value_{d}"){ .len = range.args.valuesLen() },
        lastOpSymbol(range.last_op),
        sql_fmt.ColumnListLenFormatter("skv.sk_value_{d}"){ .len = range.args.valuesLen() },
        sql_fmt.ColumnListLenFormatter("rgi.start_sk_value_{d}"){ .len = ctx.sortKey().len },
    });
}

fn lastOpSymbol(last_op: RangeOp) []const u8 {
    return switch (last_op) {
        .to_exc => "<",
        .to_inc => "<=",
        .eq => "=",
        .from_inc, .from_exc => ">=",
        else => unreachable,
    };
}

fn cursorPartialBetween(ctx: VtabCtx, arena: *ArenaAllocator, range: SortKeyRange) ![]const u8 {
    const args_len = range.args.valuesLen();

    // The last two values are the upper and lower bounds of the range, so they apply to the same
    // column.
    const upper_bound_indices = try arena.allocator()
        .alloc(usize, args_len - 1);
    for (0..(args_len - 2)) |i| {
        upper_bound_indices[i] = i;
    }
    upper_bound_indices[args_len - 2] = args_len - 1;

    // This filters need to consider that the minimum value that should be included in the result
    // set is in a row group that has a starting sort key that comes before the lower bound value.
    return fmt.allocPrintZ(arena.allocator(),
        \\WITH sort_key_values AS (
        \\  SELECT {s}
        \\), first_row_group AS (
        \\  SELECT {}, start_rowid_value
        \\  FROM "{s}_rowgroupindex" rgi
        \\  JOIN sort_key_values skv
        \\  WHERE ({s}) < ({s})
        \\  ORDER BY {}, rgi.start_rowid_value DESC
        \\  LIMIT 1
        \\)
        \\SELECT rgi.record_count, rgi.rowid_segment_id, {}, {}, rgi.start_rowid_value
        \\FROM "{s}_rowgroupindex" rgi
        \\JOIN sort_key_values skv
        \\LEFT JOIN first_row_group frg
        \\WHERE (frg.start_rowid_value IS NULL OR
        \\          ({}, rgi.start_rowid_value) >= ({}, frg.start_rowid_value)) AND
        \\      ({}) {s} ({})
        \\ORDER BY {}, rgi.start_rowid_value
    , .{
        // sort_key_values CTE
        sql_fmt.ColumnListLenFormatter("? AS sk_value_{d}"){ .len = args_len },
        // first_row_group CTE
        sql_fmt.ColumnListLenFormatter("rgi.start_sk_value_{d}"){ .len = ctx.sortKey().len },
        ctx.vtabName(),
        sql_fmt.ColumnListLenFormatter("rgi.start_sk_value_{d}"){ .len = args_len - 1 },
        sql_fmt.ColumnListLenFormatter("skv.sk_value_{d}"){ .len = args_len - 1 },
        sql_fmt.ColumnListLenFormatter("rgi.start_sk_value_{d} DESC"){ .len = ctx.sortKey().len },
        // main query
        sql_fmt.ColumnListLenFormatter("rgi.col_{d}_segment_id"){ .len = ctx.columns().len },
        sql_fmt.ColumnListLenFormatter("rgi.start_sk_value_{d}"){ .len = ctx.sortKey().len },
        ctx.vtabName(),
        sql_fmt.ColumnListLenFormatter("rgi.start_sk_value_{d}"){ .len = ctx.sortKey().len },
        sql_fmt.ColumnListLenFormatter("frg.start_sk_value_{d}"){ .len = ctx.sortKey().len },
        sql_fmt.ColumnListLenFormatter("rgi.start_sk_value_{d}"){ .len = args_len - 1 },
        if (range.last_op.between.upper_inc) "<=" else "<",
        sql_fmt.ColumnListIndicesFormatter("skv.sk_value_{d}"){ .indices = upper_bound_indices },
        sql_fmt.ColumnListLenFormatter("rgi.start_sk_value_{d}"){ .len = ctx.sortKey().len },
    });
}

pub const MergeCandidateCursor = struct {
    stmt: Stmt,
    cell: *StmtCell,
    sort_key_len: usize,
    columns_len: usize,
    is_eof: bool,

    pub const SortKey = struct {
        stmt: Stmt,
        start_index: usize,

        pub fn readValue(self: SortKey, index: usize) !ValueRef {
            return self.stmt.readSqliteValue(self.start_index + index);
        }
    };

    pub fn deinit(self: *MergeCandidateCursor) void {
        self.cell.release();
    }

    pub fn eof(self: *const MergeCandidateCursor) bool {
        return self.is_eof;
    }

    pub fn next(self: *MergeCandidateCursor) !void {
        self.is_eof = !(try self.stmt.next());
    }

    pub fn readPendInsertsLen(self: *const MergeCandidateCursor) u32 {
        const index = self.sort_key_len * 2 + self.columns_len + 5;
        return @intCast(self.stmt.read(.Int64, false, index));
    }

    pub fn pendInsertStartSortKey(self: *const MergeCandidateCursor) SortKey {
        const start_index = self.sort_key_len + self.columns_len + 3;
        return .{ .stmt = self.stmt, .start_index = start_index };
    }

    pub fn readPendInsertsStartRowid(self: *const MergeCandidateCursor) i64 {
        const index = self.sort_key_len * 2 + self.columns_len + 3;
        return self.stmt.read(.Int64, false, index);
    }

    pub fn rowGroupStartSortKey(self: *const MergeCandidateCursor) SortKey {
        return .{ .stmt = self.stmt, .start_index = 1 };
    }

    pub fn readRowGroupStartRowid(self: *const MergeCandidateCursor) i64 {
        const index = self.sort_key_len + 1;
        return self.stmt.read(.Int64, false, index);
    }

    pub fn readRowGroupLen(self: *const MergeCandidateCursor) u32 {
        if (self.stmt.read(.Int64, true, 0)) |v| {
            return @intCast(v);
        }
        return 0;
    }

    /// If there is not an associated row group, the entry's `record_count` is set to 0
    pub fn readRowGroupEntry(self: *const MergeCandidateCursor, entry: *Entry) void {
        entry.record_count = self.readRowGroupLen();
        if (entry.record_count == 0) {
            return;
        }

        var idx: usize = self.sort_key_len + 2;
        for (entry.column_segment_ids) |*csid| {
            csid.* = self.stmt.read(.Int64, false, idx);
            idx += 1;
        }
        entry.rowid_segment_id = self.stmt.read(.Int64, false, idx);
    }
};

/// At most 1 `MergeCandidateCursor` can be active at a time. Be sure deinit is called on a
/// `MergeCandidateCursor` before calling this function again.
pub fn mergeCandidates(self: *Self, tmp_arena: *ArenaAllocator) !MergeCandidateCursor {
    const stmt = try self.merge_candidates.acquire(tmp_arena, self.ctx.*);
    const eof = !(try stmt.next());
    return .{
        .stmt = stmt,
        .cell = &self.merge_candidates,
        .sort_key_len = self.ctx.sortKey().len,
        .columns_len = self.ctx.columns().len,
        .is_eof = eof,
    };
}

fn mergeCandidatesQuery(ctx: VtabCtx, arena: *ArenaAllocator) ![]const u8 {
    // This query takes advantage of a feature of sqlite where aggregation functions are not
    // required on columns that aren't part of the group by. In this case, sqlite will return
    // the first row in the group, which is the first row in the set of pending inserts that
    // match with the row group.
    return fmt.allocPrintZ(arena.allocator(),
        \\WITH row_groups AS (
        \\    SELECT
        \\        rgi.record_count, {}, rgi.start_rowid_value, {}, rgi.rowid_segment_id,
        \\        {}, pi.rowid,
        \\        COUNT(*) AS cum_count
        \\    FROM "{s}_pendinginserts" pi
        \\    LEFT JOIN "{s}_rowgroupindex" rgi
        \\        ON ({}, rgi.start_rowid_value) <= ({}, pi.rowid)
        \\    GROUP BY {}, rgi.start_rowid_value
        \\    ORDER BY {}, rgi.start_rowid_value
        \\)
        \\SELECT
        \\    *,
        \\    CASE
        \\        WHEN start_rowid_value IS NULL THEN cum_count
        \\        ELSE COALESCE(cum_count - (LEAD(cum_count) OVER subsequent), cum_count)
        \\    END AS count
        \\FROM row_groups
        \\WINDOW subsequent AS (ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING)
    , .{
        sql_fmt.ColumnListLenFormatter("rgi.start_sk_value_{d}"){ .len = ctx.sortKey().len },
        sql_fmt.ColumnListLenFormatter("rgi.col_{d}_segment_id"){ .len = ctx.columns().len },
        sql_fmt.ColumnListIndicesFormatter("pi.col_{d}"){ .indices = ctx.sortKey() },
        ctx.vtabName(),
        ctx.vtabName(),
        sql_fmt.ColumnListLenFormatter("rgi.start_sk_value_{d}"){ .len = ctx.sortKey().len },
        sql_fmt.ColumnListIndicesFormatter("pi.col_{d}"){ .indices = ctx.sortKey() },
        sql_fmt.ColumnListLenFormatter("rgi.start_sk_value_{d}"){ .len = ctx.sortKey().len },
        sql_fmt.ColumnListLenFormatter("rgi.start_sk_value_{d}"){ .len = ctx.sortKey().len },
    });
}
