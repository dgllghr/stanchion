const std = @import("std");
const fmt = std.fmt;
const testing = std.testing;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;
const ArrayListUnmanaged = std.ArrayListUnmanaged;
const DynamicBitSetUnmanaged = std.bit_set.DynamicBitSetUnmanaged;
const Order = std.math.Order;

const sqlite = @import("sqlite3.zig");
const Conn = sqlite.Conn;
const Stmt = sqlite.Stmt;
const ValueRef = sqlite.ValueRef;

const stmt_cell = @import("stmt_cell.zig");
const sql_fmt = @import("sql_fmt.zig");

const schema_mod = @import("schema.zig");
const Column = schema_mod.Column;
const ColumnType = schema_mod.ColumnType;
const DataType = schema_mod.ColumnType.DataType;
const Schema = schema_mod.Schema;

const segment = @import("segment.zig");
const SegmentDb = segment.Db;
const SegmentHandle = segment.Handle;
const SegmentPlan = segment.Plan;
const SegmentPlanner = segment.Planner;
const SegmentReader = segment.Reader;
const SegmentValue = segment.Value;
const SegmentWriter = segment.Writer;

const value_mod = @import("value.zig");
const MemoryValue = value_mod.MemoryValue;
const MemoryTuple = value_mod.MemoryTuple;

const PendingInserts = @import("PendingInserts.zig");
const PendingInsertsCursor = PendingInserts.Cursor;

const CursorRange = @import("CursorRange.zig");

pub const Index = struct {
    conn: Conn,
    vtab_table_name: []const u8,
    sort_key: []const usize,
    columns_len: usize,

    insert: StmtCell,
    delete: StmtCell,
    merge_candidates: StmtCell,

    const StmtCell = stmt_cell.StmtCell(Index);

    pub const Entry = struct {
        rowid_segment_id: i64,
        column_segment_ids: []i64,
        record_count: u32,

        pub fn deinit(self: *Entry, allocator: Allocator) void {
            allocator.free(self.column_segment_ids);
        }
    };

    pub fn create(
        tmp_arena: *ArenaAllocator,
        conn: Conn,
        vtab_table_name: []const u8,
        schema: *const Schema,
    ) !Index {
        const ddl_formatter = CreateTableDdlFormatter{
            .vtab_table_name = vtab_table_name,
            .schema = schema,
        };
        const ddl = try fmt.allocPrintZ(tmp_arena.allocator(), "{s}", .{ddl_formatter});
        try conn.exec(ddl);

        return .{
            .conn = conn,
            .vtab_table_name = vtab_table_name,
            .sort_key = schema.sort_key.items,
            .columns_len = schema.columns.items.len,
            .insert = StmtCell.init(&insertDml),
            .delete = StmtCell.init(&deleteEntryDml),
            .merge_candidates = StmtCell.init(&mergeCandidatesQuery),
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
                \\CREATE TABLE "{s}_rowgroupindex" (
            ,
                .{self.vtab_table_name},
            );
            for (self.schema.sort_key.items, 0..) |sk_index, sk_rank| {
                const col = &self.schema.columns.items[sk_index];
                const data_type = DataType.SqliteFormatter{
                    .data_type = col.column_type.data_type,
                };
                try writer.print("start_sk_value_{d} {s} NOT NULL,", .{ sk_rank, data_type });
            }
            try writer.print("start_rowid_value INTEGER NOT NULL,", .{});
            // The `col_N` columns are uesd for storing values for insert entries and
            // segment IDs for row group entries
            for (0..self.schema.columns.items.len) |rank| {
                try writer.print("col_{d}_segment_id INTEGER NULL,", .{rank});
            }
            try writer.print(
                \\rowid_segment_id INTEGER NOT NULL,
                \\record_count INTEGER NOT NULL,
                \\PRIMARY KEY (
            , .{});
            for (0..self.schema.sort_key.items.len) |sk_rank| {
                try writer.print("start_sk_value_{d},", .{sk_rank});
            }
            try writer.print("start_rowid_value)", .{});
            // TODO partial unique index on table status entry?
            // TODO in tests it would be nice to add some check constraints to ensure
            //      data is populated correctly based on the entry_type
            try writer.print(") STRICT, WITHOUT ROWID", .{});
        }
    };

    test "row group index: format create table ddl" {
        const allocator = testing.allocator;

        var columns = try std.ArrayListUnmanaged(schema_mod.Column)
            .initCapacity(allocator, 5);
        defer columns.deinit(allocator);
        columns.appendAssumeCapacity(Column{
            .rank = 0,
            .name = "quadrant",
            .column_type = .{ .data_type = .Text, .nullable = false },
            .sk_rank = 0,
        });
        columns.appendAssumeCapacity(Column{
            .rank = 1,
            .name = "sector",
            .column_type = .{ .data_type = .Integer, .nullable = false },
            .sk_rank = 1,
        });
        columns.appendAssumeCapacity(Column{
            .rank = 2,
            .name = "size",
            .column_type = .{ .data_type = .Integer, .nullable = true },
            .sk_rank = null,
        });
        columns.appendAssumeCapacity(Column{
            .rank = 3,
            .name = "gravity",
            .column_type = .{ .data_type = .Float, .nullable = true },
            .sk_rank = null,
        });

        var sort_key = try std.ArrayListUnmanaged(usize)
            .initCapacity(allocator, 5);
        defer sort_key.deinit(allocator);
        sort_key.appendAssumeCapacity(0);
        sort_key.appendAssumeCapacity(1);

        const schema = Schema{
            .columns = columns,
            .sort_key = sort_key,
        };

        const formatter = CreateTableDdlFormatter{ .vtab_table_name = "planets", .schema = &schema };
        const ddl = try fmt.allocPrintZ(allocator, "{}", .{formatter});
        defer allocator.free(ddl);

        const conn = try Conn.openInMemory();
        defer conn.close();

        conn.exec(ddl) catch |e| {
            std.log.err("sqlite error: {s}", .{conn.lastErrMsg()});
            return e;
        };
    }

    pub fn open(
        conn: Conn,
        vtab_table_name: []const u8,
        schema: *const Schema,
    ) Index {
        return .{
            .conn = conn,
            .vtab_table_name = vtab_table_name,
            .sort_key = schema.sort_key.items,
            .columns_len = schema.columns.items.len,
            .insert = StmtCell.init(&insertDml),
            .delete = StmtCell.init(&deleteEntryDml),
            .merge_candidates = StmtCell.init(&mergeCandidatesQuery),
        };
    }

    pub fn deinit(self: *Index) void {
        self.insert.deinit();
        self.delete.deinit();
        self.merge_candidates.deinit();
    }

    pub fn drop(self: *Index, tmp_arena: *ArenaAllocator) !void {
        const query = try fmt.allocPrintZ(
            tmp_arena.allocator(),
            \\DROP TABLE "{s}_rowgroupindex"
        ,
            .{self.vtab_table_name},
        );
        try self.conn.exec(query);
    }

    pub fn insertEntry(
        self: *Index,
        tmp_arena: *ArenaAllocator,
        start_sort_key: anytype,
        start_rowid: i64,
        entry: *const Entry,
    ) !void {
        const stmt = try self.insert.getStmt(tmp_arena, self);
        defer self.insert.reset();

        try stmt.bind(.Int64, 1, @intCast(entry.record_count));
        try stmt.bind(.Int64, 2, start_rowid);
        try stmt.bind(.Int64, 3, entry.rowid_segment_id);
        for (0..self.sort_key.len, 4..) |sk_idx, idx| {
            const sk_value = try start_sort_key.readValue(sk_idx);
            try sk_value.bind(stmt, idx);
        }
        const col_idx_start = self.sort_key.len + 4;
        for (0..self.columns_len, col_idx_start..) |rank, idx| {
            try stmt.bind(.Int64, idx, entry.column_segment_ids[rank]);
        }

        try stmt.exec();
    }

    fn insertDml(self: *const Index, arena: *ArenaAllocator) ![]const u8 {
        return fmt.allocPrintZ(arena.allocator(),
            \\INSERT INTO "{s}_rowgroupindex" (
            \\  record_count, start_rowid_value, rowid_segment_id, {s}, {s})
            \\VALUES (?, ?, ?, {s})
        , .{
            self.vtab_table_name,
            sql_fmt.ColumnListLenFormatter("start_sk_value_{d}"){ .len = self.sort_key.len },
            sql_fmt.ColumnListLenFormatter("col_{d}_segment_id"){ .len = self.columns_len },
            sql_fmt.ParameterListFormatter{ .len = self.columns_len + self.sort_key.len },
        });
    }

    test "row group index: insert dml" {
        const table_name = "test";

        var arena = ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();

        const conn = try Conn.openInMemory();
        defer conn.close();

        const self = Index{
            .conn = conn,
            .vtab_table_name = table_name,
            .sort_key = &[_]usize{ 0, 2 },
            .columns_len = 3,
            .insert = undefined,
            .delete = undefined,
            .merge_candidates = undefined,
        };

        const expected =
            \\INSERT INTO "test_rowgroupindex" (
            ++
            "\n  " ++
            "record_count, start_rowid_value, rowid_segment_id, start_sk_value_0, " ++
            "start_sk_value_1, col_0_segment_id, col_1_segment_id, col_2_segment_id" ++
            \\)
            \\VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ;
        const result = try self.insertDml(&arena);
        try testing.expectEqualSlices(u8, expected, result);
    }

    pub fn deleteEntry(
        self: *Index,
        tmp_arena: *ArenaAllocator,
        start_sort_key: anytype,
        start_rowid: i64,
    ) !void {
        const stmt = try self.delete.getStmt(tmp_arena, self);
        defer self.delete.reset();

        for (0..self.sort_key.len) |idx| {
            const sk_value = try start_sort_key.readValue(idx);
            try sk_value.bind(stmt, idx + 1);
        }
        try stmt.bind(.Int64, self.sort_key.len + 1, start_rowid);

        try stmt.exec();
    }

    fn deleteEntryDml(self: *const Index, arena: *ArenaAllocator) ![]const u8 {
        return fmt.allocPrintZ(arena.allocator(),
            \\DELETE FROM "{s}_rowgroupindex"
            \\WHERE ({}, start_rowid_value) = ({}, ?)
        , .{
            self.vtab_table_name,
            sql_fmt.ColumnListLenFormatter("start_sk_value_{d}"){ .len = self.sort_key.len },
            sql_fmt.ParameterListFormatter{ .len = self.sort_key.len },
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

    pub fn cursor(self: *Index, tmp_arena: *ArenaAllocator) !EntriesCursor {
        const query = try self.cursorQuery(tmp_arena);
        const stmt = try self.conn.prepare(query);
        const eof = !(try stmt.next());
        return .{
            .stmt = stmt,
            .columns_len = self.columns_len,
            .is_eof = eof,
        };
    }

    fn cursorQuery(self: *const Index, arena: *ArenaAllocator) ![]const u8 {
        return fmt.allocPrintZ(arena.allocator(),
            \\SELECT record_count, rowid_segment_id, {}
            \\FROM "{s}_rowgroupindex"
            \\ORDER BY {}, start_rowid_value
        , .{
            sql_fmt.ColumnListLenFormatter("col_{d}_segment_id"){ .len = self.columns_len },
            self.vtab_table_name,
            sql_fmt.ColumnListLenFormatter("start_sk_value_{d}"){ .len = self.sort_key.len },
        });
    }

    pub fn cursorPartial(
        self: *Index,
        tmp_arena: *ArenaAllocator,
        range: CursorRange,
    ) !EntriesCursor {
        const query = try self.cursorPartialQuery(tmp_arena, range);
        const stmt = self.conn.prepare(query) catch |e| {
            std.log.debug("{s}", .{self.conn.lastErrMsg()});
            return e;
        };

        for (0..range.key.valuesLen()) |idx| {
            const value = try range.key.readValue(idx);
            try value.bind(stmt, idx + 1);
        }

        const eof = !(try stmt.next());

        return .{
            .stmt = stmt,
            .columns_len = self.columns_len,
            .is_eof = eof,
        };
    }

    fn cursorPartialQuery(
        self: *const Index,
        arena: *ArenaAllocator,
        range: CursorRange,
    ) ![]const u8 {
        // TODO these queries need to be dynamically generated to account for varying lengths of
        //      the key and the different comparison ops (and all combinations of those). However,
        //      these queries may benefit from a dynamic cache keyed by the sort key length and the
        //      last op. This is likely to help due to common user/application common access
        //      patterns
        return switch (range.last_op) {
            .lt, .le => cursorPartialLtLe(self, arena, range),
            .eq, .gt, .ge => cursorPartialEqGtGe(self, arena, range),
        };
    }

    fn cursorPartialLtLe(
        self: *const Index,
        arena: *ArenaAllocator,
        range: CursorRange,
    ) ![]const u8 {
        return fmt.allocPrintZ(arena.allocator(),
            \\SELECT record_count, rowid_segment_id, {}, {}, start_rowid_value
            \\FROM "{s}_rowgroupindex"
            \\WHERE ({s}) {s} ({s})
            \\ORDER BY {}, start_rowid_value
        , .{
            sql_fmt.ColumnListLenFormatter("col_{d}_segment_id"){ .len = self.columns_len },
            sql_fmt.ColumnListLenFormatter("start_sk_value_{d}"){ .len = self.sort_key.len },
            self.vtab_table_name,
            sql_fmt.ColumnListLenFormatter("start_sk_value_{d}"){ .len = range.key.valuesLen() },
            lastOpSymbol(range.last_op),
            sql_fmt.ParameterListFormatter{ .len = range.key.valuesLen() },
            sql_fmt.ColumnListLenFormatter("start_sk_value_{d}"){ .len = self.sort_key.len },
        });
    }

    fn cursorPartialEqGtGe(
        self: *const Index,
        arena: *ArenaAllocator,
        range: CursorRange,
    ) ![]const u8 {
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
            \\  ORDER BY {}, start_rowid_value DESC
            \\)
        , .{
            // sort_key_values CTE
            sql_fmt.ColumnListLenFormatter("? AS sk_value_{d}"){ .len = range.key.valuesLen() },
            // first_row_group CTE
            sql_fmt.ColumnListLenFormatter("col_{d}_segment_id"){ .len = self.columns_len },
            sql_fmt.ColumnListLenFormatter("start_sk_value_{d}"){ .len = self.sort_key.len },
            self.vtab_table_name,
            sql_fmt.ColumnListLenFormatter("rgi.start_sk_value_{d}"){
                .len = range.key.valuesLen(),
            },
            sql_fmt.ColumnListLenFormatter("skv.sk_value_{d}"){ .len = range.key.valuesLen() },
            // main query
            sql_fmt.ColumnListLenFormatter("col_{d}_segment_id"){ .len = self.columns_len },
            sql_fmt.ColumnListLenFormatter("start_sk_value_{d}"){ .len = self.sort_key.len },
            sql_fmt.ColumnListLenFormatter("col_{d}_segment_id"){ .len = self.columns_len },
            sql_fmt.ColumnListLenFormatter("rgi.start_sk_value_{d}"){ .len = self.sort_key.len },
            self.vtab_table_name,
            sql_fmt.ColumnListLenFormatter("rgi.start_sk_value_{d}"){
                .len = range.key.valuesLen(),
            },
            lastOpSymbol(range.last_op),
            sql_fmt.ColumnListLenFormatter("skv.sk_value_{d}"){ .len = range.key.valuesLen() },
            sql_fmt.ColumnListLenFormatter("rgi.start_sk_value_{d}"){ .len = self.sort_key.len },
        });
    }

    fn lastOpSymbol(last_op: CursorRange.LastOp) []const u8 {
        return switch (last_op) {
            .lt => "<",
            .le => "<=",
            .eq => "=",
            .gt, .ge => ">=",
        };
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
            self.cell.reset();
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
    pub fn mergeCandidates(self: *Index, tmp_arena: *ArenaAllocator) !MergeCandidateCursor {
        const stmt = try self.merge_candidates.getStmt(tmp_arena, self);
        const eof = !(try stmt.next());
        return .{
            .stmt = stmt,
            .cell = &self.merge_candidates,
            .sort_key_len = self.sort_key.len,
            .columns_len = self.columns_len,
            .is_eof = eof,
        };
    }

    fn mergeCandidatesQuery(self: *const Index, arena: *ArenaAllocator) ![]const u8 {
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
            sql_fmt.ColumnListLenFormatter("rgi.start_sk_value_{d}"){ .len = self.sort_key.len },
            sql_fmt.ColumnListLenFormatter("rgi.col_{d}_segment_id"){ .len = self.columns_len },
            sql_fmt.ColumnListIndicesFormatter("pi.col_{d}"){ .indices = self.sort_key },
            self.vtab_table_name,
            self.vtab_table_name,
            sql_fmt.ColumnListLenFormatter("rgi.start_sk_value_{d}"){ .len = self.sort_key.len },
            sql_fmt.ColumnListIndicesFormatter("pi.col_{d}"){ .indices = self.sort_key },
            sql_fmt.ColumnListLenFormatter("rgi.start_sk_value_{d}"){ .len = self.sort_key.len },
            sql_fmt.ColumnListLenFormatter("rgi.start_sk_value_{d}"){ .len = self.sort_key.len },
        });
    }
};

pub const Creator = struct {
    columns_len: usize,
    sort_key: []const usize,
    segment_db: *SegmentDb,
    row_group_index: *Index,
    pending_inserts: *PendingInserts,

    /// Stores the order that records go into the row group. True means read from pending inserts,
    /// false from the row group
    interleaving: DynamicBitSetUnmanaged,
    rowid_segment_planner: SegmentPlanner,
    column_segment_planners: []SegmentPlanner,
    rowid_segment_writer: SegmentWriter,
    column_segment_writers: []SegmentWriter,
    writers_continue: DynamicBitSetUnmanaged,

    /// Cursor that is used to read from the source row group. This cursor is reused for every
    /// source row group that is used to create new row groups.
    src_row_group_cursor: Cursor,

    max_row_group_len: u32,

    pub fn init(
        allocator: Allocator,
        table_static_arena: *ArenaAllocator,
        segment_db: *SegmentDb,
        schema: *const Schema,
        row_group_index: *Index,
        pending_inserts: *PendingInserts,
        //primary_index: *PrimaryIndex,
        max_row_group_len: u32,
    ) !Creator {
        const columns_len = schema.columns.items.len;

        const static_allocator = table_static_arena.allocator();

        var interleaving = try DynamicBitSetUnmanaged.initEmpty(
            static_allocator,
            max_row_group_len,
        );
        errdefer interleaving.deinit(static_allocator);

        const rowid_segment_planner = SegmentPlanner.init(ColumnType.Rowid);
        const planners = try static_allocator.alloc(SegmentPlanner, columns_len);
        for (planners, schema.columns.items) |*planner, *col| {
            planner.* = SegmentPlanner.init(col.column_type);
        }

        const writers = try static_allocator.alloc(SegmentWriter, columns_len);
        const writers_continue = try DynamicBitSetUnmanaged.initEmpty(
            static_allocator,
            columns_len,
        );

        const src_row_group_cursor = try Cursor.init(allocator, segment_db, schema);

        return .{
            .columns_len = columns_len,
            .sort_key = schema.sort_key.items,
            .segment_db = segment_db,
            .row_group_index = row_group_index,
            .pending_inserts = pending_inserts,
            .interleaving = interleaving,
            .rowid_segment_planner = rowid_segment_planner,
            .column_segment_planners = planners,
            .rowid_segment_writer = undefined,
            .column_segment_writers = writers,
            .writers_continue = writers_continue,
            .src_row_group_cursor = src_row_group_cursor,
            .max_row_group_len = max_row_group_len,
        };
    }

    pub fn deinit(self: *Creator) void {
        self.src_row_group_cursor.deinit();
    }

    pub fn reset(self: *Creator) void {
        // TODO it may be wise to 0-out the rest of the fields even if their values are
        //      always overriden during row group creation
        self.resetOutput();
        self.src_row_group_cursor.reset();
    }

    fn resetOutput(self: *Creator) void {
        self.rowid_segment_planner.reset();
        for (self.column_segment_planners) |*p| {
            p.reset();
        }
        self.interleaving.setRangeValue(
            .{ .start = 0, .end = self.max_row_group_len },
            false,
        );
    }

    const NewRowGroup = struct {
        start_sort_key: []MemoryValue,
        start_rowid: i64,
        row_group_entry: Index.Entry,

        fn init(allocator: Allocator, columns_len: usize, sort_key_len: usize) !NewRowGroup {
            const start_sort_key = try allocator.alloc(MemoryValue, sort_key_len);
            const column_segment_ids = try allocator.alloc(i64, columns_len);
            return .{
                .start_sort_key = start_sort_key,
                .start_rowid = 0,
                .row_group_entry = .{
                    .rowid_segment_id = 0,
                    .column_segment_ids = column_segment_ids,
                    .record_count = 0,
                },
            };
        }
    };

    pub fn createAll(self: *Creator, tmp_arena: *ArenaAllocator) !void {
        // TODO wrap this function in a savepoint
        var candidates = try self.row_group_index.mergeCandidates(tmp_arena);
        defer candidates.deinit();

        const pend_inserts_sk_buf = try tmp_arena.allocator()
            .alloc(MemoryValue, self.sort_key.len);
        var new_row_groups = ArrayListUnmanaged(NewRowGroup){};

        while (!candidates.eof()) {
            const row_group_len = candidates.readRowGroupLen();
            const pend_inserts_len = candidates.readPendInsertsLen();
            const full_row_groups = (row_group_len + pend_inserts_len) / self.max_row_group_len;

            // If there is a row group that is merged as part of this process, then there need to
            // enough pending inserts to create 2 full row groups. Otherwise, there just need to be
            // enough to create 1 full row group with no merge;
            const min_row_groups: u32 = if (row_group_len == 0) 1 else 2;

            if (full_row_groups >= min_row_groups) {
                // Prepare the row group cursor to read from the source row group
                // TODO skip this if there is no source row group?
                defer self.src_row_group_cursor.reset();
                candidates.readRowGroupEntry(&self.src_row_group_cursor.row_group);

                // Prepare the pending inserts cursor. There is no need to copy the start key
                // values because the candidates cursor does not move for the lifetime of the
                // pending inserts cursor.
                const pend_inserts_start_sk_initial = candidates.pendInsertStartSortKey();
                const pend_inserts_start_rowid_initial = candidates.readPendInsertsStartRowid();
                var pend_inserts_limit = (full_row_groups * self.max_row_group_len) -
                    self.src_row_group_cursor.row_group.record_count;
                var pend_inserts_cursor = Limiter(PendingInsertsCursor).init(
                    try self.pending_inserts.cursorFrom(
                        tmp_arena,
                        pend_inserts_start_sk_initial,
                        pend_inserts_start_rowid_initial,
                    ),
                    pend_inserts_limit,
                );
                defer pend_inserts_cursor.deinit();

                // Allocate space for the new row groups
                try new_row_groups.ensureUnusedCapacity(tmp_arena.allocator(), full_row_groups);

                // Create the row groups
                for (0..full_row_groups) |idx| {
                    const nrg_idx = new_row_groups.items.len;
                    new_row_groups.appendAssumeCapacity(try NewRowGroup.init(
                        tmp_arena.allocator(),
                        self.columns_len,
                        self.sort_key.len,
                    ));
                    const new_row_group = &new_row_groups.items[nrg_idx];

                    defer self.resetOutput();
                    try self.createSingle(tmp_arena, &pend_inserts_cursor, new_row_group);
                    pend_inserts_limit -= pend_inserts_cursor.index;

                    // Have the pending inserts cursor start from the current spot so that when it
                    // is rewound, it starts from the proper place for the next row group being
                    // created.
                    if (idx < full_row_groups - 1) {
                        const curr_sk = pend_inserts_cursor.sortKey(self.sort_key);
                        for (0..self.sort_key.len) |sk_idx| {
                            pend_inserts_sk_buf[sk_idx] = try MemoryValue.fromRef(
                                tmp_arena.allocator(),
                                try curr_sk.readValue(sk_idx),
                            );
                        }
                        const curr_rowid = try pend_inserts_cursor.readRowid();

                        pend_inserts_cursor.deinit();

                        pend_inserts_cursor = Limiter(PendingInsertsCursor).init(
                            try self.pending_inserts.cursorFrom(
                                tmp_arena,
                                MemoryTuple{ .values = pend_inserts_sk_buf },
                                curr_rowid.asI64(),
                            ),
                            pend_inserts_limit,
                        );
                    }
                }

                // Delete the pending inserts that went into the row groups
                if (pend_inserts_cursor.inner().eof()) {
                    // The inner cursor at eof means that the the cursor reached the end of the
                    // table, so delete all the way to the end.
                    try self.pending_inserts.deleteFrom(
                        tmp_arena,
                        pend_inserts_start_sk_initial,
                        pend_inserts_start_rowid_initial,
                    );
                } else {
                    // The outer (Limiter) cursor may be eof, but the inner cursor is not. So use
                    // the current row as the upper bound on the delete range.
                    try self.pending_inserts.deleteRange(
                        tmp_arena,
                        pend_inserts_start_sk_initial,
                        pend_inserts_start_rowid_initial,
                        pend_inserts_cursor.sortKey(self.sort_key),
                        (try pend_inserts_cursor.readRowid()).asI64(),
                    );
                }

                // TODO delete the segments from the source row group

                // Delete the source row group entry
                try self.row_group_index.deleteEntry(
                    tmp_arena,
                    candidates.rowGroupStartSortKey(),
                    candidates.readRowGroupStartRowid(),
                );
            }

            try candidates.next();
        }

        for (new_row_groups.items) |*rg| {
            try self.row_group_index.insertEntry(
                tmp_arena,
                MemoryTuple{ .values = rg.start_sort_key },
                rg.start_rowid,
                &rg.row_group_entry,
            );
        }
    }

    /// Assumes the pending inserts iterator and row group cursor point to the records that are
    /// `start_index` from the start. Returns the number of records consumed from the pending
    /// inserts cursor
    fn createSingle(
        self: *Creator,
        tmp_arena: *ArenaAllocator,
        pending_inserts: *Limiter(PendingInsertsCursor),
        new_row_group: *NewRowGroup,
    ) !void {
        const rg_cursor_start = self.src_row_group_cursor.index;

        //
        // Plan
        //

        // Assign the start sort key and start row id using the first row
        if (self.src_row_group_cursor.eof()) {
            self.interleaving.set(0);
            try self.planFirstRow(tmp_arena, pending_inserts, new_row_group);
            try pending_inserts.next();
        } else if (pending_inserts.eof()) {
            try self.planFirstRow(tmp_arena, &self.src_row_group_cursor, new_row_group);
            try self.src_row_group_cursor.next();
        } else {
            const ord = try compare(self.sort_key, &self.src_row_group_cursor, pending_inserts);
            switch (ord) {
                .lt => {
                    try self.planFirstRow(tmp_arena, &self.src_row_group_cursor, new_row_group);
                    try self.src_row_group_cursor.next();
                },
                .gt => {
                    self.interleaving.set(0);
                    try self.planFirstRow(tmp_arena, pending_inserts, new_row_group);
                    try pending_inserts.next();
                },
                // Two records should never be equal because the rowid is different for every
                // record
                .eq => unreachable,
            }
        }

        new_row_group.row_group_entry.record_count += 1;

        // Plan the rest of the rows

        while (new_row_group.row_group_entry.record_count < self.max_row_group_len) {
            if (self.src_row_group_cursor.eof() or pending_inserts.eof()) {
                break;
            }

            const ord = try compare(self.sort_key, &self.src_row_group_cursor, pending_inserts);
            switch (ord) {
                .lt => {
                    try self.planRow(&self.src_row_group_cursor);
                    try self.src_row_group_cursor.next();
                },
                .gt => {
                    self.interleaving.set(new_row_group.row_group_entry.record_count);
                    try self.planRow(pending_inserts);
                    try pending_inserts.next();
                },
                // Two records should never be equal because the rowid is different for
                // every record
                .eq => unreachable,
            }

            new_row_group.row_group_entry.record_count += 1;
        }

        // Write any remaining rows from either source as long as there is space. This
        // part is only reached when at least 1 source is exhausted so at most one of the
        // following loops will run

        while (new_row_group.row_group_entry.record_count < self.max_row_group_len and
            !self.src_row_group_cursor.eof())
        {
            try self.planRow(&self.src_row_group_cursor);
            try self.src_row_group_cursor.next();
            new_row_group.row_group_entry.record_count += 1;
        }

        while (new_row_group.row_group_entry.record_count < self.max_row_group_len and
            !pending_inserts.eof())
        {
            self.interleaving.set(new_row_group.row_group_entry.record_count);
            try self.planRow(pending_inserts);
            try pending_inserts.next();
            new_row_group.row_group_entry.record_count += 1;
        }

        //
        // Write
        //

        // Init rowid segment writer

        try self.rowid_segment_writer.openCreate(
            tmp_arena,
            self.segment_db,
            try self.rowid_segment_planner.end(),
        );
        errdefer freeSegment(tmp_arena, self.segment_db, &self.rowid_segment_writer);
        defer self.rowid_segment_writer.handle.close();
        const rowid_continue = try self.rowid_segment_writer.begin();

        var writer_idx: usize = 0;
        errdefer for (0..writer_idx) |idx| {
            freeSegment(tmp_arena, self.segment_db, &self.column_segment_writers[idx]);
        };
        defer for (0..writer_idx) |idx| {
            self.column_segment_writers[idx].handle.close();
        };
        for (self.column_segment_writers, 0..) |*writer, idx| {
            try writer.openCreate(
                tmp_arena,
                self.segment_db,
                try self.column_segment_planners[idx].end(),
            );
            writer_idx += 1;
            const cont = try writer.begin();
            self.writers_continue.setValue(idx, cont);
        }

        // Write the data

        // Reset the sources back to where they were when creating the single row group started
        try pending_inserts.rewind();
        self.src_row_group_cursor.reset();
        try self.src_row_group_cursor.skip(rg_cursor_start);

        for (0..new_row_group.row_group_entry.record_count) |idx| {
            // true means read from row group, false from staged inserts
            if (self.interleaving.isSet(idx)) {
                try self.writeRow(rowid_continue, pending_inserts);
                try pending_inserts.next();
            } else {
                try self.writeRow(rowid_continue, &self.src_row_group_cursor);
                try self.src_row_group_cursor.next();
            }
        }

        // Finalize the writers

        const rowid_handle = try self.rowid_segment_writer.end();
        new_row_group.row_group_entry.rowid_segment_id = rowid_handle.id;
        const column_segment_ids = new_row_group.row_group_entry.column_segment_ids;
        for (self.column_segment_writers, column_segment_ids) |*writer, *seg_id| {
            const handle = try writer.end();
            seg_id.* = handle.id;
        }
    }

    /// Feeds the first row into the planner and stores the start sort key and start rowid in
    /// `new_row_group`
    fn planFirstRow(
        self: *Creator,
        tmp_arena: *ArenaAllocator,
        row: anytype,
        new_row_group: *NewRowGroup,
    ) !void {
        const start_rowid_value = try row.readRowid();
        const start_rowid = start_rowid_value.asI64();
        new_row_group.start_rowid = start_rowid;

        self.rowid_segment_planner.next(start_rowid_value);

        for (self.column_segment_planners, 0..) |*planner, idx| {
            const value = try row.readValue(idx);
            for (self.sort_key, 0..) |sk_col_rank, sk_rank| {
                if (sk_col_rank == idx) {
                    const mem_value = try MemoryValue.fromValue(
                        tmp_arena.allocator(),
                        planner.column_type.data_type,
                        value,
                    );
                    new_row_group.start_sort_key[sk_rank] = mem_value;
                    break;
                }
            }
            planner.next(value);
        }
    }

    fn planRow(self: *Creator, row: anytype) !void {
        const rowid = try row.readRowid();
        self.rowid_segment_planner.next(rowid);

        for (self.column_segment_planners, 0..) |*planner, idx| {
            const value = try row.readValue(idx);
            planner.next(value);
        }
    }

    fn writeRow(self: *Creator, rowid_continue: bool, row: anytype) !void {
        if (rowid_continue) {
            const rowid = try row.readRowid();
            try self.rowid_segment_writer.write(rowid);
        }
        for (self.column_segment_writers, 0..) |*writer, idx| {
            if (self.writers_continue.isSet(idx)) {
                const value = try row.readValue(idx);
                try writer.write(value);
            }
        }
    }

    fn compare(sort_key: []const usize, left_row: anytype, right_row: anytype) !Order {
        for (sort_key) |rank| {
            const left_value = try left_row.readValue(rank);
            const right_value = try right_row.readValue(rank);
            const ord = left_value.compare(right_value);
            if (ord != .eq) {
                return ord;
            }
        }

        const left_rowid = try left_row.readRowid();
        const right_rowid = try right_row.readRowid();
        return std.math.order(left_rowid.asI64(), right_rowid.asI64());
    }

    fn freeSegment(
        tmp_arena: *ArenaAllocator,
        segment_db: *SegmentDb,
        writer: *SegmentWriter,
    ) void {
        segment_db.free(tmp_arena, writer.handle) catch |e| {
            std.log.err(
                "error freeing segment blob {d}: {any}",
                .{ writer.handle.id, e },
            );
        };
    }
};

fn Limiter(comptime Cur: type) type {
    return struct {
        cur: Cur,
        limit: u32,
        index: u32,

        const Self = @This();

        pub fn init(cur: Cur, limit: u32) Self {
            return .{
                .cur = cur,
                .limit = limit,
                .index = 0,
            };
        }

        pub fn deinit(self: *Self) void {
            self.cur.deinit();
        }

        pub fn inner(self: *Self) *Cur {
            return &self.cur;
        }

        pub fn rewind(self: *Self) !void {
            try self.cur.rewind();
            self.index = 0;
        }

        pub fn eof(self: Self) bool {
            return self.cur.eof() or self.index >= self.limit;
        }

        pub fn next(self: *Self) !void {
            try self.cur.next();
            self.index += 1;
        }

        pub fn readRowid(self: Self) !ValueRef {
            return self.cur.readRowid();
        }

        pub fn readValue(self: Self, col_idx: usize) !ValueRef {
            return self.cur.readValue(col_idx);
        }

        pub fn sortKey(self: *Self, sort_key: []const usize) Cur.SortKey {
            return self.cur.sortKey(sort_key);
        }
    };
}

pub const Cursor = struct {
    segment_db: *SegmentDb,

    /// Allocator used to allocate all memory for this cursor that is tied to the
    /// lifecycle of the cursor. This memory is allocated together when the cursor is
    /// initialized and deallocated all once when the cursor is deinitialized.
    static_allocator: ArenaAllocator,

    column_types: []ColumnType,

    /// Set `row_group` before iterating. Set the record count on `row_group` to 0 to
    /// make an empty cursor.
    row_group: Index.Entry,

    rowid_segment: ?SegmentReader,
    segments: []?SegmentReader,

    /// Allocator used to allocate memory for values
    value_allocator: ArenaAllocator,

    index: u32,

    const Self = @This();

    /// Initializes the row group cursor with an undefined row group. Set the row group
    /// before iterating
    pub fn init(
        allocator: Allocator,
        segment_db: *SegmentDb,
        schema: *const Schema,
    ) !Self {
        const col_len = schema.columns.items.len;

        var arena = ArenaAllocator.init(allocator);
        errdefer arena.deinit();

        const column_types = try arena.allocator().alloc(ColumnType, col_len);
        for (schema.columns.items, 0..) |*col, idx| {
            column_types[idx] = col.column_type;
        }
        const row_group = .{
            .rowid_segment_id = undefined,
            .column_segment_ids = try arena.allocator().alloc(i64, col_len),
            .record_count = 0,
        };
        const segments = try arena.allocator().alloc(?SegmentReader, col_len);
        for (segments) |*seg| {
            seg.* = null;
        }
        var value_allocator = ArenaAllocator.init(allocator);
        errdefer value_allocator.deinit();

        return .{
            .static_allocator = arena,
            .segment_db = segment_db,
            .column_types = column_types,
            .row_group = row_group,
            .rowid_segment = null,
            .segments = segments,
            .value_allocator = value_allocator,
            .index = 0,
        };
    }

    pub fn deinit(self: *Self) void {
        if (self.rowid_segment) |*s| {
            s.handle.close();
        }
        for (self.segments) |*seg| {
            if (seg.*) |*s| {
                s.handle.close();
            }
        }
        self.value_allocator.deinit();
        self.static_allocator.deinit();
    }

    pub fn rowGroup(self: *Self) *Index.Entry {
        return &self.row_group;
    }

    pub fn reset(self: *Self) void {
        self.index = 0;

        if (self.rowid_segment) |*s| {
            s.handle.close();
        }
        self.rowid_segment = null;
        for (self.segments) |*seg| {
            if (seg.*) |*s| {
                s.handle.close();
            }
            seg.* = null;
        }

        // TODO make the retained capacity limit configurable
        _ = self.value_allocator.reset(.{ .retain_with_limit = 4000 });
    }

    pub fn eof(self: *Self) bool {
        return self.index >= self.row_group.record_count;
    }

    pub fn next(self: *Self) !void {
        if (self.rowid_segment) |*seg| {
            try seg.next();
        }
        for (self.segments) |*seg| {
            if (seg.*) |*s| {
                try s.next();
            }
        }
        // TODO make the retained capacity limit configurable
        _ = self.value_allocator.reset(.{ .retain_with_limit = 4000 });
        self.index += 1;
    }

    /// Does not check for eof
    pub fn skip(self: *Self, n: u32) !void {
        if (self.rowid_segment) |*seg| {
            for (0..n) |_| {
                try seg.next();
            }
        }
        for (self.segments) |*seg| {
            if (seg.*) |*s| {
                for (0..n) |_| {
                    try s.next();
                }
            }
        }
        self.index += n;
    }

    pub fn readRowid(self: *Self) !SegmentValue {
        if (self.rowid_segment == null) {
            try self.loadRowidSegment();
        }

        var seg = &self.rowid_segment.?;
        return seg.readNoAlloc();
    }

    pub fn readValue(self: *Self, col_idx: usize) !SegmentValue {
        if (self.segments[col_idx] == null) {
            try self.loadSegment(col_idx);
        }

        var seg = &self.segments[col_idx].?;
        return seg.read(self.value_allocator.allocator());
    }

    pub fn readInto(self: *Self, result: anytype, col_idx: usize) !void {
        if (self.segments[col_idx] == null) {
            try self.loadSegment(col_idx);
        }

        var seg = &self.segments[col_idx].?;
        try seg.readInto(self.value_allocator.allocator(), result);
    }

    fn loadRowidSegment(self: *Self) !void {
        const segment_reader = &self.rowid_segment;
        segment_reader.* = try SegmentReader.open(
            self.segment_db,
            ColumnType.Rowid.data_type,
            self.row_group.rowid_segment_id,
        );
        for (0..self.index) |_| {
            try segment_reader.*.?.next();
        }
    }

    fn loadSegment(self: *Self, col_idx: usize) !void {
        const segment_id = self.row_group.column_segment_ids[col_idx];

        const segment_reader = &self.segments[col_idx];
        segment_reader.* = try SegmentReader.open(
            self.segment_db,
            self.column_types[col_idx].data_type,
            segment_id,
        );
        for (0..self.index) |_| {
            try segment_reader.*.?.next();
        }
    }
};

test {
    _ = Index;
}

test "row group: create single from pending inserts" {
    const TableData = @import("TableData.zig");

    const conn = try Conn.openInMemory();
    defer conn.close();

    var arena = ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const table_name = "test";

    try schema_mod.Db.createTable(&arena, conn, table_name);
    var schema_db = schema_mod.Db.init(conn, table_name);
    defer schema_db.deinit();

    try SegmentDb.createTable(&arena, conn, table_name);
    var segment_db = try SegmentDb.init(&arena, conn, table_name);
    defer segment_db.deinit();

    const schema_def = try schema_mod.SchemaDef.parse(&arena, &[_][]const u8{
        "quadrant TEXT NOT NULL",
        "sector INTEGER NOT NULL",
        "size INTEGER NOT NULL",
        "SORT KEY (quadrant, sector)",
    });
    const schema = try Schema.create(&arena, &arena, &schema_db, schema_def);

    var table_data = try TableData.create(&arena, conn, table_name);
    defer table_data.deinit();

    var row_group_index = try Index.create(&arena, conn, table_name, &schema);
    defer row_group_index.deinit();

    var pending_inserts = try PendingInserts.create(
        &arena,
        conn,
        table_name,
        &schema,
        &table_data,
    );
    errdefer pending_inserts.deinit();

    var table_values = [_][3]MemoryValue{
        .{
            .{ .Text = "Alpha" }, .{ .Integer = 7 }, .{ .Integer = 100 },
        },
        .{
            .{ .Text = "Alpha" }, .{ .Integer = 9 }, .{ .Integer = 50 },
        },
        .{
            .{ .Text = "Gamma" }, .{ .Integer = 3 }, .{ .Integer = 75 },
        },
        .{
            .{ .Text = "Beta" }, .{ .Integer = 17 }, .{ .Integer = 105 },
        },
    };
    const rowids = [_]i64{ 1, 2, 4, 3 };

    for (&table_values) |*row| {
        _ = try pending_inserts.insert(&arena, MemoryTuple{ .values = row });
    }

    var new_row_group: Creator.NewRowGroup = undefined;
    new_row_group.row_group_entry.record_count = 0;
    new_row_group.start_sort_key = try arena.allocator()
        .alloc(MemoryValue, schema.sort_key.items.len);
    new_row_group.row_group_entry.column_segment_ids = try arena.allocator()
        .alloc(i64, schema.columns.items.len);

    {
        var creator = try Creator.init(
            std.testing.allocator,
            &arena,
            &segment_db,
            &schema,
            &row_group_index,
            &pending_inserts,
            4,
        );
        defer creator.deinit();

        var pend_inserts_cursor = Limiter(PendingInsertsCursor).init(
            try pending_inserts.cursor(&arena),
            10,
        );
        defer pend_inserts_cursor.deinit();

        _ = try creator.createSingle(&arena, &pend_inserts_cursor, &new_row_group);
    }

    var cursor = try Cursor.init(arena.allocator(), &segment_db, &schema);
    cursor.rowGroup().* = new_row_group.row_group_entry;

    var idx: usize = 0;
    while (!cursor.eof()) {
        const rowid = try cursor.readRowid();
        try std.testing.expectEqual(rowids[idx], rowid.asI64());

        // Rows are in sort key order not insert order. Use the expected rowid as the
        // index
        const row_idx: usize = @as(usize, @intCast(rowids[idx])) - 1;
        const quadrant = (try cursor.readValue(0)).asText();
        try std.testing.expectEqualStrings(table_values[row_idx][0].Text, quadrant);
        const sector = (try cursor.readValue(1)).asI64();
        try std.testing.expectEqual(table_values[row_idx][1].Integer, sector);

        idx += 1;
        try cursor.next();
    }
    try testing.expectEqual(@as(usize, 4), idx);
}

test "row group: create all" {
    const TableData = @import("TableData.zig");

    const conn = try Conn.openInMemory();
    defer conn.close();

    var arena = ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const table_name = "test";

    try schema_mod.Db.createTable(&arena, conn, table_name);
    var schema_db = schema_mod.Db.init(conn, table_name);
    defer schema_db.deinit();

    try SegmentDb.createTable(&arena, conn, table_name);
    var segment_db = try SegmentDb.init(&arena, conn, table_name);
    defer segment_db.deinit();

    const schema_def = try schema_mod.SchemaDef.parse(&arena, &[_][]const u8{
        "quadrant TEXT NOT NULL",
        "sector INTEGER NOT NULL",
        "size INTEGER NOT NULL",
        "SORT KEY (quadrant, sector)",
    });
    const schema = try Schema.create(&arena, &arena, &schema_db, schema_def);

    var table_data = try TableData.create(&arena, conn, table_name);
    defer table_data.deinit();

    var row_group_index = try Index.create(&arena, conn, table_name, &schema);
    defer row_group_index.deinit();

    var pending_inserts = try PendingInserts.create(
        &arena,
        conn,
        table_name,
        &schema,
        &table_data,
    );
    errdefer pending_inserts.deinit();

    var table_values = [_][3]MemoryValue{
        .{
            .{ .Text = "Alpha" }, .{ .Integer = 7 }, .{ .Integer = 100 },
        },
        .{
            .{ .Text = "Alpha" }, .{ .Integer = 9 }, .{ .Integer = 50 },
        },
        .{
            .{ .Text = "Gamma" }, .{ .Integer = 3 }, .{ .Integer = 75 },
        },
        .{
            .{ .Text = "Beta" }, .{ .Integer = 17 }, .{ .Integer = 105 },
        },
    };
    const rowids = [_]i64{ 1, 2, 4, 3 };

    for (&table_values) |*row| {
        _ = try pending_inserts.insert(&arena, MemoryTuple{ .values = row });
    }

    {
        var creator = try Creator.init(
            std.testing.allocator,
            &arena,
            &segment_db,
            &schema,
            &row_group_index,
            &pending_inserts,
            4,
        );
        defer creator.deinit();

        try creator.createAll(&arena);
    }

    var cursor = try Cursor.init(arena.allocator(), &segment_db, &schema);
    var rg_index_cursor = try row_group_index.cursor(&arena);
    rg_index_cursor.readEntry(cursor.rowGroup());

    var idx: usize = 0;
    while (!cursor.eof()) {
        const rowid = try cursor.readRowid();
        try std.testing.expectEqual(rowids[idx], rowid.asI64());

        // Rows are in sort key order not insert order. Use the expected rowid as the index
        const row_idx: usize = @as(usize, @intCast(rowids[idx])) - 1;
        const quadrant = (try cursor.readValue(0)).asText();
        try std.testing.expectEqualStrings(table_values[row_idx][0].Text, quadrant);
        const sector = (try cursor.readValue(1)).asI64();
        try std.testing.expectEqual(table_values[row_idx][1].Integer, sector);

        idx += 1;
        try cursor.next();
    }
    try testing.expectEqual(@as(usize, 4), idx);

    // All pending inserts should have been deleted
    var pend_inserts_cursor = try pending_inserts.cursor(&arena);
    try testing.expect(pend_inserts_cursor.eof());
}

pub fn benchRowGroupCreate() !void {
    const TableData = @import("TableData.zig");
    const row_group_len: u32 = 10_000;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    try tmp_dir.dir.makeDir(&tmp_dir.sub_path);
    var path_buf: [200]u8 = undefined;
    const tmp_path = try tmp_dir.parent_dir.realpath(&tmp_dir.sub_path, &path_buf);
    const db_path = try std.fs.path.joinZ(std.heap.page_allocator, &[_][]const u8{
        tmp_path,
        "bench.db",
    });

    const conn = try Conn.open(db_path);
    defer conn.close();

    var arena = ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const table_name = "test";

    try schema_mod.Db.createTable(&arena, conn, table_name);
    var schema_db = schema_mod.Db.init(conn, table_name);
    defer schema_db.deinit();

    try SegmentDb.createTable(&arena, conn, table_name);
    var segment_db = try SegmentDb.init(&arena, conn, table_name);
    defer segment_db.deinit();

    const schema_def = try schema_mod.SchemaDef.parse(&arena, &[_][]const u8{
        "quadrant TEXT NOT NULL",
        "sector INTEGER NOT NULL",
        "size INTEGER NULL",
        "gravity FLOAT NULL",
        "name TEXT NOT NULL",
        "SORT KEY (quadrant, sector)",
    });
    const schema = try Schema.create(&arena, &arena, &schema_db, schema_def);

    var table_data = try TableData.create(&arena, conn, table_name);
    defer table_data.deinit();

    var row_group_index = try Index.create(&arena, conn, table_name, &schema);
    defer row_group_index.deinit();

    var pending_inserts = try PendingInserts.create(
        &arena,
        conn,
        table_name,
        &schema,
        &table_data,
    );
    errdefer pending_inserts.deinit();

    const seed = @as(u64, @truncate(@as(u128, @bitCast(std.time.nanoTimestamp()))));
    var prng = std.rand.DefaultPrng.init(seed);

    // Create a row group from pending inserts only (no merge)

    try conn.exec("BEGIN");
    const start_insert = std.time.microTimestamp();
    for (0..row_group_len) |_| {
        var row = randomRow(&prng);
        _ = try pending_inserts.insert(&arena, MemoryTuple{ .values = &row });
    }
    try conn.exec("COMMIT");
    const end_insert = std.time.microTimestamp();
    std.log.err("insert pending insert: total: {d} micros, per insert: {d} micros", .{
        end_insert - start_insert,
        @divTrunc(end_insert - start_insert, row_group_len),
    });

    {
        var start: i64 = undefined;
        {
            var creator = try Creator.init(
                std.heap.page_allocator,
                &arena,
                &segment_db,
                &schema,
                &row_group_index,
                &pending_inserts,
                row_group_len,
            );
            defer creator.deinit();

            try conn.exec("BEGIN");
            start = std.time.microTimestamp();
            try creator.createAll(&arena);
        }
        try conn.exec("COMMIT");
        const end = std.time.microTimestamp();
        std.log.err("create row group: {d} micros", .{end - start});
    }

    // Create a row group with a merge

    try conn.exec("BEGIN");
    for (0..(row_group_len * 3)) |_| {
        var row = randomRow(&prng);
        _ = try pending_inserts.insert(&arena, MemoryTuple{ .values = &row });
    }
    try conn.exec("COMMIT");

    {
        var start: i64 = undefined;
        {
            var creator = try Creator.init(
                std.heap.page_allocator,
                &arena,
                &segment_db,
                &schema,
                &row_group_index,
                &pending_inserts,
                row_group_len,
            );
            defer creator.deinit();

            try conn.exec("BEGIN");
            start = std.time.microTimestamp();
            try creator.createAll(&arena);
        }
        try conn.exec("COMMIT");
        const end = std.time.microTimestamp();
        std.log.err("create row group: {d} micros", .{end - start});
    }
}

const quadrants = [_][]const u8{ "Alpha", "Beta", "Gamma", "Delta" };

fn randomRow(prng: *std.rand.DefaultPrng) [5]MemoryValue {
    const quadrant = quadrants[prng.random().intRangeLessThan(usize, 0, quadrants.len)];

    return [5]MemoryValue{
        .{ .Text = quadrant },
        .{ .Integer = prng.random().int(i64) },
        .{ .Integer = 100 },
        .{ .Float = 3.75 },
        .{ .Text = "Veridian 3" },
    };
}
