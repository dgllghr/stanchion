const std = @import("std");
const fmt = std.fmt;
const math = std.math;
const mem = std.mem;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const Conn = @import("sqlite3/Conn.zig");
const Stmt = @import("sqlite3/Stmt.zig");
const ValueRef = @import("sqlite3/value.zig").Ref;
const sqlite_c = @import("sqlite3/c.zig").c;

const stmt_cell = @import("stmt_cell.zig");

const s = @import("schema.zig");
const DataType = s.ColumnType.DataType;
const Schema = s.Schema;

const segment = @import("segment.zig");
const SegmentDb = segment.Db;

const Self = @This();

const StmtCell = stmt_cell.StmtCell(Self);

conn: Conn,

vtab_table_name: []const u8,
columns_len: usize,
sort_key: []const usize,

next_rowid: i64,
last_write_rowid: i64,

insert_entry: StmtCell,

/// This StmtCell holds the Stmt that can be held as a RowGroupEntryHandle, so only 1
/// RowGroupEntryHandle can be held at a time
get_row_group_entry: StmtCell,

find_preceding_row_group: StmtCell,

/// These StmtCells hold the Stmts that can be held as an StagedInsertsIterator, so only
/// 1 StagedInsertsIterator can be held at a time
inserts_iterator: StmtCell,
inserts_iterator_from_start: StmtCell,

delete_staged_inserts_range: StmtCell,
entries_iterator: StmtCell,

load_next_rowid: Stmt,
update_next_rowid: Stmt,

/// The primary index holds multiple types of entries. This is the tag that
/// differentiates the entries.
const EntryType = enum(u8) {
    TableState = 1,
    RowGroup = 2,
    Insert = 3,

    fn bind(self: EntryType, stmt: Stmt, index: usize) !void {
        try stmt.bind(.Int32, index, @intCast(@intFromEnum(self)));
    }
};

//
// Init
//

/// The `static_arena` is used to allocate any memory that lives for the lifetime of the
/// primary index. The `tmp_arena` is used to allocate memory that can be freed any time
/// after the function returns.
pub fn create(
    tmp_arena: *ArenaAllocator,
    conn: Conn,
    vtab_table_name: []const u8,
    schema: *const Schema,
) !Self {
    const ddl_formatter = CreateTableDdlFormatter{
        .vtab_table_name = vtab_table_name,
        .schema = schema,
    };
    const ddl = try fmt.allocPrintZ(tmp_arena.allocator(), "{s}", .{ddl_formatter});
    try conn.exec(ddl);

    var self = Self{
        .conn = conn,
        .vtab_table_name = vtab_table_name,
        .columns_len = schema.columns.items.len,
        .sort_key = schema.sort_key.items,
        .next_rowid = 1,
        .last_write_rowid = 1,
        .insert_entry = StmtCell.init(&insertEntryDml),
        .get_row_group_entry = StmtCell.init(&readRowGroupEntryQuery),
        .find_preceding_row_group = StmtCell.init(&precedingRowGroupQuery),
        .inserts_iterator = StmtCell.init(&insertIteratorQuery),
        .inserts_iterator_from_start = StmtCell.init(&insertIteratorFromStartQuery),
        .delete_staged_inserts_range = StmtCell.init(&deleteInsertsRangeDml),
        .entries_iterator = StmtCell.init(&entriesIteratorQuery),
        .load_next_rowid = undefined,
        .update_next_rowid = undefined,
    };

    try self.insertTableStateEntry(tmp_arena, schema);
    try self.initTableStateStmts(tmp_arena, schema);

    return self;
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
            \\CREATE TABLE "{s}_primaryindex" (
        ,
            .{self.vtab_table_name},
        );
        for (self.schema.sort_key.items, 0..) |sk_index, sk_rank| {
            const col = &self.schema.columns.items[sk_index];
            const data_type = DataType.SqliteFormatter{
                .data_type = col.column_type.data_type,
            };
            try writer.print("sk_value_{d} {s} NOT NULL,", .{ sk_rank, data_type });
        }
        // The `col_N` columns are uesd for storing values for insert entries and
        // segment IDs for row group entries
        for (0..self.schema.columns.items.len) |rank| {
            try writer.print("col_{d} ANY NULL,", .{rank});
        }
        try writer.print(
        // Used by row group entry type (rowid segment id) and table status (next
        // rowid)
            \\col_rowid INTEGER NULL,
            // Used by all entry types. The rowid value that is part of the sort key
            \\rowid INTEGER NOT NULL,
            // Only used by the row group entry type to count the number of records
            // in the row group
            \\record_count INTEGER NULL,
            // Code that specifies the type of the entry
            \\entry_type INTEGER NOT NULL,
            \\PRIMARY KEY (
        , .{});
        for (0..self.schema.sort_key.items.len) |sk_rank| {
            try writer.print("sk_value_{d},", .{sk_rank});
        }
        try writer.print("rowid)", .{});
        // TODO partial unique index on table status entry?
        // TODO in tests it would be nice to add some check constraints to ensure
        //      data is populated correctly based on the entry_type
        try writer.print(") STRICT, WITHOUT ROWID", .{});
    }
};

pub fn open(
    tmp_arena: *ArenaAllocator,
    conn: Conn,
    vtab_table_name: []const u8,
    schema: *const Schema,
) !Self {
    var self = Self{
        .conn = conn,
        .vtab_table_name = vtab_table_name,
        .columns_len = schema.columns.items.len,
        .sort_key = schema.sort_key.items,
        .next_rowid = 0,
        .last_write_rowid = 0,
        .insert_entry = StmtCell.init(&insertEntryDml),
        .get_row_group_entry = StmtCell.init(&readRowGroupEntryQuery),
        .find_preceding_row_group = StmtCell.init(&precedingRowGroupQuery),
        .inserts_iterator = StmtCell.init(&insertIteratorQuery),
        .inserts_iterator_from_start = StmtCell.init(&insertIteratorFromStartQuery),
        .delete_staged_inserts_range = StmtCell.init(&deleteInsertsRangeDml),
        .entries_iterator = StmtCell.init(&entriesIteratorQuery),
        .load_next_rowid = undefined,
        .update_next_rowid = undefined,
    };

    try self.initTableStateStmts(tmp_arena, schema);
    try self.loadNextRowid();

    return self;
}

//
// Deinit
//

/// Must be called before memory is freed by the `static_arena` provided to `create` or
/// `open`
pub fn deinit(self: *Self) void {
    self.insert_entry.deinit();
    self.get_row_group_entry.deinit();
    self.find_preceding_row_group.deinit();
    self.inserts_iterator.deinit();
    self.inserts_iterator_from_start.deinit();
    self.delete_staged_inserts_range.deinit();
    self.entries_iterator.deinit();
    self.load_next_rowid.deinit();
    self.update_next_rowid.deinit();
}

//
// Drop
//

pub fn drop(self: *Self, tmp_arena: *ArenaAllocator) !void {
    const query = try fmt.allocPrintZ(
        tmp_arena.allocator(),
        \\DROP TABLE "{s}_primaryindex"
    ,
        .{self.vtab_table_name},
    );
    try self.conn.exec(query);
}

//
// Table state
//

fn insertTableStateEntry(
    self: *Self,
    tmp_arena: *ArenaAllocator,
    schema: *const Schema,
) !void {
    const stmt = try self.insert_entry.getStmt(tmp_arena, self);
    defer self.insert_entry.reset();

    try EntryType.TableState.bind(stmt, 1);
    // Record count (null)
    try stmt.bindNull(2);
    // Use col_rowid as the next rowid column
    try stmt.bind(.Int64, 4, 1);

    // Keep the table state entry first in the table by binding the minimum possible
    // values to the sk_value_* columns and -1 to the rowid column
    // Rowid column
    try stmt.bind(.Int64, 3, -1);
    for (schema.sort_key.items, 5..) |rank, idx| {
        const data_type = schema.columns.items[rank].column_type.data_type;
        try bindMinValue(stmt, idx, data_type);
    }

    // The col_* columns are unused by the table state entry

    stmt.exec() catch |e| {
        const err_msg = sqlite_c.sqlite3_errmsg(self.conn.conn);
        std.log.err("error executing insert: {s}", .{err_msg});
        return e;
    };
}

fn initTableStateStmts(
    self: *Self,
    tmp_arena: *ArenaAllocator,
    schema: *const Schema,
) !void {
    self.load_next_rowid = try self.conn.prepare(
        try loadNextRowidQuery(self, tmp_arena),
    );
    errdefer self.load_next_rowid.deinit();
    self.update_next_rowid = try self.conn.prepare(
        try updateNextRowidDml(self, tmp_arena),
    );
    errdefer self.update_next_rowid.deinit();

    for (schema.sort_key.items, 1..) |rank, idx| {
        const data_type = schema.columns.items[rank].column_type.data_type;
        try bindMinValue(self.load_next_rowid, idx, data_type);
        // The first bound parameter in `update_next_rowid` is the new rowid
        try bindMinValue(self.update_next_rowid, idx + 1, data_type);
    }
}

fn bindMinValue(stmt: Stmt, idx: usize, data_type: DataType) !void {
    switch (data_type) {
        .Boolean => try stmt.bind(.Int32, idx, 0),
        .Integer => try stmt.bind(.Int64, idx, math.minInt(i64)),
        .Float => try stmt.bind(.Float, idx, math.floatMin(f64)),
        .Text => try stmt.bind(.Text, idx, ""),
        .Blob => try stmt.bind(.Blob, idx, ""),
    }
}

fn loadNextRowidQuery(self: *const Self, arena: *ArenaAllocator) ![]const u8 {
    return fmt.allocPrintZ(arena.allocator(),
        \\SELECT col_rowid
        \\FROM "{s}_primaryindex"
        \\WHERE ({s}, rowid) = ({s}, -1) AND entry_type = {d}
    , .{
        self.vtab_table_name,
        ColumnListFormatter("sk_value_{d}"){ .len = self.sort_key.len },
        ParameterListFormatter{ .len = self.sort_key.len },
        @intFromEnum(EntryType.TableState),
    });
}

pub fn loadNextRowid(self: *Self) !void {
    defer self.load_next_rowid.resetExec() catch {};

    _ = try self.load_next_rowid.next();

    self.next_rowid = self.load_next_rowid.read(.Int64, false, 0);
    self.last_write_rowid = self.next_rowid;
}

fn updateNextRowidDml(self: *const Self, arena: *ArenaAllocator) ![]const u8 {
    return fmt.allocPrintZ(arena.allocator(),
        \\UPDATE "{s}_primaryindex"
        \\SET col_rowid = ?
        \\WHERE ({s}, rowid) = ({s}, -1) AND entry_type = {d}
    , .{
        self.vtab_table_name,
        ColumnListFormatter("sk_value_{d}"){ .len = self.sort_key.len },
        ParameterListFormatter{ .len = self.sort_key.len },
        @intFromEnum(EntryType.TableState),
    });
}

pub fn persistNextRowid(self: *Self) !void {
    if (self.last_write_rowid != self.next_rowid) {
        defer self.update_next_rowid.resetExec() catch {};

        try self.update_next_rowid.bind(.Int64, 1, self.next_rowid);
        try self.update_next_rowid.exec();

        self.last_write_rowid = self.next_rowid;
    }
}

//
// Row Group
//

pub const RowGroupEntry = struct {
    rowid_segment_id: i64,
    column_segment_ids: []i64,
    record_count: u32,

    pub fn deinit(self: *@This(), allocator: Allocator) void {
        allocator.free(self.column_segment_ids);
    }
};

pub fn insertRowGroupEntry(
    self: *Self,
    tmp_arena: *ArenaAllocator,
    sort_key: anytype,
    rowid: i64,
    entry: *const RowGroupEntry,
) !void {
    const stmt = try self.insert_entry.getStmt(tmp_arena, self);
    defer self.insert_entry.reset();

    try EntryType.RowGroup.bind(stmt, 1);
    try stmt.bind(.Int64, 2, @intCast(entry.record_count));
    try stmt.bind(.Int64, 3, rowid);
    try stmt.bind(.Int64, 4, entry.rowid_segment_id);
    for (sort_key, 5..) |sk_value, idx| {
        try sk_value.bind(stmt, idx);
    }
    const col_idx_start = self.sort_key.len + 5;
    for (0..self.columns_len, col_idx_start..) |rank, idx| {
        try stmt.bind(.Int64, idx, entry.column_segment_ids[rank]);
    }

    stmt.exec() catch |e| {
        const err_msg = sqlite_c.sqlite3_errmsg(self.conn.conn);
        std.log.err("error executing insert: {s}", .{err_msg});
        return e;
    };
}

fn readRowGroupEntryQuery(self: *const Self, arena: *ArenaAllocator) ![]const u8 {
    return fmt.allocPrintZ(arena.allocator(),
        \\SELECT record_count, col_rowid, {s}
        \\FROM "{s}_primaryindex"
        \\WHERE ({s}, rowid) = ({s}, ?) AND entry_type = {d}
    , .{
        ColumnListFormatter("col_{d}"){ .len = self.columns_len },
        self.vtab_table_name,
        ColumnListFormatter("sk_value_{d}"){ .len = self.sort_key.len },
        ParameterListFormatter{ .len = self.sort_key.len },
        @intFromEnum(EntryType.RowGroup),
    });
}

pub fn readRowGroupEntry(
    self: *Self,
    tmp_arena: *ArenaAllocator,
    entry: *RowGroupEntry,
    sort_key: anytype,
    rowid: i64,
) !void {
    std.debug.assert(entry.column_segment_ids.len == self.columns_len);

    const stmt = try self.get_row_group_entry.getStmt(tmp_arena, self);
    defer self.get_row_group_entry.reset();

    for (0..sort_key.valuesLen(), 1..) |sk_idx, idx| {
        try stmt.bindSqliteValue(idx, sort_key.readValue(sk_idx));
    }
    try stmt.bind(.Int64, self.sort_key.len + 1, rowid);

    _ = try stmt.next();

    entry.record_count = @intCast(stmt.read(.Int64, false, 0));
    entry.rowid_segment_id = stmt.read(.Int64, false, 1);
    for (entry.column_segment_ids, 2..) |*seg_id, idx| {
        seg_id.* = stmt.read(.Int64, false, idx);
    }
}

/// Handle to a row group entry in the primary index. This handle holds an open statement
/// in sqlite and must be released with `deinit` after use. Only one of these handles can
/// be open in the primary index at a time.
pub const RowGroupEntryHandle = union(enum) {
    start,
    row_group: struct {
        /// Owned by the PrimaryIndex
        cell: *StmtCell,
        /// Owned by the PrimaryIndex
        stmt: Stmt,
        sort_key_len: usize,
        rowid: i64,

        pub const SortKey = struct {
            sort_key_len: usize,
            stmt: Stmt,

            pub fn valuesLen(self: @This()) usize {
                return self.sort_key_len;
            }

            pub fn readValue(self: @This(), index: usize) ValueRef {
                return self.stmt.readSqliteValue(index);
            }
        };

        pub fn sortKey(self: @This()) SortKey {
            return .{ .stmt = self.stmt, .sort_key_len = self.sort_key_len };
        }
    },

    pub fn deinit(self: *@This()) void {
        switch (self.*) {
            .row_group => |*rg| rg.cell.reset(),
            else => {},
        }
    }
};

fn precedingRowGroupQuery(self: *const Self, arena: *ArenaAllocator) ![]const u8 {
    return fmt.allocPrintZ(arena.allocator(),
        \\SELECT {s}, rowid
        \\FROM "{s}_primaryindex"
        \\WHERE ({s}, rowid) <= ({s}, ?) AND entry_type = {d}
        \\ORDER BY {s}, rowid DESC
        \\LIMIT 1
    , .{
        ColumnListFormatter("sk_value_{d}"){ .len = self.sort_key.len },
        self.vtab_table_name,
        ColumnListFormatter("sk_value_{d}"){ .len = self.sort_key.len },
        ParameterListFormatter{ .len = self.sort_key.len },
        @intFromEnum(EntryType.RowGroup),
        ColumnListFormatter("sk_value_{d} DESC"){ .len = self.sort_key.len },
    });
}

/// Finds the row group that precedes the provided row based on the sort key and rowid.
/// The returned handle  can be used to iterate the insert entries following the row
/// group and must be deinitialized by the caller after use.
pub fn precedingRowGroup(
    self: *Self,
    tmp_arena: *ArenaAllocator,
    rowid: i64,
    row: anytype,
) !RowGroupEntryHandle {
    const stmt = try self.find_preceding_row_group.getStmt(tmp_arena, self);
    errdefer self.find_preceding_row_group.reset();

    for (self.sort_key, 1..) |col_idx, idx| {
        const value = try row.readValue(col_idx);
        try value.bind(stmt, idx);
    }
    try stmt.bind(.Int64, self.sort_key.len + 1, rowid);

    const has_row_group = try stmt.next();
    if (!has_row_group) {
        self.find_preceding_row_group.reset();
        return .start;
    }

    const rg_rowid = stmt.read(.Int64, false, self.sort_key.len);

    return .{ .row_group = .{
        .cell = &self.find_preceding_row_group,
        .stmt = stmt,
        .sort_key_len = self.sort_key.len,
        .rowid = rg_rowid,
    } };
}

//
// Staged inserts
//

pub fn insertInsertEntry(self: *Self, tmp_arena: *ArenaAllocator, values: anytype) !i64 {
    const rowid = self.next_rowid;
    self.next_rowid += 1;
    const stmt = try self.insert_entry.getStmt(tmp_arena, self);
    defer self.insert_entry.reset();

    try EntryType.Insert.bind(stmt, 1);
    try stmt.bindNull(2);
    try stmt.bind(.Int64, 3, rowid);
    try stmt.bindNull(4);
    for (self.sort_key, 5..) |rank, idx| {
        const value = try values.readValue(rank);
        try value.bind(stmt, idx);
    }
    const col_idx_start = self.sort_key.len + 5;
    // Sort key values are duplicated into both the sort key columns and the column value
    // columns
    for (0..self.columns_len, col_idx_start..) |rank, idx| {
        const value = try values.readValue(rank);
        try value.bind(stmt, idx);
    }

    stmt.exec() catch |e| {
        const err_msg = sqlite_c.sqlite3_errmsg(self.conn.conn);
        std.log.err("error executing insert: {s}", .{err_msg});
        return e;
    };

    return rowid;
}

test "primary index: insert" {
    const MemoryTuple = @import("value.zig").MemoryTuple;
    const MemoryValue = @import("value.zig").MemoryValue;

    const conn = try @import("sqlite3/Conn.zig").openInMemory();
    defer conn.close();

    const ArrayListUnmanaged = std.ArrayListUnmanaged;
    const Column = @import("schema/Column.zig");
    var columns = ArrayListUnmanaged(Column){};
    defer columns.deinit(std.testing.allocator);
    try columns.appendSlice(std.testing.allocator, &[_]Column{
        .{
            .rank = 0,
            .name = "foo",
            .column_type = .{ .data_type = .Blob, .nullable = false },
            .sk_rank = 0,
        },
        .{
            .rank = 1,
            .name = "bar",
            .column_type = .{ .data_type = .Integer, .nullable = true },
            .sk_rank = null,
        },
    });
    var sort_key = ArrayListUnmanaged(usize){};
    defer sort_key.deinit(std.testing.allocator);
    try sort_key.append(std.testing.allocator, 0);
    const schema = Schema{
        .columns = columns,
        .sort_key = sort_key,
    };

    var arena = ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var primary_index = try create(&arena, conn, "test", &schema);
    defer primary_index.deinit();

    var row = [_]MemoryValue{
        .{ .Blob = "magnitude" },
        .{ .Integer = 100 },
    };
    _ = try primary_index.insertInsertEntry(&arena, MemoryTuple{
        .values = &row,
    });

    row = [_]MemoryValue{
        .{ .Blob = "amplitude" },
        .{ .Integer = 7 },
    };
    _ = try primary_index.insertInsertEntry(&arena, MemoryTuple{
        .values = &row,
    });
}

/// Iterates until a row group entry is found or the end of the table is reached
pub const StagedInsertsIterator = struct {
    stmt: Stmt,
    cell: *StmtCell,
    starts_at_row_group: bool,

    pub fn deinit(self: *@This()) void {
        self.cell.reset();
    }

    pub fn restart(self: *@This()) !void {
        try self.stmt.resetExec();

        // Skip the row group (the first row) when the iterator starts at a row group
        if (self.starts_at_row_group) {
            _ = try self.stmt.next();
        }
    }

    pub fn next(self: *@This()) !bool {
        const has_next = try self.stmt.next();
        if (!has_next) {
            return false;
        }
        const entry_type: EntryType = @enumFromInt(self.stmt.read(.Int32, false, 0));
        return entry_type != .RowGroup;
    }

    /// Does not check for eof
    pub fn skip(self: *@This(), n: u32) !void {
        // TODO would it be faster to have a limit parameter in the query and restart
        //      execution after changing the limit?
        for (0..n) |_| {
            _ = try self.stmt.next();
        }
    }

    pub fn readRowid(self: *@This()) !ValueRef {
        return self.stmt.readSqliteValue(1);
    }

    pub fn readValue(self: *@This(), idx: usize) !ValueRef {
        return self.stmt.readSqliteValue(idx + 2);
    }
};

fn insertIteratorQuery(self: *const Self, arena: *ArenaAllocator) ![]const u8 {
    return fmt.allocPrintZ(arena.allocator(),
        \\SELECT entry_type, rowid, {s}
        \\FROM "{s}_primaryindex"
        \\WHERE ({s}, rowid) >= ({s}, ?) AND entry_type IN ({d}, {d})
        \\ORDER BY {s}, rowid ASC
    , .{
        ColumnListFormatter("col_{d}"){ .len = self.columns_len },
        self.vtab_table_name,
        ColumnListFormatter("sk_value_{d}"){ .len = self.sort_key.len },
        ParameterListFormatter{ .len = self.sort_key.len },
        @intFromEnum(EntryType.Insert),
        @intFromEnum(EntryType.RowGroup),
        ColumnListFormatter("sk_value_{d} ASC"){ .len = self.sort_key.len },
    });
}

fn insertIteratorFromStartQuery(self: *const Self, arena: *ArenaAllocator) ![]const u8 {
    return fmt.allocPrintZ(arena.allocator(),
        \\SELECT entry_type, rowid, {s}
        \\FROM "{s}_primaryindex"
        \\WHERE entry_type IN ({d}, {d})
        \\ORDER BY {s}, rowid ASC
    , .{
        ColumnListFormatter("col_{d}"){ .len = self.columns_len },
        self.vtab_table_name,
        @intFromEnum(EntryType.Insert),
        @intFromEnum(EntryType.RowGroup),
        ColumnListFormatter("sk_value_{d} ASC"){ .len = self.sort_key.len },
    });
}

/// Create an iterator that iterates over all pending inserts after the provided row
/// group and before the subsequent row group. Only one of these iterators can be active
/// at a time.
pub fn stagedInserts(
    self: *Self,
    tmp_arena: *ArenaAllocator,
    preceding_row_group_handle: *const RowGroupEntryHandle,
) !StagedInsertsIterator {
    var iter = StagedInsertsIterator{
        .stmt = undefined,
        .cell = undefined,
        .starts_at_row_group = undefined,
    };
    switch (preceding_row_group_handle.*) {
        .row_group => |*handle| {
            iter.stmt = try self.inserts_iterator.getStmt(tmp_arena, self);
            errdefer self.inserts_iterator.reset();

            const sort_key = handle.sortKey();
            for (0..self.sort_key.len) |idx| {
                try iter.stmt.bindSqliteValue(idx + 1, sort_key.readValue(idx));
            }
            try iter.stmt.bind(.Int64, self.sort_key.len + 1, handle.rowid);

            const row_group_exists = try iter.stmt.next();
            std.debug.assert(row_group_exists);

            iter.cell = &self.inserts_iterator;
            iter.starts_at_row_group = true;
        },
        .start => {
            iter.stmt = try self.inserts_iterator_from_start.getStmt(tmp_arena, self);
            iter.cell = &self.inserts_iterator_from_start;
            iter.starts_at_row_group = false;
        },
    }

    return iter;
}

fn deleteInsertsRangeDml(self: *const Self, arena: *ArenaAllocator) ![]const u8 {
    return fmt.allocPrintZ(arena.allocator(),
        \\WITH end_rg_entry AS (
        \\  SELECT {s}, rowid
        \\  FROM "{s}_primaryindex"
        \\  WHERE ({s}, rowid) > ({s}, ?) AND entry_type = {d}
        \\  ORDER BY {s}, rowid ASC
        \\  LIMIT 1
        \\)
        \\DELETE FROM "{s}_primaryindex"
        \\WHERE ({s}, rowid) IN (
        \\  SELECT {s}, pidx.rowid
        \\  FROM "{s}_primaryindex" pidx
        \\  LEFT JOIN end_rg_entry e
        \\  WHERE ({s}, pidx.rowid) >= ({s}, ?) AND pidx.entry_type = {d}
        \\        AND (e.rowid IS NULL OR ({s}, pidx.rowid) < ({s}, e.rowid)))
    , .{
        // CTE
        ColumnListFormatter("sk_value_{d}"){ .len = self.sort_key.len },
        self.vtab_table_name,
        ColumnListFormatter("sk_value_{d}"){ .len = self.sort_key.len },
        ParameterListFormatter{ .len = self.sort_key.len },
        @intFromEnum(EntryType.RowGroup),
        ColumnListFormatter("sk_value_{d} ASC"){ .len = self.sort_key.len },
        // Delete
        self.vtab_table_name,
        ColumnListFormatter("sk_value_{d}"){ .len = self.sort_key.len },
        // Subquery
        ColumnListFormatter("pidx.sk_value_{d}"){ .len = self.sort_key.len },
        self.vtab_table_name,
        ColumnListFormatter("pidx.sk_value_{d}"){ .len = self.sort_key.len },
        ParameterListFormatter{ .len = self.sort_key.len },
        @intFromEnum(EntryType.Insert),
        ColumnListFormatter("pidx.sk_value_{d}"){ .len = self.sort_key.len },
        ColumnListFormatter("e.sk_value_{d}"){ .len = self.sort_key.len },
    });
}

/// Deletes all staged inserts with sort key >= (`start_sort_key`..., `rowid`) and < the
/// next row group entry in the table (or the end of the table)
pub fn deleteStagedInsertsRange(
    self: *Self,
    tmp_arena: *ArenaAllocator,
    start_sort_key: anytype,
    start_rowid: i64,
) !void {
    const stmt = try self.delete_staged_inserts_range.getStmt(tmp_arena, self);
    defer self.delete_staged_inserts_range.reset();

    for (start_sort_key, 1..) |sk_value, idx| {
        try sk_value.bind(stmt, idx);
    }
    try stmt.bind(.Int64, start_sort_key.len + 1, start_rowid);
    for (start_sort_key, (start_sort_key.len + 2)..) |sk_value, idx| {
        try sk_value.bind(stmt, idx);
    }
    try stmt.bind(.Int64, (start_sort_key.len * 2) + 2, start_rowid);

    stmt.exec() catch |e| {
        const err_msg = sqlite_c.sqlite3_errmsg(self.conn.conn);
        std.log.err("error deleting inserts: {s}", .{err_msg});
        return e;
    };
}

//
// Cursor
//

pub const Cursor = struct {
    stmt: Stmt,
    cell: *StmtCell,
    eof: bool,

    pub fn deinit(self: *@This()) void {
        self.cell.reset();
    }

    pub fn next(self: *@This()) !void {
        const has_next = try self.stmt.next();
        self.eof = !has_next;
    }

    pub fn entryType(self: @This()) EntryType {
        return @enumFromInt(self.stmt.read(.Int32, false, 0));
    }

    pub fn readRowGroupEntry(self: @This(), entry: *RowGroupEntry) !void {
        std.debug.assert(self.entryType() == .RowGroup);
        entry.record_count = @intCast(self.stmt.read(.Int64, false, 2));
        entry.rowid_segment_id = self.stmt.read(.Int64, false, 3);
        for (entry.column_segment_ids, 4..) |*seg_id, idx| {
            seg_id.* = self.stmt.read(.Int64, false, idx);
        }
    }

    pub fn readRowid(self: @This()) !ValueRef {
        return self.stmt.readSqliteValue(1);
    }

    pub fn read(self: @This(), col_idx: usize) !ValueRef {
        return self.stmt.readSqliteValue(col_idx + 4);
    }
};

fn entriesIteratorQuery(self: *const Self, arena: *ArenaAllocator) ![]const u8 {
    return fmt.allocPrintZ(arena.allocator(),
        \\SELECT entry_type, rowid, record_count, col_rowid, {s}
        \\FROM "{s}_primaryindex"
        \\WHERE entry_type IN ({d}, {d})
        \\ORDER BY {s}, rowid ASC
    , .{
        ColumnListFormatter("col_{d}"){ .len = self.columns_len },
        self.vtab_table_name,
        @intFromEnum(EntryType.RowGroup),
        @intFromEnum(EntryType.Insert),
        ColumnListFormatter("sk_value_{d} ASC"){ .len = self.sort_key.len },
    });
}

pub fn cursor(self: *Self, tmp_arena: *ArenaAllocator) !Cursor {
    return .{
        .stmt = try self.entries_iterator.getStmt(tmp_arena, self),
        .cell = &self.entries_iterator,
        .eof = false,
    };
}

//
// Common
//

fn insertEntryDml(self: *const Self, arena: *ArenaAllocator) ![]const u8 {
    return fmt.allocPrintZ(arena.allocator(),
        \\INSERT INTO "{s}_primaryindex" (
        \\  entry_type, record_count, rowid, col_rowid, {s}, {s}
        \\) VALUES (?, ?, ?, ?, {s})
    , .{
        self.vtab_table_name,
        ColumnListFormatter("sk_value_{d}"){ .len = self.sort_key.len },
        ColumnListFormatter("col_{d}"){ .len = self.columns_len },
        ParameterListFormatter{ .len = self.columns_len + self.sort_key.len },
    });
}

fn ColumnListFormatter(comptime column_name_template: []const u8) type {
    return struct {
        len: usize,

        pub fn format(
            self: @This(),
            comptime _: []const u8,
            _: std.fmt.FormatOptions,
            writer: anytype,
        ) !void {
            for (0..self.len) |idx| {
                if (idx > 0) {
                    try writer.print(",", .{});
                }
                try writer.print(column_name_template, .{idx});
            }
        }
    };
}

const ParameterListFormatter = struct {
    len: usize,

    pub fn format(
        self: @This(),
        comptime _: []const u8,
        _: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        for (0..self.len) |idx| {
            if (idx > 0) {
                try writer.print(",", .{});
            }
            try writer.print("?", .{});
        }
    }
};
