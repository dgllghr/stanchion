const std = @import("std");
const fmt = std.fmt;
const mem = std.mem;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const Conn = @import("sqlite3/Conn.zig");
const Stmt = @import("sqlite3/Stmt.zig");
const ValueRef = @import("sqlite3/value.zig").Ref;
const sqlite_c = @import("sqlite3/c.zig").c;

const StmtCell = @import("StmtCell.zig");

const s = @import("schema.zig");
const DataType = s.ColumnType.DataType;
const Schema = s.Schema;

const segment = @import("segment.zig");
const SegmentDb = segment.Db;

const Self = @This();

conn: Conn,

table_name: []const u8,
columns_len: usize,
sort_key: []const usize,

next_rowid: i64,

insert_entry: StmtCell,
get_row_group_entry: StmtCell,
find_preceding_row_group: StmtCell,
inserts_iterator: StmtCell,
inserts_iterator_from_start: StmtCell,
delete_staged_inserts_range: StmtCell,
entries_iterator: StmtCell,

sort_key_buf: []ValueRef,

/// The primary index holds two types of entries. This is the enum that differentiates
/// the entries.
const EntryType = enum(u8) {
    RowGroup = 1,
    Insert = 2,

    fn bind(self: EntryType, stmt: Stmt, index: usize) !void {
        try stmt.bind(.Int32, index, @intCast(@intFromEnum(self)));
    }
};

//
// Create
//

/// The `static_arena` is used to allocate any memory that lives for the lifetime of the
/// primary index. The `tmp_arena` is used to allocate memory that can be freed any time
/// after the function returns.
pub fn create(
    static_arena: *ArenaAllocator,
    tmp_arena: *ArenaAllocator,
    conn: Conn,
    table_name: []const u8,
    schema: *const Schema,
) !Self {
    const ddl_formatter = CreateTableDdlFormatter{
        .table_name = table_name,
        .schema = schema,
    };
    const ddl = try fmt.allocPrintZ(tmp_arena.allocator(), "{s}", .{ddl_formatter});
    try conn.exec(ddl);
    return open(static_arena, conn, table_name, schema, 1);
}

const CreateTableDdlFormatter = struct {
    table_name: []const u8,
    schema: *const Schema,

    pub fn format(
        self: @This(),
        comptime _: []const u8,
        _: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        try writer.print(
            \\CREATE TABLE "_stanchion_{s}_primary_index" (
        ,
            .{self.table_name},
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
        // Only used by the row group entry type
            \\rowid_segment_id INTEGER NULL,
            // Used by all entry types
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
        // TODO in tests it would be nice to add some check constraints to ensure
        //      data is populated correctly based on the entry_type
        try writer.print(") STRICT, WITHOUT ROWID", .{});
    }
};

//
// Open
//

pub fn open(
    static_arena: *ArenaAllocator,
    conn: Conn,
    table_name: []const u8,
    schema: *const Schema,
    next_rowid: i64,
) !Self {
    const sort_key = schema.sort_key.items;
    const sort_key_buf = try static_arena.allocator()
        .alloc(ValueRef, sort_key.len);

    var self = Self{
        .conn = conn,
        .table_name = table_name,
        .columns_len = schema.columns.items.len,
        .sort_key = sort_key,
        .next_rowid = next_rowid,
        .insert_entry = undefined,
        .get_row_group_entry = undefined,
        .find_preceding_row_group = undefined,
        .inserts_iterator = undefined,
        .inserts_iterator_from_start = undefined,
        .delete_staged_inserts_range = undefined,
        .entries_iterator = undefined,
        .sort_key_buf = sort_key_buf,
    };
    try self.initStatements(static_arena);

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
    sort_key: anytype,
    rowid: i64,
    entry: *const RowGroupEntry,
) !void {
    const stmt = try self.insert_entry.getStmt(self.conn);
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

pub fn readRowGroupEntry(
    self: *Self,
    entry: *RowGroupEntry,
    sort_key: anytype,
    rowid: i64,
) !void {
    std.debug.assert(entry.column_segment_ids.len == self.columns_len);

    const stmt = try self.get_row_group_entry.getStmt(self.conn);
    defer self.get_row_group_entry.reset();

    for (sort_key, 1..) |sk_value, idx| {
        try stmt.bindSqliteValue(idx, sk_value);
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
        sort_key: []ValueRef,
        rowid: i64,
    },

    pub fn deinit(self: *@This()) void {
        switch (self.*) {
            .row_group => |*rg| rg.cell.reset(),
            else => {},
        }
    }
};

/// Finds the row group that precedes the provided row based on the sort key and rowid.
/// The returned handle  can be used to iterate the insert entries following the row
/// group and must be deinitialized by the caller after use.
pub fn precedingRowGroup(
    self: *Self,
    rowid: i64,
    row: anytype,
) !RowGroupEntryHandle {
    const stmt = try self.find_preceding_row_group.getStmt(self.conn);
    errdefer self.find_preceding_row_group.reset();

    for (self.sort_key, 1..) |col_idx, idx| {
        try row.readValue(col_idx).bind(stmt, idx);
    }
    try stmt.bind(.Int64, self.sort_key.len + 1, rowid);

    const has_row_group = try stmt.next();
    if (!has_row_group) {
        self.find_preceding_row_group.reset();
        return .start;
    }

    for (0..self.sort_key.len) |idx| {
        self.sort_key_buf[idx] = stmt.readSqliteValue(idx);
    }
    const rg_rowid = stmt.read(.Int64, false, self.sort_key.len);

    return .{ .row_group = .{
        .cell = &self.find_preceding_row_group,
        .sort_key = self.sort_key_buf,
        .rowid = rg_rowid,
    } };
}

//
// Staged inserts
//

pub fn insertInsertEntry(self: *Self, values: anytype) !i64 {
    const rowid = self.next_rowid;
    self.next_rowid += 1;
    const stmt = try self.insert_entry.getStmt(self.conn);
    defer self.insert_entry.reset();

    try EntryType.Insert.bind(stmt, 1);
    try stmt.bindNull(2);
    try stmt.bind(.Int64, 3, rowid);
    try stmt.bindNull(4);
    for (self.sort_key, 5..) |rank, idx| {
        try values.readValue(rank).bind(stmt, idx);
    }
    const col_idx_start = self.sort_key.len + 5;
    // Sort key values are duplicated into both the sort key columns and the column value
    // columns
    for (0..self.columns_len, col_idx_start..) |rank, idx| {
        try values.readValue(rank).bind(stmt, idx);
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

    var primary_index = try create(&arena, &arena, conn, "test", &schema);
    defer primary_index.deinit();

    var row = [_]MemoryValue{
        .{ .Blob = "magnitude" },
        .{ .Integer = 100 },
    };
    _ = try primary_index.insertInsertEntry(MemoryTuple{
        .values = &row,
    });

    row = [_]MemoryValue{
        .{ .Blob = "amplitude" },
        .{ .Integer = 7 },
    };
    _ = try primary_index.insertInsertEntry(MemoryTuple{
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
        try self.stmt.restart();

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

    pub fn readRowId(self: *@This()) ValueRef {
        return self.stmt.readSqliteValue(1);
    }

    pub fn readValue(self: *@This(), idx: usize) ValueRef {
        return self.stmt.readSqliteValue(idx + 2);
    }
};

/// Create an iterator that iterates over all pending inserts after the provided row
/// group and before the subsequent row group.
pub fn stagedInserts(
    self: *Self,
    preceding_row_group_handle: *const RowGroupEntryHandle,
) !StagedInsertsIterator {
    var iter = StagedInsertsIterator{
        .stmt = undefined,
        .cell = undefined,
        .starts_at_row_group = undefined,
    };
    switch (preceding_row_group_handle.*) {
        .row_group => |*handle| {
            iter.stmt = try self.inserts_iterator.getStmt(self.conn);
            errdefer self.inserts_iterator.reset();

            for (0..self.sort_key.len) |idx| {
                try iter.stmt.bindSqliteValue(idx + 1, handle.sort_key[idx]);
            }
            try iter.stmt.bind(.Int64, self.sort_key.len + 1, handle.rowid);

            const row_group_exists = try iter.stmt.next();
            std.debug.assert(row_group_exists);

            iter.cell = &self.inserts_iterator;
            iter.starts_at_row_group = true;
        },
        .start => {
            iter.stmt = try self.inserts_iterator_from_start.getStmt(self.conn);
            iter.cell = &self.inserts_iterator_from_start;
            iter.starts_at_row_group = false;
        },
    }

    return iter;
}

/// Deletes all staged inserts with sort key >= (`start_sort_key`..., `rowid`) and < the
/// next row group entry in the table (or the end of the table)
pub fn deleteStagedInsertsRange(
    self: *Self,
    start_sort_key: anytype,
    start_rowid: i64,
) !void {
    const stmt = try self.delete_staged_inserts_range.getStmt(self.conn);
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

    pub fn readRowid(self: @This()) i64 {
        return self.stmt.read(.Int64, false, 1);
    }

    pub fn read(self: @This(), col_idx: usize) ValueRef {
        return self.stmt.readSqliteValue(col_idx + 4);
    }
};

pub fn cursor(self: *Self) !Cursor {
    return .{
        .stmt = try self.entries_iterator.getStmt(self.conn),
        .cell = &self.entries_iterator,
        .eof = false,
    };
}

//
// Statements
//

fn initStatements(self: *Self, static_arena: *ArenaAllocator) !void {
    const allocator = static_arena.allocator();

    const insert_entry_query = try fmt.allocPrintZ(allocator,
        \\INSERT INTO "_stanchion_{s}_primary_index" (
        \\  entry_type, record_count, rowid, rowid_segment_id, {s}, {s}
        \\) VALUES (?, ?, ?, ?, {s})
    , .{
        self.table_name,
        ColumnListFormatter("sk_value_{d}"){ .len = self.sort_key.len },
        ColumnListFormatter("col_{d}"){ .len = self.columns_len },
        ParameterListFormatter{ .len = self.columns_len + self.sort_key.len },
    });
    self.insert_entry = StmtCell.init(insert_entry_query);

    const get_row_group_entry_query = try fmt.allocPrintZ(allocator,
        \\SELECT record_count, rowid_segment_id, {s}
        \\FROM "_stanchion_{s}_primary_index"
        \\WHERE ({s}, rowid) = ({s}, ?) AND entry_type = {d}
    , .{
        ColumnListFormatter("col_{d}"){ .len = self.columns_len },
        self.table_name,
        ColumnListFormatter("sk_value_{d}"){ .len = self.sort_key.len },
        ParameterListFormatter{ .len = self.sort_key.len },
        @intFromEnum(EntryType.RowGroup),
    });
    self.get_row_group_entry = StmtCell.init(get_row_group_entry_query);

    const find_preceding_row_group_query = try fmt.allocPrintZ(allocator,
        \\SELECT {s}, rowid
        \\FROM "_stanchion_{s}_primary_index"
        \\WHERE ({s}, rowid) <= ({s}, ?) AND entry_type = {d}
        \\ORDER BY {s}, rowid DESC
        \\LIMIT 1
    , .{
        ColumnListFormatter("sk_value_{d}"){ .len = self.sort_key.len },
        self.table_name,
        ColumnListFormatter("sk_value_{d}"){ .len = self.sort_key.len },
        ParameterListFormatter{ .len = self.sort_key.len },
        @intFromEnum(EntryType.RowGroup),
        ColumnListFormatter("sk_value_{d} DESC"){ .len = self.sort_key.len },
    });
    self.find_preceding_row_group = StmtCell.init(find_preceding_row_group_query);

    const inserts_iterator_query = try fmt.allocPrintZ(allocator,
        \\SELECT entry_type, rowid, {s}
        \\FROM "_stanchion_{s}_primary_index"
        \\WHERE ({s}, rowid) >= ({s}, ?)
        \\ORDER BY {s}, rowid ASC
    , .{
        ColumnListFormatter("col_{d}"){ .len = self.columns_len },
        self.table_name,
        ColumnListFormatter("sk_value_{d}"){ .len = self.sort_key.len },
        ParameterListFormatter{ .len = self.sort_key.len },
        ColumnListFormatter("sk_value_{d} ASC"){ .len = self.sort_key.len },
    });
    self.inserts_iterator = StmtCell.init(inserts_iterator_query);

    const inserts_iterator_from_start_query = try fmt.allocPrintZ(allocator,
        \\SELECT entry_type, rowid, {s}
        \\FROM "_stanchion_{s}_primary_index"
        \\ORDER BY {s}, rowid ASC
    , .{
        ColumnListFormatter("col_{d}"){ .len = self.columns_len },
        self.table_name,
        ColumnListFormatter("sk_value_{d} ASC"){ .len = self.sort_key.len },
    });
    self.inserts_iterator_from_start = StmtCell.init(inserts_iterator_from_start_query);

    const entries_iterator_query = try fmt.allocPrintZ(allocator,
        \\SELECT entry_type, rowid, record_count, rowid_segment_id, {s}
        \\FROM "_stanchion_{s}_primary_index"
        \\ORDER BY {s}, rowid ASC
    , .{
        ColumnListFormatter("col_{d}"){ .len = self.columns_len },
        self.table_name,
        ColumnListFormatter("sk_value_{d} ASC"){ .len = self.sort_key.len },
    });
    self.entries_iterator = StmtCell.init(entries_iterator_query);

    const delete_staged_inserts_range_query = try fmt.allocPrintZ(allocator,
        \\WITH end_rg_entry AS (
        \\  SELECT {s}, rowid
        \\  FROM "_stanchion_{s}_primary_index"
        \\  WHERE ({s}, rowid) > ({s}, ?) AND entry_type = {d}
        \\  ORDER BY {s}, rowid ASC
        \\  LIMIT 1
        \\)
        \\DELETE FROM "_stanchion_{s}_primary_index"
        \\WHERE ({s}, rowid) IN (
        \\  SELECT {s}, pidx.rowid
        \\  FROM "_stanchion_{s}_primary_index" pidx
        \\  LEFT JOIN end_rg_entry e
        \\  WHERE ({s}, pidx.rowid) >= ({s}, ?) AND pidx.entry_type = {d}
        \\        AND (e.rowid IS NULL OR ({s}, pidx.rowid) < ({s}, e.rowid)))
    , .{
        // CTE
        ColumnListFormatter("sk_value_{d}"){ .len = self.sort_key.len },
        self.table_name,
        ColumnListFormatter("sk_value_{d}"){ .len = self.sort_key.len },
        ParameterListFormatter{ .len = self.sort_key.len },
        @intFromEnum(EntryType.RowGroup),
        ColumnListFormatter("sk_value_{d} ASC"){ .len = self.sort_key.len },
        // Delete
        self.table_name,
        ColumnListFormatter("sk_value_{d}"){ .len = self.sort_key.len },
        // Subquery
        ColumnListFormatter("pidx.sk_value_{d}"){ .len = self.sort_key.len },
        self.table_name,
        ColumnListFormatter("pidx.sk_value_{d}"){ .len = self.sort_key.len },
        ParameterListFormatter{ .len = self.sort_key.len },
        @intFromEnum(EntryType.Insert),
        ColumnListFormatter("pidx.sk_value_{d}"){ .len = self.sort_key.len },
        ColumnListFormatter("e.sk_value_{d}"){ .len = self.sort_key.len },
    });
    self.delete_staged_inserts_range = StmtCell.init(delete_staged_inserts_range_query);
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
