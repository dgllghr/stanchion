//! The `PrimaryIndex` stores 3 kinds of data: the table state, row group entries, and pending
//! inserts. The table state currently only contains the next rowid. It is always the first entry
//! in the index. Row group entries index row groups by providing the start sort key (+ rowid) of
//! the row group and the segment IDs of all segments that make up the row group. Finally, pending
//! inserts are the modifications (only inserts currently) that have been made to the table but not
//! merged into row groups.
//!
//! The data is organized into nodes. A node is a purely logical abstraction. Each node consists of
//! a head entry, which is either a `.RowGroup` entry or a `.TableState` entry at the beginning of
//! the node, and zero or more pending `.Insert` entries. The pending inserts in a node get merged
//! into the row group in the node when the number of pending inserts in a node gets too large. For
//! the table state node (there is only one), merging the pending inserts creates a new row group
//! entirely.
//!
//! All of this is backed by a single sqlite table. It is helpful to think of this table as a
//! persistent b+tree rather than a sql table.
//!
//! Inspiration for this structure is taken from Bω trees (where all of the data is stuffed into a
//! b+tree instead of the elegant Bω tree).

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

const entries = @import("primary_index/entries.zig");
const node = @import("primary_index/node.zig");
const pending_inserts = @import("primary_index/pending_inserts.zig");
const sql_fmt = @import("primary_index/sql_fmt.zig");
const ColumnListFormatter = sql_fmt.ColumnListFormatter;
const Ctx = @import("primary_index/Ctx.zig");
const Entries = entries.Entries;
const EntryType = @import("primary_index/entry_type.zig").EntryType;
const ParameterListFormatter = sql_fmt.ParameterListFormatter;
pub const Nodes = node.Nodes;
pub const NodeHandle = node.NodeHandle;
pub const PendingInsertsIterator = pending_inserts.Iterator;
pub const RowGroupEntry = @import("primary_index/entries.zig").RowGroupEntry;

const StmtCell = stmt_cell.StmtCell(Ctx);

const Self = @This();

ctx: Ctx,

next_rowid: i64,
last_write_rowid: i64,

entries: Entries,
nodes: Nodes,

insert_entry: StmtCell,
entries_iterator: StmtCell,

load_next_rowid: Stmt,
update_next_rowid: Stmt,

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
        .ctx = Ctx.init(conn, vtab_table_name, schema),
        .next_rowid = 1,
        .last_write_rowid = 1,
        .entries = Entries.init(),
        .nodes = Nodes.init(),
        .insert_entry = StmtCell.init(&insertEntryDml),
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
        .ctx = Ctx.init(conn, vtab_table_name, schema),
        // Initialized below
        .next_rowid = 0,
        .last_write_rowid = 0,
        .entries = Entries.init(),
        .nodes = Nodes.init(),
        .insert_entry = StmtCell.init(&insertEntryDml),
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
    self.nodes.deinit();
    self.entries.deinit();
    self.insert_entry.deinit();
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
        .{self.ctx.vtab_table_name},
    );
    try self.ctx.conn.exec(query);
}

//
// Table state
//

fn insertTableStateEntry(self: *Self, tmp_arena: *ArenaAllocator, schema: *const Schema) !void {
    const stmt = try self.insert_entry.getStmt(tmp_arena, &self.ctx);
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
        const err_msg = sqlite_c.sqlite3_errmsg(self.ctx.conn.conn);
        std.log.err("error executing insert: {s}", .{err_msg});
        return e;
    };
}

fn initTableStateStmts(self: *Self, tmp_arena: *ArenaAllocator, schema: *const Schema) !void {
    self.load_next_rowid = try self.ctx.conn.prepare(
        try loadNextRowidQuery(&self.ctx, tmp_arena),
    );
    errdefer self.load_next_rowid.deinit();
    self.update_next_rowid = try self.ctx.conn.prepare(
        try updateNextRowidDml(&self.ctx, tmp_arena),
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

fn loadNextRowidQuery(ctx: *const Ctx, arena: *ArenaAllocator) ![]const u8 {
    return fmt.allocPrintZ(arena.allocator(),
        \\SELECT col_rowid
        \\FROM "{s}_primaryindex"
        \\WHERE ({s}, rowid) = ({s}, -1) AND entry_type = {d}
    , .{
        ctx.vtab_table_name,
        ColumnListFormatter("sk_value_{d}"){ .len = ctx.sort_key.len },
        ParameterListFormatter{ .len = ctx.sort_key.len },
        @intFromEnum(EntryType.TableState),
    });
}

pub fn loadNextRowid(self: *Self) !void {
    defer self.load_next_rowid.resetExec() catch {};

    _ = try self.load_next_rowid.next();

    self.next_rowid = self.load_next_rowid.read(.Int64, false, 0);
    self.last_write_rowid = self.next_rowid;
}

fn updateNextRowidDml(ctx: *const Ctx, arena: *ArenaAllocator) ![]const u8 {
    return fmt.allocPrintZ(arena.allocator(),
        \\UPDATE "{s}_primaryindex"
        \\SET col_rowid = ?
        \\WHERE ({s}, rowid) = ({s}, -1) AND entry_type = {d}
    , .{
        ctx.vtab_table_name,
        ColumnListFormatter("sk_value_{d}"){ .len = ctx.sort_key.len },
        ParameterListFormatter{ .len = ctx.sort_key.len },
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
// Entries
//

pub fn deleteRange(
    self: *Self,
    tmp_arena: *ArenaAllocator,
    start_sort_key: anytype,
    start_rowid: i64,
    end_sort_key: anytype,
    end_rowid: i64,
) !void {
    try self.entries.deleteRange(
        tmp_arena,
        &self.ctx,
        start_sort_key,
        start_rowid,
        end_sort_key,
        end_rowid,
    );
}

pub fn deleteRangeFrom(
    self: *Self,
    tmp_arena: *ArenaAllocator,
    start_sort_key: anytype,
    start_rowid: i64,
) !void {
    try self.entries.deleteRangeFrom(tmp_arena, &self.ctx, start_sort_key, start_rowid);
}

//
// Row group entry
//

pub fn insertRowGroupEntry(
    self: *Self,
    tmp_arena: *ArenaAllocator,
    sort_key: anytype,
    rowid: i64,
    entry: *const RowGroupEntry,
) !void {
    const stmt = try self.insert_entry.getStmt(tmp_arena, &self.ctx);
    defer self.insert_entry.reset();

    try EntryType.RowGroup.bind(stmt, 1);
    try stmt.bind(.Int64, 2, @intCast(entry.record_count));
    try stmt.bind(.Int64, 3, rowid);
    try stmt.bind(.Int64, 4, entry.rowid_segment_id);
    for (sort_key, 5..) |sk_value, idx| {
        try sk_value.bind(stmt, idx);
    }
    const col_idx_start = self.ctx.sort_key.len + 5;
    for (0..self.ctx.columns_len, col_idx_start..) |rank, idx| {
        try stmt.bind(.Int64, idx, entry.column_segment_ids[rank]);
    }

    stmt.exec() catch |e| {
        const err_msg = sqlite_c.sqlite3_errmsg(self.ctx.conn.conn);
        std.log.err("error executing insert: {s}", .{err_msg});
        return e;
    };
}

//
// Nodes
//

/// Returns the handle to the start node. The returned handle does not have a head. It can be used
/// to iterate the pending inserts in the node. Only one `NodeHandle` can be in use at a time.
/// Deinitialize the `NodeHandle` from the previous call to this function or `containingNodeHandle`
/// before calling this function again.
pub fn startNodeHandle(self: *Self) !*NodeHandle {
    return self.nodes.startNodeHandle(&self.ctx);
}

/// Finds the node that contains the provided sort key and rowid. The returned handle can be used
/// to read the node head and iterate the pending inserts in the node. Only one `NodeHandle` can be
/// in use at a time. Deinitialize the `NodeHandle` from the previous call to this function or
/// startNodeHandle` before calling this function again.
pub fn containingNodeHandle(
    self: *Self,
    tmp_arena: *ArenaAllocator,
    row: anytype,
    rowid: i64,
) !*NodeHandle {
    return self.nodes.containingNodeHandle(tmp_arena, &self.ctx, row, rowid);
}

//
// Pending inserts
//

pub fn insertInsertEntry(self: *Self, tmp_arena: *ArenaAllocator, values: anytype) !i64 {
    const rowid = self.next_rowid;
    self.next_rowid += 1;
    const stmt = try self.insert_entry.getStmt(tmp_arena, &self.ctx);
    defer self.insert_entry.reset();

    try EntryType.Insert.bind(stmt, 1);
    try stmt.bindNull(2);
    try stmt.bind(.Int64, 3, rowid);
    try stmt.bindNull(4);
    for (self.ctx.sort_key, 5..) |rank, idx| {
        const value = try values.readValue(rank);
        try value.bind(stmt, idx);
    }
    const col_idx_start = self.ctx.sort_key.len + 5;
    // Sort key values are duplicated into both the sort key columns and the column value
    // columns
    for (0..self.ctx.columns_len, col_idx_start..) |rank, idx| {
        const value = try values.readValue(rank);
        try value.bind(stmt, idx);
    }

    stmt.exec() catch |e| {
        const err_msg = sqlite_c.sqlite3_errmsg(self.ctx.conn.conn);
        std.log.err("error executing insert: {s}", .{err_msg});
        return e;
    };

    return rowid;
}

test "primary index: insert staged insert" {
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

fn entriesIteratorQuery(ctx: *const Ctx, arena: *ArenaAllocator) ![]const u8 {
    return fmt.allocPrintZ(arena.allocator(),
        \\SELECT entry_type, rowid, record_count, col_rowid, {s}
        \\FROM "{s}_primaryindex"
        \\WHERE entry_type IN ({d}, {d})
        \\ORDER BY {s}, rowid ASC
    , .{
        ColumnListFormatter("col_{d}"){ .len = ctx.columns_len },
        ctx.vtab_table_name,
        @intFromEnum(EntryType.RowGroup),
        @intFromEnum(EntryType.Insert),
        ColumnListFormatter("sk_value_{d} ASC"){ .len = ctx.sort_key.len },
    });
}

pub fn cursor(self: *Self, tmp_arena: *ArenaAllocator) !Cursor {
    return .{
        .stmt = try self.entries_iterator.getStmt(tmp_arena, &self.ctx),
        .cell = &self.entries_iterator,
        .eof = false,
    };
}

//
// Common
//

fn insertEntryDml(ctx: *const Ctx, arena: *ArenaAllocator) ![]const u8 {
    return fmt.allocPrintZ(arena.allocator(),
        \\INSERT INTO "{s}_primaryindex" (
        \\  entry_type, record_count, rowid, col_rowid, {s}, {s}
        \\) VALUES (?, ?, ?, ?, {s})
    , .{
        ctx.vtab_table_name,
        ColumnListFormatter("sk_value_{d}"){ .len = ctx.sort_key.len },
        ColumnListFormatter("col_{d}"){ .len = ctx.columns_len },
        ParameterListFormatter{ .len = ctx.columns_len + ctx.sort_key.len },
    });
}
