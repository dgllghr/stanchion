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

const cursors = @import("primary_index/cursors.zig");
const entries = @import("primary_index/entries.zig");
const nodes = @import("primary_index/nodes.zig");
const pending_inserts = @import("primary_index/pending_inserts.zig");
const sql_fmt = @import("primary_index/sql_fmt.zig");
const ColumnListFormatter = sql_fmt.ColumnListFormatter;
const Ctx = @import("primary_index/Ctx.zig");
const Entries = entries.Entries;
const EntryType = @import("primary_index/entry_type.zig").EntryType;
const ParameterListFormatter = sql_fmt.ParameterListFormatter;
const TableState = @import("primary_index/TableState.zig");

pub const Cursor = cursors.Cursor;
pub const CursorRange = cursors.CursorRange;
pub const Nodes = nodes.Nodes;
pub const NodeHandle = nodes.NodeHandle;
pub const PendingInsertsIterator = pending_inserts.Iterator;
pub const RowGroupEntry = @import("primary_index/entries.zig").RowGroupEntry;

const StmtCell = stmt_cell.StmtCell(Ctx);

const Self = @This();

ctx: Ctx,

entries: Entries,
nodes: Nodes,
table_state: TableState,

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

    const ctx = Ctx.init(conn, vtab_table_name, schema);

    var table_state = try TableState.init(tmp_arena, &ctx, schema, 1, 1);
    errdefer table_state.deinit();

    var self = Self{
        .ctx = ctx,
        .entries = Entries.init(),
        .nodes = Nodes.init(),
        .table_state = table_state,
    };

    try self.entries.insertTableState(tmp_arena, &self.ctx, schema);

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
    const ctx = Ctx.init(conn, vtab_table_name, schema);

    var table_state = try TableState.init(tmp_arena, &ctx, schema, 0, 0);
    errdefer table_state.deinit();

    var self = Self{
        .ctx = ctx,
        .entries = Entries.init(),
        .nodes = Nodes.init(),
        .table_state = table_state,
    };

    try self.table_state.loadNextRowid();

    return self;
}

pub fn deinit(self: *Self) void {
    self.nodes.deinit();
    self.entries.deinit();
    self.table_state.deinit();
}

/// Drops the primary index table, deleting all persisted data. The primary index struct must not
/// be used after calling `drop`.
pub fn drop(self: *Self, tmp_arena: *ArenaAllocator) !void {
    const query = try fmt.allocPrintZ(
        tmp_arena.allocator(),
        \\DROP TABLE "{s}_primaryindex"
    ,
        .{self.ctx.vtab_table_name},
    );
    try self.ctx.conn.exec(query);
}

/// The next rowid is kept both in memory and persistent storage so that the next rowid can be read
/// and updated quickly in memory but also saved across sessions. `loadNextRowid` reads the next
/// rowid from persistent storage and overwrites the memory value of the next rowid, synchronizing
/// the memory state to be the persistent state.
pub fn loadNextRowid(self: *Self) !void {
    try self.table_state.loadNextRowid();
}

/// The next rowid is kept both in memory and persistent storage so that the next rowid can be read
/// and updated quickly in memory but also saved across sessions. `persistNextRowid` writes the
/// next rowid from memory to persistent storage, synchronizing the persistent state to be the
/// memory state.
pub fn persistNextRowid(self: *Self) !void {
    try self.table_state.persistNextRowid();
}

/// Inserts a row group entry with the starting `sort_key`, `rowid` tuple into the index
pub fn insertRowGroupEntry(
    self: *Self,
    tmp_arena: *ArenaAllocator,
    sort_key: anytype,
    rowid: i64,
    entry: *const RowGroupEntry,
) !void {
    try self.entries.insertRowGroup(tmp_arena, &self.ctx, sort_key, rowid, entry);
}

/// Inserts a pending insert entry into the index. The associated rowid is returned.
pub fn insertPendingInsertEntry(self: *Self, tmp_arena: *ArenaAllocator, values: anytype) !i64 {
    return try self.entries.insertPendingInsert(tmp_arena, &self.ctx, &self.table_state, values);
}

/// Delete all entries starting from the provided `start_sort_key`, `start_rowid` tuple (inclusive)
/// to the `end_sort_key`, `end_rowid` tuple (exclusive)
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

/// Delete all entries starting from the provided `start_sort_key`, `start_rowid` tuple (inclusive)
/// to the end of the index
pub fn deleteRangeFrom(
    self: *Self,
    tmp_arena: *ArenaAllocator,
    start_sort_key: anytype,
    start_rowid: i64,
) !void {
    try self.entries.deleteRangeFrom(tmp_arena, &self.ctx, start_sort_key, start_rowid);
}

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

/// Create a new cursor for all row group and pending insert entries in the provided range
pub fn cursor(
    self: *Self,
    comptime PartialSortKey: type,
    tmp_arena: *ArenaAllocator,
    range: ?CursorRange(PartialSortKey),
) !Cursor {
    return cursors.open(PartialSortKey, tmp_arena, &self.ctx, range);
}
