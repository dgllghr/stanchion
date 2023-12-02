const std = @import("std");
const debug = std.debug;
const fmt = std.fmt;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const Stmt = @import("../sqlite3/Stmt.zig");
const ValueRef = @import("../sqlite3/value.zig").Ref;
const stmt_cell = @import("../stmt_cell.zig");

const Schema = @import("../schema.zig").Schema;

const sql_fmt = @import("sql_fmt.zig");
const pending_inserts = @import("pending_inserts.zig");
const Ctx = @import("Ctx.zig");
const EntryType = @import("entry_type.zig").EntryType;
const PendingInserts = pending_inserts.PendingInserts;
const PendingInsertsIterator = pending_inserts.Iterator;
const TableState = @import("TableState.zig");

const StmtCell = stmt_cell.StmtCell(Ctx);

pub const Entries = struct {
    insert_entry: StmtCell,
    delete_range: StmtCell,
    delete_range_from: StmtCell,

    pub fn init() Entries {
        return .{
            .insert_entry = StmtCell.init(&insertEntryDml),
            .delete_range = StmtCell.init(&deleteRangeDml),
            .delete_range_from = StmtCell.init(&deleteRangeFromDml),
        };
    }

    pub fn deinit(self: *Entries) void {
        self.insert_entry.deinit();
        self.delete_range.deinit();
        self.delete_range_from.deinit();
    }

    pub fn insertTableState(
        self: *Entries,
        tmp_arena: *ArenaAllocator,
        ctx: *const Ctx,
        schema: *const Schema,
    ) !void {
        const stmt = try self.insert_entry.getStmt(tmp_arena, ctx);
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
            try TableState.bindMinValue(stmt, idx, data_type);
        }

        // The col_* columns are unused by the table state entry

        try stmt.exec();
    }

    pub fn insertRowGroup(
        self: *Entries,
        tmp_arena: *ArenaAllocator,
        ctx: *const Ctx,
        sort_key: anytype,
        rowid: i64,
        entry: *const RowGroupEntry,
    ) !void {
        const stmt = try self.insert_entry.getStmt(tmp_arena, ctx);
        defer self.insert_entry.reset();

        try EntryType.RowGroup.bind(stmt, 1);
        try stmt.bind(.Int64, 2, @intCast(entry.record_count));
        try stmt.bind(.Int64, 3, rowid);
        try stmt.bind(.Int64, 4, entry.rowid_segment_id);
        for (sort_key, 5..) |sk_value, idx| {
            try sk_value.bind(stmt, idx);
        }
        const col_idx_start = ctx.sort_key.len + 5;
        for (0..ctx.columns_len, col_idx_start..) |rank, idx| {
            try stmt.bind(.Int64, idx, entry.column_segment_ids[rank]);
        }

        try stmt.exec();
    }

    pub fn insertPendingInsert(
        self: *Entries,
        tmp_arena: *ArenaAllocator,
        ctx: *const Ctx,
        table_state: *TableState,
        values: anytype,
    ) !i64 {
        const rowid = table_state.getAndIncrNextRowid();
        const stmt = try self.insert_entry.getStmt(tmp_arena, ctx);
        defer self.insert_entry.reset();

        try EntryType.Insert.bind(stmt, 1);
        try stmt.bindNull(2);
        try stmt.bind(.Int64, 3, rowid);
        try stmt.bindNull(4);
        for (ctx.sort_key, 5..) |rank, idx| {
            const value = try values.readValue(rank);
            try value.bind(stmt, idx);
        }
        const col_idx_start = ctx.sort_key.len + 5;
        // Sort key values are duplicated into both the sort key columns and the column value columns
        for (0..ctx.columns_len, col_idx_start..) |rank, idx| {
            const value = try values.readValue(rank);
            try value.bind(stmt, idx);
        }

        try stmt.exec();

        return rowid;
    }

    fn insertEntryDml(ctx: *const Ctx, arena: *ArenaAllocator) ![]const u8 {
        return fmt.allocPrintZ(arena.allocator(),
            \\INSERT INTO "{s}_primaryindex" (
            \\  entry_type, record_count, rowid, col_rowid, {s}, {s}
            \\) VALUES (?, ?, ?, ?, {s})
        , .{
            ctx.vtab_table_name,
            sql_fmt.ColumnListFormatter("sk_value_{d}"){ .len = ctx.sort_key.len },
            sql_fmt.ColumnListFormatter("col_{d}"){ .len = ctx.columns_len },
            sql_fmt.ParameterListFormatter{ .len = ctx.columns_len + ctx.sort_key.len },
        });
    }

    pub fn deleteRange(
        self: *Entries,
        tmp_arena: *ArenaAllocator,
        ctx: *Ctx,
        start_sort_key: anytype,
        start_rowid: i64,
        end_sort_key: anytype,
        end_rowid: i64,
    ) !void {
        const stmt = try self.delete_range.getStmt(tmp_arena, ctx);
        defer self.delete_range.reset();

        const sort_key_len = ctx.sort_key.len;
        for (0..sort_key_len) |idx| {
            try (try start_sort_key.readValue(idx)).bind(stmt, idx + 1);
        }
        try stmt.bind(.Int64, sort_key_len + 1, start_rowid);
        for (0..sort_key_len, (sort_key_len + 2)..) |idx, bind_idx| {
            try (try end_sort_key.readValue(idx)).bind(stmt, bind_idx);
        }
        try stmt.bind(.Int64, (sort_key_len * 2) + 2, end_rowid);

        try stmt.exec();
    }

    fn deleteRangeDml(ctx: *const Ctx, arena: *ArenaAllocator) ![]const u8 {
        return fmt.allocPrintZ(arena.allocator(),
            \\DELETE FROM "{s}_primaryindex"
            \\WHERE ({s}, rowid) >= ({s}, ?) AND
            \\  ({s}, rowid) < ({s}, ?)
        , .{
            ctx.vtab_table_name,
            sql_fmt.ColumnListFormatter("sk_value_{d}"){ .len = ctx.sort_key.len },
            sql_fmt.ParameterListFormatter{ .len = ctx.sort_key.len },
            sql_fmt.ColumnListFormatter("sk_value_{d}"){ .len = ctx.sort_key.len },
            sql_fmt.ParameterListFormatter{ .len = ctx.sort_key.len },
        });
    }

    pub fn deleteRangeFrom(
        self: *Entries,
        tmp_arena: *ArenaAllocator,
        ctx: *Ctx,
        start_sort_key: anytype,
        start_rowid: i64,
    ) !void {
        const stmt = try self.delete_range_from.getStmt(tmp_arena, ctx);
        defer self.delete_range_from.reset();

        const sort_key_len = ctx.sort_key.len;
        for (0..sort_key_len) |idx| {
            try (try start_sort_key.readValue(idx)).bind(stmt, idx + 1);
        }
        try stmt.bind(.Int64, sort_key_len + 1, start_rowid);

        try stmt.exec();
    }

    fn deleteRangeFromDml(ctx: *const Ctx, arena: *ArenaAllocator) ![]const u8 {
        return fmt.allocPrintZ(arena.allocator(),
            \\DELETE FROM "{s}_primaryindex"
            \\WHERE ({s}, rowid) >= ({s}, ?)
        , .{
            ctx.vtab_table_name,
            sql_fmt.ColumnListFormatter("sk_value_{d}"){ .len = ctx.sort_key.len },
            sql_fmt.ParameterListFormatter{ .len = ctx.sort_key.len },
        });
    }
};

pub const RowGroupEntry = struct {
    rowid_segment_id: i64,
    column_segment_ids: []i64,
    /// Record count of 0 means that the row group does not exist because there are no empty row
    /// groups. If the record count is 0, the other fields on this struct are undefined and should
    /// not be read
    record_count: u32,

    pub fn deinit(self: *RowGroupEntry, allocator: Allocator) void {
        allocator.free(self.column_segment_ids);
    }
};
