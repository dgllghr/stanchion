const std = @import("std");
const debug = std.debug;
const fmt = std.fmt;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const Stmt = @import("../sqlite3/Stmt.zig");
const ValueRef = @import("../sqlite3/value.zig").Ref;
const stmt_cell = @import("../stmt_cell.zig");

const sql_fmt = @import("sql_fmt.zig");
const pending_inserts = @import("pending_inserts.zig");
const Ctx = @import("Ctx.zig");
const EntryType = @import("entry_type.zig").EntryType;
const PendingInserts = pending_inserts.PendingInserts;
const PendingInsertsIterator = pending_inserts.Iterator;

const StmtCell = stmt_cell.StmtCell(Ctx);

pub const Entries = struct {
    delete_range: StmtCell,
    delete_range_from: StmtCell,

    pub fn init() Entries {
        return .{
            .delete_range = StmtCell.init(&deleteRangeDml),
            .delete_range_from = StmtCell.init(&deleteRangeFromDml),
        };
    }

    pub fn deinit(self: *Entries) void {
        self.delete_range.deinit();
        self.delete_range_from.deinit();
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
