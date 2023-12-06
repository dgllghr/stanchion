const std = @import("std");
const fmt = std.fmt;
const ArenaAllocator = std.heap.ArenaAllocator;

const Stmt = @import("../sqlite3/Stmt.zig");
const ValueRef = @import("../sqlite3/value.zig").Ref;

const Ctx = @import("Ctx.zig");
const EntryType = @import("entry_type.zig").EntryType;
const RowGroupEntry = @import("entries.zig").RowGroupEntry;
const sql_fmt = @import("sql_fmt.zig");

/// Values outside of the provided sort key `range` may still be traversed by the cursor. The
/// `range` is used to reduce the amount of data to traverse but it does not filter on the row
/// level.
pub fn open(
    comptime PartialSortKey: type,
    tmp_arena: *ArenaAllocator,
    ctx: *Ctx,
    range: ?CursorRange(PartialSortKey),
) !Cursor {
    // TODO because the constraints are dynamically generated, it doesn't make sense to store
    //      the prepared statement in a `StmtCell`. However, this may benefit from a dynamic
    //      statement cache since applications will probably have specific access patterns

    // It is necessary to find the preceding row group as the starting point because that row group
    // may contain records that fall into the range
    var query: []const u8 = undefined;
    if (range) |r| {
        if (r.last_op == .gt or r.last_op == .ge) {
            query = try generateGtGeQuery(PartialSortKey, tmp_arena, ctx, r);
        } else if (r.last_op == .eq) {
            query = try generateEqQuery(PartialSortKey, tmp_arena, ctx, r);
        } else {
            // Lt & Le
            query = try generateLtLeQuery(PartialSortKey, tmp_arena, ctx, r);
        }
    } else {
        query = try generateUnfilteredQuery(tmp_arena, ctx);
    }

    const stmt = try ctx.conn.prepare(query);
    errdefer stmt.deinit();

    if (range) |r| {
        for (0..r.key.valuesLen()) |idx| {
            const v = try r.key.readValue(idx);
            try v.bind(stmt, idx + 1);
        }
    }

    return .{
        .stmt = stmt,
        .eof = false,
    };
}

fn generateGtGeQuery(
    comptime PartialSortKey: type,
    tmp_arena: *ArenaAllocator,
    ctx: *Ctx,
    range: CursorRange(PartialSortKey),
) ![]const u8 {
    return fmt.allocPrintZ(tmp_arena.allocator(),
        \\WITH start_key AS (
        \\  SELECT rowid, {s}
        \\  FROM "{s}_primaryindex"
        \\  WHERE entry_type != {d} AND ({s}) < ({s})
        \\  ORDER BY {s}, rowid DESC
        \\  LIMIT 1
        \\)
        \\SELECT pidx.entry_type, pidx.rowid, pidx.record_count, pidx.col_rowid, {s}
        \\FROM "{s}_primaryindex" pidx
        \\JOIN start_key sk
        \\WHERE pidx.entry_type IN ({d}, {d}) AND ({s}, pidx.rowid) >= ({s}, sk.rowid)
        \\ORDER BY {s}, pidx.rowid ASC
    , .{
        sql_fmt.ColumnListFormatter("sk_value_{d}"){ .len = ctx.sort_key.len },
        ctx.vtab_table_name,
        @intFromEnum(EntryType.Insert),
        sql_fmt.ColumnListFormatter("sk_value_{d}"){ .len = range.key.valuesLen() },
        sql_fmt.ParameterListFormatter{ .len = range.key.valuesLen() },
        sql_fmt.ColumnListFormatter("sk_value_{d} DESC"){ .len = ctx.sort_key.len },
        sql_fmt.ColumnListFormatter("pidx.col_{d}"){ .len = ctx.columns_len },
        ctx.vtab_table_name,
        @intFromEnum(EntryType.RowGroup),
        @intFromEnum(EntryType.Insert),
        sql_fmt.ColumnListFormatter("pidx.sk_value_{d}"){ .len = ctx.sort_key.len },
        sql_fmt.ColumnListFormatter("sk.sk_value_{d}"){ .len = ctx.sort_key.len },
        sql_fmt.ColumnListFormatter("pidx.sk_value_{d} ASC"){ .len = ctx.sort_key.len },
    });
}

fn generateLtLeQuery(
    comptime PartialSortKey: type,
    tmp_arena: *ArenaAllocator,
    ctx: *Ctx,
    range: CursorRange(PartialSortKey),
) ![]const u8 {
    return fmt.allocPrintZ(tmp_arena.allocator(),
        \\SELECT entry_type, rowid, record_count, col_rowid, {s}
        \\FROM "{s}_primaryindex"
        \\WHERE entry_type IN ({d}, {d}) AND ({s}) <= ({s})
        \\ORDER BY {s}, rowid ASC
    , .{
        sql_fmt.ColumnListFormatter("col_{d}"){ .len = ctx.columns_len },
        ctx.vtab_table_name,
        @intFromEnum(EntryType.RowGroup),
        @intFromEnum(EntryType.Insert),
        sql_fmt.ColumnListFormatter("sk_value_{d}"){ .len = range.key.valuesLen() },
        sql_fmt.ParameterListFormatter{ .len = range.key.valuesLen() },
        sql_fmt.ColumnListFormatter("sk_value_{d} ASC"){ .len = ctx.sort_key.len },
    });
}

fn generateEqQuery(
    comptime PartialSortKey: type,
    tmp_arena: *ArenaAllocator,
    ctx: *Ctx,
    range: CursorRange(PartialSortKey),
) ![]const u8 {
    return fmt.allocPrintZ(tmp_arena.allocator(),
        \\WITH sort_key_values AS (
        \\  SELECT {s}
        \\),
        \\first_row_group AS (
        \\  SELECT pidx.rowid, {s}
        \\  FROM "{s}_primaryindex" pidx
        \\  JOIN sort_key_values skv
        \\  WHERE pidx.entry_type = {d} AND ({s}) < ({s})
        \\  ORDER BY {s}, pidx.rowid DESC
        \\  LIMIT 1
        \\)
        \\SELECT pidx.entry_type, pidx.rowid, pidx.record_count, pidx.col_rowid, {s}
        \\FROM "{s}_primaryindex" pidx
        \\JOIN sort_key_values skv
        \\LEFT JOIN first_row_group frg
        \\  ON ({s}, pidx.rowid) >= ({s}, frg.rowid)
        \\WHERE pidx.entry_type IN ({d}, {d}) AND ({s}) <= ({s})
        \\ORDER BY {s}, pidx.rowid ASC
    , .{
        // sort_key_values CTE
        sql_fmt.ColumnListFormatter("? AS sk_value_{d}"){ .len = range.key.valuesLen() },
        // first_row_group CTE
        sql_fmt.ColumnListFormatter("pidx.sk_value_{d}"){ .len = ctx.sort_key.len },
        ctx.vtab_table_name,
        @intFromEnum(EntryType.RowGroup),
        sql_fmt.ColumnListFormatter("pidx.sk_value_{d}"){ .len = range.key.valuesLen() },
        sql_fmt.ColumnListFormatter("skv.sk_value_{d}"){ .len = range.key.valuesLen() },
        sql_fmt.ColumnListFormatter("pidx.sk_value_{d}"){ .len = ctx.sort_key.len },
        // main query
        sql_fmt.ColumnListFormatter("pidx.col_{d}"){ .len = ctx.columns_len },
        ctx.vtab_table_name,
        sql_fmt.ColumnListFormatter("pidx.sk_value_{d}"){ .len = ctx.sort_key.len },
        sql_fmt.ColumnListFormatter("frg.sk_value_{d}"){ .len = ctx.sort_key.len },
        @intFromEnum(EntryType.RowGroup),
        @intFromEnum(EntryType.Insert),
        sql_fmt.ColumnListFormatter("pidx.sk_value_{d}"){ .len = range.key.valuesLen() },
        sql_fmt.ColumnListFormatter("skv.sk_value_{d}"){ .len = range.key.valuesLen() },
        sql_fmt.ColumnListFormatter("pidx.sk_value_{d}"){ .len = ctx.sort_key.len },
    });
}

fn generateUnfilteredQuery(tmp_arena: *ArenaAllocator, ctx: *Ctx) ![]const u8 {
    return fmt.allocPrintZ(tmp_arena.allocator(),
        \\SELECT entry_type, rowid, record_count, col_rowid, {s}
        \\FROM "{s}_primaryindex"
        \\WHERE entry_type IN ({d}, {d})
        \\ORDER BY {s}, rowid ASC
    , .{
        sql_fmt.ColumnListFormatter("col_{d}"){ .len = ctx.columns_len },
        ctx.vtab_table_name,
        @intFromEnum(EntryType.RowGroup),
        @intFromEnum(EntryType.Insert),
        sql_fmt.ColumnListFormatter("sk_value_{d} ASC"){ .len = ctx.sort_key.len },
    });
}

pub const Cursor = struct {
    stmt: Stmt,
    eof: bool,

    pub fn deinit(self: *@This()) void {
        self.stmt.deinit();
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

pub fn CursorRange(comptime PartialSortKey: type) type {
    return struct {
        key: PartialSortKey,
        last_op: enum {
            eq,
            gt,
            ge,
            lt,
            le,
        },
    };
}
