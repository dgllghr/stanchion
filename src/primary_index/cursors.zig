const std = @import("std");
const fmt = std.fmt;
const ArenaAllocator = std.heap.ArenaAllocator;

const Stmt = @import("../sqlite3/Stmt.zig");
const ValueRef = @import("../sqlite3/value.zig").Ref;

const Ctx = @import("Ctx.zig");
const EntryType = @import("entry_type.zig").EntryType;
const RowGroupEntry = @import("entries.zig").RowGroupEntry;
const sql_fmt = @import("sql_fmt.zig");

pub fn open(
    comptime PartialSortKey: type,
    ctx: *Ctx,
    tmp_arena: *ArenaAllocator,
    range: ?CursorRange(PartialSortKey),
) !Cursor {
    // TODO because the constraints are dynamically generated, it doesn't make sense to store the
    //      prepared statement in a `StmtCell`. However, this may benefit from a dynamic statement
    //      cache since applications will probably take advantage of specific access patterns
    const query = try fmt.allocPrintZ(tmp_arena.allocator(),
        \\SELECT entry_type, rowid, record_count, col_rowid, {s}
        \\FROM "{s}_primaryindex"
        \\WHERE entry_type IN ({d}, {d}) {}
        \\ORDER BY {s}, rowid ASC
    , .{
        sql_fmt.ColumnListFormatter("col_{d}"){ .len = ctx.columns_len },
        ctx.vtab_table_name,
        @intFromEnum(EntryType.RowGroup),
        @intFromEnum(EntryType.Insert),
        OptionalRangeFmt(PartialSortKey){ .range = range },
        sql_fmt.ColumnListFormatter("sk_value_{d} ASC"){ .len = ctx.sort_key.len },
    });

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

            pub fn symbol(self: @This()) []const u8 {
                return switch (self) {
                    .eq => "=",
                    .gt => ">",
                    .ge => ">=",
                    .lt => "<",
                    .le => "<=",
                };
            }
        },

        pub fn format(
            self: @This(),
            comptime _: []const u8,
            _: std.fmt.FormatOptions,
            writer: anytype,
        ) !void {
            const key_len = self.key.valuesLen();
            for (0..key_len) |idx| {
                if (idx < key_len - 1) {
                    try writer.print("sk_value_{d} = ? AND ", .{idx});
                } else {
                    try writer.print("sk_value_{d} {s} ?", .{ idx, self.last_op.symbol() });
                }
            }
        }
    };
}

fn OptionalRangeFmt(comptime PartialSortKey: type) type {
    return struct {
        range: ?CursorRange(PartialSortKey),

        pub fn format(
            self: @This(),
            comptime _: []const u8,
            _: std.fmt.FormatOptions,
            writer: anytype,
        ) !void {
            if (self.range) |r| {
                try writer.print("AND {}", .{r});
            }
        }
    };
}
