const std = @import("std");
const fmt = std.fmt;
const Allocator = std.mem.Allocator;
const ArrayListUnmanaged = std.ArrayListUnmanaged;
const Type = std.builtin.Type;

const Column = @import("./Column.zig");
const ColumnType = @import("./ColumnType.zig");
const SchemaDef = @import("./SchemaDef.zig");
const db = @import("./db.zig");

const Self = @This();

columns: ArrayListUnmanaged(Column),
sort_key: ArrayListUnmanaged(usize),

pub const Error = error{ SortKeyColumnNotFound, ExecReturnedData };

pub fn create(allocator: Allocator, db_ctx: anytype, table_id: i64, def: SchemaDef) !Self {
    // Find and validate sort keys
    var sort_key = try ArrayListUnmanaged(usize)
        .initCapacity(allocator, def.sort_key.items.len);
    errdefer sort_key.deinit(allocator);
    sk: for (def.sort_key.items) |name| {
        for (def.columns.items, 0..) |col, rank| {
            // TODO support unicode
            if (std.ascii.eqlIgnoreCase(name, col.name)) {
                sort_key.appendAssumeCapacity(rank);
                continue :sk;
            }
        }
        return Error.SortKeyColumnNotFound;
    }

    // TODO create rowid column

    var columns = try ArrayListUnmanaged(Column)
        .initCapacity(allocator, def.columns.items.len);
    errdefer columns.deinit(allocator);
    errdefer for (columns.items) |*col| {
        col.deinit(allocator);
    };
    for (def.columns.items, 0..) |col_def, rank| {
        const col_name = try allocator.dupe(u8, col_def.name);
        var sk_rank: ?u16 = null;
        for (sort_key.items, 0..) |sr, r| {
            if (rank == sr) {
                sk_rank = @intCast(r);
                break;
            }
        }
        const col: Column = .{
            .rank = @intCast(rank),
            .name = col_name,
            .column_type = col_def.column_type,
            .sk_rank = sk_rank,
        };

        // TODO this could use an arena allocator
        try db.createColumn(allocator, db_ctx, table_id, &col);

        columns.appendAssumeCapacity(col);
    }

    return .{
        .columns = columns,
        .sort_key = sort_key,
    };
}

pub fn load(allocator: Allocator, db_ctx: anytype, table_id: i64) !Self {
    var columns = ArrayListUnmanaged(Column){};
    var sort_key_len: usize = undefined;
    try db.loadColumns(allocator, db_ctx, table_id, &columns, &sort_key_len);

    var sort_key = try ArrayListUnmanaged(usize).initCapacity(allocator, sort_key_len);
    sort_key.expandToCapacity();
    for (columns.items, 0..) |col, col_rank| {
        if (col.sk_rank) |sk_rank| {
            sort_key.items[sk_rank] = col_rank;
        }
    }

    return .{
        .columns = columns,
        .sort_key = sort_key,
    };
}

pub fn deinit(self: *Self, allocator: Allocator) void {
    self.sort_key.deinit(allocator);
    for (self.columns.items) |*col| {
        col.deinit(allocator);
    }
    self.columns.deinit(allocator);
}
