const std = @import("std");
const fmt = std.fmt;
const Allocator = std.mem.Allocator;

const Conn = @import("sqlite3/Conn.zig");
const Stmt = @import("sqlite3/Stmt.zig");

const s = @import("schema.zig");
const DataType = s.ColumnType.DataType;
const Schema = s.Schema;

pub const RowGroup = @import("row_group/RowGroup.zig");
const Db = @import("row_group/Db.zig");

pub const RowGroups = struct {
    const Self = @This();

    table_name: []const u8,
    db: Db,
    next_id: i64,

    pub fn create(
        allocator: Allocator,
        conn: Conn,
        table_name: []const u8,
        schema: *const Schema,
    ) !Self {
        try Db.createTable(allocator, conn, table_name, schema);
        const db = Db.init(conn, table_name, schema);
        return .{
            .table_name = table_name,
            .db = db,
            .next_id = 1,
        };
    }

    test "row groups: create" {
        const conn = try @import("sqlite3/Conn.zig").openInMemory();
        defer conn.close();

        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();

        const ArrayListUnmanaged = std.ArrayListUnmanaged;
        const Column = @import("schema/Column.zig");
        var columns = ArrayListUnmanaged(Column){};
        try columns.appendSlice(arena.allocator(), &[_]Column{
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
        try sort_key.append(arena.allocator(), 0);
        const schema = Schema{
            .columns = columns,
            .sort_key = sort_key,
        };

        _ = try create(arena.allocator(), conn, "table_1", &schema);
    }

    pub fn open(
        allocator: Allocator,
        conn: Conn,
        table_name: []const u8,
        schema: *const Schema,
    ) !Self {
        var db = Db.init(conn, table_name, schema);
        const max_id = try db.maxId(allocator);
        return .{
            .table_name = table_name,
            .db = db,
            .next_id = max_id + 1,
        };
    }

    pub fn insertRowGroup(
        self: *Self,
        allocator: Allocator,
        sort_key: anytype,
        row_group: *RowGroup,
    ) !void {
        const id = self.next_id;
        self.next_id += 1;
        errdefer self.next_id -= 1;

        try self.db.insertRowGroup(allocator, sort_key, id, row_group);
        row_group.id = id;
    }

    test "row groups: insert" {
        const conn = try @import("sqlite3/Conn.zig").openInMemory();
        defer conn.close();
        try @import("db.zig").Migrations.apply(conn);

        const table_name = "foos";
        const ArrayListUnmanaged = std.ArrayListUnmanaged;
        const Column = @import("schema/Column.zig");
        var columns = ArrayListUnmanaged(Column){};
        defer columns.deinit(std.testing.allocator);
        try columns.appendSlice(std.testing.allocator, &[_]Column{
            .{
                .rank = 0,
                .name = "foo",
                .column_type = .{ .data_type = .Integer, .nullable = false },
                .sk_rank = 0,
            },
            .{
                .rank = 1,
                .name = "bar",
                .column_type = .{ .data_type = .Float, .nullable = true },
                .sk_rank = null,
            },
        });
        var sort_key_def = ArrayListUnmanaged(usize){};
        defer sort_key_def.deinit(std.testing.allocator);
        try sort_key_def.append(std.testing.allocator, 0);
        var schema = Schema{
            .columns = columns,
            .sort_key = sort_key_def,
        };

        var row_groups = try create(std.testing.allocator, conn, table_name, &schema);

        const OwnedValue = @import("value/owned.zig").OwnedValue;
        var sort_key = try std.testing.allocator.alloc(OwnedValue, 1);
        defer std.testing.allocator.free(sort_key);
        sort_key[0] = .{ .Integer = 100 };

        var column_segment_ids = try std.testing.allocator.alloc(i64, 2);
        defer std.testing.allocator.free(column_segment_ids);
        column_segment_ids[0] = 100;
        column_segment_ids[1] = 200;
        var row_group = RowGroup{
            .id = 0,
            .rowid_segment_id = 1,
            .column_segment_ids = column_segment_ids,
            .record_count = 1000,
        };

        try row_groups.insertRowGroup(std.testing.allocator, sort_key, &row_group);

        try std.testing.expectEqual(@as(i64, 1), row_group.id);
    }
};
