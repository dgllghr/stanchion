const std = @import("std");
const fmt = std.fmt;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const Conn = @import("./sqlite3/Conn.zig");
const Stmt = @import("./sqlite3/Stmt.zig");

const DataType = @import("./schema/ColumnType.zig").DataType;
const Schema = @import("./schema/Schema.zig");

pub const Error = @import("./row_group/error.zig").Error;
pub const RowGroup = @import("./row_group/RowGroup.zig");
const Db = @import("./row_group/Db.zig");

pub const RowGroups = struct {
    const Self = @This();

    table_name: []const u8,
    db: Db,
    next_id: i64,

    pub fn create(
        arena: *ArenaAllocator,
        conn: Conn,
        table_name: []const u8,
        schema: *const Schema,
    ) !Self {
        const columnFormatter = DdlFormatter{
            .table_name = table_name,
            .schema = schema,
        };
        const ddl = try fmt.allocPrintZ(arena.allocator(), "{s}", .{columnFormatter});
        try conn.exec(ddl);
        const db = Db.init(conn, table_name, schema);
        return .{
            .table_name = table_name,
            .db = db,
            .next_id = 1,
        };
    }

    test "create row groups table" {
        const conn = try @import("./sqlite3/Conn.zig").openInMemory();
        defer conn.close();

        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();

        const ArrayListUnmanaged = std.ArrayListUnmanaged;
        const Column = @import("./schema/Column.zig");
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

        _ = try create(&arena, conn, "table_1", &schema);
    }

    const DdlFormatter = struct {
        table_name: []const u8,
        schema: *const Schema,

        pub fn format(
            self: @This(),
            comptime _: []const u8,
            _: std.fmt.FormatOptions,
            writer: anytype,
        ) !void {
            try writer.print(
                \\CREATE TABLE "_stanchion_{s}_row_groups" (
            ,
                .{self.table_name},
            );
            for (self.schema.sort_key.items, 0..) |_, sk_rank| {
                const col = &self.schema.columns.items[sk_rank];
                const data_type = DataType.SqliteFormatter{
                    .data_type = col.column_type.data_type,
                };
                try writer.print(
                    \\sk_value_{d} {s} NOT NULL,
                , .{ sk_rank, data_type });
            }
            // The `id` field is used in two ways:
            // 1. a global identifier for a row so changes to the row group can be
            //    expressed more easily
            // 2. A way to distinguish row groups with the same starting sort key. The
            //    id is always increased so splitting a row group puts the new row group
            //    after the existing one.
            try writer.print("id INTEGER NOT NULL,", .{});
            try writer.print("rowid_segment_id INTEGER NOT NULL,", .{});
            for (self.schema.columns.items) |*col| {
                try writer.print(
                    \\column_{d}_segment_id INTEGER NOT NULL,
                , .{col.rank});
            }
            try writer.print("record_count INTEGER NOT NULL,", .{});
            try writer.print("UNIQUE (id),", .{});
            try writer.print("PRIMARY KEY (", .{});
            for (self.schema.sort_key.items, 0..) |_, sk_rank| {
                try writer.print("sk_value_{d},", .{sk_rank});
            }
            try writer.print("id)", .{});
            try writer.print(") STRICT, WITHOUT ROWID", .{});
        }
    };

    pub fn open(
        arena: *ArenaAllocator,
        conn: Conn,
        table_name: []const u8,
        schema: *const Schema,
    ) !Self {
        var db = Db.init(conn, table_name, schema);
        const max_id = try db.maxId(arena.allocator());
        return .{
            .table_name = table_name,
            .db = db,
            .next_id = max_id + 1,
        };
    }

    fn insertRowGroup(
        self: *Self,
        arena: *ArenaAllocator,
        sort_key: anytype,
        row_group: *RowGroup,
    ) !void {
        const id = self.next_id;
        self.next_id += 1;
        errdefer self.next_id -= 1;

        try self.db.insertRowGroup(arena.allocator(), sort_key, id, row_group);
        row_group.id = id;
    }

    test "insert row group" {
        const conn = try @import("./sqlite3/Conn.zig").openInMemory();
        defer conn.close();
        try @import("./db.zig").Migrations.apply(conn);

        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();

        const table_name = "foos";
        const ArrayListUnmanaged = std.ArrayListUnmanaged;
        const Column = @import("./schema/Column.zig");
        var columns = ArrayListUnmanaged(Column){};
        try columns.appendSlice(arena.allocator(), &[_]Column{
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
        try sort_key_def.append(arena.allocator(), 0);
        var schema = Schema{
            .columns = columns,
            .sort_key = sort_key_def,
        };

        var row_groups = try create(&arena, conn, table_name, &schema);

        const OwnedValue = @import("./value/owned.zig").OwnedValue;
        var sort_key = try arena.allocator().alloc(OwnedValue, 1);
        sort_key[0] = .{ .Integer = 100 };

        var column_segment_ids = try arena.allocator().alloc(i64, 2);
        column_segment_ids[0] = 100;
        column_segment_ids[1] = 200;
        var row_group = RowGroup{
            .id = 0,
            .rowid_segment_id = 1,
            .column_segment_ids = column_segment_ids,
            .record_count = 1000,
        };

        try row_groups.insertRowGroup(&arena, sort_key, &row_group);

        try std.testing.expectEqual(@as(i64, 1), row_group.id);
    }

    pub fn assignRowGroup(
        self: *Self,
        arena: *ArenaAllocator,
        sort_key: anytype,
    ) !RowGroup {
        return self.db.findRowGroup(
            arena.allocator(),
            sort_key,
        );
    }

    test "assign row group" {
        const conn = try @import("./sqlite3/Conn.zig").openInMemory();
        defer conn.close();
        try @import("./db.zig").Migrations.apply(conn);

        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();

        const table_name = "foos";
        const ArrayListUnmanaged = std.ArrayListUnmanaged;
        const Column = @import("./schema/Column.zig");
        var columns = ArrayListUnmanaged(Column){};
        try columns.appendSlice(arena.allocator(), &[_]Column{
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
        try sort_key_def.append(arena.allocator(), 0);
        const schema = Schema{
            .columns = columns,
            .sort_key = sort_key_def,
        };

        var row_groups = try create(&arena, conn, table_name, &schema);

        const OwnedValue = @import("./value/owned.zig").OwnedValue;
        var sort_key = try arena.allocator().alloc(OwnedValue, 1);
        sort_key[0] = .{ .Integer = 100 };

        const res = row_groups.assignRowGroup(&arena, sort_key);
        try std.testing.expectError(Error.EmptyRowGroups, res);
    }

    // pub fn setRecordCount(
    //     self: *Self,
    //     db_ctx: anytype,
    //     row_group_id: i64,
    //     record_count: i64,
    // ) !void {}
};
