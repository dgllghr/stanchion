const std = @import("std");
const fmt = std.fmt;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const Stmt = @import("./sqlite3/Stmt.zig");

const DataType = @import("./schema/ColumnType.zig").DataType;
const Schema = @import("./schema/Schema.zig");

pub const Error = error {
    EmptyRowGroups,
};

pub const RowGroups = struct {
    const Self = @This();
    
    table_name: []const u8,
    next_id: i64,
    columns_len: usize,

    pub fn create(
        arena: *ArenaAllocator,
        db_ctx: anytype,
        table_name: []const u8,
        schema: *const Schema,
    ) !Self {
        const columnFormatter = DdlFormatter{
            .table_name = table_name,
            .schema = schema,
        };
        const ddl = try fmt.allocPrintZ(arena.allocator(), "{s}", .{columnFormatter});
        try db_ctx.conn.exec(ddl);
        return .{
            .table_name = table_name,
            .next_id = 1,
            .columns_len = schema.columns.items.len,
        };
    }

    test "create row groups table" {
        const conn = try @import("./sqlite3/Conn.zig").openInMemory();
        defer conn.close();
        var db_ctx = .{
            .conn = conn,
        };

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

        _ = try create(&arena, &db_ctx, "table_1", &schema);
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
                const data_type = DataType.SqliteFormatter {
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

    fn insertRowGroup(
        self: *Self,
        arena: *ArenaAllocator,
        db_ctx: anytype,
        sort_key: anytype,
        row_group: *RowGroup,
    ) !void {
        const id = self.next_id;
        self.next_id += 1;
        errdefer self.next_id -= 1;

        try db.insertRowGroup(
            arena.allocator(),
            db_ctx,
            self.table_name,
            sort_key,
            id,
            row_group
        );
        row_group.id = id;
    }

    test "insert row group" {
        const conn = try @import("./sqlite3/Conn.zig").openInMemory();
        defer conn.close();
        try @import("./db.zig").Migrations.apply(conn);

        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();

        var db_ctx = @import("./db.zig").Ctx(&db.db_ctx_fields){
            .conn = conn,
        };
        
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

        var row_groups = try create(&arena, &db_ctx, table_name, &schema);

        const OwnedValue = @import("./value/owned.zig").OwnedValue;
        var sort_key = try arena.allocator().alloc(OwnedValue, 1);
        sort_key[0] = .{ .Integer = 100 };

        var column_segment_ids = try arena.allocator().alloc(i64, 2);
        column_segment_ids[0] = 100;
        column_segment_ids[1] = 200;
        var row_group = RowGroup {
            .id = 0,
            .rowid_segment_id = 1,
            .column_segment_ids = column_segment_ids,
            .record_count = 1000,
        };

        try row_groups.insertRowGroup(&arena, &db_ctx, sort_key, &row_group);

        try std.testing.expectEqual(@as(i64, 1), row_group.id);
    }

    pub fn assignRowGroup(
        self: *Self,
        arena: *ArenaAllocator,
        db_ctx: anytype,
        sort_key: anytype,
    ) !RowGroup {
        return db.findRowGroup(
            arena.allocator(),
            db_ctx,
            self.table_name,
            sort_key,
            self.columns_len,
        );
    }

    test "assign row group" {
        const conn = try @import("./sqlite3/Conn.zig").openInMemory();
        defer conn.close();
        try @import("./db.zig").Migrations.apply(conn);

        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();

        var db_ctx = @import("./db.zig").Ctx(&db.db_ctx_fields){
            .conn = conn,
        };
        
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

        var row_groups = try create(&arena, &db_ctx, table_name, &schema);

        const OwnedValue = @import("./value/owned.zig").OwnedValue;
        var sort_key = try arena.allocator().alloc(OwnedValue, 1);
        sort_key[0] = .{ .Integer = 100 };

        const res = row_groups.assignRowGroup(&arena, &db_ctx, sort_key);
        try std.testing.expectError(Error.EmptyRowGroups, res);
    }

    // pub fn setRecordCount(
    //     self: *Self,
    //     db_ctx: anytype,
    //     row_group_id: i64,
    //     record_count: i64,
    // ) !void {}
};

pub const RowGroup = struct {
    const Self = @This();

    id: i64,
    rowid_segment_id: i64,
    column_segment_ids: []i64,
    record_count: i64,

    pub fn deinit(self: *Self, allocator: Allocator) void {
        allocator.free(self.column_segment_ids);
    }
};

pub const db = struct {
    pub const db_ctx_fields = [_][]const u8{
        "find_row_group_stmt",
        "insert_row_group_stmt",
    };

    pub fn insertRowGroup(
        allocator: Allocator,
        db_ctx: anytype,
        table_name: []const u8,
        sort_key: anytype,
        id: i64,
        row_group: *RowGroup,
    ) !void {
        if (db_ctx.insert_row_group_stmt == null) {
            const query = try fmt.allocPrintZ(allocator,
                \\INSERT INTO "_stanchion_{s}_row_groups" (
                \\  id, rowid_segment_id, record_count, {s}, {s}
                \\) VALUES (?, ?, ?, {s}, {s})
                , .{
                    table_name,
                    SortKeyColumnListFormatter{.len = sort_key.len},
                    SegmentIdColumnListFormatter{.len = row_group.column_segment_ids.len},
                    ParameterListFormatter{.len = sort_key.len},
                    ParameterListFormatter{.len = row_group.column_segment_ids.len},
                });
            defer allocator.free(query);
            db_ctx.insert_row_group_stmt = try db_ctx.conn.prepare(query);
        }

        const stmt = db_ctx.insert_row_group_stmt.?;
        try stmt.bind(.Int64, 1, id);
        try stmt.bind(.Int64, 2, row_group.rowid_segment_id);
        try stmt.bind(.Int64, 3, row_group.record_count);
        for (sort_key, 4..) |sk_value, idx| {
            try sk_value.bind(stmt, idx);
        }
        for (row_group.column_segment_ids, (sort_key.len + 4)..) |seg_id, idx| {
            try stmt.bind(.Int64, idx, seg_id);
        }
        try stmt.exec();
        try stmt.reset();
    }

    pub fn findRowGroup(
        allocator: Allocator,
        db_ctx: anytype,
        table_name: []const u8,
        sort_key: anytype,
        columns_len: usize,
    ) !RowGroup {
        if (db_ctx.find_row_group_stmt == null) {
            // TODO explore optimizing this query
            const query = try fmt.allocPrintZ(allocator,
                \\SELECT id, record_count, rowid_segment_id, {s}
                \\FROM "_stanchion_{s}_row_groups"
                \\WHERE ({s}) <= ({s})
                \\ORDER BY {s} DESC
                \\LIMIT 1
                , .{
                    SegmentIdColumnListFormatter{.len = columns_len},
                    table_name,
                    SortKeyColumnListFormatter{.len = sort_key.len},
                    ParameterListFormatter{.len = sort_key.len},
                    SortKeyColumnListFormatter{.len = sort_key.len},
                });
            defer allocator.free(query);
            db_ctx.find_row_group_stmt = try db_ctx.conn.prepare(query);
        }

        const stmt = db_ctx.find_row_group_stmt.?;
        for (sort_key, 1..) |sk_value, i| {
            try sk_value.bind(stmt, i);
        }
        if (!try stmt.next()) {
            // Assumes that there is a row group with the minimum sort key so that a row
            // group will always be found after the first row group is added
            return Error.EmptyRowGroups;
        }
        var row_group = RowGroup {
            .id = stmt.read(.Int64, false, 0),
            .record_count = stmt.read(.Int64, false, 1),
            .rowid_segment_id = stmt.read(.Int64, false, 2),
            .column_segment_ids = try allocator.alloc(i64, columns_len),
        };
        for (0..columns_len) |idx| {
            row_group.column_segment_ids[idx] = stmt.read(.Int64, false, idx + 3);
        }
        try stmt.reset();
        return row_group;
    }

    const SegmentIdColumnListFormatter = struct {
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
                try writer.print("column_{d}_segment_id", .{idx});
            }
        }
    };

    const SortKeyColumnListFormatter = struct {
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
                try writer.print("sk_value_{d}", .{idx});
            }
        }
    };

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
};

