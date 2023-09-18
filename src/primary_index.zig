const std = @import("std");
const fmt = std.fmt;
const mem = std.mem;
const Allocator = std.mem.Allocator;

const Conn = @import("sqlite3/Conn.zig");
const Stmt = @import("sqlite3/Stmt.zig");

const StmtCell = @import("StmtCell.zig");

const s = @import("schema.zig");
const DataType = s.ColumnType.DataType;
const Schema = s.Schema;

pub const PrimaryIndex = struct {
    const Self = @This();

    allocator: Allocator,
    db: Db,
    next_rowid: i64,

    pub fn create(
        allocator: Allocator,
        conn: Conn,
        table_name: []const u8,
        schema: *const Schema,
    ) !Self {
        try Db.createTable(allocator, conn, table_name, schema);
        const db = try Db.init(allocator, conn, table_name, schema);
        return .{
            .allocator = allocator,
            .db = db,
            .next_rowid = 1,
        };
    }

    test "insert log: create" {
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

        var primary_index = try create(std.testing.allocator, conn, "test", &schema);
        defer primary_index.deinit();
    }

    pub fn open(
        allocator: Allocator,
        conn: Conn,
        table_name: []const u8,
        schema: *const Schema,
        next_rowid: i64,
    ) !Self {
        const db = try Db.init(allocator, conn, table_name, schema);
        return .{
            .allocator = allocator,
            .db = db,
            .next_rowid = next_rowid,
        };
    }

    pub fn deinit(self: *Self) void {
        self.db.deinit(self.allocator);
    }

    pub fn insert(self: *Self, values: anytype) !void {
        const rowid = self.next_rowid;
        self.next_rowid += 1;
        try self.db.insertInsertEntry(rowid, values);
    }

    test "insert log: log insert" {
        const OwnedRow = @import("value.zig").OwnedRow;
        const OwnedValue = @import("value.zig").OwnedValue;

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

        var primary_index = try create(std.testing.allocator, conn, "test", &schema);
        defer primary_index.deinit();

        var row = [_]OwnedValue{
            .{ .Blob = "magnitude" },
            .{ .Integer = 100 },
        };
        try primary_index.insert(OwnedRow{
            .rowid = null,
            .values = &row,
        });

        row = [_]OwnedValue{
            .{ .Blob = "amplitude" },
            .{ .Integer = 7 },
        };
        try primary_index.insert(OwnedRow{
            .rowid = null,
            .values = &row,
        });
    }
};

const Db = struct {
    const Self = @This();

    conn: Conn,
    table_name: []const u8,
    columns_len: usize,
    sort_key: []const usize,
    insert_entry: StmtCell,

    pub fn init(
        allocator: Allocator,
        conn: Conn,
        table_name: []const u8,
        schema: *const Schema,
    ) !Self {
        const columns_len = schema.columns.items.len;
        const sort_key = schema.sort_key.items;

        // TODO update this comment
        // Use the fact that row groups created later always have a larger id to always
        // insert into the newest row group when multiple row groups have the same
        // starting sort key. Because of how merging the insert log works, the newest
        // row group is the only one of a group of row groups with the same starting sort
        // key that may have capacity for new records.

        const insert_entry_query = try fmt.allocPrintZ(allocator,
            \\INSERT INTO "_stanchion_{s}_primary_index" (
            \\  entry_type, record_count, rowid, rowid_segment_id, {s}, {s}
            \\) VALUES (
            \\  ?, ?, ?, ?, {s}
            \\)
        , .{
            table_name,
            ColumnListFormatter("sk_value_{d}"){ .len = sort_key.len },
            ColumnListFormatter("col_{d}"){ .len = columns_len },
            ParameterListFormatter{ .len = columns_len + sort_key.len },
        });
        const insert_entry = StmtCell.init(insert_entry_query);

        return .{
            .conn = conn,
            .table_name = table_name,
            .columns_len = columns_len,
            .sort_key = sort_key,
            .insert_entry = insert_entry,
        };
    }

    pub fn deinit(self: *Self, allocator: Allocator) void {
        self.insert_entry.deinit(allocator);
    }

    pub fn insertInsertEntry(
        self: *Self,
        rowid: i64,
        values: anytype,
    ) !void {
        const stmt = try self.insert_entry.getStmt(self.conn);
        defer self.insert_entry.reset();

        try EntryType.Insert.bind(stmt, 1);
        try stmt.bindNull(2);
        try stmt.bind(.Int64, 3, rowid);
        try stmt.bindNull(4);
        for (self.sort_key, 5..) |rank, idx| {
            try values.readValue(rank).bind(stmt, idx);
        }
        const col_idx_start = self.sort_key.len + 5;
        cols: for (0..self.columns_len, col_idx_start..) |rank, idx| {
            // Do not duplicate sort key values
            for (self.sort_key) |sk_rank| {
                if (sk_rank == rank) {
                    continue :cols;
                }
            }
            try values.readValue(rank).bind(stmt, idx);
        }

        try stmt.exec();
    }

    pub fn insertRowGroupEntry(
        self: *Self,
        sort_key: anytype,
        row_group: *const RowGroup,
    ) !void {
        const stmt = try self.insert_entry.getStmt(self.conn);
        defer self.insert_entry.reset();

        try EntryType.RowGroup.bind(stmt, 1);
        try stmt.bind(.Int64, 2, @intCast(row_group.record_count));
        try stmt.bind(.Int64, 3, row_group.rowid);
        try stmt.bind(.Int64, 4, row_group.rowid_segment_id);
        for (0..self.sort_key.len) |idx| {
            try sort_key.readValue(idx).bind(stmt, idx + 5);
        }
        const col_idx_start = self.sort_key.len + 5;
        for (row_group.column_segment_ids, col_idx_start..) |seg_id, idx| {
            try stmt.bind(.Int64, idx, seg_id);
        }

        try stmt.exec();
    }

    pub fn createTable(
        allocator: Allocator,
        conn: Conn,
        table_name: []const u8,
        schema: *const Schema,
    ) !void {
        const ddl_formatter = DdlFormatter{
            .table_name = table_name,
            .schema = schema,
        };
        const ddl = try fmt.allocPrintZ(allocator, "{s}", .{ddl_formatter});
        defer allocator.free(ddl);
        try conn.exec(ddl);
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
                \\CREATE TABLE "_stanchion_{s}_primary_index" (
            ,
                .{self.table_name},
            );
            for (self.schema.sort_key.items, 0..) |sk_index, sk_rank| {
                const col = &self.schema.columns.items[sk_index];
                const data_type = DataType.SqliteFormatter{
                    .data_type = col.column_type.data_type,
                };
                try writer.print("sk_value_{d} {s} NOT NULL,", .{ sk_rank, data_type });
            }
            // The `col_N` columns are uesd for storing values for inserts entries and
            // segment IDs for row group entries
            for (0..self.schema.columns.items.len) |rank| {
                try writer.print("col_{d} ANY NULL,", .{rank});
            }
            try writer.print(
            // Only used by the row group entry type
                \\rowid_segment_id INTEGER NULL,
                // Used by all entry types
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
            // TODO in tests it would be nice to add some check constraints to ensure
            //      data is populated correctly based on the entry_type
            try writer.print(") STRICT, WITHOUT ROWID", .{});
        }
    };

    fn ColumnListFormatter(comptime column_name_template: []const u8) type {
        return struct {
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
                    try writer.print(column_name_template, .{idx});
                }
            }
        };
    }

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

const EntryType = enum(u8) {
    RowGroup = 1,
    Insert = 2,

    fn bind(self: EntryType, stmt: Stmt, index: usize) !void {
        try stmt.bind(.Int32, index, @intCast(@intFromEnum(self)));
    }
};

const RowGroup = struct {
    const Self = @This();

    rowid: i64,
    rowid_segment_id: i64,
    column_segment_ids: []i64,
    record_count: u32,

    pub fn deinit(self: *Self, allocator: Allocator) void {
        allocator.free(self.column_segment_ids);
    }
};
