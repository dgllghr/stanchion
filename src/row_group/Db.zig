const std = @import("std");
const fmt = std.fmt;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const Conn = @import("../sqlite3/Conn.zig");
const Stmt = @import("../sqlite3/Stmt.zig");

const s = @import("../schema.zig");
const Schema = s.Schema;
const DataType = s.ColumnType.DataType;

const RowGroup = @import("RowGroup.zig");

const Self = @This();

conn: Conn,
table_name: []const u8,
columns_len: usize,
sort_key_len: usize,
insert_row_group: ?Stmt,

pub fn init(conn: Conn, table_name: []const u8, schema: *const Schema) Self {
    return .{
        .conn = conn,
        .table_name = table_name,
        .columns_len = schema.columns.items.len,
        .sort_key_len = schema.sort_key.items.len,
        .insert_row_group = null,
    };
}

pub fn deinit(self: *Self) void {
    if (self.insert_row_group) |stmt| {
        stmt.deinit();
    }
}

pub fn maxId(self: *Self, allocator: Allocator) !i64 {
    const query = try fmt.allocPrintZ(allocator,
        \\SELECT MAX(id)
        \\FROM "_stanchion_{s}_row_groups"
    , .{self.table_name});
    defer allocator.free(query);
    const stmt = try self.conn.prepare(query);
    if (!try stmt.next()) {
        return 0;
    }
    const id = stmt.read(.Int64, false, 0);
    try stmt.reset();
    return id;
}

pub fn insertRowGroup(
    self: *Self,
    allocator: Allocator,
    sort_key: anytype,
    id: i64,
    row_group: *RowGroup,
) !void {
    if (self.insert_row_group == null) {
        const query = try fmt.allocPrintZ(allocator,
            \\INSERT INTO "_stanchion_{s}_row_groups" (
            \\  id, rowid_segment_id, record_count, {s}, {s}
            \\) VALUES (?, ?, ?, {s}, {s})
        , .{
            self.table_name,
            SortKeyColumnListFormatter{ .len = self.sort_key_len },
            SegmentIdColumnListFormatter{ .len = self.columns_len },
            ParameterListFormatter{ .len = self.sort_key_len },
            ParameterListFormatter{ .len = self.columns_len },
        });
        defer allocator.free(query);
        self.insert_row_group = try self.conn.prepare(query);
    }

    const stmt = self.insert_row_group.?;
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
