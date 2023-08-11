const std = @import("std");
const fmt = std.fmt;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const Conn = @import("../sqlite3/Conn.zig");
const Stmt = @import("../sqlite3/Stmt.zig");

const RowGroup = @import("./RowGroup.zig");
const Schema = @import("../schema.zig").Schema;

const Error = @import("./error.zig").Error;

const Self = @This();

conn: Conn,
table_name: []const u8,
schema: *const Schema,
insert_row_group: ?Stmt,
find_row_group: ?Stmt,

pub fn init(conn: Conn, table_name: []const u8, schema: *const Schema) Self {
    return .{
        .conn = conn,
        .table_name = table_name,
        .schema = schema,
        .insert_row_group = null,
        .find_row_group = null,
    };
}

pub fn deinit(self: *Self) void {
    if (self.insert_row_group) |stmt| {
        stmt.deinit();
    }
    if (self.find_row_group) |stmt| {
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
        const sort_key_len = self.schema.sort_key.items.len;
        const columns_len = self.schema.columns.items.len;
        const query = try fmt.allocPrintZ(allocator,
            \\INSERT INTO "_stanchion_{s}_row_groups" (
            \\  id, rowid_segment_id, record_count, {s}, {s}
            \\) VALUES (?, ?, ?, {s}, {s})
        , .{
            self.table_name,
            SortKeyColumnListFormatter{ .len = sort_key_len },
            SegmentIdColumnListFormatter{ .len = columns_len },
            ParameterListFormatter{ .len = sort_key_len },
            ParameterListFormatter{ .len = columns_len },
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

pub fn findRowGroup(
    self: *Self,
    allocator: Allocator,
    sort_key: anytype,
) !RowGroup {
    const columns_len = self.schema.columns.items.len;
    if (self.find_row_group == null) {
        const sort_key_len = self.schema.sort_key.items.len;
        // TODO explore optimizing this query
        const query = try fmt.allocPrintZ(allocator,
            \\SELECT id, record_count, rowid_segment_id, {s}
            \\FROM "_stanchion_{s}_row_groups"
            \\WHERE ({s}) <= ({s})
            \\ORDER BY {s} DESC
            \\LIMIT 1
        , .{
            SegmentIdColumnListFormatter{ .len = columns_len },
            self.table_name,
            SortKeyColumnListFormatter{ .len = sort_key_len },
            ParameterListFormatter{ .len = sort_key_len },
            SortKeyColumnListFormatter{ .len = sort_key_len },
        });
        defer allocator.free(query);
        self.find_row_group = try self.conn.prepare(query);
    }

    const stmt = self.find_row_group.?;
    for (sort_key, 1..) |sk_value, i| {
        try sk_value.bind(stmt, i);
    }
    if (!try stmt.next()) {
        // Assumes that there is a row group with the minimum sort key so that a row
        // group will always be found after the first row group is added
        return Error.EmptyRowGroups;
    }
    var row_group = RowGroup{
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
