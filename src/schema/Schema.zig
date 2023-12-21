const std = @import("std");
const fmt = std.fmt;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;
const ArrayListUnmanaged = std.ArrayListUnmanaged;

const Column = @import("Column.zig");
const ColumnType = @import("ColumnType.zig");
const SchemaDef = @import("SchemaDef.zig");

const Self = @This();

columns: []const Column,
sort_key: []const usize,

pub fn sqliteDdlFormatter(self: Self, table_name: []const u8) SqliteDdlFormatter {
    return .{
        .columns = self.columns,
        .table_name = table_name,
    };
}

pub const SqliteDdlFormatter = struct {
    columns: []const Column,
    table_name: []const u8,

    pub fn format(
        self: @This(),
        comptime _: []const u8,
        _: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        try writer.print(
            \\CREATE TABLE "{s}" ({s})
        ,
            .{
                self.table_name,
                SqliteDdlColumnListFormatter{ .columns = self.columns },
            },
        );
    }
};

const SqliteDdlColumnListFormatter = struct {
    columns: []const Column,

    pub fn format(
        self: @This(),
        comptime _: []const u8,
        _: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        for (self.columns, 0..) |col, idx| {
            if (idx > 0) {
                try writer.print(",", .{});
            }
            try writer.print("{s}", .{col.sqliteDdlFormatter()});
        }
    }
};
