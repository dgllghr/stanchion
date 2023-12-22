const std = @import("std");
const fmt = std.fmt;
const testing = std.testing;
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

test "schema: format create table ddl for sqlite" {
    const Conn = @import("../sqlite3.zig").Conn;
    var conn = try Conn.openInMemory();
    defer conn.close();

    const columns = [_]Column{
        Column{
            .rank = 0,
            .name = "first_col",
            .column_type = .{ .data_type = .Integer, .nullable = false },
            .sk_rank = 0,
        },
        Column{
            .rank = 1,
            .name = "second_col",
            .column_type = .{ .data_type = .Float, .nullable = true },
            .sk_rank = null,
        },
        Column{
            .rank = 3,
            .name = "third_col",
            .column_type = .{ .data_type = .Text, .nullable = false },
            .sk_rank = null,
        },
        Column{
            .rank = 3,
            .name = "fourth_col",
            .column_type = .{ .data_type = .Boolean, .nullable = false },
            .sk_rank = null,
        },
    };
    const sort_key = [_]usize{0};
    const schema = Self{ .columns = &columns, .sort_key = &sort_key };

    const ddl = try fmt.allocPrintZ(testing.allocator, "{}", .{schema.sqliteDdlFormatter("test")});
    defer testing.allocator.free(ddl);
    try conn.exec(ddl);
}

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
