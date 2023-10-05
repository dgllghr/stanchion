const std = @import("std");
const Allocator = std.mem.Allocator;

const ColumnType = @import("./ColumnType.zig");

rank: i32,
name: []const u8,
column_type: ColumnType,
sk_rank: ?u16,

pub fn deinit(self: *@This(), allocator: Allocator) void {
    allocator.free(self.name);
}

pub fn sqliteDdlFormatter(self: @This()) SqliteDdlFormatter {
    return .{
        .name = self.name,
        .column_type = self.column_type,
    };
}

pub const SqliteDdlFormatter = struct {
    name: []const u8,
    column_type: ColumnType,

    pub fn format(
        self: @This(),
        comptime _: []const u8,
        _: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        try writer.print(
            \\{s} {s}
        , .{ self.name, self.column_type.sqliteFormatter() });
    }
};
