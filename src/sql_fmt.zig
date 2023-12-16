const std = @import("std");
const fmt = std.fmt;

pub fn ColumnListLenFormatter(comptime column_name_template: []const u8) type {
    return struct {
        len: usize,

        pub fn format(
            self: @This(),
            comptime _: []const u8,
            _: fmt.FormatOptions,
            writer: anytype,
        ) !void {
            for (0..self.len) |idx| {
                if (idx > 0) {
                    try writer.print(", ", .{});
                }
                try writer.print(column_name_template, .{idx});
            }
        }
    };
}

pub fn ColumnListIndicesFormatter(comptime column_name_template: []const u8) type {
    return struct {
        indices: []const usize,

        pub fn format(
            self: @This(),
            comptime _: []const u8,
            _: fmt.FormatOptions,
            writer: anytype,
        ) !void {
            for (self.indices, 0..) |idx, iter_idx| {
                if (iter_idx > 0) {
                    try writer.print(", ", .{});
                }
                try writer.print(column_name_template, .{idx});
            }
        }
    };
}

pub const ParameterListFormatter = struct {
    len: usize,

    pub fn format(
        self: @This(),
        comptime _: []const u8,
        _: fmt.FormatOptions,
        writer: anytype,
    ) !void {
        for (0..self.len) |idx| {
            if (idx > 0) {
                try writer.print(", ", .{});
            }
            try writer.print("?", .{});
        }
    }
};
