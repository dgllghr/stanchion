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