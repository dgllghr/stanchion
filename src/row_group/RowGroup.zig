const std = @import("std");
const Allocator = std.mem.Allocator;

const Self = @This();

id: i64,
rowid_segment_id: i64,
column_segment_ids: []i64,
record_count: i64,

pub fn deinit(self: *Self, allocator: Allocator) void {
    allocator.free(self.column_segment_ids);
}
