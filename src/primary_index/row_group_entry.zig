const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Entry = struct {
    rowid_segment_id: i64,
    column_segment_ids: []i64,
    /// Record count of 0 means that the row group does not exist because there are no empty row
    /// groups. If the record count is 0, the other fields on this struct are undefined and should
    /// not be read
    record_count: u32,

    pub fn deinit(self: *Entry, allocator: Allocator) void {
        allocator.free(self.column_segment_ids);
    }
};
