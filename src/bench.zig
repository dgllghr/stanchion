const row_group = @import("row_group.zig");

pub fn main() !void {
    try row_group.benchRowGroupCreate();
}
