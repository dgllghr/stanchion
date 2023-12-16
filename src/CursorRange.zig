const vtab = @import("sqlite3/vtab.zig");
const FilterArgs = vtab.FilterArgs;
const BestIndexInfoOp = vtab.BestIndexInfo.Op;

key: FilterArgs,
last_op: LastOp,

const Self = @This();

pub const LastOp = enum {
    lt,
    le,
    eq,
    gt,
    ge,
};

pub fn init(key: FilterArgs, bii_last_op: BestIndexInfoOp) Self {
    const last_op: LastOp = switch (bii_last_op) {
        .lt => .lt,
        .le => .le,
        .eq => .eq,
        .gt => .gt,
        .ge => .ge,
        else => unreachable,
    };
    return .{ .key = key, .last_op = last_op };
}
