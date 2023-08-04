pub const bit_packed_bool = @import("./encode/bit_packed_bool.zig");
pub const constant = @import("./encode/constant.zig");

pub const makeChooser = @import("./encode/chooser.zig").makeChooser;

pub const Encoding = enum(u8) {
    Direct = 1,
    Constant = 2,
    BitPacked = 3,
    _,
};
