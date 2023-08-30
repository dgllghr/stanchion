const Encoding = @import("../encoding.zig").Encoding;
const Error = @import("../error.zig").Error;

const bit_packed_bool = @import("../encode/bit_packed_bool.zig");
const constant = @import("../encode/constant.zig");

pub const Value = bool;

const Tag = enum {
    bit_packed,
    constant,
};

pub const Decoder = union(Tag) {
    const Self = @This();

    bit_packed: bit_packed_bool.Decoder,
    constant: constant.Decoder(bool, readDirect),

    pub fn init(encoding: Encoding, blob: anytype) !Self {
        return switch (encoding) {
            .Constant => .{
                .constant = try constant.Decoder(bool, readDirect).init(blob),
            },
            .BitPacked => .{
                .bit_packed = try bit_packed_bool.Decoder.init(blob),
            },
            else => return Error.InvalidEncoding,
        };
    }

    pub fn decode(self: *Self, blob: anytype, index: usize) !bool {
        switch (self.*) {
            inline else => |*d| return d.decode(blob, index),
        }
    }
};

pub const Validators = struct {
    const Self = @This();

    bit_packed: bit_packed_bool.Validator,
    constant: constant.Validator(bool, writeDirect),

    pub fn init() Self {
        return .{
            .bit_packed = bit_packed_bool.Validator.init(),
            .constant = constant.Validator(bool, writeDirect).init(),
        };
    }
};

pub const Encoder = union(Tag) {
    bit_packed: bit_packed_bool.Encoder,
    constant: constant.Encoder(bool, writeDirect),
};

pub fn readDirect(v: *const [1]u8) bool {
    if (v[0] > 0) {
        return true;
    }
    return false;
}

pub fn writeDirect(v: bool) [1]u8 {
    if (v) {
        return [1]u8{1};
    }
    return [1]u8{0};
}
