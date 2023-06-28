const encode = @import("../../encode.zig");
const enc = @import("../encoding.zig");
const MakeMessageLog = @import("../message_log.zig").MessageLog;
const Encoding = enc.Encoding;
const InvalidEncodingError = enc.InvalidEncodingError;

pub const Value = bool;

pub const Decoder = union(enum) {
    const Self = @This();

    bit_packed: encode.bit_packed_bool.Decoder,
    constant: encode.constant.Decoder(bool, readBool),

    pub fn init(encoding: Encoding, blob: anytype) !Self {
        return switch (encoding) {
            .Constant => .{
                .constant = try encode.constant.Decoder(bool, readBool).init(blob)
            },
            .BitPacked => .{
                .bit_packed = try encode.bit_packed_bool.Decoder.init(blob)
            },
            else => return InvalidEncodingError.InvalidEncoding,
        };
    }

    pub fn decode(self: *Self, blob: anytype, index: usize) !bool {
        switch (self.*) {
            inline else => |*d| return d.decode(blob, index),
        }
    }
};

pub const Encoder = union(enum) {
    bit_packed: encode.bit_packed_bool.Encoder,
    constant: encode.constant.Encoder(bool, writeBool),
};

const validators = .{
    .bit_packed = encode.bit_packed_bool.Validator.init(),
    .constant = encode.constant.Validator(bool, writeBool).init(),
};

pub const MessageLog = MakeMessageLog(bool, readBool, writeBool);

fn readBool(v: *const [1]u8) bool {
    if (v[0] > 0) {
        return true;
    }
    return false;
}

fn writeBool(v: bool) [1]u8 {
    if (v) {
        return [1]u8{1};
    }
    return [1]u8{0};
}
