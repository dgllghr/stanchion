const std = @import("std");
const mem = std.mem;

const Encoding = @import("../encoding.zig").Encoding;
const Valid = @import("../validator.zig").Valid;

const direct = @This();

pub fn Decoder(
    comptime Value: type,
    comptime fromBytes: fn (*const [@sizeOf(Value)]u8) Value,
) type {
    return struct {
        const Self = @This();

        pub fn init(_: anytype) !Self {
            return .{};
        }

        pub fn decode(self: *Self, blob: anytype, index: usize) !Value {
            var buf: [@sizeOf(Value)]u8 = undefined;
            try blob.readAt(buf[0..], index * @sizeOf(Value));
            return fromBytes(&buf);
        }
    };
}

pub fn Validator(
    comptime Value: type,
    comptime toBytes: fn (Value) [@sizeOf(Value)]u8,
) type {
    return struct {
        const Self = @This();

        count: usize,

        pub fn init() Self {
            return .{ .count = 0 };
        }

        pub fn next(self: *Self, _: Value) !void {
            self.count += 1;
        }

        pub fn finish(self: Self) !Valid(Encoder(Value, toBytes)) {
            return .{
                .byte_len = self.count * @sizeOf(Value),
                .encoding = Encoding.Direct,
                .encoder = Encoder.init(self.count),
            };
        }
    };
}

pub fn Encoder(
    comptime V: type,
    comptime toBytes: fn (V) [@sizeOf(V)]u8,
) type {
    return struct {
        const Self = @This();

        count: usize,

        pub fn init(count: usize) Self {
            return .{ .count = count };
        }

        pub fn encodeAll(_: *Self, _: anytype, _: anytype) !void {
            @panic("todo");
        }
    };
}
