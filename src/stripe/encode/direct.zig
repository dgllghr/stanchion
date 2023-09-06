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

        pub fn decode(_: *Self, blob: anytype, index: usize) !Value {
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

        count: u32,

        pub const Encoder = direct.Encoder(Value, toBytes);

        pub fn init() Self {
            return .{ .count = 0 };
        }

        pub fn unused(self: Self) bool {
            return self.count == 0;
        }

        pub fn next(self: *Self, _: Value) void {
            self.count += 1;
        }

        pub fn end(self: Self) !Valid(Self.Encoder) {
            return .{
                .meta = .{
                    .byte_len = self.count * @sizeOf(Value),
                    .encoding = Encoding.Direct,
                },
                .encoder = Self.Encoder.init(),
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

        const Value = V;

        pub fn init() Self {
            return .{ .count = 0 };
        }

        pub fn deinit(_: *Self) void {}

        pub fn begin(_: *Self, _: anytype) !bool {
            return true;
        }

        pub fn encode(self: *Self, blob: anytype, value: Value) !void {
            const buf = toBytes(value);
            try blob.writeAt(buf[0..], self.count * @sizeOf(V));
            self.count += 1;
        }

        pub fn end(_: *Self, _: anytype) !void {}
    };
}
