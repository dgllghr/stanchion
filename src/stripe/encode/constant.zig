const std = @import("std");
const mem = std.mem;

const Encoding = @import("../encoding.zig").Encoding;
const Error = @import("../error.zig").Error;
const Valid = @import("../validator.zig").Valid;

const constant = @This();

pub fn Decoder(
    comptime Value: type,
    comptime fromBytes: fn (*const [@sizeOf(Value)]u8) Value,
) type {
    return struct {
        const Self = @This();

        value: Value,

        pub fn init(blob: anytype) !Self {
            var buf: [@sizeOf(Value)]u8 = undefined;
            try blob.readAt(buf[0..], 0);
            return .{ .value = fromBytes(&buf) };
        }

        pub fn decode(self: *Self, _: anytype, _: usize) !Value {
            return self.value;
        }
    };
}

pub fn Validator(
    comptime Value: type,
    comptime toBytes: fn (Value) [@sizeOf(Value)]u8,
) type {
    return struct {
        const Self = @This();

        state: State,

        pub const Encoder = constant.Encoder;

        const State = union(enum) {
            empty,
            valid: Value,
            invalid,
        };

        pub fn init() Self {
            return .{
                .state = .empty,
            };
        }

        pub fn unused(self: Self) bool {
            return self.state == .empty;
        }

        pub fn next(self: *Self, value: Value) void {
            switch (self.state) {
                .valid => |curr_value| {
                    if (curr_value != value) {
                        self.state = .invalid;
                    }
                },
                .empty => self.state = .{ .valid = value },
                .invalid => {},
            }
        }

        pub fn end(self: Self) !Valid(Self.Encoder(Value, toBytes)) {
            switch (self.state) {
                .valid => |value| {
                    const encoder = Self.Encoder(Value, toBytes){ .value = value };
                    return .{
                        .meta = .{
                            .byte_len = @sizeOf(Value),
                            .encoding = .Constant,
                        },
                        .encoder = encoder,
                    };
                },
                else => return Error.NotEncodable,
            }
        }
    };
}

pub fn Encoder(
    comptime V: type,
    comptime toBytes: fn (V) [@sizeOf(V)]u8,
) type {
    return struct {
        const Self = @This();

        value: Value,

        const Value = V;

        pub fn init(value: Value) void {
            return .{ .value = value };
        }

        pub fn deinit(_: *Self) void {}

        pub fn begin(self: *Self, blob: anytype) !bool {
            const buf = toBytes(self.value);
            try blob.writeAt(buf[0..], 0);
            return false;
        }

        pub fn encode(_: *Self, _: anytype, _: Value) !void {}

        pub fn end(_: *Self, _: anytype) !void {}
    };
}

fn readU32(buf: *const [4]u8) u32 {
    return mem.readIntLittle(u32, buf);
}

fn writeU32(value: u32) [4]u8 {
    var buf: [4]u8 = undefined;
    mem.writeIntLittle(u32, &buf, value);
    return buf;
}
