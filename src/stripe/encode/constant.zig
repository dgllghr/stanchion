const std = @import("std");
const mem = std.mem;

const Encoding = @import("../encoding.zig").Encoding;
const Error = @import("../error.zig").Error;
const Valid = @import("../validator.zig").Valid;

const constant = @This();

pub fn Validator(
    comptime Value: type,
    comptime toBytes: fn (Value) [@sizeOf(Value)]u8,
) type {
    return struct {
        const Self = @This();

        state: State,

        pub const Encoder = constant.Encoder(Value, toBytes);

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

        pub fn end(self: Self) !Valid(Self.Encoder) {
            switch (self.state) {
                .valid => |value| {
                    const encoder = Self.Encoder.init(value);
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

        pub fn init(value: Value) Self {
            return .{ .value = value };
        }

        pub fn deinit(_: *Self) void {}

        pub fn begin(self: *Self, blob: anytype) !bool {
            const buf = toBytes(self.value);
            try blob.writeAt(buf[0..], 0);
            return false;
        }

        pub fn write(_: *Self, _: anytype, _: Value) !void {}

        pub fn end(_: *Self, _: anytype) !void {}
    };
}

pub fn Decoder(
    comptime Value: type,
    comptime fromBytes: fn (*const [@sizeOf(Value)]u8) Value,
) type {
    return struct {
        const Self = @This();

        value: Value,

        pub fn init() Self {
            return .{ .value = undefined };
        }

        pub fn begin(self: *Self, blob: anytype) !void {
            var buf: [@sizeOf(Value)]u8 = undefined;
            try blob.readAt(buf[0..], 0);
            self.value = fromBytes(&buf);
        }

        pub fn next(_: *Self, _: u32) void {}

        pub fn read(self: *Self, _: anytype) !Value {
            return self.value;
        }

        pub fn readAll(self: *Self, dst: []Value, _: anytype) !void {
            for (dst) |*cell| {
                cell.* = self.value;
            }
        }
    };
}

test "decoder" {
    const MemoryBlob = @import("../../MemoryBlob.zig");

    const allocator = std.testing.allocator;
    const buf = try allocator.alloc(u8, 4);
    defer allocator.free(buf);

    const expected_value: u32 = 29;
    mem.writeInt(u32, buf[0..4], expected_value, .little);

    const blob = MemoryBlob{ .data = buf };
    var decoder = Decoder(u32, readU32).init();
    try decoder.begin(blob);

    var value = try decoder.read(blob);
    try std.testing.expectEqual(@as(u32, 29), value);
    decoder.next(1);
    value = try decoder.read(blob);
    try std.testing.expectEqual(@as(u32, 29), value);
}

fn readU32(buf: *const [4]u8) u32 {
    return mem.readInt(u32, buf, .little);
}

fn writeU32(value: u32) [4]u8 {
    var buf: [4]u8 = undefined;
    mem.writeInt(u32, &buf, value, .little);
    return buf;
}
