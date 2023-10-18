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

        index: usize,

        pub fn init() Self {
            return .{ .index = 0 };
        }

        pub fn begin(_: *Self, _: anytype) !void {}

        pub fn decode(self: *Self, blob: anytype) !Value {
            var buf: [@sizeOf(Value)]u8 = undefined;
            try blob.readAt(buf[0..], self.index * @sizeOf(Value));
            self.index += 1;
            return fromBytes(&buf);
        }

        pub fn decodeAll(self: *Self, blob: anytype, dst: []Value) !void {
            // TODO this could be made more efficient
            for (dst) |*cell| {
                cell.* = try self.decode(blob);
            }
        }

        pub fn skip(self: *Self, n: u32) void {
            self.index += n;
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

test "decoder" {
    const MemoryBlob = @import("../../MemoryBlob.zig");

    const allocator = std.testing.allocator;
    const buf = try allocator.alloc(u8, 12);
    defer allocator.free(buf);

    const expected_values = [_]u32{
        7,
        67,
        2007,
    };
    for (expected_values, 0..) |v, idx| {
        var slice = @as(*[4]u8, @ptrCast(buf[(idx * 4)..(idx * 4 + 4)].ptr));
        mem.writeIntLittle(u32, slice, v);
    }

    var blob = MemoryBlob{ .data = buf };
    var decoder = Decoder(u32, readU32).init();
    try decoder.begin(blob);

    for (expected_values) |v| {
        var value = try decoder.decode(blob);
        try std.testing.expectEqual(v, value);
    }

    decoder = Decoder(u32, readU32).init();
    try decoder.begin(blob);
    var dst: [3]u32 = undefined;
    try decoder.decodeAll(blob, &dst);
}

fn readU32(buf: *const [4]u8) u32 {
    return mem.readIntLittle(u32, buf);
}

fn writeU32(value: u32) [4]u8 {
    var buf: [4]u8 = undefined;
    mem.writeIntLittle(u32, &buf, value);
    return buf;
}
