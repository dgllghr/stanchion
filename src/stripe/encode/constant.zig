const std = @import("std");
const mem = std.mem;

const Encoding = @import("../encoding.zig").Encoding;
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

        value: ?Value,

        pub fn init() Self {
            return .{ .value = null };
        }

        pub fn next(self: *Self, value: Value) bool {
            if (self.value) |curr_value| {
                if (curr_value != value) {
                    self.value = null;
                    return false;
                }
            } else {
                self.value = value;
            }
            return true;
        }

        pub fn finish(self: Self) ?Valid(Encoder(Value, toBytes)) {
            if (self.value) |value| {
                const encoder = Encoder(Value, toBytes){ .value = value };
                return .{
                    .byte_len = @sizeOf(Value),
                    .encoding = Self.encoding,
                    .encoder = encoder,
                };
            }
            return null;
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
        const Decoder = constant.Decoder;

        pub fn encodeAll(self: *Self, dst: anytype, _: anytype) !void {
            const buf = toBytes(self.value);
            try dst.writeAt(buf[0..], 0);
        }
    };
}

// TODO move to blob file
const TestBlob = struct {
    data: []u8,

    pub fn readAt(self: @This(), buf: []u8, start: usize) !void {
        const end = start + buf.len;
        mem.copy(u8, buf, self.data[start..end]);
    }

    pub fn writeAt(self: @This(), buf: []const u8, start: usize) !void {
        mem.copy(u8, self.data[start..], buf);
    }
};

test "decoder" {
    const allocator = std.testing.allocator;
    const buf = try allocator.alloc(u8, 10);
    defer allocator.free(buf);

    const expected_value: u32 = 17;
    mem.writeIntLittle(u32, buf[0..4], expected_value);

    var blob = TestBlob{ .data = buf };
    var decoder = try Decoder(u32, readU32).init(blob);
    const value = try decoder.decode(blob, 0);

    try std.testing.expectEqual(expected_value, value);
}

test "encoder" {
    const allocator = std.testing.allocator;
    const buf = try allocator.alloc(u8, 10);
    defer allocator.free(buf);

    const expected_value: u32 = 17;
    var encoder = Encoder(u32, writeU32){ .value = expected_value };

    var blob = TestBlob{ .data = buf };
    try encoder.encodeAll(blob, .{});

    const value = readU32(blob.data[0..4]);
    try std.testing.expectEqual(expected_value, value);
}

test "validator" {
    var validator = Validator(u32, writeU32){ .value = null };
    var cont = false;
    cont = validator.next(17);
    try std.testing.expectEqual(true, cont);
    cont = validator.next(17);
    try std.testing.expectEqual(true, cont);
    cont = validator.next(18);
    try std.testing.expectEqual(false, cont);

    validator = Validator(u32, writeU32){ .value = null };
    cont = validator.next(17);
    try std.testing.expectEqual(true, cont);
    const valid = validator.finish();
    try std.testing.expectEqual(@as(usize, 4), valid.?.byte_len);
}

fn readU32(buf: *const [4]u8) u32 {
    return mem.readIntLittle(u32, buf);
}

fn writeU32(value: u32) [4]u8 {
    var buf: [4]u8 = undefined;
    mem.writeIntLittle(u32, &buf, value);
    return buf;
}
