const std = @import("std");
const mem = std.mem;

const Encoding = @import("../encoding.zig").Encoding;
const Valid = @import("../validator.zig").Valid;

const bit_packed_bool = @This();

const Word = u32;

pub const Decoder = struct {
    const Self = @This();

    current_word_index: ?usize,
    current_word: Word,

    pub fn init(_: anytype) !Self {
        return .{
            .current_word_index = null,
            .current_word = 0,
        };
    }

    pub fn decode(self: *Self, blob: anytype, index: usize) !bool {
        const word_index = index / @bitSizeOf(Word);

        if (self.current_word_index == null or self.current_word_index.? != word_index) {
            try self.loadWord(blob, word_index);
        }

        const bit_index: u5 = @intCast(index % @bitSizeOf(Word));
        return (self.current_word >> bit_index) & 1 > 0;
    }

    fn loadWord(self: *Self, blob: anytype, word_index: usize) !void {
        var buf: [@sizeOf(Word)]u8 = undefined;
        const byte_index = word_index * @sizeOf(Word);
        try blob.readAt(buf[0..], byte_index);
        self.current_word = mem.readIntLittle(Word, &buf);
        self.current_word_index = word_index;
    }
};

pub const Validator = struct {
    const Self = @This();

    count: usize,

    pub fn init() Self {
        return .{ .count = 0 };
    }

    pub fn next(self: *Self, _: bool) !void {
        self.count += 1;
    }

    pub fn finish(self: Self) !Valid(Encoder) {
        const byte_len =
            ((self.count + @bitSizeOf(Word) - 1) / @bitSizeOf(Word)) * @sizeOf(Word);
        return .{
            .byte_len = byte_len,
            .encoding = Encoding.BitPacked,
            .encoder = Encoder.init(),
        };
    }
};

pub const Encoder = struct {
    const Self = @This();

    word: Word,
    index: usize,

    const Value = bool;
    const Decoder = bit_packed_bool.Decoder;
    const Validator = bit_packed_bool.Validator;

    fn init() Self {
        return .{ .word = 0, .index = 0 };
    }

    pub fn encodeAll(_: *Self, _: anytype, _: anytype) !void {
        @panic("not implemented");
    }
};

const MemoryBlob = @import("../../MemoryBlob.zig");

test "decoder" {
    const allocator = std.testing.allocator;
    const buf = try allocator.alloc(u8, 10);
    defer allocator.free(buf);

    const expected_value: u32 = 29;
    mem.writeIntLittle(u32, buf[0..4], expected_value);

    var blob = MemoryBlob{ .data = buf };
    var decoder = try Decoder.init(blob);

    var value = try decoder.decode(blob, 0);
    try std.testing.expectEqual(true, value);
    value = try decoder.decode(blob, 1);
    try std.testing.expectEqual(false, value);
    value = try decoder.decode(blob, 2);
    try std.testing.expectEqual(true, value);
    value = try decoder.decode(blob, 3);
    try std.testing.expectEqual(true, value);
    value = try decoder.decode(blob, 4);
    try std.testing.expectEqual(true, value);
    value = try decoder.decode(blob, 5);
    try std.testing.expectEqual(false, value);
    value = try decoder.decode(blob, 6);
    try std.testing.expectEqual(false, value);
    value = try decoder.decode(blob, 7);
    try std.testing.expectEqual(false, value);
}
