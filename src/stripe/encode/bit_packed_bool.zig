const std = @import("std");
const mem = std.mem;

const Encoding = @import("../encoding.zig").Encoding;
const Valid = @import("../validator.zig").Valid;

const bit_packed_bool = @This();

// Must be a multiple of 8 to be easily translated to/from octets
const Word = u64;
comptime {
    if (@bitSizeOf(Word) == 0 or @mod(@bitSizeOf(Word), 8) != 0) {
        @compileError("word type bit width must be a multiple of 8 and non-zero");
    }
}

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

    count: u32,

    pub const Encoder = bit_packed_bool.Encoder;

    pub fn init() Self {
        return .{ .count = 0 };
    }

    pub fn unused(self: Self) bool {
        return self.count == 0;
    }

    pub fn next(self: *Self, _: bool) void {
        self.count += 1;
    }

    pub fn end(self: Self) !Valid(Self.Encoder) {
        const byte_len =
            ((self.count + @bitSizeOf(Word) - 1) / @bitSizeOf(Word)) * @sizeOf(Word);
        return .{
            .meta = .{
                .byte_len = byte_len,
                .encoding = Encoding.BitPacked,
            },
            .encoder = Self.Encoder.init(),
        };
    }
};

pub const Encoder = struct {
    const Self = @This();

    const BitIndexInt = u6;

    word: Word,
    bit_index: BitIndexInt,
    word_index: usize,

    const Value = bool;

    fn init() Self {
        return .{ .word = 0, .bit_index = 0, .word_index = 0 };
    }

    pub fn deinit(_: *Self) void {}

    pub fn begin(_: *Self, _: anytype) !bool {
        // TODO write the word bit width?
        return true;
    }

    pub fn encode(self: *Self, blob: anytype, value: Value) !void {
        if (value) {
            self.word |= @as(Word, 1) << self.bit_index;
        }
        self.bit_index += 1;
        if (self.bit_index >= @bitSizeOf(Word)) {
            var buf: [@sizeOf(Word)]u8 = undefined;
            mem.writeIntLittle(Word, &buf, self.word);
            try blob.writeAt(&buf, self.word_index * @sizeOf(Word));
            self.word = 0;
            self.bit_index = 0;
            self.word_index += 1;
        }
    }

    pub fn end(self: *Self, blob: anytype) !void {
        if (self.bit_index > 0) {
            var buf: [@sizeOf(Word)]u8 = undefined;
            mem.writeIntLittle(Word, &buf, self.word);
            try blob.writeAt(&buf, self.word_index * @sizeOf(Word));
        }
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
