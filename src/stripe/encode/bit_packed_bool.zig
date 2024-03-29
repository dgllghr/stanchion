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

pub const Validator = struct {
    count: u32,

    const Self = @This();
    pub const Encoder = bit_packed_bool.Encoder;

    pub fn init() Self {
        return .{ .count = 0 };
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
    word: Word,
    bit_index: BitIndexInt,

    const Self = @This();
    const BitIndexInt = u6;
    const Value = bool;

    fn init() Self {
        return .{ .word = 0, .bit_index = 0 };
    }

    pub fn deinit(_: *Self) void {}

    pub fn begin(_: *Self, _: anytype) !bool {
        // TODO write the word bit width?
        return true;
    }

    pub fn write(self: *Self, writer: anytype, value: Value) !void {
        if (value) {
            self.word |= @as(Word, 1) << self.bit_index;
        }
        self.bit_index += 1;
        if (self.bit_index >= @bitSizeOf(Word)) {
            try writer.writeInt(Word, self.word, .little);
            self.word = 0;
            self.bit_index = 0;
        }
    }

    pub fn end(self: *Self, writer: anytype) !void {
        if (self.bit_index > 0) {
            try writer.writeInt(Word, self.word, .little);
        }
    }
};

pub const Decoder = struct {
    index: usize,
    current_word: ?Word,

    const Self = @This();

    pub fn init() Self {
        return .{
            .index = 0,
            .current_word = null,
        };
    }

    pub fn begin(_: *Self, _: anytype) !void {
        // TODO read the word bit width?
    }

    pub fn next(self: *Self, n: u32) void {
        const prev_word_index = self.index / @bitSizeOf(Word);
        self.index += n;
        if (self.index / @bitSizeOf(Word) != prev_word_index) {
            // Invalidate the current word
            self.current_word = null;
        }
    }

    pub fn read(self: *Self, blob: anytype) !bool {
        if (self.current_word == null) {
            const word_index = self.index / @bitSizeOf(Word);
            try self.loadWord(blob, word_index);
        }

        const bit_index: u5 = @intCast(self.index % @bitSizeOf(Word));
        return (self.current_word.? >> bit_index) & 1 > 0;
    }

    fn loadWord(self: *Self, blob: anytype, word_index: usize) !void {
        var buf: [@sizeOf(Word)]u8 = undefined;
        const byte_index = word_index * @sizeOf(Word);
        try blob.readAt(buf[0..], byte_index);
        self.current_word = mem.readInt(Word, &buf, .little);
    }
};

test "decoder" {
    const MemoryBlob = @import("../../MemoryBlob.zig");

    const allocator = std.testing.allocator;
    const buf = try allocator.alloc(u8, 10);
    defer allocator.free(buf);

    const expected_value: u32 = 29;
    mem.writeInt(u32, buf[0..4], expected_value, .little);

    const blob = MemoryBlob{ .data = buf };
    var decoder = Decoder.init();
    try decoder.begin(blob);

    var value = try decoder.read(blob);
    try std.testing.expectEqual(true, value);
    decoder.next(1);
    value = try decoder.read(blob);
    try std.testing.expectEqual(false, value);
    decoder.next(1);
    value = try decoder.read(blob);
    try std.testing.expectEqual(true, value);
    decoder.next(1);
    value = try decoder.read(blob);
    try std.testing.expectEqual(true, value);
    decoder.next(1);
    value = try decoder.read(blob);
    try std.testing.expectEqual(true, value);
    decoder.next(1);
    value = try decoder.read(blob);
    try std.testing.expectEqual(false, value);
    decoder.next(1);
    value = try decoder.read(blob);
    try std.testing.expectEqual(false, value);
    decoder.next(1);
    value = try decoder.read(blob);
    try std.testing.expectEqual(false, value);
}
