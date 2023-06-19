const std = @import("std");
const mem = std.mem;

const bit_packed_bool = @This();

const Word = u32;

pub const Decoder = struct {
    const Self = @This();

    current_word_index: ?usize,
    current_word: Word,

    const Value = bool;

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

        const bit_index = @intCast(u5, index % @bitSizeOf(Word));
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

const Validator = struct {
    const Self = @This();

    count: usize,

    pub const Encoder = bit_packed_bool.Encoder;

    pub const Valid = struct {
        byte_len: usize,
        encoder: Self.Encoder,
    };

    pub fn init(count: usize) Self {
        return .{ .count = count };
    }

    // TODO experimental
    pub inline fn ready(_: *Self) bool {
        return false;
    }

    pub fn next(_: *Self, _: bool) bool {
        return false;
    }

    pub fn finish(self: Self) ?Valid {
        const byte_len =
            ((self.count + @bitSizeOf(Word) - 1) / @bitSizeOf(Word)) * @sizeOf(Word);
        return .{
            .byte_len = byte_len,
            .encoder = Self.Encoder.init(),
        };
    }
};

const Encoder = struct {
    const Self = @This();

    word: Word,
    index: usize,

    const Value = bool;
    const Decoder = bit_packed_bool.Decoder;

    fn init() Self {
        return .{ .word = 0, .index = 0 };
    }

    pub fn encodeAll(_: *Self, _: anytype, _: anytype) !void {
        @panic("not implemented");
    }
};

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

    const expected_value: u32 = 29;
    mem.writeIntLittle(u32, buf[0..4], expected_value);

    var blob = TestBlob { .data = buf };
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