//! Packs zig-zag encoded integers from least significant bit to most in 64 bit, unsigned integers.
//! Integers are encoded as little endian bytes.

const std = @import("std");
const debug = std.debug;
const mem = std.mem;
const testing = std.testing;

const Encoding = @import("../encoding.zig").Encoding;
const Error = @import("../error.zig").Error;
const Valid = @import("../validator.zig").Valid;

const bit_packed_int = @This();

pub const Validator = struct {
    bit_width: u8,
    count: u32,

    const Self = @This();
    pub const Encoder = bit_packed_int.Encoder;

    pub fn init() Self {
        return .{
            .bit_width = 0,
            .count = 0,
        };
    }

    pub fn next(self: *Self, value: i64) void {
        // Always zig-zag encode tha value
        // TODO is it faster to skip zig zag if all inputs are >= 0?
        // TODO is it faster to calculate zig zag bit width without doing the full encoding?
        const value_zz = zig_zag.encode(value);
        const sig_bits: u8 = 64 - @clz(value_zz);
        if (sig_bits > self.bit_width) {
            self.bit_width = sig_bits;
        }

        self.count += 1;
    }

    pub fn end(self: Self) !Valid(Self.Encoder) {
        // Do not support 64 bit bit-packing because then there isn't any point to packing
        if (self.bit_width == 64) {
            return Error.NotEncodable;
        }

        // The number of octets written is always a factor of 8 (64 bits) so that reads and writes
        // operate on a 64-bit integer in memory and loads and stores operate on that same integer
        // Add 1 to the length to account for the byte that stores the bit width
        const byte_len = (((self.count * self.bit_width) + 64 - 1) / 64) * 8 + 1;
        return .{
            .meta = .{
                .byte_len = byte_len,
                .encoding = Encoding.BitPacked,
            },
            .encoder = Self.Encoder.init(@intCast(self.bit_width)),
        };
    }
};

pub const Encoder = struct {
    bit_width: u6,

    word_bit_index: u6,
    word: u64,

    const Self = @This();

    fn init(bit_width: u6) Self {
        return .{ .bit_width = bit_width, .word_bit_index = 0, .word = 0 };
    }

    pub fn deinit(_: *Self) void {}

    pub fn begin(self: *Self, writer: anytype) !bool {
        try writer.writeByte(self.bit_width);
        return true;
    }

    pub fn write(self: *Self, writer: anytype, value: i64) !void {
        const value_zz = zig_zag.encode(value);
        // The int cast is safe because bit index is <= 63 at this point
        self.word |= value_zz << @intCast(self.word_bit_index);
        const bits_used: u8 = @as(u8, 64) - self.word_bit_index;
        self.word_bit_index +%= self.bit_width;

        if (bits_used <= self.bit_width) {
            // Flush the word
            try writer.writeInt(u64, self.word, .little);

            // Some bits might have been truncated. Write them to the next word
            self.word = value_zz >> @intCast(bits_used);
        }
    }

    pub fn end(self: *Self, writer: anytype) !void {
        if (self.word_bit_index > 0) {
            try writer.writeInt(u64, self.word, .little);
        }
    }
};

pub const Decoder = struct {
    bit_width: u6,

    /// Global bit index
    bit_index: usize,
    current_words: ?struct {
        // Keep a window of 2 words because values can span at most two words
        buf: [2]u64,
        word_index: usize,
    },

    const Self = @This();

    pub fn init() Self {
        return .{
            .bit_width = 0,
            .bit_index = 0,
            .current_words = null,
        };
    }

    pub fn begin(self: *Self, blob: anytype) !void {
        var buf: [1]u8 = undefined;
        try blob.readAt(buf[0..], 0);
        debug.assert(buf[0] < 64);
        self.bit_width = @intCast(buf[0]);
    }

    pub fn next(self: *Self, n: u32) void {
        self.bit_index += @as(usize, self.bit_width) * n;

        if (self.current_words != null and
            self.currEndWordIndex() - self.current_words.?.word_index > 1)
        {
            self.current_words = null;
        }
    }

    pub fn read(self: *Self, blob: anytype) !i64 {
        if (self.current_words == null) {
            try self.loadWords(blob);
        }

        const curr_words = self.current_words.?;
        const word_bit_index: u6 = @intCast(self.bit_index % 64);
        if (curr_words.word_index == self.currWordIndex()) {
            // Value starts in the buf[0] word. The value may be spread across two words
            const lower: u64 = (curr_words.buf[0] >> word_bit_index) &
                ((@as(u64, 1) << self.bit_width) - 1);

            const end_bit_index: u8 = @as(u8, word_bit_index) + self.bit_width;
            if (end_bit_index > 64) {
                const upper: u64 = (curr_words.buf[1] &
                    ((@as(u64, 1) << @as(u6, @intCast(end_bit_index - 64))) - 1));
                const full = (upper << @intCast(@as(u8, 64) - word_bit_index)) | lower;
                return zig_zag.decode(full);
            }

            return zig_zag.decode(lower);
        }

        // Value starts in the buf[1] word. The entire value must fit in the buf[1] word
        debug.assert(self.currWordIndex() == curr_words.word_index + 1);
        const value: u64 = (curr_words.buf[1] >> word_bit_index) &
            ((@as(u64, 1) << @intCast(self.bit_width)) - 1);
        return zig_zag.decode(value);
    }

    fn loadWords(self: *Self, blob: anytype) !void {
        var buf: [16]u8 = undefined;
        const word_index = self.currWordIndex();
        // Add 1 to account for the byte that stores the bit width
        const byte_index = word_index * 8 + 1;

        // Account for byte that stores the bit width
        if (byte_index + 8 == blob.len()) {
            // Read the last byte
            try blob.readAt(buf[0..8], byte_index);
            self.current_words = .{
                .buf = [2]u64{
                    mem.readInt(u64, buf[0..8], .little),
                    undefined,
                },
                .word_index = word_index,
            };
            return;
        }

        debug.assert(byte_index + 8 < blob.len());
        try blob.readAt(buf[0..], byte_index);
        self.current_words = .{
            .buf = [2]u64{
                mem.readInt(u64, buf[0..8], .little),
                mem.readInt(u64, buf[8..], .little),
            },
            .word_index = word_index,
        };
    }

    fn currWordIndex(self: *Self) usize {
        return self.bit_index / 64;
    }

    fn currEndWordIndex(self: *Self) usize {
        return (self.bit_index + self.bit_width - 1) / 64;
    }
};

const zig_zag = struct {
    pub fn encode(value: i64) u64 {
        return @bitCast((2 * value) ^ (value >> (8 * 8 - 1)));
    }

    pub fn decode(value: u64) i64 {
        return @as(i64, @bitCast(value >> 1)) ^ (-@as(i64, @bitCast(value & 1)));
    }
};

const neg_10_to_10_encoded_bytes = [_]u8{
    0x33, 0xBE, 0xB6, 0xD2, 0x29, 0x23, 0x00, 0x41,
    0x0C, 0x52, 0xCC, 0x41, 0x49, 0x01, 0x00, 0x00,
};

test "bit packed int: encode" {
    const allocator = testing.allocator;

    var data = try std.ArrayList(u8).initCapacity(allocator, 17);
    defer data.deinit();
    const writer = data.writer();

    var encoder = Encoder.init(5);
    const cont = try encoder.begin(writer);
    try testing.expectEqual(true, cont);
    var v: i64 = -10;
    while (v <= 10) {
        try encoder.write(writer, v);
        v += 1;
    }
    try encoder.end(writer);

    try testing.expectEqual(5, data.items[0]);
    try testing.expectEqualSlices(u8, neg_10_to_10_encoded_bytes[0..], data.items[1..]);
}

test "bit packed int: decode" {
    const MemoryBlob = @import("../../MemoryBlob.zig");

    var blob_data: [neg_10_to_10_encoded_bytes.len + 1]u8 = undefined;
    blob_data[0] = 5;
    @memcpy(blob_data[1..], neg_10_to_10_encoded_bytes[0..]);
    var blob = MemoryBlob{ .data = &blob_data };

    var decoder = Decoder.init();
    try decoder.begin(&blob);
    var expected: i64 = -10;
    while (expected <= 10) {
        const v = try decoder.read(&blob);
        try testing.expectEqual(expected, v);
        decoder.next(1);
        expected += 1;
    }
}

test "bit packed int: round trip" {
    const MemoryBlob = @import("../../MemoryBlob.zig");
    const allocator = testing.allocator;

    const bit_widths = [_]u6{ 1, 2, 5, 7, 8, 30, 32, 63 };

    for (&bit_widths) |bit_width| {
        // TODO this isn't max value but number of encodable numbers (centered at 0)
        const min_value: i64 = @intCast(@max(
            -10_000,
            -@divTrunc(std.math.pow(i128, 2, @intCast(bit_width)), 2),
        ));
        const max_value: i64 = @intCast(@min(
            10_000,
            @divTrunc(std.math.pow(i128, 2, @intCast(bit_width)), 2) - 1,
        ));

        var data = try std.ArrayList(u8).initCapacity(allocator, 17);
        defer data.deinit();
        const writer = data.writer();

        var encoder = Encoder.init(bit_width);
        const cont = try encoder.begin(writer);
        try testing.expectEqual(true, cont);
        var v: i64 = min_value;
        while (v <= max_value) {
            try encoder.write(writer, v);
            v += 1;
        }
        try encoder.end(writer);

        var blob = MemoryBlob{ .data = data.items };

        var decoder = Decoder.init();
        try decoder.begin(&blob);
        var expected: i64 = min_value;
        while (expected <= max_value) {
            const value = try decoder.read(&blob);
            try testing.expectEqual(expected, value);
            decoder.next(1);
            expected += 1;
        }
    }
}
