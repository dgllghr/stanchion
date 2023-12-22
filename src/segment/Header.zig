const std = @import("std");
const mem = std.mem;
const testing = std.testing;

const stripe = @import("../stripe.zig");
const Encoding = stripe.Encoding;
const StripeMeta = stripe.Meta;

present_stripe: StripeMeta,
length_stripe: StripeMeta,
primary_stripe: StripeMeta,

const Self = @This();

pub const encoded_len: u32 = @divExact(@bitSizeOf(Self), 8) + 1;

pub fn init() Self {
    return .{
        .present_stripe = .{
            .byte_len = 0,
            .encoding = undefined,
        },
        .length_stripe = .{
            .byte_len = 0,
            .encoding = undefined,
        },
        .primary_stripe = .{
            .byte_len = 0,
            .encoding = undefined,
        },
    };
}

pub fn read(blob: anytype) !Self {
    var buf: [encoded_len]u8 = undefined;
    try blob.readAt(&buf, 0);
    std.debug.assert(buf[0] == 1);
    return .{
        .present_stripe = readStripeMeta(buf[1..6]),
        .length_stripe = readStripeMeta(buf[6..11]),
        .primary_stripe = readStripeMeta(buf[11..16]),
    };
}

fn readStripeMeta(buf: []const u8) StripeMeta {
    return .{
        .byte_len = mem.readInt(u32, buf[0..4], .little),
        .encoding = @enumFromInt(buf[4]),
    };
}

pub fn write(self: Self, blob: anytype) !void {
    var buf: [encoded_len]u8 = undefined;
    // Set the version to 1
    buf[0] = 1;
    writeStripeMeta(self.present_stripe, buf[1..6]);
    writeStripeMeta(self.length_stripe, buf[6..11]);
    writeStripeMeta(self.primary_stripe, buf[11..16]);
    try blob.writeAt(&buf, 0);
}

fn writeStripeMeta(meta: StripeMeta, buf: []u8) void {
    mem.writeInt(u32, buf[0..4], meta.byte_len, .little);
    buf[4] = @intFromEnum(meta.encoding);
}

pub fn totalStripesLen(self: Self) u32 {
    return self.present_stripe.byte_len + self.length_stripe.byte_len +
        self.primary_stripe.byte_len;
}

pub fn totalSegmentLen(self: Self) u32 {
    return self.totalStripesLen() + encoded_len;
}

test "segment: round trip header encode and decode" {
    const MemoryBlob = @import("../MemoryBlob.zig");

    const header = Self{
        .present_stripe = .{
            .byte_len = 124_235_123,
            .encoding = .Direct,
        },
        .length_stripe = .{
            .byte_len = 0,
            .encoding = @enumFromInt(0),
        },
        .primary_stripe = .{
            .byte_len = 1,
            .encoding = .Constant,
        },
    };
    const data = try testing.allocator.alloc(u8, encoded_len);
    defer testing.allocator.free(data);
    var blob = MemoryBlob{
        .data = data,
    };

    try header.write(&blob);
    const header_out = try Self.read(&blob);

    try testing.expectEqual(header, header_out);
}
