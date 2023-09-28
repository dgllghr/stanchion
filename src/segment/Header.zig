const std = @import("std");
const mem = std.mem;

const stripe = @import("../stripe.zig");
const Encoding = stripe.Encoding;
const StripeMeta = stripe.Meta;

const Self = @This();

present_stripe: StripeMeta,
length_stripe: StripeMeta,
primary_stripe: StripeMeta,

pub const encoded_len: usize = @divExact(@bitSizeOf(Self), 8) + 1;

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
        .byte_len = mem.readIntLittle(u32, buf[0..4]),
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
    mem.writeIntLittle(u32, buf[0..4], meta.byte_len);
    buf[4] = @intFromEnum(meta.encoding);
}

pub fn totalStripesLen(self: Self) u32 {
    return self.present_stripe.byte_len + self.length_stripe.byte_len +
        self.primary_stripe.byte_len;
}
