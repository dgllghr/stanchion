const std = @import("std");
const mem = std.mem;

const Encoding = @import("../encode.zig").Encoding;

const Self = @This();

encoding: Encoding,
/// Byte length of the message log messages
values_len: u32,
/// Byte length of the message log messages
message_log_messages_len: u32,
/// Byte length of the message log message index
message_log_index_len: u32,

pub const encoded_len = 14;

pub fn read(blob: anytype) !Self {
    var buf: [encoded_len]u8 = undefined;
    try blob.readAt(buf[0..], 0);
    // Assert that the version is 1
    std.debug.assert(buf[0] == 1);
    const encoding: Encoding = @enumFromInt(buf[1]);
    const values_len = mem.readIntLittle(u32, buf[2..6]);
    const message_log_messages_len = mem.readIntLittle(u32, buf[6..10]);
    const message_log_index_len = std.mem.readIntLittle(u32, buf[10..14]);
    return .{
        .encoding = encoding,
        .values_len = values_len,
        .message_log_messages_len = message_log_messages_len,
        .message_log_index_len = message_log_index_len,
    };
}

pub fn write(self: Self, blob: anytype) !void {
    var buf: [encoded_len]u8 = undefined;
    buf[0] = 1;
    buf[1] = @intFromEnum(self.encoding);
    std.mem.writeIntLittle(u32, buf[2..6], self.values_len);
    std.mem.writeIntLittle(u32, buf[6..10], self.message_log_messages_len);
    std.mem.writeIntLittle(u32, buf[10..14], self.message_log_index_len);
    try blob.writeAt(buf[0..], 0);
}