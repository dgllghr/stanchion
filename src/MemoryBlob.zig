const std = @import("std");
const mem = std.mem;

const c = @import("./sqlite3/c.zig").c;
const BlobSlice = @import("./sqlite3/Blob.zig").BlobSlice;

const Self = @This();

data: []u8,

pub fn len(self: Self) u32 {
    return @intCast(self.data.len);
}

pub fn readAt(self: Self, buf: []u8, start: usize) !void {
    const end = start + buf.len;
    mem.copy(u8, buf, self.data[start..end]);
}

pub fn writeAt(self: *Self, buf: []const u8, start: usize) !void {
    mem.copy(u8, self.data[start..], buf);
}

pub fn sliceFrom(self: *Self, from: u32) BlobSlice(Self) {
    return .{ .blob = self, .from = from };
}