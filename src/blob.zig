const std = @import("std");
const mem = std.mem;

pub const MemoryBlob = struct {
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
};

pub fn BlobSlice(comptime Blob: type) type {
    return struct {
        const Self = @This();

        blob: *Blob,
        from: u32,

        pub fn len(self: Self) u32 {
            return self.blob.len() - self.from;
        }

        pub fn readAt(self: Self, buf: []u8, start: usize) !void {
            try self.blob.readAt(buf, start + self.from);
        }

        pub fn writeAt(self: *Self, buf: []const u8, start: usize) !void {
            try self.blob.writeAt(buf, start + self.from);
        }

        pub fn sliceFrom(self: *Self, from: u32) BlobSlice(Blob) {
            return .{ .blob = self.blob, .from = self.from + from };
        }
    };
}