const std = @import("std");
const io = std.io;

const c = @import("c.zig").c;
const errors = @import("errors.zig");
const Conn = @import("Conn.zig");

const Self = @This();

blob: ?*c.sqlite3_blob,

pub fn open(
    conn: Conn,
    table_name: [:0]const u8,
    column_name: [:0]const u8,
    rowid: i64,
) !Self {
    var blob: Self = undefined;
    const res = c.sqlite3_blob_open(
        conn.conn,
        "main",
        table_name,
        column_name,
        @intCast(rowid),
        1, // read/write
        &blob.blob,
    );
    if (res != c.SQLITE_OK) {
        return errors.errorFromResultCode(res);
    }
    return blob;
}

pub fn close(self: *Self) !void {
    const res = c.sqlite3_blob_close(self.blob);
    if (res != c.SQLITE_OK) {
        return errors.errorFromResultCode(res);
    }
}

pub fn len(self: Self) u32 {
    return @intCast(c.sqlite3_blob_bytes(self.blob));
}

pub fn readAt(self: Self, buf: []u8, start: usize) !void {
    const res = c.sqlite3_blob_read(self.blob, buf.ptr, @intCast(buf.len), @intCast(start));
    if (res != c.SQLITE_OK) {
        return errors.errorFromResultCode(res);
    }
}

pub fn writeAt(self: Self, buf: []const u8, start: usize) !void {
    const res = c.sqlite3_blob_write(self.blob, buf.ptr, @intCast(buf.len), @intCast(start));
    if (res != c.SQLITE_OK) {
        return errors.errorFromResultCode(res);
    }
}

pub fn sliceFrom(self: Self, from: u32) BlobSlice(Self) {
    return .{ .blob = self, .from = from };
}

pub fn BlobSlice(comptime Blob: type) type {
    return struct {
        blob: Blob,
        from: u32,

        pub fn len(self: @This()) u32 {
            return self.blob.len() - self.from;
        }

        pub fn readAt(self: @This(), buf: []u8, start: usize) !void {
            try self.blob.readAt(buf, start + self.from);
        }

        pub fn writeAt(self: @This(), buf: []const u8, start: usize) !void {
            try self.blob.writeAt(buf, start + self.from);
        }

        pub fn sliceFrom(self: @This(), from: u32) BlobSlice(Blob) {
            return .{ .blob = self.blob, .from = self.from + from };
        }

        pub fn writer(self: @This()) BlobWriter(@This()) {
            return BlobWriter(@This()).init(self);
        }
    };
}

pub fn writer(self: Self) BlobWriter(Self) {
    return BlobWriter(Self).init(self);
}

pub fn BlobWriter(comptime Blob: type) type {
    return struct {
        blob: Blob,
        offset: usize,

        pub fn init(blob: Blob) @This() {
            return .{ .blob = blob, .offset = 0 };
        }

        pub fn write(self: *@This(), data: []const u8) errors.Error!usize {
            try self.blob.writeAt(data, self.offset);
            self.offset += data.len;
            return data.len;
        }

        pub const Writer = io.Writer(*@This(), errors.Error, write);

        pub fn writer(self: *@This()) Writer {
            return .{ .context = self };
        }
    };
}
