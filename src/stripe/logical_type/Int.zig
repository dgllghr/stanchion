const std = @import("std");

const Encoding = @import("../encoding.zig").Encoding;
const Error = @import("../error.zig").Error;
const Optimizer = @import("../optimizer.zig").Optimizer;

const constant = @import("../encode/constant.zig");
const direct = @import("../encode/direct.zig");

const Int = @This();

const Tag = enum {
    constant,
    direct,
};

pub const Validator = Optimizer(struct {
    constant: constant.Validator(i64, writeDirect),
    direct: direct.Validator(i64, writeDirect),
}, Encoder);

pub const Encoder = union(Tag) {
    const Self = @This();

    constant: constant.Encoder(i64, writeDirect),
    direct: direct.Encoder(i64, writeDirect),

    pub const Value = i64;

    pub fn deinit(self: *Self) void {
        switch (self) {
            inline else => |*e| e.deinit(),
        }
    }

    pub fn begin(self: *Self, blob: anytype) !bool {
        switch (self.*) {
            inline else => |*e| return e.begin(blob),
        }
    }

    pub fn write(self: *Self, blob: anytype, value: Value) !void {
        switch (self.*) {
            inline else => |*e| try e.write(blob, value),
        }
    }

    pub fn end(self: *Self, blob: anytype) !void {
        switch (self.*) {
            inline else => |*e| try e.end(blob),
        }
    }
};

pub const Decoder = union(Tag) {
    const Self = @This();

    constant: constant.Decoder(i64, readDirect),
    direct: direct.Decoder(i64, readDirect),

    pub fn init(encoding: Encoding) !Self {
        return switch (encoding) {
            .Constant => .{
                .constant = constant.Decoder(i64, readDirect).init(),
            },
            .Direct => .{
                .direct = direct.Decoder(i64, readDirect).init(),
            },
            else => return Error.InvalidEncoding,
        };
    }

    pub fn begin(self: *Self, blob: anytype) !void {
        switch (self.*) {
            inline else => |*d| try d.begin(blob),
        }
    }

    pub fn next(self: *Self, n: u32) void {
        switch (self.*) {
            inline else => |*d| d.next(n),
        }
    }

    pub fn read(self: *Self, blob: anytype) !i64 {
        switch (self.*) {
            inline else => |*d| return d.read(blob),
        }
    }

    pub fn readAll(self: *Self, dst: []i64, blob: anytype) !void {
        switch (self.*) {
            inline else => |*d| try d.readAll(dst, blob),
        }
    }
};

pub fn readDirect(v: *const [8]u8) i64 {
    return std.mem.readInt(i64, v, .little);
}

pub fn writeDirect(v: i64) [8]u8 {
    var buf: [8]u8 = undefined;
    std.mem.writeInt(i64, &buf, v, .little);
    return buf;
}
