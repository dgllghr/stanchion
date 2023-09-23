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

    pub fn decode(self: *Self, blob: anytype) !i64 {
        switch (self.*) {
            inline else => |*d| return d.decode(blob),
        }
    }

    pub fn decodeAll(self: *Self, blob: anytype, dst: []i64) !void {
        switch (self.*) {
            inline else => |*d| try d.decodeAll(blob, dst),
        }
    }

    pub fn skip(self: *Self, n: u32) void {
        switch (self.*) {
            inline else => |*d| d.skip(n),
        }
    }
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
            inline else => |e| e.deinit(),
        }
    }

    pub fn begin(self: *Self, blob: anytype) !bool {
        switch (self.*) {
            inline else => |*e| return e.begin(blob),
        }
    }

    pub fn encode(self: *Self, blob: anytype, value: Value) !void {
        switch (self.*) {
            inline else => |*e| try e.encode(blob, value),
        }
    }

    pub fn end(self: *Self, blob: anytype) !void {
        switch (self.*) {
            inline else => |*e| try e.end(blob),
        }
    }
};

pub fn readDirect(v: *const [8]u8) i64 {
    return std.mem.readIntLittle(i64, v);
}

pub fn writeDirect(v: i64) [8]u8 {
    var buf: [8]u8 = undefined;
    std.mem.writeIntLittle(i64, &buf, v);
    return buf;
}
