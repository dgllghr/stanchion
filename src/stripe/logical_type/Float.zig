const std = @import("std");

const Encoding = @import("../encoding.zig").Encoding;
const Error = @import("../error.zig").Error;
const Optimizer = @import("../optimizer.zig").Optimizer;

const direct = @import("../encode/direct.zig");
const constant = @import("../encode/constant.zig");

const Float = @This();

const Tag = enum {
    direct,
    /// Constant encoding for floats will only be selected if all floats have the same
    /// binary representation. This is simple and prioritizes accuracy over encoding size
    constant,
};

pub const Validator = Optimizer(struct {
    direct: direct.Validator(f64, writeDirect),
    constant: constant.Validator(f64, writeDirect),
}, Encoder);

pub const Encoder = union(Tag) {
    const Self = @This();

    direct: direct.Encoder(f64, writeDirect),
    constant: constant.Encoder(f64, writeDirect),

    pub const Value = f64;

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

    direct: direct.Decoder(f64, readDirect),
    constant: constant.Decoder(f64, readDirect),

    pub fn init(encoding: Encoding) !Self {
        return switch (encoding) {
            .Direct => .{
                .direct = direct.Decoder(f64, readDirect).init(),
            },
            .Constant => .{
                .constant = constant.Decoder(f64, readDirect).init(),
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

    pub fn read(self: *Self, blob: anytype) !f64 {
        switch (self.*) {
            inline else => |*d| return d.read(blob),
        }
    }

    pub fn readAll(self: *Self, dst: []u8, blob: anytype) !void {
        switch (self.*) {
            inline else => |*d| try d.readAll(dst, blob),
        }
    }
};

pub fn readDirect(v: *const [8]u8) f64 {
    const int_value = std.mem.readInt(u64, v, .little);
    return @bitCast(int_value);
}

pub fn writeDirect(v: f64) [8]u8 {
    const int_value: u64 = @bitCast(v);
    var buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &buf, int_value, .little);
    return buf;
}
