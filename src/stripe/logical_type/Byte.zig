const Encoding = @import("../encoding.zig").Encoding;
const Error = @import("../error.zig").Error;
const Optimizer = @import("../optimizer.zig").Optimizer;

const direct = @import("../encode/direct.zig");
const constant = @import("../encode/constant.zig");

const Byte = @This();

const Tag = enum {
    direct,
    constant,
};

pub const Decoder = union(Tag) {
    const Self = @This();

    direct: direct.Decoder,
    constant: constant.Decoder(u8, readDirect),

    pub fn init(encoding: Encoding, blob: anytype) !Self {
        return switch (encoding) {
            .Direct => .{
                .bit_packed = try constant.Decoder.init(blob),
            },
            .Constant => .{
                .constant = try constant.Decoder(bool, readDirect).init(blob),
            },
            else => return Error.InvalidEncoding,
        };
    }

    pub fn decode(self: *Self, blob: anytype, index: usize) !u8 {
        switch (self.*) {
            inline else => |*d| return d.decode(blob, index),
        }
    }
};

pub const Validator = Optimizer(struct {
    direct: direct.Validator(u8, writeDirect),
    constant: constant.Validator(u8, writeDirect),
}, Encoder);

pub const Encoder = union(Tag) {
    const Self = @This();

    direct: direct.Encoder(u8, writeDirect),
    constant: constant.Encoder(u8, writeDirect),

    pub const Value = u8;

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

pub fn readDirect(v: *const [1]u8) u8 {
    return v[0];
}

pub fn writeDirect(v: u8) [1]u8 {
    return [1]u8{v};
}
