const Encoding = @import("../encoding.zig").Encoding;
const Error = @import("../error.zig").Error;
const Optimizer = @import("../optimizer.zig").Optimizer;

const direct = @import("../encode/direct.zig");
const constant = @import("../encode/constant.zig");

const Byte = @This();

const Tag = enum {
    constant,
    direct,
};

pub const Validator = Optimizer(struct {
    constant: constant.Validator(u8, writeDirect),
    direct: direct.Validator(u8, writeDirect),
}, Encoder);

pub const Encoder = union(Tag) {
    const Self = @This();

    constant: constant.Encoder(u8, writeDirect),
    direct: direct.Encoder(u8, writeDirect),

    pub const Value = u8;

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

    constant: constant.Decoder(u8, readDirect),
    direct: direct.Decoder(u8, readDirect),

    pub fn init(encoding: Encoding) !Self {
        return switch (encoding) {
            .Direct => .{
                .direct = direct.Decoder(u8, readDirect).init(),
            },
            .Constant => .{
                .constant = constant.Decoder(u8, readDirect).init(),
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

    pub fn read(self: *Self, blob: anytype) !u8 {
        switch (self.*) {
            inline else => |*d| return d.decode(blob),
        }
    }

    pub fn readAll(self: *Self, dst: []u8, blob: anytype) !void {
        switch (self.*) {
            inline else => |*d| try d.readAll(dst, blob),
        }
    }
};

pub fn readDirect(v: *const [1]u8) u8 {
    return v[0];
}

pub fn writeDirect(v: u8) [1]u8 {
    return [1]u8{v};
}
