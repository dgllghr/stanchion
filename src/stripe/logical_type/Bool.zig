const Encoding = @import("../encoding.zig").Encoding;
const Error = @import("../error.zig").Error;
const Optimizer = @import("../optimizer.zig").Optimizer;

const bit_packed_bool = @import("../encode/bit_packed_bool.zig");
const constant = @import("../encode/constant.zig");

const Bool = @This();

const Tag = enum {
    bit_packed,
    constant,
};

pub const Decoder = union(Tag) {
    const Self = @This();

    bit_packed: bit_packed_bool.Decoder,
    constant: constant.Decoder(bool, readDirect),

    pub fn init(encoding: Encoding) !Self {
        return switch (encoding) {
            .Constant => .{
                .constant = constant.Decoder(bool, readDirect).init(),
            },
            .BitPacked => .{
                .bit_packed = bit_packed_bool.Decoder.init(),
            },
            else => return Error.InvalidEncoding,
        };
    }

    pub fn begin(self: *Self, blob: anytype) !void {
        switch (self.*) {
            inline else => |*d| try d.begin(blob),
        }
    }

    pub fn decode(self: *Self, blob: anytype) !bool {
        switch (self.*) {
            inline else => |*d| return d.decode(blob),
        }
    }

    pub fn decodeAll(self: *Self, blob: anytype, dst: []bool) !void {
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
    bit_packed: bit_packed_bool.Validator,
    constant: constant.Validator(bool, writeDirect),
}, Encoder);

pub const Encoder = union(Tag) {
    const Self = @This();

    bit_packed: bit_packed_bool.Encoder,
    constant: constant.Encoder(bool, writeDirect),

    pub const Value = bool;

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

pub fn readDirect(v: *const [1]u8) bool {
    if (v[0] > 0) {
        return true;
    }
    return false;
}

pub fn writeDirect(v: bool) [1]u8 {
    if (v) {
        return [1]u8{1};
    }
    return [1]u8{0};
}
