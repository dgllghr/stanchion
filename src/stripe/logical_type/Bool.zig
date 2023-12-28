const Encoding = @import("../encoding.zig").Encoding;
const Error = @import("../error.zig").Error;
const Optimizer = @import("../optimizer.zig").Optimizer;

const bit_packed_bool = @import("../encode/bit_packed_bool.zig");
const constant = @import("../encode/constant.zig");

const Bool = @This();

const Tag = enum {
    constant,
    bit_packed,
};

pub const Validator = Optimizer(struct {
    constant: constant.Validator(bool, writeDirect),
    bit_packed: bit_packed_bool.Validator,
}, Encoder);

pub const Encoder = union(Tag) {
    const Self = @This();

    constant: constant.Encoder(bool, writeDirect),
    bit_packed: bit_packed_bool.Encoder,

    pub const Value = bool;

    pub fn deinit(self: *Self) void {
        switch (self) {
            inline else => |*e| e.deinit(),
        }
    }

    pub fn begin(self: *Self, writer: anytype) !bool {
        switch (self.*) {
            inline else => |*e| return e.begin(writer),
        }
    }

    pub fn write(self: *Self, writer: anytype, value: Value) !void {
        switch (self.*) {
            inline else => |*e| try e.write(writer, value),
        }
    }

    pub fn end(self: *Self, writer: anytype) !void {
        switch (self.*) {
            inline else => |*e| try e.end(writer),
        }
    }
};

pub const Decoder = union(Tag) {
    const Self = @This();

    constant: constant.Decoder(bool, readDirect),
    bit_packed: bit_packed_bool.Decoder,

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

    pub fn next(self: *Self, n: u32) void {
        switch (self.*) {
            inline else => |*d| d.next(n),
        }
    }

    pub fn read(self: *Self, blob: anytype) !bool {
        switch (self.*) {
            inline else => |*d| return d.read(blob),
        }
    }

    pub fn readAll(self: *Self, dst: []bool, blob: anytype) !void {
        switch (self.*) {
            inline else => |*d| try d.readAll(dst, blob),
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
