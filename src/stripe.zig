const std = @import("std");
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
const ArrayListUnmanaged = std.ArrayListUnmanaged;

const Optimizer = @import("./stripe/optimizer.zig").Optimizer;

pub const Encoding = @import("./stripe/encoding.zig").Encoding;

pub const Type = struct {
    pub const Bool = @import("./stripe/logical_type/Bool.zig");
};

pub fn Reader(comptime Blob: type, comptime LogicalType: type) type {
    const Value = LogicalType.Value;

    return struct {
        const Self = @This();

        blob: *const Blob,
        decoder: LogicalType.Decoder,

        pub fn init(encoding: Encoding, blob: *const Blob) !Self {
            const decoder = try LogicalType.Decoder.init(encoding, blob);
            return .{
                .blob = blob,
                .decoder = decoder,
            };
        }

        pub fn get(self: *Self, index: usize) !Value {
            return self.decoder.decode(self.blob, index);
        }
    };
}

pub fn Validator(comptime LogicalType: type) type {
    return Optimizer(LogicalType);
}

pub fn Writer(comptime Blob: type, comptime LogicalType: type) type {
    const Value = LogicalType.Value;

    return struct {
        const Self = @This();

        blob: *const Blob,
        encoder: LogicalType.Encoder,

        pub fn init(encoder: LogicalType.Encoder, blob: *const Blob) !Self {
            return .{
                .blob = blob,
                .encoder = encoder,
            };
        }

        pub fn write(_: *Self, _: Value) !void {
            @panic("todo");
        }
    };
}
