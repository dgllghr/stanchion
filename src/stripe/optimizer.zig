const std = @import("std");

const Error = @import("./error.zig").Error;
const Valid = @import("./validator.zig").Valid;

pub fn Optimizer(comptime LogicalType: type) type {
    // TODO check that the encoder union has a variant with 1:1 mapping between encoder
    //      variants and validator fields

    // TODO it might improve performance to round this up to a u{power-of-2}
    const ContinueMask = @Type(.{
        .Int = .{
            .signedness = .unsigned,
            .bits = std.meta.fields(LogicalType.Validators).len,
        },
    });

    return struct {
        const Self = @This();

        validators: LogicalType.Validators,
        continue_mask: ContinueMask,

        fn init(validators: LogicalType.Validators) Self {
            return .{
                .validators = validators,
                .continue_mask = std.math.maxInt(ContinueMask),
            };
        }

        pub fn next(self: *Self, value: LogicalType.Value) !void {
            inline for (std.meta.fields(LogicalType.Validators), 0..) |f, idx| {
                if (self.continue_mask & (1 << idx) > 0) {
                    var validator = &@field(self.validators, f.name);
                    var cont = true;
                    validator.next(value) catch |_| {
                        cont = false;
                    };
                    if (cont) {
                        self.continue_mask |= 1 << idx;
                    } else {
                        self.continue_mask &= 0 << idx;
                    }
                }
            }
            if (self.continue_mask == 0) {
                return Error.ValuesNotEncodable;
            }
        }

        pub fn finish(self: *Self) !Valid(LogicalType.Encoder) {
            var found_valid = false;
            var valid = Valid(LogicalType.Encoder){
                .encoder = undefined,
                .encoding = undefined,
                .byte_len = @as(usize, std.math.maxInt(usize)),
            };
            inline for (std.meta.fields(LogicalType.Validators)) |f| {
                const validator = &@field(self.validators, f.name);
                if (validator.finish()) |v| {
                    if (v.byte_len < valid.byte_len) {
                        found_valid = true;
                        valid.byte_len = v.byte_len;
                        valid.encoding = v.encoding;
                        valid.encoder = @unionInit(
                            LogicalType.Encoder,
                            f.name,
                            v.encoder,
                        );
                    }
                }
            }
            if (!found_valid) {
                return Error.ValuesNotEncodable;
            }
            return valid;
        }
    };
}

test "optimize encoding" {
    const Bool = @import("./logical_type/Bool.zig");

    var optimizer = Optimizer(Bool).init(Bool.Validators.init());
    for (0..100) |idx| {
        // The loop will exit after the second value is passed to the constant validator
        try std.testing.expect(idx < 2);
        const cont = optimizer.next(idx % 2 == 0);
        if (!cont) {
            break;
        }
    }
    var valid = optimizer.finish();
    try std.testing.expect(valid != null);
    try std.testing.expect(valid.?.encoding == .BitPacked);
}
