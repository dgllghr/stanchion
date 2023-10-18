const std = @import("std");
const meta = std.meta;

const Error = @import("error.zig").Error;
const Valid = @import("validator.zig").Valid;

/// A Validator that receives other validators as input and chooses the encoding that
/// results in the smallest byte length for the stripe.
pub fn Optimizer(comptime Validators: type, comptime Encoder: type) type {
    const validator_fields = std.meta.fields(Validators);
    if (validator_fields.len == 0) {
        @compileError("optimizer requires at least one validator");
    }

    // TODO check that the encoder union has a variant with 1:1 mapping between encoder
    //      variants and validator fields
    const Value = Encoder.Value;

    return struct {
        const Self = @This();

        validators: Validators,

        pub fn init() Self {
            var validators: Validators = undefined;
            inline for (validator_fields) |f| {
                @field(validators, f.name) = f.type.init();
            }
            return .{ .validators = validators };
        }

        pub fn next(self: *Self, value: Value) void {
            inline for (validator_fields) |f| {
                var validator = &@field(self.validators, f.name);
                validator.next(value);
            }
        }

        pub fn end(self: *Self) !Valid(Encoder) {
            var found_valid = false;
            var valid = Valid(Encoder){
                .meta = .{
                    .byte_len = @as(u32, std.math.maxInt(u32)),
                    .encoding = undefined,
                },
                .encoder = undefined,
            };
            inline for (validator_fields) |f| {
                const validator = &@field(self.validators, f.name);
                if (validator.end()) |v| {
                    if (v.meta.byte_len < valid.meta.byte_len) {
                        found_valid = true;
                        valid.meta = v.meta;
                        valid.encoder = @unionInit(
                            Encoder,
                            f.name,
                            v.encoder,
                        );
                    }
                } else |_| {}
            }
            if (!found_valid) {
                return Error.NotEncodable;
            }
            return valid;
        }
    };
}
