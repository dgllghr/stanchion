const std = @import("std");
const Int = std.builtin.Type.Int;

pub const InvalidEncodingError = error{InvalidEncoding};

pub const Encoding = enum(u8) {
    Direct = 1,
    Constant = 2,
    BitPacked = 3,
};

pub fn makeChooser(
    comptime Value: type,
    validators: anytype,
) Chooser(Value, @TypeOf(validators)) {
    return Chooser(Value, @TypeOf(validators)).init(validators);
}

pub fn Chooser(comptime Value: type, comptime Validators: type) type {
    const ContinueMask = @Type(.{
        .Int = .{
            .signedness = .unsigned,
            .bits = std.meta.fields(Validators).len,
        },
    });

    return struct {
        const Self = @This();

        validators: Validators,
        continue_mask: ContinueMask,

        fn init(validators: Validators) Self {
            return .{
                .validators = validators,
                .continue_mask = std.math.maxInt(ContinueMask),
            };
        }

        pub fn next(self: *Self, value: Value) bool {
            inline for (std.meta.fields(Validators), 0..) |f, idx| {
                if (self.continue_mask & (1 << idx) > 0) {
                    var validator = &@field(self.validators, f.name);
                    const cont = validator.next(value);
                    if (cont) {
                        self.continue_mask |= 1 << idx;
                    } else {
                        self.continue_mask &= 0 << idx;
                    }
                }
            }
            return self.continue_mask > 0;
        }

        pub fn finish(self: *Self) ?Choice(Validators) {
            var found_valid = false;
            var byte_len = @as(usize, std.math.maxInt(usize));
            var best_encoder: EncoderUnion(Validators) = undefined;
            inline for (std.meta.fields(Validators)) |f| {
                const validator = &@field(self.validators, f.name);
                if (validator.finish()) |valid| {
                    if (valid.byte_len < byte_len) {
                        found_valid = true;
                        byte_len = valid.byte_len;
                        best_encoder = @unionInit(
                            EncoderUnion(Validators),
                            f.name,
                            valid.encoder,
                        );
                    }
                }
            }
            if (!found_valid) {
                return null;
            }
            return .{
                .encoder = best_encoder,
                .byte_len = byte_len,
            };
        }
    };
}

fn Choice(comptime Validators: type) type {
    return struct {
        encoder: EncoderUnion(Validators),
        byte_len: usize,
    };
}

fn EncoderUnion(comptime Validators: type) type {
    const UnionField = std.builtin.Type.UnionField;
    const EnumField = std.builtin.Type.EnumField;

    const validator_fields = std.meta.fields(Validators);
    var union_fields: [validator_fields.len]UnionField = undefined;
    var enum_fields: [validator_fields.len]EnumField = undefined;
    inline for (validator_fields, 0..) |f, idx| {
        const Encoder = f.type.Encoder;
        union_fields[idx] = .{
            .name = f.name,
            .type = Encoder,
            // TODO what should this be?
            .alignment = @alignOf(Encoder),
        };
        enum_fields[idx] = .{
            .name = f.name,
            .value = idx,
        };
    }
    const EncoderEnum = @Type(.{
        .Enum = .{
            .tag_type = @Type(.{
                .Int = .{ .signedness = .unsigned, .bits = validator_fields.len, },
            }),
            .fields = enum_fields[0..],
            .decls = &[_]std.builtin.Type.Declaration{},
            .is_exhaustive = true,
        }
    });
    return @Type(.{
        .Union = .{
            .layout = .Auto,
            .tag_type = EncoderEnum,
            .fields = union_fields[0..],
            .decls = &[_]std.builtin.Type.Declaration{},
        }
    });
}

test "generate union of encoders type for validators type" {
    const constant = @import("../encode/constant.zig");
    const C = Choice(@TypeOf(.{
        .foo = constant.Validator(u32, writeU32).init(),
    }));
    const union_fields = @typeInfo(@typeInfo(C).Struct.fields[0].type).Union.fields;
    try std.testing.expectEqualStrings("foo", union_fields[0].name[0..]);
}

test "choose encoding" {
    const constant = @import("../encode/constant.zig");
    var chooser = makeChooser(u32, .{
        constant.Validator(u32, writeU32).init(),
    });
    for (0..100) |idx| {
        // The loop will exit after the second value is passed to the constant validator
        try std.testing.expect(idx < 2);
        const cont = chooser.next(@intCast(idx));
        if (!cont) {
            break;
        }
    }
    var choice = chooser.finish();
    try std.testing.expect(choice == null);

    chooser = makeChooser(u32, .{
        constant.Validator(u32, writeU32).init(),
    });
    var cont = chooser.next(17);
    try std.testing.expectEqual(true, cont);
    cont = chooser.next(17);
    try std.testing.expectEqual(true, cont);
    cont = chooser.next(17);
    try std.testing.expectEqual(true, cont);
    choice = chooser.finish();
    try std.testing.expectEqual(@as(usize, 4), choice.?.byte_len);
}

fn writeU32(value: u32) [4]u8 {
    var buf: [4]u8 = undefined;
    std.mem.writeIntLittle(u32, &buf, value);
    return buf;
}