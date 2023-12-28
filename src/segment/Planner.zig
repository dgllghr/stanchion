//! A Planner analyzes the data that will go into a segment and determines the layout and encodings
//! for the segment. The layout includes the size of the segment and the sizes and encodings of the
//! stripes within the segment. Create a Planner and feed the data that will go into the segment by
//! calling `addNext` on the values that will go into the segment (in order). Then call `end` to
//! create a `Plan`, which contains the layout and encoders for a segment. The `Plan` can be used
//! to create a `Writer`, which writes the segment.

const schema_mod = @import("../schema.zig");
const ColumnType = schema_mod.ColumnType;

const stripe = @import("../stripe.zig");

const primary_mod = @import("primary.zig");
const Header = @import("Header.zig");
const PrimaryEncoder = primary_mod.Encoder;
const PrimaryValidator = primary_mod.Validator;

column_type: ColumnType,
present: stripe.Bool.Validator,
length: ?stripe.Int.Validator,
primary: PrimaryValidator,

const Self = @This();

pub const Plan = struct {
    header: Header,
    present: ?stripe.Bool.Encoder,
    length: ?stripe.Int.Encoder,
    primary: ?PrimaryEncoder,

    pub fn totalLen(self: *const Plan) u32 {
        return self.header.totalSegmentLen();
    }
};

pub fn init(column_type: ColumnType) Self {
    var length: ?stripe.Int.Validator = null;
    var primary: PrimaryValidator = undefined;
    switch (column_type.data_type) {
        .Boolean => primary = .{ .bool = stripe.Bool.Validator.init() },
        .Integer => primary = .{ .int = stripe.Int.Validator.init() },
        .Float => primary = .{ .float = stripe.Float.Validator.init() },
        .Blob, .Text => {
            length = stripe.Int.Validator.init();
            primary = .{ .byte = stripe.Byte.Validator.init() };
        },
    }
    return .{
        .column_type = column_type,
        .present = stripe.Bool.Validator.init(),
        .length = length,
        .primary = primary,
    };
}

pub fn reset(self: *Self) void {
    self.* = init(self.column_type);
}

pub fn addNext(self: *Self, value: anytype) void {
    if (value.isNull()) {
        self.present.next(false);
        return;
    }

    self.present.next(true);
    switch (self.column_type.data_type) {
        .Boolean => self.primary.bool.next(value.asBool()),
        .Integer => self.primary.int.next(value.asI64()),
        .Float => self.primary.float.next(value.asF64()),
        .Text => {
            const text = value.asText();
            self.length.?.next(@as(i64, @intCast(text.len)));
            for (text) |b| {
                self.primary.byte.next(b);
            }
        },
        .Blob => {
            const blob = value.asBlob();
            self.length.?.next(@as(i64, @intCast(blob.len)));
            for (blob) |b| {
                self.primary.byte.next(b);
            }
        },
    }
}

pub fn end(self: *Self) !Plan {
    var header = Header.init();

    var present_encoder: ?stripe.Bool.Encoder = null;
    const present = try self.present.end();

    // Optimization: skip the present stripe if all values present (no nulls) and skip the
    // primary stripe if no values present (all nulls)
    // Constant encoding is always chosen when all values are the same
    var all_nulls = false;
    var no_nulls = false;
    if (present.meta.encoding == .Constant) {
        no_nulls = present.encoder.constant.value;
        all_nulls = !no_nulls;
    }

    // All values present (no nulls) => skip the present stripe
    if (!no_nulls) {
        header.present_stripe = present.meta;
        present_encoder = present.encoder;
    }

    var length_encoder: ?stripe.Int.Encoder = null;
    if (self.length) |*length_validator| {
        const length = try length_validator.end();
        header.length_stripe = length.meta;
        length_encoder = length.encoder;
    }

    var primary_encoder: ?PrimaryEncoder = null;
    // All values not present (all nulls) => omit the primary stripe
    if (!all_nulls) {
        switch (self.primary) {
            inline else => |*v, tag| {
                const prim = try v.end();
                header.primary_stripe = prim.meta;
                primary_encoder = @unionInit(PrimaryEncoder, @tagName(tag), prim.encoder);
            },
        }
    }

    return .{
        .header = header,
        .present = present_encoder,
        .length = length_encoder,
        .primary = primary_encoder,
    };
}

test "segment: planner" {
    const testing = @import("std").testing;
    const MemoryValue = @import("../value.zig").MemoryValue;

    var planner = init(.{
        .data_type = .Integer,
        .nullable = true,
    });

    for (0..10) |v| {
        planner.addNext(MemoryValue{
            .Integer = @as(i64, @intCast(v)),
        });
    }

    const plan = try planner.end();
    try testing.expect(plan.present == null);
    try testing.expect(plan.length == null);
    try testing.expect(plan.primary != null);
}
