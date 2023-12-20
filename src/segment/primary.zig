const stripe = @import("../stripe.zig");

const TypeTag = enum {
    bool,
    byte,
    int,
    float,
};

pub const Validator = union(TypeTag) {
    bool: stripe.Bool.Validator,
    byte: stripe.Byte.Validator,
    int: stripe.Int.Validator,
    float: stripe.Float.Validator,
};

pub const Encoder = union(TypeTag) {
    bool: stripe.Bool.Encoder,
    byte: stripe.Byte.Encoder,
    int: stripe.Int.Encoder,
    float: stripe.Float.Encoder,
};

pub const Decoder = union(TypeTag) {
    bool: stripe.Bool.Decoder,
    byte: stripe.Byte.Decoder,
    int: stripe.Int.Decoder,
    float: stripe.Float.Decoder,
};
