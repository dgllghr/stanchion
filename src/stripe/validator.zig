const Encoding = @import("./encoding.zig").Encoding;

pub fn Valid(comptime Encoder: type) type {
    return struct {
        byte_len: usize,
        encoding: Encoding,
        encoder: Encoder,
    };
}
