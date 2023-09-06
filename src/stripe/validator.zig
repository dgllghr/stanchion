const Encoding = @import("encoding.zig").Encoding;

pub const Meta = struct {
    byte_len: u32,
    /// `encoding` is undefined if len == 0
    encoding: Encoding,

    pub fn init() Meta {
        return .{
            .byte_len = 0,
            .encoding = undefined,
        };
    }
};

pub fn Valid(comptime Encoder: type) type {
    return struct {
        meta: Meta,
        encoder: Encoder,
    };
}
