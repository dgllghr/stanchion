pub const Encoding = enum(u8) {
    Direct = 1,
    Constant = 2,
    BitPacked = 3,
    _,

    pub fn canonicalName(self: Encoding) []const u8 {
        return switch (self) {
            .Direct => "DIRECT",
            .Constant => "CONSTANT",
            .BitPacked => "BITPACKED",
            _ => "???",
        };
    }
};
