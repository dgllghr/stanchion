/// Code that indicates which index is being used. Currently only the sort key is supported.
pub const Code = enum(u1) {
    sort_key = 1,
};
