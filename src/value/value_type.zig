const c = @import("../sqlite3/c.zig").c;

pub const ValueType = enum(c_int) {
    Null = c.SQLITE_NULL,
    Integer = c.SQLITE_INTEGER,
    Float = c.SQLITE_FLOAT,
    Text = c.SQLITE_TEXT,
    Blob = c.SQLITE_BLOB,
};