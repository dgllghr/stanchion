const Conn = @import("../sqlite3/Conn.zig");

const Schema = @import("../schema.zig").Schema;

const Self = @This();

conn: Conn,

vtab_table_name: []const u8,
columns_len: usize,
sort_key: []const usize,

pub fn init(conn: Conn, vtab_table_name: []const u8, schema: *const Schema) Self {
    return .{
        .conn = conn,
        .vtab_table_name = vtab_table_name,
        .columns_len = schema.columns.items.len,
        .sort_key = schema.sort_key.items,
    };
}
