const std = @import("std");
const ArenaAllocator = std.heap.ArenaAllocator;

const Conn = @import("../sqlite3/Conn.zig");

const schema_mod = @import("../schema.zig");
const DataType = schema_mod.ColumnType.DataType;
const Schema = schema_mod.Schema;

const Self = @This();

conn: Conn,

vtab_table_name: []const u8,
column_data_types: []DataType,
columns_len: usize,
sort_key: []const usize,

pub fn init(
    arena: *ArenaAllocator,
    conn: Conn,
    vtab_table_name: []const u8,
    schema: *const Schema,
) !Self {
    const data_types = try arena.allocator()
        .alloc(DataType, schema.columns.items.len);
    for (schema.columns.items, data_types) |*col, *dt| {
        dt.* = col.column_type.data_type;
    }
    return .{
        .conn = conn,
        .vtab_table_name = vtab_table_name,
        .column_data_types = data_types,
        .columns_len = schema.columns.items.len,
        .sort_key = schema.sort_key.items,
    };
}
