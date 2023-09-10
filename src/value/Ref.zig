const c = @import("../sqlite3/c.zig").c;
const Stmt = @import("../sqlite3/Stmt.zig");

const ValueType = @import("./value_type.zig").ValueType;

const Self = @This();

value: ?*c.sqlite3_value,

pub fn valueType(self: Self) ValueType {
    return @enumFromInt(c.sqlite3_value_type(self.value));
}

pub fn isNull(self: Self) bool {
    return self.valueType() == .Null;
}

pub fn asBool(self: Self) bool {
    return c.sqlite3_value_int(self.value) != 0;
}

pub fn asI32(self: Self) i32 {
    return @intCast(c.sqlite3_value_int(self.value));
}

pub fn asI64(self: Self) i62 {
    return @intCast(c.sqlite3_value_int64(self.value));
}

pub fn asF64(self: Self) f64 {
    return @floatCast(c.sqlite3_value_double(self.value));
}

pub fn asBlob(self: Self) []const u8 {
    const len = c.sqlite3_value_bytes(self.value);
    if (len == 0) {
        return &[_]u8{};
    }
    const data = c.sqlite3_value_blob(self.value);
    return @as([*c]const u8, @ptrCast(data))[0..@intCast(len)];
}

pub fn asText(self: Self) []const u8 {
    const len = c.sqlite3_value_bytes(self.value);
    if (len == 0) {
        return &[_]u8{};
    }
    const data = c.sqlite3_value_text(self.value);
    return @as([*c]const u8, @ptrCast(data))[0..@intCast(len)];
}

pub fn bind(self: Self, stmt: Stmt, index: usize) !void {
    try stmt.bindSqliteValue(index, self.value);
}
