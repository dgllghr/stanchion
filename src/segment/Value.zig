const std = @import("std");
const math = std.math;
const mem = std.mem;
const Order = math.Order;

const sqlite = @import("../sqlite3.zig");
const ValueType = sqlite.ValueType;

const schema_mod = @import("../schema.zig");
const DataType = schema_mod.ColumnType.DataType;

data_type: DataType,
primary: ?PrimaryValue,

const Self = @This();

pub const PrimaryValue = union {
    bool: bool,
    int: i64,
    float: f64,
    bytes: []const u8,
};

pub fn valueType(self: Self) ValueType {
    if (self.isNull()) {
        return .Null;
    }

    return switch (self.data_type) {
        .Boolean => .Integer,
        .Integer => .Integer,
        .Float => .Float,
        .Text => .Text,
        .Blob => .Blob,
    };
}

pub fn isNull(self: Self) bool {
    return self.primary == null;
}

pub fn asBool(self: Self) bool {
    return self.primary.?.bool;
}

pub fn asI32(self: Self) i32 {
    return @intCast(self.primary.?.int);
}

pub fn asI64(self: Self) i64 {
    return self.primary.?.int;
}

pub fn asF64(self: Self) f64 {
    return self.primary.?.float;
}

pub fn asBlob(self: Self) []const u8 {
    return self.primary.?.bytes;
}

pub fn asText(self: Self) []const u8 {
    return self.primary.?.bytes;
}

pub fn compare(self: Self, other: anytype) Order {
    // This isn't correct by the sql definition
    if (self.isNull()) {
        return .eq;
    }

    return switch (self.data_type) {
        .Boolean => math.order(@intFromBool(self.asBool()), @intFromBool(other.asBool())),
        .Integer => math.order(self.asI64(), other.asI64()),
        .Float => math.order(self.asF64(), other.asF64()),
        .Text => mem.order(u8, self.asText(), other.asText()),
        .Blob => mem.order(u8, self.asBlob(), other.asBlob()),
    };
}
