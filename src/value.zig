const std = @import("std");
const meta = std.meta;
const Allocator = std.mem.Allocator;

const Stmt = @import("sqlite3/Stmt.zig");
const sqlite_value = @import("sqlite3/value.zig");
const ValueType = sqlite_value.ValueType;
const ValueRef = sqlite_value.Ref;

pub const MemoryValue = union(enum) {
    const Self = @This();

    Null,
    Boolean: bool,
    Integer: i64,
    Float: f64,
    Text: []const u8,
    Blob: []const u8,

    pub fn fromRef(allocator: Allocator, value: ValueRef) !Self {
        return switch (value.valueType()) {
            .Null => .Null,
            .Integer => .{ .Integer = value.asI64() },
            .Float => .{ .Float = value.asF64() },
            .Text => .{ .Text = try allocator.dupe(u8, value.asText()) },
            .Blob => .{ .Blob = try allocator.dupe(u8, value.asBlob()) },
        };
    }

    pub fn deinit(self: *Self, allocator: Allocator) void {
        switch (self) {
            .Null, .Boolean, .Integer, .Float => {},
            else => |bytes| allocator.free(bytes),
        }
    }

    pub fn valueType(self: Self) ValueType {
        return switch (self) {
            .Null => ValueType.Null,
            .Boolean => ValueType.Integer,
            .Integer => ValueType.Integer,
            .Float => ValueType.Float,
            .Text => ValueType.Text,
            .Blob => ValueType.Blob,
        };
    }

    pub fn isNull(self: Self) bool {
        return self == .Null;
    }

    pub fn asBool(self: Self) bool {
        return self.Boolean;
    }

    pub fn asI32(self: Self) i32 {
        return @intCast(self.Integer);
    }

    pub fn asI64(self: Self) i64 {
        return self.Integer;
    }

    pub fn asF64(self: Self) f64 {
        return self.Float;
    }

    pub fn asBlob(self: Self) []const u8 {
        return self.Blob;
    }

    pub fn asText(self: Self) []const u8 {
        return self.Text;
    }

    pub fn bind(self: Self, stmt: Stmt, index: usize) !void {
        switch (self) {
            .Null => try stmt.bindNull(index),
            .Boolean => |v| try stmt.bind(.Int32, index, if (v) 1 else 0),
            .Integer => |v| try stmt.bind(.Int64, index, v),
            .Float => |v| try stmt.bind(.Float, index, v),
            .Text => |v| try stmt.bind(.Text, index, v),
            .Blob => |v| try stmt.bind(.Blob, index, v),
        }
    }
};

pub const MemoryTuple = struct {
    const Self = @This();

    values: []MemoryValue,

    pub const Value = MemoryValue;

    pub fn init(values: []MemoryValue) Self {
        return .{ .values = values };
    }

    pub fn deinit(self: *Self, allocator: Allocator) void {
        for (self.values) |v| {
            v.deinit(allocator);
        }
        allocator.free(self.values);
    }

    /// Number of values in this change set (not including rowid). Should not be called
    /// when change type is `.Delete`
    pub fn valuesLen(self: Self) usize {
        return self.values.len;
    }

    pub fn readValue(self: Self, index: usize) MemoryValue {
        return self.values[index];
    }
};
