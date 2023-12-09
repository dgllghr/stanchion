const std = @import("std");
const math = std.math;
const mem = std.mem;
const meta = std.meta;
const Allocator = std.mem.Allocator;
const Order = math.Order;

const sqlite = @import("sqlite3.zig");
const Stmt = sqlite.Stmt;
const ValueType = sqlite.ValueType;
const ValueRef = sqlite.ValueRef;

const DataType = @import("schema.zig").ColumnType.DataType;

pub const MemoryValue = union(enum) {
    const Self = @This();

    Null,
    Boolean: bool,
    Integer: i64,
    Float: f64,
    Text: []const u8,
    Blob: []const u8,

    pub fn fromValue(
        allocator: Allocator,
        data_type: DataType,
        value: anytype,
    ) !Self {
        if (value.isNull()) {
            return .Null;
        }
        return switch (data_type) {
            .Boolean => .{ .Boolean = value.asBool() },
            .Integer => .{ .Integer = value.asI64() },
            .Float => .{ .Float = value.asF64() },
            .Text => .{ .Text = try allocator.dupe(u8, value.asText()) },
            .Blob => .{ .Blob = try allocator.dupe(u8, value.asBlob()) },
        };
    }

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

    pub fn compare(self: Self, other: anytype) Order {
        switch (self) {
            // This isn't correct by the sql definition
            .Null => .eq,
            .Boolean => |v| math.order(@intFromBool(v), @intFromBool(other.asBool())),
            .Integer => |v| math.order(v, other.asI64()),
            .Float => |v| math.order(v, other.asF64()),
            .Text => |v| mem.order(u8, v, other.asText()),
            .Blob => |v| mem.order(u8, v, other.asBlob()),
        }
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

    pub fn init(allocator: Allocator, len: usize) !Self {
        const values = try allocator.alloc(MemoryValue, len);
        return .{ .values = values };
    }

    pub fn deinit(self: *Self, allocator: Allocator) void {
        allocator.free(self.values);
    }

    /// Number of values in this change set (not including rowid). Should not be called
    /// when change type is `.Delete`
    pub fn valuesLen(self: Self) usize {
        return self.values.len;
    }

    pub fn readValue(self: Self, index: usize) !MemoryValue {
        return self.values[index];
    }
};
