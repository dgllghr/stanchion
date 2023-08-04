const std = @import("std");
const meta = std.meta;
const Allocator = std.mem.Allocator;

const Stmt = @import("../sqlite3/Stmt.zig");

const ValueType = @import("./value_type.zig").ValueType;
const ValueRef = @import("./Ref.zig");

pub const OwnedValue = union(ValueType) {
    const Self = @This();

    Null,
    Integer: i64,
    Float: f64,
    Text: []const u8,
    Blob: []const u8,

    pub fn deinit(self: *Self, allocator: Allocator) void {
        switch (self) {
            .Null, .Integer, .Float => {},
            else => |bytes| allocator.free(bytes),
        }
    }

    pub fn valueType(self: Self) ValueType {
        return meta.activeTag(self);
    }

    pub fn isNull(self: Self) bool {
        return self == .Null;
    }

    pub fn asI32(self: Self) i32 {
        return @intCast(self.Integer);
    }

    pub fn asI64(self: Self) i62 {
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
            .Integer => |v| try stmt.bind(.Int64, index, v),
            .Float => |v| try stmt.bind(.Float, index, v),
            .Text => |v| try stmt.bind(.Text, index, v),
            .Blob => |v| try stmt.bind(.Blob, index, v),
        }
    }
};

pub const OwnedRow = struct {
    const Self = @This();

    rowid: ?i64,
    values: []OwnedValue,

    pub const Value = OwnedValue;

    pub fn init(rowid: ?i64, values: []OwnedValue) Self {
        return .{
            .rowid = rowid,
            .values = values,
        };
    }

    pub fn deinit(self: *Self, allocator: Allocator) void {
        for (self.values) |v| {
            v.deinit(allocator);
        }
        allocator.free(self.values);
    }

    pub fn readRowid(self: Self) i64 {
        return self.rowid.?;
    }

    /// Number of values in this change set (not including rowid). Should not be called
    /// when change type is `.Delete`
    pub fn valuesLen(self: Self) usize {
        return self.values.len;
    }

    pub fn readValue(self: Self, index: usize) OwnedValue {
        return self.values[index];
    }
};
