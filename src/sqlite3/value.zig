const std = @import("std");

const c = @import("c.zig").c;
const Stmt = @import("Stmt.zig");

pub const ValueType = enum(c_int) {
    Null = c.SQLITE_NULL,
    Integer = c.SQLITE_INTEGER,
    Float = c.SQLITE_FLOAT,
    Text = c.SQLITE_TEXT,
    Blob = c.SQLITE_BLOB,
};

pub const Ref = struct {
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

    pub fn asI64(self: Self) i64 {
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
        try stmt.bindSqliteValue(index, self);
    }

    pub fn format(
        self: Self,
        comptime _: []const u8,
        _: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        switch (self.valueType()) {
            .Null => try writer.print("NULL", .{}),
            .Integer => try writer.print("{d}", .{self.asI64()}),
            .Float => try writer.print("{d}", .{self.asF64()}),
            .Text => try writer.print("{s}", .{self.asText()}),
            .Blob => try writer.print("{s}", .{self.asBlob()}),
        }
    }
};
