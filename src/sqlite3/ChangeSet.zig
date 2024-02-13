const c = @import("../sqlite3/c.zig").c;
const ValueRef = @import("value.zig").Ref;

const Self = @This();

values: []?*c.sqlite3_value,

pub const Value = ValueRef;

pub const ChangeType = enum {
    Insert,
    Update,
    Delete,

    pub fn name(self: ChangeType) []const u8 {
        return switch (self) {
            .Insert => "INSERT",
            .Update => "UPDATE",
            .Delete => "DELETE",
        };
    }
};

pub fn init(values: []?*c.sqlite3_value) Self {
    return .{ .values = values };
}

pub fn changeType(self: Self) ChangeType {
    if (self.values.len == 1) {
        return .Delete;
    }
    if ((ValueRef{ .value = self.values[0] }).isNull()) {
        return .Insert;
    }
    return .Update;
}

pub fn readRowid(self: Self) ValueRef {
    return .{
        .value = self.values[1],
    };
}

/// Number of values in this change set (not including rowid). Should not be called when
/// change type is `.Delete`
pub fn valuesLen(self: Self) usize {
    return self.values.len - 2;
}

pub fn readValue(self: Self, index: usize) !ValueRef {
    return .{
        .value = self.values[index + 2],
    };
}
