const c = @import("../sqlite3/c.zig").c;

const ValueRef = @import("./Ref.zig");

const Self = @This();

values: []?*c.sqlite3_value,

pub const Value = ValueRef;

pub const ChangeType = enum {
    Insert,
    Update,
    Delete,
};

pub fn init(values: []?*c.sqlite3_value) Self {
    return .{ .values = values };
}

pub fn changeType(self: Self) ChangeType {
    if (self.values.len == 0) {
        return .Delete;
    }
    if (self.readRowid().isNull()) {
        return .Insert;
    }
    return .Update;
}

pub fn readRowid(self: Self) i64 {
    const ref = ValueRef{
        .value = self.values[0],
    };
    return ref.asI64();
}

/// Number of values in this change set (not including rowid). Should not be called when
/// change type is `.Delete`
pub fn valuesLen(self: Self) usize {
    return self.values.len - 1;
}

pub fn readValue(self: Self, index: usize) ValueRef {
    return .{
        .value = self.values[index + 1],
    };
}
