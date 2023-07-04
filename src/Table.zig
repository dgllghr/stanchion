const std = @import("std");
const fmt = std.fmt;
const Allocator = std.mem.Allocator;

const vtab = @import("./sqlite3/vtab.zig");
const c = @import("./sqlite3/c.zig").c;

const Self = @This();

allocator: Allocator,
conn: *c.sqlite3,
name: []const u8,
//schema: *Schema,

//pub const Cursor = @import("./Cursor.zig");
pub const InitError = error{};

pub fn create(
    allocator: Allocator,
    conn: *c.sqlite3,
    _: *vtab.CallbackContext,
    _: []const []const u8,
) !Self {
    return .{
        .allocator = allocator,
        .conn = conn,
        .name = "",
    };
}

pub fn connect(
    allocator: Allocator,
    conn: *c.sqlite3,
    _: *vtab.CallbackContext,
    _: []const []const u8,
) !Self {
    return .{
        .allocator = allocator,
        .conn = conn,
        .name = "",
    };
}

pub fn disconnect(self: *Self) void {
    self.allocator.free(self.name);
}

pub fn destroy(self: *Self) void {
    self.allocator.free(self.name);
}

pub fn schema(_: *Self, allocator: Allocator) ![:0]const u8 {
    return fmt.allocPrintZ(allocator, "CREATE TABLE x (foo INTEGER)", .{});
}

// pub fn bestIndex(self: *Self) !void {

// }

// pub fn open(self: *Self) !Cursor {

// }

// pub fn update(self: *Self) !i64 {
    
// }

