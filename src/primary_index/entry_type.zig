const Stmt = @import("../sqlite3/Stmt.zig");

/// The primary index holds multiple types of entries. This is the tag that differentiates the
/// entries.
pub const EntryType = enum(u8) {
    TableState = 1,
    RowGroup = 2,
    Insert = 3,

    pub fn bind(self: EntryType, stmt: Stmt, index: usize) !void {
        try stmt.bind(.Int32, index, @intCast(@intFromEnum(self)));
    }
};