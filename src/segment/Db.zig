const Blob = @import("../sqlite3/Blob.zig");
const Conn = @import("../sqlite3/Conn.zig");
const Stmt = @import("../sqlite3/Stmt.zig");

const Self = @This();

conn: Conn,
insert_segment: ?Stmt,

pub const Handle = struct {
    id: i64,
    blob: Blob,
};

pub fn init(conn: Conn) Self {
    return .{
        .conn = conn,
        .insert_segment = null,
    };
}

pub fn deinit(self: *Self) void {
    if (self.insert_segment) |stmt| {
        stmt.deinit();
    }
}

pub fn allocate(self: *Self, size: usize) !Handle {
    if (self.insert_segment == null) {
        self.insert_segment = try self.conn.prepare(
            \\INSERT INTO _stanchion_segments (segment)
            \\VALUES (ZEROBLOB(?))
            \\RETURNING id
        );
    }

    const stmt = self.insert_segment.?;
    try stmt.bind(.Int64, 1, @as(i64, @intCast(size)));
    _ = try stmt.next();
    const id = stmt.read(.Int64, false, 0);
    try stmt.reset();

    const blob = try Blob.open(self.conn, "_stanchion_segments", "segment", id);
    return .{ .id = id, .blob = blob };
}

pub fn free(_: *Self, _: Handle) !void {
    @panic("todo");
}
