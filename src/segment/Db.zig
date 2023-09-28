const Blob = @import("../sqlite3/Blob.zig");
const Conn = @import("../sqlite3/Conn.zig");
const Stmt = @import("../sqlite3/Stmt.zig");

const Self = @This();

conn: Conn,
insert_segment: ?Stmt,
delete_segment: ?Stmt,

pub const Handle = struct {
    id: i64,
    blob: Blob,

    pub fn close(self: *@This()) void {
        // TODO log the error
        self.blob.close() catch {};
    }
};

const segments_table_name = "_stanchion_segments";
const segment_column_name = "segment";

pub fn init(conn: Conn) Self {
    return .{
        .conn = conn,
        .insert_segment = null,
        .delete_segment = null,
    };
}

pub fn deinit(self: *Self) void {
    if (self.insert_segment) |stmt| {
        stmt.deinit();
    }
    if (self.delete_segment) |stmt| {
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

    const blob = try Blob.open(
        self.conn,
        segments_table_name,
        segment_column_name,
        id,
    );
    return .{ .id = id, .blob = blob };
}

pub fn open(self: *Self, id: i64) !Handle {
    const blob = try Blob.open(
        self.conn,
        segments_table_name,
        segment_column_name,
        id,
    );
    return .{
        .id = id,
        .blob = blob,
    };
}

pub fn free(self: *Self, handle: Handle) !void {
    if (self.delete_segment == null) {
        self.delete_segment = try self.conn.prepare(
            \\DELETE FROM _stanchion_segments
            \\WHERE id = ?
        );
    }

    const stmt = self.delete_segment.?;
    try stmt.bind(.Int64, 1, @as(i64, @intCast(handle.id)));
    try stmt.exec();
    try stmt.reset();
}
