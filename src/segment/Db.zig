const std = @import("std");
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const Blob = @import("../sqlite3/Blob.zig");
const Conn = @import("../sqlite3/Conn.zig");
const Stmt = @import("../sqlite3/Stmt.zig");

const stmt_cell = @import("../stmt_cell.zig");

const Self = @This();

const StmtCell = stmt_cell.StmtCell(Self);

conn: Conn,
insert_segment: StmtCell,
delete_segment: StmtCell,

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
        .insert_segment = StmtCell.init(&insertSegmentDml),
        .delete_segment = StmtCell.init(&deleteSegmentDml),
    };
}

pub fn deinit(self: *Self) void {
    self.insert_segment.deinit();
    self.delete_segment.deinit();
}

fn insertSegmentDml(_: *const Self, _: *ArenaAllocator) ![]const u8 {
    return
        \\INSERT INTO _stanchion_segments (segment)
        \\VALUES (ZEROBLOB(?))
        \\RETURNING id
        ;
}

pub fn allocate(self: *Self, tmp_arena: *ArenaAllocator, size: usize) !Handle {
    const stmt = try self.insert_segment.getStmt(tmp_arena, self);
    self.insert_segment.reset();

    try stmt.bind(.Int64, 1, @as(i64, @intCast(size)));
    _ = try stmt.next();
    const id = stmt.read(.Int64, false, 0);

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

fn deleteSegmentDml(_: *const Self, _: *ArenaAllocator) ![]const u8 {
    return
        \\DELETE FROM _stanchion_segments
        \\WHERE id = ?
        ;
}

pub fn free(self: *Self, tmp_arena: *ArenaAllocator, handle: Handle) !void {
    const stmt = try self.delete_segment.getStmt(tmp_arena, self);
    self.delete_segment.reset();

    try stmt.bind(.Int64, 1, @as(i64, @intCast(handle.id)));
    try stmt.exec();
}
