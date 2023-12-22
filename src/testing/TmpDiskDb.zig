const std = @import("std");
const fs = std.fs;
const testing = std.testing;
const Allocator = std.mem.Allocator;
const TmpDir = testing.TmpDir;

const sqlite = @import("../sqlite3.zig");
const Conn = sqlite.Conn;

conn: Conn,
db_path: []const u8,
tmp_dir: TmpDir,

const Self = @This();

pub fn create(allocator: Allocator, name: []const u8) !Self {
    var tmp_dir = testing.tmpDir(.{});
    try tmp_dir.dir.makeDir(&tmp_dir.sub_path);
    var path_buf: [200]u8 = undefined;
    const tmp_path = try tmp_dir.parent_dir.realpath(&tmp_dir.sub_path, &path_buf);
    const db_path = try fs.path.joinZ(allocator, &[_][]const u8{ tmp_path, name });
    const conn = try Conn.open(db_path);
    return .{
        .conn = conn,
        .db_path = db_path,
        .tmp_dir = tmp_dir,
    };
}

pub fn deinit(self: *Self, allocator: Allocator) void {
    self.conn.close();
    self.tmp_dir.cleanup();
    allocator.free(self.db_path);
}
