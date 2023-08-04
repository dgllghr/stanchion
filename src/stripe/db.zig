const Blob = @import("../sqlite3/Blob.zig");
const Conn = @import("../sqlite3/Conn.zig");
const Stmt = @import("../sqlite3/Stmt.zig");

pub const Stripes = struct {
    const Self = @This();

    conn: Conn,
    insert_stmt: ?Stmt,

    pub fn init(conn: Conn) Self {
        return .{
            .conn = conn,
            .insert_stmt = null,
        };
    }

    pub fn create(self: *Self, size: u32) !i64 {
        if (self.insert_stmt == null) {
            const query =
                \\INSERT INTO _stanchion_stripes (stripe)
                \\VALUES (zeroblob(?))
                \\RETURNING id
            ;
            self.insert_stmt = try self.conn.prepare(query);
        }

        const stmt = self.insert_stmt.?;
        try stmt.bind(.Int64, 1, @as(i64, @intCast(size)));
        // This should always succeed
        _ = try stmt.next();
        return stmt.read(.Int64, false, 0);
    }

    pub fn openBlob(self: *Self, stripe_id: i64) !Blob {
        // TODO blob cache with reopen
        return Blob.open(self.conn, "_stanchion_stripes", "stripe", stripe_id);
    }
};
