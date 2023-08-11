const Blob = @import("../sqlite3/Blob.zig");
const Conn = @import("../sqlite3/Conn.zig");
const Stmt = @import("../sqlite3/Stmt.zig");

const Self = @This();

conn: Conn,
insert_stripe: ?Stmt,

pub fn init(conn: Conn) Self {
    return .{
        .conn = conn,
        .insert_stripe = null,
    };
}

pub fn deinit(self: *Self) void {
    if (self.insert_stripe) |stmt| {
        stmt.deinit();
    }
}

pub fn createStripe(self: *Self, size: u32) !i64 {
    if (self.insert_stripe == null) {
        const query =
            \\INSERT INTO _stanchion_stripes (stripe)
            \\VALUES (zeroblob(?))
            \\RETURNING id
        ;
        self.insert_stripe = try self.conn.prepare(query);
    }

    const stmt = self.insert_stripe.?;
    try stmt.bind(.Int64, 1, @as(i64, @intCast(size)));
    // This should always succeed
    _ = try stmt.next();
    const stripe_id = stmt.read(.Int64, false, 0);
    try stmt.reset();
    return stripe_id;
}

pub fn openStripeBlob(self: *Self, stripe_id: i64) !Blob {
    // TODO blob cache with reopen
    return Blob.open(self.conn, "_stanchion_stripes", "stripe", stripe_id);
}
