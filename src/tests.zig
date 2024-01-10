test {
    _ = @import("sqlite3/tests.zig");
    _ = @import("schema.zig");
    _ = @import("value.zig");
    _ = @import("stripe.zig");
    _ = @import("segment.zig");
    _ = @import("PendingInserts.zig");
    _ = @import("row_group.zig");
    _ = @import("index.zig");
    _ = @import("Table.zig");
}
