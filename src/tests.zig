comptime {
    _ = @import("./sqlite3/tests.zig");

    _ = @import("./schema/db.zig");
    _ = @import("./schema/SchemaDef.zig");
    _ = @import("./schema/Schema.zig");

    _ = @import("./value/owned.zig");

    _ = @import("./stripe/encode/bit_packed_bool.zig");
    _ = @import("./stripe/encode/constant.zig");
    _ = @import("./stripe/logical_type/Bool.zig");
    _ = @import("./stripe/optimizer.zig");
    _ = @import("./stripe.zig");

    _ = @import("./db.zig");
    _ = @import("./row_group.zig");
    _ = @import("./Table.zig");
}
