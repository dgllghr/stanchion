comptime {
    _ = @import("./sqlite3/tests.zig");

    _ = @import("./encode/bit_packed_bool.zig");
    _ = @import("./encode/constant.zig");
    _ = @import("./encode/chooser.zig");

    _ = @import("./stripe/message_log.zig");
    _ = @import("./stripe.zig");

    _ = @import("./schema/db.zig");
    _ = @import("./schema/SchemaDef.zig");
    _ = @import("./schema/Schema.zig");

    _ = @import("./value/owned.zig");

    _ = @import("./row_group.zig").RowGroups;
    _ = @import("./Table.zig");

    
}
