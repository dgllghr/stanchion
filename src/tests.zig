comptime {
    _ = @import("./encode/bit_packed_bool.zig");
    _ = @import("./encode/constant.zig");
    _ = @import("./encode/chooser.zig");

    _ = @import("./stripe/message_log.zig");
    _ = @import("./stripe.zig");

    _ = @import("./schema/SchemaDef.zig");
}