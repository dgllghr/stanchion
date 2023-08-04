pub fn main() !void {
    try @import("./stripe/message_log.zig").benchScanMessageLog();
}
