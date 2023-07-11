const std = @import("std");
const testing = std.testing;

const Conn = @import("./Conn.zig");

test "statement" {
    const conn = try Conn.openInMemory();

    try conn.exec("CREATE TABLE foo (x INTEGER NOT NULL, y INTEGER NULL, z TEXT NULL)");

    try conn.exec(
        \\INSERT INTO foo (x, y, z)
        \\VALUES
        \\  (100, -7, 'hello'),
        \\  (1000, 2, NULL),
        \\  (7, 9, NULL),
        \\  (7, NULL, NULL)
    );

    const stmt = try conn.prepare("SELECT * FROM foo WHERE z IS NULL");

    try testing.expect(try stmt.next());
    var x = stmt.read(.Int32, false, 0);
    try testing.expectEqual(@as(i32, 1000), x);
    var y = stmt.read(.Int32, true, 1);
    try testing.expectEqual(@as(?i32, 2), y);

    try testing.expect(try stmt.next());
    x = stmt.read(.Int32, false, 0);
    try testing.expectEqual(@as(i32, 7), x);
    y = stmt.read(.Int32, true, 1);
    try testing.expectEqual(@as(?i32, 9), y);

    try testing.expect(try stmt.next());
    x = stmt.read(.Int32, false, 0);
    try testing.expectEqual(@as(i32, 7), x);
    y = stmt.read(.Int32, true, 1);
    try testing.expectEqual(@as(?i32, null), y);

    try testing.expect(!try stmt.next());
}