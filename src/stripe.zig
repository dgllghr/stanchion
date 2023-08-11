const std = @import("std");
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
const ArrayListUnmanaged = std.ArrayListUnmanaged;

const Conn = @import("./sqlite3/Conn.zig");
const SqliteBlob = @import("./sqlite3/Blob.zig");
const BlobSlice = @import("./sqlite3/Blob.zig").BlobSlice;

const MemoryBlob = @import("./MemoryBlob.zig");

pub const Db = @import("./stripe/Db.zig");
const Header = @import("./stripe/Header.zig");
const MakeMessageLog = @import("./stripe/message_log.zig").MessageLog;

pub fn Stripe(comptime Blob: type, comptime LogicalType: type) type {
    const MessageLog = MakeMessageLog(LogicalType.Value, LogicalType.readDirect, LogicalType.writeDirect);
    const Value = LogicalType.Value;

    return struct {
        const Self = @This();

        blob: *Blob,
        /// `null` when there are no values
        values: ?ValuesReader(Blob, LogicalType),
        message_log: MessageLog,

        pub fn open(allocator: Allocator, blob: *Blob) !Self {
            const header = try Header.read(blob);
            var values_blob = blob.sliceFrom(Header.encoded_len);
            var values: ?ValuesReader(Blob, LogicalType) = null;
            if (header.values_len > 0) {
                const decoder = try LogicalType.Decoder.init(header.values_encoding, values_blob);
                values = .{
                    .blob = values_blob,
                    .decoder = decoder,
                };
            }
            const mlog_blob = values_blob.sliceFrom(header.values_len);
            const message_log = try MessageLog.open(allocator, &header, mlog_blob);
            return .{
                .blob = blob,
                .values = values,
                .message_log = message_log,
            };
        }

        pub fn deinit(self: *Self, allocator: Allocator) void {
            // TODO deinit blob / return to blob pool
            self.message_log.deinit(allocator);
        }

        pub fn get(self: *Self, index: usize) !Value {
            var idx = index;
            const value = self.message_log.scan(&idx);
            if (value) |v| {
                return v;
            }
            return self.values.?.get(idx);
        }

        pub fn insert(self: *Self, allocator: Allocator, index: u32, value: Value) !void {
            try self.message_log.logMessage(allocator, .Insert, .{ .index = index, .value = value });
        }

        pub fn update(self: *Self, allocator: Allocator, index: u32, value: Value) !void {
            try self.message_log.logMessage(allocator, .Update, .{ .index = index, .value = value });
        }

        pub fn delete(self: *Self, allocator: Allocator, index: u32) !void {
            try self.message_log.logMessage(allocator, .Delete, .{ .index = index });
        }
    };
}

pub fn DbStripe(comptime LogicalType: type) type {
    const MessageLog = MakeMessageLog(LogicalType.Value, LogicalType.readDirect, LogicalType.writeDirect);
    const Value = LogicalType.Value;

    return struct {
        const Self = @This();

        id: i64,
        stripe: Stripe(SqliteBlob, LogicalType),

        pub fn create(
            db: *Db,
            blob_size: u32,
        ) !Self {
            const stripe_id = try db.createStripe(blob_size);
            var blob = try db.openStripeBlob(stripe_id);
            const header = Header{
                // The encoding doesn't matter because `values_len` is 0
                .values_encoding = @enumFromInt(0),
                .values_len = 0,
                .message_log_messages_len = 0,
                .message_log_index_len = 0,
            };
            try header.write(&blob);
            const message_log = MessageLog.empty();

            return .{
                .id = stripe_id,
                .stripe = .{
                    .blob = &blob,
                    .values = null,
                    .message_log = message_log,
                },
            };
        }

        pub fn open(allocator: Allocator, db: *Db, stripe_id: i64) !Self {
            var blob = try db.openStripeBlob(stripe_id);
            const stripe = Stripe(SqliteBlob, LogicalType).open(allocator, &blob);
            return .{
                .id = stripe_id,
                .stripe = stripe,
            };
        }

        pub fn deinit(self: *Self, allocator: Allocator) void {
            self.stripe.deinit(allocator);
        }

        pub fn insert(self: *Self, allocator: Allocator, index: u32, value: Value) !void {
            try self.stripe.insert(allocator, index, value);
        }
    };
}

test "create db stripe" {
    const conn = try Conn.openInMemory();
    defer conn.close();
    try @import("./db.zig").Migrations.apply(conn);

    var db = Db.init(conn);
    _ = try DbStripe(Bool).create(&db, 50_000);
}

fn ValuesReader(comptime Blob: type, comptime LogicalType: type) type {
    const Value = LogicalType.Value;

    return struct {
        const Self = @This();

        blob: BlobSlice(Blob),
        decoder: LogicalType.Decoder,

        pub fn get(self: *Self, index: usize) !Value {
            return self.decoder.decode(self.blob, index);
        }
    };
}

const Bool = @import("./logical_type/bool.zig");

test "read" {
    const allocator = std.testing.allocator;
    const buf = try allocator.alloc(u8, 50);
    defer allocator.free(buf);
    var blob = MemoryBlob{ .data = buf };

    const header = Header{
        .values_encoding = .BitPacked,
        .values_len = 4,
        .message_log_messages_len = 0,
        .message_log_index_len = 0,
    };
    try header.write(&blob);

    const bit_packed_bools: u32 = 0xd72f963a;
    std.mem.writeIntLittle(u32, buf[14..18], bit_packed_bools);

    var stripe = try Stripe(MemoryBlob, Bool).open(allocator, &blob);
    defer stripe.deinit(allocator);

    var values = ArrayList(bool).init(allocator);
    defer values.deinit();

    for (0..32) |idx| {
        const value = try stripe.get(idx);
        try values.append(value);
    }

    try stripe.insert(allocator, 1, true);
    try values.insert(1, true);
    try stripe.delete(allocator, 7);
    _ = values.orderedRemove(7);
    try stripe.update(allocator, 9, false);
    values.items[9] = false;
    try stripe.insert(allocator, 3, false);
    try values.insert(3, false);
    try stripe.update(allocator, 1, false);
    values.items[1] = false;

    for (0..33) |idx| {
        const value = try stripe.get(idx);
        try std.testing.expectEqual(values.items[idx], value);
    }
}
