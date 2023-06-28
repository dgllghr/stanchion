const std = @import("std");
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
const ArrayListUnmanaged = std.ArrayListUnmanaged;

const BlobSlice = @import("./blob.zig").BlobSlice;
const Header = @import("./stripe/Header.zig");

pub const Bool = @import("./stripe/logical_type/bool.zig");

pub fn Stripe(comptime Blob: type, comptime LogicalType: type) type {
    const MessageLog = LogicalType.MessageLog;
    const Value = LogicalType.Value;

    return struct {
        const Self = @This();

        header: Header,
        values: BlobSlice(Blob),
        decoder: LogicalType.Decoder,
        message_log: MessageLog,

        pub fn open(allocator: Allocator, blob: *Blob) !Self {
            const header = try Header.read(blob);
            var values = blob.sliceFrom(Header.encoded_len);
            const decoder = try LogicalType.Decoder.init(header.encoding, values);
            const mlog = values.sliceFrom(header.values_len);
            const message_log = try MessageLog.open(allocator, &header, mlog);
            return .{
                .header = header,
                .values = values,
                .decoder = decoder,
                .message_log = message_log,
            };
        }

        pub fn deinit(self: *Self, allocator: Allocator) void {
            // TODO write the blob?
            self.message_log.deinit(allocator);
        }

        pub fn get(self: *Self, index: usize) !Value {
            var idx = index;
            const value = self.message_log.scan(&idx);
            if (value) |v| {
                return v;
            }
            return self.decoder.decode(self.values, idx);
        }

        pub fn insert(self: *Self, allocator: Allocator, index: u32, value: Value) !void {
            try self.message_log.logMessage(allocator, .{
                .Insert = .{ .index = index, .value = value }
            });
        }

        pub fn update(self: *Self, allocator: Allocator, index: u32, value: Value) !void {
            try self.message_log.logMessage(allocator, .{
                .Update = .{ .index = index, .value = value }
            });
        }

        pub fn delete(self: *Self, allocator: Allocator, index: u32) !void {
            try self.message_log.logMessage(allocator, .{
                .Delete = .{ .index = index }
            });
        }
    };
}

const MemoryBlob = @import("./blob.zig").MemoryBlob;

test "read" {
    const allocator = std.testing.allocator;
    const buf = try allocator.alloc(u8, 50);
    defer allocator.free(buf);
    var blob = MemoryBlob { .data = buf };

    const header = Header {
        .encoding = .BitPacked,
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