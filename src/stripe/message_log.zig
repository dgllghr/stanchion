const std = @import("std");
const mem = std.mem;
const meta = std.meta;
const Allocator = std.mem.Allocator;
const ArrayListUnmanaged = std.ArrayListUnmanaged;

const Header = @import("./Header.zig");

const OpCode = enum(u8) {
    Insert = 1,
    Update = 2,
    Delete = 3,
};

const Error = error {
    MessageLogTooLarge,
};

pub fn MessageLog(
    comptime Value: type,
    comptime fromBytes: fn(*const [@sizeOf(Value)]u8) Value,
    comptime toBytes: fn(Value) [@sizeOf(Value)]u8,
) type {
    return struct {
        const Self = @This();

        /// Buffer containing the encoded messages in the log
        buf: ArrayListUnmanaged(u8),
        /// Buffer containing the encoded indexes of the split offsets between messages
        index_buf: ArrayListUnmanaged(u8),

        /// `MessageLog` takes ownership of `buf` and `index_buf`
        pub fn init(
            buf: ArrayListUnmanaged(u8),
            index_buf: ArrayListUnmanaged(u8),
        ) Self {
            return .{
                .index_buf = index_buf,
                .buf = buf,
            };
        }

        pub fn empty() Self {
            return .{
                .index_buf = ArrayListUnmanaged(u8){},
                .buf = ArrayListUnmanaged(u8){},
            };
        }

        pub fn open(
            allocator: Allocator,
            header: *const Header,
            /// Sliced to the part of the blob that just contains the message log
            blob: anytype,
        ) !Self {
            // TODO start with more capacity?
            var buf = try ArrayListUnmanaged(u8)
                .initCapacity(allocator, header.message_log_messages_len);
            buf.expandToCapacity();
            try blob.readAt(buf.items, 0);

            // TODO start with more capacity?
            var index_buf = try ArrayListUnmanaged(u8)
                .initCapacity(allocator, header.message_log_index_len);
            index_buf.expandToCapacity();
            try blob.readAt(index_buf.items, header.message_log_messages_len);

            return .{
                .index_buf = index_buf,
                .buf = buf,
            };
        }

        pub fn deinit(self: *Self, allocator: Allocator) void {
            self.index_buf.deinit(allocator);
            self.buf.deinit(allocator);
        }

        /// Updates the header lengths but does not write them to the blob. `blob` is the
        /// slice of the blob that contains the message log.
        pub fn write(self: Self, header: *Header, blob: anytype) !void {
            if (self.buf.items.len + self.index_buf.items.len > blob.len()) {
                // We're going to need a bigger blob
                return Error.MessageLogTooLarge;
            }
            const messages_len: u32 = @intCast(self.buf.items.len);
            if (messages_len >= header.message_log_messages_len) {
                // Only write the portion of the messages that has been appended
                try blob.writeAt(
                    self.buf.items[header.message_log_messages_len..],
                    header.message_log_messages_len,
                );
                // The whole index needs to be written
                try blob.writeAt(self.index_buf.items[0..], messages_len);
                header.message_log_messages_len = messages_len;
                header.message_log_index_len = @intCast(self.index_buf.items.len);
            }
        }

        pub fn len(self: Self) usize {
            return self.index_buf.items.len / 4;
        }

        const MessageReader = struct {
            /// For some reason, creating the exact slice of the message first and then
            /// reading out each individual field is faster than slicing from the full
            /// `buf` of the MessageLog directly and faster than slicing from the start
            /// of the message in the full `buf` to the end of the full `buf`. There is
            /// probably an optimization going on that I don't understand but this
            /// approach is depending on.
            ///
            /// TODO figure out why doing it this way is faster and how it relates to
            ///      using `.ReleaseFast` (no bounds checks) and `.ReleaseSafe` (bounds
            ///     checks).
            buf: []const u8,

            fn op(self: @This()) OpCode {
                return @enumFromInt(self.buf[0]);
            }

            fn index(self: @This()) u32 {
                return mem.readIntLittle(u32, self.buf[1..5]);
            }

            fn value(self: @This()) Value {
                const end = 5 + @sizeOf(Value);
                return fromBytes(self.buf[5..end]);
            }
        };

        pub fn scan(self: Self, index: *usize) ?Value {
            var message_idx = self.len();
            while (message_idx > 0) {
                message_idx -= 1;
                const message = MessageReader {
                    .buf = self.messageSlice(message_idx),
                };
                const idx = message.index();
                switch (message.op()) {
                    .Insert => {
                        if (idx == index.*) {
                            return message.value();
                        }
                        if (idx <= index.*) {
                            index.* -= 1;
                        }
                    },
                    .Update => {
                        if (idx == index.*) {
                            return message.value();
                        }
                    },
                    .Delete => {
                        if (idx <= index.*) {
                            index.* += 1;
                        }
                    }
                }
            }
            return null;
        }

        pub const Message = union(OpCode) {
            Insert: struct { index: u32, value: Value },
            Update: struct { index: u32, value: Value },
            Delete: struct { index: u32 },
        };

        pub fn logMessage(
            self: *Self,
            allocator: Allocator,
            comptime op: meta.Tag(Message),
            message: meta.TagPayload(Message, op),
        ) !void {
            var idx_buf: [4]u8 = undefined;

            switch (op) {
                .Delete => {
                    try self.buf.ensureUnusedCapacity(allocator, 5);
                    self.buf.appendAssumeCapacity(@intFromEnum(OpCode.Delete));
                    mem.writeIntLittle(u32, idx_buf[0..], message.index);
                    self.buf.appendSliceAssumeCapacity(idx_buf[0..]);
                },
                inline else => {
                    try self.buf.ensureUnusedCapacity(allocator, 5 + @sizeOf(Value));
                    self.buf.appendAssumeCapacity(@intFromEnum(op));
                    mem.writeIntLittle(u32, idx_buf[0..], message.index);
                    self.buf.appendSliceAssumeCapacity(idx_buf[0..]);
                    self.buf.appendSliceAssumeCapacity(&toBytes(message.value));
                },
            }

            const split_idx: u32 = @intCast(self.buf.items.len);
            mem.writeIntLittle(u32, idx_buf[0..], split_idx);
            try self.index_buf.appendSlice(allocator, idx_buf[0..]);
        }

        fn messageSlice(self: Self, index: usize) []u8 {
            if (index > 0) {
                const start_end_slice: *const [8]u8 = @ptrCast(
                    self.index_buf.items[(index - 1) * 4..(index + 1) * 4].ptr);
                const start = mem.readIntLittle(u32, start_end_slice[0..4]);
                const end = mem.readIntLittle(u32, start_end_slice[4..8]);
                return self.buf.items[start..end];
            }
            const end_slice: *const [4]u8 = @ptrCast(
                self.index_buf.items[index * 4..(index + 1) * 4].ptr);
            const end = mem.readIntLittle(u32, end_slice);
            return self.buf.items[0..end];
        }
    };
}

test "log insert" {
    const allocator = std.heap.page_allocator;

    var messages_buf = std.ArrayListUnmanaged(u8){};
    var index_buf = std.ArrayListUnmanaged(u8){};
    var mlog = MessageLog(i64, i64FromBytes, i64ToBytes)
        .init(messages_buf, index_buf);
    defer mlog.deinit(allocator);
    
    try mlog.logMessage(allocator, .Insert, .{ .index = 5, .value = -17 });
    try mlog.logMessage(allocator, .Insert, .{ .index = 3, .value = 100 });
    try mlog.logMessage(allocator, .Insert, .{ .index = 7, .value = 66 });
    try mlog.logMessage(allocator, .Insert, .{ .index = 1, .value = 3000 });
}

const MemoryBlob = @import("../blob.zig").MemoryBlob;

test "serialize round trip" {
    const allocator = std.heap.page_allocator;

    var mlog = MessageLog(i64, i64FromBytes, i64ToBytes).empty();
    defer mlog.deinit(allocator);

    try mlog.logMessage(allocator, .Insert, .{ .index = 5, .value = -17 });
    try mlog.logMessage(allocator, .Update, .{ .index = 3, .value = 2000 });
    try mlog.logMessage(allocator, .Delete, .{ .index = 0 });
    try mlog.logMessage(allocator, .Update, .{ .index = 7, .value = -1 });

    var header = Header {
        .encoding = .Direct,
        .values_len = 0,
        .message_log_messages_len = 0,
        .message_log_index_len = 0,
    };

    var blob_data = try allocator.alloc(u8, 500);
    defer allocator.free(blob_data);
    var blob = MemoryBlob { .data = blob_data };

    try mlog.write(&header, &blob);

    const mlog2 = try MessageLog(i64, i64FromBytes, i64ToBytes)
        .open(allocator, &header, &blob);

    for (0..10) |idx| {
        var idx1 = idx;
        var idx2 = idx;
        try std.testing.expectEqual(mlog.scan(&idx1), mlog2.scan(&idx2));
        try std.testing.expectEqual(idx1, idx2);
    }
}

const RndGen = std.rand.DefaultPrng;

pub fn benchScanMessageLog() !void {
    const iterations = 1_000;
    const messages_len = 10_000;
    const allocator = std.heap.page_allocator;

    var messages_buf = std.ArrayListUnmanaged(u8){};
    var index_buf = std.ArrayListUnmanaged(u8){};
    var index_entry_buf: [4]u8 = undefined;
    var rnd = RndGen.init(77_777);
    for (0..messages_len) |_| {
        try writeRandomMessage(allocator, &messages_buf, &rnd, messages_len);
        const index_entry: u32 = @intCast(messages_buf.items.len);
        std.mem.writeIntLittle(u32, index_entry_buf[0..], index_entry);
        try index_buf.appendSlice(allocator, index_entry_buf[0..]);
    }

    var message_log = MessageLog(i64, i64FromBytes, i64ToBytes)
        .init(messages_buf, index_buf);
    defer message_log.deinit(allocator);

    const start = std.time.microTimestamp();
    for (0..iterations) |_| {
        var index = rnd.random().uintLessThan(usize, messages_len);
        _ = message_log.scan(&index);
    }
    const end = std.time.microTimestamp();
    std.log.err("Scan MessageLog: {d} micros / scan\n",
        .{ @divTrunc((end - start), @as(i64, iterations)) });
}

fn writeRandomMessage(
    allocator: Allocator,
    dst: *std.ArrayListUnmanaged(u8),
    rnd: *RndGen, max_index: u32,
) !void {
    const op = rnd.random().uintLessThan(u8, 3) + 1;
    try dst.append(allocator, op);

    const index = rnd.random().uintLessThan(u32, max_index);
    var index_buf: [4]u8 = undefined;
    std.mem.writeIntLittle(u32, index_buf[0..], index);
    try dst.appendSlice(allocator, index_buf[0..]);

    if (op != @intFromEnum(OpCode.Delete)) {
        const value = rnd.random().int(i64);
        var value_buf: [8]u8 = undefined;
        std.mem.writeIntLittle(i64, value_buf[0..], value);
        try dst.appendSlice(allocator, value_buf[0..]);
    }
}

fn i64FromBytes(buf: *const [8]u8) i64 {
    return std.mem.readIntLittle(i64, buf);
}

fn i64ToBytes(value: i64) [8]u8 {
    var buf: [8]u8 = undefined;
    std.mem.writeIntLittle(i64, buf[0..], value);
    return buf;
}