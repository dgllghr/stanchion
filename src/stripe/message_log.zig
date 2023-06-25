const std = @import("std");
const Allocator = std.mem.Allocator;
const ArrayListUnmanaged = std.ArrayListUnmanaged;

pub const OpCode = enum(u8) {
    Insert = 1,
    Update = 2,
    Delete = 3,
};

fn MessageLog(
    comptime Value: type,
    comptime fromBytes: fn(*const [@sizeOf(Value)]u8) Value,
    //comptime toBytes: fn(Value) [@sizeOf(Value)]u8,
) type {
    return struct {
        const Self = @This();

        /// Buffer containing the encoded messages in the log
        buf: ArrayListUnmanaged(u8),
        /// The index of the first byte of data in `buf` _not_ written to the underlying
        /// blob. Only data appended to `buf` (which is append only) since the last write
        /// is written on a sync to blob. This is unlike `index_buf`, which is written in
        /// its entirety on each sync.
        write_end: usize,
        /// Buffer containing the encoded indexes of the split offsets between messages
        index_buf: ArrayListUnmanaged(u8),

        const Message = struct {
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
                return @intToEnum(OpCode, self.buf[0]);
            }

            fn index(self: @This()) u32 {
                return std.mem.readIntLittle(u32, self.buf[1..5]);
            }

            fn value(self: @This()) Value {
                const end = 5 + @sizeOf(Value);
                return fromBytes(self.buf[5..end]);
            }
        };

        pub fn open(
            buf: ArrayListUnmanaged(u8),
            index_buf: ArrayListUnmanaged(u8),
        ) !Self {
            const write_end = buf.items.len;
            return .{
                .index_buf = index_buf,
                .buf = buf,
                .write_end = write_end,
            };
        }

        pub fn deinit(self: *Self, allocator: Allocator) void {
            self.index_buf.deinit(allocator);
            self.buf.deinit(allocator);
        }

        pub fn len(self: Self) usize {
            return self.index_buf.items.len / 4;
        }

        pub fn scan(self: Self, index: *usize) ?Value {
            var message_idx = self.len();
            while (message_idx > 0) {
                message_idx -= 1;
                const message = Message {
                    .buf = messageSlice(self.buf.items,
                        self.index_buf.items,
                        message_idx),
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

        fn messageSlice(buf: []u8, index_buf: []u8, index: usize) []u8 {
            if (index > 0) {
                const start_end_slice = @ptrCast(*const [8]u8,
                    index_buf[(index - 1) * 4..(index + 1) * 4].ptr);
                const start = std.mem.readIntLittle(u32, start_end_slice[0..4]);
                const end = std.mem.readIntLittle(u32, start_end_slice[4..8]);
                return buf[start..end];
            }
            const end_slice =
                @ptrCast(*const [4]u8, index_buf[index * 4..(index + 1) * 4].ptr);
            const end = std.mem.readIntLittle(u32, end_slice);
            return buf[0..end];
        }
    };
}

const RndGen = std.rand.DefaultPrng;

pub fn benchScanMessageLog() !void {
    const iterations = 1_000;
    const messages_len = 10_000;
    const allocator = std.heap.page_allocator;

    var messages_buf = std.ArrayListUnmanaged(u8){};
    var index_buf = std.ArrayListUnmanaged(u8){};
    var index_entry: [4]u8 = undefined;
    var rnd = RndGen.init(77_777);
    for (0..messages_len) |_| {
        try writeRandomMessage(allocator, &messages_buf, &rnd, messages_len);
        std.mem.writeIntLittle(u32,
            index_entry[0..],
            @intCast(u32, messages_buf.items.len));
        try index_buf.appendSlice(allocator, index_entry[0..]);
    }

    const start_open = std.time.microTimestamp();
    var message_log = try MessageLog(i64, i64FromBytes)
        .open(messages_buf, index_buf);
    defer message_log.deinit(allocator);
    const end_open = std.time.microTimestamp();
    std.log.err("Open MessageLog: {d} micros\n", .{ end_open - start_open });

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

    if (op != @enumToInt(OpCode.Delete)) {
        const value = rnd.random().int(i64);
        var value_buf: [8]u8 = undefined;
        std.mem.writeIntLittle(i64, value_buf[0..], value);
        try dst.appendSlice(allocator, value_buf[0..]);
    }
}

fn i64FromBytes(buf: *const [8]u8) i64 {
    return std.mem.readIntLittle(i64, buf);
}