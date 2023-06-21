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

        index: ArrayListUnmanaged(usize),
        buf: ArrayListUnmanaged(u8),
        write_end: usize,

        const Message = struct {
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

        pub fn open(allocator: Allocator, buf: ArrayListUnmanaged(u8)) !Self {
            const write_end = buf.items.len;
            const index = try buildIndex(allocator, buf);
            return .{
                .index = index,
                .buf = buf,
                .write_end = write_end,
            };
        }

        pub fn deinit(self: *Self, allocator: Allocator) void {
            self.index.deinit(allocator);
            self.buf.deinit(allocator);
        }

        pub fn len(self: Self) usize {
            return self.index.items.len;
        }

        pub fn scan(self: Self, index: *usize) ?Value {
            var message_idx = self.len();
            while (message_idx > 0) {
                message_idx -= 1;
                const start = if (message_idx > 0) self.index.items[message_idx - 1] else 0;
                const end = self.index.items[message_idx];
                const message = Message { .buf = self.buf.items[start..end] };
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

        fn buildIndex(
            allocator: Allocator,
            buf: ArrayListUnmanaged(u8),
        ) !ArrayListUnmanaged(usize) {
            var index = try ArrayListUnmanaged(usize)
                .initCapacity(allocator, @divFloor(buf.items.len, 5));
            var idx: usize = 0;
            while (idx < buf.items.len) {
                const op = @intToEnum(OpCode, buf.items[idx]);
                switch (op) {
                    .Insert => {
                        idx += 5 + @sizeOf(Value);
                    },
                    .Update => {
                        idx += 5 + @sizeOf(Value);
                    },
                    .Delete => {
                        idx += 5;
                    },
                }
                index.appendAssumeCapacity(idx);
            }
            return index;
        }
    };
}

const RndGen = std.rand.DefaultPrng;

pub fn benchScanMessageLog() !void {
    const iterations = 1_000;
    const messages_len = 10_000;
    const allocator = std.heap.page_allocator;

    var messages_buf = std.ArrayListUnmanaged(u8){};
    var rnd = RndGen.init(77_777);
    for (0..messages_len) |_| {
        try writeRandomMessage(allocator, &messages_buf, &rnd, messages_len);
    }

    const start_open = std.time.microTimestamp();
    var message_log = try MessageLog(i64, i64FromBytes)
        .open(allocator, messages_buf);
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