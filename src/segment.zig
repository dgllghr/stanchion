const std = @import("std");
const debug = std.debug;
const io = std.io;
const math = std.math;
const mem = std.mem;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;
const ArrayListUnmanaged = std.ArrayListUnmanaged;
const Order = math.Order;

const sqlite = @import("sqlite3.zig");
const Blob = sqlite.Blob;
const BlobSlice = Blob.BlobSlice;
const SqliteError = sqlite.errors.Error;
const ValueType = sqlite.ValueType;

const schema_mod = @import("schema.zig");
const ColumnType = schema_mod.ColumnType;

const stripe = @import("stripe.zig");
const Valid = stripe.Valid;

pub const Db = @import("segment/Db.zig");
const Header = @import("segment/Header.zig");

pub const Handle = Db.Handle;

/// A Planner analyzes the data that will go into a segment and determines the layout for
/// the segment. The layout includes the size of the segment and the sizes and encodings
/// of the stripes within the segment. Create a Planner and feed the data that will go
/// into the segment by calling `next` on the values that will go into the segment (in
/// order). Then call `end` to create a `Plan`, which contains the layout and encoders
/// for a segment. Use the `Plan` to create a `Writer`, which writes the segment.
pub const Planner = struct {
    const Self = @This();

    column_type: ColumnType,
    present: stripe.Bool.Validator,
    length: ?stripe.Int.Validator,
    primary: PrimaryValidator,

    const PrimaryValidator = union(enum) {
        Bool: stripe.Bool.Validator,
        Byte: stripe.Byte.Validator,
        Int: stripe.Int.Validator,
        Float: stripe.Float.Validator,
    };

    pub fn init(column_type: ColumnType) Self {
        var length: ?stripe.Int.Validator = null;
        var primary: PrimaryValidator = undefined;
        switch (column_type.data_type) {
            .Boolean => primary = .{ .Bool = stripe.Bool.Validator.init() },
            .Integer => primary = .{ .Int = stripe.Int.Validator.init() },
            .Float => primary = .{ .Float = stripe.Float.Validator.init() },
            .Blob, .Text => {
                length = stripe.Int.Validator.init();
                primary = .{ .Byte = stripe.Byte.Validator.init() };
            },
        }
        return .{
            .column_type = column_type,
            .present = stripe.Bool.Validator.init(),
            .length = length,
            .primary = primary,
        };
    }

    pub fn reset(self: *Self) void {
        self.* = init(self.column_type);
    }

    pub fn next(self: *Self, value: anytype) void {
        if (value.isNull()) {
            self.present.next(false);
            return;
        }

        self.present.next(true);
        switch (self.column_type.data_type) {
            .Boolean => self.primary.Bool.next(value.asBool()),
            .Integer => self.primary.Int.next(value.asI64()),
            .Float => self.primary.Float.next(value.asF64()),
            .Text => {
                const text = value.asText();
                self.length.?.next(@as(i64, @intCast(text.len)));
                for (text) |b| {
                    self.primary.Byte.next(b);
                }
            },
            .Blob => {
                const blob = value.asBlob();
                self.length.?.next(@as(i64, @intCast(blob.len)));
                for (blob) |b| {
                    self.primary.Byte.next(b);
                }
            },
        }
    }

    pub fn end(self: *Self) !Plan {
        var header = Header.init();

        var present_encoder: ?stripe.Bool.Encoder = null;
        const present = try self.present.end();
        // TODO errdefer

        // Optimization: skip the present stripe if all values present (no nulls) and
        // skip the primary stripe if no values present (all nulls)
        // TODO ensure Constant encoding is always chosen when all values are the same
        //      (not guaranteed for a number of booleans that fits in a bit-packed byte)
        var all_nulls = false;
        var no_nulls = false;
        if (present.meta.encoding == .Constant) {
            no_nulls = present.encoder.constant.value;
            all_nulls = !no_nulls;
        }

        // All values present (no nulls) => skip the present stripe
        if (!no_nulls) {
            header.present_stripe = present.meta;
            present_encoder = present.encoder;
        }

        var length_encoder: ?stripe.Int.Encoder = null;
        if (self.length) |*length_validator| {
            const length = try length_validator.end();
            // TODO errdefer
            header.length_stripe = length.meta;
            length_encoder = length.encoder;
        }

        var primary_encoder: ?PrimaryEncoder = null;
        // All values not present (all nulls) => omit the primary stripe
        if (!all_nulls) {
            switch (self.primary) {
                inline else => |*v, tag| {
                    const prim = try v.end();
                    header.primary_stripe = prim.meta;
                    primary_encoder = @unionInit(
                        PrimaryEncoder,
                        @tagName(tag),
                        prim.encoder,
                    );
                },
            }
        }

        return .{
            .header = header,
            .present = present_encoder,
            .length = length_encoder,
            .primary = primary_encoder,
        };
    }
};

/// Specifies the layout of a segment and contains the encoders that are used to write
/// that segment.
pub const Plan = struct {
    header: Header,
    present: ?stripe.Bool.Encoder,
    length: ?stripe.Int.Encoder,
    primary: ?PrimaryEncoder,
};

/// Writes a segment to a blob. Behavior is undefined when the data fed to a Writer is
/// not exactly the same and in the same order as the data fed to the Planner that
/// generated the Plan used to create the Writer.
///
/// To use a Writer, call `allocate` with a `Plan` created by a `Planner`. Call `begin`
/// to initialize the encoders and determine if any calls to `write` are necessary. If
/// `begin` returns true, call `write` with the same values in the same order that were
/// passed to the `Planner`. If `begin` returns false, do not call `write` because the
/// data is already written to the segment. Call `end` after all calls to `write`
/// regardless of whether `begin` returned true or false. If any of the functions return
/// an error, the segment is possibly in an invalid state. Call `free` to release all
/// segment resources.
pub const Writer = struct {
    handle: Handle,
    present: ?StripeWriter(stripe.Bool.Encoder),
    length: ?StripeWriter(stripe.Int.Encoder),
    primary: ?StripeWriter(PrimaryEncoder),

    const Self = @This();

    const BufferedBlobWriter = io.BufferedWriter(4096, Blob.BlobWriter(BlobSlice(Blob)).Writer);

    fn StripeWriter(comptime Encoder: type) type {
        return struct {
            /// It is necessary to store this as a field because the `writer` field references it
            blob_writer: Blob.BlobWriter(BlobSlice(Blob)),
            /// References `blob_writer`
            buf_writer: BufferedBlobWriter,
            encoder: Encoder,

            fn init(self: *@This(), slice: BlobSlice(Blob), encoder: Encoder) void {
                self.blob_writer = slice.writer();
                self.buf_writer = io.bufferedWriter(self.blob_writer.writer());
                self.encoder = encoder;
            }

            fn writer(self: *@This()) BufferedBlobWriter.Writer {
                return self.buf_writer.writer();
            }
        };
    }

    /// Creates an empty segment and initializes the writer to write to that segment
    pub fn openCreate(self: *Self, tmp_arena: *ArenaAllocator, db: *Db, plan: Plan) !void {
        const len = Header.encoded_len + plan.header.totalStripesLen();
        self.handle = try db.allocate(tmp_arena, len);
        errdefer db.free(tmp_arena, self.handle) catch {};
        try plan.header.write(&self.handle.blob);

        // In `StripeWriter`, the `writer` field references the `blob_writer` field, so initialize
        // the writers so that `blob_writer` stays in-place in memory

        var offset: u32 = Header.encoded_len;
        if (plan.present) |e| {
            self.present = .{ .blob_writer = undefined, .buf_writer = undefined, .encoder = undefined };
            self.present.?.init(self.handle.blob.sliceFrom(offset), e);
            offset += plan.header.present_stripe.byte_len;
        } else {
            self.present = null;
        }

        if (plan.length) |e| {
            self.length = .{ .blob_writer = undefined, .buf_writer = undefined, .encoder = undefined };
            self.length.?.init(self.handle.blob.sliceFrom(offset), e);
            offset += plan.header.length_stripe.byte_len;
        } else {
            self.length = null;
        }

        if (plan.primary) |e| {
            self.primary = .{ .blob_writer = undefined, .buf_writer = undefined, .encoder = undefined };
            self.primary.?.init(self.handle.blob.sliceFrom(offset), e);
            offset += plan.header.present_stripe.byte_len;
        } else {
            self.primary = null;
        }
    }

    /// If an error is returned, the Writer can no longer be used and the blob must be
    /// freed using the `handle`.
    pub fn begin(self: *Self) !bool {
        // If none of the encoders return true for continue, that means that all encoders
        // are done and `write` does not need to be called at all.
        var cont = false;
        if (self.present) |*s| {
            const c = try s.encoder.begin(s.writer());
            cont = cont or c;
        }
        if (self.length) |*s| {
            const c = try s.encoder.begin(s.writer());
            cont = cont or c;
        }
        if (self.primary) |*s| {
            switch (s.encoder) {
                inline else => |*e| {
                    const c = try e.begin(s.writer());
                    cont = cont or c;
                },
            }
        }
        return cont;
    }

    /// If an error is returned, the Writer can no longer be used and the blob must be
    /// freed using the `handle`.
    /// TODO allow writing primitive types directly without being wrapped in an interface
    pub fn write(self: *Self, value: anytype) !void {
        const value_type = value.valueType();

        if (value_type == .Null) {
            try self.present.?.encoder.write(self.present.?.writer(), false);
            return;
        }

        if (self.present) |*present| {
            try present.encoder.write(present.writer(), true);
        }

        if (self.primary) |*primary| {
            switch (primary.encoder) {
                .Bool => |*e| try e.write(primary.writer(), value.asBool()),
                .Int => |*e| try e.write(primary.writer(), value.asI64()),
                .Float => |*e| try e.write(primary.writer(), value.asF64()),
                .Byte => |*byte_encoder| {
                    const bytes =
                        if (value_type == .Text) value.asText() else value.asBlob();
                    var length = &self.length.?;
                    try length.encoder.write(length.writer(), @as(i64, @intCast(bytes.len)));
                    for (bytes) |b| {
                        try byte_encoder.write(primary.writer(), b);
                    }
                },
            }
        }
    }

    /// If an error is returned, the Writer can no longer be used and the blob must be freed using
    /// the `handle`.
    pub fn end(self: *Self) !Handle {
        if (self.present) |*present| {
            try present.encoder.end(present.writer());
            try present.buf_writer.flush();
        }
        if (self.length) |*length| {
            try length.encoder.end(length.writer());
            try length.buf_writer.flush();
        }
        if (self.primary) |*primary| {
            switch (primary.encoder) {
                inline else => |*e| try e.end(primary.writer()),
            }
            try primary.buf_writer.flush();
        }
        return self.handle;
    }
};

const PrimaryEncoder = union(enum) {
    Bool: stripe.Bool.Encoder,
    Byte: stripe.Byte.Encoder,
    Int: stripe.Int.Encoder,
    Float: stripe.Float.Encoder,
};

test "segment planner" {
    const MemoryValue = @import("value.zig").MemoryValue;

    var planner = Planner.init(.{
        .data_type = .Integer,
        .nullable = true,
    });
    for (0..10) |v| {
        planner.next(MemoryValue{
            .Integer = @as(i64, @intCast(v)),
        });
    }

    _ = try planner.end();
}

test "segment writer" {
    const MemoryValue = @import("value.zig").MemoryValue;

    const conn = try @import("sqlite3/Conn.zig").openInMemory();
    defer conn.close();

    const header = Header{
        .present_stripe = .{
            .byte_len = 0,
            .encoding = undefined,
        },
        .length_stripe = .{
            .byte_len = 0,
            .encoding = undefined,
        },
        .primary_stripe = .{
            .byte_len = 80,
            .encoding = .Direct,
        },
    };
    const direct = @import("stripe/encode/direct.zig");
    const encoder = direct.Encoder(i64, stripe.Int.writeDirect).init();
    const plan = Plan{
        .header = header,
        .present = null,
        .length = null,
        .primary = .{ .Int = .{ .direct = encoder } },
    };

    var arena = ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    try Db.createTable(&arena, conn, "test");
    var db = try Db.init(&arena, conn, "test");
    defer db.deinit();

    var writer = try arena.allocator().create(Writer);
    try writer.openCreate(&arena, &db, plan);
    //var writer = try Writer.allocate(&arena, &db, plan);
    errdefer db.free(&arena, writer.handle) catch {};

    const cont = try writer.begin();
    try std.testing.expect(cont);
    for (0..10) |v| {
        try writer.write(MemoryValue{
            .Integer = @as(i64, @intCast(v)),
        });
    }
    _ = try writer.end();
}

/// TODO it would likely be more efficient to make this an union(enum) at the top level
///      so that there is a single branch per function call
pub const Reader = struct {
    const Self = @This();

    handle: Handle,
    data_type: ColumnType.DataType,
    present: ?StripeReader(stripe.Bool.Decoder),
    length: ?StripeReader(stripe.Int.Decoder),
    primary: ?StripeReader(PrimaryDecoder),

    fn StripeReader(comptime Decoder: type) type {
        return struct {
            blob: BlobSlice(Blob),
            decoder: Decoder,
        };
    }

    pub fn open(db: *Db, data_type: ColumnType.DataType, segment_id: i64) !Self {
        var handle = try db.open(segment_id);
        errdefer handle.close();

        const header = try Header.read(handle.blob);
        var present: ?StripeReader(stripe.Bool.Decoder) = null;
        var length: ?StripeReader(stripe.Int.Decoder) = null;
        var primary: ?StripeReader(PrimaryDecoder) = null;

        var offset: u32 = Header.encoded_len;
        if (header.present_stripe.byte_len > 0) {
            const blob = handle.blob.sliceFrom(offset);
            var decoder = try stripe.Bool.Decoder.init(
                header.present_stripe.encoding,
            );
            try decoder.begin(blob);
            present = .{ .blob = blob, .decoder = decoder };
            offset += header.present_stripe.byte_len;
        }
        if (header.length_stripe.byte_len > 0) {
            const blob = handle.blob.sliceFrom(offset);
            var decoder = try stripe.Int.Decoder.init(
                header.length_stripe.encoding,
            );
            try decoder.begin(blob);
            length = .{ .blob = blob, .decoder = decoder };
            offset += header.length_stripe.byte_len;
        }
        if (header.primary_stripe.byte_len > 0) {
            const blob = handle.blob.sliceFrom(offset);
            var decoder = try PrimaryDecoder.init(
                data_type,
                header.primary_stripe.encoding,
            );
            try decoder.begin(blob);
            primary = .{ .blob = blob, .decoder = decoder };
        }

        return .{
            .handle = handle,
            .data_type = data_type,
            .present = present,
            .length = length,
            .primary = primary,
        };
    }

    pub fn next(self: *Self) !void {
        var present = true;
        if (self.present) |*pres_stripe| {
            present = try pres_stripe.decoder.read(pres_stripe.blob);
            pres_stripe.decoder.next(1);
        }

        if (present) {
            const prim_stripe = &self.primary.?;
            switch (prim_stripe.decoder) {
                .Byte => |*d| {
                    const len_stripe = &self.length.?;
                    const length = try len_stripe.decoder.read(len_stripe.blob);
                    len_stripe.decoder.next(1);
                    d.next(@intCast(length));
                },
                inline else => |*d| d.next(1),
            }
        }
    }

    /// The value will use the supplied buffer and allocator if necessary. The buffer
    /// must be managed by the caller.
    pub fn read(self: *Self, allocator: Allocator) !Value {
        var present = true;
        if (self.present) |*pres_stripe| {
            present = try pres_stripe.decoder.read(pres_stripe.blob);
        }

        var primary: ?PrimaryValue = null;
        if (present) {
            const prim_stripe = &self.primary.?;
            switch (prim_stripe.decoder) {
                .Bool => |*d| primary = .{ .bool = try d.read(prim_stripe.blob) },
                .Int => |*d| primary = .{ .int = try d.read(prim_stripe.blob) },
                .Float => |*d| primary = .{ .float = try d.read(prim_stripe.blob) },
                .Byte => |*d| {
                    var len_stripe = &self.length.?;
                    const length = try len_stripe.decoder.read(len_stripe.blob);
                    // TODO cache the length for use in `next`?
                    const buf = try allocator.alloc(u8, @intCast(length));
                    try d.readAll(buf, prim_stripe.blob);
                    primary = .{ .bytes = buf };
                },
            }
        }

        return .{ .data_type = self.data_type, .primary = primary };
    }

    /// Panics if the value being read requires allocation
    pub fn readNoAlloc(self: *Self) !Value {
        var present = true;
        if (self.present) |*pres_stripe| {
            present = try pres_stripe.decoder.read(pres_stripe.blob);
        }

        var primary: ?PrimaryValue = null;
        if (present) {
            const prim_stripe = &self.primary.?;
            switch (prim_stripe.decoder) {
                .Bool => |*d| primary = .{ .bool = try d.read(prim_stripe.blob) },
                .Int => |*d| primary = .{ .int = try d.read(prim_stripe.blob) },
                .Float => |*d| primary = .{ .float = try d.read(prim_stripe.blob) },
                .Byte => @panic("allocation required to read value"),
            }
        }

        return .{ .data_type = self.data_type, .primary = primary };
    }

    pub fn readInto(
        self: *Self,
        allocator: Allocator,
        result: anytype,
    ) !void {
        var present = true;
        if (self.present) |*pres_stripe| {
            present = try pres_stripe.decoder.read(pres_stripe.blob);
        }

        if (present) {
            const prim_stripe = &self.primary.?;
            switch (prim_stripe.decoder) {
                .Bool => |*d| result.setBool(try d.read(prim_stripe.blob)),
                .Int => |*d| result.setI64(try d.read(prim_stripe.blob)),
                .Float => |*d| result.setF64(try d.read(prim_stripe.blob)),
                .Byte => |*d| {
                    var len_stripe = &self.length.?;
                    const length = try len_stripe.decoder.read(len_stripe.blob);
                    // TODO cache the length for use in `next`?
                    const buf = try allocator.alloc(u8, @intCast(length));
                    try d.readAll(buf, prim_stripe.blob);
                    switch (self.data_type) {
                        .Blob => result.setBlob(buf),
                        .Text => result.setText(buf),
                        else => unreachable,
                    }
                },
            }
        }
    }
};

// TODO remove enum and use data_type from reader
const PrimaryDecoder = union(enum) {
    const Self = @This();

    Bool: stripe.Bool.Decoder,
    Byte: stripe.Byte.Decoder,
    Int: stripe.Int.Decoder,
    Float: stripe.Float.Decoder,

    fn init(data_type: ColumnType.DataType, encoding: stripe.Encoding) !Self {
        return switch (data_type) {
            .Boolean => .{ .Bool = try stripe.Bool.Decoder.init(encoding) },
            .Integer => .{ .Int = try stripe.Int.Decoder.init(encoding) },
            .Float => .{ .Float = try stripe.Float.Decoder.init(encoding) },
            .Text, .Blob => .{ .Byte = try stripe.Byte.Decoder.init(encoding) },
        };
    }

    fn begin(self: *Self, blob: anytype) !void {
        switch (self.*) {
            inline else => |*d| try d.begin(blob),
        }
    }
};

test "segment reader" {
    const MemoryValue = @import("value.zig").MemoryValue;

    const conn = try @import("sqlite3/Conn.zig").openInMemory();
    defer conn.close();

    const header = Header{
        .present_stripe = .{
            .byte_len = 0,
            .encoding = undefined,
        },
        .length_stripe = .{
            .byte_len = 0,
            .encoding = undefined,
        },
        .primary_stripe = .{
            .byte_len = 80,
            .encoding = .Direct,
        },
    };
    const direct = @import("stripe/encode/direct.zig");
    const encoder = direct.Encoder(i64, stripe.Int.writeDirect).init();
    const plan = Plan{
        .header = header,
        .present = null,
        .length = null,
        .primary = .{ .Int = .{ .direct = encoder } },
    };

    var arena = ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    try Db.createTable(&arena, conn, "test");
    var db = try Db.init(&arena, conn, "test");
    defer db.deinit();
    var writer = try arena.allocator().create(Writer);
    try writer.openCreate(&arena, &db, plan);
    defer db.free(&arena, writer.handle) catch {};

    const cont = try writer.begin();
    try std.testing.expect(cont);
    for (0..10) |v| {
        try writer.write(MemoryValue{
            .Integer = @as(i64, @intCast(v)),
        });
    }
    const handle = try writer.end();

    var bytes_buf = ArrayListUnmanaged(u8){};
    defer bytes_buf.deinit(std.testing.allocator);

    var reader = try Reader.open(&db, .Integer, handle.id);
    for (0..10) |idx| {
        const value = try reader.read(std.testing.allocator);
        try std.testing.expectEqual(idx, @intCast(value.asI64()));
        try reader.next();
    }
}

const PrimaryValue = union {
    bool: bool,
    int: i64,
    float: f64,
    bytes: []const u8,
};

pub const Value = struct {
    const Self = @This();

    data_type: ColumnType.DataType,
    primary: ?PrimaryValue,

    pub fn valueType(self: Self) ValueType {
        if (self.isNull()) {
            return .Null;
        }

        return switch (self.data_type) {
            .Boolean => .Integer,
            .Integer => .Integer,
            .Float => .Float,
            .Text => .Text,
            .Blob => .Blob,
        };
    }

    pub fn isNull(self: Self) bool {
        return self.primary == null;
    }

    pub fn asBool(self: Self) bool {
        return self.primary.?.bool;
    }

    pub fn asI32(self: Self) i32 {
        return @intCast(self.primary.?.int);
    }

    pub fn asI64(self: Self) i64 {
        return self.primary.?.int;
    }

    pub fn asF64(self: Self) f64 {
        return self.primary.?.float;
    }

    pub fn asBlob(self: Self) []const u8 {
        return self.primary.?.bytes;
    }

    pub fn asText(self: Self) []const u8 {
        return self.primary.?.bytes;
    }

    pub fn compare(self: Self, other: anytype) Order {
        // This isn't correct by the sql definition
        if (self.isNull()) {
            return .eq;
        }

        return switch (self.data_type) {
            .Boolean => math.order(@intFromBool(self.asBool()), @intFromBool(other.asBool())),
            .Integer => math.order(self.asI64(), other.asI64()),
            .Float => math.order(self.asF64(), other.asF64()),
            .Text => mem.order(u8, self.asText(), other.asText()),
            .Blob => mem.order(u8, self.asBlob(), other.asBlob()),
        };
    }
};
