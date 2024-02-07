//! Writes a segment to a blob. Behavior is undefined when the data fed to a Writer is not exactly
//! the same and in the same order as the data fed to the Plan used to create the Writer. A
//! `Writer` does not own the blob that it writes to. Manage the resource that is the blob
//! separately from the `Writer`. The blob must be open while the `Writer` is in use.
//!
//! Call `begin` to initialize the encoders and determine if any calls to `write` are necessary. If
//! `begin` returns true, call `write` with the same values in the same order that were passed to
//! the `Plan`. If `begin` returns false, do not call `write` because the data is already written
//! to the segment. Call `end` after all calls to `write` regardless of whether `begin` returned
//! true or false. If any of the functions return an error, the segment is possibly in an invalid
//! state.

const std = @import("std");
const debug = std.debug;
const io = std.io;
const math = std.math;
const mem = std.mem;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;
const ArrayListUnmanaged = std.ArrayListUnmanaged;
const Order = math.Order;

const sqlite = @import("../sqlite3.zig");
const Blob = sqlite.Blob;
const BlobSlice = Blob.BlobSlice;
const SqliteError = sqlite.errors.Error;
const ValueType = sqlite.ValueType;

const schema_mod = @import("../schema.zig");
const ColumnType = schema_mod.ColumnType;

const BlobHandle = @import("../BlobManager.zig").Handle;

const stripe = @import("../stripe.zig");
const Valid = stripe.Valid;

const Header = @import("Header.zig");
const Plan = @import("Planner.zig").Plan;
const PrimaryEncoder = @import("primary.zig").Encoder;

present: ?StripeWriter(stripe.Bool.Encoder),
length: ?StripeWriter(stripe.Int.Encoder),
primary: ?StripeWriter(PrimaryEncoder),

const Self = @This();

const BufferedBlobWriter = io.BufferedWriter(4096, Blob.BlobWriter(BlobSlice(Blob)).Writer);

fn StripeWriter(comptime Encoder: type) type {
    return struct {
        /// It is necessary to store this as a field because the `buf_writer` field references it
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

pub fn init(self: *Self, blob_handle: *const BlobHandle, plan: *const Plan) !void {
    try plan.header.write(&blob_handle.blob);

    // In `StripeWriter`, the `writer` field references the `blob_writer` field, so initialize the
    // writers so that `blob_writer` stays in-place in memory

    var offset: u32 = Header.encoded_len;

    if (plan.present) |e| {
        self.present = .{ .blob_writer = undefined, .buf_writer = undefined, .encoder = undefined };
        self.present.?.init(blob_handle.blob.sliceFrom(offset), e);
        offset += plan.header.present_stripe.byte_len;
    } else {
        self.present = null;
    }

    if (plan.length) |e| {
        self.length = .{ .blob_writer = undefined, .buf_writer = undefined, .encoder = undefined };
        self.length.?.init(blob_handle.blob.sliceFrom(offset), e);
        offset += plan.header.length_stripe.byte_len;
    } else {
        self.length = null;
    }

    if (plan.primary) |e| {
        self.primary = .{ .blob_writer = undefined, .buf_writer = undefined, .encoder = undefined };
        self.primary.?.init(blob_handle.blob.sliceFrom(offset), e);
    } else {
        self.primary = null;
    }
}

/// If an error is returned, the Writer can no longer be used and the segment must be destroyed
pub fn begin(self: *Self) !bool {
    // If none of the encoders return true for continue, that means that all encoders are done and
    // `write` does not need to be called at all.
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

/// If an error is returned, the Writer can no longer be used and the segment must be destroyed
/// TODO allow writing primitive types directly without being wrapped in an interface
pub fn write(self: *Self, value: anytype) !void {
    const Value = @TypeOf(value);
    switch (@typeInfo(Value)) {
        .Int => {
            if (self.present) |*present| {
                try present.encoder.write(present.writer(), true);
            }
            const primary = &self.primary.?;
            try primary.encoder.int.write(primary.writer(), value);
        },
        else => {
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
                    .bool => |*e| try e.write(primary.writer(), value.asBool()),
                    .int => |*e| try e.write(primary.writer(), value.asI64()),
                    .float => |*e| try e.write(primary.writer(), value.asF64()),
                    .byte => |*byte_encoder| {
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
        },
    }
}

/// Finalizes all writes to the segment and closes the writer. If an error is returned, the Writer
/// can no longer be used and the segment must be destroyed
pub fn end(self: *Self) !void {
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
}

test "segment: writer" {
    const VtabCtxSchemaless = @import("../ctx.zig").VtabCtxSchemaless;
    const BlobManager = @import("../BlobManager.zig");
    const MemoryValue = @import("../value.zig").MemoryValue;

    const conn = try @import("../sqlite3.zig").Conn.openInMemory();
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
    const direct = @import("../stripe/encode/direct.zig");
    const encoder = direct.Encoder(i64, stripe.Int.writeDirect).init();
    const plan = Plan{
        .header = header,
        .present = null,
        .length = null,
        .primary = .{ .int = .{ .direct = encoder } },
    };

    var arena = ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const ctx = VtabCtxSchemaless{
        .conn_ = conn,
        .vtab_name = "test",
    };

    var blob_manager = try BlobManager.init(&arena, &arena, &ctx);
    defer blob_manager.deinit();

    var blob_handle = try blob_manager.create(&arena, plan.totalLen());
    defer blob_handle.tryClose();

    var writer: Self = undefined;
    try writer.init(&blob_handle, &plan);

    const cont = try writer.begin();
    try std.testing.expect(cont);
    for (0..10) |v| {
        try writer.write(MemoryValue{
            .Integer = @as(i64, @intCast(v)),
        });
    }
    try writer.end();
}
