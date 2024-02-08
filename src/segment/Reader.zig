//! Reads data from a segment. A `Reader` does not own the blob that it writes to. Manage the
//! resource that is the blob separately from the `Reader`. The blob must be open while the
//! `Reader` is in use.
//!
//! TODO it would likely be more efficient to make this an union(enum) at the top level so that
//!      there is a single branch per function call

const std = @import("std");
const debug = std.debug;
const io = std.io;
const log = std.log;
const math = std.math;
const mem = std.mem;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;
const ArrayListUnmanaged = std.ArrayListUnmanaged;
const Order = math.Order;

const sqlite = @import("../sqlite3.zig");
const Blob = sqlite.Blob;
const BlobSlice = sqlite.Blob.BlobSlice;
const Conn = sqlite.Conn;

const schema_mod = @import("../schema.zig");
const DataType = schema_mod.ColumnType.DataType;

const BlobHandle = @import("../BlobManager.zig").Handle;

const stripe = @import("../stripe.zig");
const Valid = stripe.Valid;

const Header = @import("Header.zig");
const PrimaryDecoder = @import("primary.zig").Decoder;
const Value = @import("Value.zig");

data_type: DataType,
present: ?StripeReader(stripe.Bool.Decoder),
length: ?StripeReader(stripe.Int.Decoder),
primary: ?StripeReader(PrimaryDecoder),

const Self = @This();

fn StripeReader(comptime Decoder: type) type {
    return struct {
        blob: BlobSlice(Blob),
        decoder: Decoder,
    };
}

pub fn init(blob_handle: *const BlobHandle, data_type: DataType) !Self {
    const header = try Header.read(blob_handle.blob);
    var present: ?StripeReader(stripe.Bool.Decoder) = null;
    var length: ?StripeReader(stripe.Int.Decoder) = null;
    var primary: ?StripeReader(PrimaryDecoder) = null;

    var offset: u32 = Header.encoded_len;
    if (header.present_stripe.byte_len > 0) {
        const slice = blob_handle.blob.sliceFrom(offset);
        var decoder = try stripe.Bool.Decoder.init(header.present_stripe.encoding);
        try decoder.begin(slice);
        present = .{ .blob = slice, .decoder = decoder };
        offset += header.present_stripe.byte_len;
    }
    if (header.length_stripe.byte_len > 0) {
        const slice = blob_handle.blob.sliceFrom(offset);
        var decoder = try stripe.Int.Decoder.init(header.length_stripe.encoding);
        try decoder.begin(slice);
        length = .{ .blob = slice, .decoder = decoder };
        offset += header.length_stripe.byte_len;
    }
    if (header.primary_stripe.byte_len > 0) {
        const slice = blob_handle.blob.sliceFrom(offset);
        const encoding = header.primary_stripe.encoding;
        var decoder: PrimaryDecoder = switch (data_type) {
            .Boolean => .{ .bool = try stripe.Bool.Decoder.init(encoding) },
            .Integer => .{ .int = try stripe.Int.Decoder.init(encoding) },
            .Float => .{ .float = try stripe.Float.Decoder.init(encoding) },
            .Text, .Blob => .{ .byte = try stripe.Byte.Decoder.init(encoding) },
        };
        switch (decoder) {
            inline else => |*d| try d.begin(slice),
        }
        primary = .{ .blob = slice, .decoder = decoder };
    }

    return .{
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
            .byte => |*d| {
                const len_stripe = &self.length.?;
                const length = try len_stripe.decoder.read(len_stripe.blob);
                len_stripe.decoder.next(1);
                d.next(@intCast(length));
            },
            inline else => |*d| d.next(1),
        }
    }
}

/// The value will use the supplied buffer and allocator if necessary. The buffer must be managed
/// by the caller.
pub fn read(self: *Self, allocator: Allocator) !Value {
    var present = true;
    if (self.present) |*pres_stripe| {
        present = try pres_stripe.decoder.read(pres_stripe.blob);
    }

    var primary: ?Value.PrimaryValue = null;
    if (present) {
        const prim_stripe = &self.primary.?;
        switch (prim_stripe.decoder) {
            .bool => |*d| primary = .{ .bool = try d.read(prim_stripe.blob) },
            .int => |*d| primary = .{ .int = try d.read(prim_stripe.blob) },
            .float => |*d| primary = .{ .float = try d.read(prim_stripe.blob) },
            .byte => |*d| {
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

    var primary: ?Value.PrimaryValue = null;
    if (present) {
        const prim_stripe = &self.primary.?;
        switch (prim_stripe.decoder) {
            .bool => |*d| primary = .{ .bool = try d.read(prim_stripe.blob) },
            .int => |*d| primary = .{ .int = try d.read(prim_stripe.blob) },
            .float => |*d| primary = .{ .float = try d.read(prim_stripe.blob) },
            .byte => @panic("allocation required to read value"),
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
            .bool => |*d| result.setBool(try d.read(prim_stripe.blob)),
            .int => |*d| result.setI64(try d.read(prim_stripe.blob)),
            .float => |*d| result.setF64(try d.read(prim_stripe.blob)),
            .byte => |*d| {
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

test "segment: reader" {
    const VtabCtxSchemaless = @import("../ctx.zig").VtabCtxSchemaless;
    const BlobManager = @import("../BlobManager.zig");
    const MemoryValue = @import("../value.zig").MemoryValue;
    const Plan = @import("Planner.zig").Plan;
    const Writer = @import("Writer.zig");

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

    var blob_manager = try BlobManager.init(&arena, &ctx);
    defer blob_manager.deinit();
    try blob_manager.table().create(&arena);

    var blob_handle = try blob_manager.create(&arena, plan.totalLen());
    defer blob_handle.tryClose();

    var writer: Writer = undefined;
    try writer.init(&blob_handle, &plan);

    const cont = try writer.begin();
    try std.testing.expect(cont);
    for (0..10) |v| {
        try writer.write(MemoryValue{
            .Integer = @as(i64, @intCast(v)),
        });
    }
    try writer.end();

    var bytes_buf = ArrayListUnmanaged(u8){};
    defer bytes_buf.deinit(std.testing.allocator);

    var reader = try Self.init(&blob_handle, .Integer);
    for (0..10) |idx| {
        const value = try reader.read(std.testing.allocator);
        try std.testing.expectEqual(idx, @as(usize, @intCast(value.asI64())));
        try reader.next();
    }
}
