//! Table valued function that returns all the segments in a table

const std = @import("std");
const fmt = std.fmt;
const mem = std.mem;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const sqlite = @import("../sqlite3.zig");
const Conn = sqlite.Conn;
const vtab = sqlite.vtab;
const BestIndexError = vtab.BestIndexError;
const BestIndexInfo = vtab.BestIndexInfo;
const Result = vtab.Result;

const VtabCtxSchemaless = @import("../ctx.zig").VtabCtxSchemaless;

const schema_mod = @import("../schema.zig");
const ColumnType = schema_mod.ColumnType;
const Schema = schema_mod.Schema;
const SchemaManager = schema_mod.Manager;

const BlobManager = @import("../BlobManager.zig");

const stripe = @import("../stripe.zig");
const StripeMeta = stripe.Meta;

const segment = @import("../segment.zig");
const SegmentHeader = segment.Header;

allocator: Allocator,
conn: Conn,

const Self = @This();

pub fn connect(
    self: *Self,
    allocator: Allocator,
    conn: Conn,
    _: *vtab.CallbackContext,
    _: []const []const u8,
) !void {
    self.allocator = allocator;
    self.conn = conn;
}

pub fn disconnect(_: *Self) void {}

pub fn ddl(_: *Self, allocator: Allocator) ![:0]const u8 {
    return fmt.allocPrintZ(
        allocator,
        \\CREATE TABLE stanchion_segment_info (
        \\  stripe_index INTEGER NOT NULL,
        \\  content_type TEXT NOT NULL,
        \\  data_type TEXT NOT NULL,
        \\  encoding_code INTEGER NOT NULL,
        \\  encoding TEXT NOT NULL,
        \\  size_bytes INTEGER NOT NULL,
        \\
        \\  table_name TEXT HIDDEN,
        \\  segment_id INTEGER HIDDEN
        \\)
    ,
        .{},
    );
}

pub fn bestIndex(
    _: *Self,
    _: *vtab.CallbackContext,
    best_index_info: vtab.BestIndexInfo,
) BestIndexError!void {
    var req_constraints: struct { table_name: bool, segment_id: bool } = .{
        .table_name = false,
        .segment_id = false,
    };

    // Find the equality constraint on `table_name` and `segment_id`
    for (0..best_index_info.constraintsLen()) |idx| {
        const c = best_index_info.constraint(idx);
        if (c.usable()) {
            if (c.columnIndex() == 6 and c.op() == .eq) {
                // column index 6 is the `table_name` hidden column
                c.setOmitTest(true);
                c.includeArgInFilter(1);
                req_constraints.table_name = true;
            } else if (c.columnIndex() == 7 and c.op() == .eq) {
                // column index 7 is the `segment_id` hidden column
                c.setOmitTest(true);
                c.includeArgInFilter(2);
                req_constraints.segment_id = true;
            }
        }
    }

    if (req_constraints.table_name and req_constraints.segment_id) {
        return;
    }
    return BestIndexError.QueryImpossible;
}

pub fn open(
    self: *Self,
    _: *vtab.CallbackContext,
) !Cursor {
    return Cursor.init(self.allocator, self.conn);
}

pub const Cursor = struct {
    lifetime_arena: ArenaAllocator,

    vtab_ctx: VtabCtxSchemaless,

    segment_id: i64,
    column_type: ColumnType,
    header: SegmentHeader,

    index: i64,
    defined_index: i64,

    pub fn init(allocator: Allocator, conn: Conn) Cursor {
        return .{
            .lifetime_arena = ArenaAllocator.init(allocator),
            .vtab_ctx = VtabCtxSchemaless.init(conn, undefined),
            .segment_id = undefined,
            .column_type = undefined,
            .header = undefined,
            .index = 0,
            .defined_index = 1,
        };
    }

    pub fn deinit(self: *Cursor) void {
        self.lifetime_arena.deinit();
    }

    pub fn begin(
        self: *Cursor,
        cb_ctx: *vtab.CallbackContext,
        _: i32,
        _: [:0]const u8,
        filter_args: vtab.FilterArgs,
    ) !void {
        // TODO do not return an error if these types are wrong
        const table_name_ref = (try filter_args.readValue(0)).asText();
        self.vtab_ctx.vtab_name = try self.lifetime_arena.allocator()
            .dupe(u8, table_name_ref);
        const segment_id = (try filter_args.readValue(1)).asI64();

        var stmt = try self.vtab_ctx.conn().prepare(
            \\SELECT column_rank
            \\FROM stanchion_segments(?)
            \\WHERE segment_id = ?
            \\LIMIT 1
        );
        defer stmt.deinit();

        try stmt.bind(.Text, 1, self.vtab_ctx.vtabName());
        try stmt.bind(.Int64, 2, segment_id);
        const exists = try stmt.next();
        if (!exists) {
            return;
        }
        const col_rank = stmt.read(.Int64, false, 0);

        // TODO this unnecessarily loads all columns and stores them for the duration of the cursor
        var schema_manager = try SchemaManager.init(cb_ctx.arena, &self.vtab_ctx);
        defer schema_manager.deinit();
        const schema = try schema_manager.load(&self.lifetime_arena, cb_ctx.arena);
        if (col_rank == -1) {
            self.column_type = ColumnType.Rowid;
        } else {
            self.column_type = schema.columns[@intCast(col_rank)].column_type;
        }

        var blob_manager = try BlobManager.init(cb_ctx.arena, cb_ctx.arena, &self.vtab_ctx);
        var blob_handle = try blob_manager.open(segment_id);
        defer blob_handle.tryClose();

        self.header = try SegmentHeader.read(blob_handle.blob);
        self.advanceToNext();
    }

    pub fn eof(self: *Cursor) bool {
        return self.index == 3;
    }

    pub fn next(self: *Cursor) !void {
        self.index += 1;
        self.advanceToNext();
        self.defined_index += 1;
    }

    fn advanceToNext(self: *Cursor) void {
        while (self.index < 3) {
            const meta = self.currentMeta();
            if (meta.byte_len > 0) {
                return;
            }
            self.index += 1;
        }
    }

    pub fn rowid(self: *Cursor) !i64 {
        return self.defined_index;
    }

    pub fn column(self: *Cursor, result: Result, col_idx: usize) !void {
        switch (col_idx) {
            // stripe_index
            0 => result.setI64(self.defined_index),
            // content_type : { PRESENT, LENGTH, PRIMARY }
            1 => switch (self.index) {
                0 => result.setText("PRESENT"),
                1 => result.setText("LENGTH"),
                2 => result.setText("PRIMARY"),
                else => unreachable,
            },
            // data_type : { BOOL, INT, FLOAT, BYTE }
            2 => switch (self.index) {
                0 => result.setText("BOOL"),
                1 => result.setText("INT"),
                2 => switch (self.column_type.data_type) {
                    .Boolean => result.setText("BOOL"),
                    .Integer => result.setText("INT"),
                    .Float => result.setText("FLOAT"),
                    .Text => result.setText("BYTE"),
                    .Blob => result.setText("BYTE"),
                },
                else => unreachable,
            },
            // encoding_code
            3 => {
                const meta = self.currentMeta();
                result.setI32(@intFromEnum(meta.encoding));
            },
            // encoding
            4 => {
                const meta = self.currentMeta();
                result.setText(meta.encoding.canonicalName());
            },
            // size_bytes
            5 => {
                const meta = self.currentMeta();
                result.setI64(@intCast(meta.byte_len));
            },
            // table_name
            6 => result.setText(self.vtab_ctx.vtabName()),
            // segment_id
            7 => result.setI64(self.segment_id),
            else => unreachable,
        }
    }

    fn currentMeta(self: *Cursor) StripeMeta {
        return switch (self.index) {
            0 => self.header.present_stripe,
            1 => self.header.length_stripe,
            2 => self.header.primary_stripe,
            else => unreachable,
        };
    }
};
