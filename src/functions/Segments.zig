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

const VtabCtx = @import("../ctx.zig").VtabCtx;

const schema_mod = @import("../schema.zig");
const Schema = schema_mod.Schema;
const SchemaManager = schema_mod.Manager;

const row_group = @import("../row_group.zig");
const RowGroupIndex = row_group.Index;

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
        \\CREATE TABLE stanchion_segments (
        \\  row_group_index INTEGER NOT NULL,
        \\  column_rank INTEGER NOT NULL,
        \\  column_name TEXT NOT NULL,
        \\  segment_id INTEGER NULL,
        \\
        \\  table_name TEXT HIDDEN
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
    // Find the equality constraint on `table_name`
    for (0..best_index_info.constraintsLen()) |idx| {
        const c = best_index_info.constraint(idx);
        // column index 4 is the `table_name` hidden column
        if (c.usable() and c.columnIndex() == 4 and c.op() == .eq) {
            c.setOmitTest(true);
            c.includeArgInFilter(1);
            return;
        }
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

    vtab_ctx: VtabCtx,

    rg_index: RowGroupIndex,
    rg_index_cursor: RowGroupIndex.EntriesCursor,
    rg_entry: RowGroupIndex.Entry,

    rg_idx: i64,
    col_idx: i64,
    begun: bool,

    pub fn init(allocator: Allocator, conn: Conn) Cursor {
        return .{
            .lifetime_arena = ArenaAllocator.init(allocator),

            .vtab_ctx = VtabCtx.init(conn, undefined, undefined),

            .rg_index = undefined,
            .rg_index_cursor = undefined,
            .rg_entry = undefined,

            .rg_idx = 0,
            .col_idx = -1,
            .begun = false,
        };
    }

    pub fn deinit(self: *Cursor) void {
        if (self.begun) {
            self.rg_index_cursor.deinit();
            self.rg_index.deinit();
        }
        self.lifetime_arena.deinit();
    }

    pub fn begin(
        self: *Cursor,
        cb_ctx: *vtab.CallbackContext,
        _: i32,
        _: [:0]const u8,
        filter_args: vtab.FilterArgs,
    ) !void {
        // TODO do not return an error if this is not text
        const table_name_ref = (try filter_args.readValue(0)).asText();
        self.vtab_ctx.base.vtab_name = try self.lifetime_arena.allocator()
            .dupe(u8, table_name_ref);

        // Match SQLite behavior of `PRAGMA table_info()` and return empty set when the table does
        // not exist
        var schema_manager = SchemaManager.open(cb_ctx.arena, &self.vtab_ctx.base) catch |e| {
            if (e == SchemaManager.Error.ShadowTableDoesNotExist) {
                return;
            }
            return e;
        };
        defer schema_manager.deinit();
        self.vtab_ctx.schema = try schema_manager.load(&self.lifetime_arena, cb_ctx.arena);

        self.rg_index = RowGroupIndex.open(&self.vtab_ctx);
        errdefer self.rg_index.deinit();
        self.rg_index_cursor = try self.rg_index.cursor(cb_ctx.arena);
        errdefer self.rg_index_cursor.deinit();

        if (!self.rg_index_cursor.eof()) {
            self.rg_entry = .{
                .rowid_segment_id = 0,
                .column_segment_ids = try self.lifetime_arena.allocator()
                    .alloc(i64, self.vtab_ctx.columns().len),
                .record_count = 0,
            };
            self.rg_index_cursor.readEntry(&self.rg_entry);
        }

        self.begun = true;
    }

    pub fn eof(self: *Cursor) bool {
        // `begun` is false if the table does not exist and `begin` exited early
        return !self.begun or self.rg_index_cursor.eof();
    }

    pub fn next(self: *Cursor) !void {
        self.col_idx += 1;

        if (self.col_idx >= self.vtab_ctx.columns().len) {
            try self.rg_index_cursor.next();
            if (!self.rg_index_cursor.eof()) {
                self.rg_index_cursor.readEntry(&self.rg_entry);
            }
            self.rg_idx += 1;
            self.col_idx = -1;
        }
    }

    pub fn rowid(self: *Cursor) !i64 {
        const cols_len: i64 = @intCast(self.vtab_ctx.columns().len);
        return (self.rg_idx * (cols_len + 1)) + self.col_idx + 2;
    }

    pub fn column(self: *Cursor, result: Result, col_idx: usize) !void {
        switch (col_idx) {
            // row_group_index
            0 => result.setI64(self.rg_idx),
            // column_rank
            1 => result.setI64(self.col_idx),
            // column_name
            2 => {
                if (self.col_idx == -1) {
                    result.setText("rowid");
                    return;
                }
                const col_name = self.vtab_ctx.columns()[@intCast(self.col_idx)].name;
                result.setText(col_name);
            },
            // segment_id
            3 => {
                var seg_id: i64 = undefined;
                if (self.col_idx == -1) {
                    seg_id = self.rg_entry.rowid_segment_id;
                } else {
                    seg_id = self.rg_entry.column_segment_ids[@intCast(self.col_idx)];
                }
                result.setI64(seg_id);
            },
            // table_name
            4 => result.setText(self.vtab_ctx.vtabName()),
            else => unreachable,
        }
    }
};
