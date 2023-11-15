const std = @import("std");
const debug = std.debug;
const fmt = std.fmt;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const Stmt = @import("../sqlite3/Stmt.zig");
const ValueRef = @import("../sqlite3/value.zig").Ref;
const stmt_cell = @import("../stmt_cell.zig");

const sql_fmt = @import("sql_fmt.zig");
const pending_inserts = @import("pending_inserts.zig");
const Ctx = @import("Ctx.zig");
const EntryType = @import("entry_type.zig").EntryType;
const PendingInserts = pending_inserts.PendingInserts;
const PendingInsertsIterator = pending_inserts.Iterator;
const RowGroupEntry = @import("row_group_entry.zig").Entry;

const StmtCell = stmt_cell.StmtCell(Ctx);

/// Manages nodes within the primary index. Each node consists of a head entry, which is either a
/// `.RowGroup` entry or a `.TableState` entry at the beginning of the node, and zero or more
/// pending `.Insert` entries.
pub const Nodes = struct {
    /// This StmtCell holds the Stmt in EntryHandle, so only 1 EntryHandle can be held at a time
    node_head_entry: StmtCell,
    /// There is at most one active handle at a time. Store it here so it can be reused
    handle: NodeHandle,

    pub fn init() Nodes {
        return .{
            .node_head_entry = StmtCell.init(&headEntryQuery),
            .handle = NodeHandle.uninitialized(),
        };
    }

    pub fn deinit(self: *Nodes) void {
        self.node_head_entry.deinit();
        self.handle.deinit();
    }

    fn headEntryQuery(ctx: *const Ctx, arena: *ArenaAllocator) ![]const u8 {
        return fmt.allocPrintZ(arena.allocator(),
            \\SELECT record_count, col_rowid, rowid, {s}, {s}
            \\FROM "{s}_primaryindex"
            \\WHERE ({s}, rowid) <= ({s}, ?) AND entry_type = {d}
            \\LIMIT 1
        , .{
            sql_fmt.ColumnListFormatter("sk_value_{d}"){ .len = ctx.sort_key.len },
            sql_fmt.ColumnListFormatter("col_{d}"){ .len = ctx.sort_key.len },
            ctx.vtab_table_name,
            sql_fmt.ColumnListFormatter("sk_value_{d}"){ .len = ctx.sort_key.len },
            sql_fmt.ParameterListFormatter{ .len = ctx.sort_key.len },
            @intFromEnum(EntryType.RowGroup),
        });
    }

    /// Only one row group entry can be open at a time. Deinit any `HeadEntryHandle`s before
    /// calling this function to create a new one.
    pub fn containingNodeHandle(
        self: *Nodes,
        tmp_arena: *ArenaAllocator,
        ctx: *Ctx,
        row: anytype,
        rowid: i64,
    ) !*NodeHandle {
        const stmt = try self.node_head_entry.getStmt(tmp_arena, ctx);
        errdefer self.node_head_entry.reset();

        for (ctx.sort_key, 1..) |col_idx, idx| {
            const value = try row.readValue(col_idx);
            try value.bind(stmt, idx);
        }
        try stmt.bind(.Int64, ctx.sort_key.len + 1, rowid);

        const has_row_group = try stmt.next();
        if (!has_row_group) {
            self.node_head_entry.reset();
            self.handle = NodeHandle.init(ctx, .table_state);
            return &self.handle;
        }

        const rg_rowid = stmt.read(.Int64, false, 2);

        self.handle = NodeHandle.init(ctx, .{ .row_group = .{
            .cell = &self.node_head_entry,
            .stmt = stmt,
            .sort_key_len = ctx.sort_key.len,
            .columns_len = ctx.columns_len,
            .rowid = rg_rowid,
        } });
        return &self.handle;
    }
};

/// Handle to a row group entry in the primary index. This handle holds an open statement in sqlite
/// and must be released with `deinit` after use. Only one of these handles can be open in the
/// primary index at a time.
pub const NodeHandle = struct {
    ctx: *Ctx,

    head: Head,

    pending_inserts: PendingInserts,
    /// There is at most one active staged inserts iterator at a time. Store it here so it can be
    /// reused. The initialized field on the. Do not access this field directly.
    pending_inserts_iterator: PendingInsertsIterator,

    initialized: bool,

    fn uninitialized() NodeHandle {
        return .{
            .ctx = undefined,
            .head = undefined,
            .pending_inserts = undefined,
            .pending_inserts_iterator = undefined,
            .initialized = false,
        };
    }

    fn init(ctx: *Ctx, head: Head) NodeHandle {
        return .{
            .ctx = ctx,
            .head = head,
            .pending_inserts = PendingInserts.init(),
            .pending_inserts_iterator = PendingInsertsIterator.uninitialized(),
            .initialized = true,
        };
    }

    pub fn deinit(self: *NodeHandle) void {
        if (self.pending_inserts_iterator.initialized) {
            self.pending_inserts_iterator.deinit();
        }
        self.pending_inserts.deinit();
        switch (self.head) {
            .row_group => |*head| head.deinit(),
            else => {},
        }
        self.initialized = false;
    }

    /// Gets the iterator over the pending inserts in this node. Returns the currently active
    /// iterator if there is one.
    pub fn pendingInserts(self: *NodeHandle, tmp_arena: *ArenaAllocator) !*PendingInsertsIterator {
        if (self.pending_inserts_iterator.initialized) {
            return &self.pending_inserts_iterator;
        }

        self.pending_inserts_iterator = switch (self.head) {
            .table_state => try self.pending_inserts.iterateFromStart(tmp_arena, self.ctx),
            .row_group => |*handle| try self.pending_inserts.iterate(
                tmp_arena,
                self.ctx,
                handle.sortKey(),
                handle.rowid,
            ),
        };

        return &self.pending_inserts_iterator;
    }

    pub fn readEntryInto(self: *const NodeHandle, entry: *RowGroupEntry) !void {
        switch (self.head) {
            // This handle is not pointed at a row group so set the record count to 0 on the entry,
            // which indicates that the row group does not exist
            .table_state => entry.record_count = 0,
            .row_group => |*handle| try handle.readEntryInto(entry),
        }
    }
};

const Head = union(enum) {
    /// Always the first entry in the priamry index
    table_state,
    row_group: RowGroupHeadHandle,
};

const RowGroupHeadHandle = struct {
    cell: *StmtCell,
    stmt: Stmt,
    sort_key_len: usize,
    columns_len: usize,
    rowid: i64,

    const SortKey = struct {
        sort_key_len: usize,
        stmt: Stmt,

        pub fn valuesLen(self: SortKey) usize {
            return self.sort_key_len;
        }

        pub fn readValue(self: SortKey, index: usize) ValueRef {
            return self.stmt.readSqliteValue(index + 3);
        }
    };

    fn deinit(self: *RowGroupHeadHandle) void {
        self.cell.reset();
    }

    fn sortKey(self: *const RowGroupHeadHandle) SortKey {
        return .{ .stmt = self.stmt, .sort_key_len = self.sort_key_len };
    }

    fn readEntryInto(self: *const RowGroupHeadHandle, entry: *RowGroupEntry) !void {
        debug.assert(entry.column_segment_ids.len == self.columns_len);

        entry.record_count = @intCast(self.stmt.read(.Int64, false, 0));
        entry.rowid_segment_id = self.stmt.read(.Int64, false, 1);
        const seg_id_start = self.sort_key_len + 3;
        for (entry.column_segment_ids, seg_id_start..) |*seg_id, idx| {
            seg_id.* = self.stmt.read(.Int64, false, idx);
        }
    }
};
