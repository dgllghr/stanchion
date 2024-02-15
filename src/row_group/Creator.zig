const std = @import("std");
const fmt = std.fmt;
const testing = std.testing;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;
const ArrayListUnmanaged = std.ArrayListUnmanaged;
const DynamicBitSetUnmanaged = std.bit_set.DynamicBitSetUnmanaged;
const Order = std.math.Order;

const sqlite = @import("../sqlite3.zig");
const Conn = sqlite.Conn;
const Stmt = sqlite.Stmt;
const ValueRef = sqlite.ValueRef;

const schema_mod = @import("../schema.zig");
const Column = schema_mod.Column;
const ColumnType = schema_mod.ColumnType;
const DataType = schema_mod.ColumnType.DataType;
const Schema = schema_mod.Schema;

const BlobManager = @import("../BlobManager.zig");
const BlobHandle = BlobManager.Handle;

const segment = @import("../segment.zig");
const SegmentHandle = segment.Storage.Handle;
const SegmentPlan = segment.Plan;
const SegmentPlanner = segment.Planner;
const SegmentReader = segment.Reader;
const SegmentStorage = segment.Storage;
const SegmentValue = segment.Value;
const SegmentWriter = segment.Writer;

const value_mod = @import("../value.zig");
const MemoryValue = value_mod.MemoryValue;
const MemoryTuple = value_mod.MemoryTuple;

const PendingInserts = @import("../PendingInserts.zig");
const PendingInsertsCursor = PendingInserts.Cursor;

const Cursor = @import("Cursor.zig");
const Index = @import("Index.zig");

columns_len: usize,
sort_key: []const usize,
blob_manager: *BlobManager,
row_group_index: *Index,
pending_inserts: *PendingInserts,

pend_inserts_sk_buf: []MemoryValue,
/// Stores the order that records go into the row group. True means read from pending inserts,
/// false from the row group
interleaving: DynamicBitSetUnmanaged,
rowid_seg_blob_handle: BlobHandle,
column_seg_blob_handles: []BlobHandle,
rowid_segment_planner: SegmentPlanner,
column_segment_planners: []SegmentPlanner,
rowid_segment_writer: SegmentWriter,
column_segment_writers: []SegmentWriter,
writers_continue: DynamicBitSetUnmanaged,

/// Cursor that is used to read from the source row group. This cursor is reused for every
/// source row group that is used to create new row groups.
src_row_group_cursor: Cursor,

max_row_group_len: u32,

const Self = @This();

pub fn init(
    allocator: Allocator,
    table_static_arena: *ArenaAllocator,
    blob_manager: *BlobManager,
    schema: *const Schema,
    row_group_index: *Index,
    pending_inserts: *PendingInserts,
    max_row_group_len: u32,
) !Self {
    const columns_len = schema.columns.len;

    const static_allocator = table_static_arena.allocator();

    const pend_inserts_sk_buf = try static_allocator.alloc(MemoryValue, schema.sort_key.len);

    var interleaving = try DynamicBitSetUnmanaged.initEmpty(
        static_allocator,
        max_row_group_len,
    );
    errdefer interleaving.deinit(static_allocator);

    const column_seg_blob_handles = try static_allocator.alloc(BlobHandle, columns_len);

    const rowid_segment_planner = SegmentPlanner.init(ColumnType.Rowid);
    const planners = try static_allocator.alloc(SegmentPlanner, columns_len);
    for (planners, schema.columns) |*planner, *col| {
        planner.* = SegmentPlanner.init(col.column_type);
    }

    const writers = try static_allocator.alloc(SegmentWriter, columns_len);
    const writers_continue = try DynamicBitSetUnmanaged.initEmpty(
        static_allocator,
        columns_len,
    );

    const src_row_group_cursor = try Cursor.init(allocator, blob_manager, schema);

    return .{
        .columns_len = columns_len,
        .sort_key = schema.sort_key,
        .blob_manager = blob_manager,
        .row_group_index = row_group_index,
        .pending_inserts = pending_inserts,
        .pend_inserts_sk_buf = pend_inserts_sk_buf,
        .interleaving = interleaving,
        .rowid_seg_blob_handle = undefined,
        .column_seg_blob_handles = column_seg_blob_handles,
        .rowid_segment_planner = rowid_segment_planner,
        .column_segment_planners = planners,
        .rowid_segment_writer = undefined,
        .column_segment_writers = writers,
        .writers_continue = writers_continue,
        .src_row_group_cursor = src_row_group_cursor,
        .max_row_group_len = max_row_group_len,
    };
}

pub fn deinit(self: *Self) void {
    self.src_row_group_cursor.deinit();
}

pub fn reset(self: *Self) void {
    // TODO it may be wise to 0-out the rest of the fields even if their values are
    //      always overriden during row group creation
    self.resetOutput();
    self.src_row_group_cursor.reset();
}

fn resetOutput(self: *Self) void {
    self.rowid_segment_planner.reset();
    for (self.column_segment_planners) |*p| {
        p.reset();
    }
    self.interleaving.setRangeValue(
        .{ .start = 0, .end = self.max_row_group_len },
        false,
    );
}

const NewRowGroup = struct {
    start_sort_key: []MemoryValue,
    start_rowid: i64,
    row_group_entry: Index.Entry,

    fn init(allocator: Allocator, columns_len: usize, sort_key_len: usize) !NewRowGroup {
        const start_sort_key = try allocator.alloc(MemoryValue, sort_key_len);
        const column_segment_ids = try allocator.alloc(i64, columns_len);
        return .{
            .start_sort_key = start_sort_key,
            .start_rowid = 0,
            .row_group_entry = .{
                .rowid_segment_id = 0,
                .column_segment_ids = column_segment_ids,
                .record_count = 0,
            },
        };
    }
};

pub fn createAll(self: *Self, tmp_arena: *ArenaAllocator) !void {
    // TODO wrap this function in a savepoint
    var candidates = try self.row_group_index.mergeCandidates(tmp_arena);
    defer candidates.deinit();

    var new_row_groups = ArrayListUnmanaged(NewRowGroup){};

    while (!candidates.eof()) {
        const row_group_len = candidates.readRowGroupLen();
        const pend_inserts_len = candidates.readPendInsertsLen();
        const full_row_groups = (row_group_len + pend_inserts_len) / self.max_row_group_len;

        // If there is a row group that is merged as part of this process, then there need to
        // enough pending inserts to create 2 full row groups. Otherwise, there just need to be
        // enough to create 1 full row group with no merge
        const min_row_groups: u32 = if (row_group_len == 0) 1 else 2;
        if (full_row_groups >= min_row_groups) {
            try self.merge(tmp_arena, &candidates, full_row_groups, &new_row_groups);
        }

        try candidates.next();
    }

    for (new_row_groups.items) |*rg| {
        try self.row_group_index.insertEntry(
            tmp_arena,
            MemoryTuple{ .values = rg.start_sort_key },
            rg.start_rowid,
            &rg.row_group_entry,
        );
    }
}

/// Create 1 or more row groups by merging values from an existing row group with pending inserts
fn merge(
    self: *Self,
    tmp_arena: *ArenaAllocator,
    candidates: *const Index.MergeCandidateCursor,
    num_new_row_groups: u32,
    new_row_groups: *ArrayListUnmanaged(NewRowGroup),
) !void {
    // Prepare the row group cursor to read from the source row group
    // TODO skip this if there is no source row group?
    defer self.src_row_group_cursor.reset();
    candidates.readRowGroupEntry(&self.src_row_group_cursor.row_group);

    // Prepare the pending inserts cursor. There is no need to copy the start key values because
    // the candidates cursor does not move for the lifetime of the pending inserts cursor.
    const pend_inserts_start_sk_initial = candidates.pendInsertStartSortKey();
    const pend_inserts_start_rowid_initial = candidates.readPendInsertsStartRowid();
    var pend_inserts_limit = (num_new_row_groups * self.max_row_group_len) -
        self.src_row_group_cursor.row_group.record_count;
    var pend_inserts_cursor = Limiter(PendingInsertsCursor).init(
        try self.pending_inserts.cursorFrom(
            tmp_arena,
            pend_inserts_start_sk_initial,
            pend_inserts_start_rowid_initial,
        ),
        pend_inserts_limit,
    );
    defer pend_inserts_cursor.deinit();

    // Allocate space for the new row groups
    try new_row_groups.ensureUnusedCapacity(tmp_arena.allocator(), num_new_row_groups);

    // Create the row groups
    for (0..num_new_row_groups) |idx| {
        const nrg_idx = new_row_groups.items.len;
        new_row_groups.appendAssumeCapacity(try NewRowGroup.init(
            tmp_arena.allocator(),
            self.columns_len,
            self.sort_key.len,
        ));
        const new_row_group = &new_row_groups.items[nrg_idx];

        defer self.resetOutput();
        try self.createSingle(tmp_arena, &pend_inserts_cursor, new_row_group);
        pend_inserts_limit -= pend_inserts_cursor.index;

        // Have the pending inserts cursor start from the current spot so that when it is rewound,
        // it starts from the proper place for the next row group being created.
        if (idx < num_new_row_groups - 1) {
            const curr_sk = pend_inserts_cursor.sortKey(self.sort_key);
            for (self.pend_inserts_sk_buf, 0..) |*sk_value, sk_idx| {
                sk_value.* = try MemoryValue.fromRef(
                    tmp_arena.allocator(),
                    try curr_sk.readValue(sk_idx),
                );
            }
            const curr_rowid = (try pend_inserts_cursor.readRowid()).asI64();

            std.log.debug(
                "row group creator: checkpointing pending inserts cursor at {any} {}",
                .{ self.pend_inserts_sk_buf, curr_rowid },
            );

            pend_inserts_cursor.deinit();

            pend_inserts_cursor = Limiter(PendingInsertsCursor).init(
                try self.pending_inserts.cursorFrom(
                    tmp_arena,
                    MemoryTuple{ .values = self.pend_inserts_sk_buf },
                    curr_rowid,
                ),
                pend_inserts_limit,
            );
        }
    }

    // Delete the pending inserts that went into the row groups
    if (pend_inserts_cursor.inner().eof()) {
        // The inner cursor at eof means that the the cursor reached the end of the table, so
        // delete all the way to the end.
        try self.pending_inserts.deleteFrom(
            tmp_arena,
            pend_inserts_start_sk_initial,
            pend_inserts_start_rowid_initial,
        );
    } else {
        // The outer (Limiter) cursor may be eof, but the inner cursor is not. So use the current
        // row as the upper bound on the delete range.
        try self.pending_inserts.deleteRange(
            tmp_arena,
            pend_inserts_start_sk_initial,
            pend_inserts_start_rowid_initial,
            pend_inserts_cursor.sortKey(self.sort_key),
            (try pend_inserts_cursor.readRowid()).asI64(),
        );
    }

    // Delete the segments from the source row group since they are no longer being used
    if (self.src_row_group_cursor.row_group.record_count > 0) {
        const src_rowid_segment_id = self.src_row_group_cursor.row_group.rowid_segment_id;
        try self.blob_manager.delete(tmp_arena, src_rowid_segment_id);
        const src_col_segemnt_ids = self.src_row_group_cursor.row_group.column_segment_ids;
        for (src_col_segemnt_ids) |seg_id| {
            try self.blob_manager.delete(tmp_arena, seg_id);
        }
    }

    // Delete the source row group entry
    try self.row_group_index.deleteEntry(
        tmp_arena,
        candidates.rowGroupStartSortKey(),
        candidates.readRowGroupStartRowid(),
    );
}

/// Assumes the pending inserts iterator and row group cursor point to the records that are
/// `start_index` from the start. Returns the number of records consumed from the pending inserts
/// cursor
fn createSingle(
    self: *Self,
    tmp_arena: *ArenaAllocator,
    pending_inserts: *Limiter(PendingInsertsCursor),
    new_row_group: *NewRowGroup,
) !void {
    const rg_cursor_start = self.src_row_group_cursor.index;

    //
    // Plan
    //

    // Assign the start sort key and start row id using the first row
    if (self.src_row_group_cursor.eof()) {
        self.interleaving.set(0);
        try self.planFirstRow(tmp_arena, pending_inserts, new_row_group);
        try pending_inserts.next();
    } else if (pending_inserts.eof()) {
        try self.planFirstRow(tmp_arena, &self.src_row_group_cursor, new_row_group);
        try self.src_row_group_cursor.next();
    } else {
        const ord = try compare(self.sort_key, &self.src_row_group_cursor, pending_inserts);
        switch (ord) {
            .lt => {
                try self.planFirstRow(tmp_arena, &self.src_row_group_cursor, new_row_group);
                try self.src_row_group_cursor.next();
            },
            .gt => {
                self.interleaving.set(0);
                try self.planFirstRow(tmp_arena, pending_inserts, new_row_group);
                try pending_inserts.next();
            },
            // Two records should never be equal because the rowid is different for every record
            .eq => unreachable,
        }
    }

    new_row_group.row_group_entry.record_count += 1;

    // Plan the rest of the rows

    while (new_row_group.row_group_entry.record_count < self.max_row_group_len) {
        if (self.src_row_group_cursor.eof() or pending_inserts.eof()) {
            break;
        }

        const ord = try compare(self.sort_key, &self.src_row_group_cursor, pending_inserts);
        switch (ord) {
            .lt => {
                try self.planRow(&self.src_row_group_cursor);
                try self.src_row_group_cursor.next();
            },
            .gt => {
                self.interleaving.set(new_row_group.row_group_entry.record_count);
                try self.planRow(pending_inserts);
                try pending_inserts.next();
            },
            // Two records should never be equal because the rowid is different for every record
            .eq => unreachable,
        }

        new_row_group.row_group_entry.record_count += 1;
    }

    // Write any remaining rows from either source as long as there is space. This part is only
    // reached when at least 1 source is exhausted so at most one of the following loops will run

    while (new_row_group.row_group_entry.record_count < self.max_row_group_len and
        !self.src_row_group_cursor.eof())
    {
        try self.planRow(&self.src_row_group_cursor);
        try self.src_row_group_cursor.next();
        new_row_group.row_group_entry.record_count += 1;
    }

    while (new_row_group.row_group_entry.record_count < self.max_row_group_len and
        !pending_inserts.eof())
    {
        self.interleaving.set(new_row_group.row_group_entry.record_count);
        try self.planRow(pending_inserts);
        try pending_inserts.next();
        new_row_group.row_group_entry.record_count += 1;
    }

    //
    // Write
    //

    // Init rowid segment writer

    const rowid_segment_plan = try self.rowid_segment_planner.end();
    self.rowid_seg_blob_handle = try self.blob_manager.create(
        tmp_arena,
        rowid_segment_plan.totalLen(),
    );
    errdefer self.rowid_seg_blob_handle.tryDestroy(tmp_arena);
    defer self.rowid_seg_blob_handle.tryClose();
    try self.rowid_segment_writer.init(&self.rowid_seg_blob_handle, &rowid_segment_plan);
    const rowid_continue = try self.rowid_segment_writer.begin();

    var writer_idx: usize = 0;
    errdefer for (self.column_seg_blob_handles[0..writer_idx]) |*handle| {
        handle.tryDestroy(tmp_arena);
    };
    defer for (self.column_seg_blob_handles[0..writer_idx]) |*handle| {
        handle.tryClose();
    };
    for (
        self.column_segment_planners,
        self.column_seg_blob_handles,
        self.column_segment_writers,
        0..,
    ) |*planner, *blob_handle, *writer, idx| {
        const plan = try planner.end();
        blob_handle.* = try self.blob_manager.create(tmp_arena, plan.totalLen());
        writer_idx += 1;
        try writer.init(blob_handle, &plan);
        const cont = try writer.begin();
        self.writers_continue.setValue(idx, cont);
    }

    // Write the data

    // Reset the sources back to where they were when creating the single row group started
    try pending_inserts.rewind();
    self.src_row_group_cursor.reset();
    try self.src_row_group_cursor.skip(rg_cursor_start);

    for (0..new_row_group.row_group_entry.record_count) |idx| {
        // true means read from row group, false from staged inserts
        if (self.interleaving.isSet(idx)) {
            try self.writeRow(rowid_continue, pending_inserts);
            try pending_inserts.next();
        } else {
            try self.writeRow(rowid_continue, &self.src_row_group_cursor);
            try self.src_row_group_cursor.next();
        }
    }

    // Finalize the writers

    try self.rowid_segment_writer.end();
    new_row_group.row_group_entry.rowid_segment_id = self.rowid_seg_blob_handle.id;
    const column_segment_ids = new_row_group.row_group_entry.column_segment_ids;
    for (
        self.column_seg_blob_handles,
        self.column_segment_writers,
        column_segment_ids,
    ) |*blob_handle, *writer, *seg_id| {
        try writer.end();
        seg_id.* = blob_handle.id;
    }
}

/// Feeds the first row into the planner and stores the start sort key and start rowid in
/// `new_row_group`
fn planFirstRow(
    self: *Self,
    tmp_arena: *ArenaAllocator,
    row: anytype,
    new_row_group: *NewRowGroup,
) !void {
    const start_rowid_value = try row.readRowid();
    const start_rowid = start_rowid_value.asI64();
    new_row_group.start_rowid = start_rowid;

    self.rowid_segment_planner.addNext(start_rowid_value);

    for (self.column_segment_planners, 0..) |*planner, idx| {
        const value = try row.readValue(idx);
        for (self.sort_key, 0..) |sk_col_rank, sk_rank| {
            if (sk_col_rank == idx) {
                const mem_value = try MemoryValue.fromValue(
                    tmp_arena.allocator(),
                    planner.column_type.data_type,
                    value,
                );
                new_row_group.start_sort_key[sk_rank] = mem_value;
                break;
            }
        }
        planner.addNext(value);
    }
}

fn planRow(self: *Self, row: anytype) !void {
    const rowid = try row.readRowid();
    self.rowid_segment_planner.addNext(rowid);

    for (self.column_segment_planners, 0..) |*planner, idx| {
        const value = try row.readValue(idx);
        planner.addNext(value);
    }
}

fn writeRow(self: *Self, rowid_continue: bool, row: anytype) !void {
    if (rowid_continue) {
        const rowid = try row.readRowid();
        try self.rowid_segment_writer.write(rowid.asI64());
    }
    for (self.column_segment_writers, 0..) |*writer, idx| {
        if (self.writers_continue.isSet(idx)) {
            const value = try row.readValue(idx);
            try writer.write(value);
        }
    }
}

fn compare(sort_key: []const usize, left_row: anytype, right_row: anytype) !Order {
    for (sort_key) |rank| {
        const left_value = try left_row.readValue(rank);
        const right_value = try right_row.readValue(rank);
        const ord = left_value.compare(right_value);
        if (ord != .eq) {
            return ord;
        }
    }

    const left_rowid = try left_row.readRowid();
    const right_rowid = try right_row.readRowid();
    return std.math.order(left_rowid.asI64(), right_rowid.asI64());
}

fn Limiter(comptime Cur: type) type {
    return struct {
        cur: Cur,
        limit: u32,
        index: u32,

        const CurLimiter = @This();

        pub fn init(cur: Cur, limit: u32) CurLimiter {
            return .{
                .cur = cur,
                .limit = limit,
                .index = 0,
            };
        }

        pub fn deinit(self: *CurLimiter) void {
            self.cur.deinit();
        }

        pub fn inner(self: *CurLimiter) *Cur {
            return &self.cur;
        }

        pub fn rewind(self: *CurLimiter) !void {
            try self.cur.rewind();
            self.index = 0;
        }

        pub fn eof(self: CurLimiter) bool {
            return self.cur.eof() or self.index >= self.limit;
        }

        pub fn next(self: *CurLimiter) !void {
            try self.cur.next();
            self.index += 1;
        }

        pub fn readRowid(self: CurLimiter) !ValueRef {
            return self.cur.readRowid();
        }

        pub fn readValue(self: CurLimiter, col_idx: usize) !ValueRef {
            return self.cur.readValue(col_idx);
        }

        pub fn sortKey(self: *CurLimiter, sort_key: []const usize) Cur.SortKey {
            return self.cur.sortKey(sort_key);
        }
    };
}

test "row group: create single from pending inserts" {
    const VtabCtx = @import("../ctx.zig").VtabCtx;
    const datasets = @import("../testing/datasets.zig");
    const SchemaManager = @import("../schema.zig").Manager;
    _ = SchemaManager;
    const TableData = @import("../TableData.zig");

    const conn = try Conn.openInMemory();
    defer conn.close();

    var arena = ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const ctx = VtabCtx.init(conn, "test", datasets.planets.schema);

    var blob_manager = try BlobManager.init(&arena, &ctx.base);
    defer blob_manager.deinit();
    try blob_manager.table().create(&arena);
    var table_data = TableData.init(&ctx.base);
    defer table_data.deinit();
    try table_data.table().create(&arena);
    var row_group_index = Index.init(&ctx);
    defer row_group_index.deinit();
    try row_group_index.table().create(&arena);
    var pending_inserts = try PendingInserts.init(arena.allocator(), &ctx);
    errdefer pending_inserts.deinit();
    try pending_inserts.table().create(&arena);

    const table_values = datasets.planets.fixed_data[0..4];
    const rowids = [_]i64{ 1, 2, 4, 3 };

    for (table_values, 1..) |*row, rowid| {
        _ = try pending_inserts.insert(&arena, @intCast(rowid), MemoryTuple{ .values = row });
    }

    var new_row_group: Self.NewRowGroup = undefined;
    new_row_group.row_group_entry.record_count = 0;
    new_row_group.start_sort_key = try arena.allocator()
        .alloc(MemoryValue, ctx.schema.sort_key.len);
    new_row_group.row_group_entry.column_segment_ids = try arena.allocator()
        .alloc(i64, ctx.schema.columns.len);

    {
        var creator = try Self.init(
            std.testing.allocator,
            &arena,
            &blob_manager,
            &ctx.schema,
            &row_group_index,
            &pending_inserts,
            4,
        );
        defer creator.deinit();

        var pend_inserts_cursor = Limiter(PendingInsertsCursor).init(
            try pending_inserts.cursor(&arena),
            10,
        );
        defer pend_inserts_cursor.deinit();

        _ = try creator.createSingle(&arena, &pend_inserts_cursor, &new_row_group);
    }

    var cursor = try Cursor.init(arena.allocator(), &blob_manager, &ctx.schema);
    cursor.rowGroup().* = new_row_group.row_group_entry;

    var idx: usize = 0;
    while (!cursor.eof()) {
        const rowid = try cursor.readRowid();
        try std.testing.expectEqual(rowids[idx], rowid.asI64());

        // Rows are in sort key order not insert order. Use the expected rowid as the
        // index
        const row_idx: usize = @as(usize, @intCast(rowids[idx])) - 1;
        const quadrant = (try cursor.readValue(0)).asText();
        try std.testing.expectEqualStrings(table_values[row_idx][0].Text, quadrant);
        const sector = (try cursor.readValue(1)).asI64();
        try std.testing.expectEqual(table_values[row_idx][1].Integer, sector);

        idx += 1;
        try cursor.next();
    }
    try testing.expectEqual(@as(usize, 4), idx);
}

test "row group: create all" {
    const VtabCtx = @import("../ctx.zig").VtabCtx;
    const datasets = @import("../testing/datasets.zig");
    const TableData = @import("../TableData.zig");

    const conn = try Conn.openInMemory();
    defer conn.close();

    var arena = ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const ctx = VtabCtx.init(conn, "test", datasets.planets.schema);

    var blob_manager = try BlobManager.init(&arena, &ctx.base);
    defer blob_manager.deinit();
    try blob_manager.table().create(&arena);
    var table_data = TableData.init(&ctx.base);
    defer table_data.deinit();
    try table_data.table().create(&arena);
    var row_group_index = Index.init(&ctx);
    defer row_group_index.deinit();
    try row_group_index.table().create(&arena);
    var pending_inserts = try PendingInserts.init(arena.allocator(), &ctx);
    errdefer pending_inserts.deinit();
    try pending_inserts.table().create(&arena);

    const table_values = datasets.planets.fixed_data[0..4];
    const rowids = [_]i64{ 1, 2, 4, 3 };

    for (table_values, 1..) |*row, rowid| {
        _ = try pending_inserts.insert(&arena, @intCast(rowid), MemoryTuple{ .values = row });
    }

    {
        var creator = try Self.init(
            std.testing.allocator,
            &arena,
            &blob_manager,
            &ctx.schema,
            &row_group_index,
            &pending_inserts,
            4,
        );
        defer creator.deinit();

        try creator.createAll(&arena);
    }

    var cursor = try Cursor.init(arena.allocator(), &blob_manager, &ctx.schema);
    var rg_index_cursor = try row_group_index.cursor(&arena);
    rg_index_cursor.readEntry(cursor.rowGroup());

    var idx: usize = 0;
    while (!cursor.eof()) {
        const rowid = try cursor.readRowid();
        try std.testing.expectEqual(rowids[idx], rowid.asI64());

        // Rows are in sort key order not insert order. Use the expected rowid as the index
        const row_idx: usize = @as(usize, @intCast(rowids[idx])) - 1;
        const quadrant = (try cursor.readValue(0)).asText();
        try std.testing.expectEqualStrings(table_values[row_idx][0].Text, quadrant);
        const sector = (try cursor.readValue(1)).asI64();
        try std.testing.expectEqual(table_values[row_idx][1].Integer, sector);

        idx += 1;
        try cursor.next();
    }
    try testing.expectEqual(@as(usize, 4), idx);

    // All pending inserts should have been deleted
    var pend_inserts_cursor = try pending_inserts.cursor(&arena);
    try testing.expect(pend_inserts_cursor.eof());
}

pub fn benchRowGroupCreate() !void {
    const VtabCtx = @import("../ctx.zig").VtabCtx;
    const datasets = @import("../testing/datasets.zig");
    const TableData = @import("../TableData.zig");
    const TmpDiskDb = @import("../testing/TmpDiskDb.zig");

    const row_group_len: u32 = 10_000;

    var tmp_disk_db = try TmpDiskDb.create(std.heap.page_allocator, "bench.db");
    defer tmp_disk_db.deinit(std.heap.page_allocator);
    const conn = tmp_disk_db.conn;

    var arena = ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const ctx = VtabCtx.init(conn, "test", datasets.planets.schema);

    var blob_manager = try BlobManager.init(&arena, &ctx.base);
    defer blob_manager.deinit();
    try blob_manager.table().create(&arena);
    var table_data = TableData.init(&ctx.base);
    defer table_data.deinit();
    try table_data.table().create(&arena);
    var row_group_index = Index.init(&ctx);
    defer row_group_index.deinit();
    try row_group_index.table().create(&arena);
    var pending_inserts = try PendingInserts.init(arena.allocator(), &ctx);
    errdefer pending_inserts.deinit();
    try pending_inserts.table().create(&arena);

    const seed = @as(u64, @truncate(@as(u128, @bitCast(std.time.nanoTimestamp()))));
    var prng = std.rand.DefaultPrng.init(seed);

    // Create a row group from pending inserts only (no merge)

    var rowid: i64 = 1;

    try conn.exec("BEGIN");
    const start_insert = std.time.microTimestamp();
    for (0..row_group_len) |_| {
        var row = datasets.planets.randomRecord(&prng);
        _ = try pending_inserts.insert(&arena, @intCast(rowid), MemoryTuple{ .values = &row });
        rowid += 1;
    }
    try conn.exec("COMMIT");
    const end_insert = std.time.microTimestamp();
    std.log.err("insert pending insert: total: {d} micros, per insert: {d} micros", .{
        end_insert - start_insert,
        @divTrunc(end_insert - start_insert, row_group_len),
    });

    {
        var start: i64 = undefined;
        {
            var creator = try Self.init(
                std.heap.page_allocator,
                &arena,
                &blob_manager,
                &ctx.schema,
                &row_group_index,
                &pending_inserts,
                row_group_len,
            );
            defer creator.deinit();

            try conn.exec("BEGIN");
            start = std.time.microTimestamp();
            try creator.createAll(&arena);
        }
        try conn.exec("COMMIT");
        const end = std.time.microTimestamp();
        std.log.err("create row group: {d} micros", .{end - start});
    }

    // Create a row group with a merge

    try conn.exec("BEGIN");
    for (0..(row_group_len * 3)) |_| {
        var row = datasets.planets.randomRecord(&prng);
        _ = try pending_inserts.insert(&arena, rowid, MemoryTuple{ .values = &row });
        rowid += 1;
    }
    try conn.exec("COMMIT");

    {
        var start: i64 = undefined;
        {
            var creator = try Self.init(
                std.heap.page_allocator,
                &arena,
                &blob_manager,
                &ctx.schema,
                &row_group_index,
                &pending_inserts,
                row_group_len,
            );
            defer creator.deinit();

            try conn.exec("BEGIN");
            start = std.time.microTimestamp();
            try creator.createAll(&arena);
        }
        try conn.exec("COMMIT");
        const end = std.time.microTimestamp();
        std.log.err("create row group: {d} micros", .{end - start});
    }
}
