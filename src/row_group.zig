const std = @import("std");
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;
const ArrayListUnmanaged = std.ArrayListUnmanaged;
const DynamicBitSetUnmanaged = std.bit_set.DynamicBitSetUnmanaged;
const Order = std.math.Order;

const ValueRef = @import("sqlite3/value.zig").Ref;

const schema_mod = @import("schema.zig");
const ColumnType = schema_mod.ColumnType;
const Schema = schema_mod.Schema;

const segment = @import("segment.zig");
const SegmentDb = segment.Db;
const SegmentHandle = segment.Handle;
const SegmentPlan = segment.Plan;
const SegmentPlanner = segment.Planner;
const SegmentReader = segment.Reader;
const SegmentValue = segment.Value;
const SegmentWriter = segment.Writer;

const value_mod = @import("value.zig");
const MemoryValue = value_mod.MemoryValue;
const MemoryTuple = value_mod.MemoryTuple;

const PrimaryIndex = @import("PrimaryIndex.zig");
const NodeHandle = PrimaryIndex.NodeHandle;
const PendingInsertsIterator = PrimaryIndex.PendingInsertsIterator;
const RowGroupEntry = PrimaryIndex.RowGroupEntry;

pub const Creator = struct {
    columns_len: usize,
    sort_key: []const usize,
    segment_db: *SegmentDb,
    primary_index: *PrimaryIndex,

    /// Stores the order that records go into the row group. True means read from staged inserts,
    /// false from the row group
    interleaving: DynamicBitSetUnmanaged,
    rowid_segment_planner: SegmentPlanner,
    column_segment_planners: []SegmentPlanner,
    rowid_segment_writer: SegmentWriter,
    column_segment_writers: []SegmentWriter,
    writers_continue: DynamicBitSetUnmanaged,

    /// Cursor that is used to read from the source row group
    row_group_cursor: Cursor,

    max_row_group_len: u32,

    pub fn init(
        allocator: Allocator,
        table_static_arena: *ArenaAllocator,
        segment_db: *SegmentDb,
        schema: *const Schema,
        primary_index: *PrimaryIndex,
        max_row_group_len: u32,
    ) !Creator {
        const columns_len = schema.columns.items.len;

        const static_allocator = table_static_arena.allocator();

        var interleaving = try DynamicBitSetUnmanaged.initEmpty(
            static_allocator,
            max_row_group_len,
        );
        errdefer interleaving.deinit(static_allocator);

        const rowid_segment_planner = SegmentPlanner.init(ColumnType.Rowid);
        const planners = try static_allocator.alloc(SegmentPlanner, columns_len);
        for (planners, schema.columns.items) |*planner, *col| {
            planner.* = SegmentPlanner.init(col.column_type);
        }

        const writers = try static_allocator.alloc(SegmentWriter, columns_len);
        const writers_continue = try DynamicBitSetUnmanaged.initEmpty(
            static_allocator,
            columns_len,
        );

        const row_group_cursor = try Cursor.init(allocator, segment_db, schema);

        return .{
            .columns_len = columns_len,
            .sort_key = schema.sort_key.items,
            .segment_db = segment_db,
            .primary_index = primary_index,
            .interleaving = interleaving,
            .rowid_segment_planner = rowid_segment_planner,
            .column_segment_planners = planners,
            .rowid_segment_writer = undefined,
            .column_segment_writers = writers,
            .writers_continue = writers_continue,
            .row_group_cursor = row_group_cursor,
            .max_row_group_len = max_row_group_len,
        };
    }

    pub fn deinit(self: *Creator) void {
        self.row_group_cursor.deinit();
    }

    pub fn reset(self: *Creator) void {
        // TODO it may be wise to 0-out the rest of the fields even if their values are
        //      always overriden during row group creation
        self.resetOutput();
        self.row_group_cursor.reset();
    }

    fn resetOutput(self: *Creator) void {
        self.rowid_segment_planner.reset();
        for (self.column_segment_planners) |*p| {
            p.reset();
        }
        self.interleaving.setRangeValue(
            .{ .start = 0, .end = self.max_row_group_len },
            false,
        );
    }

    /// Creates up to `n` row groups from the provided `node`. If the head of `node` is a row
    /// group, the head row group may be destroyed in the process.
    pub fn createN(
        self: *Creator,
        tmp_arena: *ArenaAllocator,
        node: *NodeHandle,
        n: usize,
    ) !usize {
        std.debug.assert(n > 0);

        var pending_inserts = try node.pendingInserts(tmp_arena);
        // If there are no pending records, there is nothing to do
        if (pending_inserts.eof) {
            return 0;
        }
        // Count the number of pending inserts to plan the number of row groups that will be
        // created
        var pending_inserts_len: u32 = 0;
        while (!pending_inserts.eof) {
            try pending_inserts.next();
            pending_inserts_len += 1;
        }

        try node.readEntryInto(self.row_group_cursor.rowGroup());

        const full_row_groups =
            (self.row_group_cursor.row_group.record_count + pending_inserts_len) /
            self.max_row_group_len;
        if (full_row_groups == 0) {
            return 0;
        }
        const num_row_groups = @min(full_row_groups, if (node.head) |_| n + 1 else n);
        // Limit the number of pending inserts to ensure all row group records are consumed
        const num_pending_inserts = (num_row_groups * self.max_row_group_len) -
            self.row_group_cursor.row_group.record_count;
        if (num_pending_inserts == 0) {
            return 0;
        }

        var start_sort_keys = try tmp_arena.allocator()
            .alloc(MemoryValue, self.sort_key.len * num_row_groups);
        var column_segment_ids = try tmp_arena.allocator()
            .alloc(i64, self.columns_len * num_row_groups);
        const new_row_groups = try tmp_arena.allocator()
            .alloc(NewRowGroup, num_row_groups);
        for (new_row_groups, 0..) |*nrg, idx| {
            const sk_start_idx = idx * self.sort_key.len;
            const sk_end_idx = sk_start_idx + self.sort_key.len;
            const csi_start_idx = idx * self.columns_len;
            const csi_end_idx = csi_start_idx + self.columns_len;
            nrg.* = .{
                .start_sort_key = start_sort_keys[sk_start_idx..sk_end_idx],
                .start_rowid = 0,
                .row_group_entry = .{
                    .rowid_segment_id = undefined,
                    .column_segment_ids = column_segment_ids[csi_start_idx..csi_end_idx],
                    .record_count = 0,
                },
            };
        }

        try pending_inserts.restart();
        var pending_inserts_range = PendingInsertsRangeIter.init(
            pending_inserts,
            0,
            num_pending_inserts,
        );
        for (new_row_groups) |*row_group| {
            defer self.resetOutput();
            try self.createSingle(tmp_arena, &pending_inserts_range, row_group);
        }

        //
        // Update primary index
        //

        if (node.head) |*handle| {
            try self.deleteIndexEntries(
                tmp_arena,
                &pending_inserts_range,
                handle.sortKey(),
                handle.rowid,
            );
        } else {
            // When there is no head, start from the first pending insert
            const start_sort_key = MemoryTuple{ .values = start_sort_keys[0..self.sort_key.len] };
            const start_rowid = new_row_groups[0].start_rowid;
            try self.deleteIndexEntries(
                tmp_arena,
                &pending_inserts_range,
                start_sort_key,
                start_rowid,
            );
        }

        // TODO delete the segments of the row group that was deleted

        for (new_row_groups) |*rg| {
            try self.primary_index.insertRowGroupEntry(
                tmp_arena,
                rg.start_sort_key,
                rg.start_rowid,
                &rg.row_group_entry,
            );
        }

        return num_row_groups - (if (node.head) |_| @as(u32, 1) else 0);
    }

    fn deleteIndexEntries(
        self: *Creator,
        tmp_arena: *ArenaAllocator,
        pending_inserts: *PendingInsertsRangeIter,
        start_sort_key: anytype,
        start_rowid: i64,
    ) !void {
        if (pending_inserts.eofRow()) {
            const end_sort_key = pending_inserts.sortKey();
            const end_rowid = (try pending_inserts.readRowid()).asI64();
            try self.primary_index.deleteRange(
                tmp_arena,
                start_sort_key,
                start_rowid,
                end_sort_key,
                end_rowid,
            );
        } else {
            try self.primary_index.deleteRangeFrom(tmp_arena, start_sort_key, start_rowid);
        }
    }

    const NewRowGroup = struct {
        start_sort_key: []MemoryValue,
        start_rowid: i64,
        row_group_entry: RowGroupEntry,
    };

    /// Assumes the pending inserts iterator and row group cursor point to the records that are
    /// `start_index` from the start.
    fn createSingle(
        self: *Creator,
        tmp_arena: *ArenaAllocator,
        pending_inserts: *PendingInsertsRangeIter,
        new_row_group: *NewRowGroup,
    ) !void {
        // Ensure the pending inserts iterator gets set back to where it started after planning
        // and before writing so planning and writing are on the same data
        pending_inserts.markStart();
        const rg_cursor_start = self.row_group_cursor.index;

        //
        // Plan
        //

        // Assign the start sort key and start row id using the first row
        if (self.row_group_cursor.eof()) {
            self.interleaving.set(0);
            try self.planFirstRow(tmp_arena, pending_inserts, new_row_group);
            try pending_inserts.next();
        } else if (pending_inserts.eof) {
            try self.planFirstRow(tmp_arena, &self.row_group_cursor, new_row_group);
            try self.row_group_cursor.next();
        } else {
            const ord = try compare(self.sort_key, &self.row_group_cursor, pending_inserts);
            switch (ord) {
                .lt => {
                    try self.planFirstRow(tmp_arena, &self.row_group_cursor, new_row_group);
                    try self.row_group_cursor.next();
                },
                .gt => {
                    self.interleaving.set(0);
                    try self.planFirstRow(tmp_arena, pending_inserts, new_row_group);
                    try pending_inserts.next();
                },
                // Two records should never be equal because the rowid is different for every
                // record
                .eq => unreachable,
            }
        }

        new_row_group.row_group_entry.record_count += 1;

        // Plan the rest of the rows

        while (new_row_group.row_group_entry.record_count < self.max_row_group_len) {
            if (self.row_group_cursor.eof() or pending_inserts.eof) {
                break;
            }

            const ord = try compare(self.sort_key, &self.row_group_cursor, pending_inserts);
            switch (ord) {
                .lt => {
                    try self.planRow(&self.row_group_cursor);
                    try self.row_group_cursor.next();
                },
                .gt => {
                    self.interleaving.set(new_row_group.row_group_entry.record_count);
                    try self.planRow(pending_inserts);
                    try pending_inserts.next();
                },
                // Two records should never be equal because the rowid is different for
                // every record
                .eq => unreachable,
            }

            new_row_group.row_group_entry.record_count += 1;
        }

        // Write any remaining rows from either source as long as there is space. This
        // part is only reached when at least 1 source is exhausted so at most one of the
        // following loops will run

        while (new_row_group.row_group_entry.record_count < self.max_row_group_len and
            !self.row_group_cursor.eof())
        {
            try self.planRow(&self.row_group_cursor);
            try self.row_group_cursor.next();
            new_row_group.row_group_entry.record_count += 1;
        }

        while (new_row_group.row_group_entry.record_count < self.max_row_group_len and
            !pending_inserts.eof)
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

        self.rowid_segment_writer = try SegmentWriter.allocate(
            tmp_arena,
            self.segment_db,
            try self.rowid_segment_planner.end(),
        );
        errdefer freeSegment(tmp_arena, self.segment_db, &self.rowid_segment_writer);
        defer self.rowid_segment_writer.handle.close();
        const rowid_continue = try self.rowid_segment_writer.begin();

        var writer_idx: usize = 0;
        errdefer for (0..writer_idx) |idx| {
            freeSegment(tmp_arena, self.segment_db, &self.column_segment_writers[idx]);
        };
        defer for (0..writer_idx) |idx| {
            self.column_segment_writers[idx].handle.close();
        };
        for (self.column_segment_writers, 0..) |*writer, idx| {
            writer.* = try SegmentWriter.allocate(
                tmp_arena,
                self.segment_db,
                try self.column_segment_planners[idx].end(),
            );
            writer_idx += 1;
            const cont = try writer.begin();
            self.writers_continue.setValue(idx, cont);
        }

        // Write the data

        // Reset the sources back to where they were when creating the single row group started
        try pending_inserts.restart();
        self.row_group_cursor.reset();
        try self.row_group_cursor.skip(rg_cursor_start);

        for (0..new_row_group.row_group_entry.record_count) |idx| {
            // true means read from row group, false from staged inserts
            if (self.interleaving.isSet(idx)) {
                try self.writeRow(rowid_continue, pending_inserts);
                try pending_inserts.next();
            } else {
                try self.writeRow(rowid_continue, &self.row_group_cursor);
                try self.row_group_cursor.next();
            }
        }

        // Finalize the writers

        const rowid_handle = try self.rowid_segment_writer.end();
        new_row_group.row_group_entry.rowid_segment_id = rowid_handle.id;
        const column_segment_ids = new_row_group.row_group_entry.column_segment_ids;
        for (self.column_segment_writers, column_segment_ids) |*writer, *seg_id| {
            const handle = try writer.end();
            seg_id.* = handle.id;
        }
    }

    /// Feeds the first row into the planner and stores the start sort key and start rowid in
    /// `new_row_group`
    fn planFirstRow(
        self: *Creator,
        tmp_arena: *ArenaAllocator,
        row: anytype,
        new_row_group: *NewRowGroup,
    ) !void {
        const start_rowid_value = try row.readRowid();
        const start_rowid = start_rowid_value.asI64();
        new_row_group.start_rowid = start_rowid;

        self.rowid_segment_planner.next(start_rowid_value);

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
            planner.next(value);
        }
    }

    fn planRow(self: *Creator, row: anytype) !void {
        self.rowid_segment_planner.next(try row.readRowid());
        for (self.column_segment_planners, 0..) |*planner, idx| {
            planner.next(try row.readValue(idx));
        }
    }

    fn writeRow(self: *Creator, rowid_continue: bool, row: anytype) !void {
        if (rowid_continue) {
            try self.rowid_segment_writer.write(try row.readRowid());
        }
        for (self.column_segment_writers, 0..) |*writer, idx| {
            if (self.writers_continue.isSet(idx)) {
                try writer.write(try row.readValue(idx));
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

    fn freeSegment(
        tmp_arena: *ArenaAllocator,
        segment_db: *SegmentDb,
        writer: *SegmentWriter,
    ) void {
        segment_db.free(
            tmp_arena,
            writer.handle,
        ) catch |e| {
            std.log.err(
                "error freeing segment blob {d}: {any}",
                .{ writer.handle.id, e },
            );
        };
    }
};

const PendingInsertsRangeIter = struct {
    iter: *PendingInsertsIterator,
    start: u32,
    end: u32,
    idx: u32,
    eof: bool,

    fn init(iter: *PendingInsertsIterator, start: u32, end: u32) PendingInsertsRangeIter {
        return .{
            .iter = iter,
            .start = start,
            .end = end,
            .idx = start,
            .eof = start >= end,
        };
    }

    fn restart(self: *PendingInsertsRangeIter) !void {
        try self.iter.restart();
        try self.iter.skip(self.start);
        self.idx = self.start;
        self.eof = self.idx >= self.end;
    }

    fn markStart(self: *PendingInsertsRangeIter) void {
        self.start = self.idx;
    }

    fn next(self: *PendingInsertsRangeIter) !void {
        std.debug.assert(!self.eof);

        try self.iter.next();
        self.idx += 1;
        if (self.iter.eof or self.idx >= self.end) {
            self.eof = true;
        }
    }

    /// Returns whether the iterator is positioned at an eof row. This is only valid when the
    /// iterator is already in the eof state
    fn eofRow(self: *const PendingInsertsRangeIter) bool {
        std.debug.assert(self.eof);
        return !self.iter.eof or self.iter.eof_row;
    }

    fn sortKey(self: *const PendingInsertsRangeIter) PendingInsertsIterator.SortKey {
        return self.iter.sortKey();
    }

    fn readRowid(self: *const PendingInsertsRangeIter) !ValueRef {
        return self.iter.readRowid();
    }

    fn readValue(self: *const PendingInsertsRangeIter, idx: usize) !ValueRef {
        return self.iter.readValue(idx);
    }
};

pub const Cursor = struct {
    const Self = @This();

    segment_db: *SegmentDb,

    /// Allocator used to allocate all memory for this cursor that is tied to the
    /// lifecycle of the cursor. This memory is allocated together when the cursor is
    /// initialized and deallocated all once when the cursor is deinitialized.
    static_allocator: ArenaAllocator,

    column_types: []ColumnType,

    /// Set `row_group` before iterating. Set the record count on `row_group` to 0 to
    /// make an empty cursor.
    row_group: RowGroupEntry,

    rowid_segment: ?SegmentReader,
    segments: []?SegmentReader,

    /// Allocator used to allocate memory for values
    value_allocator: ArenaAllocator,

    index: u32,

    /// Initializes the row group cursor with an undefined row group. Set the row group
    /// before iterating
    pub fn init(
        allocator: Allocator,
        segment_db: *SegmentDb,
        schema: *const Schema,
    ) !Self {
        const col_len = schema.columns.items.len;

        var arena = ArenaAllocator.init(allocator);
        errdefer arena.deinit();

        const column_types = try arena.allocator().alloc(ColumnType, col_len);
        for (schema.columns.items, 0..) |*col, idx| {
            column_types[idx] = col.column_type;
        }
        const row_group = .{
            .rowid_segment_id = undefined,
            .column_segment_ids = try arena.allocator().alloc(i64, col_len),
            .record_count = 0,
        };
        const segments = try arena.allocator().alloc(?SegmentReader, col_len);
        for (segments) |*seg| {
            seg.* = null;
        }
        var value_allocator = ArenaAllocator.init(allocator);
        errdefer value_allocator.deinit();

        return .{
            .static_allocator = arena,
            .segment_db = segment_db,
            .column_types = column_types,
            .row_group = row_group,
            .rowid_segment = null,
            .segments = segments,
            .value_allocator = value_allocator,
            .index = 0,
        };
    }

    pub fn deinit(self: *Self) void {
        if (self.rowid_segment) |*s| {
            s.handle.close();
        }
        for (self.segments) |*seg| {
            if (seg.*) |*s| {
                s.handle.close();
            }
        }
        self.value_allocator.deinit();
        self.static_allocator.deinit();
    }

    pub fn rowGroup(self: *Self) *RowGroupEntry {
        return &self.row_group;
    }

    pub fn reset(self: *Self) void {
        self.index = 0;

        if (self.rowid_segment) |*s| {
            s.handle.close();
        }
        self.rowid_segment = null;
        for (self.segments) |*seg| {
            if (seg.*) |*s| {
                s.handle.close();
            }
            seg.* = null;
        }

        // TODO make the retained capacity limit configurable
        _ = self.value_allocator.reset(.{ .retain_with_limit = 4000 });
    }

    pub fn eof(self: *Self) bool {
        return self.index >= self.row_group.record_count;
    }

    pub fn next(self: *Self) !void {
        if (self.rowid_segment) |*seg| {
            try seg.next();
        }
        for (self.segments) |*seg| {
            if (seg.*) |*s| {
                try s.next();
            }
        }
        self.index += 1;
    }

    /// Does not check for eof
    pub fn skip(self: *Self, n: u32) !void {
        if (self.rowid_segment) |*seg| {
            for (0..n) |_| {
                try seg.next();
            }
        }
        for (self.segments) |*seg| {
            if (seg.*) |*s| {
                for (0..n) |_| {
                    try s.next();
                }
            }
        }
        self.index += n;
    }

    pub fn readRowid(self: *Self) !SegmentValue {
        if (self.rowid_segment == null) {
            try self.loadRowidSegment();
        }

        var seg = &self.rowid_segment.?;
        return seg.readNoAlloc();
    }

    pub fn readValue(self: *Self, col_idx: usize) !SegmentValue {
        if (self.segments[col_idx] == null) {
            try self.loadSegment(col_idx);
        }

        var seg = &self.segments[col_idx].?;
        return seg.read(self.value_allocator.allocator());
    }

    pub fn readInto(self: *Self, result: anytype, col_idx: usize) !void {
        if (self.segments[col_idx] == null) {
            try self.loadSegment(col_idx);
        }

        var seg = &self.segments[col_idx].?;
        try seg.readInto(self.value_allocator.allocator(), result);
    }

    fn loadRowidSegment(self: *Self) !void {
        const segment_reader = &self.rowid_segment;
        segment_reader.* = try SegmentReader.open(
            self.segment_db,
            ColumnType.Rowid.data_type,
            self.row_group.rowid_segment_id,
        );
        for (0..self.index) |_| {
            try segment_reader.*.?.next();
        }
    }

    fn loadSegment(self: *Self, col_idx: usize) !void {
        const segment_id = self.row_group.column_segment_ids[col_idx];

        const segment_reader = &self.segments[col_idx];
        segment_reader.* = try SegmentReader.open(
            self.segment_db,
            self.column_types[col_idx].data_type,
            segment_id,
        );
        for (0..self.index) |_| {
            try segment_reader.*.?.next();
        }
    }
};

test "row group: round trip" {
    const conn = try @import("sqlite3/Conn.zig").openInMemory();
    defer conn.close();

    var arena = ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const table_name = "test";

    try schema_mod.Db.createTable(&arena, conn, table_name);
    var schema_db = schema_mod.Db.init(conn, table_name);
    defer schema_db.deinit();

    try SegmentDb.createTable(&arena, conn, table_name);
    var segment_db = try SegmentDb.init(&arena, conn, table_name);
    defer segment_db.deinit();

    const schema_def = try schema_mod.SchemaDef.parse(&arena, &[_][]const u8{
        "quadrant TEXT NOT NULL",
        "sector INTEGER NOT NULL",
        "size INTEGER NOT NULL",
        "SORT KEY (quadrant, sector)",
    });
    const schema = try Schema.create(&arena, &arena, &schema_db, schema_def);

    var pidx = try PrimaryIndex.create(&arena, conn, table_name, &schema);

    var table_values = [_][3]MemoryValue{
        .{
            .{ .Text = "Alpha" }, .{ .Integer = 7 }, .{ .Integer = 100 },
        },
        .{
            .{ .Text = "Alpha" }, .{ .Integer = 9 }, .{ .Integer = 50 },
        },
        .{
            .{ .Text = "Gamma" }, .{ .Integer = 3 }, .{ .Integer = 75 },
        },
        .{
            .{ .Text = "Beta" }, .{ .Integer = 17 }, .{ .Integer = 105 },
        },
    };
    const rowids = [_]i64{ 1, 2, 4, 3 };

    for (&table_values) |*row| {
        _ = try pidx.insertInsertEntry(&arena, MemoryTuple{ .values = row });
    }

    {
        var node = try pidx.containingNodeHandle(
            &arena,
            MemoryTuple{ .values = &table_values[0] },
            1,
        );
        defer node.deinit();

        var creator = try Creator.init(std.testing.allocator, &arena, &segment_db, &schema, &pidx, 4);
        defer creator.deinit();
        const n = try creator.createN(&arena, node, 1);
        try std.testing.expectEqual(@as(usize, 1), n);
    }

    var cursor = try Cursor.init(arena.allocator(), &segment_db, &schema);
    var node = try pidx.containingNodeHandle(
        &arena,
        MemoryTuple{ .values = &table_values[0] },
        1,
    );
    defer node.deinit();
    try node.readEntryInto(cursor.rowGroup());
    try std.testing.expect(cursor.rowGroup().record_count > 0);

    var idx: usize = 0;
    while (!cursor.eof()) {
        const rowid = try cursor.readRowid();
        try std.testing.expectEqual(rowids[idx], rowid.asI64());

        // Rows are in sort key order not insert order. Use the expected rowid as the
        // index
        const rowIdx: usize = @as(usize, @intCast(rowids[idx])) - 1;
        const quadrant = (try cursor.readValue(0)).asText();
        try std.testing.expectEqualStrings(table_values[rowIdx][0].Text, quadrant);
        const sector = (try cursor.readValue(1)).asI64();
        try std.testing.expectEqual(table_values[rowIdx][1].Integer, sector);

        idx += 1;
        try cursor.next();
    }
}

pub fn benchRowGroupCreate() !void {
    const row_group_len: u32 = 10_000;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    try tmp_dir.dir.makeDir(&tmp_dir.sub_path);
    var path_buf: [200]u8 = undefined;
    const tmp_path = try tmp_dir.parent_dir.realpath(&tmp_dir.sub_path, &path_buf);
    const db_path = try std.fs.path.joinZ(std.heap.page_allocator, &[_][]const u8{
        tmp_path,
        "bench.db",
    });

    const Conn = @import("sqlite3/Conn.zig");
    const conn = try Conn.open(db_path);
    defer conn.close();

    var arena = ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const table_name = "test";

    try schema_mod.Db.createTable(&arena, conn, table_name);
    var schema_db = schema_mod.Db.init(conn, table_name);
    defer schema_db.deinit();

    try SegmentDb.createTable(&arena, conn, table_name);
    var segment_db = try SegmentDb.init(&arena, conn, table_name);
    defer segment_db.deinit();

    const schema_def = try schema_mod.SchemaDef.parse(&arena, &[_][]const u8{
        "quadrant TEXT NOT NULL",
        "sector INTEGER NOT NULL",
        "size INTEGER NULL",
        "gravity FLOAT NULL",
        "name TEXT NOT NULL",
        "SORT KEY (quadrant, sector)",
    });
    const schema = try Schema.create(&arena, &arena, &schema_db, schema_def);

    var pidx = try PrimaryIndex.create(&arena, conn, table_name, &schema);

    const seed = @as(u64, @truncate(@as(u128, @bitCast(std.time.nanoTimestamp()))));
    var prng = std.rand.DefaultPrng.init(seed);

    // Create a row group from pending inserts only (no merge)

    try conn.exec("BEGIN");
    const start_insert = std.time.microTimestamp();
    for (0..row_group_len) |_| {
        var row = randomRow(&prng);
        _ = try pidx.insertInsertEntry(&arena, MemoryTuple{ .values = &row });
    }
    try conn.exec("COMMIT");
    const end_insert = std.time.microTimestamp();
    std.log.err("insert pending insert: total: {d} micros, per insert: {d} micros", .{
        end_insert - start_insert,
        @divTrunc(end_insert - start_insert, row_group_len),
    });

    {
        var start: i64 = undefined;
        var n: usize = undefined;
        {
            var node = try pidx.startNodeHandle();
            defer node.deinit();

            var creator = try Creator.init(
                std.heap.page_allocator,
                &arena,
                &segment_db,
                &schema,
                &pidx,
                row_group_len,
            );
            defer creator.deinit();

            try conn.exec("BEGIN");
            start = std.time.microTimestamp();
            n = try creator.createN(&arena, node, 1);
        }
        try conn.exec("COMMIT");
        const end = std.time.microTimestamp();
        std.log.err("create row group: {d} micros", .{end - start});
        std.debug.assert(n == 1);
    }

    // Create a row group with a merge

    try conn.exec("BEGIN");
    for (0..(row_group_len * 3)) |_| {
        var row = randomRow(&prng);
        _ = try pidx.insertInsertEntry(&arena, MemoryTuple{ .values = &row });
    }
    try conn.exec("COMMIT");

    {
        var start: i64 = undefined;
        var n: usize = undefined;
        {
            var end_row = randomRow(&prng);
            end_row[0] = .{ .Text = "ZZZZ" };
            var node = try pidx.containingNodeHandle(&arena, MemoryTuple{ .values = &end_row }, 1);
            defer node.deinit();

            var creator = try Creator.init(
                std.heap.page_allocator,
                &arena,
                &segment_db,
                &schema,
                &pidx,
                row_group_len,
            );
            defer creator.deinit();

            try conn.exec("BEGIN");
            start = std.time.microTimestamp();
            n = try creator.createN(&arena, node, 1);
        }
        try conn.exec("COMMIT");
        const end = std.time.microTimestamp();
        std.log.err("create row group: {d} micros", .{end - start});
        std.debug.assert(n == 1);
    }
}

const quadrants = [_][]const u8{ "Alpha", "Beta", "Gamma", "Delta" };

fn randomRow(prng: *std.rand.DefaultPrng) [5]MemoryValue {
    const quadrant = quadrants[prng.random().intRangeLessThan(usize, 0, quadrants.len)];

    return [5]MemoryValue{
        .{ .Text = quadrant },
        .{ .Integer = prng.random().int(i64) },
        .{ .Integer = 100 },
        .{ .Float = 3.75 },
        .{ .Text = "Veridian 3" },
    };
}
