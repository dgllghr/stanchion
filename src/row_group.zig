const std = @import("std");
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;
const ArrayListUnmanaged = std.ArrayListUnmanaged;
const DynamicBitSetUnmanaged = std.bit_set.DynamicBitSetUnmanaged;

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

const OwnedValue = @import("value.zig").OwnedValue;

const PrimaryIndex = @import("PrimaryIndex.zig");
const RowGroupEntry = PrimaryIndex.RowGroupEntry;
const StagedInsertsIterator = PrimaryIndex.StagedInsertsIterator;

pub fn create(
    allocator: Allocator,
    schema: *const Schema,
    segment_db: *SegmentDb,
    primary_index: *PrimaryIndex,
    records: *StagedInsertsIterator,
) !void {
    var arena = ArenaAllocator.init(allocator);
    defer arena.deinit();

    const sort_key_len = schema.sort_key.items.len;
    const columns_len = schema.columns.items.len;

    // TODO this memory can be reused across row group creations for the same table
    //      because the memory allocated is proportional to the table width. Move this
    //      into a struct so it can be reused
    // TODO or better yet, refactor the staged inserts so that they can be iterated
    //      column-oriented and this memory does not need to be allocated at all

    var rowid_planner: SegmentPlanner = undefined;
    var planners = try arena.allocator().alloc(SegmentPlanner, columns_len);
    var rowid_plan: SegmentPlan = undefined;
    var plans = try arena.allocator().alloc(SegmentPlan, columns_len);
    var rowid_writer: SegmentWriter = undefined;
    var writers = try arena.allocator().alloc(SegmentWriter, columns_len);
    var rowid_continue = false;
    var writers_continue = try DynamicBitSetUnmanaged.initEmpty(
        arena.allocator(),
        columns_len,
    );
    var start_sort_key = try arena.allocator().alloc(OwnedValue, sort_key_len);
    var start_rowid: i64 = undefined;
    var row_group_entry: RowGroupEntry = .{
        .rowid_segment_id = undefined,
        .column_segment_ids = try arena.allocator().alloc(i64, columns_len),
        .record_count = 0,
    };

    // Plan

    rowid_planner = SegmentPlanner.init(ColumnType.Rowid);
    for (planners, schema.columns.items) |*planner, *col| {
        planner.* = SegmentPlanner.init(col.column_type);
    }

    // Assign the start sort key and start row id using the first row
    // TODO if this returns false it should be an error (empty)
    _ = try records.next();
    const start_rowid_value = records.readRowId();
    start_rowid = start_rowid_value.asI64();
    rowid_planner.next(start_rowid_value);
    for (planners, 0..) |*planner, idx| {
        const value = records.readValue(idx);
        for (schema.sort_key.items, 0..) |sk_col_rank, sk_rank| {
            if (sk_col_rank == idx) {
                const owned_value = try OwnedValue.fromRef(arena.allocator(), value);
                start_sort_key[sk_rank] = owned_value;
                break;
            }
        }
        planner.next(value);
    }
    row_group_entry.record_count += 1;

    while (try records.next()) {
        rowid_planner.next(records.readRowId());
        for (planners, 0..) |*planner, idx| {
            planner.next(records.readValue(idx));
        }
        row_group_entry.record_count += 1;
    }

    rowid_plan = try rowid_planner.end();
    for (plans, planners) |*plan, *planner| {
        plan.* = try planner.end();
    }

    // Write
    // TODO free all segment blobs on error
    rowid_writer = try SegmentWriter.allocate(segment_db, rowid_plan);
    rowid_continue = try rowid_writer.begin();
    for (writers, plans, 0..) |*writer, *plan, idx| {
        writer.* = try SegmentWriter.allocate(segment_db, plan.*);
        const cont = try writer.begin();
        writers_continue.setValue(idx, cont);
    }

    try records.restart();
    while (try records.next()) {
        if (rowid_continue) {
            try rowid_writer.write(records.readRowId());
        }
        for (writers, 0..) |*writer, idx| {
            if (writers_continue.isSet(idx)) {
                try writer.write(records.readValue(idx));
            }
        }
    }

    var handle = try rowid_writer.end();
    row_group_entry.rowid_segment_id = handle.id;
    handle.close();
    for (writers, 0..) |*writer, idx| {
        handle = try writer.end();
        row_group_entry.column_segment_ids[idx] = handle.id;
        handle.close();
    }

    // Update index

    try primary_index.deleteStagedInsertsRange(start_sort_key, start_rowid);

    try primary_index.insertRowGroupEntry(start_sort_key, start_rowid, &row_group_entry);
}

pub const Cursor = struct {
    const Self = @This();

    segment_db: *SegmentDb,

    /// Allocator used to allocate all memory for this cursor that is tied to the
    /// lifecycle of the cursor. This memory is allocated together when the cursor is
    /// initialized and deallocated all once when the cursor is deinitialized.
    static_allocator: ArenaAllocator,

    column_types: []ColumnType,
    row_group: RowGroupEntry,

    rowid_segment: ?SegmentReader,
    segments: []?SegmentReader,

    /// Allocator used to allocate memory for values
    value_buffer_allocator: Allocator,
    value_buffers: []ArrayListUnmanaged(u8),
    row_rowid: ?i64,
    row_values: []?SegmentValue,

    /// Index is initialized to 0, but because `next` is called first to see if the
    /// iterator has a next element, index is off by 1.
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
            .record_count = undefined,
        };
        var segments = try arena.allocator().alloc(?SegmentReader, col_len);
        for (segments) |*seg| {
            seg.* = null;
        }
        var value_buffers = try arena.allocator().alloc(
            ArrayListUnmanaged(u8),
            col_len,
        );
        for (value_buffers) |*buf| {
            buf.* = ArrayListUnmanaged(u8){};
        }
        // No need to initialize the row values to null because next is called first
        // and that nulls out the row values
        const row_values = try arena.allocator().alloc(?SegmentValue, col_len);

        return .{
            .static_allocator = arena,
            .segment_db = segment_db,
            .column_types = column_types,
            .row_group = row_group,
            .rowid_segment = null,
            .segments = segments,
            .value_buffer_allocator = allocator,
            .value_buffers = value_buffers,
            .row_rowid = null,
            .row_values = row_values,
            .index = 0,
        };
    }

    pub fn deinit(self: *Self) void {
        if (self.rowid_segment) |*s| {
            s.deinit(self.value_buffer_allocator);
        }
        for (self.segments) |*seg| {
            if (seg) |*s| {
                s.deinit(self.value_buffer_allocator);
            }
        }
        for (self.value_buffers) |*buf| {
            buf.deinit(self.value_buffer_allocator);
        }
        self.static_allocator.deinit();
    }

    pub fn rowGroup(self: *Self) *RowGroupEntry {
        return &self.row_group;
    }

    pub fn reset(self: *Self) void {
        self.index = 0;

        self.rowid_segment = null;
        for (self.segments) |*seg| {
            if (seg) |*s| {
                s.deinit(self.value_buffer_allocator);
                s.* = null;
            }
        }

        self.row_rowid = null;
        for (self.row_values) |*v| {
            v.* = null;
        }
    }

    pub fn next(self: *Self) !bool {
        if (self.row_rowid == null) {
            if (self.rowid_segment) |*seg| {
                try seg.skip();
            }
        }
        self.row_rowid = null;
        for (self.row_values, 0..) |*v, idx| {
            // Advance the segment reader for every loaded segment that was not read
            // (does not have a cached value)
            if (v.* == null) {
                if (self.segments[idx]) |*seg| {
                    try seg.skip();
                }
            }
            v.* = null;
        }
        self.index += 1;
        return self.index <= self.row_group.record_count;
    }

    pub fn readRowid(self: *Self) !i64 {
        std.debug.assert(self.index > 0);

        if (self.row_rowid) |rowid| {
            return rowid;
        }

        if (self.rowid_segment == null) {
            try self.loadRowidSegment();
        }

        var seg = &self.rowid_segment.?;
        const value = try seg.readNoAlloc();
        self.row_rowid = value.asI64();
        return self.row_rowid.?;
    }

    pub fn read(self: *Self, col_idx: usize) !SegmentValue {
        std.debug.assert(self.index > 0);

        if (self.row_values[col_idx]) |v| {
            return v;
        }

        if (self.segments[col_idx] == null) {
            try self.loadSegment(col_idx);
        }

        var seg = &self.segments[col_idx].?;
        const value = try seg.read(self.value_buffer_allocator, &self.value_buffers[col_idx]);
        self.row_values[col_idx] = value;
        return value;
    }

    fn loadRowidSegment(self: *Self) !void {
        var segment_reader = try SegmentReader.open(
            self.segment_db,
            ColumnType.Rowid.data_type,
            self.row_group.rowid_segment_id,
        );
        for (1..self.index) |_| {
            try segment_reader.skip();
        }
        self.rowid_segment = segment_reader;
    }

    fn loadSegment(self: *Self, col_idx: usize) !void {
        const segment_id = self.row_group.column_segment_ids[col_idx];
        var segment_reader = try SegmentReader.open(
            self.segment_db,
            self.column_types[col_idx].data_type,
            segment_id,
        );
        for (1..self.index) |_| {
            try segment_reader.skip();
        }
        self.segments[col_idx] = segment_reader;
    }
};

test "row group: round trip" {
    const OwnedRow = @import("value.zig").OwnedRow;

    const conn = try @import("sqlite3/Conn.zig").openInMemory();
    defer conn.close();
    try @import("db.zig").Migrations.apply(conn);

    var db = SegmentDb.init(conn);
    defer db.deinit();

    var arena = ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var schema_db = schema_mod.Db.init(conn);
    defer schema_db.deinit();
    var segment_db = SegmentDb.init(conn);
    defer segment_db.deinit();

    const schema_def = try schema_mod.SchemaDef.parse(&arena, &[_][]const u8{
        "quadrant TEXT NOT NULL",
        "sector INTEGER NOT NULL",
        "size INTEGER NOT NULL",
        "SORT KEY (quadrant, sector)",
    });
    const schema = try Schema.create(arena.allocator(), &schema_db, 1, schema_def);

    var pidx = try PrimaryIndex.create(&arena, &arena, conn, "foo", &schema);

    var table_values = [_][3]OwnedValue{
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
        _ = try pidx.insertInsertEntry(OwnedRow.init(null, row));
    }

    {
        var rg_entry_handle = try pidx.precedingRowGroup(
            1,
            OwnedRow.init(null, &table_values[0]),
        );
        defer rg_entry_handle.deinit();
        var iter = try pidx.stagedInserts(&rg_entry_handle);
        defer iter.deinit();

        try create(arena.allocator(), &schema, &segment_db, &pidx, &iter);
    }

    var cursor = try Cursor.init(arena.allocator(), &segment_db, &schema);
    var rg_entry_handle = try pidx.precedingRowGroup(
        1,
        OwnedRow.init(null, &table_values[0]),
    );
    defer rg_entry_handle.deinit();
    const row_group = rg_entry_handle.row_group;
    try pidx.readRowGroupEntry(cursor.rowGroup(), row_group.sort_key, row_group.rowid);

    var idx: usize = 0;
    while (try cursor.next()) {
        const rowid = try cursor.readRowid();
        try std.testing.expectEqual(rowids[idx], rowid);

        // Rows are in sort key order not insert order. Use the expected rowid as the
        // index
        const rowIdx: usize = @as(usize, @intCast(rowids[idx])) - 1;
        const quadrant = (try cursor.read(0)).asText();
        try std.testing.expectEqualStrings(table_values[rowIdx][0].Text, quadrant);
        const sector = (try cursor.read(1)).asI64();
        try std.testing.expectEqual(table_values[rowIdx][1].Integer, sector);

        idx += 1;
    }
}
