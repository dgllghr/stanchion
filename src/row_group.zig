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

const MemoryValue = @import("value.zig").MemoryValue;

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
    const sort_key_len = schema.sort_key.items.len;
    const columns_len = schema.columns.items.len;

    // TODO this memory can be reused across row group creations for the same table
    //      because the memory allocated is proportional to the table width. Move this
    //      into a struct so it can be reused
    // TODO or better yet, refactor the staged inserts so that they can be iterated
    //      column-oriented and this memory does not need to be allocated at all

    var arena = ArenaAllocator.init(allocator);
    defer arena.deinit();

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
    var start_sort_key = try arena.allocator().alloc(MemoryValue, sort_key_len);
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
    // TODO if this returns false it should be an error (empty row group)
    _ = try records.next();
    const start_rowid_value = records.readRowId();
    start_rowid = start_rowid_value.asI64();
    rowid_planner.next(start_rowid_value);
    for (planners, 0..) |*planner, idx| {
        const value = records.readValue(idx);
        for (schema.sort_key.items, 0..) |sk_col_rank, sk_rank| {
            if (sk_col_rank == idx) {
                const owned_value = try MemoryValue.fromRef(arena.allocator(), value);
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
    rowid_writer = try SegmentWriter.allocate(&arena, segment_db, rowid_plan);
    errdefer segment_db.free(&arena, rowid_writer.handle) catch |e| {
        std.log.err("error freeing segment blob {d}: {any}", .{ rowid_writer.handle.id, e });
    };
    defer rowid_writer.handle.close();
    rowid_continue = try rowid_writer.begin();
    var writer_idx: usize = 0;
    errdefer for (0..writer_idx) |idx| {
        segment_db.free(&arena, writers[idx].handle) catch |e| {
            std.log.err("error freeing segment blob {d}: {any}", .{ writers[idx].handle.id, e });
        };
    };
    defer for (0..writer_idx) |idx| {
        writers[idx].handle.close();
    };
    for (writers, plans) |*writer, *plan| {
        writer.* = try SegmentWriter.allocate(&arena, segment_db, plan.*);
        // TODO if an error occurs here, the blob handle won't be closed because
        //      `writer_idx` has not been incremented in this iteration
        const cont = try writer.begin();
        writers_continue.setValue(writer_idx, cont);
        writer_idx += 1;
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
    for (writers, 0..) |*writer, idx| {
        handle = try writer.end();
        row_group_entry.column_segment_ids[idx] = handle.id;
    }

    // Update index

    try primary_index.deleteStagedInsertsRange(
        &arena,
        start_sort_key,
        start_rowid,
    );

    try primary_index.insertRowGroupEntry(
        &arena,
        start_sort_key,
        start_rowid,
        &row_group_entry,
    );
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
        var value_buffers = try arena.allocator().alloc(ArrayListUnmanaged(u8), col_len);
        for (value_buffers) |*buf| {
            buf.* = ArrayListUnmanaged(u8){};
        }

        return .{
            .static_allocator = arena,
            .segment_db = segment_db,
            .column_types = column_types,
            .row_group = row_group,
            .rowid_segment = null,
            .segments = segments,
            .value_buffer_allocator = allocator,
            .value_buffers = value_buffers,
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
            seg.* = null;
        }
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

    pub fn readRowid(self: *Self) !i64 {
        if (self.rowid_segment == null) {
            try self.loadRowidSegment();
        }

        var seg = &self.rowid_segment.?;
        const value = try seg.readNoAlloc();
        return value.asI64();
    }

    pub fn read(self: *Self, col_idx: usize) !SegmentValue {
        if (self.segments[col_idx] == null) {
            try self.loadSegment(col_idx);
        }

        var seg = &self.segments[col_idx].?;
        return seg.read(self.value_buffer_allocator, &self.value_buffers[col_idx]);
    }

    pub fn readInto(self: *Self, result: anytype, col_idx: usize) !void {
        if (self.segments[col_idx] == null) {
            try self.loadSegment(col_idx);
        }

        var seg = &self.segments[col_idx].?;
        try seg.readInto(
            result,
            self.value_buffer_allocator,
            &self.value_buffers[col_idx],
        );
    }

    fn loadRowidSegment(self: *Self) !void {
        var segment_reader = &self.rowid_segment;
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

        var segment_reader = &self.segments[col_idx];
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
    const MemoryTuple = @import("value.zig").MemoryTuple;

    const conn = try @import("sqlite3/Conn.zig").openInMemory();
    defer conn.close();
    try @import("db.zig").Migrations.apply(conn);

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
    const schema = try Schema.create(arena.allocator(), &arena, &schema_db, schema_def);

    var pidx = try PrimaryIndex.create(&arena, conn, "foo", &schema);

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
        var rg_entry_handle = try pidx.precedingRowGroup(
            &arena,
            1,
            MemoryTuple{ .values = &table_values[0] },
        );
        defer rg_entry_handle.deinit();
        var iter = try pidx.stagedInserts(&arena, &rg_entry_handle);
        defer iter.deinit();

        try create(arena.allocator(), &schema, &segment_db, &pidx, &iter);
    }

    var cursor = try Cursor.init(arena.allocator(), &segment_db, &schema);
    var rg_entry_handle = try pidx.precedingRowGroup(
        &arena,
        1,
        MemoryTuple{ .values = &table_values[0] },
    );
    defer rg_entry_handle.deinit();
    const row_group = rg_entry_handle.row_group;
    try pidx.readRowGroupEntry(
        &arena,
        cursor.rowGroup(),
        row_group.sortKey(),
        row_group.rowid,
    );

    var idx: usize = 0;
    while (!cursor.eof()) {
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
        try cursor.next();
    }
}
