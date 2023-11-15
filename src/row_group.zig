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
const PendingInsertsIterator = PrimaryIndex.PendingInsertsIterator;

pub const Creator = struct {
    sort_key: []const usize,
    column_segment_planners: []SegmentPlanner,
    column_segment_writers: []SegmentWriter,
    writers_continue: DynamicBitSetUnmanaged,
    start_sort_key: []MemoryValue,
    row_group_entry: RowGroupEntry,

    pub fn init(table_static_arena: *ArenaAllocator, schema: *const Schema) !Creator {
        const sort_key_len = schema.sort_key.items.len;
        const columns_len = schema.columns.items.len;

        const allocator = table_static_arena.allocator();

        var planners = try allocator.alloc(SegmentPlanner, columns_len);
        for (planners, schema.columns.items) |*planner, *col| {
            planner.* = SegmentPlanner.init(col.column_type);
        }

        const writers = try allocator.alloc(SegmentWriter, columns_len);
        const writers_continue = try DynamicBitSetUnmanaged.initEmpty(
            allocator,
            columns_len,
        );

        const start_sort_key = try allocator.alloc(MemoryValue, sort_key_len);
        const row_group_entry: RowGroupEntry = .{
            .rowid_segment_id = undefined,
            .column_segment_ids = try allocator.alloc(i64, columns_len),
            .record_count = 0,
        };

        return .{
            .sort_key = schema.sort_key.items,
            .column_segment_planners = planners,
            .column_segment_writers = writers,
            .writers_continue = writers_continue,
            .start_sort_key = start_sort_key,
            .row_group_entry = row_group_entry,
        };
    }

    pub fn reset(self: *Creator) void {
        // TODO it may be wise to 0-out the rest of the fields even if their values are
        //      always overriden during create
        for (self.column_segment_planners) |*p| {
            p.reset();
        }
        self.row_group_entry.record_count = 0;
    }

    pub fn create(
        self: *Creator,
        tmp_arena: *ArenaAllocator,
        segment_db: *SegmentDb,
        primary_index: *PrimaryIndex,
        records: *PendingInsertsIterator,
    ) !void {
        //
        // Plan
        //

        // If there are no pending records, there is nothing to do
        if (records.eof) {
            return;
        }

        // Assign the start sort key and start row id using the first row

        const start_rowid_value = try records.readRowid();
        const start_rowid = start_rowid_value.asI64();

        var rowid_segment_planner = SegmentPlanner.init(ColumnType.Rowid);
        rowid_segment_planner.next(start_rowid_value);

        for (self.column_segment_planners, 0..) |*planner, idx| {
            const value = try records.readValue(idx);
            for (self.sort_key, 0..) |sk_col_rank, sk_rank| {
                if (sk_col_rank == idx) {
                    const mem_value = try MemoryValue.fromRef(
                        tmp_arena.allocator(),
                        value,
                    );
                    self.start_sort_key[sk_rank] = mem_value;
                    break;
                }
            }
            planner.next(value);
        }

        self.row_group_entry.record_count += 1;
        try records.next();

        // Plan the rest of the rows

        while (!records.eof) {
            rowid_segment_planner.next(try records.readRowid());
            for (self.column_segment_planners, 0..) |*planner, idx| {
                planner.next(try records.readValue(idx));
            }
            self.row_group_entry.record_count += 1;
            try records.next();
        }

        //
        // Write
        //

        // Init rowid segment writer

        var rowid_segment_writer = try SegmentWriter.allocate(
            tmp_arena,
            segment_db,
            try rowid_segment_planner.end(),
        );
        errdefer freeSegment(tmp_arena, segment_db, &rowid_segment_writer);
        defer rowid_segment_writer.handle.close();
        const rowid_continue = try rowid_segment_writer.begin();

        // Init column segment writers

        var writer_idx: usize = 0;
        errdefer for (0..writer_idx) |idx| {
            freeSegment(tmp_arena, segment_db, &self.column_segment_writers[idx]);
        };
        defer for (0..writer_idx) |idx| {
            self.column_segment_writers[idx].handle.close();
        };
        for (self.column_segment_writers, 0..) |*writer, idx| {
            writer.* = try SegmentWriter.allocate(
                tmp_arena,
                segment_db,
                try self.column_segment_planners[idx].end(),
            );
            writer_idx += 1;
            const cont = try writer.begin();
            self.writers_continue.setValue(idx, cont);
        }

        // Write the data

        try records.restart();

        while (!records.eof) {
            if (rowid_continue) {
                try rowid_segment_writer.write(try records.readRowid());
            }
            for (self.column_segment_writers, 0..) |*writer, idx| {
                if (self.writers_continue.isSet(idx)) {
                    try writer.write(try records.readValue(idx));
                }
            }
            try records.next();
        }

        // Finalize the writers

        const rowid_handle = try rowid_segment_writer.end();
        self.row_group_entry.rowid_segment_id = rowid_handle.id;
        var column_segment_ids = self.row_group_entry.column_segment_ids;
        for (self.column_segment_writers, column_segment_ids) |*writer, *seg_id| {
            const handle = try writer.end();
            seg_id.* = handle.id;
        }

        //
        // Update primary index
        //

        try primary_index.deleteStagedInsertsRange(
            tmp_arena,
            self.start_sort_key,
            start_rowid,
        );

        try primary_index.insertRowGroupEntry(
            tmp_arena,
            self.start_sort_key,
            start_rowid,
            &self.row_group_entry,
        );
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
        var segments = try arena.allocator().alloc(?SegmentReader, col_len);
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

        self.rowid_segment = null;
        for (self.segments) |*seg| {
            seg.* = null;
        }

        // TODO make the retained capacity limit configurable
        _ = self.value_allocator.reset(.{ .retain_with_limit = 1024 });
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
        var node = try pidx.containingNodeHandle(
            &arena,
            MemoryTuple{ .values = &table_values[0] },
            1,
        );
        defer node.deinit();

        var iter = try node.pendingInserts(&arena);

        var creator = try Creator.init(&arena, &schema);
        try creator.create(&arena, &segment_db, &pidx, iter);
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
