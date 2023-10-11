const std = @import("std");
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;
const DynamicBitSetUnmanaged = std.bit_set.DynamicBitSetUnmanaged;

const schema_mod = @import("schema.zig");
const ColumnType = schema_mod.ColumnType;
const Schema = schema_mod.Schema;

const segment = @import("segment.zig");
const SegmentDb = segment.Db;
const SegmentHandle = segment.Handle;
const SegmentPlan = segment.Plan;
const SegmentPlanner = segment.Planner;
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
