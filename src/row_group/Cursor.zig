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

const sql_fmt = @import("../sql_fmt.zig");

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

const CursorRange = @import("../CursorRange.zig");

const Index = @import("Index.zig");

blob_manager: *BlobManager,

/// Allocator used to allocate all memory for this cursor that is tied to the lifecycle of the
/// cursor. This memory is allocated together when the cursor is initialized and deallocated all
/// once when the cursor is deinitialized.
static_allocator: ArenaAllocator,

column_types: []ColumnType,

/// Set `row_group` before iterating. Set the record count on `row_group` to 0 to make an empty
/// cursor.
row_group: Index.Entry,

rowid_blob_handle: BlobHandle,
segment_blob_handles: []BlobHandle,

rowid_segment: ?SegmentReader,
segments: []?SegmentReader,

/// Allocator used to allocate memory for values
value_allocator: ArenaAllocator,

index: u32,

const Self = @This();

/// Initializes the row group cursor with an undefined row group. Set the row group before
/// iterating
pub fn init(allocator: Allocator, blob_manager: *BlobManager, schema: *const Schema) !Self {
    const col_len = schema.columns.len;

    var arena = ArenaAllocator.init(allocator);
    errdefer arena.deinit();

    const column_types = try arena.allocator().alloc(ColumnType, col_len);
    for (schema.columns, 0..) |*col, idx| {
        column_types[idx] = col.column_type;
    }
    const row_group = .{
        .rowid_segment_id = undefined,
        .column_segment_ids = try arena.allocator().alloc(i64, col_len),
        .record_count = 0,
    };
    const segment_blob_handles = try arena.allocator().alloc(BlobHandle, col_len);
    const segments = try arena.allocator().alloc(?SegmentReader, col_len);
    for (segments) |*seg| {
        seg.* = null;
    }
    var value_allocator = ArenaAllocator.init(allocator);
    errdefer value_allocator.deinit();

    return .{
        .static_allocator = arena,
        .blob_manager = blob_manager,
        .column_types = column_types,
        .row_group = row_group,
        .rowid_blob_handle = undefined,
        .segment_blob_handles = segment_blob_handles,
        .rowid_segment = null,
        .segments = segments,
        .value_allocator = value_allocator,
        .index = 0,
    };
}

pub fn deinit(self: *Self) void {
    if (self.rowid_segment) |_| {
        self.rowid_blob_handle.tryClose();
    }
    for (self.segments, self.segment_blob_handles) |*seg, *blob_handle| {
        if (seg.*) |_| {
            blob_handle.tryClose();
        }
    }
    self.value_allocator.deinit();
    self.static_allocator.deinit();
}

pub fn rowGroup(self: *Self) *Index.Entry {
    return &self.row_group;
}

pub fn reset(self: *Self) void {
    self.index = 0;

    if (self.rowid_segment) |_| {
        self.rowid_blob_handle.tryClose();
    }
    self.rowid_segment = null;
    for (self.segments, self.segment_blob_handles) |*seg, *blob_handle| {
        if (seg.*) |_| {
            blob_handle.tryClose();
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
    // TODO make the retained capacity limit configurable
    _ = self.value_allocator.reset(.{ .retain_with_limit = 4000 });
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
    self.rowid_blob_handle = try self.blob_manager.open(self.row_group.rowid_segment_id);
    errdefer self.rowid_blob_handle.tryClose();

    const segment_reader = &self.rowid_segment;
    segment_reader.* = try SegmentReader.init(
        &self.rowid_blob_handle,
        ColumnType.Rowid.data_type,
    );

    for (0..self.index) |_| {
        try segment_reader.*.?.next();
    }
}

fn loadSegment(self: *Self, col_idx: usize) !void {
    const segment_id = self.row_group.column_segment_ids[col_idx];
    const seg_blob_handle = &self.segment_blob_handles[col_idx];

    seg_blob_handle.* = try self.blob_manager.open(segment_id);
    errdefer seg_blob_handle.tryClose();

    const segment_reader = &self.segments[col_idx];
    segment_reader.* = try SegmentReader.init(
        seg_blob_handle,
        self.column_types[col_idx].data_type,
    );

    for (0..self.index) |_| {
        try segment_reader.*.?.next();
    }
}
