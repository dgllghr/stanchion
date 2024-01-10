//! Logic for choosing the best index using data passed to `xBestIndex` and passing data about that
//! index from `xBestIndex` to `xFilter`.

const std = @import("std");
const debug = std.debug;
const mem = std.mem;
const meta = std.meta;
const testing = std.testing;
const ArenaAllocator = std.heap.ArenaAllocator;

const vtab = @import("sqlite3/vtab.zig");
const BestIndexInfo = vtab.BestIndexInfo;
const BestIndexInfoOp = vtab.BestIndexInfo.Op;
const Constraint = vtab.BestIndexInfo.Constraint;
const FilterArgs = vtab.FilterArgs;

const sort_key_index = @import("index/sort_key.zig");
const IndexCode = @import("index/code.zig").Code;

pub const SortKeyRange = sort_key_index.SortKeyRange;
pub const RangeOp = sort_key_index.RangeOp;

pub const Index = union(IndexCode) {
    sort_key: sort_key_index.SortKeyRange,

    pub fn deserialize(identifier: u32, args: FilterArgs) !?Index {
        const index_code = meta.intToEnum(IndexCode, identifier & 0x1) catch {
            return null;
        };
        switch (index_code) {
            .sort_key => {
                const sk_range = try SortKeyRange.deserialize(@truncate(identifier >> 8), args);
                return .{ .sort_key = sk_range };
            },
        }
    }
};

pub fn chooseBestIndex(
    tmp_arena: *ArenaAllocator,
    sort_key_columns: []const usize,
    best_index_info: BestIndexInfo,
) !void {
    std.log.debug("choosing best index from constraints:", .{});
    for (0..best_index_info.constraintsLen()) |idx| {
        std.log.debug("constraint: {}", .{best_index_info.constraint(idx)});
    }

    // Note that "there is no guarantee that `xFilter` will be called following a successful
    // xBestIndex", which means it is tricky to determine when to deallocate data stored about the
    // selected index. Because of that, encode the data needed by the cursor in the identifiers,
    // which sqlite passes to the xFilter callback.

    // TODO consider the collation using `sqlite3_vtab_collation` for each text column to do text
    //      comparisons properly

    // Currently, only the sort key index is supported
    const use_sk = try sort_key_index.attemptSetup(tmp_arena, sort_key_columns, best_index_info);
    if (!use_sk) {
        best_index_info.setIdentifier(0);
    }
}

test {
    _ = sort_key_index;
}
