//! The sort key index may be a composite index, and the the postgres documentation has an
//! excellent summary of the logic for determining if using a composite index is possible:
//! > The exact rule is that equality constraints on leading columns, plus any inequality
//! > constraints on the first column that does not have an equality constraint, will be used
//! > to limit the portion of the index that is scanned.
//!
//! The sort key index is encoded using the operation on the last column in the sort key index for
//! which the above rule applies. This is called the last operation (`last_op`) and may be an
//! equality, inequality, or range constraint. Values are requested from sqlite for all sort key
//! columns for which the above rule applies.
//!
//! For the following examples, a sort key is defined as columns `a`, `b`, and `c`:
//!
//! `a = 1 and b < 10`
//! The last op is `<` and values are requested for column `a` and column `b`.
//!
//! `a = 1 and b = 100 and c = 1000`
//!  The last op is `=` and values are requested for columns `a`, `b`, and `c`.
//!
//! The `between` op restricts a column to be within a range of values. It is used like all other
//! ops as a last op and only applies to one column. Therefore, ranges on the sort key only work
//! when the range applies to a single column and all leading columns have equality constraints
//! on them. They do not work when the range includes different values for leading columns. This
//! limitation is due to how sqlite processes constraints. It never calls the `xBestIndex` callback
//! with a set of constraints that would capture the whole range.
//!
//! `a = 1 and b > 10 and b <= 100`
//! The last op is `between` with an exclusive lower bound and inclusive upper bound. Values are
//! requested for column `a` and both constraints on column `b` for a total of 3 values.
//!
//! The following example does *not* use the sort key index because the leading column `a` does not
//! have an equality constraint:
//! `(a > 1 or (a = 1 and b > 10)) and (a < 3 or (a = 3 and b < 5))`

const std = @import("std");
const debug = std.debug;
const mem = std.mem;
const meta = std.meta;
const testing = std.testing;
const ArenaAllocator = std.heap.ArenaAllocator;

const vtab = @import("../sqlite3/vtab.zig");
const BestIndexInfo = vtab.BestIndexInfo;
const BestIndexInfoOp = vtab.BestIndexInfo.Op;
const Constraint = vtab.BestIndexInfo.Constraint;
const FilterArgs = vtab.FilterArgs;

const IndexCode = @import("code.zig").Code;

/// Chooses the most restrictive bounds on the sort key index (if possible) based on the
/// constraint information in `best_index_info`. If constraining the sort key is not possible,
/// returns `false`. If it is possible, informs sqlite of the values that should be passed to
/// `xFilter`, sets the index identifier, sets the estimated cost of using the sort key
/// index, and returns true.
pub fn attemptSetup(
    arena: *ArenaAllocator,
    sort_key_columns: []const usize,
    best_index_info: BestIndexInfo,
) !bool {
    const bounds = try chooseConstraintBounds(arena, sort_key_columns, best_index_info);

    if (bounds[0] == null) {
        return false;
    }

    std.log.debug("using sort key index", .{});
    setup(bounds, best_index_info);
    return true;
}

/// Finds the most restrictive set of bounds for sort key columns based on the constraint ops
/// and the number of sort key columns that are involved. Does not consider the constraint
/// values that may be associated with the constraints or any information about the data in the
/// table. Returns a slice of the bounds that apply to the sort key columns in sort key column
/// order.
fn chooseConstraintBounds(
    arena: *ArenaAllocator,
    sort_key_columns: []const usize,
    best_index_info: BestIndexInfo,
) ![]?ConstraintBounds {
    var bounds = try arena.allocator()
        .alloc(?ConstraintBounds, sort_key_columns.len);
    for (bounds) |*c| {
        c.* = null;
    }

    for (0..best_index_info.constraintsLen()) |cnst_index| {
        const constraint = best_index_info.constraint(cnst_index);

        if (!constraint.usable()) {
            continue;
        }
        // Row group elimination only supports binary collation, so disqualify the constraint if
        // a different collation is required
        // TODO .rtrim may be useful for row group elimination
        if (constraint.collation() != .binary) {
            continue;
        }

        const col_index = constraint.columnIndex();
        const sk_index = mem.indexOfScalar(usize, sort_key_columns, @intCast(col_index));
        if (sk_index) |sk_idx| {
            // Ensure that the op is one that can be used at all for elimination and if
            // there is an existing bounds already recorded for the sort key column that
            // the bounds stored are the most restrictive
            const curr_bounds = ConstraintBounds.init(constraint) orelse {
                continue;
            };
            const existing_bounds = &bounds[sk_idx];
            if (existing_bounds.*) |*exist_bounds| {
                exist_bounds.mergeFrom(curr_bounds);
            } else {
                bounds[sk_idx] = curr_bounds;
            }
        }
    }

    return bounds;
}

fn setup(bounds: []?ConstraintBounds, best_index_info: BestIndexInfo) void {
    // Request values for the corresponding sort key columns be passed to `xFilter`. Find the
    // `last_op`, which is the only range op in the composite index that may not be `.eq`
    var values_requested: u32 = 0;
    var last_op: RangeOp = undefined;
    for (bounds) |*sk_col_bounds| {
        const b = sk_col_bounds.* orelse {
            break;
        };

        last_op = b.op;
        values_requested += 1;
        // `includeArgInFilter` is 1 indexed
        b.constraint_0.includeArgInFilter(values_requested);
        if (last_op != .eq) {
            if (b.constraint_1) |constraint| {
                values_requested += 1;
                constraint.includeArgInFilter(values_requested);
            }
            break;
        }
    }

    // Set the index identifier
    var identifier: u32 = @intFromEnum(IndexCode.sort_key);
    // `last_op` is guaranteed to have been set at this point
    identifier |= @as(u32, last_op.serialize()) << 8;
    best_index_info.setIdentifier(@bitCast(identifier));

    // Because only the sort key index is supported, set the estimated cost based on the number
    // of values that can be utilized to eliminate row groups. Start with a large value and
    // subtract the number of arg values that can be utilized. This is not a true cost estimate
    // and only works when comparing different plans that utilize the sort key index against
    // each other.
    // TODO in situations where the rhs value of the sort key constraints are available, it is
    //      possible to generate a true cost estimate by doing row group elimination here.
    const estimated_cost = 1_000 - @min(1_000, values_requested);
    std.log.debug("sort key index estimated cost: {}", .{estimated_cost});
    best_index_info.setEstimatedCost(@floatFromInt(estimated_cost));
}

const ConstraintBounds = struct {
    op: RangeOp,
    /// This is always the lower constraint if `op` is `.between`
    constraint_0: Constraint,
    /// This is always the upper constraint if `op` is `.between`. It is null if `op` is not
    /// `.between`.
    constraint_1: ?Constraint = null,

    pub fn init(constraint: Constraint) ?ConstraintBounds {
        const op = RangeOp.fromBestIndexOp(constraint.op()) orelse {
            return null;
        };
        return .{ .op = op, .constraint_0 = constraint };
    }

    /// Merges `other` into self. This may involve combing `self` and `other` into a range,
    /// replacing `self` with `other`, or keeping `self` as is depending on the specific
    /// constraint bounds and their respective restrictiveness.
    pub fn mergeFrom(self: *ConstraintBounds, other: ConstraintBounds) void {
        const self_op = self.op;
        switch (self_op) {
            .eq => {},
            .from_exc, .from_inc => switch (other.op) {
                .eq => self.* = other,
                .from_inc => {},
                .from_exc => if (self_op == .from_inc) {
                    self.* = other;
                },
                .to_exc, .to_inc => {
                    self.op = .{ .between = .{
                        .lower_inc = self_op == .from_inc,
                        .upper_inc = other.op == .to_inc,
                    } };
                    self.constraint_1 = other.constraint_0;
                },
                .between => |btwn| {
                    if (self_op == .from_exc and btwn.lower_inc) {
                        const lower_constraint = self.constraint_0;
                        self.* = other;
                        self.op.between.lower_inc = false;
                        self.constraint_0 = lower_constraint;
                    }
                },
            },
            .to_exc, .to_inc => switch (other.op) {
                .eq => self.* = other,
                .to_inc => {},
                .to_exc => if (self_op == .to_inc) {
                    self.* = other;
                },
                .from_exc, .from_inc => {
                    self.op = .{ .between = .{
                        .lower_inc = other.op == .from_inc,
                        .upper_inc = self_op == .to_inc,
                    } };
                    self.constraint_1 = self.constraint_0;
                    self.constraint_0 = other.constraint_0;
                },
                .between => |btwn| {
                    if (self_op == .to_exc and btwn.upper_inc) {
                        const upper_constraint = self.constraint_0;
                        self.* = other;
                        self.op.between.upper_inc = false;
                        self.constraint_1 = upper_constraint;
                    }
                },
            },
            .between => |btwn_self| switch (other.op) {
                .eq => self.* = other,
                .to_inc, .from_inc => {},
                .from_exc => if (btwn_self.lower_inc) {
                    self.op.between.lower_inc = false;
                    self.constraint_0 = other.constraint_0;
                },
                .to_exc => if (btwn_self.upper_inc) {
                    self.op.between.upper_inc = false;
                    self.constraint_1 = other.constraint_0;
                },
                .between => |btwn_other| {
                    if (btwn_self.lower_inc and !btwn_other.lower_inc) {
                        self.op.between.lower_inc = false;
                        self.constraint_0 = other.constraint_0;
                    }
                    if (btwn_self.upper_inc and !btwn_other.upper_inc) {
                        self.op.between.upper_inc = false;
                        self.constraint_1 = other.constraint_1;
                    }
                },
            },
        }
    }
};

pub const SortKeyRange = struct {
    last_op: RangeOp,
    args: FilterArgs,

    pub fn deserialize(identifier: u24, args: FilterArgs) !@This() {
        const last_op = try RangeOp.deserialize(identifier);
        return .{ .last_op = last_op, .args = args };
    }
};

const RangeOpCode = enum(u4) {
    eq = 1,
    from_inc = 2,
    from_exc = 3,
    to_inc = 4,
    to_exc = 5,
    between = 6,
};

pub const RangeOp = union(RangeOpCode) {
    eq,
    from_inc,
    from_exc,
    to_inc,
    to_exc,
    between: struct {
        lower_inc: bool,
        upper_inc: bool,
    },

    pub fn fromBestIndexOp(op: BestIndexInfoOp) ?RangeOp {
        return switch (op) {
            .eq => .eq,
            // TODO .is may need to have a separate representation when values can be nullable
            //      (they cannot be for sort key columns)
            .is => .eq,
            .lt => .to_exc,
            .le => .to_inc,
            .gt => .from_exc,
            .ge => .from_inc,
            else => null,
        };
    }

    pub fn deserialize(identifier: u24) !RangeOp {
        const code = try meta.intToEnum(RangeOpCode, identifier >> 20);
        switch (code) {
            .between => {
                return .{ .between = .{
                    .lower_inc = identifier & 0x80000 > 0,
                    .upper_inc = identifier & 0x40000 > 0,
                } };
            },
            inline else => |c| return @unionInit(RangeOp, @tagName(c), {}),
        }
    }

    pub fn serialize(self: RangeOp) u24 {
        const code = meta.activeTag(self);
        const id: u24 = @as(u24, @intFromEnum(code)) << 20;
        if (code == .between) {
            return id |
                (@as(u24, @intFromBool(self.between.lower_inc)) << 19) |
                (@as(u24, @intFromBool(self.between.upper_inc)) << 18);
        }
        return id;
    }

    test "best index: sort key index: serialize range op" {
        var op: RangeOp = .from_exc;
        var code = op.serialize();
        try std.testing.expectEqual(@as(u24, 3145728), code);

        op = .{ .between = .{ .lower_inc = true, .upper_inc = false } };
        code = op.serialize();
        try std.testing.expectEqual(@as(u24, 6815744), code);
    }
};

test {
    _ = RangeOp;
}
