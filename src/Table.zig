const std = @import("std");
const fmt = std.fmt;
const mem = std.mem;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const ChangeSet = @import("sqlite3/ChangeSet.zig");
const Conn = @import("sqlite3/Conn.zig");
const SqliteErorr = @import("sqlite3/errors.zig").Error;
const Stmt = @import("sqlite3/Stmt.zig");
const ValueRef = @import("sqlite3/value.zig").Ref;
const vtab = @import("sqlite3/vtab.zig");
const sqlite_c = @import("sqlite3/c.zig").c;
const Result = vtab.Result;

const schema_mod = @import("schema.zig");
const SchemaDef = schema_mod.SchemaDef;
const Schema = schema_mod.Schema;

const segment = @import("segment.zig");

const PrimaryIndex = @import("PrimaryIndex.zig");
const row_group = @import("row_group.zig");
const RowGroupCreator = row_group.Creator;

const Self = @This();

// TODO remove this
allocator: Allocator,
table_static_arena: ArenaAllocator,
db: struct {
    schema: schema_mod.Db,
    segment: segment.Db,
},
name: []const u8,
schema: Schema,
primary_index: PrimaryIndex,
row_group_creator: RowGroupCreator,

pub const InitError = error{
    NoColumns,
    UnsupportedDb,
} || SchemaDef.ParseError || Schema.Error || SqliteErorr || mem.Allocator.Error;

pub fn create(
    self: *Self,
    allocator: Allocator,
    conn: Conn,
    cb_ctx: *vtab.CallbackContext,
    args: []const []const u8,
) InitError!void {
    if (args.len < 5) {
        cb_ctx.setErrorMessage("table must have at least 1 column", .{});
        return InitError.NoColumns;
    }

    const db_name = args[1];
    if (!mem.eql(u8, "main", db_name)) {
        cb_ctx.setErrorMessage(
            "only 'main' db currently supported, got {s}",
            .{db_name},
        );
        return InitError.UnsupportedDb;
    }

    // Use the tmp arena because the schema def is not stored with the table. The
    // data is converted into a Schema and the Schema is stored.
    const def = SchemaDef.parse(cb_ctx.arena, args[3..]) catch |e| {
        cb_ctx.setErrorMessage("error parsing schema definition: {any}", .{e});
        return e;
    };

    self.allocator = allocator;
    self.table_static_arena = ArenaAllocator.init(allocator);
    errdefer self.table_static_arena.deinit();

    self.name = try self.table_static_arena.allocator().dupe(u8, args[2]);

    try schema_mod.Db.createTable(cb_ctx.arena, conn, self.name);
    try segment.Db.createTable(cb_ctx.arena, conn, self.name);
    self.db = .{
        .schema = schema_mod.Db.init(conn, self.name),
        .segment = try segment.Db.init(&self.table_static_arena, conn, self.name),
    };
    errdefer self.db.segment.deinit();
    errdefer self.db.schema.deinit();

    self.schema = Schema.create(
        &self.table_static_arena,
        cb_ctx.arena,
        &self.db.schema,
        def,
    ) catch |e| {
        cb_ctx.setErrorMessage("error creating schema: {any}", .{e});
        return e;
    };

    self.primary_index = PrimaryIndex.create(
        cb_ctx.arena,
        conn,
        self.name,
        &self.schema,
    ) catch |e| {
        cb_ctx.setErrorMessage("error creating primary index: {any}", .{e});
        return e;
    };
    errdefer self.primary_index.deinit();

    self.row_group_creator = RowGroupCreator.init(
        allocator,
        &self.table_static_arena,
        &self.db.segment,
        &self.schema,
        &self.primary_index,
        10_000,
    ) catch |e| {
        cb_ctx.setErrorMessage("error creating row group creator: {any}", .{e});
        return e;
    };
}

test "create table" {
    const conn = try Conn.openInMemory();
    defer conn.close();

    var arena = ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var cb_ctx = vtab.CallbackContext.init(&arena);

    var table = try cb_ctx.arena.allocator().create(Self);
    try table.create(
        std.testing.allocator,
        conn,
        &cb_ctx,
        &[_][]const u8{
            "",                  "main",          "foo",
            "id BLOB NOT NULL",  "SORT KEY (id)", "name TEXT NOT NULL",
            "size INTEGER NULL",
        },
    );
    defer table.destroy(&cb_ctx);
}

pub fn connect(
    self: *Self,
    allocator: Allocator,
    conn: Conn,
    cb_ctx: *vtab.CallbackContext,
    args: []const []const u8,
) InitError!void {
    if (args.len < 3) {
        cb_ctx.setErrorMessage("invalid arguments to vtab `connect`", .{});
        return InitError.NoColumns;
    }

    const db_name = args[1];
    if (!mem.eql(u8, "main", db_name)) {
        cb_ctx.setErrorMessage("only 'main' db currently supported, got {s}", .{db_name});
        return InitError.UnsupportedDb;
    }

    self.allocator = allocator;
    self.table_static_arena = ArenaAllocator.init(allocator);
    errdefer self.table_static_arena.deinit();

    self.name = try self.table_static_arena.allocator().dupe(u8, args[2]);

    self.db = .{
        .schema = schema_mod.Db.init(conn, self.name),
        .segment = try segment.Db.init(&self.table_static_arena, conn, self.name),
    };
    errdefer self.db.segment.deinit();
    errdefer self.db.schema.deinit();

    self.schema = Schema.load(
        &self.table_static_arena,
        cb_ctx.arena,
        &self.db.schema,
    ) catch |e| {
        cb_ctx.setErrorMessage("error loading schema: {any}", .{e});
        return e;
    };

    self.primary_index = PrimaryIndex.open(
        cb_ctx.arena,
        conn,
        self.name,
        &self.schema,
    ) catch |e| {
        cb_ctx.setErrorMessage("error opening primary index: {any}", .{e});
        return e;
    };
    errdefer self.primary_index.deinit();

    self.row_group_creator = RowGroupCreator.init(
        allocator,
        &self.table_static_arena,
        &self.db.segment,
        &self.schema,
        &self.primary_index,
        10_000,
    ) catch |e| {
        cb_ctx.setErrorMessage("error creating row group creator: {any}", .{e});
        return e;
    };
}

pub fn disconnect(self: *Self) void {
    self.row_group_creator.deinit();
    self.primary_index.deinit();

    self.db.segment.deinit();
    self.db.schema.deinit();

    self.table_static_arena.deinit();
}

pub fn destroy(self: *Self, cb_ctx: *vtab.CallbackContext) void {
    self.db.schema.dropTable(cb_ctx.arena) catch |e| {
        std.log.err("failed to drop shadow table {s}_columns: {any}", .{ self.name, e });
    };
    self.db.segment.dropTable(cb_ctx.arena) catch |e| {
        std.log.err("failed to drop shadow table {s}_segments: {any}", .{ self.name, e });
    };
    self.primary_index.drop(cb_ctx.arena) catch |e| {
        std.log.err("failed to drop shadow table {s}_primaryindex: {any}", .{ self.name, e });
    };

    self.disconnect();
}

pub fn ddl(self: *Self, allocator: Allocator) ![:0]const u8 {
    return fmt.allocPrintZ(
        allocator,
        "{}",
        .{self.schema.sqliteDdlFormatter(self.name)},
    );
}

pub fn update(
    self: *Self,
    cb_ctx: *vtab.CallbackContext,
    rowid: *i64,
    change_set: ChangeSet,
) !void {
    if (change_set.changeType() == .Insert) {
        rowid.* = self.primary_index.insertPendingInsertEntry(cb_ctx.arena, change_set) catch |e| {
            cb_ctx.setErrorMessage("failed insert insert entry: {any}", .{e});
            return e;
        };

        // TODO do the following at end of transaction (requires tracking which row groups got
        //      inserts)

        var node = try self.primary_index.containingNodeHandle(
            cb_ctx.arena,
            change_set,
            rowid.*,
        );
        defer node.deinit();

        defer self.row_group_creator.reset();
        _ = try self.row_group_creator.createN(cb_ctx.arena, node, 1);

        return;
    }

    @panic("delete and update are not supported");
}

const IndexCode = enum(u8) {
    sort_key = 1,
};

pub fn bestIndex(
    self: *Self,
    cb_ctx: *vtab.CallbackContext,
    best_index: vtab.BestIndexInfo,
) !void {
    std.log.debug("best index: {} constraints", .{best_index.constraintsLen()});

    // Determine if row group elimination using the sort key (primary) index is possible. This is
    // currently the only index that is supported. The sort key index may be a composite index, and
    // the posgres documentation has an excellent summary of the logic for determining if using a
    // composite index is possible:
    // > The exact rule is that equality constraints on leading columns, plus any inequality
    // > constraints on the first column that does not have an equality constraint, will be used to
    // > limit the portion of the index that is scanned.
    //
    // This function needs to communicate to the cursor:
    // * the sort key is being used to eliminate rows
    // * the number of equality columns being used
    // * the operation on the first non-equality column
    // Note that "there is no guarantee that xFilter will be called following a successful
    // xBestIndex", which means it is tricky to determine when to deallocate data stored about the
    // selected index. Because of that, encode the data needed by the cursor in the string
    // identifier, which sqlite passes to the xFilter callback.

    // Store the constraints that operate on sort key columns in sort key column order. Keep only
    // the most restrictive op for each sort key column
    const sort_key = self.schema.sort_key.items;
    var sort_key_constraints = try cb_ctx.arena.allocator()
        .alloc(?vtab.BestIndexInfo.Constraint, sort_key.len);
    for (sort_key_constraints) |*c| {
        c.* = null;
    }
    for (0..best_index.constraintsLen()) |cnst_index| {
        const constraint = best_index.constraint(cnst_index);
        std.log.debug(
            "evaluating constraint on column {} (usable = {})",
            .{ constraint.columnIndex(), constraint.usable() },
        );

        if (!constraint.usable()) {
            continue;
        }

        const col_index = constraint.columnIndex();
        const sk_index = std.mem.indexOfScalar(usize, sort_key, @intCast(col_index));
        if (sk_index) |sk_idx| {
            // Ensure that the op is one that can be used at all for elimination and if there is
            // an existing constraint already recorded for the sort key column that the constraint
            // stored is the most restrictive one
            const op_rank = constraintOpRank(constraint.op());
            if (op_rank) |rank| {
                if (sort_key_constraints[sk_idx]) |existing_constraint| {
                    const existing_op_rank = constraintOpRank(existing_constraint.op()).?;
                    if (rank < existing_op_rank) {
                        sort_key_constraints[sk_idx] = constraint;
                    }
                } else {
                    sort_key_constraints[sk_idx] = constraint;
                }
            }
        }
    }

    // Determine if the sort key (primary) index can be used
    var sk_eq_prefix_len: u32 = 0;
    var last_op: vtab.BestIndexInfo.Op = undefined;
    for (sort_key_constraints) |sk_constraint| {
        // TODO consider the collation using `sqlite3_vtab_collation` for each text column to do
        //      text comparisons properly
        if (sk_constraint == null) {
            break;
        }

        // Communicates to sqlite that the rhs value of this constraint should be passed to xFilter
        // At this point, it is guaranteed that the sort key index will be used so communicate this
        // here instead of looping over the constraints again
        sk_constraint.?.includeArgInFilter(sk_eq_prefix_len + 1);
        last_op = sk_constraint.?.op();
        sk_eq_prefix_len += 1;
        if (sk_constraint.?.op() != .eq and sk_constraint.?.op() != .is) {
            break;
        }
    }

    const use_sort_key_index = sk_eq_prefix_len > 0;
    if (use_sort_key_index) {
        std.log.debug("using sort key index", .{});

        var identifier: u32 = @intFromEnum(IndexCode.sort_key);
        // Embed the last_op in the MSB of the identifier
        // `last_op` is guaranteed to have been set at this point
        identifier |= @as(u32, @intCast(@intFromEnum(last_op))) << 24;
        best_index.setIdentifier(@bitCast(identifier));

        // Because only the sort key index is supported, set the estimated cost based on the number
        // of columns in the sort key that can be utilized. Start with a large value and subtract
        // the number of sort key columns that can be utilized. This is not a true cost estimate
        // and only works when comparing different plans that utilize the sort key index against
        // each other.
        // TODO in situations where the rhs value of the sort key constraints are available, it is
        //      possible to generate a true cost estimate by doing row group elimination here.
        const estimated_cost = 1_000 - @min(1_000, sk_eq_prefix_len);
        std.log.debug("estimated cost: {}", .{estimated_cost});
        best_index.setEstimatedCost(@floatFromInt(estimated_cost));
    }
}

/// Constraint ops that are supported for row group elimination. Ops are sorted from most
/// restrictive to least
const supported_constraint_ops = [_]vtab.BestIndexInfo.Op{ .eq, .is, .gt, .lt, .le, .ge };

/// Constraint ops are ordered by restrictiveness so that the most restrictive op can be associated
/// with a column. If the op is not supported, null is returned.
fn constraintOpRank(op: vtab.BestIndexInfo.Op) ?usize {
    return mem.indexOfScalar(vtab.BestIndexInfo.Op, &supported_constraint_ops, op);
}

pub fn begin(_: *Self, _: *vtab.CallbackContext) !void {
    std.log.debug("txn begin", .{});
}

pub fn commit(self: *Self, _: *vtab.CallbackContext) !void {
    std.log.debug("txn commit", .{});
    try self.primary_index.persistNextRowid();
}

pub fn rollback(self: *Self, _: *vtab.CallbackContext) !void {
    std.log.debug("txn rollback", .{});
    try self.primary_index.loadNextRowid();
}

pub fn savepoint(_: *Self, _: *vtab.CallbackContext, savepoint_id: i32) !void {
    std.log.debug("txn savepoint {d} begin", .{savepoint_id});
}

pub fn release(self: *Self, _: *vtab.CallbackContext, savepoint_id: i32) !void {
    std.log.debug("txn savepoint {d} release", .{savepoint_id});
    try self.primary_index.persistNextRowid();
}

pub fn rollbackTo(self: *Self, _: *vtab.CallbackContext, savepoint_id: i32) !void {
    std.log.debug("txn savepoint {d} rollback", .{savepoint_id});
    try self.primary_index.loadNextRowid();
}

const shadowNames = [_][:0]const u8{
    "segments",
    "columns",
    "primaryindex",
};

pub fn isShadowName(name: [:0]const u8) bool {
    std.log.debug("checking shadnow name: {s}", .{name});
    for (shadowNames) |sn| {
        if (mem.eql(u8, sn, name)) {
            return true;
        }
    }
    return false;
}

pub fn open(
    self: *Self,
    _: *vtab.CallbackContext,
) !Cursor {
    std.log.debug("open cursor", .{});
    return Cursor.init(
        self.allocator,
        &self.db.segment,
        &self.schema,
        &self.primary_index,
    );
}

pub const Cursor = struct {
    primary_index: *PrimaryIndex,

    pidx_cursor: PrimaryIndex.Cursor,
    rg_cursor: row_group.Cursor,
    in_row_group: bool,

    pidx_cursor_initialized: bool,

    pub fn init(
        allocator: Allocator,
        segment_db: *segment.Db,
        schema: *const Schema,
        primary_index: *PrimaryIndex,
    ) !Cursor {
        const rg_cursor = try row_group.Cursor.init(allocator, segment_db, schema);
        return .{
            .primary_index = primary_index,
            .pidx_cursor = undefined,
            .rg_cursor = rg_cursor,
            .in_row_group = false,
            .pidx_cursor_initialized = false,
        };
    }

    pub fn deinit(self: *Cursor) void {
        self.rg_cursor.deinit();
        if (self.pidx_cursor_initialized) {
            self.pidx_cursor.deinit();
        }
    }

    pub fn begin(
        self: *Cursor,
        cb_ctx: *vtab.CallbackContext,
        index_id_num: i32,
        _: [:0]const u8,
        filter_args: vtab.FilterArgs,
    ) !void {
        std.log.debug("index num: {}", .{index_id_num});
        const index_code_num = index_id_num & 0xFF;
        if (std.meta.intToEnum(IndexCode, index_code_num)) |index_code| {
            std.debug.assert(filter_args.valuesLen() > 0);
            const last_op: vtab.BestIndexInfo.Op = @enumFromInt(index_id_num >> 24);
            std.log.debug("filtering with index: {} {}", .{ index_code, last_op });
            std.log.debug("filtering args: {}", .{filter_args.valuesLen()});

            const range = PrimaryIndex.CursorRange(vtab.FilterArgs){
                .key = filter_args,
                .last_op = switch (last_op) {
                    .eq => .eq,
                    .gt => .gt,
                    .ge => .ge,
                    .lt => .lt,
                    .le => .le,
                    else => unreachable,
                },
            };

            self.pidx_cursor = try self.primary_index.cursor(
                vtab.FilterArgs,
                cb_ctx.arena,
                range,
            );
            self.pidx_cursor_initialized = true;
        } else |_| {
            std.log.debug("doing table scan", .{});
            self.pidx_cursor = try self.primary_index.cursor(
                vtab.FilterArgs,
                cb_ctx.arena,
                null,
            );
            self.pidx_cursor_initialized = true;
        }

        try self.next();
    }

    pub fn eof(self: *Cursor) bool {
        return self.pidx_cursor.eof;
    }

    pub fn next(self: *Cursor) !void {
        if (self.in_row_group) {
            try self.rg_cursor.next();
            if (!self.rg_cursor.eof()) {
                return;
            }
            self.in_row_group = false;
        }

        try self.pidx_cursor.next();
        if (!self.pidx_cursor.eof) {
            if (self.pidx_cursor.entryType() == .RowGroup) {
                self.rg_cursor.reset();
                // Read the row group into the rg_cursor
                try self.pidx_cursor.readRowGroupEntry(self.rg_cursor.rowGroup());
                self.in_row_group = true;
            }
            return;
        }
    }

    pub fn rowid(self: *Cursor) !i64 {
        if (self.in_row_group) {
            const value = try self.rg_cursor.readRowid();
            return value.asI64();
        }
        const value = try self.pidx_cursor.readRowid();
        return value.asI64();
    }

    pub fn column(self: *Cursor, result: Result, col_idx: usize) !void {
        // TODO make this more efficient by passing the result into the cursors
        if (self.in_row_group) {
            try self.rg_cursor.readInto(result, col_idx);
            return;
        }

        const value = try self.pidx_cursor.read(col_idx);
        result.setSqliteValue(value);
    }
};
