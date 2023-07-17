const std = @import("std");
const debug = std.debug;
const fmt = std.fmt;
const heap = std.heap;
const mem = std.mem;
const meta = std.meta;
const testing = std.testing;

const c = @import("c.zig").c;
const versionGreaterThanOrEqualTo = @import("c.zig").versionGreaterThanOrEqualTo;

/// CallbackContext is only valid for the duration of a callback from sqlite into the
/// virtual table instance. It should no tbe saved between calls, and it is provided to
/// every function.
pub const CallbackContext = struct {
    const Self = @This();

    allocator: *heap.ArenaAllocator,
    error_message: []const u8 = "unknown error",

    pub fn setErrorMessage(
        self: *Self,
        comptime format_string: []const u8,
        values: anytype,
    ) void {
        self.error_message = fmt.allocPrint(
            self.allocator.allocator(),
            format_string,
            values
        ) catch |err| switch (err) {
            error.OutOfMemory => "can't set diagnostic message, out of memory",
        };
    }
};

pub const BestIndexBuilder = struct {
    const Self = @This();

    /// Constraint operator codes.
    /// See https://sqlite.org/c3ref/c_index_constraint_eq.html
    pub const ConstraintOp = if (versionGreaterThanOrEqualTo(3, 38, 0))
        enum {
            eq,
            gt,
            le,
            lt,
            ge,
            match,
            like,
            glob,
            regexp,
            ne,
            is_not,
            is_not_null,
            is_null,
            is,
            limit,
            offset,
        }
    else
        enum {
            eq,
            gt,
            le,
            lt,
            ge,
            match,
            like,
            glob,
            regexp,
            ne,
            is_not,
            is_not_null,
            is_null,
            is,
        };

    const ConstraintOpFromCodeError = error{
        InvalidCode,
    };

    fn constraintOpFromCode(code: u8) ConstraintOpFromCodeError!ConstraintOp {
        if (comptime versionGreaterThanOrEqualTo(3, 38, 0)) {
            switch (code) {
                c.SQLITE_INDEX_CONSTRAINT_LIMIT => return .limit,
                c.SQLITE_INDEX_CONSTRAINT_OFFSET => return .offset,
                else => {},
            }
        }

        switch (code) {
            c.SQLITE_INDEX_CONSTRAINT_EQ => return .eq,
            c.SQLITE_INDEX_CONSTRAINT_GT => return .gt,
            c.SQLITE_INDEX_CONSTRAINT_LE => return .le,
            c.SQLITE_INDEX_CONSTRAINT_LT => return .lt,
            c.SQLITE_INDEX_CONSTRAINT_GE => return .ge,
            c.SQLITE_INDEX_CONSTRAINT_MATCH => return .match,
            c.SQLITE_INDEX_CONSTRAINT_LIKE => return .like,
            c.SQLITE_INDEX_CONSTRAINT_GLOB => return .glob,
            c.SQLITE_INDEX_CONSTRAINT_REGEXP => return .regexp,
            c.SQLITE_INDEX_CONSTRAINT_NE => return .ne,
            c.SQLITE_INDEX_CONSTRAINT_ISNOT => return .is_not,
            c.SQLITE_INDEX_CONSTRAINT_ISNOTNULL => return .is_not_null,
            c.SQLITE_INDEX_CONSTRAINT_ISNULL => return .is_null,
            c.SQLITE_INDEX_CONSTRAINT_IS => return .is,
            else => return error.InvalidCode,
        }
    }

    // WHERE clause constraint
    pub const Constraint = struct {
        // Column constrained. -1 for ROWID
        column: isize,
        op: ConstraintOp,
        usable: bool,

        usage: struct {
            // If >0, constraint is part of argv to xFilter
            argv_index: i32 = 0,
            // Id >0, do not code a test for this constraint
            omit: bool = false,
        },
    };

    // ORDER BY clause
    pub const OrderBy = struct {
        column: usize,
        order: enum {
            desc,
            asc,
        },
    };

    /// Internal state
    allocator: mem.Allocator,
    id_str_buffer: std.ArrayList(u8),
    index_info: *c.sqlite3_index_info,

    /// List of WHERE clause constraints
    ///
    /// Similar to `aConstraint` in the Inputs section of sqlite3_index_info except we
    /// embed the constraint usage in there too. This makes it nicer to use for the user.
    constraints: []Constraint,

    /// Indicate which columns of the virtual table are actually used by the statement.
    /// If the lowest bit of colUsed is set, that means that the first column is used.
    /// The second lowest bit corresponds to the second column. And so forth.
    ///
    /// Maps to the `colUsed` field.
    columns_used: u64,

    /// Index identifier.
    /// This is passed to the filtering function to identify which index to use.
    ///
    /// Maps to the `idxNum` and `idxStr` field in sqlite3_index_info.
    /// Id id.id_str is non empty the string will be copied to a SQLite-allocated buffer
    /// and `needToFreeIdxStr` will be 1.
    id: IndexIdentifier,

    /// If the virtual table will output its rows already in the order specified by the
    /// ORDER BY clause then this can be set to true. This will indicate to SQLite that
    /// it doesn't need to do a sorting pass.
    ///
    /// Maps to the `orderByConsumed` field.
    already_ordered: bool = false,

    /// Estimated number of "disk access operations" required to execute this query.
    ///
    /// Maps to the `estimatedCost` field.
    estimated_cost: ?f64 = null,

    /// Estimated number of rows returned by this query.
    ///
    /// Maps to the `estimatedRows` field.
    ///
    /// TODO: implement this
    estimated_rows: ?i64 = null,

    /// Additional flags for this index.
    ///
    /// Maps to the `idxFlags` field.
    flags: struct {
        unique: bool = false,
    } = .{},

    const InitError = error{} || mem.Allocator.Error || ConstraintOpFromCodeError;

    fn init(allocator: mem.Allocator, index_info: *c.sqlite3_index_info) InitError!Self {
        var res = Self{
            .allocator = allocator,
            .index_info = index_info,
            .id_str_buffer = std.ArrayList(u8).init(allocator),
            .constraints = try allocator.alloc(Constraint,
                @intCast(index_info.nConstraint)),
            .columns_used = @intCast(index_info.colUsed),
            .id = .{},
        };

        for (res.constraints, 0..) |*constraint, i| {
            const raw_constraint = index_info.aConstraint[i];

            constraint.column = @intCast(raw_constraint.iColumn);
            constraint.op = try constraintOpFromCode(raw_constraint.op);
            constraint.usable = if (raw_constraint.usable == 1) true else false;
            constraint.usage = .{};
        }

        return res;
    }

    /// Returns true if the column is used, false otherwise.
    pub fn isColumnUsed(self: *Self, column: u6) bool {
        const mask = @as(u64, 1) << column - 1;
        return self.columns_used & mask == mask;
    }

    /// Builds the final index data.
    ///
    /// Internally it populates the sqlite3_index_info "Outputs" fields using the
    /// information set by the user.
    pub fn build(self: *Self) void {
        var index_info = self.index_info;

        // Populate the constraint usage
        var constraint_usage: []c.sqlite3_index_constraint_usage =
            index_info.aConstraintUsage[0..self.constraints.len];
        for (self.constraints, 0..) |constraint, i| {
            constraint_usage[i].argvIndex = constraint.usage.argv_index;
            constraint_usage[i].omit = if (constraint.usage.omit) 1 else 0;
        }

        // Identifiers
        index_info.idxNum = @intCast(self.id.num);
        if (self.id.str.len > 0) {
            // Must always be NULL-terminated so add 1
            const tmp: [*c]u8 = @ptrCast(c.sqlite3_malloc(@intCast(self.id.str.len + 1)));

            mem.copy(u8, tmp[0..self.id.str.len], self.id.str);
            tmp[self.id.str.len] = 0;

            index_info.idxStr = tmp;
            index_info.needToFreeIdxStr = 1;
        }

        index_info.orderByConsumed = if (self.already_ordered) 1 else 0;
        if (self.estimated_cost) |estimated_cost| {
            index_info.estimatedCost = estimated_cost;
        }
        if (self.estimated_rows) |estimated_rows| {
            index_info.estimatedRows = estimated_rows;
        }

        // Flags
        index_info.idxFlags = 0;
        if (self.flags.unique) {
            index_info.idxFlags |= c.SQLITE_INDEX_SCAN_UNIQUE;
        }
    }
};

/// Identifies an index for a virtual table.
///
/// The user-provided buildBestIndex functions sets the index identifier.
/// These fields are meaningless for SQLite so they can be whatever you want as long as
/// both buildBestIndex and filter functions agree on what they mean.
pub const IndexIdentifier = struct {
    num: i32 = 0,
    str: []const u8 = "",

    fn fromC(idx_num: c_int, idx_str: [*c]const u8) IndexIdentifier {
        return IndexIdentifier{
            .num = @intCast(idx_num),
            .str = if (idx_str != null) mem.sliceTo(idx_str, 0) else "",
        };
    }
};

pub const FilterArg = struct {
    value: ?*c.sqlite3_value,
};

fn parseModuleArguments(
    allocator: mem.Allocator,
    argc: c_int,
    argv: [*c]const [*c]const u8,
) ![]const []const u8 {
    var res = try allocator.alloc([]const u8, @intCast(argc));

    for (res, 0..) |*marg, i| {
        // The documentation of sqlite says each string in argv is null-terminated
        marg.* = mem.sliceTo(argv[i], 0);
    }

    return res;
}

pub fn VirtualTable(comptime Table: type) type {
    const State = struct {
        const Self = @This();

        /// vtab must come first so sqlite interprets this as a `sqlite3_vtab` struct
        /// The different functions receive a pointer to a vtab so we have to use 
        /// fieldParentPtr to get our state.
        vtab: c.sqlite3_vtab,
        allocator: mem.Allocator,
        /// The table is the actual virtual table implementation.
        table: *Table,

        const InitError = error{} || mem.Allocator.Error || Table.InitError;

        fn init(allocator: mem.Allocator, table: *Table) InitError!*Self {
            var res = try allocator.create(Self);
            res.* = .{
                .vtab = mem.zeroes(c.sqlite3_vtab),
                .allocator = allocator,
                .table = table,
            };
            return res;
        }

        fn disconnect(self: *Self) void {
            // TODO table.destroy and table.disconnect might need to return errors
            self.table.disconnect();
            self.allocator.destroy(self);
        }

        fn destroy(self: *Self) void {
            // TODO table.destroy and table.disconnect might need to return errors
            self.table.destroy();
            self.allocator.destroy(self);
        }
    };

    return struct {
        const Self = @This();

        pub const module = if (versionGreaterThanOrEqualTo(3, 26, 0))
            c.sqlite3_module{
                .iVersion = 0,
                .xCreate = xCreate,
                .xConnect = xConnect,
                .xBestIndex = null,
                .xDisconnect = xDisconnect,
                .xDestroy = xDestroy,
                .xOpen = null,
                .xClose = null,
                .xFilter = null,
                .xNext = null,
                .xEof = null,
                .xColumn = null,
                .xRowid = null,
                .xUpdate = null,
                .xBegin = null,
                .xSync = null,
                .xCommit = null,
                .xRollback = null,
                .xFindFunction = null,
                .xRename = null,
                .xSavepoint = null,
                .xRelease = null,
                .xRollbackTo = null,
                .xShadowName = null,
            }
        else
            c.sqlite3_module{
                .iVersion = 0,
                .xCreate = xCreate,
                .xConnect = xConnect,
                .xBestIndex = null,
                .xDisconnect = xDisconnect,
                .xDestroy = xDestroy,
                .xOpen = null,
                .xClose = null,
                .xFilter = null,
                .xNext = null,
                .xEof = null,
                .xColumn = null,
                .xRowid = null,
                .xUpdate = null,
                .xBegin = null,
                .xSync = null,
                .xCommit = null,
                .xRollback = null,
                .xFindFunction = null,
                .xRename = null,
                .xSavepoint = null,
                .xRelease = null,
                .xRollbackTo = null,
            };

        table: Table,

        fn getModuleAllocator(ptr: ?*anyopaque) *mem.Allocator {
            return @ptrCast(@alignCast(ptr.?));
        }

        fn setup(
            comptime create: bool,
            db: ?*c.sqlite3,
            module_context_ptr: ?*anyopaque,
            argc: c_int,
            argv: [*c]const [*c]const u8,
            vtab: [*c][*c]c.sqlite3_vtab,
            err_str: [*c][*c]const u8,
        ) c_int {
            const allocator = getModuleAllocator(module_context_ptr).*;

            var arena = heap.ArenaAllocator.init(allocator);
            defer arena.deinit();

            const args = parseModuleArguments(arena.allocator(), argc, argv) catch {
                err_str.* = dupeToSQLiteString("out of memory");
                return c.SQLITE_ERROR;
            };
            var cb_ctx = CallbackContext { .allocator = &arena };

            const initTable = if (create) Table.create else Table.connect;
            var table = initTable(allocator, db.?, &cb_ctx, args) catch {
                err_str.* = dupeToSQLiteString(cb_ctx.error_message);
                return c.SQLITE_ERROR;
            };
            // TODO destroy or disconnect may already have been called when the state was
            //      destroyed/disconnected
            // TODO table.destroy and table.disconnect might need to return errors
            errdefer if (create) table.destroy() else table.disconnect();

            const state = State.init(allocator, &table) catch {
                err_str.* = dupeToSQLiteString(cb_ctx.error_message);
                return c.SQLITE_ERROR;
            };
            errdefer if (create) state.destroy() else state.disconnect();

            vtab.* = @ptrCast(state);

            const schema = state.table.schema(arena.allocator()) catch {
                err_str.* = dupeToSQLiteString("out of memory");
                return c.SQLITE_ERROR;
            };
            const res = c.sqlite3_declare_vtab(db, @ptrCast(schema));
            if (res != c.SQLITE_OK) {
                return c.SQLITE_ERROR;
            }

            return c.SQLITE_OK;
        }

        fn xCreate(
            db: ?*c.sqlite3,
            module_context_ptr: ?*anyopaque,
            argc: c_int,
            argv: [*c]const [*c]const u8,
            vtab: [*c][*c]c.sqlite3_vtab,
            err_str: [*c][*c]const u8,
        ) callconv(.C) c_int {
            return setup(true, db, module_context_ptr, argc, argv, vtab, err_str);
        }

        fn xConnect(
            db: ?*c.sqlite3,
            module_context_ptr: ?*anyopaque,
            argc: c_int,
            argv: [*c]const [*c]const u8,
            vtab: [*c][*c]c.sqlite3_vtab,
            err_str: [*c][*c]const u8,
        ) callconv(.C) c_int {
            return setup(false, db, module_context_ptr, argc, argv, vtab, err_str);
        }

        fn xDisconnect(vtab: [*c]c.sqlite3_vtab) callconv(.C) c_int {
            const state = @fieldParentPtr(State, "vtab", vtab);
            state.disconnect();

            return c.SQLITE_OK;
        }

        fn xDestroy(vtab: [*c]c.sqlite3_vtab) callconv(.C) c_int {
            const state = @fieldParentPtr(State, "vtab", vtab);
            state.destroy();

            return c.SQLITE_OK;
        }

        // fn xBestIndex(
        //     vtab: [*c]c.sqlite3_vtab,
        //     index_info_ptr: [*c]c.sqlite3_index_info,
        // ) callconv(.C) c_int {
        // }

        // fn xOpen(
        //     vtab: [*c]c.sqlite3_vtab,
        //     vtab_cursor: [*c][*c]c.sqlite3_vtab_cursor,
        // ) callconv(.C) c_int {
        // }

        // fn xClose(vtab_cursor: [*c]c.sqlite3_vtab_cursor) callconv(.C) c_int {
        //     const cursor_state = @fieldParentPtr(CursorState, "vtab_cursor", vtab_cursor);
        //     cursor_state.deinit();

        //     return c.SQLITE_OK;
        // }

        // fn xEof(vtab_cursor: [*c]c.sqlite3_vtab_cursor) callconv(.C) c_int {
        // }

        // const FilterArgsFromCPointerError = error{} || mem.Allocator.Error;

        // fn filterArgsFromCPointer(
        //     allocator: mem.Allocator,
        //     argc: c_int,
        //     argv: [*c]?*c.sqlite3_value,
        // ) FilterArgsFromCPointerError![]FilterArg {
        // }

        // fn xFilter(
        //     vtab_cursor: [*c]c.sqlite3_vtab_cursor,
        //     idx_num: c_int,
        //     idx_str: [*c]const u8,
        //     argc: c_int,
        //     argv: [*c]?*c.sqlite3_value,
        // ) callconv(.C) c_int {
        // }

        // fn xNext(vtab_cursor: [*c]c.sqlite3_vtab_cursor) callconv(.C) c_int {
        // }

        // fn xColumn(
        //     vtab_cursor: [*c]c.sqlite3_vtab_cursor,
        //     ctx: ?*c.sqlite3_context,
        //     n: c_int,
        // ) callconv(.C) c_int {
        // }

        // fn xRowid(
        //     vtab_cursor: [*c]c.sqlite3_vtab_cursor,
        //     row_id_ptr: [*c]c.sqlite3_int64,
        // ) callconv(.C) c_int {
        // }
    };

}

fn dupeToSQLiteString(s: []const u8) [*c]const u8 {
    const len: c_int = @intCast(s.len);
    var buffer: [*c]u8 = @ptrCast(c.sqlite3_malloc(len + 1));

    mem.copy(u8, buffer[0..s.len], s);
    buffer[s.len] = 0;

    return buffer;
}