//! This is inspired by [zig-sqlite](https://github.com/vrischmann/zig-sqlite)

const std = @import("std");
const debug = std.debug;
const fmt = std.fmt;
const heap = std.heap;
const mem = std.mem;
const meta = std.meta;
const testing = std.testing;

const c = @import("c.zig").c;
const versionGreaterThanOrEqualTo = @import("c.zig").versionGreaterThanOrEqualTo;

const ChangeSet = @import("ChangeSet.zig");
const Conn = @import("Conn.zig");
const ValueRef = @import("value.zig").Ref;

/// CallbackContext is only valid for the duration of a callback from sqlite into the
/// virtual table instance. It should no tbe saved between calls, and it is provided to
/// every function.
pub const CallbackContext = struct {
    const Self = @This();

    arena: *heap.ArenaAllocator,
    error_message: []const u8 = "unknown error",

    pub fn setErrorMessage(
        self: *Self,
        comptime format_string: []const u8,
        values: anytype,
    ) void {
        self.error_message = fmt.allocPrint(
            self.arena.allocator(),
            format_string,
            values,
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
            .constraints = try allocator.alloc(Constraint, @intCast(index_info.nConstraint)),
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

pub const Result = struct {
    const Self = @This();

    ctx: ?*c.sqlite3_context,

    pub fn setNull(self: Self) void {
        c.sqlite3_result_null(self.ctx);
    }

    pub fn setBool(self: Self, value: bool) void {
        c.sqlite3_result_int(self.ctx, @intFromBool(value));
    }

    pub fn setI64(self: Self, value: i64) void {
        c.sqlite3_result_int64(self.ctx, value);
    }

    pub fn setI32(self: Self, value: i32) void {
        c.sqlite3_result_int(self.ctx, value);
    }

    pub fn setF64(self: Self, value: f64) void {
        c.sqlite3_result_double(self.ctx, value);
    }

    pub fn setBlob(self: Self, value: []const u8) void {
        // Use the wrapper because of https://github.com/ziglang/zig/issues/15893
        c.sqlite3_result_blob_transient(
            self.ctx,
            value.ptr,
            @intCast(value.len),
        );
    }

    pub fn setText(self: Self, value: []const u8) void {
        // Use the wrapper because of https://github.com/ziglang/zig/issues/15893
        c.sqlite3_result_text_transient(
            self.ctx,
            value.ptr,
            @intCast(value.len),
        );
    }

    pub fn setValue(self: Self, value: anytype) void {
        switch (value.valueType()) {
            .Null => self.setNull(),
            .Integer => self.setI64(value.asI64()),
            .Float => self.setF64(value.asF64()),
            .Text => self.setTextTransient(value.asText()),
            .Blob => self.setBlobTransient(value.asBlob()),
        }
    }

    pub fn setSqliteValue(self: Self, value: ValueRef) void {
        c.sqlite3_result_value(self.ctx, value.value);
    }
};

/// Wrapper to create a sqlite virtual table from a zig struct that has the virtual
/// table callbacks. This makes defining a virtual table in zig more zig-like by handling
/// some of the memory layout and converts raw sqlite types to the corresponding zig
/// wrappers.
pub fn VirtualTable(comptime Table: type) type {
    const State = struct {
        const Self = @This();

        /// The different functions receive a pointer to a vtab so use `fieldParentPtr`
        /// to get the state.
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
            self.allocator.destroy(self.table);
            self.allocator.destroy(self);
        }

        fn destroy(self: *Self) void {
            // TODO table.destroy and table.disconnect might need to return errors
            self.table.destroy();
            self.allocator.destroy(self.table);
            self.allocator.destroy(self);
        }
    };

    const CursorState = struct {
        const Self = @This();

        vtab_cursor: c.sqlite3_vtab_cursor,
        allocator: mem.Allocator,
        cursor: Table.Cursor,
    };

    return struct {
        const Self = @This();

        pub const module = if (versionGreaterThanOrEqualTo(3, 26, 0))
            c.sqlite3_module{
                .iVersion = 3,
                .xCreate = xCreate,
                .xConnect = xConnect,
                .xBestIndex = xBestIndex,
                .xDisconnect = xDisconnect,
                .xDestroy = xDestroy,
                .xOpen = xOpen,
                .xClose = xClose,
                .xFilter = xFilter,
                .xNext = xNext,
                .xEof = xEof,
                .xColumn = xColumn,
                .xRowid = xRowid,
                .xUpdate = xUpdate,
                .xBegin = xBegin,
                .xSync = null,
                .xCommit = xCommit,
                .xRollback = xRollback,
                .xFindFunction = null,
                .xRename = null,
                .xSavepoint = xSavepoint,
                .xRelease = xRelease,
                .xRollbackTo = xRollbackTo,
                .xShadowName = xShadowName,
            }
        else
            c.sqlite3_module{
                .iVersion = 2,
                .xCreate = xCreate,
                .xConnect = xConnect,
                .xBestIndex = xBestIndex,
                .xDisconnect = xDisconnect,
                .xDestroy = xDestroy,
                .xOpen = xOpen,
                .xClose = xClose,
                .xFilter = xFilter,
                .xNext = xNext,
                .xEof = xEof,
                .xColumn = xColumn,
                .xRowid = xRowid,
                .xUpdate = xUpdate,
                .xBegin = xBegin,
                .xSync = null,
                .xCommit = xCommit,
                .xRollback = xRollback,
                .xFindFunction = null,
                .xRename = null,
                .xSavepoint = xSavepoint,
                .xRelease = xRelease,
                .xRollbackTo = xRollbackTo,
            };

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
            var cb_ctx = CallbackContext{ .arena = &arena };
            const conn = Conn.init(db.?);

            const initTable = if (create) Table.create else Table.connect;
            var table = allocator.create(Table) catch {
                err_str.* = dupeToSQLiteString("out of memory");
                return c.SQLITE_ERROR;
            };
            table.* = initTable(allocator, conn, &cb_ctx, args) catch {
                err_str.* = dupeToSQLiteString(cb_ctx.error_message);
                return c.SQLITE_ERROR;
            };
            // TODO destroy or disconnect may already have been called when the state was
            //      destroyed/disconnected
            // TODO table.destroy and table.disconnect might need to return errors
            errdefer if (create) table.destroy() else table.disconnect();

            const state = State.init(allocator, table) catch {
                err_str.* = dupeToSQLiteString(cb_ctx.error_message);
                return c.SQLITE_ERROR;
            };
            errdefer if (create) state.destroy() else state.disconnect();

            vtab.* = @ptrCast(state);

            const schema = state.table.ddl(arena.allocator()) catch {
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

        fn xUpdate(
            vtab: [*c]c.sqlite3_vtab,
            argc: c_int,
            argv: [*c]?*c.sqlite3_value,
            row_id_ptr: [*c]c.sqlite3_int64,
        ) callconv(.C) c_int {
            const state = @fieldParentPtr(State, "vtab", vtab);

            var arena = heap.ArenaAllocator.init(state.allocator);
            defer arena.deinit();
            var cb_ctx = CallbackContext{ .arena = &arena };

            var values = ChangeSet.init(
                @as([*c]?*c.sqlite3_value, @ptrCast(argv))[0..@intCast(argc)],
            );
            state.table.update(&cb_ctx, @ptrCast(row_id_ptr), values) catch |e| {
                std.log.err("error calling update on table: {any}", .{e});
                // TODO how to set the error message?
                //err_str.* = dupeToSQLiteString(cb_ctx.error_message);
                return c.SQLITE_ERROR;
            };

            return c.SQLITE_OK;
        }

        fn xBestIndex(
            vtab: [*c]c.sqlite3_vtab,
            index_info_ptr: [*c]c.sqlite3_index_info,
        ) callconv(.C) c_int {
            const state = @fieldParentPtr(State, "vtab", vtab);

            var arena = heap.ArenaAllocator.init(state.allocator);
            defer arena.deinit();
            var cb_ctx = CallbackContext{ .arena = &arena };

            var best_index = BestIndexBuilder.init(arena.allocator(), index_info_ptr) catch |e| {
                std.log.err("error initializing BestIndexBuilder: {any}", .{e});
                return c.SQLITE_ERROR;
            };

            state.table.bestIndex(&cb_ctx, &best_index) catch |e| {
                std.log.err("error calling bestIndex on table: {any}", .{e});
                return c.SQLITE_ERROR;
            };

            return c.SQLITE_OK;
        }

        fn xOpen(
            vtab: [*c]c.sqlite3_vtab,
            vtab_cursor: [*c][*c]c.sqlite3_vtab_cursor,
        ) callconv(.C) c_int {
            const state = @fieldParentPtr(State, "vtab", vtab);

            var arena = heap.ArenaAllocator.init(state.allocator);
            defer arena.deinit();
            var cb_ctx = CallbackContext{ .arena = &arena };

            var cursor_state = state.allocator.create(CursorState) catch {
                std.log.err("out of memory", .{});
                return c.SQLITE_ERROR;
            };
            errdefer state.allocator.destroy(cursor_state);

            cursor_state.* = .{
                .vtab_cursor = mem.zeroes(c.sqlite3_vtab_cursor),
                .allocator = state.allocator,
                .cursor = state.table.open(&cb_ctx) catch |e| {
                    std.log.err("error creating cursor: {any}", .{e});
                    return c.SQLITE_ERROR;
                },
            };

            vtab_cursor.* = @ptrCast(cursor_state);

            return c.SQLITE_OK;
        }

        fn xFilter(
            vtab_cursor: [*c]c.sqlite3_vtab_cursor,
            idx_num: c_int,
            idx_str: [*c]const u8,
            argc: c_int,
            argv: [*c]?*c.sqlite3_value,
        ) callconv(.C) c_int {
            _ = idx_num;
            _ = idx_str;
            _ = argc;
            _ = argv;
            const state = @fieldParentPtr(CursorState, "vtab_cursor", vtab_cursor);
            state.cursor.begin() catch |e| {
                std.log.err("error calling begin (xFilter) on cursor: {any}", .{e});
                return c.SQLITE_ERROR;
            };
            return c.SQLITE_OK;
        }

        // const FilterArgsFromCPointerError = error{} || mem.Allocator.Error;

        // fn filterArgsFromCPointer(
        //     allocator: mem.Allocator,
        //     argc: c_int,
        //     argv: [*c]?*c.sqlite3_value,
        // ) FilterArgsFromCPointerError![]FilterArg {
        // }

        fn xEof(vtab_cursor: [*c]c.sqlite3_vtab_cursor) callconv(.C) c_int {
            const state = @fieldParentPtr(CursorState, "vtab_cursor", vtab_cursor);
            return @intFromBool(state.cursor.eof());
        }

        fn xNext(vtab_cursor: [*c]c.sqlite3_vtab_cursor) callconv(.C) c_int {
            const state = @fieldParentPtr(CursorState, "vtab_cursor", vtab_cursor);
            state.cursor.next() catch |e| {
                std.log.err("error calling next on cursor: {any}", .{e});
                return c.SQLITE_ERROR;
            };
            return c.SQLITE_OK;
        }

        fn xColumn(
            vtab_cursor: [*c]c.sqlite3_vtab_cursor,
            ctx: ?*c.sqlite3_context,
            n: c_int,
        ) callconv(.C) c_int {
            const state = @fieldParentPtr(CursorState, "vtab_cursor", vtab_cursor);
            const result = Result{ .ctx = ctx };
            state.cursor.column(result, @intCast(n)) catch |e| {
                std.log.err("error calling rowid on cursor: {any}", .{e});
                return c.SQLITE_ERROR;
            };
            return c.SQLITE_OK;
        }

        fn xRowid(
            vtab_cursor: [*c]c.sqlite3_vtab_cursor,
            rowid_ptr: [*c]c.sqlite3_int64,
        ) callconv(.C) c_int {
            const state = @fieldParentPtr(CursorState, "vtab_cursor", vtab_cursor);
            rowid_ptr.* = state.cursor.rowid() catch |e| {
                std.log.err("error calling rowid on cursor: {any}", .{e});
                return c.SQLITE_ERROR;
            };
            return c.SQLITE_OK;
        }

        fn xClose(vtab_cursor: [*c]c.sqlite3_vtab_cursor) callconv(.C) c_int {
            var cursor_state = @fieldParentPtr(CursorState, "vtab_cursor", vtab_cursor);
            cursor_state.cursor.deinit();
            cursor_state.allocator.destroy(cursor_state);
            return c.SQLITE_OK;
        }

        fn xBegin(vtab: [*c]c.sqlite3_vtab) callconv(.C) c_int {
            return callTableCallback("begin", Table.begin, .{}, vtab);
        }

        fn xCommit(vtab: [*c]c.sqlite3_vtab) callconv(.C) c_int {
            return callTableCallback("commit", Table.commit, .{}, vtab);
        }

        fn xRollback(vtab: [*c]c.sqlite3_vtab) callconv(.C) c_int {
            return callTableCallback("rollback", Table.rollback, .{}, vtab);
        }

        fn xSavepoint(vtab: [*c]c.sqlite3_vtab, savepoint_id: c_int) callconv(.C) c_int {
            const sid: i32 = @intCast(savepoint_id);
            return callTableCallback("savepoint", Table.savepoint, .{sid}, vtab);
        }

        fn xRelease(vtab: [*c]c.sqlite3_vtab, savepoint_id: c_int) callconv(.C) c_int {
            const sid: i32 = @intCast(savepoint_id);
            return callTableCallback("release", Table.release, .{sid}, vtab);
        }

        fn xRollbackTo(
            vtab: [*c]c.sqlite3_vtab,
            savepoint_id: c_int,
        ) callconv(.C) c_int {
            const sid: i32 = @intCast(savepoint_id);
            return callTableCallback("rollbackTo", Table.rollbackTo, .{sid}, vtab);
        }

        fn xShadowName(name: [*c]const u8) callconv(.C) c_int {
            const n: [:0]const u8 = std.mem.span(name);
            return @intFromBool(Table.isShadowName(n));
        }

        fn callTableCallback(
            comptime functionName: []const u8,
            comptime function: anytype,
            args: anytype,
            vtab: [*c]c.sqlite3_vtab,
        ) c_int {
            const state = @fieldParentPtr(State, "vtab", vtab);

            var arena = heap.ArenaAllocator.init(state.allocator);
            defer arena.deinit();
            var cb_ctx = CallbackContext{ .arena = &arena };

            const full_args = .{ state.table, &cb_ctx } ++ args;

            @call(.auto, function, full_args) catch |e| {
                std.log.err("error calling {s} on table: {any}", .{ functionName, e });
                return c.SQLITE_ERROR;
            };

            return c.SQLITE_OK;
        }
    };
}

fn dupeToSQLiteString(s: []const u8) [*c]const u8 {
    const len: c_int = @intCast(s.len);
    var buffer: [*c]u8 = @ptrCast(c.sqlite3_malloc(len + 1));

    mem.copy(u8, buffer[0..s.len], s);
    buffer[s.len] = 0;

    return buffer;
}
