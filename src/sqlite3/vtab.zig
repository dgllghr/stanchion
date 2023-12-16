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
/// every callback function.
pub const CallbackContext = struct {
    arena: *heap.ArenaAllocator,
    error_message: []const u8 = "unknown error",

    pub fn init(arena: *heap.ArenaAllocator) CallbackContext {
        return .{ .arena = arena };
    }

    pub fn setErrorMessage(
        self: *CallbackContext,
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

const ArenaPool = struct {
    const FreeList = std.DoublyLinkedList(heap.ArenaAllocator);

    node_arena: heap.ArenaAllocator,
    arenas: FreeList,

    fn init(allocator: mem.Allocator) ArenaPool {
        return .{
            .node_arena = heap.ArenaAllocator.init(allocator),
            .arenas = .{},
        };
    }

    fn deinit(self: *ArenaPool) void {
        while (self.arenas.popFirst()) |*node| {
            node.*.data.deinit();
        }
        self.node_arena.deinit();
    }

    fn take(self: *ArenaPool, allocator: mem.Allocator) !*heap.ArenaAllocator {
        if (self.arenas.popFirst()) |node| {
            return &node.data;
        } else {
            const node = try self.node_arena.allocator().create(FreeList.Node);
            node.data = heap.ArenaAllocator.init(allocator);
            return &node.data;
        }
    }

    fn insert(self: *ArenaPool, arena: heap.ArenaAllocator) !void {
        const node = try self.node_arena.allocator().create(FreeList.Node);
        node.data = arena;
    }

    fn giveBack(self: *ArenaPool, arena: *heap.ArenaAllocator) void {
        // TODO make the retained capacity limit configurable
        _ = arena.reset(.{ .retain_with_limit = 4000 });
        const node = @fieldParentPtr(FreeList.Node, "data", arena);
        self.arenas.append(node);
    }
};

pub const BestIndexInfo = struct {
    index_info: ?*c.sqlite3_index_info,

    pub const Op = enum(c_char) {
        eq = c.SQLITE_INDEX_CONSTRAINT_EQ,
        gt = c.SQLITE_INDEX_CONSTRAINT_GT,
        le = c.SQLITE_INDEX_CONSTRAINT_LE,
        lt = c.SQLITE_INDEX_CONSTRAINT_LT,
        ge = c.SQLITE_INDEX_CONSTRAINT_GE,
        match = c.SQLITE_INDEX_CONSTRAINT_MATCH,
        like = c.SQLITE_INDEX_CONSTRAINT_LIKE,
        glob = c.SQLITE_INDEX_CONSTRAINT_GLOB,
        regexp = c.SQLITE_INDEX_CONSTRAINT_REGEXP,
        ne = c.SQLITE_INDEX_CONSTRAINT_NE,
        is_not = c.SQLITE_INDEX_CONSTRAINT_ISNOT,
        is_not_null = c.SQLITE_INDEX_CONSTRAINT_ISNOTNULL,
        is_null = c.SQLITE_INDEX_CONSTRAINT_ISNULL,
        is = c.SQLITE_INDEX_CONSTRAINT_IS,
        limit = c.SQLITE_INDEX_CONSTRAINT_LIMIT,
        offset = c.SQLITE_INDEX_CONSTRAINT_OFFSET,
        function = @bitCast(@as(u8, c.SQLITE_INDEX_CONSTRAINT_FUNCTION)),
        _,
    };

    pub const Constraint = struct {
        parent: ?*c.sqlite3_index_info,
        index: usize,

        pub fn columnIndex(self: Constraint) u32 {
            return @intCast(self.parent.?.*.aConstraint[self.index].iColumn);
        }

        pub fn op(self: Constraint) Op {
            return @enumFromInt(self.parent.?.*.aConstraint[self.index].op);
        }

        pub fn usable(self: Constraint) bool {
            return self.parent.?.*.aConstraint[self.index].usable != 0;
        }

        pub fn includeArgInFilter(self: Constraint, argIndex: u32) void {
            var usage = &self.parent.?.*.aConstraintUsage[self.index];
            usage.argvIndex = @intCast(argIndex);
        }

        pub fn setOmitTest(self: Constraint, omit: bool) void {
            var usage = &self.parent.?.*.aConstraintUsage[self.index];
            usage.omit = @intFromBool(omit);
        }
    };

    pub fn constraintsLen(self: BestIndexInfo) u32 {
        return @intCast(self.index_info.?.*.nConstraint);
    }

    pub fn constraint(self: BestIndexInfo, index: usize) Constraint {
        return .{ .parent = self.index_info, .index = index };
    }

    pub fn setIdentifier(self: BestIndexInfo, num: i32) void {
        self.index_info.?.*.idxNum = @intCast(num);
    }

    pub fn setEstimatedCost(self: BestIndexInfo, cost: f64) void {
        self.index_info.?.*.estimatedCost = @floatCast(cost);
    }
};

pub const FilterArgs = struct {
    values: [*c]?*c.sqlite3_value,
    values_len: usize,

    pub fn valuesLen(self: FilterArgs) usize {
        return self.values_len;
    }

    pub fn readValue(self: FilterArgs, index: usize) !ValueRef {
        return .{
            .value = self.values[index],
        };
    }
};

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
        /// The different functions receive a pointer to a vtab so use `fieldParentPtr`
        /// to get the state
        vtab: c.sqlite3_vtab,
        allocator: mem.Allocator,
        arena_pool: ArenaPool,
        /// The table is the actual virtual table implementation.
        table: Table,

        fn cbCtx(self: *@This()) !CallbackContext {
            const arena = try self.arena_pool.take(self.allocator);
            return CallbackContext.init(arena);
        }

        fn reclaimCbCtx(self: *@This(), cb_ctx: *CallbackContext) void {
            self.arena_pool.giveBack(cb_ctx.arena);
        }
    };

    const CursorState = struct {
        vtab_cursor: c.sqlite3_vtab_cursor,
        allocator: mem.Allocator,
        arena_pool: *ArenaPool,
        cursor: Table.Cursor,

        fn cbCtx(self: *@This()) !CallbackContext {
            const arena = try self.arena_pool.take(self.allocator);
            return CallbackContext.init(arena);
        }

        fn reclaimCbCtx(self: *@This(), cb_ctx: *CallbackContext) void {
            self.arena_pool.giveBack(cb_ctx.arena);
        }
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
                .xSync = xSync,
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
                .xSync = xSync,
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

            var tmp_arena = heap.ArenaAllocator.init(allocator);
            errdefer tmp_arena.deinit();

            const args = parseModuleArguments(tmp_arena.allocator(), argc, argv) catch {
                err_str.* = dupeToSQLiteString("out of memory");
                return c.SQLITE_ERROR;
            };

            var state = allocator.create(State) catch {
                err_str.* = dupeToSQLiteString("out of memory");
                return c.SQLITE_ERROR;
            };
            errdefer allocator.destroy(state);

            state.vtab = mem.zeroes(c.sqlite3_vtab);
            state.allocator = allocator;
            state.arena_pool = ArenaPool.init(allocator);
            state.arena_pool.insert(tmp_arena) catch {
                err_str.* = dupeToSQLiteString("out of memory");
                return c.SQLITE_ERROR;
            };

            const conn = Conn.init(db.?);

            var cb_ctx = state.cbCtx() catch {
                err_str.* = dupeToSQLiteString("out of memory");
                return c.SQLITE_ERROR;
            };
            defer state.reclaimCbCtx(&cb_ctx);

            const initTableFn = if (create) Table.create else Table.connect;
            initTableFn(&state.table, allocator, conn, &cb_ctx, args) catch {
                err_str.* = dupeToSQLiteString(cb_ctx.error_message);
                return c.SQLITE_ERROR;
            };
            errdefer if (create) state.table.destroy() else state.table.disconnect();

            const schema = state.table.ddl(cb_ctx.arena.allocator()) catch {
                err_str.* = dupeToSQLiteString("out of memory");
                return c.SQLITE_ERROR;
            };
            const res = c.sqlite3_declare_vtab(db, @ptrCast(schema));
            if (res != c.SQLITE_OK) {
                return c.SQLITE_ERROR;
            }

            vtab.* = @ptrCast(state);

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

            state.table.disconnect();
            state.arena_pool.deinit();
            state.allocator.destroy(state);

            return c.SQLITE_OK;
        }

        fn xDestroy(vtab: [*c]c.sqlite3_vtab) callconv(.C) c_int {
            const state = @fieldParentPtr(State, "vtab", vtab);
            var cb_ctx = state.cbCtx() catch {
                std.log.err("error allocating arena for callback context. out of memory", .{});
                return c.SQLITE_ERROR;
            };
            defer state.reclaimCbCtx(&cb_ctx);

            state.table.destroy(&cb_ctx);
            state.arena_pool.deinit();
            state.allocator.destroy(state);

            return c.SQLITE_OK;
        }

        fn xUpdate(
            vtab: [*c]c.sqlite3_vtab,
            argc: c_int,
            argv: [*c]?*c.sqlite3_value,
            row_id_ptr: [*c]c.sqlite3_int64,
        ) callconv(.C) c_int {
            const state = @fieldParentPtr(State, "vtab", vtab);
            var cb_ctx = state.cbCtx() catch {
                std.log.err("error allocating arena for callback context. out of memory", .{});
                return c.SQLITE_ERROR;
            };
            defer state.reclaimCbCtx(&cb_ctx);

            const values = ChangeSet.init(
                @as([*c]?*c.sqlite3_value, @ptrCast(argv))[0..@intCast(argc)],
            );
            state.table.update(&cb_ctx, @ptrCast(row_id_ptr), values) catch |e| {
                std.log.err("error calling update on vtab: {any}", .{e});
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
            var cb_ctx = state.cbCtx() catch {
                std.log.err("error allocating arena for callback context. out of memory", .{});
                return c.SQLITE_ERROR;
            };
            defer state.reclaimCbCtx(&cb_ctx);

            const best_index = BestIndexInfo{ .index_info = @ptrCast(index_info_ptr) };
            state.table.bestIndex(&cb_ctx, best_index) catch |e| {
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
            var cb_ctx = state.cbCtx() catch {
                std.log.err("error allocating arena for callback context. out of memory", .{});
                return c.SQLITE_ERROR;
            };
            defer state.reclaimCbCtx(&cb_ctx);

            const cursor_state = state.allocator.create(CursorState) catch {
                std.log.err("out of memory", .{});
                return c.SQLITE_ERROR;
            };
            errdefer state.allocator.destroy(cursor_state);

            cursor_state.* = .{
                .vtab_cursor = mem.zeroes(c.sqlite3_vtab_cursor),
                .allocator = state.allocator,
                .arena_pool = &state.arena_pool,
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
            const index_id_num: i32 = @intCast(idx_num);
            const index_id_str = if (idx_str == null) "" else mem.sliceTo(idx_str, 0);
            const filter_args = FilterArgs{ .values = argv, .values_len = @intCast(argc) };

            const state = @fieldParentPtr(CursorState, "vtab_cursor", vtab_cursor);
            var cb_ctx = state.cbCtx() catch {
                std.log.err("error allocating arena for callback context. out of memory", .{});
                return c.SQLITE_ERROR;
            };
            defer state.reclaimCbCtx(&cb_ctx);

            state.cursor.begin(&cb_ctx, index_id_num, index_id_str, filter_args) catch |e| {
                std.log.err("error calling begin (xFilter) on cursor: {any}", .{e});
                return c.SQLITE_ERROR;
            };

            return c.SQLITE_OK;
        }

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
                std.log.err("error calling column on cursor: {any}", .{e});
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

        fn xSync(vtab: [*c]c.sqlite3_vtab) callconv(.C) c_int {
            return callTableCallback("sync", Table.sync, .{}, vtab);
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
            var cb_ctx = state.cbCtx() catch {
                std.log.err("error allocating arena for callback context. out of memory", .{});
                return c.SQLITE_ERROR;
            };
            defer state.reclaimCbCtx(&cb_ctx);

            const full_args = .{ &state.table, &cb_ctx } ++ args;

            @call(.auto, function, full_args) catch |e| {
                std.log.err("error calling {s} on table: {any}", .{ functionName, e });
                return c.SQLITE_ERROR;
            };

            return c.SQLITE_OK;
        }
    };
}

fn parseModuleArguments(
    allocator: mem.Allocator,
    argc: c_int,
    argv: [*c]const [*c]const u8,
) ![]const []const u8 {
    const res = try allocator.alloc([]const u8, @intCast(argc));

    for (res, 0..) |*marg, i| {
        // The documentation of sqlite says each string in argv is null-terminated
        marg.* = mem.sliceTo(argv[i], 0);
    }

    return res;
}

fn dupeToSQLiteString(s: []const u8) [*c]const u8 {
    const len: c_int = @intCast(s.len);
    var buffer: [*c]u8 = @ptrCast(c.sqlite3_malloc(len + 1));

    @memcpy(buffer[0..s.len], s);
    buffer[s.len] = 0;

    return buffer;
}
