//! This is inspired by [zig-sqlite](https://github.com/vrischmann/zig-sqlite)

const std = @import("std");
const ascii = std.ascii;
const debug = std.debug;
const fmt = std.fmt;
const heap = std.heap;
const log = std.log;
const mem = std.mem;
const meta = std.meta;
const testing = std.testing;

const c = @import("c.zig").c;
const versionGreaterThanOrEqualTo = @import("c.zig").versionGreaterThanOrEqualTo;

const ChangeSet = @import("ChangeSet.zig");
const Conn = @import("Conn.zig");
const ValueRef = @import("value.zig").Ref;

/// CallbackContext is only valid for the duration of a callback from sqlite into the
/// virtual table instance. It should not be saved between calls, and it is provided to
/// every callback function.
pub const CallbackContext = struct {
    arena: *heap.ArenaAllocator,
    error_message: []const u8 = "unspecified error",

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

    pub fn captureErrMsg(
        self: *CallbackContext,
        err: anytype,
        comptime format_string: []const u8,
        values: anytype,
    ) @TypeOf(err) {
        switch (comptime meta.activeTag(@typeInfo(@TypeOf(err)))) {
            .ErrorSet => {
                self.setErrorMessage(format_string ++ ": {!}", values ++ .{err});
                return err;
            },
            else => @compileError("`err` must be an ErrorSet"),
        }
    }

    pub fn wrapErrorMessage(
        self: *CallbackContext,
        comptime error_context_msg: []const u8,
        values: anytype,
    ) void {
        self.error_message = fmt.allocPrint(
            self.arena.allocator(),
            error_context_msg ++ ": {s}",
            values ++ .{self.error_message},
        ) catch |err| switch (err) {
            error.OutOfMemory => self.error_message,
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

pub const CollationSequence = enum {
    binary,
    nocase,
    rtrim,

    pub fn fromName(name: [:0]const u8) ?CollationSequence {
        if (ascii.eqlIgnoreCase("BINARY", name)) {
            return .binary;
        }
        if (ascii.eqlIgnoreCase("NOCASE", name)) {
            return .nocase;
        }
        if (ascii.eqlIgnoreCase("RTRIM", name)) {
            return .rtrim;
        }
        return null;
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

        pub fn format(
            self: Op,
            comptime _: []const u8,
            _: std.fmt.FormatOptions,
            writer: anytype,
        ) !void {
            const op_str = switch (self) {
                .eq => "=",
                .gt => ">",
                .le => "<=",
                .lt => "<",
                .ge => ">=",
                .ne => "!=",
                .is_not => "IS NOT",
                .is => "IS",
                .is_not_null => "IS NOT NULL",
                .is_null => "IS NULL",
                .match => "MATCH",
                .like => "LIKE",
                else => "?",
            };
            try writer.print("{s}", .{op_str});
        }
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

        pub fn format(
            self: Constraint,
            comptime _: []const u8,
            _: std.fmt.FormatOptions,
            writer: anytype,
        ) !void {
            try writer.print("{} col {}", .{ self.op(), self.columnIndex() });
        }

        /// Returns null if the collation sequence is not one of the standard 3 collation sequences
        pub fn collation(self: Constraint) ?CollationSequence {
            const name = c.sqlite3_vtab_collation(self.parent.?, @intCast(self.index));
            return CollationSequence.fromName(mem.span(name));
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

        fn setErrorMsg(self: *@This(), err_msg: []const u8) void {
            // Any existing error message must be freed before a new one is set
            // See: https://www.sqlite.org/vtab.html#implementation
            if (self.vtab.zErrMsg != null) {
                c.sqlite3_free(self.vtab.zErrMsg);
            }
            self.vtab.zErrMsg = dupeToSQLiteString(err_msg);
        }
    };

    const CursorState = struct {
        vtab_cursor: c.sqlite3_vtab_cursor,
        allocator: mem.Allocator,
        arena_pool: *ArenaPool,
        cursor: Table.Cursor,

        fn vtabState(self: @This()) *State {
            return @fieldParentPtr(State, "vtab", self.vtab_cursor.pVtab);
        }

        fn cbCtx(self: *@This()) !CallbackContext {
            const arena = try self.arena_pool.take(self.allocator);
            return CallbackContext.init(arena);
        }

        fn reclaimCbCtx(self: *@This(), cb_ctx: *CallbackContext) void {
            self.arena_pool.giveBack(cb_ctx.arena);
        }
    };

    const Creatable = struct {
        //! Contains callbacks for virtual tables that can be created & destroyed (non-eponymous)

        fn xCreate(
            db: ?*c.sqlite3,
            module_context_ptr: ?*anyopaque,
            argc: c_int,
            argv: [*c]const [*c]const u8,
            vtab: [*c][*c]c.sqlite3_vtab,
            err_str: [*c][*c]const u8,
        ) callconv(.C) c_int {
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

            Table.create(&state.table, allocator, conn, &cb_ctx, args) catch {
                cb_ctx.wrapErrorMessage("failed to create virtual table `{s}`", .{args[2]});
                err_str.* = dupeToSQLiteString(cb_ctx.error_message);
                return c.SQLITE_ERROR;
            };
            errdefer state.table.destroy();

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

        fn xDestroy(vtab: [*c]c.sqlite3_vtab) callconv(.C) c_int {
            const state = @fieldParentPtr(State, "vtab", vtab);

            {
                // Ensure the cb_ctx is reclaimed before the rest of the cleanup happens

                var cb_ctx = state.cbCtx() catch {
                    state.setErrorMsg(
                        "failed to allocate arena for callback context. out of memory",
                    );
                    return c.SQLITE_ERROR;
                };
                defer state.reclaimCbCtx(&cb_ctx);

                state.table.destroy(&cb_ctx);
            }

            state.arena_pool.deinit();
            state.allocator.destroy(state);

            return c.SQLITE_OK;
        }
    };

    const Writeable = struct {
        //! Contains the update callback for virtual tables that can be written to

        fn xUpdate(
            vtab: [*c]c.sqlite3_vtab,
            argc: c_int,
            argv: [*c]?*c.sqlite3_value,
            row_id_ptr: [*c]c.sqlite3_int64,
        ) callconv(.C) c_int {
            const state = @fieldParentPtr(State, "vtab", vtab);
            var cb_ctx = state.cbCtx() catch {
                state.setErrorMsg("failed to allocate arena for callback context. out of memory");
                return c.SQLITE_ERROR;
            };
            defer state.reclaimCbCtx(&cb_ctx);

            const values = ChangeSet.init(
                @as([*c]?*c.sqlite3_value, @ptrCast(argv))[0..@intCast(argc)],
            );
            state.table.update(&cb_ctx, @ptrCast(row_id_ptr), values) catch {
                cb_ctx.wrapErrorMessage("{s} failed", .{values.changeType().name()});
                state.setErrorMsg(cb_ctx.error_message);
                return c.SQLITE_ERROR;
            };

            return c.SQLITE_OK;
        }
    };

    const Transactable = struct {
        //! Contains the transaction and savepoint callbacks for virtual tables that handle
        //! transactions

        fn xBegin(vtab: [*c]c.sqlite3_vtab) callconv(.C) c_int {
            return callTxnCallback("BEGIN", Table.begin, .{}, vtab);
        }

        fn xSync(vtab: [*c]c.sqlite3_vtab) callconv(.C) c_int {
            return callTxnCallback("SYNC", Table.sync, .{}, vtab);
        }

        fn xCommit(vtab: [*c]c.sqlite3_vtab) callconv(.C) c_int {
            const state = @fieldParentPtr(State, "vtab", vtab);
            var cb_ctx = state.cbCtx() catch {
                state.setErrorMsg("failed to allocate arena for callback context. out of memory");
                return c.SQLITE_ERROR;
            };
            defer state.reclaimCbCtx(&cb_ctx);

            // The commit callback does not return an error because it appears that any error
            // returned from `xCommit` is swallowed by SQLite (makes sense since that is the point
            // of `xSync`). Use `xSync` to return an error and abort a transaction.
            state.table.commit(&cb_ctx);

            return c.SQLITE_OK;
        }

        fn xRollback(vtab: [*c]c.sqlite3_vtab) callconv(.C) c_int {
            return callTxnCallback("ROLLBACK", Table.rollback, .{}, vtab);
        }

        fn xSavepoint(vtab: [*c]c.sqlite3_vtab, savepoint_id: c_int) callconv(.C) c_int {
            const sid: i32 = @intCast(savepoint_id);
            return callTxnCallback("SAVEPOINT", Table.savepoint, .{sid}, vtab);
        }

        fn xRelease(vtab: [*c]c.sqlite3_vtab, savepoint_id: c_int) callconv(.C) c_int {
            const sid: i32 = @intCast(savepoint_id);
            return callTxnCallback("RELEASE", Table.release, .{sid}, vtab);
        }

        fn xRollbackTo(
            vtab: [*c]c.sqlite3_vtab,
            savepoint_id: c_int,
        ) callconv(.C) c_int {
            const sid: i32 = @intCast(savepoint_id);
            return callTxnCallback("ROLLBACK TO", Table.rollbackTo, .{sid}, vtab);
        }

        fn callTxnCallback(
            comptime op_name: []const u8,
            comptime function: anytype,
            args: anytype,
            vtab: [*c]c.sqlite3_vtab,
        ) c_int {
            const state = @fieldParentPtr(State, "vtab", vtab);
            var cb_ctx = state.cbCtx() catch {
                state.setErrorMsg("failed to allocate arena for callback context. out of memory");
                return c.SQLITE_ERROR;
            };
            defer state.reclaimCbCtx(&cb_ctx);

            const full_args = .{ &state.table, &cb_ctx } ++ args;

            @call(.auto, function, full_args) catch {
                cb_ctx.wrapErrorMessage(op_name ++ " failed", .{});
                state.setErrorMsg(cb_ctx.error_message);
                return c.SQLITE_ERROR;
            };

            return c.SQLITE_OK;
        }
    };

    const Renamable = struct {
        fn xRename(vtab: [*c]c.sqlite3_vtab, new_name: [*c]const u8) callconv(.C) c_int {
            const state = @fieldParentPtr(State, "vtab", vtab);
            var cb_ctx = state.cbCtx() catch {
                state.setErrorMsg("failed to allocate arena for callback context. out of memory");
                return c.SQLITE_ERROR;
            };
            defer state.reclaimCbCtx(&cb_ctx);

            const new_name_checked: [:0]const u8 = std.mem.span(new_name);
            state.table.rename(&cb_ctx, new_name_checked) catch {
                cb_ctx.wrapErrorMessage("rename table to {s} failed", .{new_name_checked});
                state.setErrorMsg(cb_ctx.error_message);
                return c.SQLITE_ERROR;
            };

            return c.SQLITE_OK;
        }
    };

    const HasShadowTables = struct {
        fn xShadowName(name: [*c]const u8) callconv(.C) c_int {
            const n: [:0]const u8 = std.mem.span(name);
            return @intFromBool(Table.isShadowName(n));
        }
    };

    return struct {
        const Self = @This();

        pub const module = c.sqlite3_module{
            .iVersion = 3,
            .xCreate = if (tableHasDecl("create")) Creatable.xCreate else null,
            .xConnect = xConnect,
            .xBestIndex = xBestIndex,
            .xDisconnect = xDisconnect,
            .xDestroy = if (tableHasDecl("destroy")) Creatable.xDestroy else xDisconnect,
            .xOpen = xOpen,
            .xClose = xClose,
            .xFilter = xFilter,
            .xNext = xNext,
            .xEof = xEof,
            .xColumn = xColumn,
            .xRowid = xRowid,
            .xUpdate = if (tableHasDecl("update")) Writeable.xUpdate else null,
            .xBegin = if (tableHasDecl("begin")) Transactable.xBegin else null,
            .xSync = if (tableHasDecl("sync")) Transactable.xSync else null,
            .xCommit = if (tableHasDecl("commit")) Transactable.xCommit else null,
            .xRollback = if (tableHasDecl("rollback")) Transactable.xRollback else null,
            .xFindFunction = null,
            .xRename = if (tableHasDecl("rename")) Renamable.xRename else null,
            .xSavepoint = if (tableHasDecl("savepoint")) Transactable.xSavepoint else null,
            .xRelease = if (tableHasDecl("release")) Transactable.xRelease else null,
            .xRollbackTo = if (tableHasDecl("rollbackTo")) Transactable.xRollbackTo else null,
            .xShadowName = if (tableHasDecl("isShadowName")) HasShadowTables.xShadowName else null,
        };

        const table_decls = switch (@typeInfo(Table)) {
            .Struct => |table_struct| table_struct.decls,
            else => @compileError("vtab table type must be a struct"),
        };

        fn tableHasDecl(decl_name: []const u8) bool {
            for (table_decls) |decl| {
                if (mem.eql(u8, decl_name, decl.name)) {
                    return true;
                }
            }
            return false;
        }

        fn xConnect(
            db: ?*c.sqlite3,
            module_context_ptr: ?*anyopaque,
            argc: c_int,
            argv: [*c]const [*c]const u8,
            vtab: [*c][*c]c.sqlite3_vtab,
            err_str: [*c][*c]const u8,
        ) callconv(.C) c_int {
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

            Table.connect(&state.table, allocator, conn, &cb_ctx, args) catch {
                cb_ctx.wrapErrorMessage("failed to connect to virtual table `{s}`", .{args[2]});
                err_str.* = dupeToSQLiteString(cb_ctx.error_message);
                return c.SQLITE_ERROR;
            };
            errdefer state.table.disconnect();

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

        fn xDisconnect(vtab: [*c]c.sqlite3_vtab) callconv(.C) c_int {
            const state = @fieldParentPtr(State, "vtab", vtab);

            state.table.disconnect();
            state.arena_pool.deinit();
            state.allocator.destroy(state);

            return c.SQLITE_OK;
        }

        fn xBestIndex(
            vtab: [*c]c.sqlite3_vtab,
            index_info_ptr: [*c]c.sqlite3_index_info,
        ) callconv(.C) c_int {
            const state = @fieldParentPtr(State, "vtab", vtab);
            var cb_ctx = state.cbCtx() catch {
                state.setErrorMsg("failed to allocate arena for callback context. out of memory");
                return c.SQLITE_ERROR;
            };
            defer state.reclaimCbCtx(&cb_ctx);

            const best_index = BestIndexInfo{ .index_info = @ptrCast(index_info_ptr) };
            const found_solution = state.table.bestIndex(&cb_ctx, best_index) catch {
                cb_ctx.wrapErrorMessage("error occurred during query planning", .{});
                state.setErrorMsg(cb_ctx.error_message);
                return c.SQLITE_ERROR;
            };

            if (!found_solution) {
                log.debug("no query solution with specified constraints", .{});
                return c.SQLITE_CONSTRAINT;
            }

            return c.SQLITE_OK;
        }

        fn xOpen(
            vtab: [*c]c.sqlite3_vtab,
            vtab_cursor: [*c][*c]c.sqlite3_vtab_cursor,
        ) callconv(.C) c_int {
            const state = @fieldParentPtr(State, "vtab", vtab);
            var cb_ctx = state.cbCtx() catch {
                state.setErrorMsg("failed to allocate arena for callback context. out of memory");
                return c.SQLITE_ERROR;
            };
            defer state.reclaimCbCtx(&cb_ctx);

            const cursor_state = state.allocator.create(CursorState) catch {
                state.setErrorMsg("failed to allocate cursor: out of memory");
                return c.SQLITE_ERROR;
            };
            errdefer state.allocator.destroy(cursor_state);

            const cursor = state.table.open(&cb_ctx) catch {
                cb_ctx.wrapErrorMessage("failed to create vtab cursor", .{});
                state.setErrorMsg(cb_ctx.error_message);
                return c.SQLITE_ERROR;
            };
            cursor_state.* = .{
                .vtab_cursor = mem.zeroes(c.sqlite3_vtab_cursor),
                .allocator = state.allocator,
                .arena_pool = &state.arena_pool,
                .cursor = cursor,
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
                state.vtabState().setErrorMsg(
                    "failed to allocate arena for callback context. out of memory",
                );
                return c.SQLITE_ERROR;
            };
            defer state.reclaimCbCtx(&cb_ctx);

            state.cursor.begin(&cb_ctx, index_id_num, index_id_str, filter_args) catch {
                cb_ctx.wrapErrorMessage("failed to init vtab cursor with filter", .{});
                state.vtabState().setErrorMsg(cb_ctx.error_message);
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
            state.cursor.next() catch {
                state.vtabState().setErrorMsg("error in next on vtab cursor");
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
            state.cursor.column(result, @intCast(n)) catch {
                state.vtabState().setErrorMsg("error fetching column value from vtab cursor");
                return c.SQLITE_ERROR;
            };
            return c.SQLITE_OK;
        }

        fn xRowid(
            vtab_cursor: [*c]c.sqlite3_vtab_cursor,
            rowid_ptr: [*c]c.sqlite3_int64,
        ) callconv(.C) c_int {
            const state = @fieldParentPtr(CursorState, "vtab_cursor", vtab_cursor);
            rowid_ptr.* = state.cursor.rowid() catch {
                state.vtabState().setErrorMsg("error fetching rowid value from vtab cursor");
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
    };
}

fn getModuleAllocator(ptr: ?*anyopaque) *mem.Allocator {
    return @ptrCast(@alignCast(ptr.?));
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

fn dupeToSQLiteString(s: []const u8) [*c]u8 {
    const len: c_int = @intCast(s.len);
    var buffer: [*c]u8 = @ptrCast(c.sqlite3_malloc(len + 1));

    // sqlite3_malloc returns a null pointer if it can't allocate the memory
    if (buffer == null) {
        log.err(
            "failed to alloc diagnostic message in sqlite: out of memory. original message: {s}",
            .{s},
        );
        return null;
    }

    @memcpy(buffer[0..s.len], s);
    buffer[s.len] = 0;

    return buffer;
}
