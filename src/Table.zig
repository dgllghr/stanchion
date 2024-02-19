const std = @import("std");
const fmt = std.fmt;
const log = std.log;
const mem = std.mem;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const sqlite_c = @import("sqlite3/c.zig").c;
const sqlite = @import("sqlite3.zig");
const ChangeSet = sqlite.ChangeSet;
const Conn = sqlite.Conn;
const SqliteErorr = sqlite.errors.Error;
const Stmt = sqlite.Stmt;
const ValueType = sqlite.ValueType;
const ValueRef = sqlite.ValueRef;
const vtab = @import("sqlite3/vtab.zig");
const Result = vtab.Result;

const ctx_mod = @import("ctx.zig");
const VtabCtx = ctx_mod.VtabCtx;
const VtabCtxSchemaless = ctx_mod.VtabCtxSchemaless;

const ShadowTableError = @import("shadow_table.zig").Error;

const schema_mod = @import("schema.zig");
const Schema = schema_mod.Schema;
const SchemaDef = schema_mod.SchemaDef;
const SchemaManager = schema_mod.Manager;

const segment = @import("segment.zig");

const row_group = @import("row_group.zig");
const BlobManager = @import("BlobManager.zig");
const PendingInserts = @import("PendingInserts.zig");
const RowGroupCreator = row_group.Creator;
const RowGroupIndex = row_group.Index;

const TableData = @import("TableData.zig");

const index = @import("index.zig");
const Index = index.Index;

const Self = @This();

allocator: Allocator,
table_static_arena: ArenaAllocator,

ctx: VtabCtx,

schema_manager: SchemaManager,
table_data: TableData,
blob_manager: BlobManager,
row_group_index: RowGroupIndex,
pending_inserts: PendingInserts,

row_group_creator: RowGroupCreator,

/// In order to synchronize `next_rowid` across multiple connections to the same sqlite database,
/// it is loaded from the table data when the first insert in a transaction occurs. This is safe
/// because only one write transaction can be active in a sqlite database at a time. The in memory
/// value of `next_rowid` is incremented and used as additional inserts happen within a transaction
/// so that rowid generation is fast for subsequent inserts. When the transaction is about to
/// commit, `next_rowid` is persisted to the table data and cleared from memory. This process
/// allows `next_rowid` to be used as a "dirty" flag to see if any writes occurred in a
/// transaction.
next_rowid: ?i64,
warned_update_delete_not_supported: bool = false,

pub const InitError = error{
    NoColumns,
    UnsupportedDb,
} || SchemaDef.ParseError || SchemaManager.Error || SqliteErorr || Allocator.Error ||
    ShadowTableError;

pub fn create(
    self: *Self,
    allocator: Allocator,
    conn: Conn,
    cb_ctx: *vtab.CallbackContext,
    args: []const []const u8,
) InitError!void {
    // TODO wrap all of this in a savepoint

    if (args.len < 5) {
        cb_ctx.setErrorMessage("table must have at least 1 column", .{});
        return InitError.NoColumns;
    }

    const db_name = args[1];
    if (!mem.eql(u8, "main", db_name)) {
        cb_ctx.setErrorMessage(
            "unable to create table in db `{s}`: only `main` db supported",
            .{db_name},
        );
        return InitError.UnsupportedDb;
    }

    // Use the tmp arena because the schema def is not stored with the table. The data is converted
    // into a Schema and the Schema is stored.
    const def = SchemaDef.parse(cb_ctx.arena, args[3..]) catch |e| {
        return cb_ctx.captureErrMsg(e, "failed to parse schema definition", .{});
    };

    self.allocator = allocator;
    self.table_static_arena = ArenaAllocator.init(allocator);
    errdefer self.table_static_arena.deinit();

    const name = try self.table_static_arena.allocator().dupe(u8, args[2]);
    self.ctx.base = VtabCtxSchemaless.init(conn, name);

    self.schema_manager = SchemaManager.init(&self.ctx.base);
    errdefer self.schema_manager.deinit();
    self.schema_manager.table().create(cb_ctx.arena) catch |e| {
        return cb_ctx.captureErrMsg(e, "failed to create shadow table `{s}_columns`", .{name});
    };

    self.ctx.schema = self.schema_manager.create(
        &self.table_static_arena,
        cb_ctx.arena,
        &def,
    ) catch |e| {
        return cb_ctx.captureErrMsg(e, "failed to save schema", .{});
    };

    self.table_data = TableData.init(&self.ctx.base);
    errdefer self.table_data.deinit();
    self.table_data.table().create(cb_ctx.arena) catch |e| {
        return cb_ctx.captureErrMsg(e, "failed to create shadow table `{s}_tabledata`", .{name});
    };

    self.blob_manager = BlobManager.init(&self.table_static_arena, &self.ctx.base) catch |e| {
        return cb_ctx.captureErrMsg(e, "failed to init blob manager", .{});
    };
    errdefer self.blob_manager.deinit();
    self.blob_manager.table().create(cb_ctx.arena) catch |e| {
        return cb_ctx.captureErrMsg(e, "failed to create shadow table `{s}_blobs`", .{name});
    };

    self.row_group_index = RowGroupIndex.init(&self.ctx);
    errdefer self.row_group_index.deinit();
    self.row_group_index.table().create(cb_ctx.arena) catch |e| {
        return cb_ctx.captureErrMsg(
            e,
            "failed to create shadow table `{s}_rowgroupindex`",
            .{name},
        );
    };

    self.pending_inserts = PendingInserts.init(allocator, &self.ctx) catch |e| {
        return cb_ctx.captureErrMsg(e, "failed to init pending inserts", .{});
    };
    errdefer self.pending_inserts.deinit();
    self.pending_inserts.table().create(cb_ctx.arena) catch |e| {
        return cb_ctx.captureErrMsg(
            e,
            "failed to create shadow table `{s}_pendinginserts`",
            .{name},
        );
    };

    self.row_group_creator = RowGroupCreator.init(
        allocator,
        &self.table_static_arena,
        &self.blob_manager,
        &self.ctx.schema,
        &self.row_group_index,
        &self.pending_inserts,
        10_000,
    ) catch |e| {
        return cb_ctx.captureErrMsg(e, "failed to init row group creator", .{});
    };

    self.next_rowid = null;
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
    // TODO wrap all of this in a savepoint

    if (args.len < 3) {
        cb_ctx.setErrorMessage("invalid arguments to vtab `connect`", .{});
        return InitError.NoColumns;
    }

    const db_name = args[1];
    if (!mem.eql(u8, "main", db_name)) {
        cb_ctx.setErrorMessage(
            "unable to create table in db `{s}`: only `main` db supported",
            .{db_name},
        );
        return InitError.UnsupportedDb;
    }

    self.allocator = allocator;
    self.table_static_arena = ArenaAllocator.init(allocator);
    errdefer self.table_static_arena.deinit();

    const name = try self.table_static_arena.allocator().dupe(u8, args[2]);
    self.ctx.base = VtabCtxSchemaless.init(conn, name);

    self.schema_manager = SchemaManager.init(&self.ctx.base);
    errdefer self.schema_manager.deinit();
    self.schema_manager.table().verifyExists(cb_ctx.arena) catch |e| {
        return cb_ctx.captureErrMsg(e, "`{s}_columns` shadow table does not exist", .{name});
    };

    self.ctx.schema = self.schema_manager.load(&self.table_static_arena, cb_ctx.arena) catch |e| {
        return cb_ctx.captureErrMsg(e, "failed to load schema", .{});
    };

    self.table_data = TableData.init(&self.ctx.base);
    errdefer self.table_data.deinit();
    self.table_data.table().verifyExists(cb_ctx.arena) catch |e| {
        return cb_ctx.captureErrMsg(e, "`{s}_tabledata` shadow table does not exist", .{name});
    };

    self.blob_manager = BlobManager.init(&self.table_static_arena, &self.ctx.base) catch |e| {
        return cb_ctx.captureErrMsg(e, "failed to init blob manager", .{});
    };
    errdefer self.blob_manager.deinit();
    self.blob_manager.table().verifyExists(cb_ctx.arena) catch |e| {
        return cb_ctx.captureErrMsg(e, "`{s}_blobs` shadow table does not exist", .{name});
    };

    self.row_group_index = RowGroupIndex.init(&self.ctx);
    errdefer self.row_group_index.deinit();
    self.row_group_index.table().verifyExists(cb_ctx.arena) catch |e| {
        return cb_ctx.captureErrMsg(e, "`{s}_rowgroupindex` shadow table does not exist", .{name});
    };

    self.pending_inserts = PendingInserts.init(allocator, &self.ctx) catch |e| {
        return cb_ctx.captureErrMsg(e, "failed to init pending inserts", .{});
    };
    errdefer self.pending_inserts.deinit();
    self.pending_inserts.table().verifyExists(cb_ctx.arena) catch |e| {
        return cb_ctx.captureErrMsg(
            e,
            "`{s}_pendinginserts` shadow table does not exist",
            .{name},
        );
    };

    self.row_group_creator = RowGroupCreator.init(
        allocator,
        &self.table_static_arena,
        &self.blob_manager,
        &self.ctx.schema,
        &self.row_group_index,
        &self.pending_inserts,
        10_000,
    ) catch |e| {
        return cb_ctx.captureErrMsg(e, "failed to init row group creator", .{});
    };

    self.next_rowid = null;
}

pub fn disconnect(self: *Self) void {
    log.debug("disconnecting from vtab {s}", .{self.ctx.vtabName()});

    self.row_group_creator.deinit();
    self.pending_inserts.deinit();
    self.row_group_index.deinit();
    self.table_data.deinit();
    self.blob_manager.deinit();
    self.schema_manager.deinit();

    self.table_static_arena.deinit();
}

pub fn destroy(self: *Self, cb_ctx: *vtab.CallbackContext) void {
    log.debug("destroying vtab {s}", .{self.ctx.vtabName()});

    self.pending_inserts.table().drop(cb_ctx.arena) catch |e| {
        log.err(
            "failed to drop shadow table {s}_pendinginserts: {!}",
            .{ self.ctx.vtabName(), e },
        );
    };
    self.row_group_index.table().drop(cb_ctx.arena) catch |e| {
        log.err(
            "failed to drop shadow table {s}_rowgroupindex: {!}",
            .{ self.ctx.vtabName(), e },
        );
    };
    self.blob_manager.table().drop(cb_ctx.arena) catch |e| {
        log.err(
            "failed to drop shadow table {s}_blobs: {!}",
            .{ self.ctx.vtabName(), e },
        );
    };
    self.table_data.table().drop(cb_ctx.arena) catch |e| {
        log.err(
            "failed to drop shadow table {s}_tabledata: {!}",
            .{ self.ctx.vtabName(), e },
        );
    };
    self.schema_manager.table().drop(cb_ctx.arena) catch |e| {
        log.err(
            "failed to drop shadow table {s}_columns: {!}",
            .{ self.ctx.vtabName(), e },
        );
    };

    self.disconnect();
}

pub fn ddl(self: *Self, allocator: Allocator) ![:0]const u8 {
    return fmt.allocPrintZ(
        allocator,
        "{}",
        .{self.ctx.schema.sqliteDdlFormatter(self.ctx.vtabName())},
    );
}

pub fn rename(self: *Self, cb_ctx: *vtab.CallbackContext, new_name: [:0]const u8) !void {
    log.debug("renaming to {s}", .{new_name});
    // TODO savepoint

    try self.table_data.table().rename(cb_ctx.arena, new_name);
    try self.pending_inserts.table().rename(cb_ctx.arena, new_name);
    try self.row_group_index.table().rename(cb_ctx.arena, new_name);
    try self.blob_manager.table().rename(cb_ctx.arena, new_name);
    try self.schema_manager.table().rename(cb_ctx.arena, new_name);

    // After a rename succeeds, the virtual table is disconnected, which means that other places
    // that the name is stored (like in `ctx` or `blob_manager`) do not need to be updated.
    return;
}

pub fn bestIndex(
    self: *Self,
    cb_ctx: *vtab.CallbackContext,
    best_index_info: vtab.BestIndexInfo,
) !bool {
    index.chooseBestIndex(cb_ctx.arena, self.ctx.sortKey(), best_index_info) catch |e| {
        return cb_ctx.captureErrMsg(e, "error occurred while choosing the best index", .{});
    };
    // There is always a query solution because a table scan always works
    return true;
}

pub fn update(
    self: *Self,
    cb_ctx: *vtab.CallbackContext,
    rowid: *i64,
    change_set: ChangeSet,
) !void {
    const change_type = change_set.changeType();
    if (change_type == .Insert) {
        if (self.next_rowid == null) {
            self.loadNextRowid(cb_ctx.arena) catch |e| {
                return cb_ctx.captureErrMsg(e, "error loading the next rowid", .{});
            };
        }
        rowid.* = self.next_rowid.?;
        self.next_rowid.? += 1;

        self.pending_inserts.insert(cb_ctx.arena, rowid.*, change_set) catch |e| {
            return cb_ctx.captureErrMsg(e, "error inserting into pending inserts", .{});
        };

        return;
    }

    if (!self.warned_update_delete_not_supported) {
        log.warn("stanchion tables do not (yet) support UPDATE or DELETE", .{});
        self.warned_update_delete_not_supported = true;
    }
}

pub fn begin(_: *Self, _: *vtab.CallbackContext) !void {
    log.debug("txn begin", .{});
}

pub fn sync(self: *Self, cb_ctx: *vtab.CallbackContext) !void {
    log.debug("txn sync", .{});
    if (self.next_rowid) |_| {
        self.row_group_creator.createAll(cb_ctx.arena) catch |e| {
            return cb_ctx.captureErrMsg(e, "failed to create row groups", .{});
        };
        self.unloadNextRowid(cb_ctx.arena) catch |e| {
            return cb_ctx.captureErrMsg(e, "failed to persist next rowid", .{});
        };
    }
}

pub fn commit(self: *Self, cb_ctx: *vtab.CallbackContext) !void {
    _ = cb_ctx;
    _ = self;
    log.debug("txn commit", .{});
}

pub fn rollback(self: *Self, _: *vtab.CallbackContext) !void {
    log.debug("txn rollback", .{});
    if (self.next_rowid) |_| {
        self.clearNextRowid();
    }
}

pub fn savepoint(_: *Self, _: *vtab.CallbackContext, savepoint_id: i32) !void {
    log.debug("txn savepoint {d} begin", .{savepoint_id});
}

pub fn release(self: *Self, cb_ctx: *vtab.CallbackContext, savepoint_id: i32) !void {
    log.debug("txn savepoint {d} release", .{savepoint_id});
    if (self.next_rowid) |_| {
        // TODO Is this necessary? Releasing the savepoint does not end the transaction so I don't
        //      think it is necessary to persist and clear the next rowid. Also, if sync is called
        //      immediately after, row groups will not be created
        try self.unloadNextRowid(cb_ctx.arena);
    }
}

pub fn rollbackTo(self: *Self, _: *vtab.CallbackContext, savepoint_id: i32) !void {
    log.debug("txn savepoint {d} rollback", .{savepoint_id});
    if (self.next_rowid) |_| {
        self.clearNextRowid();
    }
}

fn loadNextRowid(self: *Self, tmp_arena: *ArenaAllocator) !void {
    self.next_rowid = (try self.table_data.readInt(tmp_arena, .next_rowid)) orelse 1;
}

/// Persists the next rowid and removes it from memory
fn unloadNextRowid(self: *Self, tmp_arena: *ArenaAllocator) !void {
    try self.table_data.writeInt(tmp_arena, .next_rowid, self.next_rowid.?);
    self.next_rowid = null;
}

fn clearNextRowid(self: *Self) void {
    self.next_rowid = null;
}

pub fn isShadowName(suffix: [:0]const u8) bool {
    log.debug("checking shadow name: {s}", .{suffix});
    inline for (.{ TableData, BlobManager, SchemaManager, RowGroupIndex, PendingInserts }) |st| {
        if (mem.eql(u8, st.ShadowTable.suffix, suffix)) {
            return true;
        }
    }
    return false;
}

pub fn open(
    self: *Self,
    _: *vtab.CallbackContext,
) !Cursor {
    log.debug("open cursor", .{});
    return Cursor.init(
        self.allocator,
        &self.blob_manager,
        &self.ctx.schema,
        &self.row_group_index,
        &self.pending_inserts,
    );
}

pub const Cursor = struct {
    row_group_index: *RowGroupIndex,
    pend_inserts: *PendingInserts,

    rg_index_cursor: RowGroupIndex.EntriesCursor,
    rg_cursor: row_group.Cursor,
    pend_inserts_cursor: PendingInserts.Cursor,

    begun: bool,

    pub fn init(
        allocator: Allocator,
        blob_manager: *BlobManager,
        schema: *const Schema,
        row_group_index: *RowGroupIndex,
        pend_inserts: *PendingInserts,
    ) !Cursor {
        const rg_cursor = try row_group.Cursor.init(allocator, blob_manager, schema);
        return .{
            .row_group_index = row_group_index,
            .pend_inserts = pend_inserts,
            .rg_index_cursor = undefined,
            .rg_cursor = rg_cursor,
            .pend_inserts_cursor = undefined,
            .begun = false,
        };
    }

    pub fn deinit(self: *Cursor) void {
        self.rg_cursor.deinit();
        if (self.begun) {
            self.pend_inserts_cursor.deinit();
            self.rg_index_cursor.deinit();
        }
    }

    /// Code that indicates which index is being used. Currently only the sort key is supported.
    const IndexCode = enum(u8) {
        sort_key = 1,
    };

    pub fn begin(
        self: *Cursor,
        cb_ctx: *vtab.CallbackContext,
        index_id_num: i32,
        _: [:0]const u8,
        filter_args: vtab.FilterArgs,
    ) !void {
        const best_index = Index.deserialize(@bitCast(index_id_num), filter_args) catch |e| {
            return cb_ctx.captureErrMsg(e, "failed to deserialize index id", .{});
        };
        if (best_index) |idx| {
            switch (idx) {
                .sort_key => |sk_range| try self.beginSortKeyIndex(cb_ctx, sk_range),
            }
        } else {
            log.debug("cursor begin: doing table scan", .{});
            self.rg_index_cursor = self.row_group_index.cursor(cb_ctx.arena) catch |e| {
                return cb_ctx.captureErrMsg(e, "failed to open row group index cursor", .{});
            };
            self.pend_inserts_cursor = self.pend_inserts.cursor(cb_ctx.arena) catch |e| {
                return cb_ctx.captureErrMsg(e, "failed to open pending inserts cursor", .{});
            };
        }

        if (!self.rg_index_cursor.eof()) {
            self.rg_index_cursor.readEntry(self.rg_cursor.rowGroup());
        }

        self.begun = true;
    }

    fn beginSortKeyIndex(
        self: *Cursor,
        cb_ctx: *vtab.CallbackContext,
        sk_range: index.SortKeyRange,
    ) !void {
        log.debug("cursor begin: using sort key index: {}", .{sk_range});
        self.rg_index_cursor = self.row_group_index.cursorPartial(
            cb_ctx.arena,
            sk_range,
        ) catch |e| {
            return cb_ctx.captureErrMsg(e, "failed to open partial row group index cursor", .{});
        };
        self.pend_inserts_cursor = self.pend_inserts.cursorPartial(
            cb_ctx.arena,
            sk_range,
        ) catch |e| {
            return cb_ctx.captureErrMsg(e, "failed to open partial pending inserts cursor", .{});
        };
    }

    pub fn eof(self: *Cursor) bool {
        return self.rg_index_cursor.eof() and self.pend_inserts_cursor.eof();
    }

    pub fn next(self: *Cursor) !void {
        if (!self.rg_cursor.eof()) {
            try self.rg_cursor.next();
            if (!self.rg_cursor.eof()) {
                return;
            }
        }

        if (!self.rg_index_cursor.eof()) {
            try self.rg_index_cursor.next();
            if (!self.rg_index_cursor.eof()) {
                self.rg_cursor.reset();
                // Read the row group into the rg_cursor
                self.rg_index_cursor.readEntry(self.rg_cursor.rowGroup());
            }
            return;
        }

        try self.pend_inserts_cursor.next();
    }

    pub fn rowid(self: *Cursor) !i64 {
        if (self.rg_index_cursor.eof()) {
            const value = try self.pend_inserts_cursor.readRowid();
            return value.asI64();
        }

        const value = try self.rg_cursor.readRowid();
        return value.asI64();
    }

    pub fn column(self: *Cursor, result: Result, col_idx: usize) !void {
        if (self.rg_index_cursor.eof()) {
            const value = try self.pend_inserts_cursor.readValue(col_idx);
            result.setSqliteValue(value);
            return;
        }

        try self.rg_cursor.readInto(result, col_idx);
    }
};
