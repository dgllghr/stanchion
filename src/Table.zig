const std = @import("std");
const fmt = std.fmt;
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

// TODO remove this
allocator: Allocator,
table_static_arena: ArenaAllocator,
name: []const u8,
schema: Schema,
table_data: TableData,
blob_manager: BlobManager,
row_group_index: RowGroupIndex,
pending_inserts: PendingInserts,
row_group_creator: RowGroupCreator,
dirty: bool,

pub const InitError = error{
    NoColumns,
    UnsupportedDb,
} || SchemaDef.ParseError || SchemaManager.Error || SqliteErorr || mem.Allocator.Error;

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
            "only 'main' db currently supported, got {s}",
            .{db_name},
        );
        return InitError.UnsupportedDb;
    }

    // Use the tmp arena because the schema def is not stored with the table. The data is converted
    // into a Schema and the Schema is stored.
    const def = SchemaDef.parse(cb_ctx.arena, args[3..]) catch |e| {
        cb_ctx.setErrorMessage("error parsing schema definition: {any}", .{e});
        return e;
    };

    self.allocator = allocator;
    self.table_static_arena = ArenaAllocator.init(allocator);
    errdefer self.table_static_arena.deinit();

    self.name = try self.table_static_arena.allocator().dupe(u8, args[2]);

    var schema_mgr = try SchemaManager.init(cb_ctx.arena, conn, self.name);
    defer schema_mgr.deinit();

    self.schema = schema_mgr.create(&self.table_static_arena, cb_ctx.arena, &def) catch |e| {
        cb_ctx.setErrorMessage("error creating schema: {any}", .{e});
        return e;
    };

    self.table_data = TableData.create(cb_ctx.arena, conn, self.name) catch |e| {
        cb_ctx.setErrorMessage("error creating table data: {any}", .{e});
        return e;
    };
    errdefer self.table_data.deinit();

    self.blob_manager = BlobManager.init(
        &self.table_static_arena,
        cb_ctx.arena,
        conn,
        self.name,
    ) catch |e| {
        cb_ctx.setErrorMessage("error creating blob manager: {any}", .{e});
        return e;
    };
    errdefer self.blob_manager.deinit();

    self.row_group_index = RowGroupIndex.create(
        cb_ctx.arena,
        conn,
        self.name,
        &self.schema,
    ) catch |e| {
        cb_ctx.setErrorMessage("error creating row group index: {any}", .{e});
        return e;
    };
    errdefer self.row_group_index.deinit();

    self.pending_inserts = PendingInserts.create(
        allocator,
        cb_ctx.arena,
        conn,
        self.name,
        &self.schema,
        &self.table_data,
    ) catch |e| {
        cb_ctx.setErrorMessage("error creating pending inserts: {any}", .{e});
        return e;
    };
    errdefer self.pending_inserts.deinit();

    self.row_group_creator = RowGroupCreator.init(
        allocator,
        &self.table_static_arena,
        &self.blob_manager,
        &self.schema,
        &self.row_group_index,
        &self.pending_inserts,
        10_000,
    ) catch |e| {
        cb_ctx.setErrorMessage("error creating row group creator: {any}", .{e});
        return e;
    };

    self.dirty = false;
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
        cb_ctx.setErrorMessage("only 'main' db currently supported, got {s}", .{db_name});
        return InitError.UnsupportedDb;
    }

    self.allocator = allocator;
    self.table_static_arena = ArenaAllocator.init(allocator);
    errdefer self.table_static_arena.deinit();

    self.name = try self.table_static_arena.allocator().dupe(u8, args[2]);

    var schema_mgr = try SchemaManager.init(cb_ctx.arena, conn, self.name);
    defer schema_mgr.deinit();

    self.schema = schema_mgr.load(&self.table_static_arena, cb_ctx.arena) catch |e| {
        cb_ctx.setErrorMessage("error loading schema: {any}", .{e});
        return e;
    };

    self.table_data = TableData.open(conn, self.name) catch |e| {
        cb_ctx.setErrorMessage("error opening table data: {any}", .{e});
        return e;
    };
    errdefer self.table_data.deinit();

    self.blob_manager = BlobManager.init(
        &self.table_static_arena,
        cb_ctx.arena,
        conn,
        self.name,
    ) catch |e| {
        cb_ctx.setErrorMessage("error creating blob manager: {any}", .{e});
        return e;
    };
    errdefer self.blob_manager.deinit();

    self.row_group_index = RowGroupIndex.open(conn, self.name, &self.schema);
    errdefer self.row_group_index.deinit();

    self.pending_inserts = PendingInserts.open(
        allocator,
        cb_ctx.arena,
        conn,
        self.name,
        &self.schema,
        &self.table_data,
    ) catch |e| {
        cb_ctx.setErrorMessage("error opening pending inserts: {any}", .{e});
        return e;
    };
    errdefer self.pending_inserts.deinit();

    self.row_group_creator = RowGroupCreator.init(
        allocator,
        &self.table_static_arena,
        &self.blob_manager,
        &self.schema,
        &self.row_group_index,
        &self.pending_inserts,
        10_000,
    ) catch |e| {
        cb_ctx.setErrorMessage("error creating row group creator: {any}", .{e});
        return e;
    };

    self.dirty = false;
}

pub fn disconnect(self: *Self) void {
    self.row_group_creator.deinit();
    self.pending_inserts.deinit();
    self.row_group_index.deinit();
    self.table_data.deinit();
    self.blob_manager.deinit();

    self.table_static_arena.deinit();
}

pub fn destroy(self: *Self, cb_ctx: *vtab.CallbackContext) void {
    self.table_data.drop(cb_ctx.arena) catch |e| {
        std.log.err("failed to drop shadow table {s}_tabledata: {any}", .{ self.name, e });
    };
    self.pending_inserts.drop(cb_ctx.arena) catch |e| {
        std.log.err("failed to drop shadow table {s}_pendinginserts: {any}", .{ self.name, e });
    };
    self.row_group_index.drop(cb_ctx.arena) catch |e| {
        std.log.err("failed to drop shadow table {s}_rowgroupindex: {any}", .{ self.name, e });
    };
    self.blob_manager.destroy(cb_ctx.arena) catch |e| {
        std.log.err("failed to drop shadow table {s}_blobs: {any}", .{ self.name, e });
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
        rowid.* = self.pending_inserts.insert(cb_ctx.arena, change_set) catch |e| {
            cb_ctx.setErrorMessage("failed insert insert entry: {any}", .{e});
            return e;
        };

        self.dirty = true;

        return;
    }

    @panic("delete and update are not supported");
}

pub fn bestIndex(
    self: *Self,
    cb_ctx: *vtab.CallbackContext,
    best_index_info: vtab.BestIndexInfo,
) !void {
    try index.chooseBestIndex(cb_ctx.arena, self.schema.sort_key, best_index_info);
}

pub fn begin(_: *Self, _: *vtab.CallbackContext) !void {
    std.log.debug("txn begin", .{});
}

pub fn sync(self: *Self, cb_ctx: *vtab.CallbackContext) !void {
    _ = cb_ctx;
    _ = self;
}

pub fn commit(self: *Self, cb_ctx: *vtab.CallbackContext) !void {
    std.log.debug("txn commit", .{});
    if (self.dirty) {
        try self.pending_inserts.persistNextRowid(cb_ctx.arena);
        // TODO should this be called in sync so that an error causes the transaction to be
        //      aborted?
        try self.row_group_creator.createAll(cb_ctx.arena);
        self.dirty = false;
    }
}

pub fn rollback(self: *Self, cb_ctx: *vtab.CallbackContext) !void {
    std.log.debug("txn rollback", .{});
    if (self.dirty) {
        try self.pending_inserts.loadNextRowid(cb_ctx.arena);
    }
}

pub fn savepoint(_: *Self, _: *vtab.CallbackContext, savepoint_id: i32) !void {
    std.log.debug("txn savepoint {d} begin", .{savepoint_id});
}

pub fn release(self: *Self, cb_ctx: *vtab.CallbackContext, savepoint_id: i32) !void {
    std.log.debug("txn savepoint {d} release", .{savepoint_id});
    if (self.dirty) {
        try self.pending_inserts.persistNextRowid(cb_ctx.arena);
    }
}

pub fn rollbackTo(self: *Self, cb_ctx: *vtab.CallbackContext, savepoint_id: i32) !void {
    std.log.debug("txn savepoint {d} rollback", .{savepoint_id});
    if (self.dirty) {
        try self.pending_inserts.loadNextRowid(cb_ctx.arena);
    }
}

const shadowNames = [_][:0]const u8{
    "tabledata",
    "blobs",
    "columns",
    "rowgroupindex",
    "pendinginserts",
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
        &self.blob_manager,
        &self.schema,
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
        const best_index = try Index.deserialize(@bitCast(index_id_num), filter_args);
        if (best_index) |idx| {
            switch (idx) {
                .sort_key => |sk_range| {
                    std.log.debug("cursor begin: using sort key index: {}", .{sk_range});
                    self.rg_index_cursor = try self.row_group_index.cursorPartial(
                        cb_ctx.arena,
                        sk_range,
                    );
                    self.pend_inserts_cursor = try self.pend_inserts.cursorPartial(
                        cb_ctx.arena,
                        sk_range,
                    );
                },
            }
        } else {
            std.log.debug("cursor begin: doing table scan", .{});
            self.rg_index_cursor = try self.row_group_index.cursor(cb_ctx.arena);
            self.pend_inserts_cursor = try self.pend_inserts.cursor(cb_ctx.arena);
        }

        if (!self.rg_index_cursor.eof()) {
            self.rg_index_cursor.readEntry(self.rg_cursor.rowGroup());
        }

        self.begun = true;
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
