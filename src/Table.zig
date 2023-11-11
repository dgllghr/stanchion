const std = @import("std");
const fmt = std.fmt;
const mem = std.mem;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const ChangeSet = @import("sqlite3/ChangeSet.zig");
const Conn = @import("sqlite3/Conn.zig");
const SqliteErorr = @import("sqlite3/errors.zig").Error;
const Stmt = @import("sqlite3/Stmt.zig");
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
    allocator: Allocator,
    conn: Conn,
    cb_ctx: *vtab.CallbackContext,
    args: []const []const u8,
) InitError!Self {
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

    var table_static_arena = ArenaAllocator.init(allocator);
    errdefer table_static_arena.deinit();

    const name = try table_static_arena.allocator().dupe(u8, args[2]);

    // Use the tmp arena because the schema def is not stored with the table. The
    // data is converted into a Schema and the Schema is stored.
    const def = SchemaDef.parse(&cb_ctx.arena, args[3..]) catch |e| {
        cb_ctx.setErrorMessage("error parsing schema definition: {any}", .{e});
        return e;
    };

    try schema_mod.Db.createTable(&cb_ctx.arena, conn, name);
    try segment.Db.createTable(&cb_ctx.arena, conn, name);
    var db = .{
        .schema = schema_mod.Db.init(conn, name),
        .segment = try segment.Db.init(&table_static_arena, conn, name),
    };

    var schema = Schema.create(
        table_static_arena.allocator(),
        &cb_ctx.arena,
        &db.schema,
        def,
    ) catch |e| {
        cb_ctx.setErrorMessage("error creating schema: {any}", .{e});
        return e;
    };
    errdefer schema.deinit(table_static_arena.allocator());

    var primary_index = PrimaryIndex.create(
        &cb_ctx.arena,
        conn,
        name,
        &schema,
    ) catch |e| {
        cb_ctx.setErrorMessage("error creating primary index: {any}", .{e});
        return e;
    };
    errdefer primary_index.deinit();

    const row_group_creator = RowGroupCreator.init(
        &table_static_arena,
        &schema,
    ) catch |e| {
        cb_ctx.setErrorMessage("error creating row group creator: {any}", .{e});
        return e;
    };

    return .{
        .allocator = allocator,
        .table_static_arena = table_static_arena,
        .db = db,
        .name = name,
        .schema = schema,
        .primary_index = primary_index,
        .row_group_creator = row_group_creator,
    };
}

test "create table" {
    const conn = try Conn.openInMemory();
    defer conn.close();

    var cb_ctx = vtab.CallbackContext.init(std.testing.allocator);
    defer cb_ctx.deinit();

    var table = try create(
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
    allocator: Allocator,
    conn: Conn,
    cb_ctx: *vtab.CallbackContext,
    args: []const []const u8,
) !Self {
    if (args.len < 3) {
        cb_ctx.setErrorMessage("invalid arguments to vtab `connect`", .{});
        return InitError.NoColumns;
    }

    const db_name = args[1];
    if (!mem.eql(u8, "main", db_name)) {
        cb_ctx.setErrorMessage("only 'main' db currently supported, got {s}", .{db_name});
        return InitError.UnsupportedDb;
    }

    var table_static_arena = ArenaAllocator.init(allocator);
    errdefer table_static_arena.deinit();

    const name = try table_static_arena.allocator().dupe(u8, args[2]);

    var db = .{
        .schema = schema_mod.Db.init(conn, name),
        .segment = try segment.Db.init(&table_static_arena, conn, name),
    };

    var schema = Schema.load(
        table_static_arena.allocator(),
        &cb_ctx.arena,
        &db.schema,
    ) catch |e| {
        cb_ctx.setErrorMessage("error loading schema: {any}", .{e});
        return e;
    };
    errdefer schema.deinit(table_static_arena.allocator());

    var primary_index = PrimaryIndex.open(
        &cb_ctx.arena,
        conn,
        name,
        &schema,
    ) catch |e| {
        cb_ctx.setErrorMessage("error opening primary index: {any}", .{e});
        return e;
    };
    errdefer primary_index.deinit();

    const row_group_creator = RowGroupCreator.init(
        &table_static_arena,
        &schema,
    ) catch |e| {
        cb_ctx.setErrorMessage("error creating row group creator: {any}", .{e});
        return e;
    };

    return .{
        .allocator = allocator,
        .table_static_arena = table_static_arena,
        .db = db,
        .name = name,
        .schema = schema,
        .primary_index = primary_index,
        .row_group_creator = row_group_creator,
    };
}

pub fn disconnect(self: *Self) void {
    self.primary_index.deinit();
    self.db.segment.deinit();
    self.db.schema.deinit();

    self.table_static_arena.deinit();
}

pub fn destroy(self: *Self, cb_ctx: *vtab.CallbackContext) void {
    self.db.schema.dropTable(&cb_ctx.arena) catch |e| {
        std.log.err("failed to drop shadow table {s}_columns: {any}", .{ self.name, e });
    };
    self.db.segment.dropTable(&cb_ctx.arena) catch |e| {
        std.log.err("failed to drop shadow table {s}_segments: {any}", .{ self.name, e });
    };
    self.primary_index.drop(&cb_ctx.arena) catch |e| {
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
        rowid.* = self.primary_index.insertInsertEntry(
            &cb_ctx.arena,
            change_set,
        ) catch |e| {
            cb_ctx.setErrorMessage("failed insert insert entry: {any}", .{e});
            return e;
        };

        // Count the number of pending inserts for a row group and merge them if the
        // count is above a threshold
        // TODO do this at end of transaction (requires tracking which row groups got
        //      inserts)

        var handle = try self.primary_index.precedingRowGroup(
            &cb_ctx.arena,
            rowid.*,
            change_set,
        );
        defer handle.deinit();

        var iter = try self.primary_index.stagedInserts(&cb_ctx.arena, &handle);
        defer iter.deinit();

        var count: u32 = 0;
        while (try iter.next()) {
            count += 1;
        }

        // TODO make this threshold configurable
        if (count > 10_000) {
            try iter.restart();

            defer self.row_group_creator.reset();
            try self.row_group_creator.create(
                &cb_ctx.arena,
                &self.db.segment,
                &self.primary_index,
                &iter,
            );
        }

        return;
    }

    @panic("delete and update are not supported");
}

pub fn bestIndex(
    _: *Self,
    _: *vtab.CallbackContext,
    _: *vtab.BestIndexBuilder,
) !void {
    // TODO could `estimatedCost` be as simple as the number of row groups accessed? or
    //      how about number of rows (row groups * record count) * number of columns? is
    //      columns accessed in the query even available here? actually `estimatedRows`
    //      is where the row count should be supplied
    // for (best_index.constraints) |*c| {
    //     std.log.debug("constraint on column {d}, usable: {any}", .{c.column, c.usable},);
    // }
}

pub fn open(
    self: *Self,
    cb_ctx: *vtab.CallbackContext,
) !Cursor {
    return Cursor.init(
        self.allocator,
        &cb_ctx.arena,
        &self.db.segment,
        &self.schema,
        &self.primary_index,
    );
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

pub const Cursor = struct {
    pidx_cursor: PrimaryIndex.Cursor,
    rg_cursor: row_group.Cursor,
    in_row_group: bool,

    pub fn init(
        allocator: Allocator,
        tmp_arena: *ArenaAllocator,
        segment_db: *segment.Db,
        schema: *const Schema,
        primary_index: *PrimaryIndex,
    ) !Cursor {
        var pidx_cursor = try primary_index.cursor(tmp_arena);
        errdefer pidx_cursor.deinit();
        const rg_cursor = try row_group.Cursor.init(allocator, segment_db, schema);
        return .{
            .pidx_cursor = pidx_cursor,
            .rg_cursor = rg_cursor,
            .in_row_group = false,
        };
    }

    pub fn deinit(self: *Cursor) void {
        self.rg_cursor.deinit();
        self.pidx_cursor.deinit();
    }

    pub fn begin(self: *Cursor) !void {
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
            return self.rg_cursor.readRowid();
        }
        return self.pidx_cursor.readRowid();
    }

    pub fn column(self: *Cursor, result: Result, col_idx: usize) !void {
        // TODO make this more efficient by passing the result into the cursors
        if (self.in_row_group) {
            try self.rg_cursor.readInto(result, col_idx);
            return;
        }

        const value = self.pidx_cursor.read(col_idx);
        result.setSqliteValue(value);
    }
};
