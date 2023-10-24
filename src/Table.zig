const std = @import("std");
const fmt = std.fmt;
const mem = std.mem;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const ChangeSet = @import("sqlite3/ChangeSet.zig");
const Conn = @import("sqlite3/Conn.zig");
const Stmt = @import("sqlite3/Stmt.zig");
const vtab = @import("sqlite3/vtab.zig");
const sqlite_c = @import("sqlite3/c.zig").c;
const Result = vtab.Result;

const StmtCell = @import("StmtCell.zig");

const DbError = @import("db.zig").Error;
const Migrations = @import("db.zig").Migrations;

const schema_mod = @import("schema.zig");
const SchemaDb = schema_mod.Db;
const SchemaDef = schema_mod.SchemaDef;
const Schema = schema_mod.Schema;

const segment = @import("segment.zig");

const PrimaryIndex = @import("PrimaryIndex.zig");
const row_group = @import("row_group.zig");

const Self = @This();

// TODO remove this
allocator: Allocator,
table_static_arena: ArenaAllocator,
db: struct {
    schema: SchemaDb,
    table: Db,
    segment: segment.Db,
},
id: i64,
name: []const u8,
schema: Schema,
primary_index: PrimaryIndex,
last_write_rowid: i64,

pub const InitError = error{
    NoColumns,
    UnsupportedDb,
} || SchemaDef.ParseError || Schema.Error || DbError || mem.Allocator.Error;

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

    // Use the arena allocator because the schema def is not stored with the table. The
    // data is converted into a Schema and the Schema is stored.
    const def = SchemaDef.parse(cb_ctx.arena, args[3..]) catch |e| {
        cb_ctx.setErrorMessage("error parsing schema definition: {any}", .{e});
        return e;
    };

    var db = .{
        .table = Db.init(conn),
        .schema = SchemaDb.init(conn),
        .segment = segment.Db.init(conn),
    };

    const table_id = db.table.insertTable(name) catch |e| {
        cb_ctx.setErrorMessage("error inserting table: {any}", .{e});
        return e;
    };

    var s = Schema.create(
        allocator,
        &db.schema,
        table_id,
        def,
    ) catch |e| {
        cb_ctx.setErrorMessage("error creating schema: {any}", .{e});
        return e;
    };

    const primary_index = PrimaryIndex.create(
        &table_static_arena,
        cb_ctx.arena,
        conn,
        name,
        &s,
    ) catch |e| {
        cb_ctx.setErrorMessage("error creating primary index: {any}", .{e});
        return e;
    };

    return .{
        .allocator = allocator,
        .table_static_arena = table_static_arena,
        .db = db,
        .id = table_id,
        .name = name,
        .schema = s,
        .primary_index = primary_index,
        .last_write_rowid = 1,
    };
}

test "create table" {
    const conn = try Conn.openInMemory();
    defer conn.close();
    try Migrations.apply(conn);

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var cb_ctx = vtab.CallbackContext{
        .arena = &arena,
    };

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
    defer table.destroy();
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
        .table = Db.init(conn),
        .schema = SchemaDb.init(conn),
        .segment = segment.Db.init(conn),
    };

    const table_id = db.table.loadTable(name) catch |e| {
        cb_ctx.setErrorMessage("error loading table: {any}", .{e});
        return e;
    };

    const s = Schema.load(
        allocator,
        &db.schema,
        table_id,
    ) catch |e| {
        cb_ctx.setErrorMessage("error loading schema: {any}", .{e});
        return e;
    };

    const primary_index = PrimaryIndex.open(
        &table_static_arena,
        conn,
        name,
        &s,
        1,
    ) catch |e| {
        cb_ctx.setErrorMessage("error opening primary index: {any}", .{e});
        return e;
    };

    return .{
        .allocator = allocator,
        .table_static_arena = table_static_arena,
        .db = db,
        .id = table_id,
        .name = name,
        .schema = s,
        .primary_index = primary_index,
        .last_write_rowid = try db.table.readNextRowid(table_id),
    };
}

pub fn disconnect(self: *Self) void {
    self.primary_index.deinit();
    self.db.segment.deinit();
    self.db.schema.deinit();
    self.db.table.deinit();

    self.schema.deinit(self.allocator);
    self.table_static_arena.deinit();
}

pub fn destroy(self: *Self) void {
    // TODO delete all data
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
        rowid.* = self.primary_index.insertInsertEntry(change_set) catch |e| {
            cb_ctx.setErrorMessage("failed insert insert entry: {any}", .{e});
            return e;
        };

        // Count the number of pending inserts for a row group and merge them if the
        // count is above a threshold
        // TODO do this at end of transaction (requires tracking which row groups got
        //      inserts)

        var handle = try self.primary_index.precedingRowGroup(rowid.*, change_set);
        defer handle.deinit();

        var iter = try self.primary_index.stagedInserts(&handle);
        defer iter.deinit();

        var count: u32 = 0;
        while (try iter.next()) {
            count += 1;
        }

        // TODO make this threshold configurable
        if (count > 10_000) {
            try iter.restart();
            try row_group.create(
                self.allocator,
                &self.schema,
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

pub fn open(self: *Self) !Cursor {
    return Cursor.init(
        self.allocator,
        &self.db.segment,
        &self.schema,
        &self.primary_index,
    );
}

pub fn begin(_: *Self) !void {
    std.log.debug("txn begin", .{});
}

pub fn commit(self: *Self) !void {
    std.log.debug("txn commit", .{});
    try self.flushNextRowid();
}

pub fn rollback(self: *Self) !void {
    std.log.debug("txn rollback", .{});
    try self.rollbackNextRowid();
}

pub fn savepoint(self: *Self, savepoint_id: i32) !void {
    std.log.debug("txn savepoint {d}", .{savepoint_id});
    try self.flushNextRowid();
}

pub fn release(self: *Self, savepoint_id: i32) !void {
    std.log.debug("txn release {d}", .{savepoint_id});
    try self.rollbackNextRowid();
}

pub fn rollbackTo(self: *Self, savepoint_id: i32) !void {
    std.log.debug("txn rollback to {d}", .{savepoint_id});
    try self.rollbackNextRowid();
}

fn flushNextRowid(self: *Self) !void {
    if (self.last_write_rowid != self.primary_index.next_rowid) {
        try self.db.table.updateRowid(self.id, self.primary_index.next_rowid);
        self.last_write_rowid = self.primary_index.next_rowid;
    }
}

fn rollbackNextRowid(self: *Self) !void {
    const rowid = try self.db.table.readNextRowid(self.id);
    self.primary_index.next_rowid = rowid;
    self.last_write_rowid = rowid;
}

pub const Cursor = struct {
    pidx_cursor: PrimaryIndex.Cursor,
    rg_cursor: row_group.Cursor,
    in_row_group: bool,
    eof: bool,

    pub fn init(
        allocator: Allocator,
        segment_db: *segment.Db,
        schema: *const Schema,
        primary_index: *PrimaryIndex,
    ) !Cursor {
        var pidx_cursor = try primary_index.cursor();
        errdefer pidx_cursor.deinit();
        const rg_cursor = try row_group.Cursor.init(allocator, segment_db, schema);
        return .{
            .pidx_cursor = pidx_cursor,
            .rg_cursor = rg_cursor,
            .in_row_group = false,
            .eof = false,
        };
    }

    pub fn deinit(self: *Cursor) void {
        self.rg_cursor.deinit();
        self.pidx_cursor.deinit();
    }

    pub fn begin(self: *Cursor) !void {
        try self.next();
    }

    pub fn next(self: *Cursor) !void {
        if (self.in_row_group) {
            const has_next = try self.rg_cursor.next();
            if (has_next) {
                return;
            }
            self.in_row_group = false;
        }

        const has_next = try self.pidx_cursor.next();
        if (has_next) {
            if (self.pidx_cursor.entryType() == .RowGroup) {
                self.rg_cursor.reset();
                // Read the row group into the rg_cursor
                try self.pidx_cursor.readRowGroupEntry(self.rg_cursor.rowGroup());
                self.in_row_group = true;
            }
            return;
        }
        self.eof = true;
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
            const value = try self.rg_cursor.read(col_idx);
            if (value.isNull()) {
                result.setNull();
                return;
            }
            switch (value.data_type) {
                .Boolean => result.setBool(value.asBool()),
                .Integer => result.setI64(value.asI64()),
                .Float => @panic("todo"),
                .Blob => result.setBlob(value.asBlob()),
                .Text => result.setText(value.asText()),
            }
            return;
        }

        const value = self.pidx_cursor.readColumnValue(col_idx);
        result.setSqliteValue(value);
    }
};

const Db = struct {
    conn: Conn,
    create_table: ?Stmt,
    load_table: ?Stmt,
    read_next_rowid: StmtCell,
    update_next_rowid: StmtCell,

    pub fn init(conn: Conn) Db {
        return .{
            .conn = conn,
            .create_table = null,
            .load_table = null,
            .read_next_rowid = StmtCell.init(
                \\SELECT next_rowid FROM _stanchion_tables WHERE id = ?
            ),
            .update_next_rowid = StmtCell.init(
                \\UPDATE _stanchion_tables SET next_rowid = ? WHERE id = ?
            ),
        };
    }

    pub fn deinit(self: *Db) void {
        self.update_next_rowid.deinit();
        self.read_next_rowid.deinit();
        if (self.create_table) |stmt| {
            stmt.deinit();
        }
        if (self.load_table) |stmt| {
            stmt.deinit();
        }
    }

    pub fn insertTable(self: *Db, name: []const u8) !i64 {
        if (self.create_table == null) {
            self.create_table = try self.conn.prepare(
                \\INSERT INTO _stanchion_tables (name, next_rowid)
                \\VALUES (?, 1)
                \\RETURNING id
            );
        }

        const stmt = self.create_table.?;
        try stmt.bind(.Text, 1, name);
        if (!try stmt.next()) {
            return DbError.QueryReturnedNoRows;
        }
        const table_id = stmt.read(.Int64, false, 0);
        try stmt.reset();
        return table_id;
    }

    pub fn loadTable(self: *Db, name: []const u8) !i64 {
        if (self.load_table == null) {
            self.load_table = try self.conn.prepare(
                \\SELECT id FROM _stanchion_tables WHERE name = ?
            );
        }

        const stmt = self.load_table.?;
        try stmt.bind(.Text, 1, name);
        if (!try stmt.next()) {
            return DbError.QueryReturnedNoRows;
        }
        const table_id = stmt.read(.Int64, false, 0);
        try stmt.reset();
        return table_id;
    }

    pub fn readNextRowid(self: *Db, table_id: i64) !i64 {
        const stmt = try self.read_next_rowid.getStmt(self.conn);
        defer self.read_next_rowid.reset();

        try stmt.bind(.Int64, 1, table_id);

        if (!try stmt.next()) {
            return DbError.QueryReturnedNoRows;
        }
        return stmt.read(.Int64, false, 0);
    }

    pub fn updateRowid(self: *Db, table_id: i64, rowid: i64) !void {
        const stmt = try self.update_next_rowid.getStmt(self.conn);
        defer self.update_next_rowid.reset();

        try stmt.bind(.Int64, 1, rowid);
        try stmt.bind(.Int64, 2, table_id);

        try stmt.exec();
    }
};
