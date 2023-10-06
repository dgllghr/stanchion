const std = @import("std");
const fmt = std.fmt;
const mem = std.mem;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;
const Type = std.builtin.Type;

const ChangeSet = @import("sqlite3/ChangeSet.zig");
const Conn = @import("sqlite3/Conn.zig");
const Stmt = @import("sqlite3/Stmt.zig");
const vtab = @import("sqlite3/vtab.zig");
const sqlite_c = @import("sqlite3/c.zig").c;

const DbError = @import("db.zig").Error;
const Migrations = @import("db.zig").Migrations;

const schema = @import("schema.zig");
const SchemaDef = schema.SchemaDef;
const Schema = schema.Schema;

const segment = @import("segment.zig");

const PrimaryIndex = @import("PrimaryIndex.zig");
//const row_group = @import("row_group.zig");

const Self = @This();

allocator: Allocator,
table_static_arena: ArenaAllocator,
db: struct {
    schema: schema.Db,
    table: Db,
    segment: segment.Db,
},
id: i64,
name: []const u8,
schema: Schema,
primary_index: PrimaryIndex,

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
        .schema = schema.Db.init(conn),
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
        .schema = schema.Db.init(conn),
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
        rowid.* = self.primary_index.insertInsertEntry(change_set) catch |err| {
            cb_ctx.setErrorMessage("failed to log insert", .{});
            return err;
        };

        // Count the number of pending inserts for a row group and merge them if the
        // count is above a threshold
        // TODO do this at end of transaction (requires tracking which row groups got
        //      inserts)

        var handle = try self.primary_index.precedingRowGroup(rowid.*, change_set);
        defer handle.deinit();

        var iter = try self.primary_index.stagedInserts(&handle);
        var count: u32 = 0;
        while (try iter.next()) {
            count += 1;
        }

        if (count > 10_000) {
            // TODO create a new row group for the staged inserts
            // try iter.restart();
            // try row_group.create(
            //     self.allocator,
            //     &self.schema,
            //     &self.db.segment,
            //     &self.primary_index,
            //     &iter,
            // );
        }

        return;
    }

    @panic("todo");
}

// pub fn bestIndex(self: *Self) !void {
// }

// pub fn open(self: *Self) !Cursor {
// }

const Db = struct {
    conn: Conn,
    create_table: ?Stmt,
    load_table: ?Stmt,

    pub fn init(conn: Conn) Db {
        return .{
            .conn = conn,
            .create_table = null,
            .load_table = null,
        };
    }

    pub fn deinit(self: *Db) void {
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
                \\INSERT INTO _stanchion_tables (name)
                \\VALUES (?)
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
};
