const std = @import("std");
const fmt = std.fmt;
const mem = std.mem;
const Allocator = std.mem.Allocator;
const Type = std.builtin.Type;

const Conn = @import("./sqlite3/Conn.zig");
const Stmt = @import("./sqlite3/Stmt.zig");
const vtab = @import("./sqlite3/vtab.zig");
const sqlite_c = @import("./sqlite3/c.zig").c;

const DbError = @import("./db.zig").Error;
const Migrations = @import("./db.zig").Migrations;

const schema = @import("./schema.zig");
const SchemaDef = schema.SchemaDef;
const Schema = schema.Schema;

const ChangeSet = @import("./value/ChangeSet.zig");

const RowGroups = @import("./row_group.zig").RowGroups;

const Self = @This();

db: struct {
    schema: schema.Db,
    table: Db,
},
allocator: Allocator,
id: i64,
name: []const u8,
schema: Schema,
row_groups: RowGroups,

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
        cb_ctx.setErrorMessage("only 'main' db currently supported, got {s}", .{db_name});
        return InitError.UnsupportedDb;
    }

    const name = try allocator.dupe(u8, args[2]);
    errdefer allocator.free(name);

    // Use the arena allocator because the schema def is not stored. The data is
    // converted into a Schema when the schema is created.
    const def = SchemaDef.parse(cb_ctx.arena, args[3..]) catch |e| {
        cb_ctx.setErrorMessage("error parsing schema definition: {any}", .{e});
        return e;
    };

    var db = .{
        .table = Db.init(conn),
        .schema = schema.Db.init(conn),
    };

    const table_id = try db.table.createTable(name);
    var s = try Schema.create(allocator, &db.schema, table_id, def);

    const row_groups = try RowGroups.create(cb_ctx.arena, conn, name, &s);

    return .{
        .allocator = allocator,
        .db = db,
        .id = table_id,
        .name = name,
        .schema = s,
        .row_groups = row_groups,
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

    const name = try allocator.dupe(u8, args[2]);
    errdefer allocator.free(name);

    var db = .{
        .table = Db.init(conn),
        .schema = schema.Db.init(conn),
    };

    const table_id = try db.table.loadTable(name);
    const s = try Schema.load(allocator, &db.schema, table_id);

    const row_groups = try RowGroups.open(cb_ctx.arena, conn, name, &s);

    return .{
        .allocator = allocator,
        .db = db,
        .id = table_id,
        .name = name,
        .schema = s,
        .row_groups = row_groups,
    };
}

pub fn disconnect(self: *Self) void {
    self.allocator.free(self.name);
    self.schema.deinit(self.allocator);
    self.db.table.deinit();
    self.db.schema.deinit();
}

pub fn destroy(self: *Self) void {
    // TODO delete all data
    self.allocator.free(self.name);
    self.schema.deinit(self.allocator);
    self.db.table.deinit();
    self.db.schema.deinit();
}

pub fn ddl(_: *Self, allocator: Allocator) ![:0]const u8 {
    return fmt.allocPrintZ(allocator, "CREATE TABLE x (foo INTEGER)", .{});
}

pub fn update(_: *Self, _: *vtab.CallbackContext, _: ?*i64, values: []?*sqlite_c.sqlite3_value) !void {
    _ = ChangeSet.init(values);
}

// pub fn bestIndex(self: *Self) !void {
// }

// pub fn open(self: *Self) !Cursor {
// }

// pub fn update(self: *Self) !i64 {
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

    pub fn createTable(self: *Db, name: []const u8) !i64 {
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
