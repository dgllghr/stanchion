const std = @import("std");
const fmt = std.fmt;
const mem = std.mem;
const Allocator = std.mem.Allocator;
const Type = std.builtin.Type;

const Conn = @import("./sqlite3/Conn.zig");
const vtab = @import("./sqlite3/vtab.zig");
const sqlite_c = @import("./sqlite3/c.zig").c;

const db = @import("./db.zig");

const SchemaDef = @import("./schema/SchemaDef.zig");
const Schema = @import("./schema/Schema.zig");

const Self = @This();
const DbCtx = db.Ctx(
    &db_ctx_fields ++
    &@import("./schema/db.zig").db_ctx_fields
);

db_ctx: DbCtx,
allocator: Allocator,
id: i64,
name: []const u8,
schema: Schema,

pub const InitError = error {
    NoColumns,
    UnsupportedDb,
} || SchemaDef.ParseError || Schema.Error || db.Error || mem.Allocator.Error;

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

    var db_ctx = DbCtx { .conn = conn };
    const table_id = try createTable(&db_ctx, name);
    var s = try Schema.create(allocator, &db_ctx, table_id, def);

    return .{
        .allocator = allocator,
        .id = table_id,
        .name = name,
        .schema = s,
        .db_ctx = db_ctx,
    };
}

test "create table" {
    const conn = try Conn.openInMemory();
    defer conn.close();
    try db.Migrations.apply(conn);
    
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var cb_ctx = vtab.CallbackContext {
        .arena = &arena,
    };

    var table = try create(
        std.testing.allocator,
        conn,
        &cb_ctx,
        &[_][]const u8 {
            "", "main", "foo",
            "id BLOB NOT NULL",
            "SORT KEY (id)",
            "name TEXT NOT NULL",
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

    var db_ctx = DbCtx { .conn = conn };
    const table_id = try loadTable(&db_ctx, name);
    const s = try Schema.load(allocator, &db_ctx, table_id);

    return .{
        .allocator = allocator,
        .id = table_id,
        .name = name,
        .schema = s,
        .db_ctx = db_ctx,
    };
}

pub fn disconnect(self: *Self) void {
    self.allocator.free(self.name);
    self.schema.deinit(self.allocator);
    db.deinit_ctx(self.db_ctx);
}

pub fn destroy(self: *Self) void {
    // TODO delete all data
    self.allocator.free(self.name);
    self.schema.deinit(self.allocator);
    db.deinit_ctx(self.db_ctx);
}

pub fn ddl(_: *Self, allocator: Allocator) ![:0]const u8 {
    return fmt.allocPrintZ(allocator, "CREATE TABLE x (foo INTEGER)", .{});
}

// pub fn bestIndex(self: *Self) !void {

// }

// pub fn open(self: *Self) !Cursor {

// }

// pub fn update(self: *Self) !i64 {
    
// }

pub const db_ctx_fields = [_][]const u8 {
    "create_table_stmt",
    "load_table_stmt",
};

fn createTable(db_ctx: anytype, name: []const u8) !i64 {
    if (db_ctx.create_table_stmt == null) {
        db_ctx.create_table_stmt = try db_ctx.conn.prepare(
            \\INSERT INTO _stanchion_tables (name)
            \\VALUES (?)
            \\RETURNING id
        );
    }

    const stmt = db_ctx.create_table_stmt.?;
    try stmt.bind(.Text, 1, name);
    if (!try stmt.next()) {
        return db.Error.QueryReturnedNoRows;
    }
    const table_id = stmt.read(.Int64, false, 0);
    try stmt.reset();
    return table_id;
}

fn loadTable(db_ctx: anytype, name: []const u8) !i64 {
    if (db_ctx.load_table_stmt == null) {
        db_ctx.load_table_stmt = try db_ctx.conn.prepare(
            \\SELECT id FROM _stanchion_tables WHERE name = ?
        );
    }

    const stmt = db_ctx.load_table_stmt.?;
    try stmt.bind(.Text, 1, name);
    if (!try stmt.next()) {
        return db.Error.QueryReturnedNoRows;
    }
    const table_id = stmt.read(.Int64, false, 0);
    try stmt.reset();
    return table_id;
}
