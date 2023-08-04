const std = @import("std");
const Type = std.builtin.Type;

const Conn = @import("./sqlite3/Conn.zig");
const Stmt = @import("./sqlite3/Stmt.zig");

pub const Error = error {
    QueryReturnedNoRows,
} || @import("./sqlite3/errors.zig").Error;

pub fn Ctx(comptime statement_fields: []const []const u8) type {
    var fields: [statement_fields.len + 1]Type.StructField = undefined;
    fields[0] = .{
        .name = "conn",
        .type = Conn,
        .default_value = null,
        .is_comptime = false,
        // TODO what should this be?
        .alignment = @alignOf(Conn),
    };
    inline for (statement_fields, 1..) |f, idx| {
        fields[idx] = statementField(f);
    }
    return @Type(.{
        .Struct = .{
            .layout = .Auto,
            .backing_integer = null,
            .fields = &fields,
            .decls = &[_]Type.Declaration{},
            .is_tuple = false,
        }
    });
}

pub fn deinit_ctx(ctx: anytype) void {
    const type_info = @typeInfo(@TypeOf(ctx));
    switch (type_info) {
        .Struct => |s| {
            inline for (s.fields) |field| {
                if (field.type == ?Stmt) {
                    var stmt = @field(ctx, field.name);
                    if (stmt) |st| {
                        st.deinit();
                    }
                }
            }
        },
        else => @compileError("db ctx must be a struct"),
    }
}

fn statementField(comptime name: []const u8) Type.StructField {
    return .{
        .name = name,
        .type = ?Stmt,
        .default_value = &@as(?Stmt, null),
        .is_comptime = false,
        // TODO what should this be?
        .alignment = @alignOf(?Stmt),
    };
}

pub const Migrations = struct {
    const setup =
        \\CREATE TABLE IF NOT EXISTS _stanchion_migrations (
        \\  version INTEGER NOT NULL PRIMARY KEY,
        \\  applied_at INTEGER NOT NULL
        \\) STRICT
    ;

    const v1 = [_][*:0]const u8{
        \\CREATE TABLE _stanchion_tables (
        \\  id INTEGER NOT NULL PRIMARY KEY,
        \\  -- It seems like `collate nocase` is what sqlite uses to compare table names
        \\  -- when referenced in sql queries, so use `collate nocase` on the name column
        \\  -- to match sqlite's behavior
        \\  name TEXT NOT NULL COLLATE NOCASE,
        \\  UNIQUE (name)
        \\) STRICT
        ,
        \\CREATE TABLE _stanchion_columns (
        \\  table_id INTEGER NOT NULL,
        \\  rank INTEGER NOT NULL,
        \\  name TEXT NOT NULL COLLATE NOCASE,
        \\  column_type TEXT NOT NULL,
        \\  sk_rank INTEGER NULL,
        \\  PRIMARY KEY (table_id, rank)
        \\) STRICT, WITHOUT ROWID
        ,
        \\CREATE TABLE _stanchion_stripes (
        \\  id INTEGER NOT NULL PRIMARY KEY,
        \\  stripe BLOB NOT NULL            
        \\) STRICT
        ,
        \\CREATE TABLE _stanchion_segments (
        \\  id INTEGER NOT NULL PRIMARY KEY,
        \\  present_stripe_id INTEGER NULL,
        \\  primary_stripe_id INTEGER NULL,
        \\  secondary_stripe_id INTEGER NULL    
        \\) STRICT
    };

    const migrations = [_][]const [*:0]const u8 {&v1};

    pub fn apply(conn: Conn) !void {
        try conn.exec(setup);

        const curr_version = try readCurrentVersion(conn);
        const insert_migration_stmt = try conn.prepare(
            \\INSERT INTO _stanchion_migrations (version, applied_at)
            \\VALUES (?, unixepoch())
        );
        defer insert_migration_stmt.deinit();
        for (migrations[curr_version..], 1..) |m, v| {
            const version: i32 = @intCast(curr_version + v);
            // TODO wrap each migration in a savepoint
            try applyMigration(conn, m);

            try insert_migration_stmt.reset();
            try insert_migration_stmt.bind(.Int32, 1, version);
            try insert_migration_stmt.exec();
        }
    }

    fn readCurrentVersion(conn: Conn) !usize {
        const stmt = try conn.prepare(
            \\SELECT COALESCE(MAX(version), 0) FROM _stanchion_migrations
        );
        if (!try stmt.next()) {
            return Error.QueryReturnedNoRows;
        }
        return @intCast(stmt.read(.Int32, false, 0));
    }

    fn applyMigration(conn: Conn, migration: []const [*:0]const u8) !void {
        for (migration) |stmt| {
            try conn.exec(stmt);
        }
    }
};