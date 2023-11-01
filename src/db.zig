const std = @import("std");
const Type = std.builtin.Type;

const Conn = @import("./sqlite3/Conn.zig");
const Stmt = @import("./sqlite3/Stmt.zig");

const Self = @This();

pub const Error = error{
    QueryReturnedNoRows,
} || @import("sqlite3/errors.zig").Error;

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
        \\  next_rowid INTEGER NOT NULL,
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
        \\CREATE TABLE _stanchion_segments (
        \\  id INTEGER NOT NULL PRIMARY KEY,
        \\  segment BLOB NOT NULL
        \\) STRICT
    };

    const migrations = [_][]const [*:0]const u8{&v1};

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

            try insert_migration_stmt.clearBoundParams();
            try insert_migration_stmt.resetExec();
            try insert_migration_stmt.bind(.Int32, 1, version);
            try insert_migration_stmt.exec();
        }
    }

    fn readCurrentVersion(conn: Conn) !usize {
        const stmt = try conn.prepare(
            \\SELECT COALESCE(MAX(version), 0) FROM _stanchion_migrations
        );
        defer stmt.deinit();
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
