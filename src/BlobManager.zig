const std = @import("std");
const fmt = std.fmt;
const log = std.log;
const ArenaAllocator = std.heap.ArenaAllocator;

const sqlite = @import("sqlite3.zig");
const Blob = sqlite.Blob;
const Conn = sqlite.Conn;

const VtabCtxSchemaless = @import("ctx.zig").VtabCtxSchemaless;
const prep_stmt = @import("prepared_stmt.zig");

ctx: *const VtabCtxSchemaless,

/// The full name of the shadow table so it can be passed to `Blob.open` without allocating
table_name: [:0]const u8,

insert_stmt: StmtCell,
delete_stmt: StmtCell,

const Self = @This();

const StmtCell = prep_stmt.Cell(VtabCtxSchemaless);

const blob_column_name = "blob";

pub fn init(
    lifetime_arena: *ArenaAllocator,
    tmp_arena: *ArenaAllocator,
    ctx: *const VtabCtxSchemaless,
) !Self {
    try setup(tmp_arena, ctx.*);

    const table_name = try fmt.allocPrintZ(
        lifetime_arena.allocator(),
        "{s}_blobs",
        .{ctx.vtabName()},
    );

    return .{
        .ctx = ctx,
        .table_name = table_name,
        .insert_stmt = StmtCell.init(&insertDml),
        .delete_stmt = StmtCell.init(&deleteDml),
    };
}

fn setup(tmp_arena: *ArenaAllocator, ctx: VtabCtxSchemaless) !void {
    const query = try fmt.allocPrintZ(tmp_arena.allocator(),
        \\CREATE TABLE IF NOT EXISTS "{s}_blobs" (
        \\  id INTEGER NOT NULL PRIMARY KEY,
        \\  blob BLOB NOT NULL
        \\)
    , .{ctx.vtabName()});
    try ctx.conn().exec(query);
}

pub fn deinit(self: *Self) void {
    self.insert_stmt.deinit();
    self.delete_stmt.deinit();
}

pub fn destroy(self: *Self, tmp_arena: *ArenaAllocator) !void {
    const query = try fmt.allocPrintZ(
        tmp_arena.allocator(),
        \\DROP TABLE "{s}_blobs"
    ,
        .{self.ctx.vtabName()},
    );
    try self.ctx.conn().exec(query);
}

pub const Handle = struct {
    ctx: *Self,
    id: i64,
    blob: Blob,

    pub fn init(ctx: *Self, id: i64, blob: Blob) Handle {
        return .{
            .ctx = ctx,
            .id = id,
            .blob = blob,
        };
    }

    pub fn close(self: *Handle) !void {
        try self.blob.close();
    }

    pub fn tryClose(self: *Handle) void {
        self.blob.close() catch |e| {
            log.err("unable to close blob {}: {}", .{ self.id, e });
        };
    }

    pub fn destroy(self: *Handle, tmp_arena: *ArenaAllocator) !void {
        try self.ctx.delete(tmp_arena, self.id);
    }

    pub fn tryDestroy(self: *Handle, tmp_arena: *ArenaAllocator) void {
        self.ctx.delete(tmp_arena, self.id) catch |e| {
            log.err("unable to destroy blob {}: {}", .{ self.id, e });
        };
    }
};

pub fn create(self: *Self, tmp_arena: *ArenaAllocator, size: u32) !Handle {
    const stmt = try self.insert_stmt.acquire(tmp_arena, self.ctx.*);
    defer self.insert_stmt.release();

    try stmt.bind(.Int64, 1, @as(i64, @intCast(size)));

    try stmt.exec();
    const id = self.ctx.conn().lastInsertRowid();
    errdefer self.delete(tmp_arena, id) catch |e| {
        log.err("unable to destroy segment {}: {}", .{ id, e });
    };

    const blob = try Blob.open(self.ctx.conn(), self.table_name, blob_column_name, id);
    return .{
        .ctx = self,
        .id = id,
        .blob = blob,
    };
}

fn insertDml(ctx: VtabCtxSchemaless, arena: *ArenaAllocator) ![]const u8 {
    return fmt.allocPrintZ(arena.allocator(),
        \\INSERT INTO "{s}_blobs" (blob)
        \\VALUES (ZEROBLOB(?))
    , .{ctx.vtabName()});
}

pub fn open(self: *Self, id: i64) !Handle {
    const blob = try Blob.open(self.ctx.conn(), self.table_name, blob_column_name, id);
    return .{
        .ctx = self,
        .id = id,
        .blob = blob,
    };
}

pub fn delete(self: *Self, tmp_arena: *ArenaAllocator, id: i64) !void {
    const stmt = try self.delete_stmt.acquire(tmp_arena, self.ctx.*);
    defer self.delete_stmt.release();

    try stmt.bind(.Int64, 1, @as(i64, @intCast(id)));
    try stmt.exec();
}

fn deleteDml(ctx: VtabCtxSchemaless, arena: *ArenaAllocator) ![]const u8 {
    return fmt.allocPrintZ(arena.allocator(),
        \\DELETE FROM "{s}_blobs" WHERE id = ?
    , .{ctx.vtabName()});
}
