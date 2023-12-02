const std = @import("std");
const debug = std.debug;
const fmt = std.fmt;
const math = std.math;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const Stmt = @import("../sqlite3/Stmt.zig");

const schema_mod = @import("../schema.zig");
const DataType = schema_mod.ColumnType.DataType;
const Schema = schema_mod.Schema;

const sql_fmt = @import("sql_fmt.zig");
const pending_inserts = @import("pending_inserts.zig");
const Ctx = @import("Ctx.zig");
const EntryType = @import("entry_type.zig").EntryType;

next_rowid: i64,
last_write_rowid: i64,

load_next_rowid: Stmt,
update_next_rowid: Stmt,

const Self = @This();

pub fn init(
    tmp_arena: *ArenaAllocator,
    ctx: *const Ctx,
    schema: *const Schema,
    next_rowid: i64,
    last_write_rowid: i64,
) !Self {
    const load_next_rowid = try ctx.conn.prepare(try loadNextRowidQuery(ctx, tmp_arena));
    errdefer load_next_rowid.deinit();
    const update_next_rowid = try ctx.conn.prepare(try updateNextRowidDml(ctx, tmp_arena));
    errdefer update_next_rowid.deinit();

    for (ctx.sort_key, 1..) |rank, idx| {
        const data_type = schema.columns.items[rank].column_type.data_type;
        try bindMinValue(load_next_rowid, idx, data_type);
        // The first bound parameter in `update_next_rowid` is the new rowid
        try bindMinValue(update_next_rowid, idx + 1, data_type);
    }

    return .{
        .next_rowid = next_rowid,
        .last_write_rowid = last_write_rowid,
        .load_next_rowid = load_next_rowid,
        .update_next_rowid = update_next_rowid,
    };
}

pub fn deinit(self: *Self) void {
    self.load_next_rowid.deinit();
    self.update_next_rowid.deinit();
}

pub fn bindMinValue(stmt: Stmt, idx: usize, data_type: DataType) !void {
    switch (data_type) {
        .Boolean => try stmt.bind(.Int32, idx, 0),
        .Integer => try stmt.bind(.Int64, idx, math.minInt(i64)),
        .Float => try stmt.bind(.Float, idx, math.floatMin(f64)),
        .Text => try stmt.bind(.Text, idx, ""),
        .Blob => try stmt.bind(.Blob, idx, ""),
    }
}

pub fn getAndIncrNextRowid(self: *Self) i64 {
    const rowid = self.next_rowid;
    self.next_rowid += 1;
    return rowid;
}

pub fn loadNextRowid(self: *Self) !void {
    defer self.load_next_rowid.resetExec() catch {};

    _ = try self.load_next_rowid.next();

    self.next_rowid = self.load_next_rowid.read(.Int64, false, 0);
    self.last_write_rowid = self.next_rowid;
}

fn loadNextRowidQuery(ctx: *const Ctx, arena: *ArenaAllocator) ![]const u8 {
    return fmt.allocPrintZ(arena.allocator(),
        \\SELECT col_rowid
        \\FROM "{s}_primaryindex"
        \\WHERE ({s}, rowid) = ({s}, -1) AND entry_type = {d}
    , .{
        ctx.vtab_table_name,
        sql_fmt.ColumnListFormatter("sk_value_{d}"){ .len = ctx.sort_key.len },
        sql_fmt.ParameterListFormatter{ .len = ctx.sort_key.len },
        @intFromEnum(EntryType.TableState),
    });
}

pub fn persistNextRowid(self: *Self) !void {
    if (self.last_write_rowid != self.next_rowid) {
        defer self.update_next_rowid.resetExec() catch {};

        try self.update_next_rowid.bind(.Int64, 1, self.next_rowid);
        try self.update_next_rowid.exec();

        self.last_write_rowid = self.next_rowid;
    }
}

fn updateNextRowidDml(ctx: *const Ctx, arena: *ArenaAllocator) ![]const u8 {
    return fmt.allocPrintZ(arena.allocator(),
        \\UPDATE "{s}_primaryindex"
        \\SET col_rowid = ?
        \\WHERE ({s}, rowid) = ({s}, -1) AND entry_type = {d}
    , .{
        ctx.vtab_table_name,
        sql_fmt.ColumnListFormatter("sk_value_{d}"){ .len = ctx.sort_key.len },
        sql_fmt.ParameterListFormatter{ .len = ctx.sort_key.len },
        @intFromEnum(EntryType.TableState),
    });
}
