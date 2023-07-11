const c = @import("./c.zig").c;
const errors = @import("./errors.zig");

const Self = @This();

stmt: *c.sqlite3_stmt,

pub const SqliteType = enum {
    Bool,
    Int32,
    Int64,
    Float,
    Text,
    Blob,

    fn Type(comptime self: @This()) type {
        return switch (self) {
            .Bool => bool,
            .Int32 => i32,
            .Int64 => i64,
            .Float => f64,
            .Text => []const u8,
            .Blob => []const u8,
        };
    }
};

pub fn deinit(self: Self) void {
    _ = c.sqlite3_finalize(self.stmt);
}

pub fn reset(self: Self) !void {
    var res = c.sqlite3_clear_bindings(self.stmt);
    if (res != c.SQLITE_OK) {
        return errors.errorFromResultCode(res);
    }
    res = c.sqlite3_reset(self.stmt);
    if (res != c.SQLITE_OK) {
        return errors.errorFromResultCode(res);
    }
}

pub fn bind(self: Self, comptime t: SqliteType, index: usize, value: ?t.Type()) !void {
    var res: c_int = 0;
    if (value == null) {
        res = c.sqlite3_bind_null(self.stmt, @intCast(index));
    } else {
        res = switch (t) {
            .Bool => c.sqlite3_bind_int(self.stmt, @intCast(index), if (value.?) 1 else 0),
            .Int32 => c.sqlite3_bind_int(self.stmt, @intCast(index), value.?),
            .Int64 => c.sqlite3_bind_int64(self.stmt, @intCast(index), value.?),
            .Float => c.sqlite3_bind_double(self.stmt, @intCast(index), value.?),
            // TODO is SQLITE_STATIC right?
            .Text => c.sqlite3_bind_text(self.stmt, @intCast(index), @ptrCast(value.?), @intCast(value.?.len), c.SQLITE_STATIC),
            .Blob => c.sqlite3_bind_blob(self.stmt, @intCast(index), @ptrCast(value.?), @intCast(value.?.len), c.SQLITE_STATIC),
        };
    }
    if (res != c.SQLITE_OK) {
        return errors.errorFromResultCode(res);
    }
}

pub fn exec(self: Self) !void {
    const res = c.sqlite3_step(self.stmt);
    switch (res) {
        c.SQLITE_DONE => {},
        c.SQLITE_ROW => return error.ExecReturnedData,
        else => {
            return errors.errorFromResultCode(res);
        },
    }
}

pub fn next(self: Self) !bool {
    var res = c.sqlite3_step(self.stmt);
    if (res == c.SQLITE_DONE) {
        return false;
    }
    if (res != c.SQLITE_ROW) {
        return errors.errorFromResultCode(res);
    }
    return true;
}

pub fn read(
    self: Self,
    comptime read_type: SqliteType,
    comptime nullable: bool,
    index: usize,
) if (nullable) ?read_type.Type() else read_type.Type() {
    if (nullable) {
        if (c.sqlite3_column_type(self.stmt, @intCast(index)) == c.SQLITE_NULL) {
            return null;
        }
    }

    switch (read_type) {
        .Bool => {
            const n = c.sqlite3_column_int(self.stmt, @intCast(index));
            return n > 0;
        },
        .Int32 => {
            return c.sqlite3_column_int(self.stmt, @intCast(index));
        },
        .Int64 => {
            return c.sqlite3_column_int64(self.stmt, @intCast(index));
        },
        .Float => {
            return c.sqlite3_column_double(self.stmt, @intCast(index));
        },
        .Text => {
            const c_index: c_int = @intCast(index);
            const len = c.sqlite3_column_bytes(self.stmt, c_index);
            if (len == 0) {
                return "";
            }
            const data = c.sqlite3_column_text(self.stmt, c_index);
            return @as([*c]const u8, @ptrCast(data))[0..@intCast(len)];
        },
        .Blob => {
            const c_index: c_int = @intCast(index);

            const data = c.sqlite3_column_blob(self.stmt, c_index);
            if (data == null) {
                return "";
            }

            const len = c.sqlite3_column_bytes(self.stmt, c_index);
            return @as([*c]const u8, @ptrCast(data))[0..@intCast(len)];
        },
    }
}
