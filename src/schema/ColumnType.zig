const std = @import("std");
const debug = std.debug;
const fmt = std.fmt;
const mem = std.mem;
const testing = std.testing;

const Self = @This();

data_type: DataType,
nullable: bool,

pub const DataType = enum {
    Boolean,
    Integer,
    Float,
    Text,
    Blob,

    pub fn format(
        self: @This(),
        comptime _: []const u8,
        _: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        const str = switch (self) {
            .Boolean => "BOOLEAN",
            .Integer => "INTEGER",
            .Float => "FLOAT",
            .Text => "TEXT",
            .Blob => "BLOB",
        };
        try writer.print("{s}", .{str});
    }

    pub const SqliteFormatter = struct {
        data_type: DataType,

        pub fn format(
            self: @This(),
            comptime _: []const u8,
            _: std.fmt.FormatOptions,
            writer: anytype,
        ) !void {
            const str = switch (self.data_type) {
                .Boolean => "INTEGER",
                .Integer => "INTEGER",
                .Float => "REAL",
                .Text => "TEXT",
                .Blob => "BLOB",
            };
            try writer.print("{s}", .{str});
        }
    };

    pub fn read(canonical: []const u8) @This() {
        switch (canonical[0]) {
            'B' => {
                if (canonical[1] == 'O') {
                    debug.assert(mem.eql(u8, "OLEAN", canonical[2..]));
                    return .Boolean;
                } else {
                    debug.assert(mem.eql(u8, "LOB", canonical[1..]));
                    return .Blob;
                }
            },
            'I' => {
                debug.assert(mem.eql(u8, "NTEGER", canonical[1..]));
                return .Integer;
            },
            'F' => {
                debug.assert(mem.eql(u8, "LOAT", canonical[1..]));
                return .Float;
            },
            'T' => {
                debug.assert(mem.eql(u8, "EXT", canonical[1..]));
                return .Text;
            },
            else => unreachable,
        }
    }
};

pub const Rowid: Self = .{
    .data_type = .Integer,
    .nullable = false,
};

pub fn format(
    self: Self,
    comptime _: []const u8,
    _: std.fmt.FormatOptions,
    writer: anytype,
) !void {
    const nullable = if (self.nullable) "NULL" else "NOT NULL";
    try writer.print("{s} {s}", .{ self.data_type, nullable });
}

pub fn sqliteFormatter(self: Self) SqliteFormatter {
    return .{ .column_type = self };
}

pub const SqliteFormatter = struct {
    column_type: Self,

    pub fn format(
        self: @This(),
        comptime _: []const u8,
        _: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        const nullable = if (self.column_type.nullable) "NULL" else "NOT NULL";
        try writer.print("{s} {s}", .{
            DataType.SqliteFormatter{ .data_type = self.column_type.data_type },
            nullable,
        });
    }
};

pub fn read(canonical: []const u8) Self {
    const split = mem.indexOfScalar(u8, canonical, ' ').?;
    const data_type = DataType.read(canonical[0..split]);
    const rest = canonical[(split + 1)..];
    var nullable = true;
    if (std.mem.eql(u8, "NOT NULL", rest)) {
        nullable = false;
    } else {
        std.debug.assert(mem.eql(u8, "NULL", rest));
    }
    return .{
        .data_type = data_type,
        .nullable = nullable,
    };
}

test "schema: read column type" {
    var col_type = read("INTEGER NOT NULL");
    try testing.expect(!col_type.nullable);
    try testing.expectEqual(DataType.Integer, col_type.data_type);

    col_type = read("FLOAT NULL");
    try testing.expect(col_type.nullable);
    try testing.expectEqual(DataType.Float, col_type.data_type);

    col_type = read("TEXT NOT NULL");
    try testing.expect(!col_type.nullable);
    try testing.expectEqual(DataType.Text, col_type.data_type);

    col_type = read("BLOB NULL");
    try testing.expect(col_type.nullable);
    try testing.expectEqual(DataType.Blob, col_type.data_type);

    col_type = read("BOOLEAN NOT NULL");
    try testing.expect(!col_type.nullable);
    try testing.expectEqual(DataType.Boolean, col_type.data_type);
}
