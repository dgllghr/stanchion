const std = @import("std");
const fmt = std.fmt;
const mem = std.mem;

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

    pub fn read(canonical: []const u8) @This() {
        switch (canonical[0]) {
            'B' => {
                if (canonical[1] == 'O') {
                    std.debug.assert(mem.eql(u8, "OLEAN", canonical[2..]));
                    return .Boolean;
                } else {
                    std.debug.assert(mem.eql(u8, "LOB", canonical[1..]));
                    return .Blob;
                }
            },
            'I' => {
                std.debug.assert(mem.eql(u8, "NTEGER", canonical[1..]));
                return .Integer;
            },
            'F' => {
                std.debug.assert(mem.eql(u8, "LOAT", canonical[1..]));
                return .Float;
            },
            'T' => {
                std.debug.assert(mem.eql(u8, "EXT", canonical[1..]));
                return .Text;
            },
            else => unreachable,
        }
    }
};

pub fn format(
    self: Self,
    comptime _: []const u8,
    _: std.fmt.FormatOptions,
    writer: anytype,
) !void {
    const nullable = if (self.nullable) "NULL" else "NOT NULL";
    try writer.print("{s} {s}", .{self.data_type, nullable});
}

pub fn read(canonical: []const u8) Self {
    var split = mem.indexOfScalar(u8, canonical, ' ').?;
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
