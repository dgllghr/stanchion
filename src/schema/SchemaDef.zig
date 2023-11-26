const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;
const ArrayListUnmanaged = std.ArrayListUnmanaged;

const ColumnType = @import("./ColumnType.zig");

const Self = @This();

columns: ArrayListUnmanaged(ColumnDef),
sort_key: ArrayListUnmanaged([]const u8),

pub const ColumnDef = struct {
    name: []const u8,
    column_type: ColumnType,
};

pub const ParseError = error{
    NotMatched,
    Eof,
    UnexpectedCharacter,
    MultipleSortKeyDefinitions,
    InvalidDataType,
} || Allocator.Error;

pub fn parse(arena: *ArenaAllocator, args: []const []const u8) ParseError!Self {
    var columns = try ArrayListUnmanaged(ColumnDef)
        .initCapacity(arena.allocator(), args.len);
    var sort_key = ArrayListUnmanaged([]const u8){};

    for (args) |arg| {
        var r = Reader.init(arg);
        if (consumeSortKeyKeyword(&r)) |_| {
            if (sort_key.items.len > 0) {
                return ParseError.MultipleSortKeyDefinitions;
            }
            try parseSortKeyColumns(arena.allocator(), &r, &sort_key);
        } else |_| {
            const col_def = try parseColumnDef(arena.allocator(), &r);
            columns.appendAssumeCapacity(col_def);
        }
    }

    return .{
        .columns = columns,
        .sort_key = sort_key,
    };
}

test "parse schema definition" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const args = [_][]const u8{
        "foo INTEGER NOT NULL",
        "`bar:2` REAL NULL",
        "baz BOOL NOT NULL",
        "SORT KEY (foo, `bar:2`)",
    };

    const schema = try parse(&arena, &args);

    try testing.expectEqualSlices(u8, "foo", schema.columns.items[0].name);
    try testing.expectEqualSlices(u8, "bar:2", schema.columns.items[1].name);
    try testing.expectEqualSlices(u8, "baz", schema.columns.items[2].name);

    try testing.expectEqualSlices(u8, "foo", schema.sort_key.items[0]);
    try testing.expectEqualSlices(u8, "bar:2", schema.sort_key.items[1]);
}

fn parseColumnDef(allocator: Allocator, r: *Reader) !ColumnDef {
    const name = try parseIdentifier(allocator, r);
    const column_type = try parseColumnType(r);
    return .{
        .name = name,
        .column_type = column_type,
    };
}

fn parseColumnType(r: *Reader) !ColumnType {
    const data_type = try parseDataType(r);

    // TODO support SORT KEY and constraints in column definition (they can come before
    // the NULL / NOT NULL constraint)
    var nullable = true;
    consumeWhitespace(r);
    if (!r.eof()) {
        try consumeMatchAny(r, &[_]u8{ 'N', 'n' });

        if (r.current() == 'O' or r.current() == 'o') {
            r.advance();
            try consumeMatchAny(r, &[_]u8{ 'T', 't' });
            consumeWhitespace(r);
            try consumeMatchAny(r, &[_]u8{ 'N', 'n' });
            nullable = false;
        }

        try consumeMatchAny(r, &[_]u8{ 'U', 'u' });
        try consumeMatchAny(r, &[_]u8{ 'L', 'l' });
        try consumeMatchAny(r, &[_]u8{ 'L', 'l' });
    }

    return .{
        .data_type = data_type,
        .nullable = nullable,
    };
}

fn parseDataType(r: *Reader) !ColumnType.DataType {
    consumeWhitespace(r);

    // sqlite allows types (and other keywords) to be quoted. I find that very strange
    // and will not support that unless given a good reason

    if (consumeMatchBooleanType(r)) {
        return ColumnType.DataType.Boolean;
    } else |_| if (consumeMatchIntegerType(r)) {
        return ColumnType.DataType.Integer;
    } else |_| if (consumeMatchFloatType(r)) {
        return ColumnType.DataType.Float;
    } else |_| if (consumeMatchBlobType(r)) {
        return ColumnType.DataType.Blob;
    } else |_| if (consumeMatchTextType(r)) {
        return ColumnType.DataType.Text;
    } else |_| {
        return ParseError.InvalidDataType;
    }
}

test "parse data type" {
    const inputs = [_]struct { ColumnType.DataType, []const u8 }{
        .{ ColumnType.DataType.Integer, "INT" },
        .{ ColumnType.DataType.Boolean, "Bool" },
        .{ ColumnType.DataType.Float, " real " },
        .{ ColumnType.DataType.Integer, "integer" },
        .{ ColumnType.DataType.Boolean, "bOoleAN" },
        .{ ColumnType.DataType.Float, "FLOAT" },
        .{ ColumnType.DataType.Text, "text" },
        .{ ColumnType.DataType.Blob, "BLOB" },
    };

    for (inputs) |input| {
        var r = Reader.init(input[1]);
        const dt = try parseDataType(&r);
        try testing.expectEqual(input[0], dt);
    }
}

fn consumeMatchBooleanType(r: *Reader) !void {
    const start = r.position;
    errdefer r.position = start;

    try consumeMatchAny(r, &[_]u8{ 'B', 'b' });
    try consumeMatchAny(r, &[_]u8{ 'O', 'o' });
    try consumeMatchAny(r, &[_]u8{ 'O', 'o' });
    try consumeMatchAny(r, &[_]u8{ 'L', 'l' });

    if (r.eof()) {
        return;
    }
    if (isWhitespace(r.current())) {
        return;
    }

    try consumeMatchAny(r, &[_]u8{ 'E', 'e' });
    try consumeMatchAny(r, &[_]u8{ 'A', 'a' });
    try consumeMatchAny(r, &[_]u8{ 'N', 'n' });
}

fn consumeMatchIntegerType(r: *Reader) !void {
    const start = r.position;
    errdefer r.position = start;

    try consumeMatchAny(r, &[_]u8{ 'I', 'i' });
    try consumeMatchAny(r, &[_]u8{ 'N', 'n' });
    try consumeMatchAny(r, &[_]u8{ 'T', 't' });

    if (r.eof()) {
        return;
    }
    if (isWhitespace(r.current())) {
        return;
    }

    try consumeMatchAny(r, &[_]u8{ 'E', 'e' });
    try consumeMatchAny(r, &[_]u8{ 'G', 'g' });
    try consumeMatchAny(r, &[_]u8{ 'E', 'e' });
    try consumeMatchAny(r, &[_]u8{ 'R', 'r' });
}

fn consumeMatchFloatType(r: *Reader) !void {
    const start = r.position;
    errdefer r.position = start;

    if (r.eof()) {
        return ParseError.Eof;
    }

    if (r.current() == 'F' or r.current() == 'f') {
        r.advance();
        try consumeMatchAny(r, &[_]u8{ 'L', 'l' });
        try consumeMatchAny(r, &[_]u8{ 'O', 'o' });
        try consumeMatchAny(r, &[_]u8{ 'A', 'a' });
        try consumeMatchAny(r, &[_]u8{ 'T', 't' });
    } else {
        try consumeMatchAny(r, &[_]u8{ 'R', 'r' });
        try consumeMatchAny(r, &[_]u8{ 'E', 'e' });
        try consumeMatchAny(r, &[_]u8{ 'A', 'a' });
        try consumeMatchAny(r, &[_]u8{ 'L', 'l' });
    }
}

fn consumeMatchBlobType(r: *Reader) !void {
    const start = r.position;
    errdefer r.position = start;

    try consumeMatchAny(r, &[_]u8{ 'B', 'b' });
    try consumeMatchAny(r, &[_]u8{ 'L', 'l' });
    try consumeMatchAny(r, &[_]u8{ 'O', 'o' });
    try consumeMatchAny(r, &[_]u8{ 'B', 'b' });
}

fn consumeMatchTextType(r: *Reader) !void {
    const start = r.position;
    errdefer r.position = start;

    try consumeMatchAny(r, &[_]u8{ 'T', 't' });
    try consumeMatchAny(r, &[_]u8{ 'E', 'e' });
    try consumeMatchAny(r, &[_]u8{ 'X', 'x' });
    try consumeMatchAny(r, &[_]u8{ 'T', 't' });
}

fn parseSortKeyColumns(
    allocator: Allocator,
    r: *Reader,
    sk_cols: *ArrayListUnmanaged([]const u8),
) !void {
    consumeWhitespace(r);
    try consumeMatchAny(r, &[_]u8{'('});

    const first_col = try parseIdentifier(allocator, r);
    try sk_cols.append(allocator, first_col);
    consumeWhitespace(r);
    if (r.eof()) {
        return ParseError.Eof;
    }

    while (true) {
        switch (r.current()) {
            ',' => {
                r.advance();
                if (r.eof()) {
                    return ParseError.Eof;
                }
                const col = try parseIdentifier(allocator, r);
                try sk_cols.append(allocator, col);
                consumeWhitespace(r);
                if (r.eof()) {
                    return ParseError.Eof;
                }
            },
            ')' => break,
            else => return ParseError.UnexpectedCharacter,
        }
    }
}

/// TODO support unicode characters in identifiers
fn parseIdentifier(allocator: Allocator, r: *Reader) ![]const u8 {
    consumeWhitespace(r);

    var end_char: ?u8 = null;
    if (try consumeOpenQuoteChar(r)) |qc| {
        end_char = qc;
        if (qc == '[') {
            end_char = ']';
        }
    }

    const start = r.position;
    var end: usize = undefined;
    var escapes: usize = 0;
    if (end_char == null) {
        // Must have at least 1 character that is a valid bare identifier first character
        if (!isBareIdentifierFirstChar(r.current())) {
            return ParseError.NotMatched;
        }
        r.advance();
        if (r.eof()) {
            end = r.position;
            return r.text[start..end];
        }

        while (isBareIdentifierChar(r.current())) {
            r.advance();
            if (r.eof()) {
                break;
            }
        }
        end = r.position;
    } else {
        while (true) {
            if (r.current() == end_char) {
                // `[]` quote does not have escapes
                if (end_char == ']') {
                    end = r.position;
                    r.advance();
                    break;
                }
                const next = r.peek() orelse {
                    end = r.position;
                    break;
                };
                if (next != end_char) {
                    end = r.position;
                    r.advance();
                    break;
                }
                escapes += 1;
                r.advance();
            }
            r.advance();
            if (r.eof()) {
                return ParseError.Eof;
            }
        }
    }

    if (escapes == 0) {
        return r.text[start..end];
    } else {
        // It's okay to allocate a new string because it's an arena allocator
        var ident = try allocator.alloc(u8, end - start - escapes);
        var text_idx = start;
        for (0..ident.len) |idx| {
            const c = r.text[text_idx];
            ident[idx] = c;
            if (c == end_char) {
                text_idx += 1;
            }
            text_idx += 1;
        }
        return ident;
    }
}

test "parse identifier" {
    const inputs = [_]struct { []const u8, []const u8 }{
        .{ "hello_world", "hello_world" },
        .{ "hello$world", "hello$world" },
        .{ "hello", "hello world" },
        .{ "hello world", "[hello world]" },
        .{ "hello`world", "`hello``world`" },
        .{ "hello` world", "[hello` world]" },
        .{ "hello``world", "`hello````world`" },
        .{ "h", "h" },
        .{ "h", "\"h\"" },
    };

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    for (inputs) |input| {
        var r = Reader.init(input[1]);
        const id = try parseIdentifier(arena.allocator(), &r);
        try testing.expectEqualSlices(u8, input[0], id);
    }
}

/// TODO support unicode
fn isBareIdentifierFirstChar(c: u8) bool {
    return c == '_' or (c >= 0x41 and c <= 0x5a) or (c >= 0x61 and c <= 0x7a);
}

/// TODO support unicode
fn isBareIdentifierChar(c: u8) bool {
    return c == '_' or c == '$' or (c >= 0x41 and c <= 0x5a) or
        (c >= 0x61 and c <= 0x7a) or (c >= 0x30 and c <= 0x39);
}

fn consumeSortKeyKeyword(r: *Reader) !void {
    const start = r.position;
    errdefer r.position = start;

    consumeWhitespace(r);

    try consumeMatchAny(r, &[_]u8{ 'S', 's' });
    try consumeMatchAny(r, &[_]u8{ 'O', 'o' });
    try consumeMatchAny(r, &[_]u8{ 'R', 'r' });
    try consumeMatchAny(r, &[_]u8{ 'T', 't' });

    // Must have separating whitespace
    const sort_end = r.position;
    consumeWhitespace(r);
    if (r.position == sort_end) {
        return ParseError.NotMatched;
    }

    try consumeMatchAny(r, &[_]u8{ 'K', 'k' });
    try consumeMatchAny(r, &[_]u8{ 'E', 'e' });
    try consumeMatchAny(r, &[_]u8{ 'Y', 'y' });
}

test "consume sort key keyword" {
    var r = Reader.init("SORT KEY");
    try consumeSortKeyKeyword(&r);
    r = Reader.init("sort key");
    try consumeSortKeyKeyword(&r);
    r = Reader.init("SoRt kEy");
    try consumeSortKeyKeyword(&r);
    r = Reader.init("  SORT    KEY");
    try consumeSortKeyKeyword(&r);

    r = Reader.init("SORTKEY");
    try testing.expectError(ParseError.NotMatched, consumeSortKeyKeyword(&r));
    r = Reader.init("sort");
    try testing.expectError(ParseError.NotMatched, consumeSortKeyKeyword(&r));
    r = Reader.init("foobar");
    try testing.expectError(ParseError.NotMatched, consumeSortKeyKeyword(&r));
}

fn consumeMatchAny(r: *Reader, comptime chars: []const u8) !void {
    if (r.eof()) {
        return ParseError.Eof;
    }
    const nc = r.current();
    var matched = false;
    inline for (chars) |c| {
        if (c == nc) {
            matched = true;
            break;
        }
    }
    if (!matched) {
        return ParseError.NotMatched;
    }
    r.advance();
}

const open_quote_chars = [_]u8{ '"', '[', '`', '\'' };

/// If the current character in the reader is an open quote char, advances the reader and
/// returns the quote character
fn consumeOpenQuoteChar(r: *Reader) !?u8 {
    if (r.eof()) {
        return ParseError.Eof;
    }
    const c = r.current();
    inline for (open_quote_chars) |qc| {
        if (c == qc) {
            r.advance();
            return qc;
        }
    }
    return null;
}

fn consumeWhitespace(r: *Reader) void {
    while (!r.eof() and isWhitespace(r.current())) {
        r.advance();
    }
}

fn isWhitespace(c: u8) bool {
    return c == '\n' or c == '\t' or c == ' ' or c == '\r';
}

const Reader = struct {
    text: []const u8,
    position: usize,

    fn init(text: []const u8) @This() {
        return .{
            .text = text,
            .position = 0,
        };
    }

    fn current(self: @This()) u8 {
        return self.text[self.position];
    }

    fn peek(self: @This()) ?u8 {
        if (self.position + 1 >= self.text.len) {
            return null;
        }
        return self.text[self.position + 1];
    }

    fn advance(self: *@This()) void {
        self.position += 1;
    }

    fn eof(self: @This()) bool {
        return self.position >= self.text.len;
    }
};
