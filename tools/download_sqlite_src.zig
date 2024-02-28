const std = @import("std");
const fmt = std.fmt;
const fs = std.fs;
const http = std.http;
const log = std.log;
const mem = std.mem;
const Allocator = mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;
const ArrayList = std.ArrayList;
const HttpClient = std.http.Client;

const FindReleaseError = error{
    FetchSQLiteHistoryPageError,
    SQLiteHistoryPageFormatError,
    SQLiteVersionNotFound,
};

const DownloadError = error{
    FetchSQLiteSrcError,
};

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    // Args are version and output file path
    const args = try std.process.argsAlloc(arena.allocator());
    if (args.len != 3) fatal("invalid arguments. expected: {} found: {}", .{ 2, args.len - 1 });

    const version = args[1];

    const output_file_path = args[2];
    var out_file = std.fs.cwd().createFile(output_file_path, .{}) catch |err| {
        fatal("unable to open '{s}': {s}", .{ output_file_path, @errorName(err) });
    };
    defer out_file.close();

    var http_client = http.Client{
        .allocator = arena.allocator(),
    };
    defer http_client.deinit();

    const version_slug = generateVersionSlug(version);
    const year = findReleaseYear(arena.allocator(), &http_client, version) catch |e| switch (e) {
        FindReleaseError.FetchSQLiteHistoryPageError => fatal(
            "error response when fetching sqlite version history page: {!}",
            .{e},
        ),
        FindReleaseError.SQLiteHistoryPageFormatError => fatal(
            "failed to parse sqlite version history page: {!}",
            .{e},
        ),
        FindReleaseError.SQLiteVersionNotFound => fatal(
            "sqlite version {s} not found: {!}",
            .{ version, e },
        ),
        else => fatal("io error finding sqlite version: {!}", .{e}),
    };

    downloadSource(
        arena.allocator(),
        &http_client,
        year,
        version_slug,
        out_file,
    ) catch |e| switch (e) {
        DownloadError.FetchSQLiteSrcError => fatal(
            "error response when downloading sqlite source: {!}",
            .{e},
        ),
        else => fatal("io error finding sqlite version: {!}", .{e}),
    };
}

fn generateVersionSlug(version: []const u8) [7]u8 {
    var version_slug: [7]u8 = .{ '0', '0', '0', '0', '0', '0', '0' };
    var version_parts = mem.splitScalar(u8, version, '.');
    // The major version number is always 1 digit
    version_slug[0] = version_parts.next().?[0];
    // Minor and patch versions are two digits
    var idx: usize = 1;
    while (version_parts.next()) |vp| {
        if (vp.len == 1) {
            version_slug[idx + 1] = vp[0];
        } else {
            version_slug[idx] = vp[0];
            version_slug[idx + 1] = vp[1];
        }
        idx += 2;
    }
    return version_slug;
}

fn findReleaseYear(allocator: Allocator, client: *HttpClient, version: []const u8) !u16 {
    var body = ArrayList(u8).init(allocator);
    defer body.deinit();
    const history_url = "https://www.sqlite.org/chronology.html";
    const res = try client.fetch(.{
        .location = .{ .url = history_url },
        .response_storage = .{ .dynamic = &body },
        .max_append_size = 16 * 1024 * 1024,
    });
    if (res.status != .ok) {
        return FindReleaseError.FetchSQLiteHistoryPageError;
    }
    return parseVersionYear(version, body.items);
}

/// Don't look at this function
fn parseVersionYear(version: []const u8, html: []const u8) !u16 {
    // I told you not to look
    const body_start = mem.indexOf(u8, html, "<tbody>");
    if (body_start == null) {
        return FindReleaseError.SQLiteHistoryPageFormatError;
    }

    const body = html[body_start.?..];
    // Why are you still looking?
    var ver_rows = mem.split(u8, body, "</tr>");
    while (ver_rows.next()) |row| {
        if (mem.indexOf(u8, row, version)) |_| {
            var parts = mem.splitScalar(u8, row, '>');
            while (parts.next()) |part| {
                const year_candidate = part[0..4];
                const year = fmt.parseInt(u16, year_candidate, 10) catch {
                    continue;
                };
                return year;
            }
            return FindReleaseError.SQLiteHistoryPageFormatError;
        }
    }
    return FindReleaseError.SQLiteVersionNotFound;
}

fn downloadSource(
    allocator: Allocator,
    client: *HttpClient,
    year: u16,
    version_slug: [7]u8,
    out: fs.File,
) !void {
    const src_url = try fmt.allocPrint(
        allocator,
        "https://www.sqlite.org/{}/sqlite-amalgamation-{s}.zip",
        .{ year, version_slug },
    );
    defer allocator.free(src_url);

    var body = ArrayList(u8).init(allocator);
    defer body.deinit();
    const res = try client.fetch(.{
        .location = .{ .url = src_url },
        .response_storage = .{ .dynamic = &body },
        .max_append_size = 16 * 1024 * 1024 * 1024,
    });
    if (res.status != .ok) {
        return DownloadError.FetchSQLiteSrcError;
    }

    try out.writeAll(body.items);
}

fn fatal(comptime msg: []const u8, args: anytype) noreturn {
    std.debug.print(msg, args);
    std.process.exit(1);
}
