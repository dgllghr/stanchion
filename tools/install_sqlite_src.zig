const std = @import("std");
const fmt = std.fmt;
const fs = std.fs;
const mem = std.mem;
const Allocator = mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    // Args are version and output file path
    const args = try std.process.argsAlloc(arena.allocator());
    if (args.len != 4) fatal("wrong number of arguments", .{});
    const amalgamation_dir_path = args[1];
    const src_file_dst_path = args[2];
    const shell_dst_path = args[3];

    copyFiles(amalgamation_dir_path, src_file_dst_path, shell_dst_path) catch |e| {
        fatal("error copying files: {!}", .{e});
    };
}

fn copyFiles(
    amalgamation_dir_path: []const u8,
    src_file_dst_path: []const u8,
    shell_dst_path: []const u8,
) !void {
    const cwd = fs.cwd();
    var amalgamation_dir = try cwd.openDir(amalgamation_dir_path, .{});
    defer amalgamation_dir.close();
    var src_file_dst_dir = try cwd.openDir(fs.path.dirname(src_file_dst_path).?, .{});
    defer src_file_dst_dir.close();
    var shell_dst_dir = try cwd.openDir(fs.path.dirname(shell_dst_path).?, .{});
    defer shell_dst_dir.close();

    try amalgamation_dir.copyFile(
        "sqlite3.c",
        src_file_dst_dir,
        fs.path.basename(src_file_dst_path),
        .{},
    );
    try amalgamation_dir.copyFile(
        "shell.c",
        shell_dst_dir,
        fs.path.basename(shell_dst_path),
        .{},
    );
}

fn fatal(comptime format: []const u8, args: anytype) noreturn {
    std.debug.print(format, args);
    std.process.exit(1);
}
