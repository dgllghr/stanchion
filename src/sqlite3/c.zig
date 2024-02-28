const build_options = @import("build_options");

pub const c = if (build_options.loadable_extension)
    @import("c/loadable_extension.zig")
else
    @cImport({
        @cInclude("sqlite3.h");
    });

pub const headerVersionNumber: u32 = @intCast(c.SQLITE_VERSION_NUMBER);

/// Returns the sqlite version encoded as a number
pub fn versionNumber() u32 {
    return @intCast(c.sqlite3_libversion_number());
}

/// Generates a sqlite encoded version number as an integer from major.minor.patch
pub fn encodeVersionNumber(major: u8, minor: u8, patch: u8) u32 {
    return @as(u32, major) * 1_000_000 + @as(u32, minor) * 1000 + @as(u32, patch);
}
