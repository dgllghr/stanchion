const build_options = @import("build_options");

pub const c = if (build_options.loadable_extension)
    @import("c/loadable_extension.zig")
else
    @cImport({
        @cInclude("sqlite3.h");
        // TODO remove when issue is resolved:
        //      https://github.com/ziglang/zig/issues/15893
        @cInclude("result-transient.h");
    });

/// versionGreaterThanOrEqualTo returns true if the SQLite version is >= to the
/// major.minor.patch provided.
pub fn versionGreaterThanOrEqualTo(major: u8, minor: u8, patch: u8) bool {
    return c.SQLITE_VERSION_NUMBER >=
        @as(u32, major) * 1000000 + @as(u32, minor) * 1000 + @as(u32, patch);
}

comptime {
    if (!versionGreaterThanOrEqualTo(3, 21, 0)) {
        @compileError("must use SQLite >= 3.21.0");
    }
}
