const std = @import("std");

// Although this function looks imperative, note that its job is to
// declaratively construct a build graph that will be executed by an external
// runner.
pub fn build(b: *std.Build) void {
    // Standard target options allows the person running `zig build` to choose
    // what target to build for. Here we do not override the defaults, which
    // means any target is allowed, and the default is native. Other options
    // for restricting supported target set are available.
    const target = b.standardTargetOptions(.{});

    // Standard optimization options allow the person running `zig build` to select
    // between Debug, ReleaseSafe, ReleaseFast, and ReleaseSmall. Here we do not
    // set a preferred release mode, allowing the user to decide how to optimize.
    const optimize = b.standardOptimizeOption(.{});

    const lib = b.addStaticLibrary(.{
        .name = "stanchion",
        // In this case the main source file is merely a path, however, in more
        // complicated build scripts, this could be a generated file.
        .root_source_file = .{ .path = "src/main.zig" },
        .target = target,
        .optimize = optimize,
    });
    lib.addCSourceFile(.{
        .file = .{ .path = "src/sqlite3/c/sqlite3.c" },
        .flags = &[_][]const u8{"-std=c99"},
    });
    // TODO remove when issue is resolved:
    //      https://github.com/ziglang/zig/issues/15893
    lib.addCSourceFile(.{
        .file = .{ .path = "src/sqlite3/c/result-transient.c" },
        .flags = &[_][]const u8{"-std=c99"},
    });
    lib.addIncludePath(.{ .path = "src/sqlite3/c" });
    lib.linkLibC();
    const lib_options = b.addOptions();
    lib.addOptions("build_options", lib_options);
    // TODO this assumes that a loadable extension that is statically linked still uses
    //      the loadable extension API. Check this assumption
    lib_options.addOption(bool, "loadable_extension", true);

    // This declares intent for the library to be installed into the standard
    // location when the user invokes the "install" step (the default step when
    // running `zig build`).
    b.installArtifact(lib);

    const loadable_ext = b.addSharedLibrary(.{
        .name = "stanchion",
        .root_source_file = .{ .path = "src/main.zig" },
        .target = target,
        .optimize = optimize,
    });
    loadable_ext.force_pic = true;
    // TODO remove when issue is resolved:
    //      https://github.com/ziglang/zig/issues/15893
    lib.addCSourceFile(.{
        .file = .{ .path = "src/sqlite3/c/result-transient-ext.c" },
        .flags = &[_][]const u8{"-std=c99"},
    });
    loadable_ext.addIncludePath(.{ .path = "src/sqlite3/c" });
    // TODO add anonymous module?
    //      https://github.com/vrischmann/zig-sqlite/blob/193240aa5ecc9c9b14b25dec662505d5cf9f5340/build.zig#L350
    loadable_ext.linkLibrary(lib);
    const loadable_ext_options = b.addOptions();
    loadable_ext.addOptions("build_options", loadable_ext_options);
    loadable_ext_options.addOption(bool, "loadable_extension", true);

    const install_loadable_ext = b.addInstallArtifact(loadable_ext, .{});

    const loadable_ext_build = b.step("ext", "Build the 'stanchion' sqlite loadable extension");
    loadable_ext_build.dependOn(&install_loadable_ext.step);

    // Creates a step for unit testing. This only builds the test executable
    // but does not run it.
    const main_tests = b.addTest(.{
        .root_source_file = .{ .path = "src/tests.zig" },
        .target = target,
        .optimize = optimize,
    });
    main_tests.addCSourceFile(.{
        .file = .{ .path = "src/sqlite3/c/sqlite3.c" },
        .flags = &[_][]const u8{"-std=c99"},
    });
    main_tests.addIncludePath(.{ .path = "src/sqlite3/c" });
    const main_tests_options = b.addOptions();
    main_tests.addOptions("build_options", main_tests_options);
    main_tests_options.addOption(bool, "loadable_extension", false);

    const run_main_tests = b.addRunArtifact(main_tests);

    const benches = b.addExecutable(.{
        .name = "stanchion_benches",
        .root_source_file = .{ .path = "src/bench.zig" },
        .target = target,
        // This may be too aggressive but the speed up is significant for scanning the
        // message log (3-5x improvement on my machine)
        .optimize = .ReleaseFast,
    });
    const benches_options = b.addOptions();
    benches.addOptions("build_options", benches_options);
    benches_options.addOption(bool, "loadable_extension", false);

    const run_benches = b.addRunArtifact(benches);

    // This creates a build step. It will be visible in the `zig build --help` menu,
    // and can be selected like this: `zig build test`
    // This will evaluate the `test` step rather than the default, which is "install".
    const test_step = b.step("test", "Run library tests");
    test_step.dependOn(&run_main_tests.step);

    // Benchmark build step
    const bench_step = b.step("bench", "Run benchmarks");
    bench_step.dependOn(&run_benches.step);
}
