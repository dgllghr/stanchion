const std = @import("std");
const Build = std.Build;
const Compile = Build.Step.Compile;
const CrossTarget = std.zig.CrossTarget;
const OptimizeMode = std.builtin.OptimizeMode;

pub fn buildLoadableExt(b: *Build, target: CrossTarget, optimize: OptimizeMode) *Compile {
    const loadable_ext = b.addSharedLibrary(.{
        .name = "stanchion",
        .root_source_file = .{ .path = "src/main.zig" },
        .target = target,
        .optimize = optimize,
    });
    // TODO remove when issue is resolved:
    //      https://github.com/ziglang/zig/issues/7935
    if (target.cpu_arch == .x86) {
        loadable_ext.link_z_notext = true;
    }
    loadable_ext.force_pic = true;
    // TODO remove when issue is resolved:
    //      https://github.com/ziglang/zig/issues/15893
    loadable_ext.addCSourceFile(.{
        .file = .{ .path = "src/sqlite3/c/result-transient-ext.c" },
        .flags = &[_][]const u8{"-std=c99"},
    });
    loadable_ext.addIncludePath(.{ .path = "src/sqlite3/c" });

    const loadable_ext_options = b.addOptions();
    loadable_ext.addOptions("build_options", loadable_ext_options);
    loadable_ext_options.addOption(bool, "loadable_extension", true);

    return loadable_ext;
}

// Although this function looks imperative, note that its job is to
// declaratively construct a build graph that will be executed by an external
// runner.
pub fn build(b: *Build) void {
    // Standard target options allows the person running `zig build` to choose
    // what target to build for. Here we do not override the defaults, which
    // means any target is allowed, and the default is native. Other options
    // for restricting supported target set are available.
    const target = b.standardTargetOptions(.{});

    // Standard optimization options allow the person running `zig build` to select
    // between Debug, ReleaseSafe, ReleaseFast, and ReleaseSmall. Here we do not
    // set a preferred release mode, allowing the user to decide how to optimize.
    const optimize = b.standardOptimizeOption(.{});

    // Loadable extension

    const loadable_ext = buildLoadableExt(b, target, optimize);
    const install_loadable_ext = b.addInstallArtifact(loadable_ext, .{});

    const loadable_ext_build = b.step("ext", "Build the 'stanchion' sqlite loadable extension");
    loadable_ext_build.dependOn(&install_loadable_ext.step);

    // Unit tests

    const main_tests = b.addTest(.{
        .root_source_file = .{ .path = "src/tests.zig" },
        .target = target,
        .optimize = optimize,
    });
    main_tests.linkLibC();
    main_tests.addCSourceFile(.{
        .file = .{ .path = "src/sqlite3/c/sqlite3.c" },
        .flags = &[_][]const u8{"-std=c99"},
    });
    main_tests.addIncludePath(.{ .path = "src/sqlite3/c" });
    const main_tests_options = b.addOptions();
    main_tests.addOptions("build_options", main_tests_options);
    main_tests_options.addOption(bool, "loadable_extension", false);

    const run_main_tests = b.addRunArtifact(main_tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_main_tests.step);

    // Integration tests

    const run_integration_tests = b.addSystemCommand(&[_][]const u8{
        "bash", "test/runtest.sh",
    });
    // Build the loadable extension and then run the integration test script
    const loadable_ext_itest = buildLoadableExt(b, target, .ReleaseFast);
    const install_loadable_ext_itest = b.addInstallArtifact(loadable_ext_itest, .{});
    const integration_test_step = b.step("itest", "Run integration tests");
    run_integration_tests.step.dependOn(&install_loadable_ext_itest.step);
    integration_test_step.dependOn(&run_integration_tests.step);

    // Benchmarks

    const benches = b.addExecutable(.{
        .name = "stanchion_benches",
        .root_source_file = .{ .path = "src/bench.zig" },
        .target = target,
        .optimize = .ReleaseFast,
    });
    benches.linkLibC();
    benches.addCSourceFile(.{
        .file = .{ .path = "src/sqlite3/c/sqlite3.c" },
        .flags = &[_][]const u8{"-std=c99"},
    });
    benches.addIncludePath(.{ .path = "src/sqlite3/c" });
    const benches_options = b.addOptions();
    benches.addOptions("build_options", benches_options);
    benches_options.addOption(bool, "loadable_extension", false);

    const run_benches = b.addRunArtifact(benches);
    const bench_step = b.step("bench", "Run benchmarks");
    bench_step.dependOn(&run_benches.step);
}
