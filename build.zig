const std = @import("std");
const Build = std.Build;
const Compile = Build.Step.Compile;
const OptimizeMode = std.builtin.OptimizeMode;

const InstallSqliteSrc = struct {
    include_dir: Build.LazyPath,
    src_file: Build.LazyPath,
    shell_src_file: Build.LazyPath,
};

// Although this function looks imperative, note that its job is to
// declaratively construct a build graph that will be executed by an external
// runner.
pub fn build(b: *Build) void {
    // If not specified, will attempt to use system sqlite
    const sqlite_version = b.option(
        []const u8,
        "sqlite-test-version",
        "Version of sqlite to use for testing",
    );

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

    var install_sqlite_src: ?InstallSqliteSrc = null;
    var build_sqlite_shell: ?*Build.Step.Compile = null;
    if (sqlite_version) |version| {
        // Install SQLite src

        const download_sqlite_src = b.addExecutable(.{
            .name = "download_sqlite_src",
            .root_source_file = .{ .path = "tools/download_sqlite_src.zig" },
            .target = b.host,
        });
        const run_dl_sqlite_src = b.addRunArtifact(download_sqlite_src);
        run_dl_sqlite_src.addArg(version);
        const sqlite_src_zip_name = b.fmt("sqlite-{s}.zip", .{version});
        const sqlite_src_zip = run_dl_sqlite_src.addOutputFileArg(sqlite_src_zip_name);

        const unzip_sqlite_src = b.addSystemCommand(&.{ "unzip", "-oj" });
        unzip_sqlite_src.addFileArg(sqlite_src_zip);
        unzip_sqlite_src.addArg("-d");
        const sqlite_src_dir_name = b.fmt("sqlite-{s}", .{version});
        const sqlite_src_dir = unzip_sqlite_src.addOutputFileArg(sqlite_src_dir_name);

        const install_sqlite_step = b.step("install-sqlite", "Install SQLite");
        install_sqlite_step.dependOn(&unzip_sqlite_src.step);

        const inst_sqlite_src = b.addExecutable(.{
            .name = "install_sqlite_src",
            .root_source_file = .{ .path = "tools/install_sqlite_src.zig" },
            .target = b.host,
        });
        const run_install_sqlite_src = b.addRunArtifact(inst_sqlite_src);
        run_install_sqlite_src.addDirectoryArg(sqlite_src_dir);
        const sqlite_src_file = run_install_sqlite_src.addOutputFileArg("sqlite3.c");
        const shell_src_file = run_install_sqlite_src.addOutputFileArg("shell.c");

        install_sqlite_src = .{
            .include_dir = sqlite_src_dir,
            .src_file = sqlite_src_file,
            .shell_src_file = shell_src_file,
        };

        // SQLite shell

        const sqlite_shell = b.addExecutable(.{
            .name = "sqlite_shell",
            .target = target,
            .optimize = .ReleaseFast,
        });
        sqlite_shell.linkLibC();
        sqlite_shell.addCSourceFile(.{
            .file = install_sqlite_src.?.src_file,
            .flags = &[_][]const u8{"-std=c99"},
        });
        sqlite_shell.addCSourceFile(.{
            .file = install_sqlite_src.?.shell_src_file,
        });
        sqlite_shell.addIncludePath(install_sqlite_src.?.include_dir);

        build_sqlite_shell = sqlite_shell;

        const run_sqlite_shell = b.addRunArtifact(sqlite_shell);
        const sqlite_shell_step = b.step("sqlite-shell", "Run sqlite shell");
        sqlite_shell_step.dependOn(&run_sqlite_shell.step);
    }

    // Unit tests

    const main_tests = b.addTest(.{
        .root_source_file = .{ .path = "src/tests.zig" },
        .target = target,
        .optimize = optimize,
    });
    main_tests.linkLibC();
    if (install_sqlite_src) |install_src| {
        main_tests.addCSourceFile(.{
            .file = install_src.src_file,
            .flags = &[_][]const u8{"-std=c99"},
        });
        main_tests.addIncludePath(install_src.include_dir);
    } else {
        main_tests.linkSystemLibrary("sqlite3");
        main_tests.addCSourceFile(.{
            .file = .{ .path = "src/sqlite3/c/result-transient.c" },
            .flags = &[_][]const u8{"-std=c99"},
        });
    }
    main_tests.addIncludePath(.{ .path = "src/sqlite3/c" });
    const main_tests_options = b.addOptions();
    main_tests.root_module.addOptions("build_options", main_tests_options);
    main_tests_options.addOption(bool, "loadable_extension", false);

    const run_main_tests = b.addRunArtifact(main_tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_main_tests.step);

    // Integration tests

    // Build the loadable extension in debug mode
    const loadable_ext_itest = buildLoadableExt(b, target, .Debug);
    const run_integration_tests = b.addSystemCommand(&[_][]const u8{ "bash", "test/runtest.sh" });
    if (build_sqlite_shell) |bss| {
        run_integration_tests.addFileArg(bss.getEmittedBin());
    } else {
        run_integration_tests.addArg("sqlite3");
    }
    run_integration_tests.addFileArg(loadable_ext_itest.getEmittedBin());
    const integration_test_step = b.step("itest", "Run integration tests");
    integration_test_step.dependOn(&run_integration_tests.step);

    // Benchmarks

    const benches = b.addExecutable(.{
        .name = "stanchion_benches",
        .root_source_file = .{ .path = "src/bench.zig" },
        .target = target,
        .optimize = .ReleaseFast,
    });
    benches.linkLibC();
    if (install_sqlite_src) |install_src| {
        benches.addCSourceFile(.{
            .file = install_src.src_file,
            .flags = &[_][]const u8{"-std=c99"},
        });
        benches.addIncludePath(install_src.include_dir);
    } else {
        benches.linkSystemLibrary("sqlite3");
        benches.addCSourceFile(.{
            .file = .{ .path = "src/sqlite3/c/result-transient.c" },
            .flags = &[_][]const u8{"-std=c99"},
        });
    }
    benches.addIncludePath(.{ .path = "src/sqlite3/c" });
    const benches_options = b.addOptions();
    benches.root_module.addOptions("build_options", benches_options);
    benches_options.addOption(bool, "loadable_extension", false);

    const run_benches = b.addRunArtifact(benches);
    const bench_step = b.step("bench", "Run benchmarks");
    bench_step.dependOn(&run_benches.step);
}

pub fn buildLoadableExt(b: *Build, target: Build.ResolvedTarget, optimize: OptimizeMode) *Compile {
    const loadable_ext = b.addSharedLibrary(.{
        .name = "stanchion",
        .root_source_file = .{ .path = "src/main.zig" },
        .target = target,
        .optimize = optimize,
    });
    // TODO remove when issue is resolved:
    //      https://github.com/ziglang/zig/issues/7935
    if (target.result.cpu.arch == .x86) {
        loadable_ext.link_z_notext = true;
    }
    // TODO should this be added back somehow?
    //loadable_ext.force_pic = true;
    // TODO remove when issue is resolved:
    //      https://github.com/ziglang/zig/issues/15893
    loadable_ext.addCSourceFile(.{
        .file = .{ .path = "src/sqlite3/c/result-transient-ext.c" },
        .flags = &[_][]const u8{"-std=c99"},
    });
    loadable_ext.addIncludePath(.{ .path = "src/sqlite3/c" });

    const loadable_ext_options = b.addOptions();
    loadable_ext.root_module.addOptions("build_options", loadable_ext_options);
    loadable_ext_options.addOption(bool, "loadable_extension", true);

    return loadable_ext;
}
