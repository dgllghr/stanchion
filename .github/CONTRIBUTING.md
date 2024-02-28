Thank you for your interest in contributing to stanchion!

One of the easiest ways you can contribute to stanchion is to star the project and share it with others. Having users is the best way for a project like stanchion to grow.

Another great way to help is to test stanchion. Stanchion benefits from being tested across a large number of environments and use cases, so if you have an idea of how you might want to use stanchion, please try it out and file issues for any bugs, difficulties, or missing features you come across. This will help prioritize the roadmap and get stanchion production ready for your use case.

Please also file issues for any ideas or questions you have for or about the project.

Stanchion is a young project and isn't ready for code contributions for new features from the community, yet. However, the following code contributions are all very welcome:

* Bug fixes
* Additional tests
* Performance optimizations
* Code cleanups

## Edit Code

You will need to [install Zig (master)](https://ziglang.org/learn/getting-started/#installing-zig) and clone the `stanchion` repository. Then clone the stanchion repo and run `zig build test` to ensure zig is installed and stanchion compiles.

For Zig editor and tool support, see [Zig Tools](https://ziglang.org/learn/tools/).

When making changes to stanchion, there are a few commands that are useful for testing and debugging purposes:

* [zig build test]: run unit tests
* [zig build itest]: run integration tests
* [zig build ext]: build the runtime loadable extension as a dynamic library which can be found in the `zig-out/lib` folder

Once your changes are ready, open a pull requests against the stanchion GitHub repository.

### Test across SQLite versions

By default, tests use the system SQLite library. However, stanchion's build can optionally download and compile a specific version of SQLite and use that version when running tests. Pass `-Dsqlite-test-version=$SQLITE_VERSION` to the build for unit and integration tests. For example:

```
zig build test -Dsqlite-test-version=3.38.5
```

It is also possible to launch a SQLite shell for any version of SQLite (a convenience feature for debugging):

```
zig build sqlite-shell -Dsqlite-test-version=3.43.2
```
