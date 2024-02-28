#!/bin/sh

set -x
set -e

## Requires
## ---
## wget
## tar
## jq
## unzip
##
## Env Vars
## ---
## SQLITE_VERSION
## SQLITE_YEAR

# TODO check that required environment variables are set

####
## Dependencies
####

mkdir -p ~/deps

## Zig (master)
wget -qO ~/deps/zig-version-index.json 'https://ziglang.org/download/index.json'
latestversion=$(cat ~/deps/zig-version-index.json | jq -r '.master.version')
existingversion=$([ ! -f ~/deps/ZIGVERSION ] || cat ~/deps/ZIGVERSION)
if [ "$latestversion" != "$existingversion" ]; then
    echo ">> Downloading and installing zig (master)"

    rm -rf ~/deps/zig
    mkdir ~/deps/zig

    zigurl=$(cat ~/deps/zig-version-index.json | jq -r '.master."x86_64-macos".tarball')
    wget -qO ~/deps/zig.tar.xz "$zigurl"
    tar -xf ~/deps/zig.tar.xz --strip-components=1 -C ~/deps/zig

    echo "$latestversion" > ~/deps/ZIGVERSION

    rm ~/deps/zig.tar.xz
else
    echo ">> Using cached zig"
fi
rm ~/deps/zig-version-index.json

PATH="$HOME/deps/zig:$PATH"
zig version

####
## Checks
####

## Lint
zig fmt --check .

## Unit tests
zig build test -Dsqlite-test-version=${SQLITE_VERSION} --summary all

## Integration tests
zig build itest -Dsqlite-test-version=${SQLITE_VERSION} --summary all

exit 0