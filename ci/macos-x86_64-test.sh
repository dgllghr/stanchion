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

## Sqlite
# macos dropped support for 32 bit executables so do not install sqlite if a 64 bit executable is
# not available and don't run the integration tests (where the sqlite installation is used)
sqlite32bit=0
if [ ! -f ~/deps/SQLITE32BIT ]; then
    echo ">> Downloading and installing SQLite (${SQLITE_VERSION})"

    rm -rf ~/deps/sqlite
    mkdir ~/deps/sqlite

    IFS='.' read -r maj min pat <<EOF
$SQLITE_VERSION
EOF
    if [ ${#min} = 1 ]; then
        min="0${min}"
    fi
    if [ ${#pat} = 1 ]; then
        pat="0${pat}"
    fi
    versionstring="${maj}${min}${pat}00"

    sqliteurl="https://www.sqlite.org/$SQLITE_YEAR/sqlite-tools-osx-x64-$versionstring.zip"
    # See if it comes in 64 bit
    wget -q --spider --tries 1 "$sqliteurl" || sqlite32bit=$?
    if [ $sqlite32bit -eq 0 ]; then
        wget -qO ~/deps/sqlite.zip "$sqliteurl"
        unzip -j -d ~/deps/sqlite ~/deps/sqlite.zip

        rm ~/deps/sqlite.zip
    fi

    echo "$sqlite32bit" > ~/deps/SQLITE32BIT
else
    echo ">> Using cached SQLite"
    sqlite32bit=$(cat ~/deps/SQLITE32BIT)
fi

PATH="$HOME/deps/sqlite:$PATH"
sqlite3 --version

####
## Checks
####

## Lint
zig fmt --check .

## Unit tests
zig build test --summary all

## Integration tests
# macos dropped support for 32 bit executables so do not run the integration tests if 64
# bit sqlite is not available
if [ $sqlite32bit -eq 0 ]; then
    zig build itest --summary all
fi

exit 0