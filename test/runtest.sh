#!/bin/bash

set -e

mkdir -p zig-out/test/expected
mkdir -p zig-out/test/results

TESTS=$(ls test/tests | cut -f 1 -d '.')

for test_name in $TESTS; do
    echo ""
    echo $test_name
    echo ""

    # Generate the expected output
    sqlite3 ":memory:" \
        ".mode csv" \
        ".read test/schema_expected.sql" \
        ".read test/tests/$test_name.sql" \
        > "zig-out/test/expected/$test_name.csv"

    # Run the test
    sqlite3 ":memory:" \
        ".load zig-out/lib/libstanchion" \
        ".mode csv" \
        ".read test/schema.sql" \
        ".read test/tests/$test_name.sql" \
        > "zig-out/test/results/$test_name.csv"

    # diff succeeds if the files are the same, fails if they are different
    diff -b "zig-out/test/expected/$test_name.csv" "zig-out/test/results/$test_name.csv"
done;
