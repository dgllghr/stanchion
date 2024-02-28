#!/bin/bash

set -e

# Arg is sqlite bin path to use
sqlite3=$1
stanchionlib=$2

echo "$stanchionlib"

# Account for differences between linux mktemp and macos mktemp programs
testdir=$(mktemp -d 2>/dev/null || mktemp -d -t 'stanchion-itest')
mkdir -p "$testdir/expected"
mkdir -p "$testdir/results"

TESTS=$(ls test/tests | cut -f 1 -d '.')

for test_name in $TESTS; do
    echo ""
    echo "RUNNING $test_name"

    # Generate the expected output
    $sqlite3 ":memory:" \
        ".mode csv" \
        ".read test/schema_expected.sql" \
        ".read test/tests/$test_name.sql" \
        > "$testdir/expected/$test_name.csv"

    # Run the test
    $sqlite3 ":memory:" \
        ".load $stanchionlib" \
        ".mode csv" \
        ".read test/schema.sql" \
        ".read test/tests/$test_name.sql" \
        > "$testdir/results/$test_name.csv"

    # diff succeeds if the files are the same, fails if they are different
    diff -b "$testdir/results/$test_name.csv" "$testdir/expected/$test_name.csv"

    echo "DONE"
    echo ""
done;

rm -r "$testdir/expected"
rm -r "$testdir/results"