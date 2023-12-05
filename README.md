# Stanchion

Column-oriented tables in SQLite.

Stanchion is a SQLite extension that makes it possible to have column-oriented tables using SQLite's [virtual table](https://www.sqlite.org/vtab.html) mechanism.

## Status

Stanchion is in *alpha*. The storage format may change in backwards incompatible ways. **Do not use this in production**... yet.

## Usage

```sql
.load ./stanchion

CREATE VIRTUAL TABLE dnd_monsters
USING stanchion (
    id INTEGER NOT NULL,
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    size INTEGER NOT NULL,
    challenge_rating FLOAT NOT NULL,
    SORT KEY (id)
);

INSERT INTO dnd_monsters (id, name, type, size, challenge_rating)
VALUES
    (1, 'Beholder', 'ABERRATION', 4, 13),
    (2, 'Gelatinous Cube', 'OOZE', 4, 2),
    (3, 'Mimic', 'MONSTROSITY', 3, 2),
    (4, 'Lich', 'UNDEAD', 3, 21);

-- Because the `dnd_monsters` table is column-oriented, the following query
-- only reads data from the `name`, `type`, and `challenge_rating` columns.
-- Data in the `id` and `size` columns are not accessed or scanned at all!
SELECT name
FROM dnd_monsters
WHERE type = 'UNDEAD' AND challenge_rating >= 18;
```

## Build

[Install Zig (master)](https://ziglang.org/learn/getting-started/#installing-zig) and clone the `stanchion` repository. Then run:

```shell
zig build ext
```

The SQLite extension is the dynamic library named `libstanchion` in the `zig-out` directory.

## Differences from SQLite

### `BOOLEAN` type

Stanchion has a dedicated `BOOLEAN` type. Boolean values are used within stanchion as part of each nullable segment (see [Row Groups and Segments](#row-groups-segments-and-stripes)) and exposed so it can also be used directly.

SQLite uses `INTEGER` to represent booleans. Stanchion converts `BOOLEAN` values to from `INTEGER` values when passed through SQLite. Querying a `BOOLEAN` stanchion column returns `INTEGER` SQLite values.

### Clustered index by `SORT KEY`

In Stanchion, the order of the records in the table (aka the clustered index) is controlled by the `SORT KEY`. Currently, every table in stanchion must have an explicit `SORT KEY` made up of 1 or more columns. It is declared when the table is created and cannot be changed. Unlike a `PRIMARY KEY`, it does not enforce uniqueness.

This differs from SQLite where tables are sorted by the `ROWID` by default or by the `PRIMARY KEY` if the table is a `WITHOUT ROWID` table.

### No uniqueness (`UNIQUE` or `PRIMARY KEY`) or foreign key constraints 

This may change in the future. Implementing these will likely require external indexes. When/if a `PRIMARY KEY` is introduced, it will likley make sense to follow the lead of Clickhouse's `MergeTree` engine and require that the `PRIMARY KEY` [must be a prefix](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree#choosing-a-primary-key-that-differs-from-the-sorting-key) of the `SORT KEY`.

### No external indexes

There is currently no external index mechanism. See [The primary index](#the-primary-index) for details on how stanchion indexes data and what queries can be made more efficient by those indexes.

### Column types are enforced

Values being inserted into Stanchion tables must be of the column's declared type. This is equivalent to declaring a SQLite table with the `STRICT` table option.

## Data storage

### Row groups, segments, and stripes

Records in each table are grouped into row groups. Each row group contains up to a fixed number of records. Row groups are logical (not physical) groupings and are indexed by the minimum sort key value within the row group. Currently, row groups are immutable and are constructed from a batch of newly inserted records (and possibly values from an existing row group) when the number of inserted records exceeds a threshold.

A row group is made up of multiple segments. A segment contains data for a single column. Think of each row group as a span of data within a table and each segment as a span of data within a column. A segment is backed by a single SQLite `BLOB` value. Currently, segments are immutable.

Each segment is composed of 1 or more stripes. A stripe is a chunk of data of the same type that is backed by a portion (slice) of the segment's `BLOB` value. A segment can contain the following stripes: present, primary, and length. When a segment contains null values, the primary stripe is used to indicate whether the value in each record is not `NULL`. For `BOOLEAN`, `INTEGER`, and `FLOAT` columns, the values are stored in the primary stripe. For `TEXT` and `BLOB` columns, the bytes of the values are stored in the primary stripe, and the length of each value is stored in the length stripe.

### The primary index

The primary index is a persistent B+ Tree (read: row-oriented SQLite table) that indexes the row groups by the starting (min) sort key of each row group.

The primary index also contains newly inserted records, which are called pending inserts. Pending inserts are sorted by the record's sort key along with the min sort keys of the indexed row groups. Row groups are created by merging runs of pending inserts within the primary index with the preceding row group.

When a query filters on sort key columns, Stanchion applies that filter to the primary index to restrict which row groups are accessed and which pending inserts are scanned. This is the only indexing mechanism currently supported by Stanchion.

