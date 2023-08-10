# Stanchion

Column-oriented tables in SQLite.

Stanchion is a SQLite extension that makes it possible to have column-oriented
tables using SQLite's [virtual table](https://www.sqlite.org/vtab.html)
mechanism.

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
    (1, 'Beholder', 'ABERRATION' 4, 13),
    (2, 'Gelatinous Cube', 'OOZE', 4, 2),
    (3, 'Mimic', 'MONSTROSITY', 3, 2),
    (4, 'Lich', 'UNDEAD' 3, 21);

-- Because the `dnd_monsters` table is column-oriented, the following query
-- only reads data from the `name`, `type`, and `challenge_rating` columns.
-- Data in the `id` and `size` columns are not accessed or scanned at all!
SELECT name
FROM dnd_monsters
WHERE type = 'UNDEAD' AND challenge_rating >= 18;
```

## Differences from SQLite

### `BOOLEAN` type

Stanchion has a dedicated `BOOLEAN` type. Boolean values are used within
stanchion as part of each nullable segment (see
[Row Groups and Segments](#row-groups-segments-and-stripes)). Specialized encodings
exist for boolean types, and they will be added to stanchion in the future. The
dedicated boolean type makes it simpler to utilize these dedicated encodings.

SQLite uses `INTEGER` to represent booleans. When stanchion passes boolean data
out of the virtual table, it uses an integer so that stanchion's booleans are
compatible. Likewise, integer values are converted to booleans when passed into
the virtual table.

### Clustered index by `SORT KEY`

The order of the records in the table (aka the clustered index) is controlled
by the `SORT KEY`. The `SORT KEY` is unlike a `PRIMARY KEY` in that it does not
enforce uniqueness. Currently, every table in stanchion must have an explicit
sort key.

### No uniqueness (`UNIQUE` or `PRIMARY KEY`) or foreign key constraints 

This may change in the future, but will likely require external indexes. As a
first step, it may be a good idea to allow declaring these constraints for 
documentation purposes without enforcing them (similar to what Snowflake does).
Enforcing these constraints will likley be implemented separately. When/if a
`PRIMARY KEY` is introduced, it will likley make sense to follow the lead of 
Clickhouse's `MergeTree` engine in that the `PRIMARY KEY`
[must be a prefix](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree#choosing-a-primary-key-that-differs-from-the-sorting-key)
of the `SORT KEY`.

### No external indexes

There is currently no external index mechanism. See [Indexing](#indexing) for
details on how stanchion indexes data and what queries can be made more
efficient by those indexes.

## Build

[Install Zig](https://ziglang.org/learn/getting-started/#installing-zig) and
clone the `stanchion` repository.

```bash
cd /path/to/stanchion
zig build ext
```

The SQLite extension is the dynamic library in the `zig-out` directory. The
name varies depending on the platform but it is `libstanchion.dylib` on macos.

## Data storage

### Row groups, segments, and stripes

Records in each table are segmented into row groups. Each row group contains up
to a fixed number of records. Row groups are indexed by the minimum sort key
value within the row group, and records are placed in a row group by finding
the row group with the largest starting sort key value less than or equal to
the record's sort key value. When more records are added to a row group than
the maximum size, the row group is split. In the future, row groups will also
be merged when they fall below a size threshold.

A row group is also made up of multiple segments. A segment contains data for a
single column. Think of each row group as a span of data within a table and
each segment as a span of data within a column.

Each segment is composed of 1 or more stripes. A stripe is a chunk of data of
the same type. Row groups and segments are logical containers, while each
stripe is stored in a single SQLite `BLOB`. Within the internals of SQLite,
a blob is [made up of 1 or more pages](https://www.sqlite.org/fileformat.html).

A segment can contain the following stripes: present, primary, and secondary.
When a segment contains null values, a stripe is used to indicate whether the
value in each record is not `NULL`. This is called the present stripe. For
`BOOLEAN`, `INTEGER`, and `FLOAT` columns, the values are stored in the primary
stripe. For `TEXT` and `BLOB` columns, the bytes of the values are stored in
the primary stripe, and the length of each value is stored in the secondary
stripe.

Because SQLite blobs cannot be resized (and because not resizing blobs is good
for efficiency), stripes are allocated in large chunks (e.g. 50 KB). A stripe
contains 2 sections: the values section and the message log section. The values
sections contains all the encoded values in the stripe. Modifications to the
stripe (inserts, updates, and deletes) are recorded by appending a message to
the end of the message log. A message contains the type of modification, the
index to which the modification is applied, and the values (for inserts). When
the message log becomes too big to fit into the blob, the messages are applied
to the encoded values, the optimal encoding for the result is calculated, and
all the values are encoded and written to the values section (and the message
log is cleared). If the resulting values are too large for the blob, a new blob
is allocated with sufficient capacity + additional space, the values are
written to the new blob, and the segment is updated with the reference to the
new stripe.

### Indexing

Currently, stanchion has only one index mechanism: the starting sort key of
each row group. This index is used to insert records into the appropriate row
group. It will also be used to select which row groups to access when a query
filters on the sort key or a prefix of the sort key when querying is
implemented.

In the future, each segment will also contain a minimum and maximum value of
data in the segment so that segments for all columns can be skipped if filters
on those columns have values outside the minimum - maximum range.
