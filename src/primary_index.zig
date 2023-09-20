//! This whole system requires that all entries are assigned a rowid where later entries
//! have a higher rowid than earlier entries. This does limit a table to at most
//! 9,223,372,036,854,775,807 records. Row group entries are given the rowid of the first
//! record in the row group. The rowid is necessary because the sort key does not
//! guarantee uniqueness, so multiple row groups may have the same starting sort key.
//! Using a monotonic rowid ensures that later records are placed into the last row group
//! of a set of row groups with the same sort key.

const std = @import("std");
const fmt = std.fmt;
const mem = std.mem;
const Allocator = std.mem.Allocator;

const Conn = @import("sqlite3/Conn.zig");
const Stmt = @import("sqlite3/Stmt.zig");
const sqlite_c = @import("sqlite3/c.zig").c;

const StmtCell = @import("StmtCell.zig");

const s = @import("schema.zig");
const DataType = s.ColumnType.DataType;
const Schema = s.Schema;

pub const PrimaryIndex = struct {
    const Self = @This();

    allocator: Allocator,
    db: Db,
    next_rowid: i64,

    pub fn create(
        allocator: Allocator,
        conn: Conn,
        table_name: []const u8,
        schema: *const Schema,
    ) !Self {
        try Db.createTable(allocator, conn, table_name, schema);
        const db = try Db.init(allocator, conn, table_name, schema);
        return .{
            .allocator = allocator,
            .db = db,
            .next_rowid = 1,
        };
    }

    test "insert log: create" {
        const conn = try @import("sqlite3/Conn.zig").openInMemory();
        defer conn.close();

        const ArrayListUnmanaged = std.ArrayListUnmanaged;
        const Column = @import("schema/Column.zig");
        var columns = ArrayListUnmanaged(Column){};
        defer columns.deinit(std.testing.allocator);
        try columns.appendSlice(std.testing.allocator, &[_]Column{
            .{
                .rank = 0,
                .name = "foo",
                .column_type = .{ .data_type = .Blob, .nullable = false },
                .sk_rank = 0,
            },
            .{
                .rank = 1,
                .name = "bar",
                .column_type = .{ .data_type = .Integer, .nullable = true },
                .sk_rank = null,
            },
        });
        var sort_key = ArrayListUnmanaged(usize){};
        defer sort_key.deinit(std.testing.allocator);
        try sort_key.append(std.testing.allocator, 0);
        const schema = Schema{
            .columns = columns,
            .sort_key = sort_key,
        };

        var primary_index = try create(std.testing.allocator, conn, "test", &schema);
        defer primary_index.deinit();
    }

    pub fn open(
        allocator: Allocator,
        conn: Conn,
        table_name: []const u8,
        schema: *const Schema,
        next_rowid: i64,
    ) !Self {
        const db = try Db.init(allocator, conn, table_name, schema);
        return .{
            .allocator = allocator,
            .db = db,
            .next_rowid = next_rowid,
        };
    }

    pub fn deinit(self: *Self) void {
        self.db.deinit(self.allocator);
    }

    pub fn insert(self: *Self, values: anytype) !void {
        const rowid = self.next_rowid;
        self.next_rowid += 1;
        try self.db.insertInsertEntry(rowid, values);

        // TODO only do the check and merge at the end of a transaction

        // Check if the row group should be merged
        var row_group_handle = try self.db.findRowGroup(self.allocator, values, rowid);
        defer if (row_group_handle) |*rg| {
            rg.deinit(self.allocator);
        };

        const staged_inserts_count = try self.db.countRowGroupInserts(&row_group_handle);
        // TODO make this threshold configurable
        if (staged_inserts_count > 1000) {
            // TODO merge the row group
        }
    }

    test "insert log: log insert" {
        const OwnedRow = @import("value.zig").OwnedRow;
        const OwnedValue = @import("value.zig").OwnedValue;

        const conn = try @import("sqlite3/Conn.zig").openInMemory();
        defer conn.close();

        const ArrayListUnmanaged = std.ArrayListUnmanaged;
        const Column = @import("schema/Column.zig");
        var columns = ArrayListUnmanaged(Column){};
        defer columns.deinit(std.testing.allocator);
        try columns.appendSlice(std.testing.allocator, &[_]Column{
            .{
                .rank = 0,
                .name = "foo",
                .column_type = .{ .data_type = .Blob, .nullable = false },
                .sk_rank = 0,
            },
            .{
                .rank = 1,
                .name = "bar",
                .column_type = .{ .data_type = .Integer, .nullable = true },
                .sk_rank = null,
            },
        });
        var sort_key = ArrayListUnmanaged(usize){};
        defer sort_key.deinit(std.testing.allocator);
        try sort_key.append(std.testing.allocator, 0);
        const schema = Schema{
            .columns = columns,
            .sort_key = sort_key,
        };

        var primary_index = try create(std.testing.allocator, conn, "test", &schema);
        defer primary_index.deinit();

        var row = [_]OwnedValue{
            .{ .Blob = "magnitude" },
            .{ .Integer = 100 },
        };
        try primary_index.insert(OwnedRow{
            .rowid = null,
            .values = &row,
        });

        row = [_]OwnedValue{
            .{ .Blob = "amplitude" },
            .{ .Integer = 7 },
        };
        try primary_index.insert(OwnedRow{
            .rowid = null,
            .values = &row,
        });
    }
};

const Db = struct {
    const Self = @This();

    conn: Conn,
    table_name: []const u8,
    columns_len: usize,
    sort_key: []const usize,
    insert_entry: StmtCell,
    find_row_group: StmtCell,
    iterator: StmtCell,
    iterator_from_start: StmtCell,

    pub fn init(
        allocator: Allocator,
        conn: Conn,
        table_name: []const u8,
        schema: *const Schema,
    ) !Self {
        const columns_len = schema.columns.items.len;
        const sort_key = schema.sort_key.items;

        const insert_entry_query = try fmt.allocPrintZ(allocator,
            \\INSERT INTO "_stanchion_{s}_primary_index" (
            \\  entry_type, record_count, rowid, rowid_segment_id, {s}, {s}
            \\) VALUES (
            \\  ?, ?, ?, ?, {s}
            \\)
        , .{
            table_name,
            ColumnListFormatter("sk_value_{d}"){ .len = sort_key.len },
            ColumnListFormatter("col_{d}"){ .len = columns_len },
            ParameterListFormatter{ .len = columns_len + sort_key.len },
        });
        const insert_entry = StmtCell.init(insert_entry_query);

        const find_row_group_query = try fmt.allocPrintZ(allocator,
            \\  SELECT {s}, rowid, record_count
            \\  FROM "_stanchion_{s}_primary_index"
            \\  WHERE ({s}, rowid) <= ({s}, ?) AND entry_type = {d}
            \\  ORDER BY {s}, rowid DESC
            \\  LIMIT 1
        , .{
            ColumnListFormatter("sk_value_{d}"){ .len = sort_key.len },
            table_name,
            ColumnListFormatter("sk_value_{d}"){ .len = sort_key.len },
            ParameterListFormatter{ .len = sort_key.len },
            @intFromEnum(EntryType.RowGroup),
            ColumnListFormatter("sk_value_{d} DESC"){ .len = sort_key.len },
        });
        const find_row_group = StmtCell.init(find_row_group_query);

        const iterator_query = try fmt.allocPrintZ(allocator,
            \\SELECT entry_type, record_count, rowid, rowid_segment_id, {s}, {s}
            \\FROM "_stanchion_{s}_primary_index"
            \\WHERE ({s}, rowid) >= ({s}, ?)
            \\ORDER BY {s}, rowid DESC
        , .{
            ColumnListFormatter("sk_value_{d}"){ .len = sort_key.len },
            ColumnListFormatter("col_{d}"){ .len = columns_len },
            table_name,
            ColumnListFormatter("sk_value_{d}"){ .len = sort_key.len },
            ParameterListFormatter{ .len = sort_key.len },
            ColumnListFormatter("sk_value_{d} DESC"){ .len = sort_key.len },
        });
        const iterator = StmtCell.init(iterator_query);

        const iterator_from_start_query = try fmt.allocPrintZ(allocator,
            \\SELECT entry_type, record_count, rowid, rowid_segment_id, {s}, {s}
            \\FROM "_stanchion_{s}_primary_index"
            \\ORDER BY {s}, rowid DESC
        , .{
            ColumnListFormatter("sk_value_{d}"){ .len = sort_key.len },
            ColumnListFormatter("col_{d}"){ .len = columns_len },
            table_name,
            ColumnListFormatter("sk_value_{d} DESC"){ .len = sort_key.len },
        });
        const iterator_from_start = StmtCell.init(iterator_from_start_query);

        return .{
            .conn = conn,
            .table_name = table_name,
            .columns_len = columns_len,
            .sort_key = sort_key,
            .insert_entry = insert_entry,
            .find_row_group = find_row_group,
            .iterator = iterator,
            .iterator_from_start = iterator_from_start,
        };
    }

    pub fn deinit(self: *Self, allocator: Allocator) void {
        self.insert_entry.deinit(allocator);
        self.find_row_group.deinit(allocator);
        self.iterator.deinit(allocator);
        self.iterator_from_start.deinit(allocator);
    }

    pub fn insertInsertEntry(
        self: *Self,
        rowid: i64,
        values: anytype,
    ) !void {
        const stmt = try self.insert_entry.getStmt(self.conn);
        defer self.insert_entry.reset();

        try EntryType.Insert.bind(stmt, 1);
        try stmt.bindNull(2);
        try stmt.bind(.Int64, 3, rowid);
        try stmt.bindNull(4);
        for (self.sort_key, 5..) |rank, idx| {
            try values.readValue(rank).bind(stmt, idx);
        }
        const col_idx_start = self.sort_key.len + 5;
        cols: for (0..self.columns_len, col_idx_start..) |rank, idx| {
            // Do not duplicate sort key values
            for (self.sort_key) |sk_rank| {
                if (sk_rank == rank) {
                    continue :cols;
                }
            }
            try values.readValue(rank).bind(stmt, idx);
        }

        try stmt.exec();
    }

    pub fn insertRowGroupEntry(
        self: *Self,
        sort_key: anytype,
        row_group: *const RowGroup,
    ) !void {
        const stmt = try self.insert_entry.getStmt(self.conn);
        defer self.insert_entry.reset();

        try EntryType.RowGroup.bind(stmt, 1);
        try stmt.bind(.Int64, 2, @intCast(row_group.record_count));
        try stmt.bind(.Int64, 3, row_group.rowid);
        try stmt.bind(.Int64, 4, row_group.rowid_segment_id);
        for (0..self.sort_key.len) |idx| {
            try sort_key.readValue(idx).bind(stmt, idx + 5);
        }
        const col_idx_start = self.sort_key.len + 5;
        for (row_group.column_segment_ids, col_idx_start..) |seg_id, idx| {
            try stmt.bind(.Int64, idx, seg_id);
        }

        try stmt.exec();
    }

    pub fn findRowGroup(
        self: *Self,
        allocator: Allocator,
        row: anytype,
        rowid: i64,
    ) !?RowGroupHandle {
        const stmt = try self.find_row_group.getStmt(self.conn);
        errdefer self.find_row_group.reset();

        for (self.sort_key, 1..) |col_idx, idx| {
            try row.readValue(col_idx).bind(stmt, idx);
        }
        try stmt.bind(.Int64, self.sort_key.len + 1, rowid);

        const has_row_group = try stmt.next();
        if (!has_row_group) {
            self.find_row_group.reset();
            return null;
        }

        var rg_sort_key = try allocator.alloc(
            ?*sqlite_c.sqlite3_value,
            self.sort_key.len,
        );
        for (0..self.sort_key.len) |idx| {
            rg_sort_key[idx] = stmt.readSqliteValue(idx);
        }
        const rg_rowid = stmt.read(.Int64, false, self.sort_key.len);
        const record_count: u32 = @intCast(
            stmt.read(.Int64, false, self.sort_key.len + 1),
        );

        return .{
            .cell = &self.find_row_group,
            .sort_key = rg_sort_key,
            .rowid = rg_rowid,
            .record_count = record_count,
        };
    }

    pub const RowGroupHandle = struct {
        cell: *StmtCell,
        sort_key: []?*sqlite_c.sqlite3_value,
        rowid: i64,
        record_count: u32,

        pub fn deinit(self: *@This(), allocator: Allocator) void {
            self.cell.reset();
            // TODO deinit sqlite values?
            allocator.free(self.sort_key);
        }
    };

    /// Counts the number of buffered insert entries in the row group containing the
    /// provided sort key, rowid combination
    pub fn countRowGroupInserts(
        self: *Self,
        row_group_handle: *const ?RowGroupHandle,
    ) !u32 {
        var stmt: Stmt = undefined;
        if (row_group_handle.*) |handle| {
            stmt = try self.iterator.getStmt(self.conn);
            errdefer self.iterator.reset();

            for (0..self.sort_key.len) |idx| {
                try stmt.bindSqliteValue(idx, handle.sort_key[idx]);
            }
            try stmt.bind(.Int64, self.sort_key.len, handle.rowid);

            const row_group_exists = try stmt.next();
            std.debug.assert(row_group_exists);
        } else {
            stmt = try self.iterator_from_start.getStmt(self.conn);
        }

        var iter = RowGroupInsertsIterator{
            .stmt = stmt,
            .cell = if (row_group_handle.* != null) &self.iterator else &self.iterator,
        };
        defer iter.deinit();

        var count: u32 = 0;
        while (try iter.next()) {
            count += 1;
        }
        return count;
    }

    /// Iterates until a row group entry is found or the end of the table is reached
    const RowGroupInsertsIterator = struct {
        stmt: Stmt,
        cell: *StmtCell,

        fn deinit(self: *@This()) void {
            self.cell.reset();
        }

        fn next(self: *@This()) !bool {
            const has_next = try self.stmt.next();
            if (!has_next) {
                return false;
            }
            const entry_type: EntryType = @enumFromInt(self.stmt.read(.Int32, false, 0));
            return entry_type != .RowGroup;
        }
    };

    pub fn createTable(
        allocator: Allocator,
        conn: Conn,
        table_name: []const u8,
        schema: *const Schema,
    ) !void {
        const ddl_formatter = DdlFormatter{
            .table_name = table_name,
            .schema = schema,
        };
        const ddl = try fmt.allocPrintZ(allocator, "{s}", .{ddl_formatter});
        defer allocator.free(ddl);
        try conn.exec(ddl);
    }

    const DdlFormatter = struct {
        table_name: []const u8,
        schema: *const Schema,

        pub fn format(
            self: @This(),
            comptime _: []const u8,
            _: std.fmt.FormatOptions,
            writer: anytype,
        ) !void {
            try writer.print(
                \\CREATE TABLE "_stanchion_{s}_primary_index" (
            ,
                .{self.table_name},
            );
            for (self.schema.sort_key.items, 0..) |sk_index, sk_rank| {
                const col = &self.schema.columns.items[sk_index];
                const data_type = DataType.SqliteFormatter{
                    .data_type = col.column_type.data_type,
                };
                try writer.print("sk_value_{d} {s} NOT NULL,", .{ sk_rank, data_type });
            }
            // The `col_N` columns are uesd for storing values for insert entries and
            // segment IDs for row group entries
            for (0..self.schema.columns.items.len) |rank| {
                try writer.print("col_{d} ANY NULL,", .{rank});
            }
            try writer.print(
            // Only used by the row group entry type
                \\rowid_segment_id INTEGER NULL,
                // Used by all entry types
                \\rowid INTEGER NOT NULL,
                // Only used by the row group entry type to count the number of records
                // in the row group
                \\record_count INTEGER NULL,
                // Code that specifies the type of the entry
                \\entry_type INTEGER NOT NULL,
                \\PRIMARY KEY (
            , .{});
            for (0..self.schema.sort_key.items.len) |sk_rank| {
                try writer.print("sk_value_{d},", .{sk_rank});
            }
            try writer.print("rowid)", .{});
            // TODO in tests it would be nice to add some check constraints to ensure
            //      data is populated correctly based on the entry_type
            try writer.print(") STRICT, WITHOUT ROWID", .{});
        }
    };

    fn ColumnListFormatter(comptime column_name_template: []const u8) type {
        return struct {
            len: usize,

            pub fn format(
                self: @This(),
                comptime _: []const u8,
                _: std.fmt.FormatOptions,
                writer: anytype,
            ) !void {
                for (0..self.len) |idx| {
                    if (idx > 0) {
                        try writer.print(",", .{});
                    }
                    try writer.print(column_name_template, .{idx});
                }
            }
        };
    }

    const ParameterListFormatter = struct {
        len: usize,

        pub fn format(
            self: @This(),
            comptime _: []const u8,
            _: std.fmt.FormatOptions,
            writer: anytype,
        ) !void {
            for (0..self.len) |idx| {
                if (idx > 0) {
                    try writer.print(",", .{});
                }
                try writer.print("?", .{});
            }
        }
    };
};

const EntryType = enum(u8) {
    RowGroup = 1,
    Insert = 2,

    fn bind(self: EntryType, stmt: Stmt, index: usize) !void {
        try stmt.bind(.Int32, index, @intCast(@intFromEnum(self)));
    }
};

const RowGroup = struct {
    const Self = @This();

    rowid: i64,
    rowid_segment_id: i64,
    column_segment_ids: []i64,
    record_count: u32,

    pub fn deinit(self: *Self, allocator: Allocator) void {
        allocator.free(self.column_segment_ids);
    }
};
