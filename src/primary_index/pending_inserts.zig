const std = @import("std");
const fmt = std.fmt;
const ArenaAllocator = std.heap.ArenaAllocator;

const Stmt = @import("../sqlite3/Stmt.zig");
const ValueRef = @import("../sqlite3/value.zig").Ref;

const stmt_cell = @import("../stmt_cell.zig");

const sql_fmt = @import("sql_fmt.zig");
const Ctx = @import("Ctx.zig");
const EntryType = @import("entry_type.zig").EntryType;
const RowGroupEntry = @import("row_group_entry.zig").Entry;

const StmtCell = stmt_cell.StmtCell(Ctx);

pub const PendingInserts = struct {
    /// These StmtCells hold the Stmts that are used in Iterator, so only 1 Iterator can be open at
    /// a time
    iterator_from_start: StmtCell,
    iterator: StmtCell,

    pub fn init() PendingInserts {
        return .{
            .iterator_from_start = StmtCell.init(&insertIteratorFromStartQuery),
            .iterator = StmtCell.init(&insertIteratorQuery),
        };
    }

    pub fn deinit(self: *PendingInserts) void {
        self.iterator_from_start.deinit();
        self.iterator.deinit();
    }

    /// Creates an iterator from the beginning of the primary index
    pub fn iterateFromStart(
        self: *PendingInserts,
        tmp_arena: *ArenaAllocator,
        ctx: *Ctx,
    ) !Iterator {
        const stmt = try self.iterator_from_start.getStmt(tmp_arena, ctx);
        return Iterator.init(&self.iterator_from_start, stmt);
    }

    fn insertIteratorFromStartQuery(self: *const Ctx, arena: *ArenaAllocator) ![]const u8 {
        return fmt.allocPrintZ(arena.allocator(),
            \\SELECT entry_type, rowid, {s}
            \\FROM "{s}_primaryindex"
            \\ORDER BY {s}, rowid ASC
        , .{
            sql_fmt.ColumnListFormatter("col_{d}"){ .len = self.columns_len },
            self.vtab_table_name,
            sql_fmt.ColumnListFormatter("sk_value_{d} ASC"){ .len = self.sort_key.len },
        });
    }

    /// Creates an iterator from the provided sort key
    pub fn iterate(
        self: *PendingInserts,
        tmp_arena: *ArenaAllocator,
        ctx: *Ctx,
        sort_key: anytype,
        rowid: i64,
    ) !Iterator {
        const stmt = try self.iterator_from_start.getStmt(tmp_arena, ctx);

        for (0..sort_key.valuesLen()) |idx| {
            try stmt.bindSqliteValue(idx + 1, sort_key.readValue(idx));
        }
        try stmt.bind(.Int64, sort_key.valuesLen() + 1, rowid);

        return Iterator.init(&self.iterator_from_start, stmt);
    }

    fn insertIteratorQuery(self: *const Ctx, arena: *ArenaAllocator) ![]const u8 {
        return fmt.allocPrintZ(arena.allocator(),
            \\SELECT entry_type, rowid, {s}
            \\FROM "{s}_primaryindex"
            \\WHERE ({s}, rowid) >= ({s}, ?)
            \\ORDER BY {s}, rowid ASC
        , .{
            sql_fmt.ColumnListFormatter("col_{d}"){ .len = self.columns_len },
            self.vtab_table_name,
            sql_fmt.ColumnListFormatter("sk_value_{d}"){ .len = self.sort_key.len },
            sql_fmt.ParameterListFormatter{ .len = self.sort_key.len },
            sql_fmt.ColumnListFormatter("sk_value_{d} ASC"){ .len = self.sort_key.len },
        });
    }
};

pub const Iterator = struct {
    stmt: Stmt,
    cell: *StmtCell,
    starts_at_node_head: bool,
    eof: bool,
    initialized: bool,

    pub fn uninitialized() Iterator {
        return .{
            .stmt = undefined,
            .cell = undefined,
            .starts_at_node_head = undefined,
            .eof = undefined,
            .initialized = false,
        };
    }

    fn init(cell: *StmtCell, stmt: Stmt) !Iterator {
        var self: Iterator = .{
            .stmt = stmt,
            .cell = cell,
            .starts_at_node_head = undefined,
            .eof = false,
            .initialized = true,
        };

        // Position the iterator on the first row
        try self.next();
        self.starts_at_node_head = readEntryType(self.stmt) != .Insert;
        if (self.starts_at_node_head and !self.eof) {
            try self.next();
        }

        return self;
    }

    pub fn deinit(self: *Iterator) void {
        self.cell.reset();
        self.initialized = false;
    }

    pub fn restart(self: *Iterator) !void {
        try self.stmt.resetExec();

        self.eof = !(try self.stmt.next());
        // Skip the row group (the first row) when the iterator starts at a head entry
        if (self.starts_at_node_head and !self.eof) {
            self.eof = !(try self.stmt.next());
        }
    }

    pub fn next(self: *Iterator) !void {
        const has_next = try self.stmt.next();
        if (!has_next) {
            self.eof = true;
            return;
        }
        const entry_type = readEntryType(self.stmt);
        if (entry_type == .RowGroup) {
            self.eof = true;
        }
    }

    /// Does not check for eof
    pub fn skip(self: *Iterator, n: u32) !void {
        // TODO would it be faster to have a offset parameter in the query and restart
        //      execution after changing the limit?
        for (0..n) |_| {
            _ = try self.stmt.next();
        }
    }

    pub fn readRowid(self: *Iterator) !ValueRef {
        return self.stmt.readSqliteValue(1);
    }

    pub fn readValue(self: *Iterator, idx: usize) !ValueRef {
        return self.stmt.readSqliteValue(idx + 2);
    }

    fn readEntryType(stmt: Stmt) EntryType {
        return @enumFromInt(stmt.read(.Int32, false, 0));
    }
};
