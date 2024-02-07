const sqlite = @import("sqlite3.zig");
const Conn = sqlite.Conn;

const schema_mod = @import("schema.zig");
const Column = schema_mod.Column;
const Schema = schema_mod.Schema;

pub const VtabCtxSchemaless = struct {
    conn_: Conn,
    vtab_name: []const u8,

    pub fn init(conn_: Conn, vtab_name: []const u8) VtabCtxSchemaless {
        return .{ .conn_ = conn_, .vtab_name = vtab_name };
    }

    pub fn conn(self: VtabCtxSchemaless) Conn {
        return self.conn_;
    }

    pub fn vtabName(self: VtabCtxSchemaless) []const u8 {
        return self.vtab_name;
    }
};

pub const VtabCtx = struct {
    base: VtabCtxSchemaless,
    schema: Schema,

    pub fn init(conn_: Conn, vtab_name: []const u8, schema: Schema) VtabCtx {
        return .{ .base = VtabCtxSchemaless.init(conn_, vtab_name), .schema = schema };
    }

    pub fn conn(self: VtabCtx) Conn {
        return self.base.conn();
    }

    pub fn vtabName(self: VtabCtx) []const u8 {
        return self.base.vtabName();
    }

    pub fn columns(self: VtabCtx) []const Column {
        return self.schema.columns;
    }

    pub fn sortKey(self: VtabCtx) []const usize {
        return self.schema.sort_key;
    }
};
