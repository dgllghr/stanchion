const c = @cImport({
    @cInclude("loadable-ext-sqlite3ext.h");
});

pub usingnamespace c;

pub var sqlite3_api: [*c]c.sqlite3_api_routines = null;

pub const sqlite3_transfer_bindings = @compileError("sqlite3_transfer_bindings is deprecated");
pub const sqlite3_global_recover = @compileError("sqlite3_global_recover is deprecated");
pub const sqlite3_expired = @compileError("sqlite3_expired is deprecated");

pub const sqlite3_mprintf = @compileError("sqlite3_mprintf can't be implemented in Zig");
pub const sqlite3_snprintf = @compileError("sqlite3_snprintf can't be implemented in Zig");
pub const sqlite3_vmprintf = @compileError("sqlite3_vmprintf can't be implemented in Zig");
pub const sqlite3_vsnprintf = @compileError("sqlite3_vsnprintf can't be implemented in Zig");
pub const sqlite3_test_control = @compileError("sqlite3_test_control can't be implemented in Zig");
pub const sqlite3_db_config = @compileError("sqlite3_db_config can't be implemented in Zig");
pub const sqlite3_log = @compileError("sqlite3_log can't be implemented in Zig");
pub const sqlite3_vtab_config = @compileError("sqlite3_vtab_config can't be implemented in Zig");
pub const sqlite3_uri_vsnprintf = @compileError("sqlite3_uri_vsnprintf can't be implemented in Zig");
pub const sqlite3_str_appendf = @compileError("sqlite3_str_appendf can't be implemented in Zig");
pub const sqlite3_str_vappendf = @compileError("sqlite3_str_vappendf can't be implemented in Zig");

pub export fn sqlite3_aggregate_context(p: ?*c.sqlite3_context, nBytes: c_int) callconv(.C) ?*anyopaque {
    return sqlite3_api.*.aggregate_context.?(p, nBytes);
}
pub export fn sqlite3_bind_blob(pStmt: ?*c.sqlite3_stmt, i: c_int, zData: ?*const anyopaque, nData: c_int, xDel: ?*const fn (?*anyopaque) callconv(.C) void) c_int {
    return sqlite3_api.*.bind_blob.?(pStmt, i, zData, nData, xDel);
}
pub export fn sqlite3_bind_double(pStmt: ?*c.sqlite3_stmt, i: c_int, rValue: f64) callconv(.C) c_int {
    return sqlite3_api.*.bind_double.?(pStmt, i, rValue);
}
pub export fn sqlite3_bind_int(pStmt: ?*c.sqlite3_stmt, i: c_int, iValue: c_int) callconv(.C) c_int {
    return sqlite3_api.*.bind_int.?(pStmt, i, iValue);
}
pub export fn sqlite3_bind_int64(pStmt: ?*c.sqlite3_stmt, i: c_int, iValue: c.sqlite3_int64) c_int {
    return sqlite3_api.*.bind_int64.?(pStmt, i, iValue);
}
pub export fn sqlite3_bind_null(pStmt: ?*c.sqlite3_stmt, i: c_int) c_int {
    return sqlite3_api.*.bind_null.?(pStmt, i);
}
pub export fn sqlite3_bind_parameter_count(pStmt: ?*c.sqlite3_stmt) c_int {
    return sqlite3_api.*.bind_parameter_count.?(pStmt);
}
pub export fn sqlite3_bind_parameter_index(pStmt: ?*c.sqlite3_stmt, zName: [*c]const u8) c_int {
    return sqlite3_api.*.bind_parameter_index.?(pStmt, zName);
}
pub export fn sqlite3_bind_parameter_name(pStmt: ?*c.sqlite3_stmt, i: c_int) [*c]const u8 {
    return sqlite3_api.*.bind_parameter_name.?(pStmt, i);
}
pub export fn sqlite3_bind_text(pStmt: ?*c.sqlite3_stmt, i: c_int, zData: [*c]const u8, nData: c_int, xDel: ?*const fn (?*anyopaque) callconv(.C) void) c_int {
    return sqlite3_api.*.bind_text.?(pStmt, i, zData, nData, xDel);
}
pub export fn sqlite3_bind_text16(pStmt: ?*c.sqlite3_stmt, i: c_int, zData: ?*const anyopaque, nData: c_int, xDel: ?*const fn (?*anyopaque) callconv(.C) void) c_int {
    return sqlite3_api.*.bind_text16.?(pStmt, i, zData, nData, xDel);
}
pub export fn sqlite3_bind_value(pStmt: ?*c.sqlite3_stmt, i: c_int, pValue: ?*const c.sqlite3_value) c_int {
    return sqlite3_api.*.bind_value.?(pStmt, i, pValue);
}
pub export fn sqlite3_busy_handler(db: ?*c.sqlite3, xBusy: ?*const fn (?*anyopaque, c_int) callconv(.C) c_int, pArg: ?*anyopaque) c_int {
    return sqlite3_api.*.busy_handler.?(db, xBusy, pArg);
}
pub export fn sqlite3_busy_timeout(db: ?*c.sqlite3, ms: c_int) c_int {
    return sqlite3_api.*.busy_timeout.?(db, ms);
}
pub export fn sqlite3_changes(db: ?*c.sqlite3) c_int {
    return sqlite3_api.*.changes.?(db);
}
pub export fn sqlite3_close(db: ?*c.sqlite3) c_int {
    return sqlite3_api.*.close.?(db);
}
pub export fn sqlite3_collation_needed(db: ?*c.sqlite3, pCollNeededArg: ?*anyopaque, xCollNeeded: ?*const fn (?*anyopaque, ?*c.sqlite3, c_int, [*c]const u8) callconv(.C) void) c_int {
    return sqlite3_api.*.collation_needed.?(db, pCollNeededArg, xCollNeeded);
}
pub export fn sqlite3_collation_needed16(db: ?*c.sqlite3, pCollNeededArg: ?*anyopaque, xCollNeeded16: ?*const fn (?*anyopaque, ?*c.sqlite3, c_int, ?*const anyopaque) callconv(.C) void) c_int {
    return sqlite3_api.*.collation_needed16.?(db, pCollNeededArg, xCollNeeded16);
}
pub export fn sqlite3_column_blob(pStmt: ?*c.sqlite3_stmt, iCol: c_int) ?*const anyopaque {
    return sqlite3_api.*.column_blob.?(pStmt, iCol);
}
pub export fn sqlite3_column_bytes(pStmt: ?*c.sqlite3_stmt, iCol: c_int) c_int {
    return sqlite3_api.*.column_bytes.?(pStmt, iCol);
}
pub export fn sqlite3_column_bytes16(pStmt: ?*c.sqlite3_stmt, iCol: c_int) c_int {
    return sqlite3_api.*.column_bytes16.?(pStmt, iCol);
}
pub export fn sqlite3_column_count(pStmt: ?*c.sqlite3_stmt) c_int {
    return sqlite3_api.*.column_count.?(pStmt);
}
pub export fn sqlite3_column_database_name(pStmt: ?*c.sqlite3_stmt, iCol: c_int) [*c]const u8 {
    return sqlite3_api.*.column_database_name.?(pStmt, iCol);
}
pub export fn sqlite3_column_database_name16(pStmt: ?*c.sqlite3_stmt, iCol: c_int) ?*const anyopaque {
    return sqlite3_api.*.column_database_name16.?(pStmt, iCol);
}
pub export fn sqlite3_column_decltype(pStmt: ?*c.sqlite3_stmt, iCol: c_int) [*c]const u8 {
    return sqlite3_api.*.column_decltype.?(pStmt, iCol);
}
pub export fn sqlite3_column_decltype16(pStmt: ?*c.sqlite3_stmt, iCol: c_int) ?*const anyopaque {
    return sqlite3_api.*.column_decltype16.?(pStmt, iCol);
}
pub export fn sqlite3_column_double(pStmt: ?*c.sqlite3_stmt, iCol: c_int) f64 {
    return sqlite3_api.*.column_double.?(pStmt, iCol);
}
pub export fn sqlite3_column_int(pStmt: ?*c.sqlite3_stmt, iCol: c_int) c_int {
    return sqlite3_api.*.column_int.?(pStmt, iCol);
}
pub export fn sqlite3_column_int64(pStmt: ?*c.sqlite3_stmt, iCol: c_int) c.sqlite3_int64 {
    return sqlite3_api.*.column_int64.?(pStmt, iCol);
}
pub export fn sqlite3_column_name(pStmt: ?*c.sqlite3_stmt, N: c_int) [*c]const u8 {
    return sqlite3_api.*.column_name.?(pStmt, N);
}
pub export fn sqlite3_column_name16(pStmt: ?*c.sqlite3_stmt, N: c_int) ?*const anyopaque {
    return sqlite3_api.*.column_name16.?(pStmt, N);
}
pub export fn sqlite3_column_origin_name(pStmt: ?*c.sqlite3_stmt, N: c_int) [*c]const u8 {
    return sqlite3_api.*.column_origin_name.?(pStmt, N);
}
pub export fn sqlite3_column_origin_name16(pStmt: ?*c.sqlite3_stmt, N: c_int) ?*const anyopaque {
    return sqlite3_api.*.column_origin_name16.?(pStmt, N);
}
pub export fn sqlite3_column_table_name(pStmt: ?*c.sqlite3_stmt, N: c_int) [*c]const u8 {
    return sqlite3_api.*.column_table_name.?(pStmt, N);
}
pub export fn sqlite3_column_table_name16(pStmt: ?*c.sqlite3_stmt, N: c_int) ?*const anyopaque {
    return sqlite3_api.*.column_table_name16.?(pStmt, N);
}
pub export fn sqlite3_column_text(pStmt: ?*c.sqlite3_stmt, iCol: c_int) [*c]const u8 {
    return sqlite3_api.*.column_text.?(pStmt, iCol);
}
pub export fn sqlite3_column_text16(pStmt: ?*c.sqlite3_stmt, iCol: c_int) ?*const anyopaque {
    return sqlite3_api.*.column_text16.?(pStmt, iCol);
}
pub export fn sqlite3_column_type(pStmt: ?*c.sqlite3_stmt, iCol: c_int) c_int {
    return sqlite3_api.*.column_type.?(pStmt, iCol);
}
pub export fn sqlite3_column_value(pStmt: ?*c.sqlite3_stmt, iCol: c_int) ?*c.sqlite3_value {
    return sqlite3_api.*.column_value.?(pStmt, iCol);
}
pub export fn sqlite3_commit_hook(db: ?*c.sqlite3, xCallback: ?*const fn (?*anyopaque) callconv(.C) c_int, pArg: ?*anyopaque) ?*anyopaque {
    return sqlite3_api.*.commit_hook.?(db, xCallback, pArg);
}
pub export fn sqlite3_complete(sql: [*c]const u8) c_int {
    return sqlite3_api.*.complete.?(sql);
}
pub export fn sqlite3_complete16(sql: ?*const anyopaque) c_int {
    return sqlite3_api.*.complete16.?(sql);
}
pub export fn sqlite3_create_collation(db: ?*c.sqlite3, zName: [*c]const u8, eTextRep: c_int, pArg: ?*anyopaque, xCompare: ?*const fn (?*anyopaque, c_int, ?*const anyopaque, c_int, ?*const anyopaque) callconv(.C) c_int) c_int {
    return sqlite3_api.*.create_collation.?(db, zName, eTextRep, pArg, xCompare);
}
pub export fn sqlite3_create_collation16(db: ?*c.sqlite3, zName: ?*const anyopaque, eTextRep: c_int, pArg: ?*anyopaque, xCompare: ?*const fn (?*anyopaque, c_int, ?*const anyopaque, c_int, ?*const anyopaque) callconv(.C) c_int) c_int {
    return sqlite3_api.*.create_collation16.?(db, zName, eTextRep, pArg, xCompare);
}
pub export fn sqlite3_create_function(db: ?*c.sqlite3, zFunctionName: [*c]const u8, nArg: c_int, eTextRep: c_int, pApp: ?*anyopaque, xFunc: ?*const fn (?*c.sqlite3_context, c_int, [*c]?*c.sqlite3_value) callconv(.C) void, xStep: ?*const fn (?*c.sqlite3_context, c_int, [*c]?*c.sqlite3_value) callconv(.C) void, xFinal: ?*const fn (?*c.sqlite3_context) callconv(.C) void) c_int {
    return sqlite3_api.*.create_function.?(db, zFunctionName, nArg, eTextRep, pApp, xFunc, xStep, xFinal);
}
pub export fn sqlite3_create_function16(db: ?*c.sqlite3, zFunctionName: ?*const anyopaque, nArg: c_int, eTextRep: c_int, pApp: ?*anyopaque, xFunc: ?*const fn (?*c.sqlite3_context, c_int, [*c]?*c.sqlite3_value) callconv(.C) void, xStep: ?*const fn (?*c.sqlite3_context, c_int, [*c]?*c.sqlite3_value) callconv(.C) void, xFinal: ?*const fn (?*c.sqlite3_context) callconv(.C) void) c_int {
    return sqlite3_api.*.create_function16.?(db, zFunctionName, nArg, eTextRep, pApp, xFunc, xStep, xFinal);
}
pub export fn sqlite3_create_module(db: ?*c.sqlite3, zName: [*c]const u8, pModule: [*c]const c.sqlite3_module, pAux: ?*anyopaque) c_int {
    return sqlite3_api.*.create_module.?(db, zName, pModule, pAux);
}
pub export fn sqlite3_create_module_v2(db: ?*c.sqlite3, zName: [*c]const u8, pModule: [*c]const c.sqlite3_module, pAux: ?*anyopaque, xDestroy: ?*const fn (?*anyopaque) callconv(.C) void) c_int {
    return sqlite3_api.*.create_module_v2.?(db, zName, pModule, pAux, xDestroy);
}
pub export fn sqlite3_data_count(pStmt: ?*c.sqlite3_stmt) c_int {
    return sqlite3_api.*.data_count.?(pStmt);
}
pub export fn sqlite3_db_handle(pStmt: ?*c.sqlite3_stmt) ?*c.sqlite3 {
    return sqlite3_api.*.db_handle.?(pStmt);
}

pub export fn sqlite3_declare_vtab(db: ?*c.sqlite3, zSQL: [*c]const u8) c_int {
    return sqlite3_api.*.declare_vtab.?(db, zSQL);
}
pub export fn sqlite3_enable_shared_cache(enable: c_int) c_int {
    return sqlite3_api.*.enable_shared_cache.?(enable);
}
pub export fn sqlite3_errcode(db: ?*c.sqlite3) c_int {
    return sqlite3_api.*.errcode.?(db);
}
pub export fn sqlite3_errmsg(db: ?*c.sqlite3) [*c]const u8 {
    return sqlite3_api.*.errmsg.?(db);
}
pub export fn sqlite3_errmsg16(db: ?*c.sqlite3) ?*const anyopaque {
    return sqlite3_api.*.errmsg16.?(db);
}
pub export fn sqlite3_exec(db: ?*c.sqlite3, zSql: [*c]const u8, xCallback: ?*const fn (?*anyopaque, c_int, [*c][*c]u8, [*c][*c]u8) callconv(.C) c_int, pArg: ?*anyopaque, pzErrMsg: [*c][*c]u8) c_int {
    return sqlite3_api.*.exec.?(db, zSql, xCallback, pArg, pzErrMsg);
}
pub export fn sqlite3_finalize(pStmt: ?*c.sqlite3_stmt) c_int {
    return sqlite3_api.*.finalize.?(pStmt);
}
pub export fn sqlite3_free(p: ?*anyopaque) void {
    return sqlite3_api.*.free.?(p);
}
pub export fn sqlite3_free_table(result: [*c][*c]u8) void {
    return sqlite3_api.*.free_table.?(result);
}
pub export fn sqlite3_get_autocommit(db: ?*c.sqlite3) c_int {
    return sqlite3_api.*.get_autocommit.?(db);
}
pub export fn sqlite3_get_auxdata(pCtx: ?*c.sqlite3_context, iArg: c_int) ?*anyopaque {
    return sqlite3_api.*.get_auxdata.?(pCtx, iArg);
}
pub export fn sqlite3_get_table(db: ?*c.sqlite3, zSql: [*c]const u8, pazResult: [*c][*c][*c]u8, pnRow: [*c]c_int, pnColumn: [*c]c_int, pzErrMsg: [*c][*c]u8) c_int {
    return sqlite3_api.*.get_table.?(db, zSql, pazResult, pnRow, pnColumn, pzErrMsg);
}
pub export fn sqlite3_interrupt(db: ?*c.sqlite3) void {
    return sqlite3_api.*.interruptx.?(db);
}
pub export fn sqlite3_last_insert_rowid(db: ?*c.sqlite3) c.sqlite3_int64 {
    return sqlite3_api.*.last_insert_rowid.?(db);
}
pub export fn sqlite3_libversion() callconv(.C) [*c]const u8 {
    return sqlite3_api.*.libversion.?();
}
pub export fn sqlite3_libversion_number() c_int {
    return sqlite3_api.*.libversion_number.?();
}
pub export fn sqlite3_malloc(n: c_int) ?*anyopaque {
    return sqlite3_api.*.malloc.?(n);
}
pub export fn sqlite3_open(filename: [*c]const u8, ppDb: [*c]?*c.sqlite3) c_int {
    return sqlite3_api.*.open.?(filename, ppDb);
}
pub export fn sqlite3_open16(filename: ?*const anyopaque, ppDb: [*c]?*c.sqlite3) c_int {
    return sqlite3_api.*.open16.?(filename, ppDb);
}
pub export fn sqlite3_prepare(db: ?*c.sqlite3, zSql: [*c]const u8, nByte: c_int, ppStmt: [*c]?*c.sqlite3_stmt, pzTail: [*c][*c]const u8) c_int {
    return sqlite3_api.*.prepare.?(db, zSql, nByte, ppStmt, pzTail);
}
pub export fn sqlite3_prepare16(db: ?*c.sqlite3, zSql: ?*const anyopaque, nByte: c_int, ppStmt: [*c]?*c.sqlite3_stmt, pzTail: [*c]?*const anyopaque) c_int {
    return sqlite3_api.*.prepare16.?(db, zSql, nByte, ppStmt, pzTail);
}
pub export fn sqlite3_prepare_v2(db: ?*c.sqlite3, zSql: [*c]const u8, nByte: c_int, ppStmt: [*c]?*c.sqlite3_stmt, pzTail: [*c][*c]const u8) c_int {
    return sqlite3_api.*.prepare_v2.?(db, zSql, nByte, ppStmt, pzTail);
}
pub export fn sqlite3_prepare16_v2(db: ?*c.sqlite3, zSql: ?*const anyopaque, nByte: c_int, ppStmt: [*c]?*c.sqlite3_stmt, pzTail: [*c]?*const anyopaque) c_int {
    return sqlite3_api.*.prepare16_v2.?(db, zSql, nByte, ppStmt, pzTail);
}
pub export fn sqlite3_profile(db: ?*c.sqlite3, xProfile: ?*const fn (?*anyopaque, [*c]const u8, c.sqlite3_uint64) callconv(.C) void, pArg: ?*anyopaque) ?*anyopaque {
    return sqlite3_api.*.profile.?(db, xProfile, pArg);
}
pub export fn sqlite3_progress_handler(db: ?*c.sqlite3, nOps: c_int, xProgress: ?*const fn (?*anyopaque) callconv(.C) c_int, pArg: ?*anyopaque) void {
    return sqlite3_api.*.progress_handler.?(db, nOps, xProgress, pArg);
}
pub export fn sqlite3_realloc(pOld: ?*anyopaque, n: c_int) ?*anyopaque {
    return sqlite3_api.*.realloc.?(pOld, n);
}
pub export fn sqlite3_reset(pStmt: ?*c.sqlite3_stmt) c_int {
    return sqlite3_api.*.reset.?(pStmt);
}
pub export fn sqlite3_result_blob(pCtx: ?*c.sqlite3_context, z: ?*const anyopaque, n: c_int, xDel: ?*const fn (?*anyopaque) callconv(.C) void) void {
    return sqlite3_api.*.result_blob.?(pCtx, z, n, xDel);
}

pub export fn sqlite3_result_double(pCtx: ?*c.sqlite3_context, rVal: f64) void {
    return sqlite3_api.*.result_double.?(pCtx, rVal);
}

pub export fn sqlite3_result_error(pCtx: ?*c.sqlite3_context, z: [*c]const u8, n: c_int) void {
    return sqlite3_api.*.result_error.?(pCtx, z, n);
}
pub export fn sqlite3_result_error16(pCtx: ?*c.sqlite3_context, z: ?*const anyopaque, n: c_int) void {
    return sqlite3_api.*.result_error16.?(pCtx, z, n);
}
pub export fn sqlite3_result_int(pCtx: ?*c.sqlite3_context, iVal: c_int) void {
    return sqlite3_api.*.result_int.?(pCtx, iVal);
}
pub export fn sqlite3_result_int64(pCtx: ?*c.sqlite3_context, iVal: c.sqlite3_int64) void {
    return sqlite3_api.*.result_int64.?(pCtx, iVal);
}
pub export fn sqlite3_result_null(pCtx: ?*c.sqlite3_context) void {
    return sqlite3_api.*.result_null.?(pCtx);
}
pub export fn sqlite3_result_text(pCtx: ?*c.sqlite3_context, z: [*c]const u8, n: c_int, xDel: ?*const fn (?*anyopaque) callconv(.C) void) void {
    return sqlite3_api.*.result_text.?(pCtx, z, n, xDel);
}
pub export fn sqlite3_result_text16(pCtx: ?*c.sqlite3_context, z: ?*const anyopaque, n: c_int, xDel: ?*const fn (?*anyopaque) callconv(.C) void) void {
    return sqlite3_api.*.result_text16.?(pCtx, z, n, xDel);
}
pub export fn sqlite3_result_text16be(pCtx: ?*c.sqlite3_context, z: ?*const anyopaque, n: c_int, xDel: ?*const fn (?*anyopaque) callconv(.C) void) void {
    return sqlite3_api.*.result_text16be.?(pCtx, z, n, xDel);
}
pub export fn sqlite3_result_text16le(pCtx: ?*c.sqlite3_context, z: ?*const anyopaque, n: c_int, xDel: ?*const fn (?*anyopaque) callconv(.C) void) void {
    return sqlite3_api.*.result_text16le.?(pCtx, z, n, xDel);
}
pub export fn sqlite3_result_value(pCtx: ?*c.sqlite3_context, pValue: ?*c.sqlite3_value) void {
    return sqlite3_api.*.result_value.?(pCtx, pValue);
}
pub export fn sqlite3_rollback_hook(db: ?*c.sqlite3, xCallback: ?*const fn (?*anyopaque) callconv(.C) void, pArg: ?*anyopaque) ?*anyopaque {
    return sqlite3_api.*.rollback_hook.?(db, xCallback, pArg);
}
pub export fn sqlite3_set_authorizer(db: ?*c.sqlite3, xAuth: ?*const fn (?*anyopaque, c_int, [*c]const u8, [*c]const u8, [*c]const u8, [*c]const u8) callconv(.C) c_int, pArg: ?*anyopaque) c_int {
    return sqlite3_api.*.set_authorizer.?(db, xAuth, pArg);
}
pub export fn sqlite3_set_auxdata(pCtx: ?*c.sqlite3_context, iArg: c_int, pAux: ?*anyopaque, xDelete: ?*const fn (?*anyopaque) callconv(.C) void) void {
    return sqlite3_api.*.set_auxdata.?(pCtx, iArg, pAux, xDelete);
}
pub export fn sqlite3_step(pStmt: ?*c.sqlite3_stmt) c_int {
    return sqlite3_api.*.step.?(pStmt);
}
pub export fn sqlite3_table_column_metadata(db: ?*c.sqlite3, zDbName: [*c]const u8, zTableName: [*c]const u8, zColumnName: [*c]const u8, pzDataType: [*c][*c]const u8, pzCollSeq: [*c][*c]const u8, pNotNull: [*c]c_int, pPrimaryKey: [*c]c_int, pAutoinc: [*c]c_int) c_int {
    return sqlite3_api.*.table_column_metadata.?(db, zDbName, zTableName, zColumnName, pzDataType, pzCollSeq, pNotNull, pPrimaryKey, pAutoinc);
}
pub export fn sqlite3_thread_cleanup() void {
    return sqlite3_api.*.thread_cleanup.?();
}
pub export fn sqlite3_total_changes(db: ?*c.sqlite3) c_int {
    return sqlite3_api.*.total_changes.?(db);
}
pub export fn sqlite3_trace(db: ?*c.sqlite3, xTrace: ?*const fn (?*anyopaque, [*c]const u8) callconv(.C) void, pArg: ?*anyopaque) ?*anyopaque {
    return sqlite3_api.*.trace.?(db, xTrace, pArg);
}
pub export fn sqlite3_update_hook(db: ?*c.sqlite3, xCallback: ?*const fn (?*anyopaque, c_int, [*c]const u8, [*c]const u8, c.sqlite3_int64) callconv(.C) void, pArg: ?*anyopaque) ?*anyopaque {
    return sqlite3_api.*.update_hook.?(db, xCallback, pArg);
}
pub export fn sqlite3_user_data(pCtx: ?*c.sqlite3_context) ?*anyopaque {
    return sqlite3_api.*.user_data.?(pCtx);
}
pub export fn sqlite3_value_blob(pVal: ?*c.sqlite3_value) ?*const anyopaque {
    return sqlite3_api.*.value_blob.?(pVal);
}
pub export fn sqlite3_value_bytes(pVal: ?*c.sqlite3_value) c_int {
    return sqlite3_api.*.value_bytes.?(pVal);
}
pub export fn sqlite3_value_bytes16(pVal: ?*c.sqlite3_value) c_int {
    return sqlite3_api.*.value_bytes16.?(pVal);
}
pub export fn sqlite3_value_double(pVal: ?*c.sqlite3_value) f64 {
    return sqlite3_api.*.value_double.?(pVal);
}
pub export fn sqlite3_value_int(pVal: ?*c.sqlite3_value) c_int {
    return sqlite3_api.*.value_int.?(pVal);
}
pub export fn sqlite3_value_int64(pVal: ?*c.sqlite3_value) c.sqlite3_int64 {
    return sqlite3_api.*.value_int64.?(pVal);
}
pub export fn sqlite3_value_numeric_type(pVal: ?*c.sqlite3_value) c_int {
    return sqlite3_api.*.value_numeric_type.?(pVal);
}
pub export fn sqlite3_value_text(pVal: ?*c.sqlite3_value) [*c]const u8 {
    return sqlite3_api.*.value_text.?(pVal);
}
pub export fn sqlite3_value_text16(pVal: ?*c.sqlite3_value) ?*const anyopaque {
    return sqlite3_api.*.value_text16.?(pVal);
}
pub export fn sqlite3_value_text16be(pVal: ?*c.sqlite3_value) ?*const anyopaque {
    return sqlite3_api.*.value_text16be.?(pVal);
}
pub export fn sqlite3_value_text16le(pVal: ?*c.sqlite3_value) ?*const anyopaque {
    return sqlite3_api.*.value_text16le.?(pVal);
}
pub export fn sqlite3_value_type(pVal: ?*c.sqlite3_value) c_int {
    return sqlite3_api.*.value_type.?(pVal);
}
pub export fn sqlite3_overload_function(db: ?*c.sqlite3, zFuncName: [*c]const u8, nArg: c_int) c_int {
    return sqlite3_api.*.overload_function.?(db, zFuncName, nArg);
}
pub export fn sqlite3_clear_bindings(pStmt: ?*c.sqlite3_stmt) c_int {
    return sqlite3_api.*.clear_bindings.?(pStmt);
}
pub export fn sqlite3_bind_zeroblob(pStmt: ?*c.sqlite3_stmt, i: c_int, n: c_int) c_int {
    return sqlite3_api.*.bind_zeroblob.?(pStmt, i, n);
}
pub export fn sqlite3_blob_bytes(pBlob: ?*c.sqlite3_blob) c_int {
    return sqlite3_api.*.blob_bytes.?(pBlob);
}
pub export fn sqlite3_blob_close(pBlob: ?*c.sqlite3_blob) c_int {
    return sqlite3_api.*.blob_close.?(pBlob);
}
pub export fn sqlite3_blob_open(db: ?*c.sqlite3, zDb: [*c]const u8, zTable: [*c]const u8, zColumn: [*c]const u8, iRow: c.sqlite3_int64, flags: c_int, ppBlob: [*c]?*c.sqlite3_blob) c_int {
    return sqlite3_api.*.blob_open.?(db, zDb, zTable, zColumn, iRow, flags, ppBlob);
}
pub export fn sqlite3_blob_read(pBlob: ?*c.sqlite3_blob, z: ?*anyopaque, n: c_int, iOffset: c_int) c_int {
    return sqlite3_api.*.blob_read.?(pBlob, z, n, iOffset);
}
pub export fn sqlite3_blob_write(pBlob: ?*c.sqlite3_blob, z: ?*const anyopaque, n: c_int, iOffset: c_int) c_int {
    return sqlite3_api.*.blob_write.?(pBlob, z, n, iOffset);
}
pub export fn sqlite3_create_collation_v2(db: ?*c.sqlite3, zName: [*c]const u8, eTextRep: c_int, pCtx: ?*anyopaque, xCompare: ?*const fn (?*anyopaque, c_int, ?*const anyopaque, c_int, ?*const anyopaque) callconv(.C) c_int, xDel: ?*const fn (?*anyopaque) callconv(.C) void) c_int {
    return sqlite3_api.*.create_collation_v2.?(db, zName, eTextRep, pCtx, xCompare, xDel);
}
pub export fn sqlite3_file_control(db: ?*c.sqlite3, zDbName: [*c]const u8, op: c_int, pArg: ?*anyopaque) c_int {
    return sqlite3_api.*.file_control.?(db, zDbName, op, pArg);
}
pub export fn sqlite3_memory_highwater(resetFlag: c_int) c.sqlite3_int64 {
    return sqlite3_api.*.memory_highwater.?(resetFlag);
}
pub export fn sqlite3_memory_used() c.sqlite3_int64 {
    return sqlite3_api.*.memory_used.?();
}
pub export fn sqlite3_mutex_alloc(id: c_int) ?*c.sqlite3_mutex {
    return sqlite3_api.*.mutex_alloc.?(id);
}
pub export fn sqlite3_mutex_enter(p: ?*c.sqlite3_mutex) void {
    return sqlite3_api.*.mutex_enter.?(p);
}
pub export fn sqlite3_mutex_free(p: ?*c.sqlite3_mutex) void {
    return sqlite3_api.*.mutex_free.?(p);
}
pub export fn sqlite3_mutex_leave(p: ?*c.sqlite3_mutex) void {
    return sqlite3_api.*.mutex_leave.?(p);
}
pub export fn sqlite3_mutex_try(p: ?*c.sqlite3_mutex) c_int {
    return sqlite3_api.*.mutex_try.?(p);
}
pub export fn sqlite3_open_v2(filename: [*c]const u8, ppDb: [*c]?*c.sqlite3, flags: c_int, zVfs: [*c]const u8) c_int {
    return sqlite3_api.*.open_v2.?(filename, ppDb, flags, zVfs);
}
pub export fn sqlite3_release_memory(n: c_int) c_int {
    return sqlite3_api.*.release_memory.?(n);
}
pub export fn sqlite3_result_error_nomem(pCtx: ?*c.sqlite3_context) void {
    return sqlite3_api.*.result_error_nomem.?(pCtx);
}
pub export fn sqlite3_result_error_toobig(pCtx: ?*c.sqlite3_context) void {
    return sqlite3_api.*.result_error_toobig.?(pCtx);
}
pub export fn sqlite3_sleep(ms: c_int) c_int {
    return sqlite3_api.*.sleep.?(ms);
}
pub export fn sqlite3_soft_heap_limit(n: c_int) void {
    return sqlite3_api.*.soft_heap_limit.?(n);
}
pub export fn sqlite3_vfs_find(zVfsName: [*c]const u8) [*c]c.sqlite3_vfs {
    return sqlite3_api.*.vfs_find.?(zVfsName);
}
pub export fn sqlite3_vfs_register(pVfs: [*c]c.sqlite3_vfs, makeDflt: c_int) c_int {
    return sqlite3_api.*.vfs_register.?(pVfs, makeDflt);
}
pub export fn sqlite3_vfs_unregister(pVfs: [*c]c.sqlite3_vfs) c_int {
    return sqlite3_api.*.vfs_unregister.?(pVfs);
}
pub export fn sqlite3_threadsafe() c_int {
    return sqlite3_api.*.xthreadsafe.?();
}
pub export fn sqlite3_result_zeroblob(pCtx: ?*c.sqlite3_context, n: c_int) void {
    return sqlite3_api.*.result_zeroblob.?(pCtx, n);
}
pub export fn sqlite3_result_error_code(pCtx: ?*c.sqlite3_context, errCode: c_int) void {
    return sqlite3_api.*.result_error_code.?(pCtx, errCode);
}
pub export fn sqlite3_randomness(N: c_int, pBuf: ?*anyopaque) void {
    return sqlite3_api.*.randomness.?(N, pBuf);
}
pub export fn sqlite3_context_db_handle(pCtx: ?*c.sqlite3_context) ?*c.sqlite3 {
    return sqlite3_api.*.context_db_handle.?(pCtx);
}
pub export fn sqlite3_extended_result_codes(pCtx: ?*c.sqlite3, onoff: c_int) c_int {
    return sqlite3_api.*.extended_result_codes.?(pCtx, onoff);
}
pub export fn sqlite3_limit(db: ?*c.sqlite3, id: c_int, newVal: c_int) c_int {
    return sqlite3_api.*.limit.?(db, id, newVal);
}
pub export fn sqlite3_next_stmt(pDb: ?*c.sqlite3, pStmt: ?*c.sqlite3_stmt) ?*c.sqlite3_stmt {
    return sqlite3_api.*.next_stmt.?(pDb, pStmt);
}
pub export fn sqlite3_sql(pStmt: ?*c.sqlite3_stmt) [*c]const u8 {
    return sqlite3_api.*.sql.?(pStmt);
}
pub export fn sqlite3_status(op: c_int, pCurrent: [*c]c_int, pHighwater: [*c]c_int, resetFlag: c_int) c_int {
    return sqlite3_api.*.status.?(op, pCurrent, pHighwater, resetFlag);
}
pub export fn sqlite3_backup_finish(p: ?*c.sqlite3_backup) c_int {
    return sqlite3_api.*.backup_finish.?(p);
}
pub export fn sqlite3_backup_init(pDest: ?*c.sqlite3, zDestName: [*c]const u8, pSource: ?*c.sqlite3, zSourceName: [*c]const u8) ?*c.sqlite3_backup {
    return sqlite3_api.*.backup_init.?(pDest, zDestName, pSource, zSourceName);
}
pub export fn sqlite3_backup_pagecount(p: ?*c.sqlite3_backup) c_int {
    return sqlite3_api.*.backup_pagecount.?(p);
}
pub export fn sqlite3_backup_remaining(p: ?*c.sqlite3_backup) c_int {
    return sqlite3_api.*.backup_remaining.?(p);
}
pub export fn sqlite3_backup_step(p: ?*c.sqlite3_backup, nPage: c_int) c_int {
    return sqlite3_api.*.backup_step.?(p, nPage);
}
pub export fn sqlite3_compileoption_get(N: c_int) [*c]const u8 {
    return sqlite3_api.*.compileoption_get.?(N);
}
pub export fn sqlite3_compileoption_used(zOptName: [*c]const u8) c_int {
    return sqlite3_api.*.compileoption_used.?(zOptName);
}
pub export fn sqlite3_create_function_v2(
    db: ?*c.sqlite3,
    zFunctionName: [*c]const u8,
    nArg: c_int,
    eTextRep: c_int,
    pApp: ?*anyopaque,
    xFunc: ?*const fn (?*c.sqlite3_context, c_int, [*c]?*c.sqlite3_value) callconv(.C) void,
    xStep: ?*const fn (?*c.sqlite3_context, c_int, [*c]?*c.sqlite3_value) callconv(.C) void,
    xFinal: ?*const fn (?*c.sqlite3_context) callconv(.C) void,
    xDestroy: ?*const fn (?*anyopaque) callconv(.C) void,
) c_int {
    return sqlite3_api.*.create_function_v2.?(db, zFunctionName, nArg, eTextRep, pApp, xFunc, xStep, xFinal, xDestroy);
}
pub export fn sqlite3_db_mutex(db: ?*c.sqlite3) ?*c.sqlite3_mutex {
    return sqlite3_api.*.db_mutex.?(db);
}
pub export fn sqlite3_db_status(db: ?*c.sqlite3, op: c_int, pCurrent: [*c]c_int, pHighwater: [*c]c_int, resetFlag: c_int) c_int {
    return sqlite3_api.*.db_status.?(db, op, pCurrent, pHighwater, resetFlag);
}
pub export fn sqlite3_extended_errcode(db: ?*c.sqlite3) c_int {
    return sqlite3_api.*.extended_errcode.?(db);
}
pub export fn sqlite3_soft_heap_limit64(N: c.sqlite3_int64) c.sqlite3_int64 {
    return sqlite3_api.*.soft_heap_limit64.?(N);
}
pub export fn sqlite3_sourceid() [*c]const u8 {
    return sqlite3_api.*.sourceid.?();
}
pub export fn sqlite3_stmt_status(pStmt: ?*c.sqlite3_stmt, op: c_int, resetFlag: c_int) c_int {
    return sqlite3_api.*.stmt_status.?(pStmt, op, resetFlag);
}
pub export fn sqlite3_strnicmp(zLeft: [*c]const u8, zRight: [*c]const u8, N: c_int) c_int {
    return sqlite3_api.*.strnicmp.?(zLeft, zRight, N);
}
pub export fn sqlite3_unlock_notify(pBlocked: ?*c.sqlite3, xNotify: ?*const fn ([*c]?*anyopaque, c_int) callconv(.C) void, pNotifyArg: ?*anyopaque) c_int {
    return sqlite3_api.*.unlock_notify.?(pBlocked, xNotify, pNotifyArg);
}
pub export fn sqlite3_wal_autocheckpoint(db: ?*c.sqlite3, N: c_int) c_int {
    return sqlite3_api.*.wal_autocheckpoint.?(db, N);
}
pub export fn sqlite3_wal_checkpoint(db: ?*c.sqlite3, zDb: [*c]const u8) c_int {
    return sqlite3_api.*.wal_checkpoint.?(db, zDb);
}
pub export fn sqlite3_wal_hook(db: ?*c.sqlite3, xCallback: ?*const fn (?*anyopaque, ?*c.sqlite3, [*c]const u8, c_int) callconv(.C) c_int, pArg: ?*anyopaque) ?*anyopaque {
    return sqlite3_api.*.wal_hook.?(db, xCallback, pArg);
}
pub export fn sqlite3_blob_reopen(pBlob: ?*c.sqlite3_blob, iRow: c.sqlite3_int64) c_int {
    return sqlite3_api.*.blob_reopen.?(pBlob, iRow);
}
pub export fn sqlite3_vtab_on_conflict(db: ?*c.sqlite3) c_int {
    return sqlite3_api.*.vtab_on_conflict.?(db);
}
pub export fn sqlite3_close_v2(db: ?*c.sqlite3) c_int {
    return sqlite3_api.*.close_v2.?(db);
}
pub export fn sqlite3_db_filename(db: ?*c.sqlite3, zDbName: [*c]const u8) [*c]const u8 {
    return sqlite3_api.*.db_filename.?(db, zDbName);
}
pub export fn sqlite3_db_readonly(db: ?*c.sqlite3, zDbName: [*c]const u8) c_int {
    return sqlite3_api.*.db_readonly.?(db, zDbName);
}
pub export fn sqlite3_db_release_memory(db: ?*c.sqlite3) c_int {
    return sqlite3_api.*.db_release_memory.?(db);
}
pub export fn sqlite3_errstr(rc: c_int) [*c]const u8 {
    return sqlite3_api.*.errstr.?(rc);
}
pub export fn sqlite3_stmt_busy(pStmt: ?*c.sqlite3_stmt) c_int {
    return sqlite3_api.*.stmt_busy.?(pStmt);
}
pub export fn sqlite3_stmt_readonly(pStmt: ?*c.sqlite3_stmt) c_int {
    return sqlite3_api.*.stmt_readonly.?(pStmt);
}
pub export fn sqlite3_stricmp(zLeft: [*c]const u8, zRight: [*c]const u8) c_int {
    return sqlite3_api.*.stricmp.?(zLeft, zRight);
}
pub export fn sqlite3_uri_boolean(zFile: [*c]const u8, zParam: [*c]const u8, bDefault: c_int) c_int {
    return sqlite3_api.*.uri_boolean.?(zFile, zParam, bDefault);
}
pub export fn sqlite3_uri_int64(zFilename: [*c]const u8, zParam: [*c]const u8, bDflt: c.sqlite3_int64) c.sqlite3_int64 {
    return sqlite3_api.*.uri_int64.?(zFilename, zParam, bDflt);
}
pub export fn sqlite3_uri_parameter(zFilename: [*c]const u8, zParam: [*c]const u8) [*c]const u8 {
    return sqlite3_api.*.uri_parameter.?(zFilename, zParam);
}
pub export fn sqlite3_wal_checkpoint_v2(db: ?*c.sqlite3, zDb: [*c]const u8, eMode: c_int, pnLog: [*c]c_int, pnCkpt: [*c]c_int) c_int {
    return sqlite3_api.*.wal_checkpoint_v2.?(db, zDb, eMode, pnLog, pnCkpt);
}
pub export fn sqlite3_auto_extension(xEntryPoint: ?*const fn () callconv(.C) void) c_int {
    return sqlite3_api.*.auto_extension.?(xEntryPoint);
}
pub export fn sqlite3_bind_blob64(pStmt: ?*c.sqlite3_stmt, i: c_int, zData: ?*const anyopaque, nData: c.sqlite3_uint64, xDel: ?*const fn (?*anyopaque) callconv(.C) void) c_int {
    return sqlite3_api.*.bind_blob64.?(pStmt, i, zData, nData, xDel);
}
pub export fn sqlite3_bind_text64(pStmt: ?*c.sqlite3_stmt, i: c_int, zData: [*c]const u8, nData: c.sqlite3_uint64, xDel: ?*const fn (?*anyopaque) callconv(.C) void, encoding: u8) c_int {
    return sqlite3_api.*.bind_text64.?(pStmt, i, zData, nData, xDel, encoding);
}
pub export fn sqlite3_cancel_auto_extension(xEntryPoint: ?*const fn () callconv(.C) void) c_int {
    return sqlite3_api.*.cancel_auto_extension.?(xEntryPoint);
}
pub export fn sqlite3_load_extension(db: ?*c.sqlite3, zFile: [*c]const u8, zProc: [*c]const u8, pzErrMsg: [*c][*c]u8) c_int {
    return sqlite3_api.*.load_extension.?(db, zFile, zProc, pzErrMsg);
}
pub export fn sqlite3_malloc64(n: c.sqlite3_uint64) ?*anyopaque {
    return sqlite3_api.*.malloc64.?(n);
}
pub export fn sqlite3_msize(p: ?*anyopaque) c.sqlite3_uint64 {
    return sqlite3_api.*.msize.?(p);
}
pub export fn sqlite3_realloc64(pOld: ?*anyopaque, n: c.sqlite3_uint64) ?*anyopaque {
    return sqlite3_api.*.realloc64.?(pOld, n);
}
pub export fn sqlite3_reset_auto_extension() void {
    return sqlite3_api.*.reset_auto_extension.?();
}
pub export fn sqlite3_result_blob64(pCtx: ?*c.sqlite3_context, z: ?*const anyopaque, n: c.sqlite3_uint64, xDel: ?*const fn (?*anyopaque) callconv(.C) void) void {
    return sqlite3_api.*.result_blob64.?(pCtx, z, n, xDel);
}
pub export fn sqlite3_result_text64(pCtx: ?*c.sqlite3_context, z: [*c]const u8, n: c.sqlite3_uint64, xDel: ?*const fn (?*anyopaque) callconv(.C) void, encoding: u8) void {
    return sqlite3_api.*.result_text64.?(pCtx, z, n, xDel, encoding);
}
pub export fn sqlite3_strglob(zGlob: [*c]const u8, zStr: [*c]const u8) c_int {
    return sqlite3_api.*.strglob.?(zGlob, zStr);
}
pub export fn sqlite3_value_dup(pOrig: ?*const c.sqlite3_value) ?*c.sqlite3_value {
    return sqlite3_api.*.value_dup.?(pOrig);
}
pub export fn sqlite3_value_free(pOld: ?*c.sqlite3_value) void {
    return sqlite3_api.*.value_free.?(pOld);
}
pub export fn sqlite3_result_zeroblob64(pCtx: ?*c.sqlite3_context, n: c.sqlite3_uint64) c_int {
    return sqlite3_api.*.result_zeroblob64.?(pCtx, n);
}
pub export fn sqlite3_bind_zeroblob64(pStmt: ?*c.sqlite3_stmt, i: c_int, n: c.sqlite3_uint64) c_int {
    return sqlite3_api.*.bind_zeroblob64.?(pStmt, i, n);
}
pub export fn sqlite3_value_subtype(pVal: ?*c.sqlite3_value) c_uint {
    return sqlite3_api.*.value_subtype.?(pVal);
}
pub export fn sqlite3_result_subtype(pCtx: ?*c.sqlite3_context, eSubtype: c_uint) void {
    return sqlite3_api.*.result_subtype.?(pCtx, eSubtype);
}
pub export fn sqlite3_status64(op: c_int, pCurrent: [*c]c.sqlite3_int64, pHighwater: [*c]c.sqlite3_int64, resetFlag: c_int) c_int {
    return sqlite3_api.*.status64.?(op, pCurrent, pHighwater, resetFlag);
}
pub export fn sqlite3_strlike(zGlob: [*c]const u8, zStr: [*c]const u8, cEsc: c_uint) c_int {
    return sqlite3_api.*.strlike.?(zGlob, zStr, cEsc);
}
pub export fn sqlite3_db_cacheflush(db: ?*c.sqlite3) c_int {
    return sqlite3_api.*.db_cacheflush.?(db);
}
pub export fn sqlite3_system_errno(db: ?*c.sqlite3) c_int {
    return sqlite3_api.*.system_errno.?(db);
}
pub export fn sqlite3_trace_v2(db: ?*c.sqlite3, uMask: c_uint, xCallback: ?*const fn (c_uint, ?*anyopaque, ?*anyopaque, ?*anyopaque) callconv(.C) c_int, pCtx: ?*anyopaque) c_int {
    return sqlite3_api.*.trace_v2.?(db, uMask, xCallback, pCtx);
}
pub export fn sqlite3_expanded_sql(pStmt: ?*c.sqlite3_stmt) [*c]u8 {
    return sqlite3_api.*.expanded_sql.?(pStmt);
}
pub export fn sqlite3_set_last_insert_rowid(db: ?*c.sqlite3, iRowid: c.sqlite3_int64) void {
    return sqlite3_api.*.set_last_insert_rowid.?(db, iRowid);
}
pub export fn sqlite3_prepare_v3(db: ?*c.sqlite3, zSql: [*c]const u8, nByte: c_int, prepFlags: c_uint, ppStmt: [*c]?*c.sqlite3_stmt, pzTail: [*c][*c]const u8) c_int {
    return sqlite3_api.*.prepare_v3.?(db, zSql, nByte, prepFlags, ppStmt, pzTail);
}
pub export fn sqlite3_prepare16_v3(db: ?*c.sqlite3, zSql: ?*const anyopaque, nByte: c_int, prepFlags: c_uint, ppStmt: [*c]?*c.sqlite3_stmt, pzTail: [*c]?*const anyopaque) c_int {
    return sqlite3_api.*.prepare16_v3.?(db, zSql, nByte, prepFlags, ppStmt, pzTail);
}
pub export fn sqlite3_bind_pointer(pStmt: ?*c.sqlite3_stmt, i: c_int, pPtr: ?*anyopaque, zPTtype: [*c]const u8, xDestructor: ?*const fn (?*anyopaque) callconv(.C) void) c_int {
    return sqlite3_api.*.bind_pointer.?(pStmt, i, pPtr, zPTtype, xDestructor);
}
pub export fn sqlite3_result_pointer(pCtx: ?*c.sqlite3_context, pPtr: ?*anyopaque, zPType: [*c]const u8, xDestructor: ?*const fn (?*anyopaque) callconv(.C) void) void {
    return sqlite3_api.*.result_pointer.?(pCtx, pPtr, zPType, xDestructor);
}
pub export fn sqlite3_value_pointer(pVal: ?*c.sqlite3_value, zPType: [*c]const u8) ?*anyopaque {
    return sqlite3_api.*.value_pointer.?(pVal, zPType);
}
pub export fn sqlite3_vtab_nochange(pCtx: ?*c.sqlite3_context) c_int {
    return sqlite3_api.*.vtab_nochange.?(pCtx);
}
pub export fn sqlite3_value_nochange(pVal: ?*c.sqlite3_value) c_int {
    return sqlite3_api.*.value_nochange.?(pVal);
}
pub export fn sqlite3_vtab_collation(pIdxInfo: [*c]c.sqlite3_index_info, iCons: c_int) [*c]const u8 {
    return sqlite3_api.*.vtab_collation.?(pIdxInfo, iCons);
}
pub export fn sqlite3_keyword_count() c_int {
    return sqlite3_api.*.keyword_count.?();
}
pub export fn sqlite3_keyword_name(i: c_int, pzName: [*c][*c]const u8, pnName: [*c]c_int) c_int {
    return sqlite3_api.*.keyword_name.?(i, pzName, pnName);
}
pub export fn sqlite3_keyword_check(zName: [*c]const u8, nName: c_int) c_int {
    return sqlite3_api.*.keyword_check.?(zName, nName);
}
pub export fn sqlite3_str_new(db: ?*c.sqlite3) ?*c.sqlite3_str {
    return sqlite3_api.*.str_new.?(db);
}
pub export fn sqlite3_str_finish(p: ?*c.sqlite3_str) [*c]u8 {
    return sqlite3_api.*.str_finish.?(p);
}
pub export fn sqlite3_str_append(p: ?*c.sqlite3_str, zIn: [*c]const u8, N: c_int) void {
    return sqlite3_api.*.str_append.?(p, zIn, N);
}
pub export fn sqlite3_str_appendall(p: ?*c.sqlite3_str, zIn: [*c]const u8) void {
    return sqlite3_api.*.str_appendall.?(p, zIn);
}
pub export fn sqlite3_str_appendchar(p: ?*c.sqlite3_str, N: c_int, C: u8) void {
    return sqlite3_api.*.str_appendchar.?(p, N, C);
}
pub export fn sqlite3_str_reset(p: ?*c.sqlite3_str) void {
    return sqlite3_api.*.str_reset.?(p);
}
pub export fn sqlite3_str_errcode(p: ?*c.sqlite3_str) c_int {
    return sqlite3_api.*.str_errcode.?(p);
}
pub export fn sqlite3_str_length(p: ?*c.sqlite3_str) c_int {
    return sqlite3_api.*.str_length.?(p);
}
pub export fn sqlite3_str_value(p: ?*c.sqlite3_str) [*c]u8 {
    return sqlite3_api.*.str_value.?(p);
}
pub export fn sqlite3_create_window_function(
    db: ?*c.sqlite3,
    zFunctionName: [*c]const u8,
    nArg: c_int,
    eTextRep: c_int,
    pArg: ?*anyopaque,
    xStep: ?*const fn (?*c.sqlite3_context, c_int, [*c]?*c.sqlite3_value) callconv(.C) void,
    xFinal: ?*const fn (?*c.sqlite3_context) callconv(.C) void,
    xValue: ?*const fn (?*c.sqlite3_context) callconv(.C) void,
    xInverse: ?*const fn (?*c.sqlite3_context, c_int, [*c]?*c.sqlite3_value) callconv(.C) void,
    xDestroy: ?*const fn (?*anyopaque) callconv(.C) void,
) c_int {
    return sqlite3_api.*.create_window_function.?(
        db,
        zFunctionName,
        nArg,
        eTextRep,
        pArg,
        xStep,
        xFinal,
        xValue,
        xInverse,
        xDestroy,
    );
}
pub export fn sqlite3_stmt_isexplain(pStmt: ?*c.sqlite3_stmt) c_int {
    return sqlite3_api.*.stmt_isexplain.?(pStmt);
}
pub export fn sqlite3_value_frombind(pVal: ?*c.sqlite3_value) c_int {
    return sqlite3_api.*.value_frombind.?(pVal);
}
pub export fn sqlite3_drop_modules(db: ?*c.sqlite3, azKeep: [*c][*c]const u8) c_int {
    return sqlite3_api.*.drop_modules.?(db, azKeep);
}
pub export fn sqlite3_hard_heap_limit64(N: c.sqlite3_int64) c.sqlite3_int64 {
    return sqlite3_api.*.hard_heap_limit64.?(N);
}
pub export fn sqlite3_uri_key(zFilename: [*c]const u8, N: c_int) [*c]const u8 {
    return sqlite3_api.*.uri_key.?(zFilename, N);
}
pub export fn sqlite3_filename_database(zFilename: [*c]const u8) [*c]const u8 {
    return sqlite3_api.*.filename_database.?(zFilename);
}
pub export fn sqlite3_filename_journal(zFilename: [*c]const u8) [*c]const u8 {
    return sqlite3_api.*.filename_journal.?(zFilename);
}
pub export fn sqlite3_filename_wal(zFilename: [*c]const u8) [*c]const u8 {
    return sqlite3_api.*.filename_wal.?(zFilename);
}
pub export fn sqlite3_create_filename(zDatabase: [*c]const u8, zJournal: [*c]const u8, zWal: [*c]const u8, nParam: c_int, azParam: [*c][*c]const u8) [*c]u8 {
    return sqlite3_api.*.create_filename.?(zDatabase, zJournal, zWal, nParam, azParam);
}
pub export fn sqlite3_free_filename(p: [*c]u8) void {
    return sqlite3_api.*.free_filename.?(p);
}
pub export fn sqlite3_database_file_object(zName: [*c]const u8) [*c]c.sqlite3_file {
    return sqlite3_api.*.database_file_object.?(zName);
}
pub export fn sqlite3_txn_state(db: ?*c.sqlite3, zSchema: [*c]const u8) c_int {
    return sqlite3_api.*.txn_state.?(db, zSchema);
}
pub export fn sqlite3_changes64(db: ?*c.sqlite3) c.sqlite3_int64 {
    return sqlite3_api.*.changes64.?(db);
}
pub export fn sqlite3_total_changes64(db: ?*c.sqlite3) c.sqlite3_int64 {
    return sqlite3_api.*.total_changes64.?(db);
}
pub export fn sqlite3_autovacuum_pages(db: ?*c.sqlite3, xCallback: ?*const fn (?*anyopaque, [*c]const u8, c_uint, c_uint, c_uint) callconv(.C) c_uint, pArg: ?*anyopaque, xDestructor: ?*const fn (?*anyopaque) callconv(.C) void) c_int {
    return sqlite3_api.*.autovacuum_pages.?(db, xCallback, pArg, xDestructor);
}
pub export fn sqlite3_error_offset(db: ?*c.sqlite3) c_int {
    return sqlite3_api.*.error_offset.?(db);
}
pub export fn sqlite3_vtab_rhs_value(pIdxInfo: [*c]c.sqlite3_index_info, iCons: c_int, ppVal: [*c]?*c.sqlite3_value) c_int {
    return sqlite3_api.*.vtab_rhs_value.?(pIdxInfo, iCons, ppVal);
}
pub export fn sqlite3_vtab_distinct(pIdxInfo: [*c]c.sqlite3_index_info) c_int {
    return sqlite3_api.*.vtab_distinct.?(pIdxInfo);
}
pub export fn sqlite3_vtab_in(pIdxInfo: [*c]c.sqlite3_index_info, iCons: c_int, bHandle: c_int) c_int {
    return sqlite3_api.*.vtab_in.?(pIdxInfo, iCons, bHandle);
}
pub export fn sqlite3_vtab_in_first(pVal: ?*c.sqlite3_value, ppOut: [*c]?*c.sqlite3_value) c_int {
    return sqlite3_api.*.vtab_in_first.?(pVal, ppOut);
}
pub export fn sqlite3_vtab_in_next(pVal: ?*c.sqlite3_value, ppOut: [*c]?*c.sqlite3_value) c_int {
    return sqlite3_api.*.vtab_in_next.?(pVal, ppOut);
}
pub export fn sqlite3_deserialize(db: ?*c.sqlite3, zSchema: [*c]const u8, pData: [*c]u8, szDb: c.sqlite3_int64, szBuf: c.sqlite3_int64, mFlags: c_uint) c_int {
    return sqlite3_api.*.deserialize.?(db, zSchema, pData, szDb, szBuf, mFlags);
}
pub export fn sqlite3_serialize(db: ?*c.sqlite3, zSchema: [*c]const u8, piSize: [*c]c.sqlite3_int64, mFlags: c_uint) [*c]u8 {
    return sqlite3_api.*.serialize.?(db, zSchema, piSize, mFlags);
}
pub export fn sqlite3_db_name(db: ?*c.sqlite3, N: c_int) [*c]const u8 {
    return sqlite3_api.*.db_name.?(db, N);
}
