// TODO remove when issue is resolved:
//      https://github.com/ziglang/zig/issues/15893

#include "loadable-ext-sqlite3ext.h"
#include "result-transient-ext.h"

void sqlite3_result_blob_transient_wrapper(
  sqlite3_api_routines* api,
  sqlite3_context* ctx,
  const void* bytes,
  int n
) {
  api->result_blob(ctx, bytes, n, SQLITE_TRANSIENT);
}

void sqlite3_result_text_transient_wrapper(
  sqlite3_api_routines* api,
  sqlite3_context* ctx,
  const void* bytes,
  int n
) {
  api->result_text(ctx, bytes, n, SQLITE_TRANSIENT);
}