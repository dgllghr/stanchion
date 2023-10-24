// TODO remove when issue is resolved:
//      https://github.com/ziglang/zig/issues/15893

#include "result-transient.h"

void sqlite3_result_blob_transient(sqlite3_context* ctx, const void* blob, int n) {
    sqlite3_result_blob(ctx, blob, n, SQLITE_TRANSIENT);
}

void sqlite3_result_text_transient(sqlite3_context* ctx, const void* blob, int n) {
    sqlite3_result_text(ctx, blob, n, SQLITE_TRANSIENT);
}
