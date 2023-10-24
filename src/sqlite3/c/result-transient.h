// TODO remove when issue is resolved:
//      https://github.com/ziglang/zig/issues/15893

SQLITE_API void sqlite3_result_blob_transient(sqlite3_context*, const void*, int);

SQLITE_API void sqlite3_result_text_transient(sqlite3_context*, const void*, int);