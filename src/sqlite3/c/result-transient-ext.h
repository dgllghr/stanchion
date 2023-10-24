// TODO remove when issue is resolved:
//      https://github.com/ziglang/zig/issues/15893

#include "loadable-ext-sqlite3ext.h";

void sqlite3_result_blob_transient_wrapper(
  sqlite3_api_routines*,
  sqlite3_context*,
  const void*,
  int
);

void sqlite3_result_text_transient_wrapper(
  sqlite3_api_routines*,
  sqlite3_context*,
  const void*,
  int
);