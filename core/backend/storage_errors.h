#pragma once

#include "core/api/canonical.h"

#include <iceberg/result.h>

#include <string_view>
#include <system_error>

namespace glamdb::detail {

// Where a storage failure was observed. Every path read on the Read side comes
// from committed metadata, so malformed or missing persisted state there is
// corruption. Preflight inspects the warehouse tree before an operation binds.
enum class EStoragePath {
    Read,
    Write,
    Preflight,
};

// Maps a filesystem error to an engine error code. See SCANNER.md.
std::string_view ClassifyFsError(std::error_code error, EStoragePath path);

ErrorDetail StorageError(std::error_code error, EStoragePath path, std::string_view context);

// Maps an Iceberg error from storage. The storage module marks capacity and
// corruption failures with the CAPACITY:/CORRUPTION: message prefixes.
// `fallback` is used when neither the prefix nor the kind is decisive.
ErrorDetail
StorageError(const iceberg::Error& error, EStoragePath path, std::string_view context, std::string_view fallback);

} // namespace glamdb::detail
