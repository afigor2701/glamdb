#include "core/backend/storage_errors.h"

#include "core/storage/iceberg_error_boundary.h"

#include <string>

namespace glamdb::detail {
namespace {

bool IsPersistedDecodeKind(iceberg::ErrorKind kind)
{
    switch (kind) {
        case iceberg::ErrorKind::kInvalid:
        case iceberg::ErrorKind::kInvalidArgument:
        case iceberg::ErrorKind::kNotFound:
        case iceberg::ErrorKind::kValidationFailed:
        case iceberg::ErrorKind::kDecompressError:
        case iceberg::ErrorKind::kUnknownError:
        case iceberg::ErrorKind::kInvalidManifest:
        case iceberg::ErrorKind::kInvalidManifestList:
        case iceberg::ErrorKind::kInvalidSchema:
        case iceberg::ErrorKind::kJsonParseError:
            return true;
        default:
            return false;
    }
}

RetryHint HintFor(std::string_view code)
{
    return code == "INVALID_ARGUMENT" ? RetryHint::FixInput : RetryHint::OperatorAction;
}

} // namespace

std::string_view ClassifyFsError(std::error_code error, EStoragePath path)
{
    if (error == std::errc::state_not_recoverable || error == std::errc::too_many_symbolic_link_levels ||
        error == std::errc::not_a_directory || error == std::errc::cross_device_link ||
        error == std::errc::no_such_file_or_directory)
    {
        return "CORRUPTION";
    }
    if (error == std::errc::invalid_argument) {
        // Preflight: a symlink or special file in the warehouse tree. Read: a
        // committed file that is not a regular file.
        return path == EStoragePath::Write ? "IO_ERROR" : "CORRUPTION";
    }
    if (error == std::errc::file_too_large) {
        return path == EStoragePath::Read ? "CORRUPTION" : "RESOURCE_EXHAUSTED";
    }
    if (error == std::errc::no_space_on_device || error == std::errc::value_too_large ||
        error == std::errc::not_enough_memory)
    {
        return "RESOURCE_EXHAUSTED";
    }
    return "IO_ERROR";
}

ErrorDetail StorageError(std::error_code error, EStoragePath path, std::string_view context)
{
    const auto code = ClassifyFsError(error, path);
    return {std::string(code), BoundedDiagnostic(std::string(context) + ": " + error.message()), HintFor(code)};
}

ErrorDetail
StorageError(const iceberg::Error& error, EStoragePath path, std::string_view context, std::string_view fallback)
{
    const std::string_view message(error.message);
    std::string_view code = fallback;
    if (message.starts_with(NGlamDB::NStorage::CorruptionPrefix)) {
        code = "CORRUPTION";
    } else if (path == EStoragePath::Read) {
        // Retained state that no longer fits its limits cannot be served
        // either: it fails closed like any other invariant violation.
        if (IsPersistedDecodeKind(error.kind) || message.starts_with(NGlamDB::NStorage::CapacityPrefix)) {
            code = "CORRUPTION";
        }
    } else if (message.starts_with(NGlamDB::NStorage::CapacityPrefix)) {
        code = "RESOURCE_EXHAUSTED";
    }
    return {std::string(code), BoundedDiagnostic(std::string(context) + ": " + error.message), HintFor(code)};
}

} // namespace glamdb::detail
