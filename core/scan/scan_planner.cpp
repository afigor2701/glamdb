#include "core/scan/scan_planner.h"

#include "core/api/binary.h"
#include "core/api/canonical.h"
#include "core/backend/storage_errors.h"
#include "core/scan/buffer_file.h"
#include "core/storage/capacity.h"
#include "core/storage/format_registry.h"
#include "core/storage/format_verifier.h"

#include <iceberg/constants.h>
#include <iceberg/file_format.h>
#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/manifest/manifest_list.h>
#include <iceberg/manifest/manifest_reader.h>
#include <iceberg/partition_spec.h>
#include <iceberg/snapshot.h>
#include <iceberg/table_metadata.h>

#include <new>

namespace glamdb::detail {
namespace {

// Every path read here comes from committed metadata: malformed or missing
// persisted state is corruption.
template <class T>
T Take(iceberg::Result<T> result, std::string_view code, std::string_view context)
{
    if (!result) {
        throw Failure(StorageError(result.error(), context, EStoragePath::Read, code));
    }
    return std::move(*result);
}

// Upstream Avro/Iceberg manifest decoders may throw avro::Exception (derived from
// std::exception) on malformed persisted files instead of returning iceberg::Status.
template <class TFunction>
decltype(auto) DecodePersisted(std::string_view context, TFunction&& fn)
{
    try {
        return fn();
    } catch (const Failure&) {
        throw;
    } catch (const std::bad_alloc& error) {
        throw Failure({"RESOURCE_EXHAUSTED",
            BoundedDiagnostic(std::string(context) + ": " + error.what()),
            RetryHint::OperatorAction});
    } catch (const std::exception& error) {
        throw Failure(
            {"CORRUPTION", BoundedDiagnostic(std::string(context) + ": " + error.what()), RetryHint::OperatorAction});
    }
}

// Reads a committed Avro file through the store FileIO before decoding it.
// The upstream Avro stream treats a read error as end of input, which would
// make a transient I/O failure look like a malformed file; decoding from
// memory leaves only genuine format errors to the decoder.
std::shared_ptr<iceberg::FileIO> ReadPersisted(const std::shared_ptr<iceberg::FileIO>& io,
    const std::string& location,
    std::optional<int64_t> length,
    std::string_view context)
{
    Require(!length || *length > 0, "CORRUPTION", "committed file has an invalid length");
    auto bytes = Take(io->ReadFile(location, length ? std::optional(static_cast<size_t>(*length)) : std::nullopt),
        "IO_ERROR",
        context);
    Require(!length || static_cast<int64_t>(bytes.size()) == *length,
        "CORRUPTION",
        "committed file length differs from its metadata");
    return std::make_shared<TBufferFileIO>(location, std::move(bytes));
}

[[noreturn]] void Unsupported(std::string_view message)
{
    throw Failure({"UNSUPPORTED_TABLE_FEATURE", std::string(message), RetryHint::OperatorAction});
}

std::shared_ptr<iceberg::Snapshot> SelectSnapshot(const iceberg::TableMetadata& metadata,
    const SnapshotSelector& selector)
{
    switch (selector.kind) {
        case SnapshotSelectorKind::Latest:
            if (metadata.current_snapshot_id == iceberg::kInvalidSnapshotId) {
                return nullptr;
            }
            return Take(metadata.Snapshot(), "CORRUPTION", "current snapshot");
        case SnapshotSelectorKind::Id: {
            auto snapshot = metadata.SnapshotById(selector.snapshot_id);
            Require(snapshot.has_value(), "NOT_FOUND", "snapshot is not retained by this collection");
            return *snapshot;
        }
        case SnapshotSelectorKind::InitialEmpty:
            return nullptr;
    }
    throw Failure({"INVALID_ARGUMENT", "unknown snapshot selector", RetryHint::FixInput});
}

void CheckRoot(const iceberg::TableMetadata& metadata, const BaseIdentity& base)
{
    const auto current = metadata.current_snapshot_id == iceberg::kInvalidSnapshotId
        ? std::nullopt
        : std::optional(metadata.current_snapshot_id);
    Require(metadata.table_uuid == base.table_uuid && current == base.current_snapshot_id,
        "CORRUPTION",
        "committed root does not match the catalog identity");
    Require(metadata.format_version == 2 && metadata.current_schema_id == base.schema_id &&
            metadata.default_spec_id == base.spec_id,
        "UNSUPPORTED_TABLE_FEATURE",
        "only the initial unpartitioned Iceberg v2 schema is supported");
}

std::vector<std::shared_ptr<iceberg::DataFile>> ReadDataFiles(const TScanManifests& plan,
    const std::shared_ptr<iceberg::FileIO>& io,
    const iceberg::ManifestFile& manifest)
{
    if (manifest.content != iceberg::ManifestContent::kData) {
        Unsupported("delete manifests are not supported");
    }
    const auto spec = std::ranges::find_if(plan.Specs, [&](const auto& item) {
        return item && item->spec_id() == manifest.partition_spec_id;
    });
    Require(spec != plan.Specs.end(), "CORRUPTION", "partition spec");
    if (!(*spec)->IsUnpartitioned()) {
        Unsupported("partitioned manifests are not supported");
    }
    const auto bytes = ReadPersisted(io, manifest.manifest_path, manifest.manifest_length, "read manifest");
    auto reader = DecodePersisted("open manifest", [&] {
        return Take(iceberg::ManifestReader::Make(manifest, bytes, plan.Schema, *spec), "IO_ERROR", "open manifest");
    });
    const auto entries = DecodePersisted("read manifest", [&] {
        return Take(reader->LiveEntries(), "IO_ERROR", "read manifest");
    });
    std::vector<std::shared_ptr<iceberg::DataFile>> files;
    for (const auto& entry : entries) {
        const auto& file = entry.data_file;
        Require(file != nullptr, "CORRUPTION", "manifest entry has no data file");
        if (file->content != iceberg::DataFile::Content::kData ||
            file->file_format != iceberg::FileFormatType::kParquet)
        {
            Unsupported("only Parquet data files are supported");
        }
        Require(file->file_size_in_bytes > 0 && file->record_count > 0,
            "CORRUPTION",
            "manifest has invalid data file metrics");
        files.push_back(file);
    }
    return files;
}

} // namespace

Result<std::unique_ptr<iceberg::TableMetadata>> ReadScanRoot(const std::shared_ptr<iceberg::FileIO>& io,
    const BaseIdentity& base)
{
    try {
        Require(io != nullptr, "DEPENDENCY_UNAVAILABLE", "storage reader is not installed");
        NGlamDB::NStorage::RegisterStorageFormats();
        const auto limit = static_cast<int64_t>(NGlamDB::NStorage::TStorageLimits{}.RootBytes);
        auto metadata = Take(NGlamDB::NStorage::ReadTableMetadata(*io, base.metadata_location, std::nullopt, limit),
            "IO_ERROR",
            "read committed root");
        CheckRoot(*metadata, base);
        return metadata;
    } catch (const Failure& error) {
        return std::unexpected(error.detail);
    } catch (const std::bad_alloc& error) {
        return std::unexpected(
            ErrorDetail{"RESOURCE_EXHAUSTED", BoundedDiagnostic(error.what()), RetryHint::OperatorAction});
    } catch (const std::exception& error) {
        return std::unexpected(ErrorDetail{"INTERNAL", BoundedDiagnostic(error.what()), RetryHint::OperatorAction});
    }
}

Result<TScanManifests>
PlanManifests(const std::shared_ptr<iceberg::FileIO>& io, const BaseIdentity& base, const SnapshotSelector& selector)
{
    try {
        const auto root = ReadScanRoot(io, base);
        if (!root) {
            return std::unexpected(root.error());
        }
        const auto& metadata = *root;
        TScanManifests plan;
        const auto snapshot = SelectSnapshot(*metadata, selector);
        if (!snapshot) {
            return plan;
        }
        plan.SnapshotID = snapshot->snapshot_id;
        plan.Schema = Take(metadata->Schema(), "CORRUPTION", "table schema");
        plan.Specs = metadata->partition_specs;
        const auto listBytes = ReadPersisted(io, snapshot->manifest_list, std::nullopt, "read manifest list");
        const auto list = DecodePersisted("open manifest list", [&] {
            return Take(iceberg::ManifestListReader::Make(snapshot->manifest_list, listBytes),
                "IO_ERROR",
                "open manifest list");
        });
        plan.Manifests = DecodePersisted("read manifest list", [&] {
            return Take(list->Files(), "IO_ERROR", "read manifest list");
        });
        return plan;
    } catch (const Failure& error) {
        return std::unexpected(error.detail);
    } catch (const std::bad_alloc& error) {
        return std::unexpected(
            ErrorDetail{"RESOURCE_EXHAUSTED", BoundedDiagnostic(error.what()), RetryHint::OperatorAction});
    } catch (const std::exception& error) {
        return std::unexpected(ErrorDetail{"INTERNAL", BoundedDiagnostic(error.what()), RetryHint::OperatorAction});
    }
}

Result<std::vector<TScanFile>>
ManifestFiles(const std::shared_ptr<iceberg::FileIO>& io, const TScanManifests& plan, size_t index)
{
    auto data = ReadManifestDataFiles(io, plan, index);
    if (!data) {
        return std::unexpected(data.error());
    }
    try {
        std::vector<TScanFile> files;
        files.reserve(data->size());
        for (const auto& file : *data) {
            files.push_back({file->file_path, file->file_size_in_bytes, file->record_count});
        }
        return files;
    } catch (const std::bad_alloc& error) {
        return std::unexpected(
            ErrorDetail{"RESOURCE_EXHAUSTED", BoundedDiagnostic(error.what()), RetryHint::OperatorAction});
    }
}

Result<std::vector<std::shared_ptr<iceberg::DataFile>>>
ReadManifestDataFiles(const std::shared_ptr<iceberg::FileIO>& io, const TScanManifests& plan, size_t index)
{
    try {
        Require(io != nullptr, "DEPENDENCY_UNAVAILABLE", "storage reader is not installed");
        Require(index < plan.Manifests.size(), "INTERNAL", "manifest index is out of range");
        return ReadDataFiles(plan, io, plan.Manifests[index]);
    } catch (const Failure& error) {
        return std::unexpected(error.detail);
    } catch (const std::bad_alloc& error) {
        return std::unexpected(
            ErrorDetail{"RESOURCE_EXHAUSTED", BoundedDiagnostic(error.what()), RetryHint::OperatorAction});
    } catch (const std::exception& error) {
        return std::unexpected(ErrorDetail{"INTERNAL", BoundedDiagnostic(error.what()), RetryHint::OperatorAction});
    }
}

Result<TScanPlan>
PlanScan(const std::shared_ptr<iceberg::FileIO>& io, const BaseIdentity& base, const SnapshotSelector& selector)
{
    auto manifests = PlanManifests(io, base, selector);
    if (!manifests) {
        return std::unexpected(manifests.error());
    }
    TScanPlan plan{manifests->SnapshotID, {}};
    for (size_t index = 0; index < manifests->Manifests.size(); ++index) {
        auto files = ManifestFiles(io, *manifests, index);
        if (!files) {
            return std::unexpected(files.error());
        }
        plan.Files.insert(plan.Files.end(), files->begin(), files->end());
    }
    return plan;
}

} // namespace glamdb::detail
