#include "core/scan/scan_planner.h"

#include "core/api/binary.h"
#include "core/api/canonical.h"
#include "core/backend/storage_errors.h"
#include "core/scan/buffer_file.h"
#include "core/scan/metadata_cache.h"
#include "core/storage/base_state.h"
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

std::shared_ptr<const std::vector<std::shared_ptr<const iceberg::DataFile>>> ReadDataFiles(const TScanManifests& plan,
    const std::shared_ptr<iceberg::FileIO>& io,
    const iceberg::ManifestFile& manifest,
    TSnapshotMetadataCache* cache)
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
    Require(manifest.manifest_length > 0, "CORRUPTION", "committed file has an invalid length");
    if (cache != nullptr) {
        if (auto cached = cache->FindManifest(manifest.manifest_path, manifest.manifest_length)) {
            return cached;
        }
    }
    const auto bytes = ReadPersisted(io, manifest.manifest_path, manifest.manifest_length, "read manifest");
    auto reader = DecodePersisted("open manifest", [&] {
        return Take(iceberg::ManifestReader::Make(manifest, bytes, plan.Schema, *spec), "IO_ERROR", "open manifest");
    });
    const auto entries = DecodePersisted("read manifest", [&] {
        return Take(reader->LiveEntries(), "IO_ERROR", "read manifest");
    });
    std::vector<std::shared_ptr<const iceberg::DataFile>> files;
    files.reserve(entries.size());
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
    auto sharedFiles = std::make_shared<const std::vector<std::shared_ptr<const iceberg::DataFile>>>(std::move(files));
    if (cache != nullptr) {
        cache->TryPutManifest(manifest.manifest_path, manifest.manifest_length, sharedFiles);
    }
    return sharedFiles;
}

} // namespace

Result<std::shared_ptr<const iceberg::TableMetadata>>
ReadScanRoot(const std::shared_ptr<iceberg::FileIO>& io, const BaseIdentity& base, TSnapshotMetadataCache* cache)
{
    try {
        Require(io != nullptr, "DEPENDENCY_UNAVAILABLE", "storage reader is not installed");
        NGlamDB::NStorage::RegisterStorageFormats();
        const auto limit = static_cast<int64_t>(NGlamDB::NStorage::TStorageLimits{}.RootBytes);
        if (cache != nullptr) {
            if (!base.metadata_location.ends_with(".metadata.json")) {
                throw Failure(StorageError(
                    iceberg::InvalidArgument("Metadata verification requires an uncompressed JSON file.").error(),
                    "read committed root",
                    EStoragePath::Read,
                    "IO_ERROR"));
            }
            auto file = Take(io->NewInputFile(base.metadata_location), "IO_ERROR", "read committed root");
            const auto actualSize = Take(file->Size(), "IO_ERROR", "read committed root");
            if (actualSize >= 0 && actualSize <= limit) {
                if (auto cached = cache->FindRoot(base.metadata_location, actualSize)) {
                    CheckRoot(*cached, base);
                    return cached;
                }
            }
            auto metadata =
                Take(NGlamDB::NStorage::ReadTableMetadataFromInput(*file, base.metadata_location, actualSize, limit),
                    "IO_ERROR",
                    "read committed root");
            CheckRoot(*metadata, base);
            std::shared_ptr<const iceberg::TableMetadata> shared(std::move(metadata));
            cache->TryPutRoot(base.metadata_location, actualSize, shared);
            return shared;
        }
        auto metadata = Take(NGlamDB::NStorage::ReadTableMetadata(*io, base.metadata_location, std::nullopt, limit),
            "IO_ERROR",
            "read committed root");
        CheckRoot(*metadata, base);
        return std::shared_ptr<const iceberg::TableMetadata>(std::move(metadata));
    } catch (const Failure& error) {
        return std::unexpected(error.detail);
    } catch (const std::bad_alloc& error) {
        return std::unexpected(
            ErrorDetail{"RESOURCE_EXHAUSTED", BoundedDiagnostic(error.what()), RetryHint::OperatorAction});
    } catch (const std::exception& error) {
        return std::unexpected(ErrorDetail{"INTERNAL", BoundedDiagnostic(error.what()), RetryHint::OperatorAction});
    }
}

Result<TScanManifests> PlanManifests(const std::shared_ptr<iceberg::FileIO>& io,
    const BaseIdentity& base,
    const SnapshotSelector& selector,
    TSnapshotMetadataCache* cache)
{
    try {
        const auto root = ReadScanRoot(io, base, cache);
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
        if (cache != nullptr) {
            if (auto cachedList = cache->FindManifestList(snapshot->manifest_list)) {
                plan.Manifests = *cachedList;
                return plan;
            }
        }
        const auto listBytes = ReadPersisted(io, snapshot->manifest_list, std::nullopt, "read manifest list");
        const auto list = DecodePersisted("open manifest list", [&] {
            return Take(iceberg::ManifestListReader::Make(snapshot->manifest_list, listBytes),
                "IO_ERROR",
                "open manifest list");
        });
        auto manifests =
            std::make_shared<const std::vector<iceberg::ManifestFile>>(DecodePersisted("read manifest list", [&] {
                return Take(list->Files(), "IO_ERROR", "read manifest list");
            }));
        if (cache != nullptr) {
            cache->TryPutManifestList(snapshot->manifest_list, manifests);
        }
        plan.Manifests = *manifests;
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

Result<TScanManifests>
PlanVerifiedScan(const NGlamDB::NStorage::TBaseState& state, const BaseIdentity& base, const SnapshotSelector& selector)
{
    try {
        Require(state.Metadata && state.ScanIndex && state.MetadataLocation == base.metadata_location &&
                state.Metadata->location == "store://" + base.store_id + "/warehouse/" + base.collection_id,
            "CORRUPTION",
            "scan index does not match the confirmed root");
        CheckRoot(*state.Metadata, base);
        TScanManifests plan;
        plan.Index = state.ScanIndex;
        switch (selector.kind) {
            case SnapshotSelectorKind::Latest:
                plan.SnapshotID = base.current_snapshot_id;
                break;
            case SnapshotSelectorKind::Id:
                plan.SnapshotID = selector.snapshot_id;
                break;
            case SnapshotSelectorKind::InitialEmpty:
                break;
            default:
                throw Failure({"INVALID_ARGUMENT", "unknown snapshot selector", RetryHint::FixInput});
        }
        if (plan.SnapshotID) {
            const auto boundary = std::ranges::lower_bound(plan.Index->Snapshots,
                *plan.SnapshotID,
                {},
                &NGlamDB::NStorage::TSnapshotBoundary::SnapshotID);
            Require(boundary != plan.Index->Snapshots.end() && boundary->SnapshotID == *plan.SnapshotID,
                selector.kind == SnapshotSelectorKind::Id ? "NOT_FOUND" : "CORRUPTION",
                "snapshot is not retained by the verified scan index");
            Require(boundary->FileCount <= plan.Index->Files.size(),
                "CORRUPTION",
                "selected snapshot has an invalid membership boundary");
            plan.FileCount = boundary->FileCount;
        }
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

Result<std::vector<TScanFile>> ManifestFiles(const std::shared_ptr<iceberg::FileIO>& io,
    const TScanManifests& plan,
    size_t index,
    TSnapshotMetadataCache* cache)
{
    auto data = ReadManifestDataFiles(io, plan, index, cache);
    if (!data) {
        return std::unexpected(data.error());
    }
    try {
        std::vector<TScanFile> files;
        files.reserve((*data)->size());
        for (const auto& file : **data) {
            files.push_back({file->file_path, file->file_size_in_bytes, file->record_count, file});
        }
        return files;
    } catch (const std::bad_alloc& error) {
        return std::unexpected(
            ErrorDetail{"RESOURCE_EXHAUSTED", BoundedDiagnostic(error.what()), RetryHint::OperatorAction});
    }
}

Result<std::shared_ptr<const std::vector<std::shared_ptr<const iceberg::DataFile>>>> ReadManifestDataFiles(
    const std::shared_ptr<iceberg::FileIO>& io,
    const TScanManifests& plan,
    size_t index,
    TSnapshotMetadataCache* cache)
{
    try {
        Require(io != nullptr, "DEPENDENCY_UNAVAILABLE", "storage reader is not installed");
        Require(index < plan.Manifests.size(), "INTERNAL", "manifest index is out of range");
        return ReadDataFiles(plan, io, plan.Manifests[index], cache);
    } catch (const Failure& error) {
        return std::unexpected(error.detail);
    } catch (const std::bad_alloc& error) {
        return std::unexpected(
            ErrorDetail{"RESOURCE_EXHAUSTED", BoundedDiagnostic(error.what()), RetryHint::OperatorAction});
    } catch (const std::exception& error) {
        return std::unexpected(ErrorDetail{"INTERNAL", BoundedDiagnostic(error.what()), RetryHint::OperatorAction});
    }
}

Result<TScanPlan> PlanScan(const std::shared_ptr<iceberg::FileIO>& io,
    const BaseIdentity& base,
    const SnapshotSelector& selector,
    TSnapshotMetadataCache* cache)
{
    auto manifests = PlanManifests(io, base, selector, cache);
    if (!manifests) {
        return std::unexpected(manifests.error());
    }
    TScanPlan plan{manifests->SnapshotID, {}};
    for (size_t index = 0; index < manifests->Manifests.size(); ++index) {
        auto files = ManifestFiles(io, *manifests, index, cache);
        if (!files) {
            return std::unexpected(files.error());
        }
        plan.Files.insert(plan.Files.end(), files->begin(), files->end());
    }
    return plan;
}

} // namespace glamdb::detail
