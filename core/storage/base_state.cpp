#include "core/storage/base_state.h"
#include "core/io/io_attempt.h"
#include "core/io/store_uri.h"
#include "core/storage/format_registry.h"
#include "core/storage/format_verifier.h"
#include "core/storage/iceberg_error_boundary.h"
#include "core/storage/metadata_validation.h"
#include "core/storage/recovery_visited.h"
#include "core/storage/table_profile.h"

#include <iceberg/constants.h>
#include <iceberg/file_io.h>
#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/manifest/manifest_reader.h>
#include <iceberg/schema.h>
#include <iceberg/snapshot.h>
#include <iceberg/table_metadata.h>

#include <algorithm>
#include <limits>
#include <set>
#include <span>
#include <unordered_set>

namespace NGlamDB::NStorage {
using iceberg::Error;
namespace {

iceberg::Status CheckPath(std::string_view path, const NIO::TStoreUri& table, NIO::EFileKind kind)
{
    const auto parsed = NIO::TStoreUri::Parse(path, table.StoreId());
    if (!parsed || parsed.Value().CollectionId() != table.CollectionId() || parsed.Value().Kind() != kind) {
        return iceberg::ValidationFailed("Invalid reachable file path: {}.", path);
    }
    return {};
}

iceberg::Status CheckReceipt(const std::shared_ptr<NIO::TIOAttempt>& attempt,
    const std::string& path,
    NIO::EFileKind kind,
    std::int64_t length,
    bool requireFormat = false)
{
    if (length < 0) {
        return iceberg::ValidationFailed("Negative file length: {}.", path);
    }
    if (!attempt) {
        return {};
    }
    const auto record = attempt->Record(path);
    if (!record) {
        return iceberg::ValidationFailed("Missing verified durable receipt: {}.", path);
    }
    const auto& entry = record.Value();
    const auto& receipt = entry.Receipt;
    if (!receipt || entry.Kind != kind || entry.State != NIO::EFileState::DirSynced || entry.FirstError ||
        receipt->Length != static_cast<std::uint64_t>(length) || receipt->AttemptId != attempt->Id() ||
        (requireFormat && !entry.FormatVerified))
    {
        return iceberg::ValidationFailed("Missing verified durable receipt: {}.", path);
    }
    return {};
}

iceberg::Status MarkVerified(const std::shared_ptr<NIO::TIOAttempt>& attempt, const std::string& path)
{
    if (attempt) {
        if (auto error = attempt->MarkFormatVerified(path)) {
            return iceberg::IOError("Cannot verify {}: {}.", path, error.message());
        }
    }
    return {};
}

iceberg::Result<std::uint64_t> CheckAvro(iceberg::FileIO& fileIO,
    const std::string& path,
    std::int64_t size,
    NIO::EFileKind kind,
    std::uint64_t limit,
    std::uint64_t maxRecords,
    const std::shared_ptr<NIO::TIOAttempt>& attempt)
{
    if (limit > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()) ||
        maxRecords > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()))
    {
        return iceberg::InvalidArgument("Invalid verification limits.");
    }
    auto input = size < 0 ? fileIO.NewInputFile(path) : fileIO.NewInputFile(path, size);
    if (!input) {
        return std::unexpected(std::move(input.error()));
    }
    ICEBERG_ASSIGN_OR_RAISE(auto actual, (*input)->Size());
    ICEBERG_RETURN_UNEXPECTED(CheckReceipt(attempt, path, kind, actual));
    ICEBERG_RETURN_UNEXPECTED(CheckAvroContainer(**input,
        actual,
        {static_cast<std::int64_t>(limit), static_cast<std::int64_t>(maxRecords), static_cast<std::int64_t>(limit)}));
    return static_cast<std::uint64_t>(actual);
}

iceberg::Result<std::uint64_t> CheckData(const std::shared_ptr<iceberg::FileIO>& fileIO,
    const iceberg::DataFile& file,
    const iceberg::Schema& schema,
    const NIO::TStoreUri& table,
    const std::shared_ptr<NIO::TIOAttempt>& attempt)
{
    if (file.content != iceberg::DataFile::Content::kData || file.file_format != iceberg::FileFormatType::kParquet ||
        file.record_count <= 0 || file.partition.num_fields() != 0 || !file.key_metadata.empty())
    {
        return iceberg::ValidationFailed("Invalid append data file.");
    }
    ICEBERG_RETURN_UNEXPECTED(CheckPath(file.file_path, table, NIO::EFileKind::Data));
    ICEBERG_RETURN_UNEXPECTED(
        CheckReceipt(attempt, file.file_path, NIO::EFileKind::Data, file.file_size_in_bytes, true));
    ICEBERG_ASSIGN_OR_RAISE(auto input, fileIO->NewInputFile(file.file_path, file.file_size_in_bytes));
    ICEBERG_ASSIGN_OR_RAISE(auto format, CheckParquet(*input, schema, {file.file_size_in_bytes, file.record_count}));
    return format.FooterBytes;
}

iceberg::Result<TStorageUsage> CheckManifest(const std::shared_ptr<iceberg::FileIO>& fileIO,
    const iceberg::ManifestFile& manifest,
    const iceberg::Snapshot& snapshot,
    const iceberg::TableMetadata& metadata,
    const NIO::TStoreUri& table,
    const std::shared_ptr<NIO::TIOAttempt>& attempt,
    const TStorageLimits& limits,
    TRecoveryVisited* visited,
    TScanMembership* scanIndex)
{
    if (manifest.content != iceberg::ManifestContent::kData || manifest.partition_spec_id != metadata.default_spec_id ||
        manifest.added_snapshot_id != snapshot.snapshot_id || manifest.sequence_number != snapshot.sequence_number ||
        manifest.min_sequence_number != snapshot.sequence_number || !manifest.partitions.empty() ||
        !manifest.key_metadata.empty() || manifest.first_row_id)
    {
        return iceberg::ValidationFailed("Invalid append manifest.");
    }
    ICEBERG_RETURN_UNEXPECTED(CheckPath(manifest.manifest_path, table, NIO::EFileKind::Manifest));
    ICEBERG_ASSIGN_OR_RAISE(auto bytes,
        CheckAvro(*fileIO,
            manifest.manifest_path,
            manifest.manifest_length,
            NIO::EFileKind::Manifest,
            limits.ManifestBytes,
            1,
            attempt));
    ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());
    ICEBERG_ASSIGN_OR_RAISE(auto spec, metadata.PartitionSpec());
    ICEBERG_ASSIGN_OR_RAISE(auto reader, iceberg::ManifestReader::Make(manifest, fileIO, schema, spec));
    ICEBERG_ASSIGN_OR_RAISE(auto entries, reader->Entries());
    if (entries.size() != 1 || !entries.front().data_file) {
        return iceberg::ValidationFailed("An append manifest must contain one data file.");
    }
    const auto& entry = entries.front();
    if (entry.status != iceberg::ManifestStatus::kAdded || entry.snapshot_id != snapshot.snapshot_id ||
        entry.sequence_number != snapshot.sequence_number || entry.file_sequence_number != snapshot.sequence_number ||
        manifest.added_files_count != 1 || manifest.existing_files_count != 0 || manifest.deleted_files_count != 0 ||
        manifest.added_rows_count != entry.data_file->record_count || manifest.existing_rows_count != 0 ||
        manifest.deleted_rows_count != 0)
    {
        return iceberg::ValidationFailed("Manifest counters or entry inheritance differ.");
    }
    if (visited) {
        ICEBERG_ASSIGN_OR_RAISE(auto inserted, visited->Insert(NIO::EFileKind::Data, entry.data_file->file_path));
        if (!inserted) {
            return iceberg::ValidationFailed("Append reuses a retained data file.");
        }
    }
    ICEBERG_ASSIGN_OR_RAISE(auto footerBytes, CheckData(fileIO, *entry.data_file, *schema, table, attempt));
    ICEBERG_RETURN_UNEXPECTED(MarkVerified(attempt, manifest.manifest_path));
    if (scanIndex) {
        const auto& file = *entry.data_file;
        scanIndex->Files.push_back({file.file_path, file.file_size_in_bytes, file.record_count, entry.data_file});
    }
    TStorageUsage delta;
    delta.DataFiles = 1;
    delta.Manifests = 1;
    delta.ManifestBytes = bytes;
    delta.FooterBytes = footerBytes;
    return delta;
}

iceberg::Status CheckRecoveryProfile(const iceberg::TableMetadata& metadata)
{
    ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());
    return CheckCreateMetadata(metadata, TTableProfile{schema, metadata.properties.configs()}, metadata.location);
}

iceberg::Result<TBaseState> ReadRecoveryRoot(const std::shared_ptr<iceberg::FileIO>& fileIO,
    const std::string& rootLocation,
    const std::string& storeID,
    const std::string& collectionID,
    const TStorageLimits& limits)
{
    const auto root = NIO::TStoreUri::Parse(rootLocation, storeID);
    if (!fileIO || !root || root.Value().CollectionId() != collectionID ||
        root.Value().Kind() != NIO::EFileKind::Metadata || limits.RootBytes > INT64_MAX)
    {
        return iceberg::InvalidArgument("Invalid recovery root.");
    }
    ICEBERG_ASSIGN_OR_RAISE(auto input, fileIO->NewInputFile(rootLocation));
    ICEBERG_ASSIGN_OR_RAISE(auto rootBytes, input->Size());
    ICEBERG_ASSIGN_OR_RAISE(auto metadata,
        ReadTableMetadata(*fileIO, rootLocation, std::nullopt, static_cast<std::int64_t>(limits.RootBytes)));
    const auto location = "store://" + storeID + "/warehouse/" + collectionID;
    if (metadata->location != location || metadata->format_version != 2 ||
        metadata->snapshots.size() != metadata->snapshot_log.size() ||
        metadata->metadata_log.size() > limits.MetadataLogEntries)
    {
        return iceberg::ValidationFailed("Unsupported recovery metadata.");
    }
    TBaseState state{rootLocation, std::shared_ptr<iceberg::TableMetadata>(std::move(metadata)), {}, {}};
    state.Usage.RootBytes = static_cast<std::uint64_t>(rootBytes);
    state.Usage.Snapshots = state.Metadata->snapshots.size();
    state.Usage.SnapshotLogEntries = state.Metadata->snapshot_log.size();
    if (CheckStorageCapacity(state.Usage, limits)) {
        return iceberg::ValidationFailed("Recovery metadata exceeds storage capacity.");
    }
    return state;
}

// The root without history: the profile every retained transition starts from.
iceberg::Result<std::shared_ptr<iceberg::TableMetadata>> EmptyHistoryView(const iceberg::TableMetadata& original)
{
    auto view = std::make_shared<iceberg::TableMetadata>(original);
    view->snapshots.clear();
    view->snapshot_log.clear();
    view->refs.clear();
    view->current_snapshot_id = iceberg::kInvalidSnapshotId;
    view->last_sequence_number = 0;
    ICEBERG_RETURN_UNEXPECTED(CheckRecoveryProfile(*view));
    return view;
}

std::shared_ptr<iceberg::TableMetadata> AppendToView(const iceberg::TableMetadata& view,
    const std::shared_ptr<iceberg::Snapshot>& snapshot,
    const iceberg::SnapshotLogEntry& log)
{
    auto next = std::make_shared<iceberg::TableMetadata>(view);
    next->snapshots.push_back(snapshot);
    next->snapshot_log.push_back(log);
    next->current_snapshot_id = snapshot->snapshot_id;
    next->last_sequence_number = snapshot->sequence_number;
    next->refs["main"] = std::make_shared<iceberg::SnapshotRef>(
        iceberg::SnapshotRef{.snapshot_id = snapshot->snapshot_id, .retention = iceberg::SnapshotRef::Branch{}});
    return next;
}

iceberg::Result<TBaseState> CheckAppendClosureImpl(const std::shared_ptr<iceberg::FileIO>& fileIO,
    TBaseState candidate,
    const std::vector<iceberg::ManifestFile>& previousManifests,
    const std::shared_ptr<NIO::TIOAttempt>& attempt,
    const TStorageLimits& limits,
    TRecoveryVisited* visited,
    bool commitRoot,
    TScanMembership* scanIndex)
{
    return CatchIcebergExceptions([&]() -> iceberg::Result<TBaseState> {
        if (!fileIO || !candidate.Metadata) {
            return iceberg::InvalidArgument("Missing closure verification context.");
        }
        RegisterStorageFormats();
        const auto location = std::string_view(candidate.Metadata->location);
        if (location.size() < 44) {
            return iceberg::ValidationFailed("Invalid table location.");
        }
        const auto table = NIO::TStoreUri::ParseTable(location, attempt ? attempt->StoreId() : location.substr(8, 36));
        if (!table) {
            return iceberg::ValidationFailed("Invalid table location.");
        }
        ICEBERG_ASSIGN_OR_RAISE(auto snapshot, candidate.Metadata->Snapshot());
        ICEBERG_RETURN_UNEXPECTED(CheckPath(snapshot->manifest_list, table.Value(), NIO::EFileKind::ManifestList));
        ICEBERG_ASSIGN_OR_RAISE(auto listBytes,
            CheckAvro(*fileIO,
                snapshot->manifest_list,
                -1,
                NIO::EFileKind::ManifestList,
                limits.ManifestListBytes,
                limits.Store.Manifests,
                attempt));
        ICEBERG_ASSIGN_OR_RAISE(auto reader, iceberg::ManifestListReader::Make(snapshot->manifest_list, fileIO));
        ICEBERG_ASSIGN_OR_RAISE(auto manifests, reader->Files());
        if (manifests.size() <= previousManifests.size() || manifests.size() > limits.Store.Manifests) {
            return iceberg::ValidationFailed("Invalid append manifest list size.");
        }
        const auto added = manifests.size() - previousManifests.size();
        if (added > limits.NewManifests ||
            !std::ranges::equal(previousManifests, std::span(manifests).subspan(added), EqualManifest))
        {
            return iceberg::ValidationFailed("Append changes retained manifests.");
        }
        std::set<std::string_view> paths;
        for (const auto& manifest : manifests) {
            if (!paths.insert(manifest.manifest_path).second) {
                return iceberg::ValidationFailed("Duplicate manifest path.");
            }
        }
        TStorageUsage delta;
        delta.ManifestListBytes = listBytes;
        for (const auto& manifest : std::span(manifests).first(added)) {
            ICEBERG_ASSIGN_OR_RAISE(auto fileUsage,
                CheckManifest(fileIO,
                    manifest,
                    *snapshot,
                    *candidate.Metadata,
                    table.Value(),
                    attempt,
                    limits,
                    visited,
                    scanIndex));
            auto total = ReplaceUsage(delta, {.Next = fileUsage});
            if (!total) {
                return iceberg::ValidationFailed("Manifest usage overflows.");
            }
            delta = total.Value();
        }
        // Only the commit that wrote the candidate root is charged for it.
        // Recovery replays history against the final root, whose size older
        // commits never wrote; their manifest and list must still fit.
        const auto rootBytes = commitRoot ? candidate.Usage.RootBytes : 0;
        if (delta.DataFiles != 1 || delta.ManifestBytes > limits.CommitBytes ||
            listBytes > limits.CommitBytes - delta.ManifestBytes ||
            rootBytes > limits.CommitBytes - delta.ManifestBytes - listBytes)
        {
            return iceberg::ValidationFailed("CAPACITY: Append exceeds file or accumulator limits.");
        }
        const auto usage = ReplaceUsage(candidate.Usage, {.Next = delta});
        if (!usage || CheckStorageCapacity(usage.Value(), limits)) {
            return iceberg::ValidationFailed("CAPACITY: Append exceeds storage capacity.");
        }
        candidate.Usage = usage.Value();
        candidate.CurrentManifests = std::move(manifests);
        ICEBERG_RETURN_UNEXPECTED(MarkVerified(attempt, snapshot->manifest_list));
        if (scanIndex) {
            const auto boundary = std::ranges::lower_bound(scanIndex->Snapshots,
                snapshot->snapshot_id,
                {},
                &TSnapshotBoundary::SnapshotID);
            if (boundary != scanIndex->Snapshots.end() && boundary->SnapshotID == snapshot->snapshot_id) {
                return iceberg::ValidationFailed("Duplicate scan index snapshot.");
            }
            scanIndex->Snapshots.insert(boundary, {snapshot->snapshot_id, scanIndex->Files.size()});
        }
        return std::move(candidate);
    });
}

}

iceberg::Result<TBaseState> CheckAppendClosure(const std::shared_ptr<iceberg::FileIO>& fileIO,
    TBaseState candidate,
    const std::vector<iceberg::ManifestFile>& previousManifests,
    const std::shared_ptr<NIO::TIOAttempt>& attempt,
    const TStorageLimits& limits)
{
    return CatchIcebergExceptions([&]() -> iceberg::Result<TBaseState> {
        if (!candidate.ScanIndex) {
            return iceberg::ValidationFailed("Append has no verified scan index.");
        }
        auto scanIndex = std::make_shared<TScanMembership>(*candidate.ScanIndex);
        ICEBERG_ASSIGN_OR_RAISE(auto state,
            CheckAppendClosureImpl(fileIO,
                std::move(candidate),
                previousManifests,
                attempt,
                limits,
                nullptr,
                true,
                scanIndex.get()));
        state.ScanIndex = std::move(scanIndex);
        return state;
    });
}

iceberg::Result<TBaseState> VerifyRetainedSnapshot(const std::shared_ptr<iceberg::FileIO>& fileIO,
    const std::string& rootLocation,
    const std::string& storeID,
    const std::string& collectionID,
    std::int64_t snapshotID,
    const TStorageLimits& limits)
{
    return CatchIcebergExceptions([&]() -> iceberg::Result<TBaseState> {
        ICEBERG_ASSIGN_OR_RAISE(auto state, ReadRecoveryRoot(fileIO, rootLocation, storeID, collectionID, limits));
        const auto& snapshots = state.Metadata->snapshots;
        const auto found = std::ranges::find_if(snapshots, [&](const auto& snapshot) {
            return snapshot && snapshot->snapshot_id == snapshotID;
        });
        if (found == snapshots.end()) {
            return iceberg::ValidationFailed("Committed snapshot is not retained.");
        }
        const auto index = static_cast<std::size_t>(found - snapshots.begin());
        ICEBERG_ASSIGN_OR_RAISE(auto view, EmptyHistoryView(*state.Metadata));
        std::vector<iceberg::ManifestFile> previous;
        if (index > 0) {
            const auto& parent = snapshots[index - 1];
            if (!parent) {
                return iceberg::ValidationFailed("Invalid retained snapshot history.");
            }
            const auto table = NIO::TStoreUri::ParseTable(state.Metadata->location, storeID);
            if (!table) {
                return iceberg::ValidationFailed("Invalid table location.");
            }
            ICEBERG_RETURN_UNEXPECTED(CheckPath(parent->manifest_list, table.Value(), NIO::EFileKind::ManifestList));
            RegisterStorageFormats();
            ICEBERG_RETURN_UNEXPECTED(CheckAvro(*fileIO,
                parent->manifest_list,
                -1,
                NIO::EFileKind::ManifestList,
                limits.ManifestListBytes,
                limits.Store.Manifests,
                nullptr));
            ICEBERG_ASSIGN_OR_RAISE(auto reader, iceberg::ManifestListReader::Make(parent->manifest_list, fileIO));
            ICEBERG_ASSIGN_OR_RAISE(previous, reader->Files());
            view = AppendToView(*view, parent, state.Metadata->snapshot_log[index - 1]);
        }
        auto next = AppendToView(*view, *found, state.Metadata->snapshot_log[index]);
        ICEBERG_RETURN_UNEXPECTED(CheckAppendMetadata(*view, *next, {}));
        // As in VerifyReachable: only the commit that wrote this root is charged for it.
        const bool commitRoot = index + 1 == snapshots.size();
        TBaseState candidate{rootLocation, next, {}, {}};
        if (commitRoot) {
            candidate.Usage.RootBytes = state.Usage.RootBytes;
        }
        ICEBERG_RETURN_UNEXPECTED(
            CheckAppendClosureImpl(fileIO, std::move(candidate), previous, {}, limits, nullptr, commitRoot, nullptr));
        return state;
    });
}

iceberg::Result<TBaseState> VerifyReachable(const std::shared_ptr<iceberg::FileIO>& fileIO,
    const std::string& rootLocation,
    const std::string& storeID,
    const std::string& collectionID,
    const TStorageLimits& limits,
    const std::string& workspaceDirectory)
{
    return CatchIcebergExceptions([&]() -> iceberg::Result<TBaseState> {
        ICEBERG_ASSIGN_OR_RAISE(auto state, ReadRecoveryRoot(fileIO, rootLocation, storeID, collectionID, limits));
        const auto original = state.Metadata;
        ICEBERG_ASSIGN_OR_RAISE(auto view, EmptyHistoryView(*original));
        ICEBERG_ASSIGN_OR_RAISE(auto visited, TRecoveryVisited::Make(256ULL << 20, workspaceDirectory));
        auto scanIndex = std::make_shared<TScanMembership>();
        scanIndex->Files.reserve(original->snapshots.size());
        scanIndex->Snapshots.reserve(original->snapshots.size());
        std::unordered_set<std::int64_t> snapshotIDs;
        snapshotIDs.reserve(original->snapshots.size());
        for (std::size_t index = 0; index < original->snapshots.size(); ++index) {
            const auto& snapshot = original->snapshots[index];
            if (!snapshot || !snapshotIDs.insert(snapshot->snapshot_id).second) {
                return iceberg::ValidationFailed("Invalid retained snapshot history.");
            }
            ICEBERG_ASSIGN_OR_RAISE(auto inserted,
                visited->Insert(NIO::EFileKind::ManifestList, snapshot->manifest_list));
            if (!inserted) {
                return iceberg::ValidationFailed("Invalid retained snapshot history.");
            }
            auto next = AppendToView(*view, snapshot, original->snapshot_log[index]);
            ICEBERG_RETURN_UNEXPECTED(CheckAppendMetadata(*view, *next, {}));
            auto candidate = state;
            candidate.Metadata = next;
            ICEBERG_ASSIGN_OR_RAISE(state,
                CheckAppendClosureImpl(fileIO,
                    std::move(candidate),
                    state.CurrentManifests,
                    {},
                    limits,
                    visited.get(),
                    index + 1 == original->snapshots.size(),
                    scanIndex.get()));
            if (next->snapshots.size() > 1) {
                next->snapshots.erase(next->snapshots.begin());
                next->snapshot_log.erase(next->snapshot_log.begin());
            }
            view = std::move(next);
        }
        view->snapshots = original->snapshots;
        view->snapshot_log = original->snapshot_log;
        if (*original != *view) {
            return iceberg::ValidationFailed("Recovery root has an inconsistent current snapshot.");
        }
        state.Metadata = original;
        state.ScanIndex = std::move(scanIndex);
        return state;
    });
}

}
