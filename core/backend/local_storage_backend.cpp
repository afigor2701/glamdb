#include "core/backend/local_storage_backend.h"

#include "core/api/binary.h"
#include "core/api/canonical.h"
#include "core/backend/inert_catalog.h"
#include "core/engine/storage_mapping.h"
#include "core/io/io_attempt.h"
#include "core/io/local_fs.h"
#include "core/io/store_dirs.h"
#include "core/io/write_latch.h"
#include "core/storage/append_stager.h"
#include "core/storage/base_state.h"
#include "core/storage/publication_guard.h"
#include "core/storage/table_profile.h"

#include <arrow/record_batch.h>
#include <arrow/util/byte_size.h>
#include <iceberg/constants.h>
#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/partition_spec.h>
#include <iceberg/schema.h>
#include <iceberg/snapshot.h>
#include <iceberg/sort_order.h>
#include <iceberg/table.h>
#include <iceberg/table_identifier.h>
#include <iceberg/table_metadata.h>
#include <iceberg/transaction.h>
#include <iceberg/util/timepoint.h>

#include <algorithm>
#include <charconv>
#include <format>
#include <limits>
#include <set>
#include <unordered_map>

namespace glamdb::detail {
namespace {

namespace NIO = NGlamDB::NIO;
namespace NStorage = NGlamDB::NStorage;

constexpr std::string_view CatalogNamespace = "columnar";
constexpr std::string_view OperationKey = "glamdb.operation-id";
constexpr std::string_view ContentHashKey = "glamdb.content-hash";

[[noreturn]] void Fail(std::string_view code, std::string_view context, std::string_view message)
{
    const auto hint = code == "INVALID_ARGUMENT" ? RetryHint::FixInput : RetryHint::OperatorAction;
    throw Failure({std::string(code), BoundedDiagnostic(std::string(context) + ": " + std::string(message)), hint});
}

template <class T>
T TakeIceberg(iceberg::Result<T> result, std::string_view code, std::string_view context)
{
    if (!result) {
        const auto effective =
            result.error().kind == iceberg::ErrorKind::kInvalidArgument ? std::string_view("INVALID_ARGUMENT") : code;
        Fail(effective, context, result.error().message);
    }
    return std::move(*result);
}

void CheckIceberg(const iceberg::Status& status, std::string_view code, std::string_view context)
{
    if (!status) {
        Fail(code, context, status.error().message);
    }
}

template <class T>
T TakeIO(NIO::TResult<T> result, std::string_view code, std::string_view context)
{
    if (!result) {
        Fail(code, context, result.Error().message());
    }
    return std::move(result).Value();
}

void CheckIO(std::error_code error, std::string_view code, std::string_view context)
{
    if (error) {
        Fail(code, context, error.message());
    }
}

// Storage failures are reported outcomes; no exception crosses the backend seam.
template <class TResult, class TFunction>
TResult Guarded(TFunction&& body)
{
    try {
        return body();
    } catch (const Failure& error) {
        return std::unexpected(error.detail);
    } catch (const std::exception& error) {
        return std::unexpected(ErrorDetail{"INTERNAL", BoundedDiagnostic(error.what()), RetryHint::OperatorAction});
    }
}

void CheckDeadline(std::chrono::steady_clock::time_point deadline)
{
    if (std::chrono::steady_clock::now() >= deadline) {
        Fail("DEADLINE_EXCEEDED", "append", "deadline expired during storage production");
    }
}

std::string TableLocation(std::string_view storeID, std::string_view collectionID)
{
    return std::format("store://{}/warehouse/{}", storeID, collectionID);
}

std::string MetadataLocation(std::string_view table, uint64_t version, std::string_view attemptID)
{
    return std::format("{}/metadata/{:05}-{}.metadata.json", table, version, attemptID);
}

uint64_t MetadataVersion(std::string_view location)
{
    const auto slash = location.rfind('/');
    const auto leaf = slash == std::string_view::npos ? location : location.substr(slash + 1);
    const auto dash = leaf.find('-');
    uint64_t version = 0;
    if (dash == std::string_view::npos || dash == 0) {
        Fail("CORRUPTION", "base metadata", "root location has no version prefix");
    }
    const auto parsed = std::from_chars(leaf.data(), leaf.data() + dash, version);
    if (parsed.ec != std::errc{} || parsed.ptr != leaf.data() + dash || version == std::numeric_limits<uint64_t>::max())
    {
        Fail("CORRUPTION", "base metadata", "root location has an invalid version prefix");
    }
    return version;
}

std::optional<int64_t> CurrentSnapshot(const iceberg::TableMetadata& metadata)
{
    if (metadata.current_snapshot_id == iceberg::kInvalidSnapshotId) {
        return std::nullopt;
    }
    return metadata.current_snapshot_id;
}

int64_t SnapshotTimeUs(const iceberg::Snapshot& snapshot, std::string_view code)
{
    const auto ms = iceberg::UnixMsFromTimePointMs(snapshot.timestamp_ms);
    if (ms < -62135596800000LL || ms > 253402300799999LL) {
        Fail(code, "snapshot timestamp", "snapshot timestamp is outside supported UTC range");
    }
    return ms * 1000;
}

NStorage::TStorageUsage AddUsage(NStorage::TStorageUsage total, const NStorage::TStorageUsage& item)
{
    total.RootBytes += item.RootBytes;
    total.Snapshots += item.Snapshots;
    total.SnapshotLogEntries += item.SnapshotLogEntries;
    total.DataFiles += item.DataFiles;
    total.Manifests += item.Manifests;
    total.ManifestBytes += item.ManifestBytes;
    total.ManifestListBytes += item.ManifestListBytes;
    total.FooterBytes += item.FooterBytes;
    return total;
}

NStorage::TTableProfile MakeProfile(const SchemaSpec& schema, std::string_view code)
{
    auto profile = NStorage::MakeTableProfile(ToStorageSchema(schema));
    if (!profile) {
        Fail(code, "table profile", profile.error().message);
    }
    return std::move(*profile);
}

void CheckWriterInput(const SchemaSpec& schema, const NormalizedInput& input, const NStorage::TDataWriterLimits& limits)
{
    if (limits.MaxInputBytes <= 0 || limits.MaxRows <= 0 || limits.MaxRowGroupBytes <= 0 || limits.MaxFileBytes <= 0) {
        Fail("INVALID_ARGUMENT", "append", "data writer limits must be positive");
    }
    if (input.rows.empty() || static_cast<int64_t>(input.rows.size()) > limits.MaxRows) {
        Fail("INVALID_ARGUMENT", "append", std::format("an append must contain 1..{} rows", limits.MaxRows));
    }
    int64_t maxRowBytes = 1;
    for (const auto& row : input.rows) {
        if (row.size() != schema.fields.size()) {
            Fail("INVALID_ARGUMENT", "append", "row field count does not match the collection schema");
        }
        int64_t rowBytes = 0;
        for (size_t column = 0; column < schema.fields.size(); ++column) {
            const auto type = schema.fields[column].type;
            rowBytes += 1 + (type == FieldType::String ? 4 : type == FieldType::Bool ? 1 : 8);
            if (const auto* text = std::get_if<std::string>(&row[column])) {
                rowBytes += static_cast<int64_t>(text->size());
            }
        }
        if (rowBytes > (2LL << 20)) {
            throw Failure({"RESOURCE_EXHAUSTED", "append: Row exceeds its byte limit.", RetryHint::FixInput});
        }
        maxRowBytes = std::max(maxRowBytes, rowBytes);
    }
    const auto offsetBytes = 4 * static_cast<int64_t>(std::ranges::count_if(schema.fields, [](const auto& field) {
        return field.type == FieldType::String;
    }));
    if (limits.MaxRowGroupBytes < offsetBytes || maxRowBytes > limits.MaxRowGroupBytes - offsetBytes) {
        throw Failure({"RESOURCE_EXHAUSTED", "append: row exceeds the row group byte limit", RetryHint::FixInput});
    }
    const auto batch = MakeRecordBatch(schema, input.rows);
    if (!batch) {
        throw Failure(batch.error());
    }
    const auto totalBytes = arrow::util::TotalBufferSize(**batch);
    if (totalBytes < 0 || totalBytes > limits.MaxInputBytes) {
        throw Failure({"RESOURCE_EXHAUSTED", "append: Input buffers exceed their byte limit.", RetryHint::FixInput});
    }
}

void CheckIdentity(const NStorage::TBaseState& state, const BaseIdentity& base)
{
    Require(state.Metadata != nullptr && state.MetadataLocation == base.metadata_location &&
            state.Metadata->table_uuid == base.table_uuid &&
            CurrentSnapshot(*state.Metadata) == base.current_snapshot_id,
        "CORRUPTION",
        "retained table root does not match the catalog identity");
}

void CheckSchema(const iceberg::TableMetadata& metadata, const SchemaSpec& schema)
{
    const auto profile = MakeProfile(schema, "CORRUPTION");
    const auto current = TakeIceberg(metadata.Schema(), "CORRUPTION", "retained schema");
    Require(*current == *profile.Schema, "CORRUPTION", "retained table schema differs from the catalog schema");
}

bool SummaryMatches(const iceberg::Snapshot& snapshot, const std::string& key, bool required, std::string_view expected)
{
    const auto it = snapshot.summary.find(key);
    if (it == snapshot.summary.end()) {
        return !required;
    }
    return it->second == expected;
}

void CheckReceipt(const iceberg::Snapshot& snapshot, const AppendResult& receipt)
{
    using Fields = iceberg::SnapshotSummaryFields;
    Require(receipt.snapshot_id == snapshot.snapshot_id && receipt.parent_snapshot_id == snapshot.parent_snapshot_id &&
            receipt.snapshot_created_at_us == SnapshotTimeUs(snapshot, "CORRUPTION") &&
            SummaryMatches(snapshot, std::string(ContentHashKey), true, receipt.payload_hash) &&
            SummaryMatches(snapshot, Fields::kAddedRecords, false, std::to_string(receipt.rows_appended)) &&
            SummaryMatches(snapshot, Fields::kAddedDataFiles, false, std::to_string(receipt.data_files_added)) &&
            SummaryMatches(snapshot,
                Fields::kAddedFileSize,
                false,
                std::to_string(receipt.compressed_data_bytes_added)),
        "CORRUPTION",
        "retained snapshot differs from its committed receipt");
}

// Bidirectional: every retained snapshot has exactly one committed receipt of
// this collection, and every committed receipt has exactly one snapshot.
// Bound and Failed operations therefore have no retained snapshot.
void CheckHistory(const iceberg::TableMetadata& metadata,
    std::string_view collectionID,
    std::span<const OperationRecord> operations)
{
    std::map<std::string, const AppendResult*, std::less<>> committed;
    for (const auto& operation : operations) {
        if (operation.binding.collection_id != collectionID || operation.state != OperationState::Committed) {
            continue;
        }
        if (!operation.outcome.has_value() || !operation.outcome->result.has_value()) {
            Fail("CORRUPTION", "history", "committed operation has no receipt");
        }
        committed.emplace(operation.binding.operation_id, &*operation.outcome->result);
    }
    std::set<std::string, std::less<>> seen;
    for (const auto& snapshot : metadata.snapshots) {
        Require(snapshot != nullptr, "CORRUPTION", "retained snapshot is missing");
        const auto operation = snapshot->summary.find(std::string(OperationKey));
        Require(operation != snapshot->summary.end(), "CORRUPTION", "retained snapshot has no operation identity");
        const auto receipt = committed.find(operation->second);
        Require(receipt != committed.end() && seen.insert(operation->second).second,
            "CORRUPTION",
            "retained snapshot has no unique committed receipt");
        CheckReceipt(*snapshot, *receipt->second);
    }
    Require(seen.size() == committed.size(), "CORRUPTION", "committed receipt has no retained snapshot");
}

class TStagedCreate final : public StagedCreate
{
public:
    TStagedCreate(std::shared_ptr<TLocalStorageBackend> owner,
        CollectionInfo info,
        std::string root,
        NStorage::TPublicationContext context,
        NStorage::TAttemptScope scope)
        : Owner_(std::move(owner))
        , Info_(std::move(info))
        , Root_(std::move(root))
        , Context_(std::move(context))
        , Scope_(std::move(scope))
    { }

    ~TStagedCreate() override
    {
        Owner_->DiscardCandidate(Info_.collection_id);
    }

    CollectionInfo Info() const override
    {
        return Info_;
    }

    iceberg::Status Publish(iceberg::sql::CatalogStore& catalog) override
    {
        return catalog.InsertTable(CatalogNamespace, Info_.name, Root_);
    }

    Status VerifyClosure(std::string_view location) override
    {
        return Guarded<Status>([&]() -> Status {
            Require(location == Root_, "IO_ERROR", "catalog published an unexpected collection root");
            NStorage::TStoragePublicationGuard guard(Context_);
            CheckIceberg(guard.BeforePointerChange({NStorage::EPointerOp::InsertTable,
                             std::string(CatalogNamespace),
                             Info_.name,
                             std::nullopt,
                             std::string(location)}),
                "IO_ERROR",
                "verify collection root");
            Owner_->RecordCandidate(Info_.collection_id, guard.GetCandidate());
            return {};
        });
    }

private:
    const std::shared_ptr<TLocalStorageBackend> Owner_;
    const CollectionInfo Info_;
    const std::string Root_;
    const NStorage::TPublicationContext Context_;
    // Keeps the attempt bound until publication finishes or fails.
    NStorage::TAttemptScope Scope_;
};

class TStagedAppend final : public StagedAppend
{
public:
    TStagedAppend(std::shared_ptr<TLocalStorageBackend> owner,
        std::string collectionID,
        std::string name,
        std::string baseLocation,
        std::string root,
        AppendResult receipt,
        NStorage::TPublicationContext context,
        NStorage::TAttemptScope scope)
        : Owner_(std::move(owner))
        , CollectionID_(std::move(collectionID))
        , Name_(std::move(name))
        , BaseLocation_(std::move(baseLocation))
        , Root_(std::move(root))
        , Receipt_(std::move(receipt))
        , Context_(std::move(context))
        , Scope_(std::move(scope))
    { }

    ~TStagedAppend() override
    {
        Owner_->DiscardCandidate(CollectionID_);
    }

    AppendResult Receipt() const override
    {
        return Receipt_;
    }

    iceberg::Status Publish(iceberg::sql::CatalogStore& catalog) override
    {
        const auto updated =
            catalog.UpdateTableMetadataLocation(CatalogNamespace, Name_, Root_, BaseLocation_, BaseLocation_);
        if (!updated) {
            return std::unexpected(updated.error());
        }
        if (*updated != 1) {
            return iceberg::CommitFailed("catalog pointer changed before publication");
        }
        return {};
    }

    Status VerifyClosure(std::string_view location) override
    {
        return Guarded<Status>([&]() -> Status {
            Require(location == Root_, "IO_ERROR", "catalog published an unexpected append root");
            NStorage::TStoragePublicationGuard guard(Context_);
            CheckIceberg(guard.BeforePointerChange({NStorage::EPointerOp::UpdateLocation,
                             std::string(CatalogNamespace),
                             Name_,
                             BaseLocation_,
                             std::string(location)}),
                "IO_ERROR",
                "verify append root");
            Owner_->RecordCandidate(CollectionID_, guard.GetCandidate());
            return {};
        });
    }

private:
    const std::shared_ptr<TLocalStorageBackend> Owner_;
    const std::string CollectionID_;
    const std::string Name_;
    const std::string BaseLocation_;
    const std::string Root_;
    const AppendResult Receipt_;
    const NStorage::TPublicationContext Context_;
    NStorage::TAttemptScope Scope_;
};

} // namespace

TLocalStorageBackend::TLocalStorageBackend(StoreInfo store,
    TLocalStorageOptions options,
    std::shared_ptr<NIO::TStoreDirs> dirs,
    std::shared_ptr<NStorage::TDurableLocalFileIO> fileIO)
    : Store_(std::move(store))
    , Options_(options)
    , Dirs_(std::move(dirs))
    , FileIO_(std::move(fileIO))
    , Catalog_(std::make_shared<TInertCatalog>())
{ }

Result<std::shared_ptr<TLocalStorageBackend>>
TLocalStorageBackend::Open(const std::filesystem::path& root, const StoreInfo& store, TLocalStorageOptions options)
{
    return Guarded<Result<std::shared_ptr<TLocalStorageBackend>>>(
        [&]() -> Result<std::shared_ptr<TLocalStorageBackend>> {
            auto dirs = TakeIO(NIO::TStoreDirs::Open(std::make_shared<NIO::TPosixLocalFS>(),
                                   root,
                                   std::make_shared<NIO::TWriteLatch>()),
                "IO_ERROR",
                "open store directories");
            auto fileIO = TakeIceberg(NStorage::TDurableLocalFileIO::Make(dirs, store.store_id, options.FileIO),
                "IO_ERROR",
                "open durable FileIO");
            return std::shared_ptr<TLocalStorageBackend>(
                new TLocalStorageBackend(store, options, std::move(dirs), std::move(fileIO)));
        });
}

Result<std::unique_ptr<StagedCreate>> TLocalStorageBackend::PrepareCreate(const StoreInfo& store,
    const CreateCollectionRequest& request,
    std::string_view attempt_id)
{
    return Guarded<Result<std::unique_ptr<StagedCreate>>>([&]() -> Result<std::unique_ptr<StagedCreate>> {
        CheckStore(store);
        CheckIO(Dirs_->Latch()->Error(), "IO_ERROR", "write latch");
        auto profile = MakeProfile(request.schema, "INVALID_ARGUMENT");
        CheckIO(NStorage::CheckWriteCapacity(*Dirs_, Options_.Storage), "RESOURCE_EXHAUSTED", "storage preflight");
        const auto minUsage =
            TakeIO(NStorage::ReplaceUsage(ComputeStoreUsage(), {.Previous = {}, .Next = {.RootBytes = 1}}),
                "RESOURCE_EXHAUSTED",
                "storage capacity");
        CheckIO(NStorage::CheckStorageCapacity(minUsage, Options_.Storage), "RESOURCE_EXHAUSTED", "storage capacity");
        TakeIO(Dirs_->CreateCollection(request.collection_id), "IO_ERROR", "create collection directories");
        auto attempt = TakeIO(NIO::TIOAttempt::Make({Store_.store_id,
                                                        request.collection_id,
                                                        std::string(attempt_id),
                                                        NIO::EAttemptKind::CreateCollection,
                                                        std::nullopt},
                                  Dirs_->Latch()),
            "IO_ERROR",
            "create attempt");
        auto scope = TakeIceberg(FileIO_->BindAttempt(attempt), "IO_ERROR", "bind create attempt");

        const auto location = TableLocation(Store_.store_id, request.collection_id);
        const auto metadata = TakeIceberg(iceberg::TableMetadata::Make(*profile.Schema,
                                              *iceberg::PartitionSpec::Unpartitioned(),
                                              *iceberg::SortOrder::Unsorted(),
                                              location,
                                              profile.Properties),
            "IO_ERROR",
            "build initial metadata");
        auto root = MetadataLocation(location, 0, attempt_id);
        CheckIceberg(iceberg::TableMetadataUtil::Write(*FileIO_, root, *metadata),
            "IO_ERROR",
            "write initial metadata");

        CollectionInfo info{request.collection_id, request.exact_name, metadata->table_uuid, request.schema, 0, {}};
        NStorage::TPublicationContext context{.Directories = Dirs_,
            .FileIO = FileIO_,
            .Attempt = attempt,
            .Namespace = std::string(CatalogNamespace),
            .Name = request.exact_name,
            .Profile = std::move(profile),
            .Base = {},
            .ExpectedSummary = {},
            .StoreUsage = ComputeStoreUsage(),
            .Limits = Options_.Storage};
        return std::make_unique<TStagedCreate>(shared_from_this(),
            std::move(info),
            std::move(root),
            std::move(context),
            std::move(scope));
    });
}

Status TLocalStorageBackend::CheckAppendAdmissible(const StoreInfo& store,
    const CollectionInfo& collection,
    const BaseIdentity& /*base*/,
    const NormalizedInput& input)
{
    return Guarded<Status>([&]() -> Status {
        CheckStore(store);
        CheckIO(Dirs_->Latch()->Error(), "IO_ERROR", "write latch");
        (void)MakeProfile(collection.schema, "INVALID_ARGUMENT");
        CheckWriterInput(collection.schema, input, Options_.Writer);
        CheckIO(NStorage::CheckWriteCapacity(*Dirs_, Options_.Storage), "RESOURCE_EXHAUSTED", "storage preflight");
        const auto minUsage = TakeIO(NStorage::ReplaceUsage(ComputeStoreUsage(),
                                         {.Previous = {},
                                             .Next = {.Snapshots = 1,
                                                 .SnapshotLogEntries = 1,
                                                 .DataFiles = 1,
                                                 .Manifests = 1,
                                                 .ManifestBytes = 1,
                                                 .ManifestListBytes = 1,
                                                 .FooterBytes = 1}}),
            "RESOURCE_EXHAUSTED",
            "storage capacity");
        CheckIO(NStorage::CheckStorageCapacity(minUsage, Options_.Storage), "RESOURCE_EXHAUSTED", "storage capacity");
        return {};
    });
}

Result<std::unique_ptr<StagedAppend>> TLocalStorageBackend::PrepareAppend(const StoreInfo& store,
    const BoundOperation& binding,
    const CollectionInfo& collection,
    const NormalizedInput& input,
    std::chrono::steady_clock::time_point deadline)
{
    return Guarded<Result<std::unique_ptr<StagedAppend>>>([&]() -> Result<std::unique_ptr<StagedAppend>> {
        CheckStore(store);
        CheckIO(Dirs_->Latch()->Error(), "IO_ERROR", "write latch");
        Require(binding.collection_id == collection.collection_id &&
                binding.base.collection_id == collection.collection_id &&
                binding.base.table_uuid == collection.table_uuid && binding.base.store_id == Store_.store_id,
            "INVALID_ARGUMENT",
            "binding does not match the collection");
        CheckWriterInput(collection.schema, input, Options_.Writer);
        const auto base = ResolveBase(binding.base);
        CheckDeadline(deadline);
        CheckIO(NStorage::CheckWriteCapacity(*Dirs_, Options_.Storage), "RESOURCE_EXHAUSTED", "storage preflight");

        auto attempt = TakeIO(NIO::TIOAttempt::Make({Store_.store_id,
                                                        binding.collection_id,
                                                        binding.attempt_id,
                                                        NIO::EAttemptKind::Append,
                                                        binding.operation_id},
                                  Dirs_->Latch()),
            "IO_ERROR",
            "create attempt");
        auto scope = TakeIceberg(FileIO_->BindAttempt(attempt), "IO_ERROR", "bind append attempt");
        auto batch = MakeRecordBatch(collection.schema, input.rows);
        if (!batch) {
            throw Failure(batch.error());
        }

        const auto location = TableLocation(Store_.store_id, binding.collection_id);
        const auto dataPath =
            std::format("{}/data/{}-{}-00000.parquet", location, binding.operation_id, binding.attempt_id);
        const auto data =
            TakeIceberg(NStorage::WriteDataFile(FileIO_, attempt, *base->Metadata, dataPath, {*batch}, Options_.Writer),
                "IO_ERROR",
                "write data file");
        Require(data.DataFile != nullptr && data.DataFile->record_count == static_cast<int64_t>(input.rows.size()),
            "IO_ERROR",
            "data file does not contain every input row");
        CheckDeadline(deadline);

        auto table = TakeIceberg(
            iceberg::Table::Make(
                iceberg::TableIdentifier{.ns = iceberg::Namespace{.levels = {std::string(CatalogNamespace)}},
                    .name = collection.name},
                std::make_shared<iceberg::TableMetadata>(*base->Metadata),
                base->MetadataLocation,
                FileIO_,
                Catalog_),
            "IO_ERROR",
            "load base table");
        const std::unordered_map<std::string, std::string> summary{{std::string(OperationKey), binding.operation_id},
            {std::string(ContentHashKey), binding.payload_hash}};
        auto staged = TakeIceberg(NStorage::StageAppend(std::move(table), data.DataFile, summary, attempt),
            "IO_ERROR",
            "stage append");
        auto root = MetadataLocation(location, MetadataVersion(base->MetadataLocation) + 1, binding.attempt_id);
        CheckIceberg(iceberg::TableMetadataUtil::Write(*FileIO_, root, staged.Transaction->current()),
            "IO_ERROR",
            "write append metadata");
        const auto snapshot = TakeIceberg(staged.Transaction->current().SnapshotById(staged.SnapshotID),
            "IO_ERROR",
            "read staged snapshot");
        Require(staged.ParentID == binding.base.current_snapshot_id,
            "IO_ERROR",
            "staged snapshot has an unexpected parent");

        AppendResult receipt{.collection_id = binding.collection_id,
            .operation_id = binding.operation_id,
            .payload_hash = binding.payload_hash,
            .hash_version = 1,
            .snapshot_id = staged.SnapshotID,
            .parent_snapshot_id = staged.ParentID,
            .rows_appended = static_cast<int64_t>(input.rows.size()),
            .data_files_added = 1,
            .compressed_data_bytes_added = data.DataFile->file_size_in_bytes,
            .snapshot_created_at_us = SnapshotTimeUs(*snapshot, "IO_ERROR"),
            .durability_profile = Store_.profile_id,
            .receipt_version = 1};
        NStorage::TPublicationContext context{.Directories = Dirs_,
            .FileIO = FileIO_,
            .Attempt = attempt,
            .Namespace = std::string(CatalogNamespace),
            .Name = collection.name,
            .Profile = MakeProfile(collection.schema, "CORRUPTION"),
            .Base = base,
            .ExpectedSummary = summary,
            .StoreUsage = ComputeStoreUsage(),
            .Limits = Options_.Storage};
        return std::make_unique<TStagedAppend>(shared_from_this(),
            binding.collection_id,
            collection.name,
            binding.base.metadata_location,
            std::move(root),
            std::move(receipt),
            std::move(context),
            std::move(scope));
    });
}

void TLocalStorageBackend::ConfirmPublication(std::string_view collectionID, std::string_view metadataLocation)
{
    std::lock_guard lock(Mutex_);
    auto it = Collections_.find(collectionID);
    if (it == Collections_.end()) {
        return;
    }
    auto& state = it->second;
    if (state.Tentative && state.Tentative->MetadataLocation == metadataLocation) {
        state.Confirmed = std::move(state.Tentative);
    }
    state.Tentative.reset();
}

void TLocalStorageBackend::DiscardCandidate(std::string_view collectionID)
{
    std::lock_guard lock(Mutex_);
    auto it = Collections_.find(collectionID);
    if (it != Collections_.end()) {
        it->second.Tentative.reset();
    }
}

void TLocalStorageBackend::InvalidateCache()
{
    std::lock_guard lock(Mutex_);
    VerifiedGeneration_ = false;
}

Status TLocalStorageBackend::VerifyRetained(const StoreInfo& store,
    std::span<const CollectionView> collections,
    std::span<const OperationRecord> operations)
{
    return Guarded<Status>([&]() -> Status {
        CheckStore(store);
        CheckIO(Dirs_->Latch()->Error(), "IO_ERROR", "write latch");
        std::map<std::string, TCollectionState, std::less<>> verified;
        NStorage::TStorageUsage totalUsage;
        for (const auto& view : collections) {
            Require(view.base.collection_id == view.info.collection_id &&
                    view.base.table_uuid == view.info.table_uuid &&
                    view.base.current_snapshot_id == view.info.current_snapshot_id,
                "CORRUPTION",
                "collection view identity is inconsistent");
            auto state = ResolveVerified(view.base);
            CheckSchema(*state->Metadata, view.info.schema);
            CheckHistory(*state->Metadata, view.info.collection_id, operations);
            totalUsage = TakeIO(NStorage::ReplaceUsage(totalUsage, {.Previous = {}, .Next = state->Usage}),
                "CORRUPTION",
                "aggregate storage usage");
            verified[view.info.collection_id].Confirmed = std::move(state);
        }
        CheckIO(NStorage::CheckStorageCapacity(totalUsage, Options_.Storage),
            "CORRUPTION",
            "retained storage capacity");
        for (const auto& operation : operations) {
            Require(operation.state != OperationState::Committed || verified.contains(operation.binding.collection_id),
                "CORRUPTION",
                "committed operation belongs to an unknown collection");
        }
        std::lock_guard lock(Mutex_);
        Collections_ = std::move(verified);
        VerifiedGeneration_ = true;
        return {};
    });
}

Result<std::shared_ptr<iceberg::FileIO>> TLocalStorageBackend::ReadOnlyFileIO()
{
    auto view = FileIO_->ReadOnlyView();
    if (!view) {
        return std::unexpected(ErrorDetail{"IO_ERROR",
            BoundedDiagnostic("open read-only FileIO: " + view.error().message),
            RetryHint::OperatorAction});
    }
    return std::shared_ptr<iceberg::FileIO>(std::move(*view));
}

void TLocalStorageBackend::RecordCandidate(std::string_view collectionID,
    std::shared_ptr<const NStorage::TBaseState> candidate)
{
    std::lock_guard lock(Mutex_);
    auto it = Collections_.find(collectionID);
    if (it == Collections_.end()) {
        it = Collections_.emplace(std::string(collectionID), TCollectionState{}).first;
    }
    it->second.Tentative = std::move(candidate);
}

void TLocalStorageBackend::CheckStore(const StoreInfo& store) const
{
    Require(store.store_id == Store_.store_id && store.profile_id == Store_.profile_id,
        "INVALID_ARGUMENT",
        "storage backend belongs to a different store");
}

NStorage::TStorageUsage TLocalStorageBackend::ComputeStoreUsage()
{
    std::lock_guard lock(Mutex_);
    NStorage::TStorageUsage total;
    for (const auto& [id, state] : Collections_) {
        if (state.Confirmed) {
            total = AddUsage(total, state.Confirmed->Usage);
        }
    }
    return total;
}

std::shared_ptr<const NStorage::TBaseState> TLocalStorageBackend::ResolveBase(const BaseIdentity& base)
{
    {
        std::lock_guard lock(Mutex_);
        if (VerifiedGeneration_) {
            auto it = Collections_.find(base.collection_id);
            if (it != Collections_.end()) {
                auto& state = it->second;
                // The coordinator builds a binding from the committed SQLite
                // pointer, so a matching tentative root is now confirmed.
                if (state.Tentative && state.Tentative->MetadataLocation == base.metadata_location) {
                    state.Confirmed = std::move(state.Tentative);
                }
                state.Tentative.reset();
                if (state.Confirmed && state.Confirmed->MetadataLocation == base.metadata_location) {
                    CheckIdentity(*state.Confirmed, base);
                    return state.Confirmed;
                }
            }
        }
    }
    auto verified = VerifyCollection(base);
    std::lock_guard lock(Mutex_);
    Collections_[base.collection_id].Confirmed = verified;
    return verified;
}

std::shared_ptr<const NStorage::TBaseState> TLocalStorageBackend::ResolveVerified(const BaseIdentity& base)
{
    std::shared_ptr<const NStorage::TBaseState> cached;
    {
        std::lock_guard lock(Mutex_);
        if (VerifiedGeneration_) {
            const auto it = Collections_.find(base.collection_id);
            if (it != Collections_.end() && it->second.Confirmed &&
                it->second.Confirmed->MetadataLocation == base.metadata_location)
            {
                cached = it->second.Confirmed;
            }
        }
    }
    if (cached) {
        CheckIdentity(*cached, base);
        const std::shared_ptr<iceberg::FileIO> view =
            TakeIceberg(FileIO_->ReadOnlyView(), "IO_ERROR", "open read-only FileIO");
        (void)TakeIceberg(view->ReadFile(base.metadata_location, static_cast<size_t>(cached->Usage.RootBytes)),
            "CORRUPTION",
            "verify retained root");
        return cached;
    }
    return VerifyCollection(base);
}

std::shared_ptr<const NStorage::TBaseState> TLocalStorageBackend::VerifyCollection(const BaseIdentity& base) const
{
    const std::shared_ptr<iceberg::FileIO> view =
        TakeIceberg(FileIO_->ReadOnlyView(), "IO_ERROR", "open read-only FileIO");
    auto state = TakeIceberg(
        NStorage::VerifyReachable(view, base.metadata_location, Store_.store_id, base.collection_id, Options_.Storage),
        "CORRUPTION",
        "verify retained collection");
    CheckIdentity(state, base);
    return std::make_shared<const NStorage::TBaseState>(std::move(state));
}

} // namespace glamdb::detail
