#pragma once

#include "core/api/canonical.h"
#include "core/catalog/durable_sqlite_catalog.h"

#include <span>

namespace iceberg {
class FileIO;
} // namespace iceberg

namespace glamdb::detail {

struct CollectionView
{
    CollectionInfo info;
    BaseIdentity base;
};

class StagedCreate
{
public:
    virtual ~StagedCreate() = default;
    virtual CollectionInfo Info() const = 0;
    virtual iceberg::Status Publish(iceberg::sql::CatalogStore& catalog) = 0;
    virtual Status VerifyClosure(std::string_view final_metadata_location) = 0;
};

class StagedAppend
{
public:
    virtual ~StagedAppend() = default;
    virtual AppendResult Receipt() const = 0;
    // This call runs inside the coordinator's owned outer transaction. It must
    // use a fresh Iceberg table with retries=0 and metadata deletion disabled,
    // the same catalog instance, and the exact durable prepared binding.
    virtual iceberg::Status Publish(iceberg::sql::CatalogStore& catalog) = 0;
    // Called by the catalog immediately before INSERT/CAS, after final root
    // completion. Validate every new reachable receipt, reused base membership,
    // sticky I/O errors and candidate reopen capacity. A closed flag is not
    // proof.
    virtual Status VerifyClosure(std::string_view final_metadata_location) = 0;
};

class StorageBackend
{
public:
    virtual ~StorageBackend() = default;
    virtual Result<std::unique_ptr<StagedCreate>>
    PrepareCreate(const StoreInfo& store, const CreateCollectionRequest& request, std::string_view attempt_id) = 0;
    // Pre-binding admission against the exact committed base that the
    // coordinator is about to bind. A rejection consumes no operation ID.
    virtual Status
    CheckAppendAdmissible(const StoreInfo&, const CollectionInfo&, const BaseIdentity&, const NormalizedInput&)
    {
        return {};
    }
    virtual Result<std::unique_ptr<StagedAppend>> PrepareAppend(const StoreInfo& store,
        const BoundOperation& binding,
        const CollectionInfo& collection,
        const NormalizedInput& input,
        std::chrono::steady_clock::time_point deadline) = 0;
    virtual void ConfirmPublication(std::string_view, std::string_view)
    { }
    virtual void InvalidateCache()
    { }
    // Independent, bidirectional validation: each committed receipt matches
    // exactly one retained snapshot/tag, every tag has that committed receipt,
    // Bound/Failed have none; validate UUID/schema/ancestry and reachable
    // files. Absence is authoritative only after the old SQLite handle is
    // closed.
    virtual Status VerifyRetained(const StoreInfo& store,
        std::span<const CollectionView> collections,
        std::span<const OperationRecord> operations) = 0;
    // Independent read-only view for scans; it never joins a writer attempt.
    virtual Result<std::shared_ptr<iceberg::FileIO>> ReadOnlyFileIO()
    {
        return std::unexpected(
            ErrorDetail{"DEPENDENCY_UNAVAILABLE", "storage reader is not installed", RetryHint::OperatorAction});
    }
};

} // namespace glamdb::detail
