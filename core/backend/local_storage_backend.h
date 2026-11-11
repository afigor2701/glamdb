#pragma once

#include "core/engine/storage_backend.h"
#include "core/storage/capacity.h"
#include "core/storage/data_file_writer.h"
#include "core/storage/durable_local_file_io.h"

#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>

namespace arrow {
class MemoryPool;
}

namespace NGlamDB::NIO {
struct ILocalFS;
class TStoreDirs;
} // namespace NGlamDB::NIO

namespace NGlamDB::NStorage {
struct TBaseState;
} // namespace NGlamDB::NStorage

namespace iceberg {
class Catalog;
} // namespace iceberg

namespace glamdb::detail {

struct TLocalStorageOptions
{
    NGlamDB::NStorage::TStorageLimits Storage{};
    NGlamDB::NStorage::TDataWriterLimits Writer{};
    NGlamDB::NStorage::TFileIOLimits FileIO{};
    // Allocator for the normalized writer batch; empty selects Arrow's default.
    std::shared_ptr<arrow::MemoryPool> InputPool{};
    // Filesystem adapter; empty selects the POSIX implementation. Tests inject
    // faults here.
    std::shared_ptr<NGlamDB::NIO::ILocalFS> Fs{};
};

// Production bridge from the coordinator seam to the durable storage
// primitives. The coordinator serializes writers: a staged object holds the
// only active FileIO attempt from Prepare* through Publish and VerifyClosure,
// all on the writer thread. Reads use independent read-only FileIO views.
class TLocalStorageBackend final
    : public StorageBackend
    , public std::enable_shared_from_this<TLocalStorageBackend>
{
public:
    // The store lease has already created or validated root/ and warehouse/.
    static Result<std::shared_ptr<TLocalStorageBackend>>
    Open(const std::filesystem::path& root, const StoreInfo& store, TLocalStorageOptions options = {});

    Health GetHealth() const override;
    EngineCapabilities GetCapabilities() const override
    {
        return {.backend = BackendKind::LocalStorage, .append = true, .scan = true, .metadata = true};
    }

    Result<std::unique_ptr<StagedCreate>>
    PrepareCreate(const StoreInfo& store, const CreateCollectionRequest& request, std::string_view attempt_id) override;
    Status CheckAppendAdmissible(const StoreInfo& store,
        const CollectionInfo& collection,
        const BaseIdentity& base,
        NormalizedInput& input) override;
    Result<std::unique_ptr<StagedAppend>> PrepareAppend(const StoreInfo& store,
        const BoundOperation& binding,
        const CollectionInfo& collection,
        const NormalizedInput& input,
        std::chrono::steady_clock::time_point deadline) override;
    void PublicationFinished(std::string_view collectionID,
        std::string_view metadataLocation,
        CommitDisposition result) override;
    void InvalidateCache() override;
    void RetireRoot(const BaseIdentity& superseded) override;
    Status VerifyRetained(const StoreInfo& store,
        std::span<const CollectionView> collections,
        std::span<const OperationRecord> operations) override;
    Result<std::shared_ptr<iceberg::FileIO>> ReadOnlyFileIO() override;

    // A verified candidate is tentative until PublicationFinished confirms the
    // outer COMMIT. Recovery rebuilds the generation independently.
    void RecordCandidate(std::string_view collectionID, std::shared_ptr<const NGlamDB::NStorage::TBaseState> candidate);
    void DiscardCandidate(std::string_view collectionID);

private:
    struct TCollectionState
    {
        std::shared_ptr<const NGlamDB::NStorage::TBaseState> Confirmed;
        std::shared_ptr<const NGlamDB::NStorage::TBaseState> Tentative;
    };

    const StoreInfo Store_;
    const TLocalStorageOptions Options_;
    const std::shared_ptr<NGlamDB::NIO::TStoreDirs> Dirs_;
    const std::shared_ptr<NGlamDB::NStorage::TDurableLocalFileIO> FileIO_;
    const std::shared_ptr<iceberg::Catalog> Catalog_;

    std::mutex Mutex_;
    std::map<std::string, TCollectionState, std::less<>> Collections_;
    bool VerifiedGeneration_ = false;
    // Warehouse bytes measured by the admission preflight of the append that
    // is about to be prepared. Binding writes only the catalog, so
    // PrepareAppend reuses them instead of walking the warehouse again.
    std::optional<std::uint64_t> AdmittedWarehouseBytes_;

    TLocalStorageBackend(StoreInfo store,
        TLocalStorageOptions options,
        std::shared_ptr<NGlamDB::NIO::TStoreDirs> dirs,
        std::shared_ptr<NGlamDB::NStorage::TDurableLocalFileIO> fileIO);

    void CheckStore(const StoreInfo& store) const;
    // Root identity, warehouse limit and free-space reserve before a write.
    // `remember` keeps the measured warehouse bytes for the same append's
    // PrepareAppend; `reuse` consumes them instead of walking again.
    std::error_code CheckWriteCapacity(bool remember, bool reuse);
    // The caller holds Mutex_. Usage and cached bases are authoritative only
    // after VerifyRetained; InvalidateCache revokes them.
    void RequireVerified() const;
    NGlamDB::NStorage::TStorageUsage ComputeStoreUsage();
    std::shared_ptr<const NGlamDB::NStorage::TBaseState> ResolveBase(const BaseIdentity& base);
    std::shared_ptr<const NGlamDB::NStorage::TBaseState> VerifyCollection(const BaseIdentity& base) const;
};

} // namespace glamdb::detail
