#pragma once

#include "core/engine/storage_backend.h"
#include "core/resources/envelopes.h"
#include "core/storage/capacity.h"
#include "core/storage/data_file_writer.h"
#include "core/storage/durable_local_file_io.h"

#include <cstdlib>
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

namespace glamdb {
class ResourceReservation;
}

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
    std::shared_ptr<ResourceOwner> Resources{};
    // Publication rechecks the physical warehouse cap. By default it adds the
    // append's own durable files to the admission measurement, so growth by
    // anything else after admission is first seen by the next admission.
    // True walks the whole warehouse again before every append publication.
    bool WalkWarehouseAtPublication = false;
    // The guard validates an append's in-memory staged root after comparing
    // the durable file with its serialized bytes. True also parses the file
    // and requires the parse to equal the staged root and to reserialize to
    // the same bytes. Defaults to GLAMDB_VERIFY_ROUNDTRIP when set ("0" is
    // off), otherwise on in builds without NDEBUG.
    bool VerifyStagedRoot = DefaultVerifyStagedRoot();
    uint64_t ReadCacheBytes = ReadCacheMemoryBytes;
    bool EnablePruning = true;

    static bool DefaultVerifyStagedRoot()
    {
        if (const char* value = std::getenv("GLAMDB_VERIFY_ROUNDTRIP"); value && *value) {
            return std::string_view(value) != "0";
        }
#ifdef NDEBUG
        return false;
#else
        return true;
#endif
    }
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
    EngineCapabilities GetCapabilities() const override;

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
    Result<std::shared_ptr<const NGlamDB::NStorage::TBaseState>> VerifiedScanState(const BaseIdentity& base) override;
    std::shared_ptr<TSnapshotMetadataCache> ReadMetadataCache() override
    {
        return ReadCache_;
    }

    // A verified candidate is tentative until PublicationFinished confirms the
    // outer COMMIT. Recovery rebuilds the generation independently.
    void RecordCandidate(std::string_view collectionID, std::shared_ptr<const NGlamDB::NStorage::TBaseState> candidate);
    void DiscardCandidate(std::string_view collectionID);

private:
    struct TCollectionState
    {
        std::shared_ptr<const NGlamDB::NStorage::TBaseState> Confirmed;
        std::shared_ptr<const NGlamDB::NStorage::TBaseState> Tentative;
        std::shared_ptr<ResourceReservation> ConfirmedMemory, TentativeMemory;
    };

    const StoreInfo Store_;
    const TLocalStorageOptions Options_;
    const std::shared_ptr<NGlamDB::NIO::TStoreDirs> Dirs_;
    const std::shared_ptr<NGlamDB::NStorage::TDurableLocalFileIO> FileIO_;
    const std::shared_ptr<iceberg::Catalog> Catalog_;
    const std::shared_ptr<TSnapshotMetadataCache> ReadCache_;

    std::mutex Mutex_;
    std::map<std::string, TCollectionState, std::less<>> Collections_;
    bool VerifiedGeneration_ = false;
    // Preserve admitted root capacity while invalidating uncertain metadata.
    // Recovery can rebuild the selected root even when ordinary memory is full.
    std::map<std::string, std::shared_ptr<ResourceReservation>, std::less<>> CacheMemory_;
    // Warehouse bytes measured by the admission preflight of the append that
    // is about to be prepared. Binding writes only the catalog, so
    // PrepareAppend reuses them instead of walking the warehouse again.
    std::optional<std::uint64_t> AdmittedWarehouseBytes_;

    TLocalStorageBackend(StoreInfo store,
        TLocalStorageOptions options,
        std::shared_ptr<NGlamDB::NIO::TStoreDirs> dirs,
        std::shared_ptr<NGlamDB::NStorage::TDurableLocalFileIO> fileIO,
        std::shared_ptr<TSnapshotMetadataCache> readCache);

    void CheckStore(const StoreInfo& store) const;
    // Root identity, warehouse limit and free-space reserve before a write.
    // `remember` keeps the measured warehouse bytes for the same append's
    // PrepareAppend; `reuse` consumes them instead of walking again. Returns
    // the warehouse bytes it checked.
    NGlamDB::NIO::TResult<std::uint64_t> CheckWriteCapacity(bool remember, bool reuse);
    // The caller holds Mutex_. Usage and cached bases are authoritative only
    // after VerifyRetained; InvalidateCache revokes them.
    void RequireVerified() const;
    NGlamDB::NStorage::TStorageUsage ComputeStoreUsage();
    std::shared_ptr<const NGlamDB::NStorage::TBaseState> ResolveBase(const BaseIdentity& base);
    std::shared_ptr<ResourceReservation> ReserveRootMemory(std::string_view collectionID, uint64_t bytes, bool reuse);
    std::shared_ptr<const NGlamDB::NStorage::TBaseState> VerifyCollection(const BaseIdentity& base);
};

} // namespace glamdb::detail
