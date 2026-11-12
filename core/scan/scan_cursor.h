#pragma once

#include "storage/scan.h"

#include <atomic>
#include <functional>
#include <memory>

namespace arrow {
class MemoryPool;
} // namespace arrow

namespace iceberg {
class FileIO;
} // namespace iceberg

namespace NGlamDB::NStorage {
struct TBaseState;
}

namespace glamdb::detail {

class TSnapshotMetadataCache;

struct TScanContext
{
    std::shared_ptr<ResourceOwner> Resources{};
    CollectionInfo Collection;
    // The committed view resolved once by the engine.
    BaseIdentity Base;
    std::shared_ptr<iceberg::FileIO> FileIO;
    std::shared_ptr<TSnapshotMetadataCache> Cache{};
    // Aliases the engine's closing flag and retains the engine state, its
    // catalog and store lease until the cursor is released.
    std::shared_ptr<const std::atomic<bool>> Closing;
    // Retains engine resources and the store lock in returned Arrow buffers
    // until the last batch, extracted buffer, slice or C Data export is released.
    std::shared_ptr<const void> Lease;
    // Optional owned upstream allocator for decoded and filtered Arrow buffers.
    // A per-scan resource pool wraps it; empty selects Arrow's default pool.
    std::shared_ptr<arrow::MemoryPool> Pool{};
    bool EnableFilePruning = true;
    bool EnableRowGroupPruning = true;
    std::function<void(const ErrorDetail&)> OnError;
    std::shared_ptr<const NGlamDB::NStorage::TBaseState> VerifiedState{};
};

Result<std::shared_ptr<ScanCursor>> OpenScan(TScanContext context, const ScanRequest& request);

} // namespace glamdb::detail
