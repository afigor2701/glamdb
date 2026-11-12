#pragma once

#include "core/storage/scan_index.h"
#include "storage/scan.h"

#include <iceberg/manifest/manifest_list.h>

#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace iceberg {
struct DataFile;
class FileIO;
class PartitionSpec;
class Schema;
struct TableMetadata;
} // namespace iceberg

namespace NGlamDB::NStorage {
struct TBaseState;
}

namespace glamdb::detail {

class TSnapshotMetadataCache;

Result<std::shared_ptr<const iceberg::TableMetadata>> ReadScanRoot(const std::shared_ptr<iceberg::FileIO>& io,
    const BaseIdentity& base,
    TSnapshotMetadataCache* cache = nullptr);

using TScanFile = NGlamDB::NStorage::TScanDataFile;

struct TScanPlan
{
    // nullopt is the initial empty view.
    std::optional<int64_t> SnapshotID;
    std::vector<TScanFile> Files;
};

// A verified membership prefix, or manifests for the uncached planner.
struct TScanManifests
{
    // nullopt is the initial empty view.
    std::optional<int64_t> SnapshotID;
    std::shared_ptr<iceberg::Schema> Schema;
    std::vector<std::shared_ptr<iceberg::PartitionSpec>> Specs;
    std::vector<iceberg::ManifestFile> Manifests;
    // An indexed plan selects a prefix without copying its descriptors. Files
    // are consumed in reverse order to match Iceberg fast-append manifests.
    std::shared_ptr<const NGlamDB::NStorage::TScanMembership> Index{};
    size_t FileCount = 0;
};

// Resolves the selector once against the committed root of `base` and reads
// that snapshot's manifest list. Warehouse directories are never listed, so
// orphan and unpublished files are not visible.
Result<TScanManifests> PlanManifests(const std::shared_ptr<iceberg::FileIO>& io,
    const BaseIdentity& base,
    const SnapshotSelector& selector,
    TSnapshotMetadataCache* cache = nullptr);

Result<TScanManifests> PlanVerifiedScan(const NGlamDB::NStorage::TBaseState& state,
    const BaseIdentity& base,
    const SnapshotSelector& selector);

// The live data files of one planned manifest.
Result<std::vector<TScanFile>> ManifestFiles(const std::shared_ptr<iceberg::FileIO>& io,
    const TScanManifests& plan,
    size_t index,
    TSnapshotMetadataCache* cache = nullptr);

Result<std::shared_ptr<const std::vector<std::shared_ptr<const iceberg::DataFile>>>> ReadManifestDataFiles(
    const std::shared_ptr<iceberg::FileIO>& io,
    const TScanManifests& plan,
    size_t index,
    TSnapshotMetadataCache* cache = nullptr);

// PlanManifests followed by every manifest's files.
Result<TScanPlan> PlanScan(const std::shared_ptr<iceberg::FileIO>& io,
    const BaseIdentity& base,
    const SnapshotSelector& selector,
    TSnapshotMetadataCache* cache = nullptr);

} // namespace glamdb::detail
