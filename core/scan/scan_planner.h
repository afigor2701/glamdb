#pragma once

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

namespace glamdb::detail {

Result<std::unique_ptr<iceberg::TableMetadata>> ReadScanRoot(const std::shared_ptr<iceberg::FileIO>& io,
    const BaseIdentity& base);

struct TScanFile
{
    std::string Path;
    int64_t SizeBytes = 0;
    int64_t RecordCount = 0;
};

struct TScanPlan
{
    // nullopt is the initial empty view.
    std::optional<int64_t> SnapshotID;
    std::vector<TScanFile> Files;
};

// The selected snapshot's manifests; their data files are listed on demand.
struct TScanManifests
{
    // nullopt is the initial empty view.
    std::optional<int64_t> SnapshotID;
    std::shared_ptr<iceberg::Schema> Schema;
    std::vector<std::shared_ptr<iceberg::PartitionSpec>> Specs;
    std::vector<iceberg::ManifestFile> Manifests;
};

// Resolves the selector once against the committed root of `base` and reads
// that snapshot's manifest list. Warehouse directories are never listed, so
// orphan and unpublished files are not visible.
Result<TScanManifests>
PlanManifests(const std::shared_ptr<iceberg::FileIO>& io, const BaseIdentity& base, const SnapshotSelector& selector);

// The live data files of one planned manifest.
Result<std::vector<TScanFile>>
ManifestFiles(const std::shared_ptr<iceberg::FileIO>& io, const TScanManifests& plan, size_t index);

Result<std::vector<std::shared_ptr<iceberg::DataFile>>>
ReadManifestDataFiles(const std::shared_ptr<iceberg::FileIO>& io, const TScanManifests& plan, size_t index);

// PlanManifests followed by every manifest's files.
Result<TScanPlan>
PlanScan(const std::shared_ptr<iceberg::FileIO>& io, const BaseIdentity& base, const SnapshotSelector& selector);

} // namespace glamdb::detail
