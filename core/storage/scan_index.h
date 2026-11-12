#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace iceberg {
struct DataFile;
}

namespace NGlamDB::NStorage {

struct TScanDataFile
{
    std::string Path;
    std::int64_t SizeBytes = 0;
    std::int64_t RecordCount = 0;
    std::shared_ptr<const iceberg::DataFile> DataFile{};

    bool operator==(const TScanDataFile& other) const
    {
        return Path == other.Path && SizeBytes == other.SizeBytes && RecordCount == other.RecordCount;
    }
};

struct TSnapshotBoundary
{
    std::int64_t SnapshotID;
    std::size_t FileCount;
};

// Built only by strict append-closure verification. Files are chronological;
// each retained snapshot selects a prefix. Boundaries are sorted by snapshot
// ID (IDs themselves are opaque). This representation requires the fixed-schema,
// unpartitioned, append-only profile: no deletes, branches or compaction.
struct TScanMembership
{
    std::vector<TScanDataFile> Files;
    std::vector<TSnapshotBoundary> Snapshots;
};

} // namespace NGlamDB::NStorage
