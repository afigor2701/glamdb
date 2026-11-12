#pragma once

#include "core/resources/envelopes.h"

#include <iceberg/manifest/manifest_list.h>

#include <cstddef>
#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

namespace iceberg {
struct DataFile;
struct TableMetadata;
} // namespace iceberg

namespace parquet {
class FileMetaData;
} // namespace parquet

namespace glamdb {
class ResourceReservation;
} // namespace glamdb

namespace glamdb::detail {

struct TSnapshotMetadataCacheOptions
{
    uint64_t MaxBytes = ReadCacheMemoryBytes;
    size_t MaxRoots = 16;
    size_t MaxManifestLists = 64;
    size_t MaxManifests = 512;
    size_t MaxFooters = 1024;
};

struct TSnapshotMetadataCacheStats
{
    uint64_t UsedBytes = 0;
    uint64_t MaxBytes = 0;
    size_t RootEntries = 0;
    size_t ManifestListEntries = 0;
    size_t ManifestEntries = 0;
    size_t FooterEntries = 0;
    uint64_t Hits = 0;
    uint64_t Misses = 0;
    uint64_t Evictions = 0;
};

// Bounded, thread-safe per-store cache of immutable Iceberg roots, manifest
// lists, manifest live-entry vectors, and verified Parquet footers.
class TSnapshotMetadataCache
{
public:
    using TManifestListValue = std::vector<iceberg::ManifestFile>;
    using TManifestDataFilesValue = std::vector<std::shared_ptr<const iceberg::DataFile>>;

    explicit TSnapshotMetadataCache(std::shared_ptr<ResourceReservation> reservation = {},
        TSnapshotMetadataCacheOptions options = {});

    std::shared_ptr<const iceberg::TableMetadata> FindRoot(std::string_view metadataLocation, int64_t fileSize);
    void PutRoot(std::string_view metadataLocation,
        int64_t fileSize,
        std::shared_ptr<const iceberg::TableMetadata> metadata);
    void EvictRoot(std::string_view metadataLocation);

    std::shared_ptr<const TManifestListValue> FindManifestList(std::string_view manifestListPath);
    void PutManifestList(std::string_view manifestListPath, std::shared_ptr<const TManifestListValue> manifests);

    std::shared_ptr<const TManifestDataFilesValue> FindManifest(std::string_view manifestPath, int64_t manifestLength);
    void PutManifest(std::string_view manifestPath,
        int64_t manifestLength,
        std::shared_ptr<const TManifestDataFilesValue> files);

    std::shared_ptr<parquet::FileMetaData> FindFooter(std::string_view path, int64_t sizeBytes, int64_t recordCount);
    void PutFooter(std::string_view path,
        int64_t sizeBytes,
        int64_t recordCount,
        std::shared_ptr<parquet::FileMetaData> footer);

    void Clear();
    TSnapshotMetadataCacheStats Stats() const;

private:
    struct TRootKey
    {
        std::string MetadataLocation;
        int64_t FileSize = 0;

        auto operator<=>(const TRootKey&) const = default;
    };

    struct TManifestKey
    {
        std::string ManifestPath;
        int64_t ManifestLength = 0;

        auto operator<=>(const TManifestKey&) const = default;
    };

    struct TFooterKey
    {
        std::string Path;
        int64_t SizeBytes = 0;
        int64_t RecordCount = 0;

        auto operator<=>(const TFooterKey&) const = default;
    };

    template <typename TKey, typename TValue>
    struct TLruEntry
    {
        TKey Key;
        std::shared_ptr<TValue> Value;
        uint64_t Bytes = 0;
        uint64_t Tick = 0;
    };

    using TRootList = std::list<TLruEntry<TRootKey, const iceberg::TableMetadata>>;
    using TManifestListList = std::list<TLruEntry<std::string, const TManifestListValue>>;
    using TManifestEntriesList = std::list<TLruEntry<TManifestKey, const TManifestDataFilesValue>>;
    using TFooterList = std::list<TLruEntry<TFooterKey, parquet::FileMetaData>>;

    const std::shared_ptr<ResourceReservation> Reservation_;
    const TSnapshotMetadataCacheOptions Options_;

    mutable std::mutex Mutex_;
    uint64_t UsedBytes_ = 0;
    uint64_t Tick_ = 0;
    uint64_t Hits_ = 0;
    uint64_t Misses_ = 0;
    uint64_t Evictions_ = 0;

    TRootList RootsLru_;
    std::map<TRootKey, TRootList::iterator> Roots_;

    TManifestListList ManifestListsLru_;
    std::map<std::string, TManifestListList::iterator, std::less<>> ManifestLists_;

    TManifestEntriesList ManifestsLru_;
    std::map<TManifestKey, TManifestEntriesList::iterator> Manifests_;

    TFooterList FootersLru_;
    std::map<TFooterKey, TFooterList::iterator> Footers_;

    bool EvictOldestLocked();
    void MakeRoomForBytesLocked(uint64_t incomingBytes);
};

} // namespace glamdb::detail
