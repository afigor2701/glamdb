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
#include <tuple>
#include <type_traits>
#include <utility>
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

#if defined(GLAMDB_ENABLE_CACHE_TEST_HOOKS)

void MaybeFailCacheAllocationForTesting();

// Thread-local allocation counter and deterministic std::bad_alloc fault
// injector for unit tests of TSnapshotMetadataCache.
class TScopedCacheAllocInjector
{
public:
    explicit TScopedCacheAllocInjector(size_t failOnCall, bool failAll = false) noexcept;
    ~TScopedCacheAllocInjector();

    TScopedCacheAllocInjector(const TScopedCacheAllocInjector&) = delete;
    TScopedCacheAllocInjector& operator=(const TScopedCacheAllocInjector&) = delete;

    static size_t AllocationCount() noexcept;
};

template <typename T>
struct TCacheAllocator
{
    using value_type = T;
    using is_always_equal = std::true_type;

    constexpr TCacheAllocator() noexcept = default;

    template <typename U>
    constexpr TCacheAllocator(const TCacheAllocator<U>&) noexcept
    { }

    T* allocate(size_t count)
    {
        MaybeFailCacheAllocationForTesting();
        return std::allocator<T>{}.allocate(count);
    }

    void deallocate(T* ptr, size_t count) noexcept
    {
        std::allocator<T>{}.deallocate(ptr, count);
    }

    template <typename U>
    constexpr bool operator==(const TCacheAllocator<U>&) const noexcept
    {
        return true;
    }
};

#else

template <typename T>
using TCacheAllocator = std::allocator<T>;

#endif

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
    void TryPutRoot(std::string_view metadataLocation,
        int64_t fileSize,
        std::shared_ptr<const iceberg::TableMetadata> metadata) noexcept;
    void EvictRoot(std::string_view metadataLocation);

    std::shared_ptr<const TManifestListValue> FindManifestList(std::string_view manifestListPath);
    void PutManifestList(std::string_view manifestListPath, std::shared_ptr<const TManifestListValue> manifests);
    void TryPutManifestList(std::string_view manifestListPath,
        std::shared_ptr<const TManifestListValue> manifests) noexcept;

    std::shared_ptr<const TManifestDataFilesValue> FindManifest(std::string_view manifestPath, int64_t manifestLength);
    void PutManifest(std::string_view manifestPath,
        int64_t manifestLength,
        std::shared_ptr<const TManifestDataFilesValue> files);
    void TryPutManifest(std::string_view manifestPath,
        int64_t manifestLength,
        std::shared_ptr<const TManifestDataFilesValue> files) noexcept;

    std::shared_ptr<parquet::FileMetaData> FindFooter(std::string_view path, int64_t sizeBytes, int64_t recordCount);
    void PutFooter(std::string_view path,
        int64_t sizeBytes,
        int64_t recordCount,
        std::shared_ptr<parquet::FileMetaData> footer);
    void TryPutFooter(std::string_view path,
        int64_t sizeBytes,
        int64_t recordCount,
        std::shared_ptr<parquet::FileMetaData> footer) noexcept;

    void Clear();
    TSnapshotMetadataCacheStats Stats() const;

private:
    using TCacheString = std::basic_string<char, std::char_traits<char>, TCacheAllocator<char>>;

    struct TCacheStringLess
    {
        using is_transparent = void;

        template <typename TLhs, typename TRhs>
        constexpr bool operator()(const TLhs& lhs, const TRhs& rhs) const noexcept
        {
            return std::string_view(lhs) < std::string_view(rhs);
        }
    };

    struct TRootKey
    {
        TCacheString MetadataLocation;
        int64_t FileSize = 0;
    };

    struct TRootKeyView
    {
        std::string_view MetadataLocation;
        int64_t FileSize = 0;
    };

    struct TRootKeyLess
    {
        using is_transparent = void;

        static constexpr std::tuple<std::string_view, int64_t> Project(const TRootKey& key) noexcept
        {
            return {std::string_view(key.MetadataLocation), key.FileSize};
        }

        static constexpr std::tuple<std::string_view, int64_t> Project(const TRootKeyView& key) noexcept
        {
            return {key.MetadataLocation, key.FileSize};
        }

        template <typename TLhs, typename TRhs>
        constexpr bool operator()(const TLhs& lhs, const TRhs& rhs) const noexcept
        {
            return Project(lhs) < Project(rhs);
        }
    };

    struct TManifestKey
    {
        TCacheString ManifestPath;
        int64_t ManifestLength = 0;
    };

    struct TManifestKeyView
    {
        std::string_view ManifestPath;
        int64_t ManifestLength = 0;
    };

    struct TManifestKeyLess
    {
        using is_transparent = void;

        static constexpr std::tuple<std::string_view, int64_t> Project(const TManifestKey& key) noexcept
        {
            return {std::string_view(key.ManifestPath), key.ManifestLength};
        }

        static constexpr std::tuple<std::string_view, int64_t> Project(const TManifestKeyView& key) noexcept
        {
            return {key.ManifestPath, key.ManifestLength};
        }

        template <typename TLhs, typename TRhs>
        constexpr bool operator()(const TLhs& lhs, const TRhs& rhs) const noexcept
        {
            return Project(lhs) < Project(rhs);
        }
    };

    struct TFooterKey
    {
        TCacheString Path;
        int64_t SizeBytes = 0;
        int64_t RecordCount = 0;
    };

    struct TFooterKeyView
    {
        std::string_view Path;
        int64_t SizeBytes = 0;
        int64_t RecordCount = 0;
    };

    struct TFooterKeyLess
    {
        using is_transparent = void;

        static constexpr std::tuple<std::string_view, int64_t, int64_t> Project(const TFooterKey& key) noexcept
        {
            return {std::string_view(key.Path), key.SizeBytes, key.RecordCount};
        }

        static constexpr std::tuple<std::string_view, int64_t, int64_t> Project(const TFooterKeyView& key) noexcept
        {
            return {key.Path, key.SizeBytes, key.RecordCount};
        }

        template <typename TLhs, typename TRhs>
        constexpr bool operator()(const TLhs& lhs, const TRhs& rhs) const noexcept
        {
            return Project(lhs) < Project(rhs);
        }
    };

    template <typename TKey, typename TValue>
    struct TLruEntry
    {
        TKey Key;
        std::shared_ptr<TValue> Value;
        uint64_t Bytes = 0;
        uint64_t Tick = 0;
    };

    template <typename TKey, typename TValue>
    using TCacheList = std::list<TLruEntry<TKey, TValue>, TCacheAllocator<TLruEntry<TKey, TValue>>>;

    template <typename TKey, typename TList, typename TCompare>
    using TCacheMap = std::
        map<TKey, typename TList::iterator, TCompare, TCacheAllocator<std::pair<const TKey, typename TList::iterator>>>;

    using TRootList = TCacheList<TRootKey, const iceberg::TableMetadata>;
    using TManifestListList = TCacheList<TCacheString, const TManifestListValue>;
    using TManifestEntriesList = TCacheList<TManifestKey, const TManifestDataFilesValue>;
    using TFooterList = TCacheList<TFooterKey, parquet::FileMetaData>;

    const std::shared_ptr<ResourceReservation> Reservation_;
    const TSnapshotMetadataCacheOptions Options_;

    mutable std::mutex Mutex_;
    uint64_t UsedBytes_ = 0;
    uint64_t Tick_ = 0;
    uint64_t Hits_ = 0;
    uint64_t Misses_ = 0;
    uint64_t Evictions_ = 0;

    TRootList RootsLru_;
    TCacheMap<TRootKey, TRootList, TRootKeyLess> Roots_;

    TManifestListList ManifestListsLru_;
    TCacheMap<TCacheString, TManifestListList, TCacheStringLess> ManifestLists_;

    TManifestEntriesList ManifestsLru_;
    TCacheMap<TManifestKey, TManifestEntriesList, TManifestKeyLess> Manifests_;

    TFooterList FootersLru_;
    TCacheMap<TFooterKey, TFooterList, TFooterKeyLess> Footers_;

    template <typename TKey, typename TKeyView, typename TValue, typename TList, typename TMap, typename TMakeKey>
    void PutEntry(TKeyView keyView,
        TMakeKey&& makeKey,
        std::shared_ptr<TValue> value,
        uint64_t charge,
        size_t maxEntries,
        TList& lru,
        TMap& map);

    bool EvictOldestLocked();
    void MakeRoomForBytesLocked(uint64_t incomingBytes);
};

} // namespace glamdb::detail
