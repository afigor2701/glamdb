#if GLAMDB_TEST_STORAGE

#include "core/scan/metadata_cache.h"

#include "core/engine/storage_mapping.h"
#include "core/storage/table_profile.h"
#include "storage/schema.h"

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <gtest/gtest.h>
#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/manifest/manifest_list.h>
#include <iceberg/partition_spec.h>
#include <iceberg/sort_order.h>
#include <iceberg/table_metadata.h>
#include <parquet/arrow/writer.h>
#include <parquet/file_reader.h>
#include <parquet/metadata.h>

#include <algorithm>
#include <cstdlib>
#include <new>
#include <string>
#include <vector>

#if !defined(GLAMDB_HAS_INTERCEPTING_SANITIZER)
#if defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__)
#define GLAMDB_HAS_INTERCEPTING_SANITIZER 1
#elif defined(__has_feature)
#if __has_feature(address_sanitizer) || __has_feature(thread_sanitizer) || __has_feature(memory_sanitizer) ||          \
    __has_feature(leak_sanitizer)
#define GLAMDB_HAS_INTERCEPTING_SANITIZER 1
#endif
#endif
#endif

#if !defined(GLAMDB_HAS_INTERCEPTING_SANITIZER)

namespace {

struct TGlobalNewFaultState
{
    bool Active = false;
    bool FailAll = false;
    size_t Calls = 0;
    size_t FailOnCall = 0;
};

thread_local TGlobalNewFaultState TlsNewFault{};

class TScopedGlobalNewFault
{
public:
    explicit TScopedGlobalNewFault(size_t failOnCall, bool failAll = false) noexcept
    {
        TlsNewFault = TGlobalNewFaultState{
            .Active = true,
            .FailAll = failAll,
            .Calls = 0,
            .FailOnCall = failOnCall,
        };
    }

    ~TScopedGlobalNewFault()
    {
        TlsNewFault = TGlobalNewFaultState{};
    }

    static size_t Calls() noexcept
    {
        return TlsNewFault.Calls;
    }
};

bool ShouldFailGlobalNew() noexcept
{
    if (!TlsNewFault.Active) {
        return false;
    }
    ++TlsNewFault.Calls;
    return TlsNewFault.FailAll || (TlsNewFault.FailOnCall > 0 && TlsNewFault.Calls == TlsNewFault.FailOnCall);
}

void* AllocateUnaligned(std::size_t size, bool canThrow)
{
    if (ShouldFailGlobalNew()) {
        if (canThrow) {
            throw std::bad_alloc{};
        }
        return nullptr;
    }
    if (void* ptr = std::malloc(size == 0 ? 1 : size)) {
        return ptr;
    }
    if (canThrow) {
        throw std::bad_alloc{};
    }
    return nullptr;
}

void* AllocateAligned(std::size_t size, std::align_val_t alignment, bool canThrow)
{
    if (ShouldFailGlobalNew()) {
        if (canThrow) {
            throw std::bad_alloc{};
        }
        return nullptr;
    }
    const std::size_t effectiveAlign = std::max(static_cast<std::size_t>(alignment), sizeof(void*));
    if ((effectiveAlign & (effectiveAlign - 1)) != 0) {
        if (canThrow) {
            throw std::bad_alloc{};
        }
        return nullptr;
    }
    void* ptr = nullptr;
    if (posix_memalign(&ptr, effectiveAlign, size == 0 ? 1 : size) == 0) {
        return ptr;
    }
    if (canThrow) {
        throw std::bad_alloc{};
    }
    return nullptr;
}

} // namespace

void* operator new(std::size_t size)
{
    return AllocateUnaligned(size, true);
}

void* operator new(std::size_t size, const std::nothrow_t&) noexcept
{
    return AllocateUnaligned(size, false);
}

void* operator new[](std::size_t size)
{
    return AllocateUnaligned(size, true);
}

void* operator new[](std::size_t size, const std::nothrow_t&) noexcept
{
    return AllocateUnaligned(size, false);
}

void* operator new(std::size_t size, std::align_val_t alignment)
{
    return AllocateAligned(size, alignment, true);
}

void* operator new(std::size_t size, std::align_val_t alignment, const std::nothrow_t&) noexcept
{
    return AllocateAligned(size, alignment, false);
}

void* operator new[](std::size_t size, std::align_val_t alignment)
{
    return AllocateAligned(size, alignment, true);
}

void* operator new[](std::size_t size, std::align_val_t alignment, const std::nothrow_t&) noexcept
{
    return AllocateAligned(size, alignment, false);
}

void operator delete(void* ptr) noexcept
{
    std::free(ptr);
}

void operator delete(void* ptr, std::size_t) noexcept
{
    std::free(ptr);
}

void operator delete(void* ptr, const std::nothrow_t&) noexcept
{
    std::free(ptr);
}

void operator delete[](void* ptr) noexcept
{
    std::free(ptr);
}

void operator delete[](void* ptr, std::size_t) noexcept
{
    std::free(ptr);
}

void operator delete[](void* ptr, const std::nothrow_t&) noexcept
{
    std::free(ptr);
}

void operator delete(void* ptr, std::align_val_t) noexcept
{
    std::free(ptr);
}

void operator delete(void* ptr, std::size_t, std::align_val_t) noexcept
{
    std::free(ptr);
}

void operator delete(void* ptr, std::align_val_t, const std::nothrow_t&) noexcept
{
    std::free(ptr);
}

void operator delete[](void* ptr, std::align_val_t) noexcept
{
    std::free(ptr);
}

void operator delete[](void* ptr, std::size_t, std::align_val_t) noexcept
{
    std::free(ptr);
}

void operator delete[](void* ptr, std::align_val_t, const std::nothrow_t&) noexcept
{
    std::free(ptr);
}

namespace glamdb::detail {
namespace {

std::shared_ptr<const iceberg::TableMetadata> MakeRoot()
{
    const SchemaSpec schema{{
        {"key", FieldType::String, false},
        {"value", FieldType::Int64, true},
    }};
    auto profile = NGlamDB::NStorage::MakeTableProfile(ToStorageSchema(schema));
    EXPECT_TRUE(profile.has_value());
    auto root = iceberg::TableMetadata::Make(*profile->Schema,
        *iceberg::PartitionSpec::Unpartitioned(),
        *iceberg::SortOrder::Unsorted(),
        "store://s/warehouse/c/metadata/00001-a.metadata.json",
        profile->Properties);
    EXPECT_TRUE(root.has_value());
    return std::shared_ptr<const iceberg::TableMetadata>(std::move(*root));
}

std::shared_ptr<parquet::FileMetaData> MakeFooter()
{
    auto arrowSchema = arrow::schema({arrow::field("key", arrow::utf8(), false)});
    arrow::StringBuilder builder;
    EXPECT_TRUE(builder.Append("row-0").ok());
    std::shared_ptr<arrow::Array> col;
    EXPECT_TRUE(builder.Finish(&col).ok());
    auto table = arrow::Table::Make(arrowSchema, {col});
    auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
    EXPECT_TRUE(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink, 1).ok());
    auto buffer = sink->Finish().ValueOrDie();
    auto reader = parquet::ParquetFileReader::Open(std::make_shared<arrow::io::BufferReader>(buffer));
    return reader->metadata();
}

TEST(SnapshotMetadataCacheGlobalOomTest, GlobalOperatorNewFailuresNeverCorruptLruOrUsedBytes)
{
    const auto root = MakeRoot();
    const auto emptyMl = std::make_shared<const TSnapshotMetadataCache::TManifestListValue>();
    const auto emptyManifest = std::make_shared<const TSnapshotMetadataCache::TManifestDataFilesValue>();
    const auto footer = MakeFooter();

    // Pre-build 600-char keys outside global operator new fault scopes.
    std::vector<std::string> keys;
    for (int i = 0; i < 12; ++i) {
        keys.push_back(std::string(600, static_cast<char>('a' + i)) + "-" + std::to_string(i));
    }

    // Exact PR #142 review probe: MaxBytes = 1024 with 600-char keys.
    {
        TSnapshotMetadataCache cache({}, TSnapshotMetadataCacheOptions{.MaxBytes = 1024});
        for (size_t step = 1; step <= 6; ++step) {
            TScopedGlobalNewFault fault(step);
            try {
                cache.PutManifestList(keys[0], emptyMl);
            } catch (const std::bad_alloc&) {
            }
            cache.Clear();
            EXPECT_EQ(cache.Stats().UsedBytes, 0U);
        }
        cache.Clear();

        // Fail mid-insertion without Clear(), then insert 8 keys forcing eviction:
        // UsedBytes must never underflow!
        for (size_t step = 1; step <= 4; ++step) {
            bool threw = false;
            {
                TScopedGlobalNewFault fault(step);
                try {
                    cache.PutManifestList(keys[step], emptyMl);
                } catch (const std::bad_alloc&) {
                    threw = true;
                }
            }
            EXPECT_TRUE(threw) << "step=" << step;
            EXPECT_EQ(cache.Stats().UsedBytes, 0U);
            EXPECT_EQ(cache.Stats().ManifestListEntries, 0U);
        }
        for (int i = 0; i < 8; ++i) {
            cache.PutManifestList(keys[static_cast<size_t>(i)], emptyMl);
            EXPECT_LE(cache.Stats().UsedBytes, 1024U);
        }

        // Lookups on 600-char keys perform zero global heap allocations.
        {
            size_t calls = 0;
            std::shared_ptr<const TSnapshotMetadataCache::TManifestListValue> hit;
            std::shared_ptr<const TSnapshotMetadataCache::TManifestListValue> miss;
            {
                TScopedGlobalNewFault noAlloc(/*failOnCall=*/0, /*failAll=*/true);
                hit = cache.FindManifestList(keys[7]);
                miss = cache.FindManifestList(keys[0]);
                calls = TScopedGlobalNewFault::Calls();
            }
            EXPECT_EQ(hit, emptyMl);
            EXPECT_EQ(miss, nullptr);
            EXPECT_EQ(calls, 0U);
        }
    }

    // Global operator new check across Root, Manifest, and Footer as well.
    {
        TSnapshotMetadataCache cache({}, TSnapshotMetadataCacheOptions{.MaxBytes = 8192});
        for (size_t step = 1; step <= 4; ++step) {
            bool threwRoot = false;
            bool threwManifest = false;
            bool threwFooter = false;
            {
                TScopedGlobalNewFault fault(step);
                try {
                    cache.PutRoot(keys[0], 0, root);
                } catch (const std::bad_alloc&) {
                    threwRoot = true;
                }
            }
            {
                TScopedGlobalNewFault fault(step);
                try {
                    cache.PutManifest(keys[1], 64, emptyManifest);
                } catch (const std::bad_alloc&) {
                    threwManifest = true;
                }
            }
            {
                TScopedGlobalNewFault fault(step);
                try {
                    cache.PutFooter(keys[2], 256, 1, footer);
                } catch (const std::bad_alloc&) {
                    threwFooter = true;
                }
            }
            EXPECT_TRUE(threwRoot) << "step=" << step;
            EXPECT_TRUE(threwManifest) << "step=" << step;
            EXPECT_TRUE(threwFooter) << "step=" << step;
            EXPECT_EQ(cache.Stats().UsedBytes, 0U);
        }
        cache.PutRoot(keys[0], 0, root);
        cache.PutManifest(keys[1], 64, emptyManifest);
        cache.PutFooter(keys[2], 256, 1, footer);
        EXPECT_LE(cache.Stats().UsedBytes, 8192U);

        size_t calls = 0;
        std::shared_ptr<const iceberg::TableMetadata> gotRoot;
        std::shared_ptr<const TSnapshotMetadataCache::TManifestDataFilesValue> gotManifest;
        std::shared_ptr<parquet::FileMetaData> gotFooter;
        std::shared_ptr<const iceberg::TableMetadata> evictedRoot;
        {
            TScopedGlobalNewFault noAlloc(/*failOnCall=*/0, /*failAll=*/true);
            gotRoot = cache.FindRoot(keys[0], 0);
            gotManifest = cache.FindManifest(keys[1], 64);
            gotFooter = cache.FindFooter(keys[2], 256, 1);
            cache.EvictRoot(keys[0]);
            evictedRoot = cache.FindRoot(keys[0], 0);
            calls = TScopedGlobalNewFault::Calls();
        }
        EXPECT_EQ(gotRoot, root);
        EXPECT_EQ(gotManifest, emptyManifest);
        EXPECT_EQ(gotFooter, footer);
        EXPECT_EQ(evictedRoot, nullptr);
        EXPECT_EQ(calls, 0U);
    }
}

} // namespace
} // namespace glamdb::detail

#else

#include <gtest/gtest.h>

TEST(SnapshotMetadataCacheGlobalOomTest, CoveredByAllocatorInjectorUnderSanitizer)
{
    SUCCEED();
}

#endif

#endif
