#if GLAMDB_TEST_STORAGE

#include "core/scan/metadata_cache.h"

#include "core/engine/storage_mapping.h"
#include "core/storage/table_profile.h"
#include "storage/resources.h"
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

#include <atomic>
#include <thread>
#include <vector>

namespace glamdb::detail {
namespace {

std::shared_ptr<const iceberg::TableMetadata> MakeDummyRoot(std::string_view location)
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
        std::string(location),
        profile->Properties);
    EXPECT_TRUE(root.has_value());
    return std::shared_ptr<const iceberg::TableMetadata>(std::move(*root));
}

std::shared_ptr<const TSnapshotMetadataCache::TManifestListValue> MakeDummyManifestList(std::string_view path)
{
    std::vector<iceberg::ManifestFile> list(1);
    list[0].manifest_path = std::string(path);
    list[0].manifest_length = 1024;
    return std::make_shared<const TSnapshotMetadataCache::TManifestListValue>(std::move(list));
}

std::shared_ptr<const TSnapshotMetadataCache::TManifestDataFilesValue> MakeDummyManifest(std::string_view filePath)
{
    auto df = std::make_shared<iceberg::DataFile>();
    df->file_path = std::string(filePath);
    df->file_size_in_bytes = 2048;
    df->record_count = 10;
    df->value_counts[1] = 10;
    df->null_value_counts[1] = 0;
    df->lower_bounds[1] = {'a'};
    df->upper_bounds[1] = {'z'};
    std::vector<std::shared_ptr<const iceberg::DataFile>> entries{df};
    return std::make_shared<const TSnapshotMetadataCache::TManifestDataFilesValue>(std::move(entries));
}

std::shared_ptr<parquet::FileMetaData> MakeDummyFooter()
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

TEST(SnapshotMetadataCacheUnitTest, EnforcesPerKindAndByteCapsWithLruOrderAndOversizeBypass)
{
    TSnapshotMetadataCache cache({},
        TSnapshotMetadataCacheOptions{
            .MaxBytes = 64ULL << 10,
            .MaxRoots = 2,
            .MaxManifestLists = 2,
            .MaxManifests = 2,
            .MaxFooters = 2,
        });

    const auto r1 = MakeDummyRoot("store://s/warehouse/c/metadata/00001-a.metadata.json");
    const auto r2 = MakeDummyRoot("store://s/warehouse/c/metadata/00002-b.metadata.json");
    const auto r3 = MakeDummyRoot("store://s/warehouse/c/metadata/00003-c.metadata.json");

    cache.PutRoot("root-1", 512, r1);
    cache.PutRoot("root-2", 512, r2);
    // Touch root-1 so root-2 becomes LRU among roots.
    EXPECT_EQ(cache.FindRoot("root-1", 512), r1);
    cache.PutRoot("root-3", 512, r3);
    EXPECT_EQ(cache.Stats().RootEntries, 2U);
    EXPECT_EQ(cache.FindRoot("root-1", 512), r1);
    EXPECT_EQ(cache.FindRoot("root-2", 512), nullptr);
    EXPECT_EQ(cache.FindRoot("root-3", 512), r3);

    const auto ml1 = MakeDummyManifestList("ml-1");
    const auto ml2 = MakeDummyManifestList("ml-2");
    const auto ml3 = MakeDummyManifestList("ml-3");
    cache.PutManifestList("ml-1", ml1);
    cache.PutManifestList("ml-2", ml2);
    EXPECT_EQ(cache.FindManifestList("ml-1"), ml1);
    cache.PutManifestList("ml-3", ml3);
    EXPECT_EQ(cache.Stats().ManifestListEntries, 2U);
    EXPECT_EQ(cache.FindManifestList("ml-2"), nullptr);
    EXPECT_EQ(cache.FindManifestList("ml-1"), ml1);
    EXPECT_EQ(cache.FindManifestList("ml-3"), ml3);

    const auto m1 = MakeDummyManifest("data-1.parquet");
    const auto m2 = MakeDummyManifest("data-2.parquet");
    const auto m3 = MakeDummyManifest("data-3.parquet");
    cache.PutManifest("m-1", 100, m1);
    cache.PutManifest("m-2", 100, m2);
    EXPECT_EQ(cache.FindManifest("m-1", 100), m1);
    cache.PutManifest("m-3", 100, m3);
    EXPECT_EQ(cache.Stats().ManifestEntries, 2U);
    EXPECT_EQ(cache.FindManifest("m-2", 100), nullptr);

    const auto footer = MakeDummyFooter();
    cache.PutFooter("f-1.parquet", 500, 1, footer);
    cache.PutFooter("f-2.parquet", 500, 1, footer);
    EXPECT_EQ(cache.FindFooter("f-1.parquet", 500, 1), footer);
    cache.PutFooter("f-3.parquet", 500, 1, footer);
    cache.PutFooter("f-3.parquet", 500, 1, footer); // idempotent overwrite of same key
    EXPECT_EQ(cache.Stats().FooterEntries, 2U);
    EXPECT_EQ(cache.FindFooter("f-2.parquet", 500, 1), nullptr);
    EXPECT_EQ(cache.FindFooter("f-1.parquet", 500, 1), footer);
    EXPECT_EQ(cache.FindFooter("f-3.parquet", 500, 1), footer);
    EXPECT_LE(cache.Stats().UsedBytes, cache.Stats().MaxBytes);

    // Oversized item (> 64 KiB estimated root charge via huge fileSize) is bypassed without evicting live entries!
    const auto statsBefore = cache.Stats();
    cache.PutRoot("huge-root", 64LL << 20, r1);
    EXPECT_EQ(cache.FindRoot("huge-root", 64LL << 20), nullptr);
    EXPECT_EQ(cache.Stats().RootEntries, statsBefore.RootEntries);
    EXPECT_EQ(cache.Stats().FooterEntries, statsBefore.FooterEntries);
    EXPECT_EQ(cache.FindFooter("f-1.parquet", 500, 1), footer);

    // Tight global byte cap evicts oldest across ALL 4 entry kinds while preserving touched items!
    TSnapshotMetadataCache sizer({}, TSnapshotMetadataCacheOptions{.MaxBytes = 1ULL << 20});
    sizer.PutManifestList("ml-old", ml1);
    const uint64_t mlBytes = sizer.Stats().UsedBytes;
    sizer.PutManifest("m-keep", 100, m1);
    const uint64_t mlAndManifestBytes = sizer.Stats().UsedBytes;
    const uint64_t manifestBytes = mlAndManifestBytes - mlBytes;
    sizer.PutRoot("root-new", 512, r1);
    const uint64_t rootBytes = sizer.Stats().UsedBytes - mlAndManifestBytes;

    // Fit m-keep + root-new (and ml-old + m-keep), but NOT all three together!
    const uint64_t tightCap = std::max(mlAndManifestBytes, manifestBytes + rootBytes) + 64;
    ASSERT_LT(tightCap, mlBytes + manifestBytes + rootBytes);

    TSnapshotMetadataCache tight({}, TSnapshotMetadataCacheOptions{.MaxBytes = tightCap});
    tight.PutManifestList("ml-old", ml1);
    tight.PutManifest("m-keep", 100, m1);
    // Touch m-keep so ml-old has the oldest global tick.
    ASSERT_EQ(tight.FindManifest("m-keep", 100), m1);
    tight.PutRoot("root-new", 512, r1);
    EXPECT_LE(tight.Stats().UsedBytes, tight.Stats().MaxBytes);
    EXPECT_EQ(tight.FindManifestList("ml-old"), nullptr);
    EXPECT_EQ(tight.FindManifest("m-keep", 100), m1);
    EXPECT_EQ(tight.FindRoot("root-new", 512), r1);
}

TEST(SnapshotMetadataCacheUnitTest, DisabledZeroByteCacheEvictRootClearAndThreadSafety)
{
    const auto r1 = MakeDummyRoot("store://s/warehouse/c/metadata/00001-a.metadata.json");
    const auto r2 = MakeDummyRoot("store://s/warehouse/c/metadata/00002-b.metadata.json");
    const auto m1 = MakeDummyManifest("data-1.parquet");
    const auto footer = MakeDummyFooter();

    // Zero-byte cache is cleanly disabled.
    {
        TSnapshotMetadataCache disabled({}, TSnapshotMetadataCacheOptions{.MaxBytes = 0});
        disabled.PutRoot("r1", 256, r1);
        disabled.PutManifest("m1", 100, m1);
        disabled.PutFooter("f1", 200, 1, footer);
        EXPECT_EQ(disabled.FindRoot("r1", 256), nullptr);
        EXPECT_EQ(disabled.FindManifest("m1", 100), nullptr);
        EXPECT_EQ(disabled.FindFooter("f1", 200, 1), nullptr);
        EXPECT_EQ(disabled.Stats().UsedBytes, 0U);
    }

    // EvictRoot removes only matching root location while Clear resets UsedBytes without dropping ResourceReservation.
    auto owner = std::make_shared<ResourceOwner>();
    auto reserved = owner->Reserve(ResourceClass::Metadata, 1ULL << 20);
    ASSERT_TRUE(reserved.has_value());
    {
        TSnapshotMetadataCache cache(*reserved, TSnapshotMetadataCacheOptions{.MaxBytes = 1ULL << 20});
        cache.PutRoot("loc-a", 100, r1);
        cache.PutRoot("loc-a", 200, r1);
        cache.PutRoot("loc-b", 100, r2);
        cache.PutManifest("m1", 100, m1);
        cache.EvictRoot("loc-a");
        EXPECT_EQ(cache.FindRoot("loc-a", 100), nullptr);
        EXPECT_EQ(cache.FindRoot("loc-a", 200), nullptr);
        EXPECT_EQ(cache.FindRoot("loc-b", 100), r2);
        EXPECT_EQ(cache.FindManifest("m1", 100), m1);

        reserved->reset();
        EXPECT_EQ(owner->GetUsage().memory_bytes[1], 1ULL << 20);
        cache.Clear();
        EXPECT_EQ(cache.Stats().UsedBytes, 0U);
        EXPECT_EQ(cache.Stats().RootEntries, 0U);
        EXPECT_EQ(cache.Stats().ManifestEntries, 0U);
        // Reservation stays owned by cache across Clear() until cache is destroyed.
        EXPECT_EQ(owner->GetUsage().memory_bytes[1], 1ULL << 20);

        // Concurrent readers/writers/evictors under small cap.
        std::vector<std::thread> threads;
        for (int t = 0; t < 4; ++t) {
            threads.emplace_back([&, t] {
                for (int i = 0; i < 100; ++i) {
                    const auto key = "k-" + std::to_string((t + i) % 6);
                    cache.PutRoot(key, 128, r1);
                    (void)cache.FindRoot(key, 128);
                    cache.PutManifest(key, 64, m1);
                    (void)cache.FindManifest(key, 64);
                    cache.PutFooter(key, 256, 1, footer);
                    (void)cache.FindFooter(key, 256, 1);
                    if (i % 17 == 0) {
                        cache.EvictRoot(key);
                    }
                    if (i % 41 == 0) {
                        cache.Clear();
                    }
                    EXPECT_LE(cache.Stats().UsedBytes, cache.Stats().MaxBytes);
                }
            });
        }
        for (auto& thread : threads) {
            thread.join();
        }
    }
    EXPECT_EQ(owner->GetUsage().memory_bytes[1], 0U);
}

TEST(SnapshotMetadataCacheUnitTest, StrongExceptionSafetyOnBadAllocAndZeroAllocLookups)
{
    const auto r1 = MakeDummyRoot("store://s/warehouse/c/metadata/00001-a.metadata.json");
    const auto r2 = MakeDummyRoot("store://s/warehouse/c/metadata/00002-b.metadata.json");
    const auto emptyMl = std::make_shared<const TSnapshotMetadataCache::TManifestListValue>();
    const auto emptyManifest = std::make_shared<const TSnapshotMetadataCache::TManifestDataFilesValue>();
    const auto footer = MakeDummyFooter();

    // 1. Exact reproducer from PR #142 review: 600-char key with tight MaxBytes = 1024
    //    for PutManifestList and PutManifest, plus tight caps for PutRoot and PutFooter.
    const std::string longPrefix(600, 'x');

    // Verify PutManifestList across every allocation step 1..N (both short and 600-char keys).
    for (const std::string& base : {std::string("s"), longPrefix}) {
        size_t allocSteps = 0;
        {
            TSnapshotMetadataCache probe({}, TSnapshotMetadataCacheOptions{.MaxBytes = 1024});
            TScopedCacheAllocInjector counter(/*failOnCall=*/0);
            probe.PutManifestList(base + "-0", emptyMl);
            allocSteps = TScopedCacheAllocInjector::AllocationCount();
        }
        ASSERT_GE(allocSteps, 2U);
        if (base.size() >= 600) {
            EXPECT_EQ(allocSteps, 4U);
        }

        TSnapshotMetadataCache cache({}, TSnapshotMetadataCacheOptions{.MaxBytes = 1024, .MaxManifestLists = 3});
        for (size_t step = 1; step <= allocSteps; ++step) {
            TScopedCacheAllocInjector fail(step);
            EXPECT_THROW(cache.PutManifestList(base + "-fail-" + std::to_string(step), emptyMl), std::bad_alloc);
            EXPECT_EQ(cache.Stats().UsedBytes, 0U);
            EXPECT_EQ(cache.Stats().ManifestListEntries, 0U);
        }

        // Insert a live entry, then fail an overwrite AND fail a new insertion at every step,
        // then insert 8 keys forcing evictions: UsedBytes must never underflow or exceed MaxBytes!
        cache.PutManifestList(base + "-keep", emptyMl);
        const auto seededStats = cache.Stats();
        ASSERT_GT(seededStats.UsedBytes, 0U);
        ASSERT_LE(seededStats.UsedBytes, 1024U);

        for (size_t step = 1; step <= allocSteps; ++step) {
            {
                TScopedCacheAllocInjector failOverwrite(step);
                EXPECT_THROW(cache.PutManifestList(base + "-keep", emptyMl), std::bad_alloc);
            }
            EXPECT_EQ(cache.Stats().UsedBytes, seededStats.UsedBytes);
            EXPECT_EQ(cache.Stats().ManifestListEntries, 1U);
            EXPECT_EQ(cache.FindManifestList(base + "-keep"), emptyMl);

            {
                TScopedCacheAllocInjector failNew(step);
                EXPECT_THROW(cache.PutManifestList(base + "-other-" + std::to_string(step), emptyMl), std::bad_alloc);
            }
            EXPECT_EQ(cache.Stats().UsedBytes, seededStats.UsedBytes);
            EXPECT_EQ(cache.Stats().ManifestListEntries, 1U);
        }

        for (int i = 0; i < 8; ++i) {
            cache.PutManifestList(base + "-ok-" + std::to_string(i), emptyMl);
            const auto st = cache.Stats();
            EXPECT_LE(st.UsedBytes, st.MaxBytes);
            EXPECT_LE(st.ManifestListEntries, 3U);
        }
    }

    // 2. Same transactional rollback check for PutManifest (MaxBytes = 1024, 600-char key),
    //    PutRoot (MaxBytes = 4096, 600-char key), and PutFooter (MaxBytes = 4096, 600-char key).
    {
        TSnapshotMetadataCache cache({}, TSnapshotMetadataCacheOptions{.MaxBytes = 1024, .MaxManifests = 3});
        for (size_t step = 1; step <= 4; ++step) {
            TScopedCacheAllocInjector fail(step);
            EXPECT_THROW(cache.PutManifest(longPrefix + "-m", 64, emptyManifest), std::bad_alloc);
            EXPECT_EQ(cache.Stats().UsedBytes, 0U);
            EXPECT_EQ(cache.Stats().ManifestEntries, 0U);
        }
        for (int i = 0; i < 8; ++i) {
            cache.PutManifest(longPrefix + "-m-" + std::to_string(i), 64, emptyManifest);
            EXPECT_LE(cache.Stats().UsedBytes, cache.Stats().MaxBytes);
        }
    }
    {
        TSnapshotMetadataCache cache({}, TSnapshotMetadataCacheOptions{.MaxBytes = 4096, .MaxRoots = 2});
        for (size_t step = 1; step <= 4; ++step) {
            TScopedCacheAllocInjector fail(step);
            EXPECT_THROW(cache.PutRoot(longPrefix + "-r", 0, r1), std::bad_alloc);
            EXPECT_EQ(cache.Stats().UsedBytes, 0U);
            EXPECT_EQ(cache.Stats().RootEntries, 0U);
        }
        cache.PutRoot(longPrefix + "-r-0", 0, r1);
        const uint64_t used = cache.Stats().UsedBytes;
        for (size_t step = 1; step <= 4; ++step) {
            TScopedCacheAllocInjector fail(step);
            EXPECT_THROW(cache.PutRoot(longPrefix + "-r-0", 0, r2), std::bad_alloc);
            EXPECT_EQ(cache.Stats().UsedBytes, used);
            EXPECT_EQ(cache.FindRoot(longPrefix + "-r-0", 0), r1);
        }
        for (int i = 1; i < 8; ++i) {
            cache.PutRoot(longPrefix + "-r-" + std::to_string(i), 0, r2);
            EXPECT_LE(cache.Stats().UsedBytes, cache.Stats().MaxBytes);
        }
    }
    {
        TSnapshotMetadataCache cache({}, TSnapshotMetadataCacheOptions{.MaxBytes = 4096, .MaxFooters = 2});
        for (size_t step = 1; step <= 4; ++step) {
            TScopedCacheAllocInjector fail(step);
            EXPECT_THROW(cache.PutFooter(longPrefix + "-f.parquet", 256, 1, footer), std::bad_alloc);
            EXPECT_EQ(cache.Stats().UsedBytes, 0U);
            EXPECT_EQ(cache.Stats().FooterEntries, 0U);
        }
        for (int i = 0; i < 8; ++i) {
            cache.PutFooter(longPrefix + "-f-" + std::to_string(i) + ".parquet", 256, 1, footer);
            EXPECT_LE(cache.Stats().UsedBytes, cache.Stats().MaxBytes);
        }
    }

    // 3. Find*, EvictRoot, Stats, and Clear perform ZERO cache allocations even with 600-char keys!
    {
        TSnapshotMetadataCache cache({}, TSnapshotMetadataCacheOptions{.MaxBytes = 64ULL << 10});
        const std::string rootKey = longPrefix + "-root";
        const std::string mlKey = longPrefix + "-ml";
        const std::string mKey = longPrefix + "-manifest";
        const std::string fKey = longPrefix + "-footer.parquet";
        const std::string missKey = longPrefix + "-missing";

        cache.PutRoot(rootKey, 128, r1);
        cache.PutManifestList(mlKey, emptyMl);
        cache.PutManifest(mKey, 64, emptyManifest);
        cache.PutFooter(fKey, 256, 1, footer);

        TScopedCacheAllocInjector failAll(/*failOnCall=*/0, /*failAll=*/true);
        EXPECT_EQ(cache.FindRoot(rootKey, 128), r1);
        EXPECT_EQ(cache.FindRoot(missKey, 128), nullptr);
        EXPECT_EQ(cache.FindManifestList(mlKey), emptyMl);
        EXPECT_EQ(cache.FindManifestList(missKey), nullptr);
        EXPECT_EQ(cache.FindManifest(mKey, 64), emptyManifest);
        EXPECT_EQ(cache.FindManifest(missKey, 64), nullptr);
        EXPECT_EQ(cache.FindFooter(fKey, 256, 1), footer);
        EXPECT_EQ(cache.FindFooter(missKey, 256, 1), nullptr);
        cache.EvictRoot(rootKey);
        EXPECT_EQ(cache.FindRoot(rootKey, 128), nullptr);
        EXPECT_GT(cache.Stats().UsedBytes, 0U);
        cache.Clear();
        EXPECT_EQ(cache.Stats().UsedBytes, 0U);
        EXPECT_EQ(TScopedCacheAllocInjector::AllocationCount(), 0U);
    }
}

} // namespace
} // namespace glamdb::detail

#endif
