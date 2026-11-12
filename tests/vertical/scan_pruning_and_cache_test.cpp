// Vertical storage verification for conservative SQL-3VL file & row-group pruning
// and bounded immutable metadata/manifest/footer caching.

#include "core/scan/metadata_cache.h"
#include "core/storage/format_verifier.h"
#include "storage/resources.h"
#include "tests/support/fault_fs.h"
#include "tests/vertical/vertical_support.h"

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <iceberg/arrow/arrow_io_util.h>
#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/manifest/manifest_list.h>
#include <iceberg/manifest/manifest_reader.h>
#include <iceberg/manifest/manifest_writer.h>
#include <iceberg/snapshot.h>
#include <iceberg/table_metadata.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>
#include <parquet/statistics.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <optional>
#include <set>
#include <string>
#include <variant>
#include <vector>

namespace glamdb {
namespace {

using namespace std::chrono_literals;
using namespace glamdb::vertical;
using NGlamDB::NTesting::EIOOp;
using NGlamDB::NTesting::TFaultFS;

const SchemaSpec PruningSchema{{
    {"key", FieldType::String, false},
    {"label", FieldType::String, true},
    {"value", FieldType::Int64, true},
    {"score", FieldType::Float64, true},
    {"flag", FieldType::Bool, true},
    {"at", FieldType::TimestampUsUtc, true},
    {"payload", FieldType::String, true},
}};

struct TPruningRow
{
    std::string Key;
    std::optional<std::string> Label{};
    std::optional<int64_t> Value{};
    std::optional<double> Score{};
    std::optional<bool> Flag{};
    std::optional<int64_t> At{};
    std::optional<std::string> Payload{};
};

std::shared_ptr<arrow::Schema> PruningArrowSchema()
{
    return arrow::schema({
        arrow::field("key", arrow::utf8(), false),
        arrow::field("label", arrow::utf8(), true),
        arrow::field("value", arrow::int64(), true),
        arrow::field("score", arrow::float64(), true),
        arrow::field("flag", arrow::boolean(), true),
        arrow::field("at", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"), true),
        arrow::field("payload", arrow::utf8(), true),
    });
}

std::shared_ptr<arrow::RecordBatch> MakePruningBatch(const std::vector<TPruningRow>& rows)
{
    std::vector<std::optional<std::string>> keys;
    std::vector<std::optional<std::string>> labels;
    std::vector<std::optional<int64_t>> values;
    std::vector<std::optional<double>> scores;
    std::vector<std::optional<bool>> flags;
    std::vector<std::optional<int64_t>> times;
    std::vector<std::optional<std::string>> payloads;
    keys.reserve(rows.size());
    labels.reserve(rows.size());
    values.reserve(rows.size());
    scores.reserve(rows.size());
    flags.reserve(rows.size());
    times.reserve(rows.size());
    payloads.reserve(rows.size());
    for (const auto& row : rows) {
        keys.emplace_back(row.Key);
        labels.push_back(row.Label);
        values.push_back(row.Value);
        scores.push_back(row.Score);
        flags.push_back(row.Flag);
        times.push_back(row.At);
        payloads.push_back(row.Payload);
    }
    const auto tsType = arrow::timestamp(arrow::TimeUnit::MICRO, "UTC");
    return arrow::RecordBatch::Make(PruningArrowSchema(),
        static_cast<int64_t>(rows.size()),
        {
            Column<arrow::StringBuilder>(keys, arrow::utf8()),
            Column<arrow::StringBuilder>(labels, arrow::utf8()),
            Column<arrow::Int64Builder>(values, arrow::int64()),
            Column<arrow::DoubleBuilder>(scores, arrow::float64()),
            Column<arrow::BooleanBuilder>(flags, arrow::boolean()),
            Column<arrow::TimestampBuilder>(times, tsType),
            Column<arrow::StringBuilder>(payloads, arrow::utf8()),
        });
}

std::vector<std::string> DrainRendered(ScanCursor& cursor)
{
    std::vector<std::string> out;
    while (true) {
        const auto batch = cursor.Next();
        EXPECT_TRUE(batch) << (batch ? std::string() : batch.error().code + ": " + batch.error().message);
        if (!batch || !*batch) {
            break;
        }
        const auto rendered = Render(**batch);
        out.insert(out.end(), rendered.begin(), rendered.end());
    }
    std::ranges::sort(out);
    return out;
}

class ScanPruningVerticalTest : public ::testing::Test
{
protected:
    test::TempDir Temporary_;
    std::filesystem::path Root_ = Temporary_.path / "store";
    NGlamDB::NIO::TPosixLocalFS Posix_;
    std::shared_ptr<TFaultFS> Faults_ = std::make_shared<TFaultFS>(Posix_);
    std::shared_ptr<detail::TLocalStorageBackend> StorageBackend_;
    std::shared_ptr<Engine> Engine_;

    void TearDown() override
    {
        if (Engine_) {
            [[maybe_unused]] const auto closed = Engine_->Shutdown(1000ms);
            Engine_.reset();
        }
        StorageBackend_.reset();
    }

    detail::BackendFactory MakeFactory(detail::TLocalStorageOptions options)
    {
        options.Fs = Faults_;
        return [this, options](const std::filesystem::path& path,
                   const StoreInfo& info) -> Result<std::shared_ptr<detail::StorageBackend>> {
            auto backend = detail::TLocalStorageBackend::Open(path, info, options);
            if (!backend) {
                return std::unexpected(backend.error());
            }
            StorageBackend_ = *backend;
            return std::shared_ptr<detail::StorageBackend>(std::move(*backend));
        };
    }

    void CreateEngine(detail::TLocalStorageOptions options = {})
    {
        auto created = detail::EngineFactory::CreateWith({test::store_id, Root_.string()}, MakeFactory(options));
        ASSERT_TRUE(created) << created.error().message;
        Engine_ = std::move(*created);
        ASSERT_EQ(Engine_->CreateCollection({test::collection_id, "events", PruningSchema}).disposition,
            CommitDisposition::Committed);
    }

    void ReopenEngine(detail::TLocalStorageOptions options = {})
    {
        if (Engine_) {
            ASSERT_TRUE(Engine_->Shutdown(1000ms));
            Engine_.reset();
        }
        StorageBackend_.reset();
        auto reopened = detail::EngineFactory::OpenWith({Root_.string(), test::store_id}, MakeFactory(options));
        ASSERT_TRUE(reopened) << reopened.error().message;
        Engine_ = std::move(*reopened);
    }

    size_t CountSuffixEventsSince(size_t startEvent,
        std::string_view suffix,
        std::optional<EIOOp> filterOp = std::nullopt) const
    {
        size_t count = 0;
        const auto events = Faults_->Events();
        for (size_t i = startEvent; i < events.size(); ++i) {
            if (events[i].Path.ends_with(suffix) && (!filterOp || events[i].Op == *filterOp)) {
                ++count;
            }
        }
        return count;
    }

    size_t CountParquetEventsSince(size_t startEvent, std::optional<EIOOp> filterOp = std::nullopt) const
    {
        size_t count = 0;
        const auto events = Faults_->Events();
        for (size_t i = startEvent; i < events.size(); ++i) {
            if (events[i].Path.ends_with(".parquet") && (!filterOp || events[i].Op == *filterOp)) {
                ++count;
            }
        }
        return count;
    }

    size_t DistinctParquetPathsOpenedSince(size_t startEvent) const
    {
        std::vector<std::string> paths;
        const auto events = Faults_->Events();
        for (size_t i = startEvent; i < events.size(); ++i) {
            if (events[i].Op == EIOOp::OpenRead && events[i].Path.ends_with(".parquet")) {
                paths.push_back(events[i].Path);
            }
        }
        std::ranges::sort(paths);
        paths.erase(std::unique(paths.begin(), paths.end()), paths.end());
        return paths.size();
    }

    uint64_t SumParquetPReadBytesSince(size_t startEvent) const
    {
        uint64_t total = 0;
        const auto events = Faults_->Events();
        for (size_t i = startEvent; i < events.size(); ++i) {
            if (events[i].Op == EIOOp::PRead && events[i].Path.ends_with(".parquet")) {
                total += events[i].Bytes;
            }
        }
        return total;
    }
};

TEST_F(ScanPruningVerticalTest, FilePruningSkipsParquetOpensCompletely)
{
    CreateEngine();

    // File 1: value in [1, 3], File 2: value in [100, 102], File 3: all-NULL nullable fields.
    ASSERT_EQ(Engine_
                  ->AppendDurable({test::collection_id,
                      FirstOperation,
                      {},
                      {MakePruningBatch({
                          {"a-1", "alpha-prefix-00001", 1, 1.0, true, 100, "p1"},
                          {"a-2", "alpha-prefix-00002", 3, 2.0, false, 200, "p2"},
                      })}})
                  .disposition,
        CommitDisposition::Committed);
    ASSERT_EQ(Engine_
                  ->AppendDurable({test::collection_id,
                      SecondOperation,
                      {},
                      {MakePruningBatch({
                          {"b-1", "beta-prefix-000001", 100, 10.0, true, 1000, "p3"},
                          {"b-2", "beta-prefix-000002", 102, 20.0, true, 2000, "p4"},
                      })}})
                  .disposition,
        CommitDisposition::Committed);
    ASSERT_EQ(
        Engine_
            ->AppendDurable({test::collection_id,
                ThirdOperation,
                {},
                {MakePruningBatch({
                    {"c-null", std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt},
                })}})
            .disposition,
        CommitDisposition::Committed);

    // 1. Predicate matching no file: zero `.parquet` OpenRead / PRead syscalls!
    {
        const size_t before = Faults_->Events().size();
        const auto cursor = Engine_->Scan({
            .collection_id = test::collection_id,
            .projection = std::vector<std::string>{"key"},
            .predicate = Predicate::Compare("value", CompareOp::Gt, int64_t{500}),
        });
        ASSERT_TRUE(cursor) << cursor.error().message;
        EXPECT_TRUE(DrainRendered(**cursor).empty());
        const auto profile = (*cursor)->Profile();
        EXPECT_EQ(profile.data_files_pruned.value, 3);
        EXPECT_EQ(profile.data_files_read.value, 0);
        EXPECT_EQ(profile.data_bytes_read.value, 0);
        EXPECT_EQ(profile.rows_decoded.value, 0);
        EXPECT_EQ(profile.rows_returned.value, 0);
        EXPECT_EQ(CountParquetEventsSince(before), 0U);
    }

    // 2. Predicate matching only File 2: exactly 1 `.parquet` OpenRead!
    {
        const size_t before = Faults_->Events().size();
        const auto cursor = Engine_->Scan({
            .collection_id = test::collection_id,
            .projection = std::vector<std::string>{"key"},
            .predicate = Predicate::Compare("value", CompareOp::Ge, int64_t{50}),
        });
        ASSERT_TRUE(cursor) << cursor.error().message;
        EXPECT_EQ(DrainRendered(**cursor), (std::vector<std::string>{"b-1", "b-2"}));
        const auto profile = (*cursor)->Profile();
        EXPECT_EQ(profile.data_files_pruned.value, 2);
        EXPECT_EQ(profile.data_files_read.value, 1);
        EXPECT_EQ(profile.rows_decoded.value, 2);
        EXPECT_EQ(profile.rows_returned.value, 2);
        EXPECT_EQ(DistinctParquetPathsOpenedSince(before), 1U);
    }

    // 3. `IS NULL` on `value` skips File 1 and File 2 (null_count == 0) and reads only File 3!
    {
        const size_t before = Faults_->Events().size();
        const auto cursor = Engine_->Scan({
            .collection_id = test::collection_id,
            .projection = std::vector<std::string>{"key"},
            .predicate = Predicate::IsNull("value"),
        });
        ASSERT_TRUE(cursor) << cursor.error().message;
        EXPECT_EQ(DrainRendered(**cursor), std::vector<std::string>{"c-null"});
        EXPECT_EQ((*cursor)->Profile().data_files_pruned.value, 2);
        EXPECT_EQ((*cursor)->Profile().data_files_read.value, 1);
        EXPECT_EQ(DistinctParquetPathsOpenedSince(before), 1U);
    }

    // 4. Zero-column projection (`projection = {}`) with predicate still prunes files & counts matching rows!
    {
        const size_t before = Faults_->Events().size();
        const auto cursor = Engine_->Scan({
            .collection_id = test::collection_id,
            .projection = std::vector<std::string>{},
            .predicate = Predicate::Compare("value", CompareOp::Ge, int64_t{50}),
        });
        ASSERT_TRUE(cursor) << cursor.error().message;
        int64_t rows = 0;
        while (true) {
            const auto batch = (*cursor)->Next();
            ASSERT_TRUE(batch) << batch.error().message;
            if (!*batch) {
                break;
            }
            EXPECT_EQ((*batch)->num_columns(), 0);
            rows += (*batch)->num_rows();
        }
        EXPECT_EQ(rows, 2);
        EXPECT_EQ((*cursor)->Profile().data_files_pruned.value, 2);
        EXPECT_EQ((*cursor)->Profile().data_files_read.value, 1);
        EXPECT_EQ(DistinctParquetPathsOpenedSince(before), 1U);
    }
}

TEST_F(ScanPruningVerticalTest, MultiRowGroupPruningDecodesOnlySurvivingRowGroups)
{
    detail::TLocalStorageOptions options;
    // Small row-group byte budget so 9 rows split across 3 row groups of 3 rows each.
    // Row bytes: 7 columns (3 strings: 1+4+len, int64: 9, float64: 9, bool: 2, ts: 9) + 12 offset bytes.
    std::vector<TPruningRow> rows;
    const std::string bigPayload(200, 'x');
    for (int i = 0; i < 9; ++i) {
        rows.push_back({
            .Key = "rg-k" + std::to_string(i),
            .Label = "lbl-" + std::to_string(i),
            .Value = int64_t{i * 10},
            .Score = static_cast<double>(i),
            .Flag = (i % 2 == 0),
            .At = int64_t{1000 + i},
            .Payload = bigPayload,
        });
    }
    // Single row size in CheckRow: (5+5) + (5+5) + 9 + 9 + 2 + 9 + (5+200) = 254 bytes; + 12 offset bytes.
    options.Writer.MaxRowGroupBytes = 12 + 3 * 254;
    CreateEngine(options);

    const auto appended = Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {MakePruningBatch(rows)}});
    ASSERT_EQ(appended.disposition, CommitDisposition::Committed)
        << (appended.error ? appended.error->message : std::string());

    const auto dataFiles = FilesIn(Root_ / "warehouse" / test::collection_id / "data");
    ASSERT_EQ(dataFiles.size(), 1U);
    const auto fileBytes = ReadBytes(dataFiles.front());
    auto reader =
        parquet::arrow::OpenFile(std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(fileBytes)),
            arrow::default_memory_pool());
    ASSERT_TRUE(reader.ok());
    ASSERT_EQ((*reader)->num_row_groups(), 3);

    // RG0: [0, 20], RG1: [30, 50], RG2: [60, 80].
    // Query `value == 40`: manifest [0, 80] keeps file, row-group pruning keeps ONLY RG1 (3 rows)!
    {
        const size_t before = Faults_->Events().size();
        const auto cursor = Engine_->Scan({
            .collection_id = test::collection_id,
            .projection = std::vector<std::string>{"key"},
            .predicate = Predicate::Compare("value", CompareOp::Eq, int64_t{40}),
        });
        ASSERT_TRUE(cursor) << cursor.error().message;
        EXPECT_EQ(DrainRendered(**cursor), std::vector<std::string>{"rg-k4"});
        const auto profile = (*cursor)->Profile();
        EXPECT_EQ(profile.data_files_read.value, 1);
        EXPECT_EQ(profile.row_groups_pruned.value, 2);
        EXPECT_EQ(profile.rows_decoded.value, 3);
        EXPECT_EQ(profile.rows_returned.value, 1);
        const auto preadBytes = SumParquetPReadBytesSince(before);
        EXPECT_GT(preadBytes, 0U);
        EXPECT_LT(preadBytes, fileBytes.size());
    }

    // Query `value == 25`: manifest [0, 80] keeps file, but ALL 3 row groups ([0,20], [30,50], [60,80]) are pruned!
    {
        const auto cursor = Engine_->Scan({
            .collection_id = test::collection_id,
            .projection = std::vector<std::string>{"key"},
            .predicate = Predicate::Compare("value", CompareOp::Eq, int64_t{25}),
        });
        ASSERT_TRUE(cursor) << cursor.error().message;
        EXPECT_TRUE(DrainRendered(**cursor).empty());
        const auto profile = (*cursor)->Profile();
        EXPECT_EQ(profile.data_files_read.value, 1);
        EXPECT_EQ(profile.row_groups_pruned.value, 3);
        EXPECT_EQ(profile.rows_decoded.value, 0);
        EXPECT_EQ(profile.rows_returned.value, 0);
    }
}

TEST_F(ScanPruningVerticalTest, PruningOnAndOffProduceIdenticalResultsAcross3VLAndTruncatedStrings)
{
    detail::TLocalStorageOptions options;
    options.Writer.MaxRowGroupBytes = 12 + 2 * 300;
    CreateEngine(options);

    // Long >16-codepoint UTF-8 strings on `label` exercise Iceberg `truncate(16)` manifest bounds.
    const std::string prefix16 = "shared_prefix_16"; // 16 ASCII chars
    ASSERT_EQ(Engine_
                  ->AppendDurable({test::collection_id,
                      FirstOperation,
                      {},
                      {MakePruningBatch({
                          {"k1", prefix16 + "_alpha", 10, 0.0, true, -62135596800000000LL, std::string(180, 'a')},
                          {"k2", prefix16 + "_beta", 20, -1.5, false, 0, std::string(180, 'b')},
                          {"k3", std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt},
                          {"k4", prefix16 + "_gamma", 30, 3.5, true, 253402300799999999LL, std::string(180, 'c')},
                      })}})
                  .disposition,
        CommitDisposition::Committed);

    ASSERT_EQ(Engine_
                  ->AppendDurable({test::collection_id,
                      SecondOperation,
                      {},
                      {MakePruningBatch({
                          {"k5",
                              "\xD0\xBF\xD1\x80\xD0\xB5\xD1\x84\xD0\xB8\xD0\xBA\xD1\x81_0123456789_end",
                              100,
                              10.0,
                              false,
                              500,
                              std::string(180, 'd')},
                          {"k6",
                              "\xD0\xBF\xD1\x80\xD0\xB5\xD1\x84\xD0\xB8\xD0\xBA\xD1\x81_9999999999_end",
                              100,
                              10.0,
                              false,
                              500,
                              std::string(180, 'e')},
                      })}})
                  .disposition,
        CommitDisposition::Committed);

    const std::vector<Predicate> queries{
        Predicate::True(),
        Predicate::Compare("label", CompareOp::Eq, prefix16 + "_beta"),
        Predicate::Compare("label", CompareOp::Ne, prefix16 + "_beta"),
        Predicate::Compare("label", CompareOp::Gt, prefix16 + "_beta"),
        Predicate::Compare("label", CompareOp::Lt, std::string("0000")),
        Predicate::Compare("score", CompareOp::Eq, -0.0),
        Predicate::Compare("at", CompareOp::Eq, TimestampUs{-62135596800000000LL}),
        Predicate::Compare("at", CompareOp::Eq, TimestampUs{253402300799999999LL}),
        Predicate::CompareNull("value", CompareOp::Eq),
        Predicate::Not(Predicate::In("value", {int64_t{10}, std::nullopt})),
        Predicate::In("value", {int64_t{20}, int64_t{100}, std::nullopt}),
        Predicate::Not(Predicate::In("value", {int64_t{10}, int64_t{20}})),
        Predicate::Or({
            Predicate::And({
                Predicate::Compare("value", CompareOp::Ge, int64_t{100}),
                Predicate::Compare("label",
                    CompareOp::Eq,
                    std::string("\xD0\xBF\xD1\x80\xD0\xB5\xD1\x84\xD0\xB8\xD0\xBA\xD1\x81_9999999999_end")),
            }),
            Predicate::IsNull("label"),
        }),
    };

    std::vector<std::vector<std::string>> prunedResults;
    for (const auto& q : queries) {
        const auto explain = Engine_->ExplainScan({.collection_id = test::collection_id, .predicate = q});
        ASSERT_TRUE(explain) << explain.error().message;
        EXPECT_TRUE(explain->file_pruning);
        EXPECT_TRUE(explain->row_group_pruning);

        const auto cursor = Engine_->Scan({
            .collection_id = test::collection_id,
            .projection = std::vector<std::string>{"key", "label", "value"},
            .predicate = q,
            .batch_rows = 2,
        });
        ASSERT_TRUE(cursor) << cursor.error().message;
        prunedResults.push_back(DrainRendered(**cursor));
    }

    // Reopen the exact same store with EnablePruning = false and verify 100% row parity!
    detail::TLocalStorageOptions offOptions = options;
    offOptions.EnablePruning = false;
    ReopenEngine(offOptions);
    EXPECT_FALSE(Engine_->GetCapabilities().file_pruning);
    EXPECT_FALSE(Engine_->GetCapabilities().row_group_pruning);

    for (size_t i = 0; i < queries.size(); ++i) {
        SCOPED_TRACE(i);
        const auto explain = Engine_->ExplainScan({.collection_id = test::collection_id, .predicate = queries[i]});
        ASSERT_TRUE(explain) << explain.error().message;
        EXPECT_FALSE(explain->file_pruning);
        EXPECT_FALSE(explain->row_group_pruning);

        const auto cursor = Engine_->Scan({
            .collection_id = test::collection_id,
            .projection = std::vector<std::string>{"key", "label", "value"},
            .predicate = queries[i],
            .batch_rows = 2,
        });
        ASSERT_TRUE(cursor) << cursor.error().message;
        EXPECT_EQ(DrainRendered(**cursor), prunedResults[i]);
    }
}

TEST_F(ScanPruningVerticalTest, SelectiveProjectedScanOverThousandRowAndTenRowAppends)
{
    CreateEngine();

    std::vector<TPruningRow> thousandRows;
    thousandRows.reserve(1000);
    const std::string heavyPayload(512, 'P');
    for (int i = 0; i < 1000; ++i) {
        thousandRows.push_back({
            .Key = "bulk-" + std::to_string(i),
            .Label = "bulk-label",
            .Value = int64_t{i},
            .Score = static_cast<double>(i) * 0.5,
            .Flag = (i % 2 == 0),
            .At = int64_t{10000 + i},
            .Payload = heavyPayload,
        });
    }
    std::vector<TPruningRow> tenRows;
    tenRows.reserve(10);
    for (int i = 0; i < 10; ++i) {
        tenRows.push_back({
            .Key = "tail-" + std::to_string(i),
            .Label = "tail-label",
            .Value = int64_t{10000 + i},
            .Score = static_cast<double>(i),
            .Flag = true,
            .At = int64_t{50000 + i},
            .Payload = heavyPayload,
        });
    }

    const auto r1 = Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {MakePruningBatch(thousandRows)}});
    ASSERT_EQ(r1.disposition, CommitDisposition::Committed);
    const auto r2 = Engine_->AppendDurable({test::collection_id, SecondOperation, {}, {MakePruningBatch(tenRows)}});
    ASSERT_EQ(r2.disposition, CommitDisposition::Committed);

    const int64_t totalCompressed = r1.result->compressed_data_bytes_added + r2.result->compressed_data_bytes_added;

    const auto cursor = Engine_->Scan({
        .collection_id = test::collection_id,
        .projection = std::vector<std::string>{"key"},
        .predicate = Predicate::Compare("value", CompareOp::Ge, int64_t{10005}),
        .limit = 3,
        .batch_rows = 2,
    });
    ASSERT_TRUE(cursor) << cursor.error().message;
    const auto rows = DrainRendered(**cursor);
    ASSERT_EQ(rows.size(), 3U);

    const auto profile = (*cursor)->Profile();
    // 1000-row file is pruned by manifest bounds ([0, 999] < 10005); only the 10-row file is opened!
    EXPECT_EQ(profile.data_files_read.value, 1);
    EXPECT_LE(profile.rows_decoded.value.value_or(1000), 10);
    EXPECT_EQ(profile.rows_returned.value, 3);
    EXPECT_LT(profile.data_bytes_read.value.value_or(totalCompressed), r2.result->compressed_data_bytes_added);
}

TEST_F(ScanPruningVerticalTest, RepeatedScansAndMetadataReuseCachedRootManifestsAndFooters)
{
    CreateEngine();
    ASSERT_NE(StorageBackend_, nullptr);
    const auto cache = StorageBackend_->ReadMetadataCache();
    ASSERT_NE(cache, nullptr);

    ASSERT_EQ(Engine_
                  ->AppendDurable({test::collection_id,
                      FirstOperation,
                      {},
                      {MakePruningBatch({
                          {"c1-a", "lbl-1", 10, 1.0, true, 100, std::string(200, 'x')},
                          {"c1-b", "lbl-2", 20, 2.0, false, 200, std::string(200, 'y')},
                      })}})
                  .disposition,
        CommitDisposition::Committed);
    ASSERT_EQ(Engine_
                  ->AppendDurable({test::collection_id,
                      SecondOperation,
                      {},
                      {MakePruningBatch({
                          {"c2-a", "lbl-3", 30, 3.0, true, 300, std::string(200, 'z')},
                      })}})
                  .disposition,
        CommitDisposition::Committed);

    // 1. Cold Scan populates 2 Parquet footers (using VerifiedScanState for zero-IO membership);
    //    cold ListFiles populates root, manifest-list, and 2 manifests in TSnapshotMetadataCache.
    int64_t coldDataBytes = 0;
    {
        const auto cold = Engine_->Scan({
            .collection_id = test::collection_id,
            .projection = std::vector<std::string>{"key"},
        });
        ASSERT_TRUE(cold) << cold.error().message;
        EXPECT_EQ(DrainRendered(**cold), (std::vector<std::string>{"c1-a", "c1-b", "c2-a"}));
        coldDataBytes = (*cold)->Profile().data_bytes_read.value.value_or(0);
        EXPECT_GT(coldDataBytes, 0);
        EXPECT_EQ(cache->Stats().FooterEntries, 2U);

        ASSERT_TRUE(Engine_->ListFiles(test::collection_id));
    }
    EXPECT_EQ(cache->Stats().RootEntries, 1U);
    EXPECT_EQ(cache->Stats().ManifestListEntries, 1U);
    EXPECT_EQ(cache->Stats().ManifestEntries, 2U);
    EXPECT_EQ(cache->Stats().FooterEntries, 2U);

    // 2. Warm scan + GetStatistics + ListFiles + ExplainScan perform ZERO root/Avro PReads
    //    and ZERO Parquet header/trailer/footer PReads (only 1 column-page PRead per file)!
    {
        const size_t before = Faults_->Events().size();
        const auto warm = Engine_->Scan({
            .collection_id = test::collection_id,
            .projection = std::vector<std::string>{"key"},
        });
        ASSERT_TRUE(warm) << warm.error().message;
        EXPECT_EQ(DrainRendered(**warm), (std::vector<std::string>{"c1-a", "c1-b", "c2-a"}));
        const int64_t warmDataBytes = (*warm)->Profile().data_bytes_read.value.value_or(0);
        EXPECT_GT(warmDataBytes, 0);
        EXPECT_LT(warmDataBytes, coldDataBytes);

        // Zero PReads on *.metadata.json and *.avro!
        EXPECT_EQ(CountSuffixEventsSince(before, ".metadata.json", EIOOp::PRead), 0U);
        EXPECT_EQ(CountSuffixEventsSince(before, ".avro", EIOOp::PRead), 0U);
        // Only 1 PRead per Parquet file (column data page only; 0 header/trailer/footer PReads)!
        EXPECT_EQ(CountParquetEventsSince(before, EIOOp::PRead), 2U);

        const size_t beforeMeta = Faults_->Events().size();
        ASSERT_TRUE(Engine_->GetStatistics(test::collection_id));
        ASSERT_TRUE(Engine_->ListFiles(test::collection_id));
        ASSERT_TRUE(Engine_->ExplainScan({.collection_id = test::collection_id}));
        EXPECT_EQ(CountSuffixEventsSince(beforeMeta, ".metadata.json", EIOOp::PRead), 0U);
        EXPECT_EQ(CountSuffixEventsSince(beforeMeta, ".avro", EIOOp::PRead), 0U);
    }

    // 3. New append retires superseded root from cache; next Scan reads 0 metadata & 1 new footer,
    //    and next ListFiles reads ONLY the 1 new root + 1 new manifest-list + 1 new manifest (reusing 2 cached
    //    manifests)!
    ASSERT_EQ(Engine_
                  ->AppendDurable({test::collection_id,
                      ThirdOperation,
                      {},
                      {MakePruningBatch({
                          {"c3-a", "lbl-4", 40, 4.0, false, 400, std::string(200, 'w')},
                      })}})
                  .disposition,
        CommitDisposition::Committed);
    EXPECT_EQ(cache->Stats().RootEntries, 0U);

    {
        const size_t beforeScan = Faults_->Events().size();
        const auto afterAppend = Engine_->Scan({
            .collection_id = test::collection_id,
            .projection = std::vector<std::string>{"key"},
        });
        ASSERT_TRUE(afterAppend) << afterAppend.error().message;
        EXPECT_EQ(DrainRendered(**afterAppend), (std::vector<std::string>{"c1-a", "c1-b", "c2-a", "c3-a"}));
        EXPECT_EQ(CountSuffixEventsSince(beforeScan, ".metadata.json", EIOOp::PRead), 0U);
        EXPECT_EQ(CountSuffixEventsSince(beforeScan, ".avro", EIOOp::OpenRead), 0U);
        EXPECT_EQ(cache->Stats().FooterEntries, 3U);

        const size_t beforeList = Faults_->Events().size();
        const auto files = Engine_->ListFiles(test::collection_id);
        ASSERT_TRUE(files) << files.error().message;
        EXPECT_EQ(files->Files.size(), 3U);
        // Only 1 new root PRead, and only 2 Avro files read (1 new manifest list + 1 new manifest; 2 old manifests hit
        // cache). ReadPersisted calls Size() + Open() = 2 OpenReads per uncached Avro file -> 4 OpenReads total (not
        // 8!).
        EXPECT_EQ(CountSuffixEventsSince(beforeList, ".metadata.json", EIOOp::PRead), 1U);
        EXPECT_EQ(CountSuffixEventsSince(beforeList, ".avro", EIOOp::OpenRead), 4U);
        EXPECT_EQ(cache->Stats().RootEntries, 1U);
        EXPECT_EQ(cache->Stats().ManifestEntries, 3U);
    }
}

TEST_F(ScanPruningVerticalTest, TransientReadErrorsNeverPoisonCacheAndReconcileOrZeroBudgetClearsIt)
{
    CreateEngine();
    ASSERT_EQ(Engine_
                  ->AppendDurable({test::collection_id,
                      FirstOperation,
                      {},
                      {MakePruningBatch({
                          {"k-ok", "lbl", 7, 1.5, true, 100, "p"},
                      })}})
                  .disposition,
        CommitDisposition::Committed);

    const auto cache = StorageBackend_->ReadMetadataCache();
    ASSERT_NE(cache, nullptr);
    EXPECT_EQ(cache->Stats().UsedBytes, 0U);

    // One-shot PRead failures on root, manifest-list, and manifest (via ListFiles) never poison TSnapshotMetadataCache.
    for (const char* pattern : {"*.metadata.json", "*snap-*.avro", "*-m*.avro"}) {
        SCOPED_TRACE(pattern);
        cache->Clear();
        Faults_->AddRule({EIOOp::PRead, pattern, 1, 1, std::make_error_code(std::errc::io_error), std::nullopt});

        const auto listing = Engine_->ListFiles(test::collection_id);
        ASSERT_FALSE(listing);
        EXPECT_EQ(listing.error().code, "IO_ERROR");
        EXPECT_TRUE(Engine_->GetHealth().read_available);

        // Retry immediately succeeds and warms root + manifest-list + manifest cleanly.
        const auto retry = Engine_->ListFiles(test::collection_id);
        ASSERT_TRUE(retry) << retry.error().message;
        EXPECT_EQ(retry->Files.size(), 1U);
        EXPECT_EQ(cache->Stats().RootEntries, 1U);
        EXPECT_EQ(cache->Stats().ManifestListEntries, 1U);
        EXPECT_EQ(cache->Stats().ManifestEntries, 1U);
    }

    // One-shot PRead failure on Parquet footer (via Scan) never poisons FooterEntries.
    {
        cache->Clear();
        Faults_->AddRule({EIOOp::PRead, "*.parquet", 1, 1, std::make_error_code(std::errc::io_error), std::nullopt});
        const auto cursor = Engine_->Scan({
            .collection_id = test::collection_id,
            .projection = std::vector<std::string>{"key"},
        });
        ASSERT_TRUE(cursor) << cursor.error().message;
        const auto batch = (*cursor)->Next();
        ASSERT_FALSE(batch);
        EXPECT_EQ(batch.error().code, "IO_ERROR");
        EXPECT_EQ(cache->Stats().FooterEntries, 0U);
        EXPECT_TRUE(Engine_->GetHealth().read_available);

        const auto retry = Engine_->Scan({
            .collection_id = test::collection_id,
            .projection = std::vector<std::string>{"key"},
        });
        ASSERT_TRUE(retry) << retry.error().message;
        EXPECT_EQ(DrainRendered(**retry), std::vector<std::string>{"k-ok"});
        EXPECT_EQ(cache->Stats().FooterEntries, 1U);
    }

    // Reconcile clears the cache while keeping its reservation valid for subsequent reads.
    ASSERT_TRUE(Engine_->Reconcile());
    EXPECT_EQ(cache->Stats().UsedBytes, 0U);
    EXPECT_EQ(cache->Stats().FooterEntries, 0U);

    // Reopen with ReadCacheBytes = 0 cleanly disables caching without breaking scans.
    detail::TLocalStorageOptions zeroCache;
    zeroCache.ReadCacheBytes = 0;
    ReopenEngine(zeroCache);
    EXPECT_EQ(StorageBackend_->ReadMetadataCache(), nullptr);
    const auto uncached = Engine_->Scan({
        .collection_id = test::collection_id,
        .projection = std::vector<std::string>{"key"},
    });
    ASSERT_TRUE(uncached) << uncached.error().message;
    EXPECT_EQ(DrainRendered(**uncached), std::vector<std::string>{"k-ok"});
}

TEST_F(ScanPruningVerticalTest, MultiRowGroupOver4KiBStringManifestPruningPreservesCommittedRows)
{
    // Default production writer settings (MaxRowGroupBytes = 8 MiB):
    // 10 rows with 910 KiB `label` filler split into RG0 (8 rows) and RG1 (2 rows).
    // RG0 contains a canonical 5000-byte string in `payload` (> Parquet's 4096-byte min/max cap),
    // so RG0 omits `payload` min/max while RG1 has `["m", "m"]`. Manifest bounds on `payload`
    // must be omitted rather than claiming `["m", "m"]` for the whole file!
    CreateEngine();

    const std::string filler910K(910 * 1024, 'L');
    const std::string over4K(5000, 'z');
    std::vector<TPruningRow> rows;
    rows.reserve(10);
    for (int i = 0; i < 10; ++i) {
        std::string payload = "m";
        if (i == 0) {
            payload = "a";
        } else if (i == 1) {
            payload = over4K;
        }
        rows.push_back({
            .Key = "mrg-" + std::to_string(i),
            .Label = filler910K,
            .Value = int64_t{i},
            .Score = static_cast<double>(i),
            .Flag = (i % 2 == 0),
            .At = int64_t{100 + i},
            .Payload = std::move(payload),
        });
    }

    const auto appended = Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {MakePruningBatch(rows)}});
    ASSERT_EQ(appended.disposition, CommitDisposition::Committed)
        << (appended.error ? appended.error->message : std::string());

    const auto listing = Engine_->ListFiles(test::collection_id);
    ASSERT_TRUE(listing) << listing.error().message;
    ASSERT_EQ(listing->Files.size(), 1U);
    const auto details = Engine_->GetFileDetails(test::collection_id, listing->Files.front().FileID);
    ASSERT_TRUE(details) << details.error().message;
    ASSERT_EQ(details->RowGroups.size(), 2U);

    // Manifest FieldMetrics for `payload` (FieldID = 7) must have NO LowerBound / UpperBound!
    bool checkedPayloadMetrics = false;
    for (const auto& fm : listing->Files.front().FieldMetrics) {
        if (fm.FieldID == 7) {
            checkedPayloadMetrics = true;
            EXPECT_FALSE(fm.LowerBound.has_value());
            EXPECT_FALSE(fm.UpperBound.has_value());
        }
    }
    EXPECT_TRUE(checkedPayloadMetrics);

    const std::vector<std::pair<Predicate, std::vector<std::string>>> queries{
        {Predicate::Compare("payload", CompareOp::Eq, std::string("a")), {"mrg-0"}},
        {Predicate::Compare("payload", CompareOp::Lt, std::string("m")), {"mrg-0"}},
        {Predicate::Compare("payload", CompareOp::Gt, std::string("n")), {"mrg-1"}},
        {Predicate::In("payload", {std::string("a"), over4K}), {"mrg-0", "mrg-1"}},
        {Predicate::Not(Predicate::And({
             Predicate::Compare("payload", CompareOp::Ge, std::string("b")),
             Predicate::Compare("payload", CompareOp::Le, std::string("y")),
         })),
            {"mrg-0", "mrg-1"}},
    };

    for (size_t i = 0; i < queries.size(); ++i) {
        SCOPED_TRACE(i);
        const auto cursor = Engine_->Scan({
            .collection_id = test::collection_id,
            .projection = std::vector<std::string>{"key"},
            .predicate = queries[i].first,
        });
        ASSERT_TRUE(cursor) << cursor.error().message;
        EXPECT_EQ(DrainRendered(**cursor), queries[i].second);
        const auto profile = (*cursor)->Profile();
        EXPECT_EQ(profile.data_files_pruned.value, 0);
        // RG1 has exact `["m", "m"]`, so every query above still prunes RG1 at the row-group level!
        EXPECT_EQ(profile.row_groups_pruned.value, 1);
    }

    detail::TLocalStorageOptions offOptions;
    offOptions.EnablePruning = false;
    ReopenEngine(offOptions);
    for (size_t i = 0; i < queries.size(); ++i) {
        SCOPED_TRACE(i);
        const auto cursor = Engine_->Scan({
            .collection_id = test::collection_id,
            .projection = std::vector<std::string>{"key"},
            .predicate = queries[i].first,
        });
        ASSERT_TRUE(cursor) << cursor.error().message;
        EXPECT_EQ(DrainRendered(**cursor), queries[i].second);
    }
}

TEST_F(ScanPruningVerticalTest, MalformedOnDiskParquetFooterStatsPruneNothingAndKeepStoreHealthy)
{
    CreateEngine();

    ASSERT_EQ(Engine_
                  ->AppendDurable({test::collection_id,
                      FirstOperation,
                      {},
                      {MakePruningBatch({
                          {"k0", "lbl-0", 10, 1.0, true, 100, "hello_min"},
                          {"k1", "lbl-1", 20, 2.0, false, 200, "world_max"},
                      })}})
                  .disposition,
        CommitDisposition::Committed);

    const auto listing = Engine_->ListFiles(test::collection_id);
    ASSERT_TRUE(listing) << listing.error().message;
    ASSERT_EQ(listing->Files.size(), 1U);
    const auto details = Engine_->GetFileDetails(test::collection_id, listing->Files.front().FileID);
    ASSERT_TRUE(details) << details.error().message;
    StorageBackend_->ReadMetadataCache()->Clear();

    const auto dataFiles = FilesIn(Root_ / "warehouse" / test::collection_id / "data");
    ASSERT_EQ(dataFiles.size(), 1U);
    std::string bytes = ReadBytes(dataFiles.front());
    const size_t footerStart = bytes.size() - static_cast<size_t>(details->FooterBytes) - 8U;
    const std::string needle = "hello_min";
    const std::string invertedMin = "zzzzz_min"; // same 9-byte length, > "world_max"!
    size_t pos = bytes.find(needle, footerStart);
    ASSERT_NE(pos, std::string::npos);
    while (pos != std::string::npos) {
        bytes.replace(pos, needle.size(), invertedMin);
        pos = bytes.find(needle, pos + invertedMin.size());
    }
    {
        std::ofstream out(dataFiles.front(), std::ios::binary | std::ios::trunc);
        ASSERT_TRUE(out.good());
        out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        ASSERT_TRUE(out.good());
    }

    // Both cold scan (footer verification + caching) and warm scan discard the contradictory
    // `min > max` column statistics (`prune nothing`), decode exact data, and keep readiness open!
    for (int pass = 0; pass < 2; ++pass) {
        SCOPED_TRACE(pass);
        const auto hitCursor = Engine_->Scan({
            .collection_id = test::collection_id,
            .projection = std::vector<std::string>{"key"},
            .predicate = Predicate::Compare("payload", CompareOp::Eq, std::string("hello_min")),
        });
        ASSERT_TRUE(hitCursor) << hitCursor.error().message;
        EXPECT_EQ(DrainRendered(**hitCursor), std::vector<std::string>{"k0"});
        EXPECT_EQ((*hitCursor)->Profile().row_groups_pruned.value, 0);

        const auto missCursor = Engine_->Scan({
            .collection_id = test::collection_id,
            .projection = std::vector<std::string>{"key"},
            .predicate = Predicate::Compare("payload", CompareOp::Eq, std::string("mid_value")),
        });
        ASSERT_TRUE(missCursor) << missCursor.error().message;
        EXPECT_TRUE(DrainRendered(**missCursor).empty());
        EXPECT_EQ((*missCursor)->Profile().row_groups_pruned.value, 0);

        const auto health = Engine_->GetHealth();
        EXPECT_TRUE(health.read_available);
        EXPECT_TRUE(health.write_ready);
    }

    // GetFileDetails and Reconcile must also keep the store healthy on contradictory bounds.
    const auto invertedDetails = Engine_->GetFileDetails(test::collection_id, listing->Files.front().FileID);
    ASSERT_TRUE(invertedDetails) << invertedDetails.error().message;
    EXPECT_TRUE(Engine_->GetHealth().read_available);
    EXPECT_TRUE(Engine_->Reconcile());
}

namespace {

enum class ETest3VL : uint8_t { False, Unknown, True };

ETest3VL Not3VL(ETest3VL v)
{
    if (v == ETest3VL::True) {
        return ETest3VL::False;
    }
    if (v == ETest3VL::False) {
        return ETest3VL::True;
    }
    return ETest3VL::Unknown;
}

ETest3VL And3VL(ETest3VL a, ETest3VL b)
{
    if (a == ETest3VL::False || b == ETest3VL::False) {
        return ETest3VL::False;
    }
    if (a == ETest3VL::True && b == ETest3VL::True) {
        return ETest3VL::True;
    }
    return ETest3VL::Unknown;
}

ETest3VL Or3VL(ETest3VL a, ETest3VL b)
{
    if (a == ETest3VL::True || b == ETest3VL::True) {
        return ETest3VL::True;
    }
    if (a == ETest3VL::False && b == ETest3VL::False) {
        return ETest3VL::False;
    }
    return ETest3VL::Unknown;
}

OptionalLiteral RowFieldValue(const TPruningRow& row, std::string_view field)
{
    if (field == "key") {
        return Literal{row.Key};
    }
    if (field == "label") {
        return row.Label ? OptionalLiteral{Literal{*row.Label}} : std::nullopt;
    }
    if (field == "value") {
        return row.Value ? OptionalLiteral{Literal{*row.Value}} : std::nullopt;
    }
    if (field == "score") {
        if (!row.Score) {
            return std::nullopt;
        }
        const double normalized = (*row.Score == 0.0) ? 0.0 : *row.Score;
        return OptionalLiteral{Literal{normalized}};
    }
    if (field == "flag") {
        return row.Flag ? OptionalLiteral{Literal{*row.Flag}} : std::nullopt;
    }
    if (field == "at") {
        return row.At ? OptionalLiteral{Literal{TimestampUs{*row.At}}} : std::nullopt;
    }
    if (field == "payload") {
        return row.Payload ? OptionalLiteral{Literal{*row.Payload}} : std::nullopt;
    }
    ADD_FAILURE() << "unknown field: " << field;
    return std::nullopt;
}

int CompareTestLiterals(const Literal& lhs, const Literal& rhs)
{
    return std::visit(
        [](const auto& a, const auto& b) -> int {
            using TA = std::decay_t<decltype(a)>;
            using TB = std::decay_t<decltype(b)>;
            if constexpr (!std::is_same_v<TA, TB>) {
                ADD_FAILURE() << "mismatched literal types";
                return 0;
            } else if constexpr (std::is_same_v<TA, TimestampUs>) {
                return a.value < b.value ? -1 : (a.value > b.value ? 1 : 0);
            } else if constexpr (std::is_same_v<TA, Decimal>) {
                if (a.coefficient_high != b.coefficient_high) {
                    return a.coefficient_high < b.coefficient_high ? -1 : 1;
                }
                return a.coefficient_low < b.coefficient_low ? -1 : (a.coefficient_low > b.coefficient_low ? 1 : 0);
            } else if constexpr (std::is_same_v<TA, Date>) {
                return a.days_since_epoch < b.days_since_epoch ? -1 : (a.days_since_epoch > b.days_since_epoch ? 1 : 0);
            } else if constexpr (std::is_same_v<TA, double>) {
                const double na = (a == 0.0) ? 0.0 : a;
                const double nb = (b == 0.0) ? 0.0 : b;
                return na < nb ? -1 : (na > nb ? 1 : 0);
            } else if constexpr (std::is_same_v<TA, bool>) {
                const int ia = a ? 1 : 0;
                const int ib = b ? 1 : 0;
                return ia < ib ? -1 : (ia > ib ? 1 : 0);
            } else {
                return a < b ? -1 : (a > b ? 1 : 0);
            }
        },
        lhs,
        rhs);
}

ETest3VL EvalTest3VL(const Predicate& pred, const TPruningRow& row)
{
    switch (pred.kind()) {
        case Predicate::Kind::True:
            return ETest3VL::True;
        case Predicate::Kind::IsNull:
            return RowFieldValue(row, pred.field()).has_value() ? ETest3VL::False : ETest3VL::True;
        case Predicate::Kind::IsNotNull:
            return RowFieldValue(row, pred.field()).has_value() ? ETest3VL::True : ETest3VL::False;
        case Predicate::Kind::Compare: {
            const auto cell = RowFieldValue(row, pred.field());
            const auto& rhs = pred.optional_value();
            if (!cell.has_value() || !rhs.has_value()) {
                return ETest3VL::Unknown;
            }
            const int cmp = CompareTestLiterals(*cell, *rhs);
            bool ok = false;
            switch (pred.op()) {
                case CompareOp::Eq:
                    ok = (cmp == 0);
                    break;
                case CompareOp::Ne:
                    ok = (cmp != 0);
                    break;
                case CompareOp::Lt:
                    ok = (cmp < 0);
                    break;
                case CompareOp::Le:
                    ok = (cmp <= 0);
                    break;
                case CompareOp::Gt:
                    ok = (cmp > 0);
                    break;
                case CompareOp::Ge:
                    ok = (cmp >= 0);
                    break;
            }
            return ok ? ETest3VL::True : ETest3VL::False;
        }
        case Predicate::Kind::In: {
            const auto cell = RowFieldValue(row, pred.field());
            const auto vals = pred.values();
            if (vals.empty()) {
                return cell.has_value() ? ETest3VL::False : ETest3VL::Unknown;
            }
            if (!cell.has_value()) {
                return ETest3VL::Unknown;
            }
            bool sawNull = false;
            for (const auto& item : vals) {
                if (!item.has_value()) {
                    sawNull = true;
                } else if (CompareTestLiterals(*cell, *item) == 0) {
                    return ETest3VL::True;
                }
            }
            return sawNull ? ETest3VL::Unknown : ETest3VL::False;
        }
        case Predicate::Kind::Not:
            return Not3VL(EvalTest3VL(pred.children()[0], row));
        case Predicate::Kind::And: {
            ETest3VL acc = ETest3VL::True;
            for (const auto& child : pred.children()) {
                acc = And3VL(acc, EvalTest3VL(child, row));
            }
            return acc;
        }
        case Predicate::Kind::Or: {
            ETest3VL acc = ETest3VL::False;
            for (const auto& child : pred.children()) {
                acc = Or3VL(acc, EvalTest3VL(child, row));
            }
            return acc;
        }
    }
    return ETest3VL::False;
}

} // namespace

TEST_F(ScanPruningVerticalTest, GenerativeThreeValuedPredicateOracleMatchesPrunedAndUnprunedScans)
{
    detail::TLocalStorageOptions options;
    // Split multi-row appends into ~2-row Parquet row groups so both manifest and row-group pruning fire.
    options.Writer.MaxRowGroupBytes = 12 + 2 * 360;
    CreateEngine(options);

    const std::string pfx16 = "prefix_16_chars!"; // 16 ASCII code points
    const std::string cyrA = "\xD0\xBF\xD1\x80\xD0\xB5\xD1\x84\xD0\xB8\xD0\xBA\xD1\x81_\xD0\xB0";
    const std::string cyrYa = "\xD0\xBF\xD1\x80\xD0\xB5\xD1\x84\xD0\xB8\xD0\xBA\xD1\x81_\xD1\x8F";
    const std::string emojiStart = "\xF0\x9F\x9A\x80_start";
    const std::string emojiEnd = "\xF0\x9F\x9A\x80_end";

    const std::vector<std::vector<TPruningRow>> fileBatches{
        // File 0 (4 rows -> 2 RGs): mixed values + NULLs + 16-codepoint prefixes.
        {
            {"f0-0", pfx16 + "_alpha", 10, 0.0, true, 100, std::string(200, 'a')},
            {"f0-1", std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt},
            {"f0-2", pfx16 + "_beta", 20, 2.5, false, 200, std::string(200, 'b')},
            {"f0-3", pfx16 + "_omega", 30, 5.0, true, 300, std::string(200, 'c')},
        },
        // File 1 (2 rows -> 1 RG): all nullable columns NULL.
        {
            {"f1-0", std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt},
            {"f1-1", std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt},
        },
        // File 2 (3 rows -> 2 RGs): constant values per column.
        {
            {"f2-0", "const_16_chars!!", 42, -3.5, true, 500, std::string(200, 'k')},
            {"f2-1", "const_16_chars!!", 42, -3.5, true, 500, std::string(200, 'k')},
            {"f2-2", "const_16_chars!!", 42, -3.5, true, 500, std::string(200, 'k')},
        },
        // File 3 (4 rows -> 2 RGs): Cyrillic, 4-byte UTF-8 emoji, min/max TimestampUsUtc.
        {
            {"f3-0", cyrA, 100, 10.0, false, -62135596800000000LL, std::string(180, 'm')},
            {"f3-1", cyrYa, 110, 12.5, true, 0, std::string(180, 'n')},
            {"f3-2", emojiStart, 120, 15.0, false, 1000, std::string(180, 'o')},
            {"f3-3", emojiEnd, 130, 17.5, true, 253402300799999999LL, std::string(180, 'p')},
        },
        // File 4 (2 rows -> 1 RG): high positive domain.
        {
            {"f4-0", "zzz_high_0", 1000, 100.5, false, 9000, std::string(200, 'x')},
            {"f4-1", "zzz_high_1", 2000, 200.5, false, 9500, std::string(200, 'y')},
        },
        // File 5 (3 rows -> 2 RGs): negative domain + NULL mix.
        {
            {"f5-0", "000_neg_0", -500, -99.5, true, -5000, std::string(200, 'u')},
            {"f5-1", "000_neg_1", -100, -10.25, false, -1000, std::string(200, 'v')},
            {"f5-2", "000_neg_null", std::nullopt, -10.25, std::nullopt, -500, std::string(200, 'w')},
        },
    };

    std::vector<TPruningRow> allRows;
    std::vector<TPruningRow> histRows;
    int64_t histSnapshotId = 0;
    for (size_t f = 0; f < fileBatches.size(); ++f) {
        char opId[37];
        std::snprintf(opId, sizeof(opId), "00000000-0000-4000-8000-000000000b%02zu", f + 1);
        const auto res = Engine_->AppendDurable({test::collection_id, opId, {}, {MakePruningBatch(fileBatches[f])}});
        ASSERT_EQ(res.disposition, CommitDisposition::Committed) << (res.error ? res.error->message : std::string());
        allRows.insert(allRows.end(), fileBatches[f].begin(), fileBatches[f].end());
        if (f < 3) {
            histRows.insert(histRows.end(), fileBatches[f].begin(), fileBatches[f].end());
            histSnapshotId = res.result->snapshot_id;
        }
    }

    const std::vector<Predicate> leaves{
        Predicate::True(),
        Predicate::Compare("key", CompareOp::Eq, std::string("f0-0")),
        Predicate::Compare("key", CompareOp::Ne, std::string("f2-1")),
        Predicate::Compare("key", CompareOp::Lt, std::string("f1-0")),
        Predicate::Compare("key", CompareOp::Le, std::string("f1-0")),
        Predicate::Compare("key", CompareOp::Gt, std::string("f4-1")),
        Predicate::Compare("key", CompareOp::Ge, std::string("f4-0")),
        Predicate::Compare("label", CompareOp::Eq, pfx16 + "_beta"),
        Predicate::Compare("label", CompareOp::Ne, pfx16 + "_beta"),
        Predicate::Compare("label", CompareOp::Gt, pfx16 + "_beta"),
        Predicate::Compare("label", CompareOp::Lt, pfx16 + "_alpha"),
        Predicate::Compare("label", CompareOp::Eq, std::string("const_16_chars!!")),
        Predicate::Compare("label", CompareOp::Ne, std::string("const_16_chars!!")),
        Predicate::Compare("label", CompareOp::Eq, cyrYa),
        Predicate::Compare("label", CompareOp::Ge, emojiStart),
        Predicate::Compare("label", CompareOp::Lt, std::string("000")),
        Predicate::Compare("value", CompareOp::Eq, int64_t{42}),
        Predicate::Compare("value", CompareOp::Ne, int64_t{42}),
        Predicate::Compare("value", CompareOp::Lt, int64_t{-500}),
        Predicate::Compare("value", CompareOp::Le, int64_t{-100}),
        Predicate::Compare("value", CompareOp::Gt, int64_t{1500}),
        Predicate::Compare("value", CompareOp::Ge, int64_t{1000}),
        Predicate::Compare("value", CompareOp::Eq, int64_t{25}),
        Predicate::Compare("score", CompareOp::Eq, -0.0),
        Predicate::Compare("score", CompareOp::Ne, -3.5),
        Predicate::Compare("score", CompareOp::Lt, -50.0),
        Predicate::Compare("score", CompareOp::Le, 0.0),
        Predicate::Compare("score", CompareOp::Gt, 150.0),
        Predicate::Compare("score", CompareOp::Ge, 100.5),
        Predicate::Compare("flag", CompareOp::Eq, true),
        Predicate::Compare("flag", CompareOp::Ne, true),
        Predicate::Compare("flag", CompareOp::Gt, false),
        Predicate::Compare("at", CompareOp::Eq, TimestampUs{-62135596800000000LL}),
        Predicate::Compare("at", CompareOp::Eq, TimestampUs{253402300799999999LL}),
        Predicate::Compare("at", CompareOp::Lt, TimestampUs{0}),
        Predicate::Compare("at", CompareOp::Gt, TimestampUs{9000}),
        Predicate::CompareNull("value", CompareOp::Eq),
        Predicate::CompareNull("key", CompareOp::Ne),
        Predicate::CompareNull("label", CompareOp::Lt),
        Predicate::IsNull("key"),
        Predicate::IsNotNull("key"),
        Predicate::IsNull("value"),
        Predicate::IsNotNull("value"),
        Predicate::IsNull("label"),
        Predicate::IsNotNull("flag"),
        Predicate::In("value", {}),
        Predicate::In("value", {std::nullopt}),
        Predicate::In("value", {int64_t{42}}),
        Predicate::In("value", {int64_t{42}, std::nullopt}),
        Predicate::In("value", {int64_t{9999}, std::nullopt}),
        Predicate::In("value", {int64_t{-500}, int64_t{20}, int64_t{2000}}),
        Predicate::In("label", {cyrA, emojiEnd}),
        Predicate::In("label", {pfx16 + "_omega", std::nullopt}),
        Predicate::In("flag", {false, true}),
        Predicate::In("flag", {true, std::nullopt}),
        Predicate::In("score", {-0.0, -3.5, 200.5}),
        Predicate::In("at", {TimestampUs{-62135596800000000LL}, TimestampUs{500}}),
    };

    struct TGeneratedQuery
    {
        Predicate Pred;
        bool IsRootNot = false;
    };
    std::vector<TGeneratedQuery> generated;
    generated.reserve(700);
    for (const auto& leaf : leaves) {
        generated.push_back({leaf, false});
        generated.push_back({Predicate::Not(leaf), true});
    }
    // Depth-2 And / Or combinations:
    for (size_t i = 0; i < leaves.size(); ++i) {
        const auto& a = leaves[i];
        const auto& b = leaves[(i * 7 + 3) % leaves.size()];
        const auto& c = leaves[(i * 13 + 11) % leaves.size()];
        generated.push_back({Predicate::And({a, b}), false});
        generated.push_back({Predicate::Or({a, b}), false});
        generated.push_back({Predicate::And({a, Predicate::Not(c)}), false});
        generated.push_back({Predicate::Or({Predicate::Not(a), c}), false});
        // Depth-3 combinations (`NOT(AND(...))`, `NOT(OR(...))`, `AND(OR(...), NOT(...))`, `OR(AND(...), ...)`):
        generated.push_back({Predicate::Not(Predicate::And({a, b})), true});
        generated.push_back({Predicate::Not(Predicate::Or({a, c})), true});
        generated.push_back({Predicate::And({Predicate::Or({a, b}), Predicate::Not(c)}), false});
        generated.push_back({Predicate::Or({Predicate::And({a, b}), c}), false});
        generated.push_back({Predicate::Not(Predicate::Or({Predicate::And({a, b}), Predicate::Not(c)})), true});
    }
    ASSERT_GE(generated.size(), 600U);

    const auto evalOracleKeys = [](const std::vector<TPruningRow>& dataset, const Predicate& pred) {
        std::vector<std::string> expected;
        for (const auto& row : dataset) {
            if (EvalTest3VL(pred, row) == ETest3VL::True) {
                expected.push_back(row.Key);
            }
        }
        std::ranges::sort(expected);
        return expected;
    };

    int64_t totalFilesPruned = 0;
    int64_t notFilesPruned = 0;
    int64_t totalRowGroupsPruned = 0;
    std::vector<std::vector<std::string>> prunedLatestResults;
    prunedLatestResults.reserve(generated.size());
    std::vector<std::vector<std::string>> prunedLimitedResults;
    std::vector<std::vector<std::string>> prunedHistResults;

    for (size_t i = 0; i < generated.size(); ++i) {
        SCOPED_TRACE(i);
        const auto expected = evalOracleKeys(allRows, generated[i].Pred);
        const auto cursor = Engine_->Scan({
            .collection_id = test::collection_id,
            .projection = std::vector<std::string>{"key"},
            .predicate = generated[i].Pred,
            .batch_rows = 3,
        });
        ASSERT_TRUE(cursor) << cursor.error().message;
        auto actual = DrainRendered(**cursor);
        EXPECT_EQ(actual, expected);
        const auto profile = (*cursor)->Profile();
        totalFilesPruned += profile.data_files_pruned.value.value_or(0);
        totalRowGroupsPruned += profile.row_groups_pruned.value.value_or(0);
        if (generated[i].IsRootNot) {
            notFilesPruned += profile.data_files_pruned.value.value_or(0);
        }
        prunedLatestResults.push_back(std::move(actual));

        if (i % 6 == 0) {
            const auto limCursor = Engine_->Scan({
                .collection_id = test::collection_id,
                .projection = std::vector<std::string>{"key"},
                .predicate = generated[i].Pred,
                .limit = 5,
                .batch_rows = 3,
            });
            ASSERT_TRUE(limCursor) << limCursor.error().message;
            auto limActual = DrainRendered(**limCursor);
            EXPECT_EQ(limActual.size(), std::min<size_t>(5U, expected.size()));
            const std::set<std::string> expectedSet(expected.begin(), expected.end());
            for (const auto& k : limActual) {
                EXPECT_TRUE(expectedSet.contains(k));
            }
            prunedLimitedResults.push_back(std::move(limActual));

            const auto histExpected = evalOracleKeys(histRows, generated[i].Pred);
            const auto histCursor = Engine_->Scan({
                .collection_id = test::collection_id,
                .selector = SnapshotSelector::Id(histSnapshotId),
                .projection = std::vector<std::string>{"key"},
                .predicate = generated[i].Pred,
                .batch_rows = 3,
            });
            ASSERT_TRUE(histCursor) << histCursor.error().message;
            auto histActual = DrainRendered(**histCursor);
            EXPECT_EQ(histActual, histExpected);
            prunedHistResults.push_back(std::move(histActual));
        }
    }

    EXPECT_GT(totalFilesPruned, 0);
    EXPECT_GT(notFilesPruned, 0);
    EXPECT_GT(totalRowGroupsPruned, 0);

    // Reopen with EnablePruning = false and verify exact parity across Latest, limit = 5, and historical snapshot!
    detail::TLocalStorageOptions offOptions = options;
    offOptions.EnablePruning = false;
    ReopenEngine(offOptions);

    size_t subIdx = 0;
    for (size_t i = 0; i < generated.size(); ++i) {
        SCOPED_TRACE(i);
        const auto cursor = Engine_->Scan({
            .collection_id = test::collection_id,
            .projection = std::vector<std::string>{"key"},
            .predicate = generated[i].Pred,
            .batch_rows = 3,
        });
        ASSERT_TRUE(cursor) << cursor.error().message;
        EXPECT_EQ(DrainRendered(**cursor), prunedLatestResults[i]);

        if (i % 6 == 0) {
            const auto limCursor = Engine_->Scan({
                .collection_id = test::collection_id,
                .projection = std::vector<std::string>{"key"},
                .predicate = generated[i].Pred,
                .limit = 5,
                .batch_rows = 3,
            });
            ASSERT_TRUE(limCursor) << limCursor.error().message;
            EXPECT_EQ(DrainRendered(**limCursor), prunedLimitedResults[subIdx]);

            const auto histCursor = Engine_->Scan({
                .collection_id = test::collection_id,
                .selector = SnapshotSelector::Id(histSnapshotId),
                .projection = std::vector<std::string>{"key"},
                .predicate = generated[i].Pred,
                .batch_rows = 3,
            });
            ASSERT_TRUE(histCursor) << histCursor.error().message;
            EXPECT_EQ(DrainRendered(**histCursor), prunedHistResults[subIdx]);
            ++subIdx;
        }
    }
}

TEST_F(ScanPruningVerticalTest, ByteExactParquetPReadAccountingAndColumnChunkEnomemClassification)
{
    CreateEngine();
    ASSERT_NE(StorageBackend_, nullptr);
    const auto cache = StorageBackend_->ReadMetadataCache();
    ASSERT_NE(cache, nullptr);

    ASSERT_EQ(Engine_
                  ->AppendDurable({test::collection_id,
                      FirstOperation,
                      {},
                      {MakePruningBatch({
                          {"io-0", "lbl-0", 1, 1.0, true, 100, std::string(2048, 'A')},
                          {"io-1", "lbl-1", 2, 2.0, false, 200, std::string(2048, 'B')},
                          {"io-2", "lbl-2", 3, 3.0, true, 300, std::string(2048, 'C')},
                          {"io-3", "lbl-3", 4, 4.0, false, 400, std::string(2048, 'D')},
                      })}})
                  .disposition,
        CommitDisposition::Committed);

    const auto listing = Engine_->ListFiles(test::collection_id);
    ASSERT_TRUE(listing) << listing.error().message;
    ASSERT_EQ(listing->Files.size(), 1U);
    const auto details = Engine_->GetFileDetails(test::collection_id, listing->Files.front().FileID);
    ASSERT_TRUE(details) << details.error().message;
    ASSERT_EQ(details->RowGroups.size(), 1U);
    ASSERT_EQ(details->RowGroups[0].Columns.size(), 7U);

    const int64_t footerBytes = details->FooterBytes;
    const int64_t keyCompressedBytes = details->RowGroups[0].Columns[0].CompressedBytes;
    ASSERT_GT(footerBytes, 0);
    ASSERT_GT(keyCompressedBytes, 0);

    // 1. Cold scan (cache cleared): reads 4B header + 8B tail + footerBytes + keyCompressedBytes.
    cache->Clear();
    {
        const size_t beforeCold = Faults_->Events().size();
        const auto cold = Engine_->Scan({
            .collection_id = test::collection_id,
            .projection = std::vector<std::string>{"key"},
        });
        ASSERT_TRUE(cold) << cold.error().message;
        EXPECT_EQ(DrainRendered(**cold), (std::vector<std::string>{"io-0", "io-1", "io-2", "io-3"}));
        const auto profile = (*cold)->Profile();
        ASSERT_TRUE(profile.data_bytes_read.value.has_value());
        EXPECT_EQ(*profile.data_bytes_read.value, static_cast<int64_t>(SumParquetPReadBytesSince(beforeCold)));
        EXPECT_EQ(*profile.data_bytes_read.value, 12 + footerBytes + keyCompressedBytes);
        // 3 footer-related PReads (header 4B, tail 8B, footer) + 1 column-chunk PRead = 4 PReads.
        EXPECT_EQ(CountParquetEventsSince(beforeCold, EIOOp::PRead), 4U);
    }

    // 2. Warm scan (footer cached): reads ONLY keyCompressedBytes in 1 single PRead (0 footer PReads).
    {
        const size_t beforeWarm = Faults_->Events().size();
        const auto warm = Engine_->Scan({
            .collection_id = test::collection_id,
            .projection = std::vector<std::string>{"key"},
        });
        ASSERT_TRUE(warm) << warm.error().message;
        EXPECT_EQ(DrainRendered(**warm), (std::vector<std::string>{"io-0", "io-1", "io-2", "io-3"}));
        const auto profile = (*warm)->Profile();
        ASSERT_TRUE(profile.data_bytes_read.value.has_value());
        EXPECT_EQ(*profile.data_bytes_read.value, static_cast<int64_t>(SumParquetPReadBytesSince(beforeWarm)));
        EXPECT_EQ(*profile.data_bytes_read.value, keyCompressedBytes);
        EXPECT_EQ(CountParquetEventsSince(beforeWarm, EIOOp::PRead), 1U);
    }

    // 3. Inject ENOMEM (`std::errc::not_enough_memory`) on the column-chunk PRead (1st PRead when footer is cached):
    //    must classify as `RESOURCE_EXHAUSTED` and keep `read_available` and `write_ready` healthy!
    {
        Faults_->AddRule(
            {EIOOp::PRead, "*.parquet", 1, 1, std::make_error_code(std::errc::not_enough_memory), std::nullopt});
        const auto cursor = Engine_->Scan({
            .collection_id = test::collection_id,
            .projection = std::vector<std::string>{"key"},
        });
        ASSERT_TRUE(cursor) << cursor.error().message;
        const auto batch = (*cursor)->Next();
        ASSERT_FALSE(batch);
        EXPECT_EQ(batch.error().code, "RESOURCE_EXHAUSTED") << batch.error().message;
        EXPECT_EQ((*cursor)->Profile().state, ScanState::Failed);

        const auto health = Engine_->GetHealth();
        EXPECT_TRUE(health.read_available);
        EXPECT_TRUE(health.write_ready);

        // Subsequent scan immediately succeeds.
        const auto retry = Engine_->Scan({
            .collection_id = test::collection_id,
            .projection = std::vector<std::string>{"key"},
        });
        ASSERT_TRUE(retry) << retry.error().message;
        EXPECT_EQ(DrainRendered(**retry), (std::vector<std::string>{"io-0", "io-1", "io-2", "io-3"}));
    }
}

TEST_F(ScanPruningVerticalTest, StructuralFooterCorruptionInUnreadColumnFailsScanAvoidsCacheAndBlocksReconcile)
{
    CreateEngine();
    const auto cache = StorageBackend_->ReadMetadataCache();
    ASSERT_NE(cache, nullptr);

    ASSERT_EQ(Engine_
                  ->AppendDurable({test::collection_id,
                      FirstOperation,
                      {},
                      {MakePruningBatch({
                          {"k0", "lbl-0", 10, 1.0, true, 100, std::nullopt},
                          {"k1", std::nullopt, 20, 2.0, false, 200, "p1"},
                          {"k2", "lbl-2", 30, 3.0, true, 300, "p2"},
                      })}})
                  .disposition,
        CommitDisposition::Committed);

    const auto listing = Engine_->ListFiles(test::collection_id);
    ASSERT_TRUE(listing) << listing.error().message;
    ASSERT_EQ(listing->Files.size(), 1U);
    const auto fileId = listing->Files.front().FileID;
    const auto details = Engine_->GetFileDetails(test::collection_id, fileId);
    ASSERT_TRUE(details) << details.error().message;

    const auto path = FilesIn(Root_ / "warehouse" / test::collection_id / "data").front();
    const std::string original = ReadBytes(path);
    const size_t footerStart = original.size() - static_cast<size_t>(details->FooterBytes) - 8U;

    // Locate ColumnMetaData for column 1 ("label"): path_in_schema (`\x19\x18\x05label`),
    // followed by codec (`\x15` + varint) and num_values (`\x16\x06` for zigzag(3) == 6).
    const size_t labelPath = original.find(std::string("\x19\x18\x05") + "label", footerStart);
    ASSERT_NE(labelPath, std::string::npos);
    const size_t numValuesField = original.find(std::string("\x16\x06"), labelPath + 8);
    ASSERT_NE(numValuesField, std::string::npos);

    // Case 1: Structural corruption (`num_values` 3 -> 4) in unread column 1 ("label").
    {
        std::string damaged = original;
        damaged[numValuesField + 1] = '\x08'; // zigzag(4) == 8
        WriteBytes(path, damaged);

        const auto parsed = parquet::ParquetFileReader::OpenFile(path.string())->metadata();
        EXPECT_EQ(parsed->RowGroup(0)->ColumnChunk(0)->num_values(), 3);
        EXPECT_EQ(parsed->RowGroup(0)->ColumnChunk(1)->num_values(), 4);

        cache->Clear();
        const auto cursor = Engine_->Scan({
            .collection_id = test::collection_id,
            .projection = std::vector<std::string>{"key"},
        });
        ASSERT_TRUE(cursor) << cursor.error().message;
        const auto batch = (*cursor)->Next();
        ASSERT_FALSE(batch);
        EXPECT_EQ(batch.error().code, "CORRUPTION") << batch.error().message;
        EXPECT_EQ(cache->Stats().FooterEntries, 0U);
        EXPECT_FALSE(Engine_->GetHealth().read_available);
        EXPECT_FALSE(Engine_->GetHealth().write_ready);

        // Reconcile without repairing the file must also reject the broken footer (no livelock).
        const auto reconciled = Engine_->Reconcile();
        ASSERT_FALSE(reconciled);
        EXPECT_EQ(reconciled.error().code, "CORRUPTION") << reconciled.error().message;
        EXPECT_FALSE(Engine_->GetHealth().read_available);

        WriteBytes(path, original);
        ASSERT_TRUE(Engine_->Reconcile());
        EXPECT_TRUE(Engine_->GetHealth().read_available);
    }

    // Case 2: Structural corruption (`data_page_offset` / `dictionary_page_offset` < 4) in unread column 1 ("label").
    {
        const auto origMeta = parquet::ParquetFileReader::OpenFile(path.string())->metadata();
        const auto chunk1 = origMeta->RowGroup(0)->ColumnChunk(1);
        const int64_t targetOffset =
            chunk1->has_dictionary_page() ? chunk1->dictionary_page_offset() : chunk1->data_page_offset();
        ASSERT_GE(targetOffset, 4);
        // Find the 1- or 2-byte zigzag varint of targetOffset after total_compressed_size in column 1.
        const uint64_t zz = static_cast<uint64_t>(targetOffset) << 1U;
        std::string encodedOffset;
        uint64_t rem = zz;
        while (rem >= 0x80U) {
            encodedOffset.push_back(static_cast<char>((rem & 0x7fU) | 0x80U));
            rem >>= 7U;
        }
        encodedOffset.push_back(static_cast<char>(rem));
        // Replace with same-byte-width varint encoding zigzag(1) == 2 (padded with 0x80 continuation if 2 bytes).
        std::string invalidOffset;
        if (encodedOffset.size() == 1U) {
            invalidOffset.push_back('\x04'); // zigzag(2) -> offset 2 < 4
        } else {
            ASSERT_EQ(encodedOffset.size(), 2U);
            invalidOffset.push_back('\x84'); // low 7 bits = 4, continuation = 1
            invalidOffset.push_back('\x00'); // high bits = 0 -> zigzag(2) -> offset 2 < 4
        }
        // Both field 9 (data_page_offset after field 7) and field 11 (dictionary_page_offset after field 9)
        // have Compact Thrift field-id delta 2 (`\x26`).
        const size_t offsetField = original.find(std::string(1, '\x26') + encodedOffset, numValuesField + 2);
        ASSERT_NE(offsetField, std::string::npos);

        std::string damaged = original;
        damaged.replace(offsetField + 1, invalidOffset.size(), invalidOffset);
        WriteBytes(path, damaged);

        const auto parsed = parquet::ParquetFileReader::OpenFile(path.string())->metadata();
        const auto alteredChunk1 = parsed->RowGroup(0)->ColumnChunk(1);
        const int64_t alteredOffset = alteredChunk1->has_dictionary_page() ? alteredChunk1->dictionary_page_offset()
                                                                           : alteredChunk1->data_page_offset();
        EXPECT_EQ(alteredOffset, 2);

        cache->Clear();
        const auto cursor = Engine_->Scan({
            .collection_id = test::collection_id,
            .projection = std::vector<std::string>{"key"},
        });
        ASSERT_TRUE(cursor) << cursor.error().message;
        const auto batch = (*cursor)->Next();
        ASSERT_FALSE(batch);
        EXPECT_EQ(batch.error().code, "CORRUPTION") << batch.error().message;
        EXPECT_EQ(cache->Stats().FooterEntries, 0U);
        EXPECT_FALSE(Engine_->GetHealth().read_available);

        const auto reconciled = Engine_->Reconcile();
        ASSERT_FALSE(reconciled);
        EXPECT_EQ(reconciled.error().code, "CORRUPTION") << reconciled.error().message;
        EXPECT_FALSE(Engine_->GetHealth().read_available);

        WriteBytes(path, original);
        ASSERT_TRUE(Engine_->Reconcile());
    }

    // Case 3: Malformed `null_count > rows` in Parquet footer statistics of column 1 ("label")
    // must NOT close readiness in either `Scan` or `GetFileDetails`; `GetFileDetails` omits `NullCount`.
    {
        // Column 1 ("label") has 1 NULL across 3 rows -> in Compact Thrift Statistics,
        // field 3 `null_count` (`\x36\x02`, zigzag(1) == 2) precedes field 5 `max_value` (`\x28\x05lbl-2`).
        const size_t nullCountPos = original.find(std::string("\x36\x02\x28\x05") + "lbl-2", labelPath + 8);
        ASSERT_NE(nullCountPos, std::string::npos);

        std::string damagedStats = original;
        damagedStats[nullCountPos + 1] = '\x0a'; // zigzag(5) -> null_count = 5 > rows (3)!
        WriteBytes(path, damagedStats);

        const auto parsed = parquet::ParquetFileReader::OpenFile(path.string())->metadata();
        const auto stats = parsed->RowGroup(0)->ColumnChunk(1)->encoded_statistics();
        ASSERT_NE(stats, nullptr);
        ASSERT_TRUE(stats->has_null_count);
        EXPECT_EQ(stats->null_count, 5);

        cache->Clear();
        const auto cursor = Engine_->Scan({
            .collection_id = test::collection_id,
            .projection = std::vector<std::string>{"key"},
            .predicate = Predicate::IsNull("label"),
        });
        ASSERT_TRUE(cursor) << cursor.error().message;
        EXPECT_EQ(DrainRendered(**cursor), std::vector<std::string>{"k1"});
        EXPECT_TRUE(Engine_->GetHealth().read_available);

        const auto statsDetails = Engine_->GetFileDetails(test::collection_id, fileId);
        ASSERT_TRUE(statsDetails) << statsDetails.error().message;
        ASSERT_EQ(statsDetails->RowGroups.size(), 1U);
        EXPECT_FALSE(statsDetails->RowGroups[0].Columns[1].NullCount.has_value());
        EXPECT_TRUE(Engine_->GetHealth().read_available);

        // Also check contradictory `null_count == rows` (3) combined with non-empty min/max bounds:
        // `GetFileDetails` must omit `NullCount`, `LowerBound`, and `UpperBound` without closing readiness.
        damagedStats[nullCountPos + 1] = '\x06'; // zigzag(3) -> null_count = 3 == rows alongside min/max!
        WriteBytes(path, damagedStats);
        cache->Clear();
        const auto allNullBoundsDetails = Engine_->GetFileDetails(test::collection_id, fileId);
        ASSERT_TRUE(allNullBoundsDetails) << allNullBoundsDetails.error().message;
        ASSERT_EQ(allNullBoundsDetails->RowGroups.size(), 1U);
        EXPECT_FALSE(allNullBoundsDetails->RowGroups[0].Columns[1].NullCount.has_value());
        EXPECT_FALSE(allNullBoundsDetails->RowGroups[0].Columns[1].LowerBound.has_value());
        EXPECT_FALSE(allNullBoundsDetails->RowGroups[0].Columns[1].UpperBound.has_value());
        EXPECT_TRUE(Engine_->GetHealth().read_available);
        EXPECT_TRUE(Engine_->Reconcile());
    }
}

#if defined(GLAMDB_ENABLE_CACHE_TEST_HOOKS)
TEST_F(ScanPruningVerticalTest, CacheAllocFailuresDuringColdReadsRemainBestEffort)
{
    CreateEngine();
    const auto cache = StorageBackend_->ReadMetadataCache();
    ASSERT_NE(cache, nullptr);

    ASSERT_EQ(Engine_
                  ->AppendDurable({test::collection_id,
                      FirstOperation,
                      {},
                      {MakePruningBatch({
                          {"k0", "lbl-0", 10, 1.0, true, 100, "p0"},
                          {"k1", "lbl-1", 20, 2.0, false, 200, "p1"},
                      })}})
                  .disposition,
        CommitDisposition::Committed);

    cache->Clear();
    ASSERT_EQ(cache->Stats().UsedBytes, 0U);

    size_t attemptedAllocs = 0;
    {
        detail::TScopedCacheAllocInjector failAllCacheAllocs(/*failOnCall=*/1, /*failAll=*/true);

        const auto stats = Engine_->GetStatistics(test::collection_id);
        ASSERT_TRUE(stats) << stats.error().message;
        EXPECT_EQ(stats->rows.value, 2);

        const auto files = Engine_->ListFiles(test::collection_id);
        ASSERT_TRUE(files) << files.error().message;
        EXPECT_EQ(files->Files.size(), 1U);

        const auto explain = Engine_->ExplainScan({.collection_id = test::collection_id});
        ASSERT_TRUE(explain) << explain.error().message;
        EXPECT_EQ(explain->candidates.rows.value, 2);

        const auto cursor = Engine_->Scan({
            .collection_id = test::collection_id,
            .projection = std::vector<std::string>{"key"},
        });
        ASSERT_TRUE(cursor) << cursor.error().message;
        EXPECT_EQ(DrainRendered(**cursor), (std::vector<std::string>{"k0", "k1"}));

        attemptedAllocs = detail::TScopedCacheAllocInjector::AllocationCount();
    }

    EXPECT_GT(attemptedAllocs, 0U);
    const auto cacheStats = cache->Stats();
    EXPECT_EQ(cacheStats.UsedBytes, 0U);
    EXPECT_EQ(cacheStats.RootEntries, 0U);
    EXPECT_EQ(cacheStats.ManifestListEntries, 0U);
    EXPECT_EQ(cacheStats.ManifestEntries, 0U);
    EXPECT_EQ(cacheStats.FooterEntries, 0U);
}
#endif

TEST_F(ScanPruningVerticalTest, ExplainScanFitsSingleMetadataEnvelopeUnder1200MiBBudget)
{
    ResourceLimits limits;
    limits.memory_bytes[static_cast<size_t>(ResourceClass::Metadata)] = 1200ULL << 20;
    auto owner = std::make_shared<ResourceOwner>(limits);

    detail::TLocalStorageOptions storageOpts;
    storageOpts.Resources = owner;
    auto created = detail::EngineFactory::CreateWith(
        {.store_id = test::store_id, .absolute_root = Root_.string(), .resources = owner},
        MakeFactory(storageOpts));
    ASSERT_TRUE(created) << created.error().message;
    Engine_ = *created;

    ASSERT_EQ(Engine_->CreateCollection({test::collection_id, "pruning_events", PruningSchema}).disposition,
        CommitDisposition::Committed);
    ASSERT_EQ(Engine_
                  ->AppendDurable({test::collection_id,
                      FirstOperation,
                      {},
                      {MakePruningBatch({
                          {"k0", "lbl-0", 10, 1.0, true, 100, "p0"},
                          {"k1", "lbl-1", 20, 2.0, false, 200, "p1"},
                      })}})
                  .disposition,
        CommitDisposition::Committed);

    const ScanRequest request{
        .collection_id = test::collection_id,
        .projection = std::vector<std::string>{"key"},
        .predicate = Predicate::In("label", {std::string("lbl-0"), std::string("lbl-1")}),
    };

    const auto cursor = Engine_->Scan(request);
    ASSERT_TRUE(cursor) << cursor.error().message;
    EXPECT_EQ(DrainRendered(**cursor), (std::vector<std::string>{"k0", "k1"}));

    const auto explain = Engine_->ExplainScan(request);
    ASSERT_TRUE(explain) << explain.error().message;
    EXPECT_EQ(explain->candidates.rows.value, 2);
}

TEST_F(ScanPruningVerticalTest, MultiRowGroupOverlappingChunksAndContradictoryAllNullBounds)
{
    const auto makeIncompressible = [](size_t len, uint64_t seed) {
        std::string out;
        out.reserve(len);
        uint64_t state = seed;
        for (size_t i = 0; i < len; ++i) {
            state = state * 6364136223846793005ULL + 1442695040888963407ULL;
            out.push_back(static_cast<char>('!' + ((state >> 56U) % 90U)));
        }
        return out;
    };
    const auto encodeZigZag = [](int64_t val) {
        uint64_t zz = (static_cast<uint64_t>(val) << 1U) ^ static_cast<uint64_t>(-(val < 0));
        std::string out;
        while (zz >= 0x80U) {
            out.push_back(static_cast<char>((zz & 0x7fU) | 0x80U));
            zz >>= 7U;
        }
        out.push_back(static_cast<char>(zz));
        return out;
    };
    const auto encodeFixedWidthZigZag = [&](int64_t val, size_t width) {
        std::string out = encodeZigZag(val);
        EXPECT_LE(out.size(), width);
        while (out.size() < width) {
            out.back() = static_cast<char>(static_cast<uint8_t>(out.back()) | 0x80U);
            out.push_back('\x00');
        }
        return out;
    };

    detail::TLocalStorageOptions options;
    options.Writer.MaxRowGroupBytes = 13000;
    CreateEngine(options);
    const auto cache = StorageBackend_->ReadMetadataCache();
    ASSERT_NE(cache, nullptr);

    ASSERT_EQ(Engine_
                  ->AppendDurable({test::collection_id,
                      FirstOperation,
                      {},
                      {MakePruningBatch({
                          {"k0", std::nullopt, 10, 1.0, true, 100, makeIncompressible(12000, 1)},
                          {"k1", std::nullopt, 20, 2.0, false, 200, makeIncompressible(12000, 2)},
                      })}})
                  .disposition,
        CommitDisposition::Committed);

    const auto listing = Engine_->ListFiles(test::collection_id);
    ASSERT_TRUE(listing) << listing.error().message;
    ASSERT_EQ(listing->Files.size(), 1U);
    const auto fileId = listing->Files.front().FileID;
    const auto details = Engine_->GetFileDetails(test::collection_id, fileId);
    ASSERT_TRUE(details) << details.error().message;
    ASSERT_EQ(details->RowGroups.size(), 2U);

    const auto dataPath = FilesIn(Root_ / "warehouse" / test::collection_id / "data").front();
    const std::string originalData = ReadBytes(dataPath);
    const int64_t fileSize = static_cast<int64_t>(originalData.size());
    const int64_t dataEnd = fileSize - details->FooterBytes - 8;
    ASSERT_GT(2 * (dataEnd - 8), fileSize);

    const auto readIo = StorageBackend_->ReadOnlyFileIO();
    ASSERT_TRUE(readIo);
    const auto rootMeta = NGlamDB::NStorage::ReadTableMetadata(**readIo, listing->Identity.metadata_location);
    ASSERT_TRUE(rootMeta);
    const auto snapshotRes = (*rootMeta)->Snapshot();
    ASSERT_TRUE(snapshotRes);
    const auto& snapshot = *snapshotRes;
    const iceberg::SnapshotReader snapReader(snapshot.get());
    const auto manifestsRes = snapReader.Manifests(*readIo);
    ASSERT_TRUE(manifestsRes);
    ASSERT_EQ(manifestsRes->size(), 1U);
    const iceberg::ManifestFile origManifestFile = manifestsRes->front();

    const auto manifestDiskPath = Root_ / "warehouse" / test::collection_id / "metadata" /
        std::filesystem::path(origManifestFile.manifest_path).filename();
    const auto manifestListDiskPath = Root_ / "warehouse" / test::collection_id / "metadata" /
        std::filesystem::path(snapshot->manifest_list).filename();
    const std::string originalManifest = ReadBytes(manifestDiskPath);
    const std::string originalManifestList = ReadBytes(manifestListDiskPath);

    const auto schema = (*rootMeta)->Schema().value();
    const auto spec = (*rootMeta)->PartitionSpec().value();
    const auto mReader = iceberg::ManifestReader::Make(origManifestFile, *readIo, schema, spec);
    ASSERT_TRUE(mReader);
    const auto liveEntries = (*mReader)->LiveEntries();
    ASSERT_TRUE(liveEntries);
    ASSERT_EQ(liveEntries->size(), 1U);
    const iceberg::DataFile origDataFile = *liveEntries->front().data_file;

    const auto rewriteManifest = [&](const iceberg::DataFile& updatedFile) {
        std::shared_ptr<iceberg::FileIO> memIo = iceberg::arrow::MakeMockFileIO();
        auto mWriter =
            iceberg::ManifestWriter::MakeWriter(2, snapshot->snapshot_id, "mem://m.avro", memIo, spec, schema);
        ASSERT_TRUE(mWriter) << mWriter.error().message;
        ASSERT_TRUE((*mWriter)->WriteAddedEntry(std::make_shared<iceberg::DataFile>(updatedFile)));
        ASSERT_TRUE((*mWriter)->Close());
        const auto mBytes = memIo->ReadFile("mem://m.avro", std::nullopt);
        ASSERT_TRUE(mBytes);

        iceberg::ManifestFile newManifestFile = origManifestFile;
        newManifestFile.manifest_length = static_cast<int64_t>(mBytes->size());
        auto mlWriter = iceberg::ManifestListWriter::MakeWriter(2,
            snapshot->snapshot_id,
            snapshot->parent_snapshot_id,
            "mem://ml.avro",
            memIo,
            snapshot->sequence_number);
        ASSERT_TRUE(mlWriter) << mlWriter.error().message;
        ASSERT_TRUE((*mlWriter)->Add(newManifestFile));
        ASSERT_TRUE((*mlWriter)->Close());
        const auto mlBytes = memIo->ReadFile("mem://ml.avro", std::nullopt);
        ASSERT_TRUE(mlBytes);

        WriteBytes(manifestDiskPath, *mBytes);
        WriteBytes(manifestListDiskPath, *mlBytes);
    };

    // Sanity check: rewriting identical DataFile passes Reconcile cleanly.
    rewriteManifest(origDataFile);
    ASSERT_TRUE(Engine_->Reconcile());

    const size_t footerStart = static_cast<size_t>(dataEnd);
    const auto origParquet = parquet::ParquetFileReader::OpenFile(dataPath.string())->metadata();

    // Locate ColumnMetaData for column 6 ("payload", field_id == 7) in RG 0 and RG 1.
    const size_t payload0Pos = originalData.find(std::string("\x19\x18\x07") + "payload", footerStart);
    ASSERT_NE(payload0Pos, std::string::npos);
    const size_t payload1Pos = originalData.find(std::string("\x19\x18\x07") + "payload", payload0Pos + 10);
    ASSERT_NE(payload1Pos, std::string::npos);

    const auto patchPayloadChunk = [&](std::string& buf, size_t pathPos, int rgIdx, int64_t newComp, int64_t newOff) {
        const auto chunk = origParquet->RowGroup(rgIdx)->ColumnChunk(6);
        const std::string origCompEnc = encodeZigZag(chunk->total_compressed_size());
        // Field 7 (total_compressed_size after field 6 total_uncompressed_size) has delta 1 (`\x16`).
        const size_t compField = buf.find(std::string(1, '\x16') + origCompEnc, pathPos + 10);
        ASSERT_NE(compField, std::string::npos);
        const std::string newCompEnc = encodeFixedWidthZigZag(newComp, origCompEnc.size());
        buf.replace(compField + 1, origCompEnc.size(), newCompEnc);

        const std::string origDataOffEnc = encodeZigZag(chunk->data_page_offset());
        // Field 9 (data_page_offset after field 7) has delta 2 (`\x26`).
        const size_t dataOffField =
            buf.find(std::string(1, '\x26') + origDataOffEnc, compField + 1 + newCompEnc.size());
        ASSERT_NE(dataOffField, std::string::npos);
        const std::string newDataOffEnc = encodeFixedWidthZigZag(newOff, origDataOffEnc.size());
        buf.replace(dataOffField + 1, origDataOffEnc.size(), newDataOffEnc);

        if (chunk->has_dictionary_page()) {
            const std::string origDictOffEnc = encodeZigZag(chunk->dictionary_page_offset());
            const size_t dictOffField =
                buf.find(std::string(1, '\x26') + origDictOffEnc, dataOffField + 1 + newDataOffEnc.size());
            ASSERT_NE(dictOffField, std::string::npos);
            const std::string newDictOffEnc = encodeFixedWidthZigZag(newOff, origDictOffEnc.size());
            buf.replace(dictOffField + 1, origDictOffEnc.size(), newDictOffEnc);
        }
    };

    // Case 1: 2-row-group overlapping chunks whose per-chunk ranges fit in [4, dataEnd),
    // while summed compressed bytes exceed file_size_in_bytes and match manifest column_sizes[7].
    // Both ListFiles/GetFileDetails/Scan AND Engine::Reconcile() must reject with CORRUPTION (no livelock).
    {
        const int64_t inflatedChunk = dataEnd - 8;
        std::string damagedData = originalData;
        patchPayloadChunk(damagedData, payload0Pos, 0, inflatedChunk, 4);
        patchPayloadChunk(damagedData, payload1Pos, 1, inflatedChunk, 4);
        ASSERT_EQ(static_cast<int64_t>(damagedData.size()), fileSize);
        WriteBytes(dataPath, damagedData);

        iceberg::DataFile inflatedFile = origDataFile;
        inflatedFile.column_sizes[7] = 2 * inflatedChunk;
        ASSERT_GT(inflatedFile.column_sizes[7], fileSize);
        rewriteManifest(inflatedFile);

        // Force ReadView through cold manifest read by invalidating via Reconcile or cold ListFiles:
        // First trigger Reconcile directly, and also verify ListFiles -> Reconcile stays in RecoveryRequired!
        ASSERT_TRUE(Engine_->Shutdown(1000ms));
        Engine_.reset();
        StorageBackend_.reset();
        auto reopened = detail::EngineFactory::OpenWith({Root_.string(), test::store_id}, MakeFactory(options));
        ASSERT_FALSE(reopened);
        EXPECT_EQ(reopened.error().code, "CORRUPTION") << reopened.error().message;

        // Restore valid state, reopen Ready, then corrupt on disk + clear cache and check ListFiles -> Reconcile!
        WriteBytes(dataPath, originalData);
        WriteBytes(manifestDiskPath, originalManifest);
        WriteBytes(manifestListDiskPath, originalManifestList);
        reopened = detail::EngineFactory::OpenWith({Root_.string(), test::store_id}, MakeFactory(options));
        ASSERT_TRUE(reopened) << reopened.error().message;
        Engine_ = *reopened;
        const auto reopenedCache = StorageBackend_->ReadMetadataCache();
        ASSERT_NE(reopenedCache, nullptr);

        WriteBytes(dataPath, damagedData);
        rewriteManifest(inflatedFile);
        reopenedCache->Clear();

        // Scan/GetFileDetails reads Parquet footer and latches RecoveryRequired; Reconcile must ALSO reject!
        const auto badDetails = Engine_->GetFileDetails(test::collection_id, fileId);
        ASSERT_FALSE(badDetails);
        EXPECT_EQ(badDetails.error().code, "CORRUPTION") << badDetails.error().message;
        EXPECT_FALSE(Engine_->GetHealth().read_available);

        const auto reconciled = Engine_->Reconcile();
        ASSERT_FALSE(reconciled);
        EXPECT_EQ(reconciled.error().code, "CORRUPTION") << reconciled.error().message;
        EXPECT_FALSE(Engine_->GetHealth().read_available);

        WriteBytes(dataPath, originalData);
        WriteBytes(manifestDiskPath, originalManifest);
        WriteBytes(manifestListDiskPath, originalManifestList);
        ASSERT_TRUE(Engine_->Reconcile());
        EXPECT_TRUE(Engine_->GetHealth().read_available);
    }

    // Case 2: Overlapping column chunks across 2 row groups WITHOUT exceeding file_size_in_bytes
    // (keep original compressed sizes, only point RG 1 col 6 offset at RG 0 col 6 offset):
    // Scan, GetFileDetails, and Reconcile all reject with CORRUPTION.
    {
        const auto rg0Chunk6 = origParquet->RowGroup(0)->ColumnChunk(6);
        const int64_t rg0Start =
            rg0Chunk6->has_dictionary_page() ? rg0Chunk6->dictionary_page_offset() : rg0Chunk6->data_page_offset();
        const int64_t rg1Comp = origParquet->RowGroup(1)->ColumnChunk(6)->total_compressed_size();
        std::string overlapData = originalData;
        patchPayloadChunk(overlapData, payload1Pos, 1, rg1Comp, rg0Start);
        WriteBytes(dataPath, overlapData);

        StorageBackend_->ReadMetadataCache()->Clear();
        const auto cursor = Engine_->Scan({
            .collection_id = test::collection_id,
            .projection = std::vector<std::string>{"key"},
        });
        ASSERT_TRUE(cursor) << cursor.error().message;
        const auto batch = (*cursor)->Next();
        ASSERT_FALSE(batch);
        EXPECT_EQ(batch.error().code, "CORRUPTION") << batch.error().message;
        EXPECT_FALSE(Engine_->GetHealth().read_available);

        const auto reconciled = Engine_->Reconcile();
        ASSERT_FALSE(reconciled);
        EXPECT_EQ(reconciled.error().code, "CORRUPTION") << reconciled.error().message;

        WriteBytes(dataPath, originalData);
        ASSERT_TRUE(Engine_->Reconcile());
    }

    // Case 3: Contradictory bounds on an all-null column ("label", field_id == 2, NullCount == 2 == record_count)
    // in the manifest must NOT close readiness; ListFiles omits both NullCount and LowerBound/UpperBound.
    {
        iceberg::DataFile contradictoryStats = origDataFile;
        contradictoryStats.null_value_counts[2] = contradictoryStats.record_count;
        contradictoryStats.lower_bounds[2] = {'a'};
        contradictoryStats.upper_bounds[2] = {'z'};
        rewriteManifest(contradictoryStats);
        ASSERT_TRUE(Engine_->Reconcile());

        const auto files = Engine_->ListFiles(test::collection_id);
        ASSERT_TRUE(files) << files.error().message;
        ASSERT_EQ(files->Files.size(), 1U);
        const auto& labelMetrics = files->Files.front().FieldMetrics[1];
        EXPECT_EQ(labelMetrics.FieldID, 2);
        EXPECT_FALSE(labelMetrics.NullCount.has_value());
        EXPECT_FALSE(labelMetrics.LowerBound.has_value());
        EXPECT_FALSE(labelMetrics.UpperBound.has_value());
        EXPECT_TRUE(Engine_->GetHealth().read_available);

        WriteBytes(manifestDiskPath, originalManifest);
        WriteBytes(manifestListDiskPath, originalManifestList);
        ASSERT_TRUE(Engine_->Reconcile());
    }
}

} // namespace
} // namespace glamdb
