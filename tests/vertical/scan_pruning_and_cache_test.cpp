// Vertical storage verification for conservative SQL-3VL file & row-group pruning
// and bounded immutable metadata/manifest/footer caching.

#include "core/scan/metadata_cache.h"
#include "tests/support/fault_fs.h"
#include "tests/vertical/vertical_support.h"

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <parquet/arrow/reader.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <optional>
#include <string>
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

} // namespace
} // namespace glamdb
