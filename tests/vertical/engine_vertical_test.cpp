#include "tests/support/checked.h"
#include "tests/vertical/vertical_support.h"

#include <arrow/c/bridge.h>

#include <chrono>
#include <future>

namespace glamdb {
namespace {

using namespace std::chrono_literals;
using namespace glamdb::vertical;

class EngineVerticalTest : public ::testing::Test
{
protected:
    test::TempDir Temporary_;
    std::filesystem::path Root_ = Temporary_.path / "store";
    std::shared_ptr<Engine> Engine_;

    void SetUp() override
    {
        auto created = CreateStore({test::store_id, Root_.string()});
        ASSERT_TRUE(created) << created.error().message;
        Engine_ = *created;
        const auto outcome = Engine_->CreateCollection({test::collection_id, "events", Schema});
        ASSERT_EQ(outcome.disposition, CommitDisposition::Committed)
            << (outcome.error ? outcome.error->message : std::string());
    }

    void TearDown() override
    {
        if (Engine_) {
            EXPECT_TRUE(Engine_->Shutdown(1000ms));
        }
    }

    AppendOutcome Append(const char* operation, const std::vector<TInputRow>& rows, SnapshotPrecondition expected = {})
    {
        return Engine_->AppendDurable({test::collection_id, operation, expected, {Batch(rows)}});
    }

    AppendResult
    Committed(const char* operation, const std::vector<TInputRow>& rows, SnapshotPrecondition expected = {})
    {
        const auto outcome = Append(operation, rows, expected);
        EXPECT_EQ(outcome.disposition, CommitDisposition::Committed)
            << (outcome.error ? outcome.error->code + ": " + outcome.error->message : std::string());
        return outcome.result.value_or(AppendResult{});
    }

    std::shared_ptr<ScanCursor> Open(ScanRequest request)
    {
        request.collection_id = test::collection_id;
        auto cursor = Engine_->Scan(request);
        EXPECT_TRUE(cursor) << (cursor ? std::string() : cursor.error().code + ": " + cursor.error().message);
        return cursor ? *cursor : nullptr;
    }

    std::vector<std::string> Drain(ScanCursor& cursor, int64_t batchRows = 65536)
    {
        std::vector<std::string> rows;
        while (true) {
            const auto batch = cursor.Next();
            EXPECT_TRUE(batch) << (batch ? std::string() : batch.error().message);
            if (!batch || !*batch) {
                break;
            }
            EXPECT_TRUE((*batch)->schema()->Equals(*cursor.Info().output_schema));
            EXPECT_GT((*batch)->num_rows(), 0);
            EXPECT_LE((*batch)->num_rows(), batchRows);
            const auto rendered = Render(**batch);
            rows.insert(rows.end(), rendered.begin(), rendered.end());
        }
        return rows;
    }

    std::vector<std::string> Read(ScanRequest request)
    {
        const auto cursor = Open(std::move(request));
        return cursor ? Sorted(Drain(*cursor)) : std::vector<std::string>{};
    }

    void Reopen()
    {
        ASSERT_TRUE(Engine_->Shutdown(1000ms));
        Engine_.reset();
        auto opened = OpenExisting({Root_.string(), test::store_id});
        ASSERT_TRUE(opened) << opened.error().code << ": " << opened.error().message;
        Engine_ = *opened;
    }

    std::vector<std::filesystem::path> DataFiles() const
    {
        std::vector<std::filesystem::path> files;
        for (const auto& entry :
            std::filesystem::directory_iterator(Root_ / "warehouse" / test::collection_id / "data"))
        {
            files.push_back(entry.path());
        }
        return files;
    }
};

TEST_F(EngineVerticalTest, CreatesAnEmptyCollectionRoot)
{
    bool root = false;
    for (const auto& entry :
        std::filesystem::directory_iterator(Root_ / "warehouse" / test::collection_id / "metadata"))
    {
        root = root || entry.path().filename().string().starts_with("00000-");
    }
    EXPECT_TRUE(root);
    EXPECT_FALSE(Engine_->GetCollection(test::collection_id)->current_snapshot_id);

    const auto cursor = Open({});
    ASSERT_NE(cursor, nullptr);
    EXPECT_EQ(cursor->Info().selector_resolved, SnapshotSelector::InitialEmpty());
    EXPECT_TRUE(cursor->Info().output_schema->Equals(**Engine_->Schema(test::collection_id)));
    EXPECT_TRUE(Drain(*cursor).empty());
}

TEST_F(EngineVerticalTest, AppendsParquetAndScansFixedSnapshots)
{
    const auto first = Committed(FirstOperation, FirstRows);
    EXPECT_EQ(first.rows_appended, 3);
    EXPECT_EQ(first.data_files_added, 1);
    EXPECT_GT(first.compressed_data_bytes_added, 0);
    EXPECT_FALSE(first.parent_snapshot_id);
    EXPECT_EQ(Engine_->GetCollection(test::collection_id)->current_snapshot_id, first.snapshot_id);
    EXPECT_EQ(DataFiles().size(), 1U);

    const auto second = Committed(SecondOperation, SecondRows, {SnapshotPreconditionKind::Equals, first.snapshot_id});
    EXPECT_EQ(second.parent_snapshot_id, first.snapshot_id);

    EXPECT_EQ(Read({}), Expected({&FirstRows, &SecondRows}));
    EXPECT_EQ(Read({.selector = SnapshotSelector::Id(first.snapshot_id)}), Expected({&FirstRows}));
    EXPECT_TRUE(Read({.selector = SnapshotSelector::InitialEmpty()}).empty());
}

TEST_F(EngineVerticalTest, OpenCursorKeepsItsResolvedSnapshot)
{
    const auto first = Committed(FirstOperation, FirstRows);
    const auto cursor = Open({});
    ASSERT_NE(cursor, nullptr);
    EXPECT_EQ(cursor->Info().selector_resolved, SnapshotSelector::Id(first.snapshot_id));
    Committed(SecondOperation, SecondRows);
    EXPECT_EQ(Sorted(Drain(*cursor)), Expected({&FirstRows}));
    EXPECT_EQ(Read({}), Expected({&FirstRows, &SecondRows}));
}

TEST_F(EngineVerticalTest, FiltersProjectsAndLimits)
{
    Committed(FirstOperation, FirstRows);
    Committed(SecondOperation, SecondRows);

    EXPECT_EQ(Read({.predicate = Predicate::Compare("value", CompareOp::Gt, int64_t{1})}).size(), 2U);
    // NULL compares as UNKNOWN: it is excluded even by "not equal".
    EXPECT_EQ(Read({.predicate = Predicate::Compare("value", CompareOp::Ne, int64_t{2})}).size(), 3U);
    EXPECT_EQ(Read({.predicate = Predicate::Compare("key", CompareOp::Eq, std::string("duplicate"))}).size(), 3U);
    EXPECT_EQ(Read({.predicate = Predicate::Compare("at", CompareOp::Ge, TimestampUs{20})}).size(), 2U);

    const auto keys = Read(
        {.projection = std::vector<std::string>{"key"}, .predicate = Predicate::Compare("flag", CompareOp::Eq, true)});
    EXPECT_EQ(keys, Sorted({"duplicate", "duplicate", "\xD0\xBA\xD0\xBB\xD1\x8E\xD1\x87"}));

    const auto limited = Open({.limit = 4, .batch_rows = 1});
    ASSERT_NE(limited, nullptr);
    EXPECT_EQ(limited->Info().output_schema->num_fields(), 5);
    EXPECT_EQ(Drain(*limited, 1).size(), 4U);

    const auto none = Open({.limit = 0});
    ASSERT_NE(none, nullptr);
    EXPECT_TRUE(Drain(*none).empty());
}

TEST_F(EngineVerticalTest, CompoundPredicatesAcrossSnapshotsDuplicatesAndBatchSizes)
{
    const auto first = Committed(FirstOperation, FirstRows);
    Committed(SecondOperation, SecondRows);

    // FirstRows:
    //   ("duplicate", 1, 0.5, true, 10)
    //   ("duplicate", 1, 0.5, true, 10)
    //   ("other", null, null, null, null)
    // SecondRows:
    //   ("duplicate", 2, -1.5, false, 20)
    //   ("\xD0\xBA\xD0\xBB\xD1\x8E\xD1\x87", 3, 2.5, true, 30)
    const auto compound = Predicate::Or({
        Predicate::And({
            Predicate::Compare("key", CompareOp::Eq, std::string("duplicate")),
            Predicate::In("value", {int64_t{1}, std::nullopt}),
            Predicate::Compare("score", CompareOp::Gt, 0.0),
            Predicate::Compare("flag", CompareOp::Eq, true),
        }),
        Predicate::And({
            Predicate::IsNull("value"),
            Predicate::Not(Predicate::IsNotNull("at")),
        }),
    });

    // Latest snapshot with projection = {"key"} (filtering on non-projected value, score, flag, at)
    // and batch_rows = 1:
    const auto cursor = Open({
        .projection = std::vector<std::string>{"key"},
        .predicate = compound,
        .batch_rows = 1,
    });
    ASSERT_NE(cursor, nullptr);
    EXPECT_EQ(cursor->Info().output_schema->num_fields(), 1);
    EXPECT_EQ(Sorted(Drain(*cursor, 1)), Sorted({"duplicate", "duplicate", "other"}));

    // Historical snapshot with NOT(value IN []): excludes NULL row ("other") under SQL 3VL!
    EXPECT_EQ(Read({
                  .selector = SnapshotSelector::Id(first.snapshot_id),
                  .projection = std::vector<std::string>{"key"},
                  .predicate = Predicate::Not(Predicate::In("value", {})),
              }),
        Sorted({"duplicate", "duplicate"}));

    // CompareNull in Or with IsNull: Unknown OR True == True for row "other".
    EXPECT_EQ(Read({
                  .projection = std::vector<std::string>{"key"},
                  .predicate = Predicate::Or({
                      Predicate::CompareNull("value", CompareOp::Eq),
                      Predicate::IsNull("score"),
                  }),
              }),
        Sorted({"other"}));
}

TEST_F(EngineVerticalTest, InvalidCompoundPredicatesPreserveReadinessAndFilteredBuffersRetainStoreLease)
{
    Committed(FirstOperation, FirstRows);
    Committed(SecondOperation, SecondRows);

    // Invalid compound predicates fail with INVALID_ARGUMENT without latching engine readiness.
    for (const auto& bad : {
             Predicate::And({}),
             Predicate::Or({}),
             Predicate::And({Predicate::True(), Predicate::Compare("missing", CompareOp::Eq, int64_t{1})}),
             Predicate::In("value", {int64_t{1}, 2.5}),
         })
    {
        const auto res = Engine_->Scan({.collection_id = test::collection_id, .predicate = bad});
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().code, "INVALID_ARGUMENT");
        const auto health = Engine_->GetHealth();
        EXPECT_TRUE(health.read_available);
        EXPECT_TRUE(health.write_ready);
    }

    // Filtered buffers (via TakeRows) must retain StoreLease after cursor and Engine are released.
    auto cursor = Open({
        .projection = std::vector<std::string>{"key"},
        .predicate = Predicate::And({
            Predicate::IsNotNull("value"),
            Predicate::Compare("value", CompareOp::Ge, int64_t{2}),
        }),
    });
    ASSERT_NE(cursor, nullptr);
    auto batch = cursor->Next();
    ASSERT_TRUE(batch);
    ASSERT_NE(*batch, nullptr);
    EXPECT_EQ((*batch)->num_rows(), 2);

    auto slice = (*batch)->column(0)->Slice(0, 1);
    batch = std::shared_ptr<arrow::RecordBatch>{};
    cursor.reset();
    ASSERT_TRUE(Engine_->Shutdown(1000ms));
    Engine_.reset();

    const auto locked = OpenExisting({Root_.string(), test::store_id});
    ASSERT_FALSE(locked);
    EXPECT_EQ(locked.error().code, "STATE_IN_USE");

    slice.reset();
    auto reopened = OpenExisting({Root_.string(), test::store_id});
    ASSERT_TRUE(reopened) << reopened.error().code << ": " << reopened.error().message;
    Engine_ = *reopened;
}

TEST_F(EngineVerticalTest, RejectsInvalidScans)
{
    const auto code = [&](const ScanRequest& request) {
        const auto cursor = Engine_->Scan(request);
        return cursor ? std::string() : cursor.error().code;
    };
    EXPECT_EQ(code({.collection_id = "not-a-uuid"}), "INVALID_ARGUMENT");
    EXPECT_EQ(code({.collection_id = test::table_uuid}), "COLLECTION_NOT_FOUND");
    ScanRequest request{.collection_id = test::collection_id};
    request.projection = std::vector<std::string>{};
    EXPECT_EQ(code(request), "INVALID_ARGUMENT");
    request.projection = std::vector<std::string>{"key", "key"};
    EXPECT_EQ(code(request), "INVALID_ARGUMENT");
    EXPECT_EQ(code({.collection_id = test::collection_id, .batch_rows = 0}), "INVALID_ARGUMENT");
    EXPECT_EQ(code({.collection_id = test::collection_id, .limit = -1}), "INVALID_ARGUMENT");
    EXPECT_EQ(
        code({.collection_id = test::collection_id, .predicate = Predicate::Compare("value", CompareOp::Eq, 1.0)}),
        "INVALID_ARGUMENT");
    EXPECT_EQ(code({.collection_id = test::collection_id, .selector = SnapshotSelector::Id(12345)}), "NOT_FOUND");
}

TEST_F(EngineVerticalTest, RetryReturnsTheOriginalReceipt)
{
    const auto first = Append(FirstOperation, FirstRows);
    ASSERT_EQ(first.disposition, CommitDisposition::Committed);
    EXPECT_EQ(Append(FirstOperation, FirstRows), first);
    const auto conflict = Append(FirstOperation, SecondRows);
    EXPECT_EQ(conflict.error ? conflict.error->code : "", "OPERATION_CONFLICT");
    EXPECT_EQ(Read({}), Expected({&FirstRows}));
}

TEST_F(EngineVerticalTest, ReopenVerifiesAndContinuesTheHistory)
{
    const auto first = Committed(FirstOperation, FirstRows);
    Reopen();
    EXPECT_EQ(Read({}), Expected({&FirstRows}));
    const auto second = Committed(SecondOperation, SecondRows);
    EXPECT_EQ(second.parent_snapshot_id, first.snapshot_id);
    Reopen();
    EXPECT_EQ(Read({}), Expected({&FirstRows, &SecondRows}));
    EXPECT_EQ(Read({.selector = SnapshotSelector::Id(first.snapshot_id)}), Expected({&FirstRows}));
}

TEST_F(EngineVerticalTest, OversizedAppendFailsDefinitely)
{
    std::vector<TInputRow> rows(10001, TInputRow{"k", 1, 1.0, true, 1});
    const auto outcome = Append(FirstOperation, rows);
    EXPECT_EQ(outcome.disposition, CommitDisposition::DefinitelyNotCommitted);
    EXPECT_FALSE(Engine_->GetCollection(test::collection_id)->current_snapshot_id);
    Committed(SecondOperation, SecondRows);
    EXPECT_EQ(Read({}), Expected({&SecondRows}));
}

TEST_F(EngineVerticalTest, CancelAndShutdownStopCursors)
{
    Committed(FirstOperation, FirstRows);
    const auto cancelled = Open({});
    ASSERT_NE(cancelled, nullptr);
    cancelled->Cancel();
    const auto next = cancelled->Next();
    ASSERT_FALSE(next);
    EXPECT_EQ(next.error().code, "CANCELLED");

    const auto open = Open({});
    ASSERT_NE(open, nullptr);
    ASSERT_TRUE(Engine_->Shutdown(1000ms));
    Engine_.reset();
    EXPECT_EQ(open->Info().output_schema->num_fields(), 5);
    const auto stopped = open->Next();
    ASSERT_FALSE(stopped);
    EXPECT_EQ(stopped.error().code, "CANCELLED");
}

TEST_F(EngineVerticalTest, MissingCommittedDataFailsClosed)
{
    Committed(FirstOperation, FirstRows);
    Committed(ThirdOperation, SecondRows);
    ASSERT_TRUE(Engine_->Shutdown(1000ms));
    Engine_.reset();
    const auto files = DataFiles();
    ASSERT_EQ(files.size(), 2U);
    std::filesystem::remove(files.front());
    EXPECT_FALSE(OpenExisting({Root_.string(), test::store_id}));
}

TEST_F(EngineVerticalTest, CursorDetectedCorruptionClosesReadiness)
{
    Committed(FirstOperation, FirstRows);
    const auto cursor = Open({});
    ASSERT_NE(cursor, nullptr);

    const auto files = DataFiles();
    ASSERT_EQ(files.size(), 1U);
    std::filesystem::remove(files.front());

    const auto next = cursor->Next();
    ASSERT_FALSE(next);
    EXPECT_EQ(next.error().code, "CORRUPTION");

    const auto collection = Engine_->GetCollection(test::collection_id);
    ASSERT_FALSE(collection);
    EXPECT_EQ(collection.error().code, "RECOVERY_REQUIRED");

    const auto blocked = Append(SecondOperation, SecondRows);
    EXPECT_EQ(blocked.disposition, CommitDisposition::CommitUnknown);
    ASSERT_TRUE(blocked.error);
    EXPECT_EQ(test::Present(blocked.error).code, "RECOVERY_REQUIRED");
    Engine_.reset();
}

TEST_F(EngineVerticalTest, ReturnedArrowBuffersRetainStoreLeaseAfterCursorAndEngineRelease)
{
    Committed(FirstOperation, FirstRows);
    auto cursor = Open({});
    ASSERT_NE(cursor, nullptr);

    auto batch = cursor->Next();
    ASSERT_TRUE(batch);
    ASSERT_NE(*batch, nullptr);

    auto slice = (*batch)->column(0)->Slice(0, 1);
    ArrowArray exported{};
    ASSERT_TRUE(arrow::ExportArray(*(*batch)->column(1), &exported).ok());

    batch = std::shared_ptr<arrow::RecordBatch>{};
    cursor.reset();
    ASSERT_TRUE(Engine_->Shutdown(1000ms));
    Engine_.reset();

    const auto lockedBySlice = OpenExisting({Root_.string(), test::store_id});
    ASSERT_FALSE(lockedBySlice);
    EXPECT_EQ(lockedBySlice.error().code, "STATE_IN_USE");

    slice.reset();
    const auto lockedByExport = OpenExisting({Root_.string(), test::store_id});
    ASSERT_FALSE(lockedByExport);
    EXPECT_EQ(lockedByExport.error().code, "STATE_IN_USE");

    if (exported.release != nullptr) {
        exported.release(&exported);
    }

    auto reopened = OpenExisting({Root_.string(), test::store_id});
    ASSERT_TRUE(reopened) << reopened.error().code << ": " << reopened.error().message;
    Engine_ = *reopened;
}

TEST_F(EngineVerticalTest, ZeroLimitCursorChecksCancellationBeforeReturningEos)
{
    Committed(FirstOperation, FirstRows);
    const auto zeroLimit = Open({.limit = 0});
    ASSERT_NE(zeroLimit, nullptr);
    zeroLimit->Cancel();
    const auto next = zeroLimit->Next();
    ASSERT_FALSE(next);
    EXPECT_EQ(next.error().code, "CANCELLED");
}

TEST_F(EngineVerticalTest, PreBindingWriterRowLimitDoesNotConsumeOperationId)
{
    constexpr char WideCollection[] = "00000000-0000-4000-8000-0000000000c1";
    const SchemaSpec wideSchema{{
        {"s1", FieldType::String, false},
        {"s2", FieldType::String, false},
        {"n", FieldType::Int64, true},
    }};
    ASSERT_EQ(Engine_->CreateCollection({WideCollection, "wide", wideSchema}).disposition,
        CommitDisposition::Committed);

    const auto arrowSchema = arrow::schema({
        arrow::field("s1", arrow::utf8(), false),
        arrow::field("s2", arrow::utf8(), false),
        arrow::field("n", arrow::int64(), true),
    });
    const auto oversizedBatch = arrow::RecordBatch::Make(arrowSchema,
        1,
        {
            Column<arrow::StringBuilder>(std::vector<std::optional<std::string>>{std::string(1048576, 'a')},
                arrow::utf8()),
            Column<arrow::StringBuilder>(std::vector<std::optional<std::string>>{std::string(1048561, 'b')},
                arrow::utf8()),
            Column<arrow::Int64Builder>(std::vector<std::optional<int64_t>>{std::nullopt}, arrow::int64()),
        });

    const auto rejected = Engine_->AppendDurable({WideCollection, FirstOperation, {}, {oversizedBatch}});
    EXPECT_EQ(rejected.disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(rejected.error);
    EXPECT_EQ(test::Present(rejected.error).code, "RESOURCE_EXHAUSTED");
    EXPECT_EQ(test::Present(rejected.error).retry_hint, RetryHint::FixInput);

    const auto lookup = Engine_->FindAppendOutcome(WideCollection, FirstOperation);
    ASSERT_TRUE(lookup);
    EXPECT_FALSE(lookup->has_value());

    const auto validBatch = arrow::RecordBatch::Make(arrowSchema,
        1,
        {
            Column<arrow::StringBuilder>(std::vector<std::optional<std::string>>{std::string("ok")}, arrow::utf8()),
            Column<arrow::StringBuilder>(std::vector<std::optional<std::string>>{std::string("row")}, arrow::utf8()),
            Column<arrow::Int64Builder>(std::vector<std::optional<int64_t>>{42}, arrow::int64()),
        });
    const auto committed = Engine_->AppendDurable({WideCollection, FirstOperation, {}, {validBatch}});
    EXPECT_EQ(committed.disposition, CommitDisposition::Committed);
}

TEST_F(EngineVerticalTest, MultiCollectionStoreCapacityEnforcedAcrossCommitsAndRecovery)
{
    ASSERT_TRUE(Engine_->Shutdown(1000ms));
    Engine_.reset();

    const auto customRoot = Temporary_.path / "capped-store";
    const auto countWarehouseFiles = [](const std::filesystem::path& warehouse) {
        std::size_t count = 0;
        for (const auto& entry : std::filesystem::recursive_directory_iterator(warehouse)) {
            if (entry.is_regular_file()) {
                ++count;
            }
        }
        return count;
    };
    const auto makeFactory = [](std::size_t maxSnapshots) -> detail::BackendFactory {
        return [maxSnapshots](const std::filesystem::path& path,
                   const StoreInfo& info) -> Result<std::shared_ptr<detail::StorageBackend>> {
            detail::TLocalStorageOptions options;
            options.Storage.Store.Snapshots = maxSnapshots;
            auto backend = detail::TLocalStorageBackend::Open(path, info, options);
            if (!backend) {
                return std::unexpected(backend.error());
            }
            return std::shared_ptr<detail::StorageBackend>(std::move(*backend));
        };
    };

    auto capped = detail::EngineFactory::CreateWith({test::store_id, customRoot.string()}, makeFactory(1));
    ASSERT_TRUE(capped) << capped.error().message;
    auto engine = std::move(*capped);
    ASSERT_EQ(engine->CreateCollection({test::collection_id, "events-a", Schema}).disposition,
        CommitDisposition::Committed);
    ASSERT_EQ(engine->CreateCollection({SecondCollection, "events-b", Schema}).disposition,
        CommitDisposition::Committed);

    const auto first = engine->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}});
    ASSERT_EQ(first.disposition, CommitDisposition::Committed);
    const auto filesBefore = countWarehouseFiles(customRoot / "warehouse");

    const auto second = engine->AppendDurable({SecondCollection, SecondOperation, {}, {Batch(SecondRows)}});
    EXPECT_EQ(second.disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(second.error);
    EXPECT_EQ(test::Present(second.error).code, "RESOURCE_EXHAUSTED");
    const auto secondLookup = engine->FindAppendOutcome(SecondCollection, SecondOperation);
    ASSERT_TRUE(secondLookup);
    EXPECT_FALSE(secondLookup->has_value());
    EXPECT_EQ(countWarehouseFiles(customRoot / "warehouse"), filesBefore);
    EXPECT_FALSE(engine->GetCollection(SecondCollection)->current_snapshot_id);
    ASSERT_TRUE(engine->Shutdown(1000ms));
    engine.reset();

    auto wider = detail::EngineFactory::OpenWith({customRoot.string(), test::store_id}, makeFactory(2));
    ASSERT_TRUE(wider) << wider.error().message;
    engine = std::move(*wider);
    ASSERT_EQ(engine->AppendDurable({SecondCollection, SecondOperation, {}, {Batch(SecondRows)}}).disposition,
        CommitDisposition::Committed);
    ASSERT_TRUE(engine->Shutdown(1000ms));
    engine.reset();

    const auto tooSmall = detail::EngineFactory::OpenWith({customRoot.string(), test::store_id}, makeFactory(1));
    ASSERT_FALSE(tooSmall);
    EXPECT_EQ(tooSmall.error().code, "CORRUPTION");
}

TEST_F(EngineVerticalTest, MalformedAvroManifestAndOutOfRangeTimestampFailClosedAsCorruption)
{
    Committed(FirstOperation, FirstRows);

    std::filesystem::path manifestPath;
    std::filesystem::path metadataPath;
    for (const auto& entry :
        std::filesystem::directory_iterator(Root_ / "warehouse" / test::collection_id / "metadata"))
    {
        const auto filename = entry.path().filename().string();
        if (entry.path().extension() == ".avro" && !filename.starts_with("snap-")) {
            manifestPath = entry.path();
        } else if (filename.starts_with("00001-") && filename.ends_with(".metadata.json")) {
            metadataPath = entry.path();
        }
    }
    ASSERT_FALSE(manifestPath.empty());
    ASSERT_FALSE(metadataPath.empty());

    std::string manifestBytes;
    {
        std::ifstream input(manifestPath, std::ios::binary);
        manifestBytes.assign(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
    }
    ASSERT_FALSE(manifestBytes.empty());
    {
        auto corrupted = manifestBytes;
        corrupted[0] = 'X';
        std::ofstream output(manifestPath, std::ios::binary | std::ios::trunc);
        output.write(corrupted.data(), static_cast<std::streamsize>(corrupted.size()));
    }

    // Manifests are read on demand: the failure surfaces at open or first Next().
    const auto scan = Engine_->Scan({.collection_id = test::collection_id});
    const auto failure = scan ? (*scan)->Next() : std::unexpected(scan.error());
    ASSERT_FALSE(failure);
    EXPECT_EQ(failure.error().code, "CORRUPTION");
    const auto afterManifestCorruption = Engine_->ListCollections();
    ASSERT_FALSE(afterManifestCorruption);
    EXPECT_EQ(afterManifestCorruption.error().code, "RECOVERY_REQUIRED");

    {
        std::ofstream output(manifestPath, std::ios::binary | std::ios::trunc);
        output.write(manifestBytes.data(), static_cast<std::streamsize>(manifestBytes.size()));
    }
    ASSERT_TRUE(Engine_->Reconcile());

    std::string metadataJson;
    {
        std::ifstream input(metadataPath, std::ios::binary);
        metadataJson.assign(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
    }
    const std::string key = "\"timestamp-ms\"";
    const auto keyPos = metadataJson.find(key);
    ASSERT_NE(keyPos, std::string::npos);
    const auto colonPos = metadataJson.find(':', keyPos + key.size());
    ASSERT_NE(colonPos, std::string::npos);
    const auto valueStart = metadataJson.find_first_of("-0123456789", colonPos + 1);
    ASSERT_NE(valueStart, std::string::npos);
    const auto valueEnd = metadataJson.find_first_not_of("-0123456789", valueStart);
    ASSERT_NE(valueEnd, std::string::npos);
    {
        auto badMetadata = metadataJson;
        badMetadata.replace(valueStart, valueEnd - valueStart, "-62135596800001");
        std::ofstream output(metadataPath, std::ios::binary | std::ios::trunc);
        output.write(badMetadata.data(), static_cast<std::streamsize>(badMetadata.size()));
    }

    const auto badReconcile = Engine_->Reconcile();
    ASSERT_FALSE(badReconcile);
    EXPECT_EQ(badReconcile.error().code, "CORRUPTION");

    {
        std::ofstream output(metadataPath, std::ios::binary | std::ios::trunc);
        output.write(metadataJson.data(), static_cast<std::streamsize>(metadataJson.size()));
    }
    ASSERT_TRUE(Engine_->Reconcile());
}

} // namespace
} // namespace glamdb
