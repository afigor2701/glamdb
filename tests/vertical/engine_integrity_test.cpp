// Engine/storage integrity: concurrent corruption reports, fail-closed
// classification and retry verification over the real storage backend.

#include "tests/support/fault_fs.h"
#include "tests/vertical/vertical_support.h"

#include <arrow/io/memory.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include <sys/stat.h>

#include <chrono>
#include <future>

namespace glamdb {
namespace {

using namespace std::chrono_literals;
using namespace glamdb::vertical;

class EngineIntegrityTest : public ::testing::Test
{
protected:
    test::TempDir Temporary_;
    std::filesystem::path Root_ = Temporary_.path / "store";
    std::shared_ptr<THookBackend> Hooks_;
    std::shared_ptr<Engine> Engine_;

    void TearDown() override
    {
        if (Engine_) {
            Engine_->Shutdown(1000ms);
        }
    }

    void CreateTwoCollections(detail::TLocalStorageOptions options = {})
    {
        auto created =
            detail::EngineFactory::CreateWith({test::store_id, Root_.string()}, THookBackend::Factory(Hooks_, options));
        ASSERT_TRUE(created) << created.error().message;
        Engine_ = std::move(*created);
        ASSERT_EQ(Engine_->CreateCollection({test::collection_id, "events-a", Schema}).disposition,
            CommitDisposition::Committed);
        ASSERT_EQ(Engine_->CreateCollection({SecondCollection, "events-b", Schema}).disposition,
            CommitDisposition::Committed);
    }

    void CommitFirst()
    {
        const auto outcome = Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}});
        ASSERT_EQ(outcome.disposition, CommitDisposition::Committed)
            << (outcome.error ? outcome.error->code + ": " + outcome.error->message : std::string());
    }

    std::filesystem::path Warehouse(std::string_view collection, std::string_view kind) const
    {
        return Root_ / "warehouse" / std::string(collection) / std::string(kind);
    }

    std::filesystem::path OnlyDataFile(std::string_view collection) const
    {
        const auto files = FilesIn(Warehouse(collection, "data"));
        EXPECT_EQ(files.size(), 1U);
        return files.empty() ? std::filesystem::path{} : files.front();
    }

    std::shared_ptr<ScanCursor> OpenCursor(std::string_view collection)
    {
        auto cursor = Engine_->Scan({.collection_id = std::string(collection)});
        EXPECT_TRUE(cursor) << (cursor ? std::string() : cursor.error().message);
        return cursor ? *cursor : nullptr;
    }

    AppendOutcome AppendSecond()
    {
        return Engine_->AppendDurable({SecondCollection, SecondOperation, {}, {Batch(SecondRows)}});
    }
};

// Corruption reported after PrepareAppend and before publication prevents it.
TEST_F(EngineIntegrityTest, CorruptionAfterPrepareAbortsPublication)
{
    CreateTwoCollections();
    CommitFirst();
    const auto cursor = OpenCursor(test::collection_id);
    ASSERT_NE(cursor, nullptr);
    const auto dataFile = OnlyDataFile(test::collection_id);
    const auto saved = ReadBytes(dataFile);

    std::promise<void> entered;
    std::promise<void> resume;
    Hooks_->AfterPrepareAppend = [&](const detail::BoundOperation& binding) {
        if (binding.collection_id == SecondCollection) {
            entered.set_value();
            resume.get_future().wait();
        }
    };
    auto writer = std::async(std::launch::async, [&] {
        return AppendSecond();
    });
    entered.get_future().wait();
    std::filesystem::remove(dataFile);
    const auto next = cursor->Next();
    ASSERT_FALSE(next);
    EXPECT_EQ(next.error().code, "CORRUPTION");
    resume.set_value();

    const auto outcome = writer.get();
    EXPECT_EQ(outcome.disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(outcome.error);
    EXPECT_EQ(outcome.error->code, "RECOVERY_REQUIRED");
    const auto blocked = Engine_->GetCollection(SecondCollection);
    ASSERT_FALSE(blocked);
    EXPECT_EQ(blocked.error().code, "RECOVERY_REQUIRED");

    WriteBytes(dataFile, saved);
    ASSERT_TRUE(Engine_->Reconcile());
    EXPECT_FALSE(Engine_->GetCollection(SecondCollection)->current_snapshot_id);
    const auto lookup = Engine_->FindAppendOutcome(SecondCollection, SecondOperation);
    ASSERT_TRUE(lookup);
    ASSERT_TRUE(lookup->has_value());
    EXPECT_EQ((*lookup)->disposition, CommitDisposition::DefinitelyNotCommitted);
}

// Corruption completed inside the outer catalog transaction, before the
// guard's final health decision, rolls the publication back.
TEST_F(EngineIntegrityTest, CorruptionBeforeGuardDecisionAbortsPublication)
{
    CreateTwoCollections();
    CommitFirst();
    const auto cursor = OpenCursor(test::collection_id);
    ASSERT_NE(cursor, nullptr);
    const auto dataFile = OnlyDataFile(test::collection_id);
    const auto saved = ReadBytes(dataFile);

    Hooks_->BeforeVerifyClosure = [&](std::string_view collection) {
        if (collection != SecondCollection) {
            return;
        }
        std::filesystem::remove(dataFile);
        auto reader = std::async(std::launch::async, [&] {
            return cursor->Next();
        });
        ASSERT_EQ(reader.wait_for(10s), std::future_status::ready);
        const auto next = reader.get();
        ASSERT_FALSE(next);
        EXPECT_EQ(next.error().code, "CORRUPTION");
    };
    const auto outcome = AppendSecond();
    EXPECT_EQ(outcome.disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(outcome.error);
    EXPECT_EQ(outcome.error->code, "RECOVERY_REQUIRED");

    Hooks_->BeforeVerifyClosure = nullptr;
    WriteBytes(dataFile, saved);
    ASSERT_TRUE(Engine_->Reconcile());
    EXPECT_FALSE(Engine_->GetCollection(SecondCollection)->current_snapshot_id);
}

// Corruption observed after the guard decision but before the outer SQLite
// COMMIT is surfaced only after that publication completes.
TEST_F(EngineIntegrityTest, CorruptionAfterGuardBeforeCommitSurfacesAfterPublication)
{
    CreateTwoCollections();
    CommitFirst();
    const auto cursor = OpenCursor(test::collection_id);
    ASSERT_NE(cursor, nullptr);
    const auto dataFile = OnlyDataFile(test::collection_id);
    const auto saved = ReadBytes(dataFile);

    std::future<Result<std::shared_ptr<arrow::RecordBatch>>> reader;
    bool blockedUntilCommit = false;
    Hooks_->AfterPublish = [&](std::string_view collection) {
        if (collection != SecondCollection) {
            return;
        }
        std::filesystem::remove(dataFile);
        reader = std::async(std::launch::async, [&] {
            return cursor->Next();
        });
        blockedUntilCommit = reader.wait_for(300ms) == std::future_status::timeout;
    };
    const auto outcome = AppendSecond();
    ASSERT_EQ(outcome.disposition, CommitDisposition::Committed)
        << (outcome.error ? outcome.error->code + ": " + outcome.error->message : std::string());
    EXPECT_TRUE(blockedUntilCommit);
    ASSERT_TRUE(reader.valid());
    const auto next = reader.get();
    ASSERT_FALSE(next);
    EXPECT_EQ(next.error().code, "CORRUPTION");
    const auto blocked = Engine_->GetCollection(SecondCollection);
    ASSERT_FALSE(blocked);
    EXPECT_EQ(blocked.error().code, "RECOVERY_REQUIRED");

    Hooks_->AfterPublish = nullptr;
    WriteBytes(dataFile, saved);
    ASSERT_TRUE(Engine_->Reconcile());
    EXPECT_EQ(Engine_->GetCollection(SecondCollection)->current_snapshot_id, outcome.result->snapshot_id);
    const auto lookup = Engine_->FindAppendOutcome(SecondCollection, SecondOperation);
    ASSERT_TRUE(lookup);
    ASSERT_TRUE(lookup->has_value());
    EXPECT_EQ((*lookup)->disposition, CommitDisposition::Committed);
}

// ---- Fail-closed classification of warehouse layout violations ----

TEST_F(EngineIntegrityTest, SymlinkInWarehouseFailsClosedBeforeBinding)
{
    CreateTwoCollections();
    CommitFirst();
    const auto before = CountRegularFiles(Root_ / "warehouse");
    const auto link = Warehouse(test::collection_id, "data") / "planted-link";
    std::filesystem::create_symlink(Temporary_.path, link);

    const auto rejected = AppendSecond();
    EXPECT_EQ(rejected.disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(rejected.error);
    EXPECT_EQ(rejected.error->code, "CORRUPTION");
    EXPECT_EQ(CountRegularFiles(Root_ / "warehouse"), before);
    EXPECT_EQ(Engine_->ListCollections().error().code, "RECOVERY_REQUIRED");

    std::filesystem::remove(link);
    ASSERT_TRUE(Engine_->Reconcile());
    const auto lookup = Engine_->FindAppendOutcome(SecondCollection, SecondOperation);
    ASSERT_TRUE(lookup);
    EXPECT_FALSE(lookup->has_value());
    EXPECT_EQ(AppendSecond().disposition, CommitDisposition::Committed);
}

TEST_F(EngineIntegrityTest, SymlinkReplacingPublishedParquetIsCorruption)
{
    CreateTwoCollections();
    CommitFirst();
    const auto dataFile = OnlyDataFile(test::collection_id);
    const auto copy = Temporary_.path / "copy.parquet";
    std::filesystem::copy_file(dataFile, copy);
    std::filesystem::remove(dataFile);
    std::filesystem::create_symlink(copy, dataFile);

    const auto cursor = OpenCursor(test::collection_id);
    ASSERT_NE(cursor, nullptr);
    const auto next = cursor->Next();
    ASSERT_FALSE(next);
    EXPECT_EQ(next.error().code, "CORRUPTION");
    EXPECT_EQ(Engine_->ListCollections().error().code, "RECOVERY_REQUIRED");

    std::filesystem::remove(dataFile);
    std::filesystem::copy_file(copy, dataFile);
    ASSERT_TRUE(Engine_->Reconcile());
}

TEST_F(EngineIntegrityTest, FifoReplacingManifestIsCorruption)
{
    CreateTwoCollections();
    CommitFirst();
    const auto manifests = FilesIn(Warehouse(test::collection_id, "metadata"), [](const auto& path) {
        return path.extension() == ".avro" && !path.filename().string().starts_with("snap-");
    });
    ASSERT_EQ(manifests.size(), 1U);
    const auto saved = ReadBytes(manifests.front());
    std::filesystem::remove(manifests.front());
    ASSERT_EQ(::mkfifo(manifests.front().c_str(), 0600), 0);

    const auto scan = Engine_->Scan({.collection_id = test::collection_id});
    ASSERT_FALSE(scan);
    EXPECT_EQ(scan.error().code, "CORRUPTION");
    EXPECT_EQ(Engine_->ListCollections().error().code, "RECOVERY_REQUIRED");

    std::filesystem::remove(manifests.front());
    WriteBytes(manifests.front(), saved);
    ASSERT_TRUE(Engine_->Reconcile());
}

// ---- Idempotent retries verify the full retained closure ----

TEST_F(EngineIntegrityTest, RetryAfterDeletedParquetFailsClosed)
{
    CreateTwoCollections();
    CommitFirst();
    const auto original = Engine_->FindAppendOutcome(test::collection_id, FirstOperation);
    const auto dataFile = OnlyDataFile(test::collection_id);
    const auto saved = ReadBytes(dataFile);
    std::filesystem::remove(dataFile);

    const auto retry = Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}});
    EXPECT_EQ(retry.disposition, CommitDisposition::CommitUnknown);
    ASSERT_TRUE(retry.error);
    EXPECT_EQ(retry.error->code, "CORRUPTION");
    EXPECT_EQ(Engine_->GetCollection(test::collection_id).error().code, "RECOVERY_REQUIRED");

    WriteBytes(dataFile, saved);
    ASSERT_TRUE(Engine_->Reconcile());
    const auto again = Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}});
    ASSERT_EQ(again.disposition, CommitDisposition::Committed);
    EXPECT_EQ(again.result->snapshot_id, (*original)->result->snapshot_id);
}

TEST_F(EngineIntegrityTest, RetryAfterSameLengthGarbageRootFailsClosed)
{
    CreateTwoCollections();
    CommitFirst();
    const auto roots = FilesIn(Warehouse(test::collection_id, "metadata"), [](const auto& path) {
        return path.filename().string().starts_with("00001-");
    });
    ASSERT_EQ(roots.size(), 1U);
    const auto saved = ReadBytes(roots.front());
    WriteBytes(roots.front(), std::string(saved.size(), 'x'));

    const auto retry = Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}});
    EXPECT_EQ(retry.disposition, CommitDisposition::CommitUnknown);
    ASSERT_TRUE(retry.error);
    EXPECT_EQ(retry.error->code, "CORRUPTION");

    WriteBytes(roots.front(), saved);
    ASSERT_TRUE(Engine_->Reconcile());
    EXPECT_EQ(Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}}).disposition,
        CommitDisposition::Committed);
}

TEST_F(EngineIntegrityTest, CreateRetryAfterCorruptManifestFailsClosed)
{
    CreateTwoCollections();
    CommitFirst();
    const auto manifests = FilesIn(Warehouse(test::collection_id, "metadata"), [](const auto& path) {
        return path.extension() == ".avro" && !path.filename().string().starts_with("snap-");
    });
    ASSERT_EQ(manifests.size(), 1U);
    const auto saved = ReadBytes(manifests.front());
    auto corrupted = saved;
    corrupted[0] = 'X';
    WriteBytes(manifests.front(), corrupted);

    const auto retry = Engine_->CreateCollection({test::collection_id, "events-a", Schema});
    EXPECT_EQ(retry.disposition, CommitDisposition::CommitUnknown);
    ASSERT_TRUE(retry.error);
    EXPECT_EQ(retry.error->code, "CORRUPTION");

    WriteBytes(manifests.front(), saved);
    ASSERT_TRUE(Engine_->Reconcile());
    EXPECT_EQ(Engine_->CreateCollection({test::collection_id, "events-a", Schema}).disposition,
        CommitDisposition::Committed);
}

// ---- Strict Parquet schema and decoded values ----

// Rows with long keys, so a crafted replacement with short keys fits in the
// committed file length.
std::vector<TInputRow> LongKeyRows()
{
    std::vector<TInputRow> rows;
    for (int index = 0; index < 16; ++index) {
        std::string key(1000, 'a');
        for (size_t at = 0; at < key.size(); ++at) {
            key[at] = static_cast<char>('a' + (at * 7 + static_cast<size_t>(index) * 13 + at * at) % 26);
        }
        rows.push_back({key, index, 0.25 * index, index % 2 == 0, 1000 + index});
    }
    return rows;
}

std::shared_ptr<arrow::Table> ReadParquet(const std::string& bytes)
{
    auto source = std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(bytes));
    auto reader = parquet::arrow::OpenFile(source, arrow::default_memory_pool());
    EXPECT_TRUE(reader.ok()) << reader.status().ToString();
    auto table = (*reader)->ReadTable();
    EXPECT_TRUE(table.ok()) << table.status().ToString();
    return table.ok() ? *table : nullptr;
}

// Writes `table` uncompressed and pads it before the footer to `length` bytes.
// Footer offsets are absolute and point before the padding; readers locate
// the footer from the end, so the file stays valid at the manifest length.
std::string SameLengthParquet(const arrow::Table& table, size_t length)
{
    auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
    const auto properties = parquet::WriterProperties::Builder()
                                .compression(parquet::Compression::UNCOMPRESSED)
                                ->disable_statistics()
                                ->build();
    EXPECT_TRUE(parquet::arrow::WriteTable(table, arrow::default_memory_pool(), sink, 1024, properties).ok());
    auto bytes = sink->Finish().ValueOrDie()->ToString();
    EXPECT_LE(bytes.size(), length);
    uint32_t footer = 0;
    std::memcpy(&footer, bytes.data() + bytes.size() - 8, 4);
    const auto footerStart = bytes.size() - 8 - footer;
    bytes.insert(footerStart, length - bytes.size(), '\0');
    return bytes;
}

std::shared_ptr<arrow::Table> ReplaceColumn(const arrow::Table& table, int index, std::shared_ptr<arrow::Array> array)
{
    auto replaced = table.SetColumn(index, table.schema()->field(index), std::make_shared<arrow::ChunkedArray>(array));
    EXPECT_TRUE(replaced.ok()) << replaced.status().ToString();
    return *replaced;
}

std::vector<std::string> ShortKeys(int64_t count)
{
    std::vector<std::string> keys;
    for (int64_t index = 0; index < count; ++index) {
        keys.push_back("k" + std::to_string(index));
    }
    return keys;
}

std::shared_ptr<arrow::Array> Utf8Keys(const std::vector<std::string>& keys)
{
    arrow::BinaryBuilder builder;
    for (const auto& key : keys) {
        EXPECT_TRUE(builder.Append(key).ok());
    }
    std::shared_ptr<arrow::Array> binary;
    EXPECT_TRUE(builder.Finish(&binary).ok());
    auto data = binary->data()->Copy();
    data->type = arrow::utf8();
    return arrow::MakeArray(data);
}

class EngineParquetIntegrityTest : public EngineIntegrityTest
{
protected:
    std::filesystem::path DataFile_;
    std::string Original_;
    std::shared_ptr<arrow::Table> Table_;

    void SetUp() override
    {
        CreateTwoCollections();
        ASSERT_EQ(Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(LongKeyRows())}}).disposition,
            CommitDisposition::Committed);
        DataFile_ = OnlyDataFile(test::collection_id);
        Original_ = ReadBytes(DataFile_);
        Table_ = ReadParquet(Original_);
        ASSERT_NE(Table_, nullptr);
        Table_ = ReplaceColumn(*Table_, 0, Utf8Keys(ShortKeys(Table_->num_rows())));
    }

    void ExpectScanCorruption(const arrow::Table& crafted)
    {
        WriteBytes(DataFile_, SameLengthParquet(crafted, Original_.size()));
        const auto cursor = OpenCursor(test::collection_id);
        ASSERT_NE(cursor, nullptr);
        const auto next = cursor->Next();
        ASSERT_FALSE(next);
        EXPECT_EQ(next.error().code, "CORRUPTION") << next.error().message;
        EXPECT_EQ(Engine_->ListCollections().error().code, "RECOVERY_REQUIRED");
        const auto sticky = cursor->Next();
        ASSERT_FALSE(sticky);
        EXPECT_EQ(sticky.error().code, "CORRUPTION");
    }
};

TEST_F(EngineParquetIntegrityTest, CraftedReplacementWithValidValuesScans)
{
    // Control: the same-length technique itself yields a readable file.
    WriteBytes(DataFile_, SameLengthParquet(*Table_, Original_.size()));
    const auto cursor = OpenCursor(test::collection_id);
    ASSERT_NE(cursor, nullptr);
    int64_t rows = 0;
    while (true) {
        const auto batch = cursor->Next();
        ASSERT_TRUE(batch) << batch.error().message;
        if (!*batch) {
            break;
        }
        rows += (*batch)->num_rows();
    }
    EXPECT_EQ(rows, 16);
}

TEST_F(EngineParquetIntegrityTest, ParquetWithoutFieldIdsIsCorruption)
{
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (const auto& field : Table_->schema()->fields()) {
        fields.push_back(field->RemoveMetadata());
    }
    ExpectScanCorruption(*arrow::Table::Make(arrow::schema(fields), Table_->columns()));
    // Recovery checks the same footer.
    const auto reconciled = Engine_->Reconcile();
    ASSERT_FALSE(reconciled);
    EXPECT_EQ(reconciled.error().code, "CORRUPTION");
}

TEST_F(EngineParquetIntegrityTest, InvalidUtf8IsCorruption)
{
    auto keys = ShortKeys(Table_->num_rows());
    keys[3] = std::string("\xFF\xFE", 2);
    ExpectScanCorruption(*ReplaceColumn(*Table_, 0, Utf8Keys(keys)));
}

TEST_F(EngineParquetIntegrityTest, NonFiniteAndNegativeZeroFloatsAreCorruption)
{
    for (const double bad : {std::numeric_limits<double>::quiet_NaN(), std::numeric_limits<double>::infinity(), -0.0}) {
        arrow::DoubleBuilder builder;
        for (int64_t row = 0; row < Table_->num_rows(); ++row) {
            EXPECT_TRUE(builder.Append(row == 5 ? bad : 1.0).ok());
        }
        std::shared_ptr<arrow::Array> scores;
        EXPECT_TRUE(builder.Finish(&scores).ok());
        ExpectScanCorruption(*ReplaceColumn(*Table_, 2, scores));
        WriteBytes(DataFile_, Original_);
        ASSERT_TRUE(Engine_->Reconcile());
    }
}

TEST_F(EngineParquetIntegrityTest, OutOfRangeTimestampIsCorruption)
{
    arrow::TimestampBuilder builder(arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"), arrow::default_memory_pool());
    for (int64_t row = 0; row < Table_->num_rows(); ++row) {
        EXPECT_TRUE(builder.Append(row == 7 ? 253402300800000000LL : 1).ok());
    }
    std::shared_ptr<arrow::Array> times;
    EXPECT_TRUE(builder.Finish(&times).ok());
    ExpectScanCorruption(*ReplaceColumn(*Table_, 4, times));
}

TEST_F(EngineIntegrityTest, MultiRowGroupLimitStopsMidBatch)
{
    detail::TLocalStorageOptions options;
    // Fixed slots for this schema: 1 + 4 + key, 1 + 8, 1 + 8, 1 + 1, 1 + 8.
    // Three six-byte-key rows per row group.
    options.Writer.MaxRowGroupBytes = 4 + 3 * 40;
    CreateTwoCollections(options);
    std::vector<TInputRow> rows;
    for (int index = 0; index < 9; ++index) {
        rows.push_back({"key-0" + std::to_string(index), index, 1.0, true, index});
    }
    ASSERT_EQ(Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(rows)}}).disposition,
        CommitDisposition::Committed);
    const auto table = ReadParquet(ReadBytes(OnlyDataFile(test::collection_id)));
    ASSERT_NE(table, nullptr);
    auto source = std::make_shared<arrow::io::BufferReader>(
        arrow::Buffer::FromString(ReadBytes(OnlyDataFile(test::collection_id))));
    auto reader = parquet::arrow::OpenFile(source, arrow::default_memory_pool());
    ASSERT_TRUE(reader.ok());
    ASSERT_EQ((*reader)->num_row_groups(), 3);

    auto cursor = Engine_->Scan({.collection_id = test::collection_id,
        .projection = std::vector<std::string>{"key"},
        .limit = 4,
        .batch_rows = 2});
    ASSERT_TRUE(cursor) << cursor.error().message;
    std::vector<std::string> keys;
    while (true) {
        const auto batch = (*cursor)->Next();
        ASSERT_TRUE(batch) << batch.error().message;
        if (!*batch) {
            break;
        }
        EXPECT_LE((*batch)->num_rows(), 2);
        const auto rendered = Render(**batch);
        keys.insert(keys.end(), rendered.begin(), rendered.end());
    }
    EXPECT_EQ(keys, (std::vector<std::string>{"key-00", "key-01", "key-02", "key-03"}));
}

// ---- Capacity before and after binding ----

// The admission check predicts the next root's metadata log exactly: appends
// do not grow it, so a zero-entry limit still admits every append.
TEST_F(EngineIntegrityTest, MetadataHistoryDoesNotGrowWithAppends)
{
    detail::TLocalStorageOptions options;
    options.Storage.MetadataLogEntries = 0;
    CreateTwoCollections(options);
    CommitFirst();
    EXPECT_EQ(Engine_->AppendDurable({test::collection_id, SecondOperation, {}, {Batch(SecondRows)}}).disposition,
        CommitDisposition::Committed);
    EXPECT_EQ(Engine_->AppendDurable({test::collection_id, ThirdOperation, {}, {Batch(SecondRows)}}).disposition,
        CommitDisposition::Committed);
}

TEST_F(EngineIntegrityTest, RootByteQuotaRejectsBeforeBinding)
{
    CreateTwoCollections();
    CommitFirst();
    const auto roots = FilesIn(Warehouse(test::collection_id, "metadata"), [](const auto& path) {
        return path.filename().string().starts_with("00001-");
    });
    ASSERT_EQ(roots.size(), 1U);
    ASSERT_TRUE(Engine_->Shutdown(1000ms));
    Engine_.reset();

    detail::TLocalStorageOptions options;
    options.Storage.RootBytes = std::filesystem::file_size(roots.front());
    auto opened =
        detail::EngineFactory::OpenWith({Root_.string(), test::store_id}, THookBackend::Factory(Hooks_, options));
    ASSERT_TRUE(opened) << opened.error().message;
    Engine_ = std::move(*opened);
    const auto before = CountRegularFiles(Root_ / "warehouse");
    const auto rejected = Engine_->AppendDurable({test::collection_id, SecondOperation, {}, {Batch(SecondRows)}});
    EXPECT_EQ(rejected.disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(rejected.error);
    EXPECT_EQ(rejected.error->code, "RESOURCE_EXHAUSTED");
    EXPECT_EQ(CountRegularFiles(Root_ / "warehouse"), before);
    const auto lookup = Engine_->FindAppendOutcome(test::collection_id, SecondOperation);
    ASSERT_TRUE(lookup);
    EXPECT_FALSE(lookup->has_value());
}

TEST_F(EngineIntegrityTest, WriterFileLimitAfterBindingIsResourceExhausted)
{
    detail::TLocalStorageOptions options;
    options.Writer.MaxFileBytes = 64;
    CreateTwoCollections(options);
    const auto failed = Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}});
    EXPECT_EQ(failed.disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(failed.error);
    EXPECT_EQ(failed.error->code, "RESOURCE_EXHAUSTED") << failed.error->message;
    EXPECT_EQ(failed.error->retry_hint, RetryHint::OperatorAction);
    const auto recorded = Engine_->FindAppendOutcome(test::collection_id, FirstOperation);
    ASSERT_TRUE(recorded) << recorded.error().message;
    ASSERT_TRUE(recorded->has_value());
    EXPECT_EQ((*recorded)->error->code, "RESOURCE_EXHAUSTED");
    EXPECT_EQ((*recorded)->error->retry_hint, RetryHint::OperatorAction);
    EXPECT_TRUE(Engine_->ListCollections());
}

// ---- Write latch ----

TEST_F(EngineIntegrityTest, LatchedWriteFailureAllowsReconcileShutdownAndReopen)
{
    NGlamDB::NIO::TPosixLocalFS posix;
    auto faults = std::make_shared<NGlamDB::NTesting::TFaultFS>(posix);
    faults->AddRule({NGlamDB::NTesting::EIOOp::SyncFile,
        "*.parquet",
        1,
        1,
        std::make_error_code(std::errc::io_error),
        std::nullopt});
    detail::TLocalStorageOptions options;
    options.Fs = faults;
    CreateTwoCollections(options);

    const auto failed = Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}});
    EXPECT_EQ(failed.disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(failed.error);
    EXPECT_EQ(failed.error->code, "IO_ERROR");

    const auto latched = Engine_->AppendDurable({test::collection_id, ThirdOperation, {}, {Batch(FirstRows)}});
    EXPECT_EQ(latched.disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(latched.error);
    EXPECT_EQ(latched.error->code, "IO_ERROR");
    EXPECT_EQ(latched.error->retry_hint, RetryHint::OperatorAction);
    const auto unbound = Engine_->FindAppendOutcome(test::collection_id, ThirdOperation);
    ASSERT_TRUE(unbound);
    EXPECT_FALSE(unbound->has_value());

    ASSERT_TRUE(Engine_->Reconcile());
    EXPECT_NE(OpenCursor(test::collection_id), nullptr);
    ASSERT_TRUE(Engine_->Shutdown(1000ms));
    Engine_.reset();

    auto reopened = detail::EngineFactory::OpenWith({Root_.string(), test::store_id}, THookBackend::Factory(Hooks_));
    ASSERT_TRUE(reopened) << reopened.error().message;
    Engine_ = std::move(*reopened);
    EXPECT_EQ(Engine_->AppendDurable({test::collection_id, ThirdOperation, {}, {Batch(FirstRows)}}).disposition,
        CommitDisposition::Committed);
}

} // namespace
} // namespace glamdb
