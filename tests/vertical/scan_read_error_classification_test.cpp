// Verified scans reuse membership; metadata APIs still read authoritative files.
// Environmental read failures preserve their classification and readiness.

#include "tests/support/fault_fs.h"
#include "tests/vertical/vertical_support.h"

#include <chrono>
#include <system_error>

namespace glamdb {
namespace {

using namespace std::chrono_literals;
using namespace glamdb::vertical;
using NGlamDB::NTesting::EIOOp;
using NGlamDB::NTesting::TFaultFS;

class ScanReadErrorClassificationTest : public ::testing::Test
{
protected:
    test::TempDir Temporary_;
    std::filesystem::path Root_ = Temporary_.path / "store";
    // Declared before the engine: the fault FS wraps it and the backend keeps the
    // fault FS alive until the engine is released in TearDown.
    NGlamDB::NIO::TPosixLocalFS Posix_;
    std::shared_ptr<TFaultFS> Faults_ = std::make_shared<TFaultFS>(Posix_);
    std::shared_ptr<detail::TLocalStorageBackend> Backend_;
    std::shared_ptr<Engine> Engine_;

    void SetUp() override
    {
        const auto faults = Faults_;
        const detail::BackendFactory factory =
            [this, faults](const std::filesystem::path& path,
                const StoreInfo& info) -> Result<std::shared_ptr<detail::StorageBackend>> {
            detail::TLocalStorageOptions options;
            options.Fs = faults;
            auto backend = detail::TLocalStorageBackend::Open(path, info, options);
            if (!backend) {
                return std::unexpected(backend.error());
            }
            Backend_ = *backend;
            return std::shared_ptr<detail::StorageBackend>(std::move(*backend));
        };
        auto created = detail::EngineFactory::CreateWith({test::store_id, Root_.string()}, factory);
        ASSERT_TRUE(created) << created.error().message;
        Engine_ = std::move(*created);
        ASSERT_EQ(Engine_->CreateCollection({test::collection_id, "events", Schema}).disposition,
            CommitDisposition::Committed);
        const auto appended = Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}});
        ASSERT_EQ(appended.disposition, CommitDisposition::Committed)
            << (appended.error ? appended.error->code + ": " + appended.error->message : std::string());
    }

    void TearDown() override
    {
        if (Engine_) {
            EXPECT_TRUE(Engine_->Shutdown(1000ms));
            Engine_.reset();
        }
        Backend_.reset();
    }

    int64_t CountRows()
    {
        const auto cursor = Engine_->Scan({.collection_id = test::collection_id});
        EXPECT_TRUE(cursor) << (cursor ? std::string() : cursor.error().code + ": " + cursor.error().message);
        if (!cursor) {
            return -1;
        }
        int64_t rows = 0;
        while (true) {
            const auto batch = (*cursor)->Next();
            EXPECT_TRUE(batch) << (batch ? std::string() : batch.error().code + ": " + batch.error().message);
            if (!batch || !*batch) {
                break;
            }
            rows += (*batch)->num_rows();
        }
        return rows;
    }
};

TEST_F(ScanReadErrorClassificationTest, VerifiedScansDoNotRereadRoots)
{
    for (const auto error : {std::errc::io_error, std::errc::too_many_files_open, std::errc::not_enough_memory}) {
        SCOPED_TRACE(std::make_error_code(error).message());
        Faults_->AddRule({EIOOp::OpenRead, "*.metadata.json", 1, 1, std::make_error_code(error), std::nullopt});
        EXPECT_EQ(CountRows(), static_cast<int64_t>(FirstRows.size()));
        // The metadata API still reads the authoritative root and consumes the fault.
        const auto history = Engine_->ListSnapshots(test::collection_id);
        ASSERT_FALSE(history);
        EXPECT_NE(history.error().code, "CORRUPTION");
        EXPECT_TRUE(Engine_->GetHealth().read_available);
        EXPECT_TRUE(Engine_->ListSnapshots(test::collection_id));
    }
}

TEST_F(ScanReadErrorClassificationTest, VerifiedScansDoNotRereadManifests)
{
    Faults_->AddRule({EIOOp::PRead, "*-m[0-9].avro", 1, 1, std::make_error_code(std::errc::io_error), std::nullopt});
    EXPECT_EQ(CountRows(), static_cast<int64_t>(FirstRows.size()));
    const auto statistics = Engine_->GetStatistics(test::collection_id);
    ASSERT_FALSE(statistics);
    EXPECT_EQ(statistics.error().code, "IO_ERROR");
    EXPECT_TRUE(Engine_->GetHealth().read_available);
    EXPECT_TRUE(Engine_->GetStatistics(test::collection_id));
}

TEST_F(ScanReadErrorClassificationTest, MetadataFailuresPreserveClassificationAndExplainReadsNoData)
{
    for (const int method : {0, 1, 2}) {
        SCOPED_TRACE(method);
        Faults_->AddRule(
            {EIOOp::OpenRead, "*.metadata.json", 1, 1, std::make_error_code(std::errc::io_error), std::nullopt});
        const auto failure = method == 0 ? Status(Engine_->GetStatistics(test::collection_id).transform([](auto) {
        }))
            : method == 1                ? Status(Engine_->ListSnapshots(test::collection_id).transform([](auto) {
              }))
                          : Status(Engine_->ExplainScan({.collection_id = test::collection_id}).transform([](auto) {
                            }));
        ASSERT_FALSE(failure);
        EXPECT_EQ(failure.error().code, "IO_ERROR");
        EXPECT_TRUE(Engine_->GetHealth().read_available);
    }
    // If explain or statistics opens Parquet, this persistent fault rejects it.
    Faults_->AddRule({EIOOp::OpenRead, "*.parquet", 1, 100, std::make_error_code(std::errc::io_error), std::nullopt});
    EXPECT_TRUE(Engine_->GetStatistics(test::collection_id));
    EXPECT_TRUE(Engine_->ExplainScan({.collection_id = test::collection_id}));
    const auto cursor = Engine_->Scan({.collection_id = test::collection_id});
    ASSERT_TRUE(cursor);
    const auto failed = (*cursor)->Next();
    ASSERT_FALSE(failed);
    EXPECT_EQ(failed.error().code, "IO_ERROR");
    EXPECT_EQ((*cursor)->Profile().state, ScanState::Failed);
    EXPECT_EQ((*cursor)->Profile().data_files_read.value, 0);
    EXPECT_EQ((*cursor)->Profile().rows_returned.value, 0);
}

TEST_F(ScanReadErrorClassificationTest, MissingMetadataClosesReadiness)
{
    Faults_->AddRule({EIOOp::OpenRead,
        "*.metadata.json",
        1,
        1,
        std::make_error_code(std::errc::no_such_file_or_directory),
        std::nullopt});
    const auto result = Engine_->ListSnapshots(test::collection_id);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, "CORRUPTION");
    EXPECT_FALSE(Engine_->GetHealth().read_available);
    const auto blocked = Engine_->GetStatistics(test::collection_id);
    ASSERT_FALSE(blocked);
    EXPECT_EQ(blocked.error().code, "RECOVERY_REQUIRED");
    ASSERT_TRUE(Engine_->Reconcile());
    EXPECT_TRUE(Engine_->GetStatistics(test::collection_id));
}

TEST_F(ScanReadErrorClassificationTest, FileDetailsClassifyFailuresAndReadOnlyFooterRanges)
{
    const auto listing = Engine_->ListFiles(test::collection_id);
    ASSERT_TRUE(listing) << listing.error().message;
    ASSERT_EQ(listing->Files.size(), 1U);
    const auto& file = listing->Files.front();
    const auto firstEvent = Faults_->Events().size();
    const auto details = Engine_->GetFileDetails(test::collection_id, file.FileID);
    ASSERT_TRUE(details) << details.error().message;
    const auto events = Faults_->Events();
    uint64_t dataBytes = 0;
    for (size_t index = firstEvent; index < events.size(); ++index) {
        if (events[index].Op == EIOOp::PRead && events[index].Path.ends_with(".parquet")) {
            dataBytes += events[index].Bytes;
        }
    }
    EXPECT_GT(dataBytes, 12U);
    EXPECT_LT(dataBytes, static_cast<uint64_t>(file.CompressedBytes));

    for (const auto operation : {EIOOp::OpenRead, EIOOp::PRead, EIOOp::Close}) {
        Faults_->AddRule({operation, "*.parquet", 1, 1, std::make_error_code(std::errc::io_error), std::nullopt});
        const auto failed = Engine_->GetFileDetails(test::collection_id, file.FileID);
        ASSERT_FALSE(failed);
        EXPECT_EQ(failed.error().code, "IO_ERROR");
        EXPECT_TRUE(Engine_->GetHealth().read_available);
        EXPECT_TRUE(Engine_->GetFileDetails(test::collection_id, file.FileID));
    }
    Faults_->AddRule({EIOOp::OpenRead, "*.parquet", 1, 100, std::make_error_code(std::errc::io_error), std::nullopt});
    EXPECT_TRUE(Engine_->ListFiles(test::collection_id));
    const auto rejected = Engine_->GetFileDetails(test::collection_id, std::string(64, '0'));
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error().code, "NOT_FOUND");
}

TEST_F(ScanReadErrorClassificationTest, MissingSelectedDataFileClosesReadiness)
{
    const auto listing = Engine_->ListFiles(test::collection_id);
    ASSERT_TRUE(listing);
    ASSERT_EQ(listing->Files.size(), 1U);
    Faults_->AddRule(
        {EIOOp::OpenRead, "*.parquet", 1, 1, std::make_error_code(std::errc::no_such_file_or_directory), std::nullopt});
    const auto failed = Engine_->GetFileDetails(test::collection_id, listing->Files.front().FileID);
    ASSERT_FALSE(failed);
    EXPECT_EQ(failed.error().code, "CORRUPTION");
    EXPECT_FALSE(Engine_->GetHealth().read_available);
    ASSERT_TRUE(Engine_->Reconcile());
}

TEST_F(ScanReadErrorClassificationTest, PositionalParquetScanReadsNeededRangesAndPreservesTransientErrors)
{
    const auto listing = Engine_->ListFiles(test::collection_id);
    ASSERT_TRUE(listing);
    ASSERT_EQ(listing->Files.size(), 1U);
    const auto fileSize = static_cast<uint64_t>(listing->Files.front().CompressedBytes);

    const auto startEvent = Faults_->Events().size();
    const auto projected =
        Engine_->Scan({.collection_id = test::collection_id, .projection = std::vector<std::string>{"key"}});
    ASSERT_TRUE(projected);
    int64_t rows = 0;
    while (true) {
        const auto batch = (*projected)->Next();
        ASSERT_TRUE(batch) << batch.error().message;
        if (!*batch) {
            break;
        }
        rows += (*batch)->num_rows();
    }
    EXPECT_EQ(rows, static_cast<int64_t>(FirstRows.size()));
    uint64_t preadBytes = 0;
    const auto events = Faults_->Events();
    for (size_t index = startEvent; index < events.size(); ++index) {
        if (events[index].Op == EIOOp::PRead && events[index].Path.ends_with(".parquet")) {
            preadBytes += events[index].Bytes;
        }
    }
    EXPECT_GT(preadBytes, 12U);
    EXPECT_LT(preadBytes, fileSize);
    EXPECT_EQ((*projected)->Profile().data_bytes_read.value, static_cast<int64_t>(preadBytes));

    // Inject transient PRead failures at the 1st (header), 3rd (footer), and 4th (column page) reads,
    // plus a transient Close failure after reading all batches. None may be misclassified as CORRUPTION.
    for (const uint32_t nthRead : {1U, 3U, 4U}) {
        for (const auto& [err, expected] :
            {std::pair{std::errc::io_error, "IO_ERROR"}, std::pair{std::errc::not_enough_memory, "RESOURCE_EXHAUSTED"}})
        {
            SCOPED_TRACE(std::to_string(nthRead) + ":" + expected);
            ASSERT_NE(Backend_, nullptr);
            Backend_->InvalidateCache();
            Faults_->AddRule({EIOOp::PRead, "*.parquet", nthRead, 1, std::make_error_code(err), std::nullopt});
            const auto cursor =
                Engine_->Scan({.collection_id = test::collection_id, .projection = std::vector<std::string>{"key"}});
            ASSERT_TRUE(cursor);
            const auto failed = (*cursor)->Next();
            ASSERT_FALSE(failed);
            EXPECT_EQ(failed.error().code, expected) << failed.error().message;
            EXPECT_TRUE(Engine_->GetHealth().read_available);
            EXPECT_EQ(CountRows(), static_cast<int64_t>(FirstRows.size()));
        }
    }

    Faults_->AddRule({EIOOp::Close, "*.parquet", 1, 1, std::make_error_code(std::errc::io_error), std::nullopt});
    {
        const auto cursor =
            Engine_->Scan({.collection_id = test::collection_id, .projection = std::vector<std::string>{"key"}});
        ASSERT_TRUE(cursor);
        ErrorDetail error{};
        bool failed = false;
        while (true) {
            const auto batch = (*cursor)->Next();
            if (!batch) {
                error = batch.error();
                failed = true;
                break;
            }
            if (!*batch) {
                break;
            }
        }
        ASSERT_TRUE(failed) << "expected Close failure to be surfaced by Next()";
        EXPECT_EQ(error.code, "IO_ERROR") << error.message;
        EXPECT_TRUE(Engine_->GetHealth().read_available);
    }
    EXPECT_EQ(CountRows(), static_cast<int64_t>(FirstRows.size()));
}

TEST_F(ScanReadErrorClassificationTest, LimitCompletionUsesCheckedCloseWhileCancelUsesBestEffort)
{
    // 1. Mid-file limit (.limit = 1 out of 3 rows): first Next() delivers 1 row;
    //    second Next() closes the open Parquet stream via FinishFile() and surfaces IO_ERROR.
    Faults_->AddRule({EIOOp::Close, "*.parquet", 1, 1, std::make_error_code(std::errc::io_error), std::nullopt});
    {
        const auto cursor = Engine_->Scan({
            .collection_id = test::collection_id,
            .projection = std::vector<std::string>{"key"},
            .limit = 1,
        });
        ASSERT_TRUE(cursor);
        const auto first = (*cursor)->Next();
        ASSERT_TRUE(first) << first.error().message;
        ASSERT_NE(*first, nullptr);
        EXPECT_EQ((*first)->num_rows(), 1);

        const auto second = (*cursor)->Next();
        ASSERT_FALSE(second) << "expected Close failure on limit EOS instead of silent EOS";
        EXPECT_EQ(second.error().code, "IO_ERROR") << second.error().message;
        EXPECT_EQ((*cursor)->Profile().state, ScanState::Failed);
        EXPECT_TRUE(Engine_->GetHealth().read_available);

        // Sticky terminal failure on repeated Next().
        const auto third = (*cursor)->Next();
        ASSERT_FALSE(third);
        EXPECT_EQ(third.error().code, "IO_ERROR");
    }

    // 2. Exact end-of-file limit (.limit == FirstRows.size(), reached before reading EOF batch):
    //    next Next() still performs checked close and surfaces IO_ERROR.
    Faults_->AddRule({EIOOp::Close, "*.parquet", 1, 1, std::make_error_code(std::errc::io_error), std::nullopt});
    {
        const auto cursor = Engine_->Scan({
            .collection_id = test::collection_id,
            .projection = std::vector<std::string>{"key"},
            .limit = static_cast<int64_t>(FirstRows.size()),
            .batch_rows = 2,
        });
        ASSERT_TRUE(cursor);
        int64_t got = 0;
        while (got < static_cast<int64_t>(FirstRows.size())) {
            const auto batch = (*cursor)->Next();
            ASSERT_TRUE(batch) << batch.error().message;
            ASSERT_NE(*batch, nullptr);
            got += (*batch)->num_rows();
        }
        EXPECT_EQ(got, static_cast<int64_t>(FirstRows.size()));

        const auto eos = (*cursor)->Next();
        ASSERT_FALSE(eos);
        EXPECT_EQ(eos.error().code, "IO_ERROR") << eos.error().message;
        EXPECT_EQ((*cursor)->Profile().state, ScanState::Failed);
        EXPECT_TRUE(Engine_->GetHealth().read_available);
    }

    // 3. Cancellation after hitting .limit = 1 uses best-effort Abort() and returns CANCELLED.
    Faults_->AddRule({EIOOp::Close, "*.parquet", 1, 1, std::make_error_code(std::errc::io_error), std::nullopt});
    {
        const auto cursor = Engine_->Scan({
            .collection_id = test::collection_id,
            .projection = std::vector<std::string>{"key"},
            .limit = 1,
        });
        ASSERT_TRUE(cursor);
        const auto first = (*cursor)->Next();
        ASSERT_TRUE(first) << first.error().message;
        ASSERT_NE(*first, nullptr);
        EXPECT_EQ((*first)->num_rows(), 1);

        (*cursor)->Cancel();
        const auto cancelled = (*cursor)->Next();
        ASSERT_FALSE(cancelled);
        EXPECT_EQ(cancelled.error().code, "CANCELLED") << cancelled.error().message;
        EXPECT_EQ((*cursor)->Profile().state, ScanState::Cancelled);
        EXPECT_TRUE(Engine_->GetHealth().read_available);
    }

    // 4. Limit reached inside the 2nd data file after normal checked close of the 1st data file.
    const auto secondAppend = Engine_->AppendDurable({test::collection_id, SecondOperation, {}, {Batch(SecondRows)}});
    ASSERT_EQ(secondAppend.disposition, CommitDisposition::Committed);
    Faults_->AddRule({EIOOp::Close, "*.parquet", 2, 1, std::make_error_code(std::errc::io_error), std::nullopt});
    {
        const int64_t targetRows = static_cast<int64_t>(FirstRows.size()) + 1;
        const auto cursor = Engine_->Scan({
            .collection_id = test::collection_id,
            .projection = std::vector<std::string>{"key"},
            .limit = targetRows,
            .batch_rows = 2,
        });
        ASSERT_TRUE(cursor);
        int64_t got = 0;
        while (got < targetRows) {
            const auto batch = (*cursor)->Next();
            ASSERT_TRUE(batch) << batch.error().message;
            ASSERT_NE(*batch, nullptr);
            got += (*batch)->num_rows();
        }
        EXPECT_EQ(got, targetRows);

        const auto eos = (*cursor)->Next();
        ASSERT_FALSE(eos);
        EXPECT_EQ(eos.error().code, "IO_ERROR") << eos.error().message;
        EXPECT_EQ((*cursor)->Profile().state, ScanState::Failed);
        EXPECT_TRUE(Engine_->GetHealth().read_available);
    }

    // 5. .limit = 0 opens no Parquet file and completes cleanly even when OpenRead/Close would fail.
    Faults_->AddRule({EIOOp::OpenRead, "*.parquet", 1, 10, std::make_error_code(std::errc::io_error), std::nullopt});
    Faults_->AddRule({EIOOp::Close, "*.parquet", 1, 10, std::make_error_code(std::errc::io_error), std::nullopt});
    {
        const auto cursor = Engine_->Scan({
            .collection_id = test::collection_id,
            .projection = std::vector<std::string>{"key"},
            .limit = 0,
        });
        ASSERT_TRUE(cursor);
        const auto eos = (*cursor)->Next();
        ASSERT_TRUE(eos) << eos.error().message;
        EXPECT_EQ(*eos, nullptr);
        const auto profile = (*cursor)->Profile();
        EXPECT_EQ(profile.state, ScanState::Complete);
        EXPECT_EQ(profile.data_files_read.value, 0);
        EXPECT_EQ(profile.rows_returned.value, 0);
        EXPECT_EQ(profile.limit_stops.value, 1);
    }
}

} // namespace
} // namespace glamdb
