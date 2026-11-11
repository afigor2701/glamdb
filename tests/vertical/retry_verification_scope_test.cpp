// Healthy terminal receipt lookups use the verified store generation without
// rereading warehouse files or modifying rows.

#include "tests/support/checked.h"
#include "tests/support/fault_fs.h"
#include "tests/vertical/vertical_support.h"

#include <chrono>
#include <format>

namespace glamdb {
namespace {

using namespace std::chrono_literals;
using namespace glamdb::vertical;
namespace NT = NGlamDB::NTesting;

std::string B1Operation(int index)
{
    return std::format("00000000-0000-4000-8000-{:012x}", 0xb100 + index);
}

class BackendRetryScopeTest : public ::testing::Test
{
protected:
    test::TempDir Temporary_;
    std::filesystem::path Root_ = Temporary_.path / "store";
    // Declared before the engine so the pass-through recorder outlives it.
    NGlamDB::NIO::TPosixLocalFS Posix_;
    std::shared_ptr<NT::TFaultFS> Faults_ = std::make_shared<NT::TFaultFS>(Posix_);
    std::shared_ptr<THookBackend> Hooks_;
    std::shared_ptr<Engine> Engine_;

    void SetUp() override
    {
        detail::TLocalStorageOptions options;
        options.Fs = Faults_;
        auto created =
            detail::EngineFactory::CreateWith({test::store_id, Root_.string()}, THookBackend::Factory(Hooks_, options));
        ASSERT_TRUE(created) << created.error().message;
        Engine_ = std::move(*created);
        ASSERT_EQ(Engine_->CreateCollection({test::collection_id, "events-a", Schema}).disposition,
            CommitDisposition::Committed);
        ASSERT_EQ(Engine_->CreateCollection({SecondCollection, "events-b", Schema}).disposition,
            CommitDisposition::Committed);
    }

    void TearDown() override
    {
        if (Engine_) {
            EXPECT_TRUE(Engine_->Shutdown(1000ms));
            Engine_.reset();
        }
    }

    AppendOutcome Append(std::string_view collection,
        const std::string& operation,
        const std::vector<TInputRow>& rows,
        SnapshotPrecondition expected = {})
    {
        return Engine_->AppendDurable({std::string(collection), operation, expected, {Batch(rows)}});
    }

    // Warehouse files opened for reading since event index `mark`.
    std::vector<std::string> OpenedSince(std::size_t mark) const
    {
        const auto events = Faults_->Events();
        std::vector<std::string> paths;
        for (std::size_t index = mark; index < events.size(); ++index) {
            if (events[index].Op == NT::EIOOp::OpenRead && events[index].Path.find("/warehouse/") != std::string::npos)
            {
                paths.push_back(events[index].Path);
            }
        }
        return paths;
    }
};

TEST_F(BackendRetryScopeTest, RetryOfCommittedAppendDoesNotVerifyOtherCollections)
{
    const auto first = Append(test::collection_id, B1Operation(0), FirstRows);
    ASSERT_EQ(first.disposition, CommitDisposition::Committed);
    ASSERT_EQ(Append(SecondCollection, B1Operation(1), SecondRows).disposition, CommitDisposition::Committed);

    const auto mark = Faults_->Events().size();
    const auto retry = Append(test::collection_id, B1Operation(0), FirstRows);
    ASSERT_EQ(retry.disposition, CommitDisposition::Committed);
    ASSERT_TRUE(retry.result && first.result);
    EXPECT_EQ(test::Present(retry.result).snapshot_id, test::Present(first.result).snapshot_id);

    const auto foreign = "/warehouse/" + std::string(SecondCollection) + "/";
    std::size_t foreignReads = 0;
    for (const auto& path : OpenedSince(mark)) {
        foreignReads += path.find(foreign) != std::string::npos ? 1 : 0;
    }
    EXPECT_EQ(foreignReads, 0U);
}

TEST_F(BackendRetryScopeTest, OldReceiptLookupReadsNoWarehouseAsHistoryGrows)
{
    const auto first = Append(test::collection_id, B1Operation(0), FirstRows);
    ASSERT_EQ(first.disposition, CommitDisposition::Committed);
    for (int index = 1; index <= 16; ++index) {
        ASSERT_EQ(Append(test::collection_id, B1Operation(index), SecondRows).disposition,
            CommitDisposition::Committed);
        if (index != 1 && index != 8 && index != 16) {
            continue;
        }
        SCOPED_TRACE(index);
        const auto files = CountRegularFiles(Root_ / "warehouse");
        const auto mark = Faults_->Events().size();
        // Terminal retries ignore a new precondition and return the exact receipt.
        EXPECT_EQ(Append(test::collection_id, B1Operation(0), FirstRows, {SnapshotPreconditionKind::NoSnapshot, 0}),
            first);
        const auto lookup = Engine_->FindAppendOutcome(test::collection_id, B1Operation(0));
        ASSERT_TRUE(lookup);
        ASSERT_TRUE(lookup->has_value());
        EXPECT_EQ(test::Present(*lookup), first);
        EXPECT_EQ(Faults_->Events().size(), mark);
        EXPECT_EQ(CountRegularFiles(Root_ / "warehouse"), files);
    }
    auto cursor = Engine_->Scan({.collection_id = test::collection_id});
    ASSERT_TRUE(cursor);
    int64_t rows = 0;
    while (true) {
        const auto batch = (*cursor)->Next();
        ASSERT_TRUE(batch);
        if (!*batch) {
            break;
        }
        rows += (*batch)->num_rows();
    }
    EXPECT_EQ(rows, 35); // Three initial rows and sixteen two-row appends.
}

TEST_F(BackendRetryScopeTest, RecoveryVerifiesWarehouseBeforeReceiptLookupResumes)
{
    const auto first = Append(test::collection_id, B1Operation(0), FirstRows);
    ASSERT_EQ(first.disposition, CommitDisposition::Committed);
    ASSERT_EQ(Append(test::collection_id, B1Operation(1), SecondRows).disposition, CommitDisposition::Committed);
    const auto files = FilesIn(Root_ / "warehouse" / test::collection_id / "data");
    ASSERT_EQ(files.size(), 2U);
    const auto saved = ReadBytes(files.front());
    std::filesystem::remove(files.front());

    const auto mark = Faults_->Events().size();
    const auto broken = Engine_->Reconcile();
    ASSERT_FALSE(broken);
    EXPECT_EQ(broken.error().code, "CORRUPTION");
    EXPECT_FALSE(OpenedSince(mark).empty());
    EXPECT_FALSE(Engine_->GetHealth().write_ready);
    EXPECT_EQ(Append(test::collection_id, B1Operation(0), FirstRows).disposition, CommitDisposition::CommitUnknown);
    const auto blocked = Engine_->FindAppendOutcome(test::collection_id, B1Operation(0));
    ASSERT_FALSE(blocked);
    EXPECT_EQ(blocked.error().code, "RECOVERY_REQUIRED");

    WriteBytes(files.front(), saved);
    const auto recoveryMark = Faults_->Events().size();
    ASSERT_TRUE(Engine_->Reconcile());
    EXPECT_FALSE(OpenedSince(recoveryMark).empty());
    const auto lookupMark = Faults_->Events().size();
    EXPECT_EQ(Append(test::collection_id, B1Operation(0), FirstRows), first);
    EXPECT_EQ(Faults_->Events().size(), lookupMark);
}

TEST_F(BackendRetryScopeTest, RetryOfFailedAppendReadsNoWarehouseFile)
{
    ASSERT_EQ(Append(test::collection_id, B1Operation(0), FirstRows).disposition, CommitDisposition::Committed);
    ASSERT_EQ(Append(SecondCollection, B1Operation(1), SecondRows).disposition, CommitDisposition::Committed);

    // Bound first, then rejected by the precondition: a permanent failed outcome.
    const SnapshotPrecondition stale{SnapshotPreconditionKind::Equals, 1};
    const auto failed = Append(test::collection_id, B1Operation(2), SecondRows, stale);
    ASSERT_EQ(failed.disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(failed.error);
    ASSERT_EQ(test::Present(failed.error).code, "PRECONDITION_FAILED");

    const auto mark = Faults_->Events().size();
    const auto retry = Append(test::collection_id, B1Operation(2), SecondRows, stale);
    ASSERT_EQ(retry.disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(retry.error);
    EXPECT_EQ(test::Present(retry.error).code, "PRECONDITION_FAILED");
    EXPECT_TRUE(OpenedSince(mark).empty());
}

} // namespace
} // namespace glamdb
