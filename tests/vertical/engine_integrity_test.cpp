// Engine/storage integrity: concurrent corruption reports, fail-closed
// classification and retry verification over the real storage backend.

#include "tests/vertical/vertical_support.h"

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

} // namespace
} // namespace glamdb
