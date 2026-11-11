#include "tests/support/checked.h"
#include "tests/support/journal_fault_vfs.h"
#include "tests/vertical/vertical_support.h"

#include <chrono>

namespace glamdb {
namespace {

using namespace vertical;
using namespace std::chrono_literals;

class BackendPublicationTest : public ::testing::Test
{
protected:
    test::TempDir Temporary_;
    std::filesystem::path Root_ = Temporary_.path / "store";
    std::shared_ptr<detail::TLocalStorageBackend> Backend_;
    std::shared_ptr<detail::DurableSqliteCatalogStore> Catalog_;
    StoreInfo Store_;
    detail::CollectionView View_;
    detail::NormalizedInput Input_;

    void SetUp() override
    {
        auto engine = CreateStore({test::store_id, Root_.string()});
        ASSERT_TRUE(engine);
        ASSERT_EQ((*engine)->CreateCollection({test::collection_id, "events", Schema}).disposition,
            CommitDisposition::Committed);
        Store_ = (*engine)->GetStoreInfo();
        ASSERT_TRUE((*engine)->Shutdown(1000ms));
        engine->reset();
        Catalog_ = detail::DurableSqliteCatalogStore::Open(Root_ / "catalog.sqlite", Store_, {});
        View_ = {Catalog_->Collection(test::collection_id), Catalog_->Base(test::collection_id)};
        auto backend = detail::TLocalStorageBackend::Open(Root_, Store_);
        ASSERT_TRUE(backend);
        Backend_ = *backend;
        ASSERT_TRUE(Backend_->VerifyRetained(Store_, std::span(&View_, 1), {}));
        auto input = detail::NormalizeBatches(Schema, test::collection_id, {Batch(FirstRows)});
        ASSERT_TRUE(input);
        Input_ = std::move(*input);
        ASSERT_TRUE(Backend_->CheckAppendAdmissible(Store_, View_.info, View_.base, Input_));
    }

    detail::BoundOperation Binding() const
    {
        return {test::collection_id,
            FirstOperation,
            Input_.payload_hash,
            1,
            View_.base,
            "00000000-0000-7000-8000-000000000034"};
    }
};

TEST_F(BackendPublicationTest, AdmittedBatchRetainsItsAllocatorAfterBackendRelease)
{
    auto pool = std::make_shared<arrow::ProxyMemoryPool>(arrow::default_memory_pool());
    const std::weak_ptr<arrow::MemoryPool> lifetime = pool;
    detail::TLocalStorageOptions options;
    options.InputPool = pool;
    Backend_.reset();
    auto backend = detail::TLocalStorageBackend::Open(Root_, Store_, options);
    ASSERT_TRUE(backend);
    ASSERT_TRUE((*backend)->VerifyRetained(Store_, std::span(&View_, 1), {}));
    ASSERT_TRUE((*backend)->CheckAppendAdmissible(Store_, View_.info, View_.base, Input_));
    auto batch = Input_.writer_batch;
    ASSERT_NE(batch, nullptr);
    Input_ = {};
    backend->reset();
    options.InputPool.reset();
    pool.reset();
    EXPECT_FALSE(lifetime.expired());
    EXPECT_TRUE(batch->ValidateFull().ok());
    batch.reset();
    EXPECT_TRUE(lifetime.expired());
}

// A caller presenting a tentative root must not authorize its promotion.
TEST_F(BackendPublicationTest, TentativePointerCannotPromoteCandidate)
{
    auto binding = Binding();
    Catalog_->BindOperation(binding);
    auto staged = Backend_->PrepareAppend(Store_, binding, View_.info, Input_, std::chrono::steady_clock::now() + 10s);
    ASSERT_TRUE(staged) << staged.error().code << ": " << staged.error().message;
    auto receipt = (*staged)->Receipt();
    Catalog_->PublishAppend(
        binding,
        receipt,
        [&](std::string_view location) -> Status {
            auto verified = (*staged)->VerifyClosure(location);
            if (!verified) {
                return verified;
            }
            auto tentative = View_.base;
            tentative.metadata_location = location;
            tentative.current_snapshot_id = receipt.snapshot_id;
            auto info = View_.info;
            info.current_snapshot_id = receipt.snapshot_id;
            const auto admitted = Backend_->CheckAppendAdmissible(Store_, info, tentative, Input_);
            EXPECT_FALSE(admitted) << "tentative candidate was accepted before outer COMMIT";
            if (!admitted) {
                EXPECT_EQ(admitted.error().code, "RECOVERY_REQUIRED");
            }
            return {};
        },
        [&] {
            return (*staged)->Publish(*Catalog_);
        });
}

TEST_F(BackendPublicationTest, MetadataOnlyBaseChangeRequiresVerification)
{
    auto changed = View_.base;
    const auto original = Root_ / "warehouse" / test::collection_id / "metadata";
    auto files = FilesIn(original);
    ASSERT_EQ(files.size(), 1U);
    const auto replacement = original / (std::string("00000-") + SecondOperation + ".metadata.json");
    std::filesystem::copy_file(files.front(), replacement);
    changed.metadata_location =
        changed.metadata_location.substr(0, changed.metadata_location.rfind('/') + 1) + replacement.filename().string();
    const auto admitted = Backend_->CheckAppendAdmissible(Store_, View_.info, changed, Input_);
    EXPECT_FALSE(admitted) << "metadata-only change silently replaced the verified base";
    if (!admitted) {
        EXPECT_EQ(admitted.error().code, "RECOVERY_REQUIRED");
    }
}

TEST_F(BackendPublicationTest, MismatchedConfirmationInvalidatesGeneration)
{
    Backend_->PublicationFinished(test::collection_id, "unexpected-root", CommitDisposition::Committed);
    const auto admitted = Backend_->CheckAppendAdmissible(Store_, View_.info, View_.base, Input_);
    EXPECT_FALSE(admitted) << "confirmation without its exact candidate left a usable generation";
    if (!admitted) {
        EXPECT_EQ(admitted.error().code, "RECOVERY_REQUIRED");
    }
}

TEST_F(BackendPublicationTest, OnlySuccessfulOuterCommitPromotesCandidate)
{
    const auto binding = Binding();
    Catalog_->BindOperation(binding);
    auto staged = Backend_->PrepareAppend(Store_, binding, View_.info, Input_, std::chrono::steady_clock::now() + 10s);
    ASSERT_TRUE(staged) << staged.error().message;
    const auto receipt = (*staged)->Receipt();
    std::string location;
    Catalog_->PublishAppend(
        binding,
        receipt,
        [&](std::string_view root) {
            location = root;
            return (*staged)->VerifyClosure(root);
        },
        [&] {
            return (*staged)->Publish(*Catalog_);
        });
    auto next = Catalog_->Base(test::collection_id);
    auto info = Catalog_->Collection(test::collection_id);
    // Even an authoritative pointer is not an implicit publication handoff.
    EXPECT_FALSE(Backend_->CheckAppendAdmissible(Store_, info, next, Input_));
    Backend_->PublicationFinished(test::collection_id, location, CommitDisposition::Committed);
    staged->reset();
    EXPECT_TRUE(Backend_->CheckAppendAdmissible(Store_, info, next, Input_));
    EXPECT_FALSE(Backend_->CheckAppendAdmissible(Store_, View_.info, View_.base, Input_));
    const auto operation = Catalog_->FindOperation(test::collection_id, FirstOperation);
    const auto& outcome = test::Present(test::Present(operation).outcome);
    EXPECT_EQ(test::Present(outcome.result), receipt);
}

TEST_F(BackendPublicationTest, RolledBackOuterTransactionDiscardsCandidateAndPreservesBase)
{
    const auto binding = Binding();
    Catalog_->BindOperation(binding);
    auto staged = Backend_->PrepareAppend(Store_, binding, View_.info, Input_, std::chrono::steady_clock::now() + 10s);
    ASSERT_TRUE(staged) << staged.error().message;
    std::string location;
    EXPECT_THROW(Catalog_->PublishAppend(
                     binding,
                     (*staged)->Receipt(),
                     [&](std::string_view root) {
                         location = root;
                         return (*staged)->VerifyClosure(root);
                     },
                     [&]() -> iceberg::Status {
                         auto published = (*staged)->Publish(*Catalog_);
                         if (!published) {
                             return published;
                         }
                         return iceberg::CommitFailed("rollback after tentative pointer update");
                     }),
        detail::Failure);
    ASSERT_FALSE(location.empty());
    Backend_->PublicationFinished(test::collection_id, location, CommitDisposition::DefinitelyNotCommitted);
    EXPECT_EQ(Catalog_->Base(test::collection_id), View_.base);
    const auto operation = Catalog_->FindOperation(test::collection_id, FirstOperation);
    EXPECT_EQ(test::Present(operation).state, detail::OperationState::Bound);
    EXPECT_TRUE(Backend_->CheckAppendAdmissible(Store_, View_.info, View_.base, Input_));
    // A late, erroneous success handoff must not revive the discarded candidate.
    Backend_->PublicationFinished(test::collection_id, location, CommitDisposition::Committed);
    auto tentative = View_.base;
    tentative.metadata_location = location;
    tentative.current_snapshot_id = (*staged)->Receipt().snapshot_id;
    auto info = View_.info;
    info.current_snapshot_id = tentative.current_snapshot_id;
    EXPECT_FALSE(Backend_->CheckAppendAdmissible(Store_, info, tentative, Input_));
    EXPECT_FALSE(Backend_->CheckAppendAdmissible(Store_, View_.info, View_.base, Input_));
    staged->reset();
}

TEST_F(BackendPublicationTest, UnknownResultRequiresFullVerificationBeforeAnotherWrite)
{
    Backend_->PublicationFinished(test::collection_id, View_.base.metadata_location, CommitDisposition::CommitUnknown);
    EXPECT_FALSE(Backend_->CheckAppendAdmissible(Store_, View_.info, View_.base, Input_));
    ASSERT_TRUE(Backend_->VerifyRetained(Store_, std::span(&View_, 1), {}));
    EXPECT_TRUE(Backend_->CheckAppendAdmissible(Store_, View_.info, View_.base, Input_));
}

// Real SQLite journal fault, Parquet and Iceberg. The injected error loses the
// result after the outer COMMIT has unlinked the journal; it is not power loss.
TEST(BackendPublicationIntegration, LostCommitResultKeepsReadersOnConfirmedViewUntilRecovery)
{
    test::JournalFaultVfs vfs;
    test::TempDir temporary;
    const auto root = temporary.path / "store";
    std::shared_ptr<THookBackend> hooks;
    auto created =
        detail::EngineFactory::CreateWith({test::store_id, root.string()}, THookBackend::Factory(hooks), vfs.Name());
    ASSERT_TRUE(created);
    auto engine = *created;
    ASSERT_EQ(engine->CreateCollection({test::collection_id, "events", Schema}).disposition,
        CommitDisposition::Committed);
    const auto original = engine->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}});
    ASSERT_EQ(original.disposition, CommitDisposition::Committed);
    const auto drain = [](ScanCursor& cursor) {
        std::vector<std::string> rows;
        while (true) {
            auto next = cursor.Next();
            EXPECT_TRUE(next);
            if (!next || !*next) {
                break;
            }
            auto rendered = Render(**next);
            rows.insert(rows.end(), rendered.begin(), rendered.end());
        }
        return Sorted(rows);
    };
    const auto read = [&]() -> std::vector<std::string> {
        auto cursor = engine->Scan({.collection_id = test::collection_id});
        EXPECT_TRUE(cursor) << (cursor ? "" : cursor.error().message);
        return cursor ? drain(**cursor) : std::vector<std::string>{};
    };
    auto retained = engine->Scan({.collection_id = test::collection_id});
    ASSERT_TRUE(retained);
    std::vector<std::string> duringTransaction;
    hooks->AfterPublish = [&](std::string_view) {
        duringTransaction = read();
    };
    vfs.Arm(2, true);
    const auto unknown = engine->AppendDurable({test::collection_id, SecondOperation, {}, {Batch(SecondRows)}});
    hooks->AfterPublish = {};
    ASSERT_EQ(unknown.disposition, CommitDisposition::CommitUnknown);
    EXPECT_FALSE(engine->GetHealth().write_ready);
    EXPECT_EQ(duringTransaction, Expected({&FirstRows}));
    EXPECT_EQ(drain(**retained), Expected({&FirstRows}));
    const auto blocked = engine->Scan({.collection_id = test::collection_id});
    ASSERT_FALSE(blocked);
    EXPECT_EQ(blocked.error().code, "RECOVERY_REQUIRED");
    const auto blockedRetry = engine->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}});
    EXPECT_EQ(blockedRetry.disposition, CommitDisposition::CommitUnknown);
    ASSERT_TRUE(blockedRetry.recovery);
    EXPECT_TRUE(test::Present(blockedRetry.recovery).lookup_blocked);
    EXPECT_FALSE(engine->FindAppendOutcome(test::collection_id, FirstOperation));

    // Verifying files alone cannot reopen admission: the recovery fence must commit.
    vfs.Arm(1, true);
    EXPECT_FALSE(engine->Reconcile());
    EXPECT_EQ(engine->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}}).disposition,
        CommitDisposition::CommitUnknown);
    vfs.Disarm();
    ASSERT_TRUE(engine->Reconcile());
    const auto retry = engine->AppendDurable({test::collection_id, SecondOperation, {}, {Batch(SecondRows)}});
    ASSERT_EQ(retry.disposition, CommitDisposition::Committed);
    ASSERT_TRUE(retry.result);
    const auto first = engine->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}});
    ASSERT_EQ(first.disposition, CommitDisposition::Committed);
    ASSERT_TRUE(first.result);
    EXPECT_EQ(first, original);
    EXPECT_EQ(test::Present(retry.result).parent_snapshot_id, test::Present(first.result).snapshot_id);
    EXPECT_EQ(read(), Expected({&FirstRows, &SecondRows}));
    EXPECT_EQ(engine->AppendDurable({test::collection_id, ThirdOperation, {}, {Batch(SecondRows)}}).disposition,
        CommitDisposition::Committed);
    retained->reset();
    ASSERT_TRUE(engine->Shutdown(1000ms));
}

} // namespace
} // namespace glamdb
