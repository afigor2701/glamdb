#include "storage/engine.h"

#include "core/api/binary.h"
#include "core/api/codec.h"
#include "core/engine/engine_factory.h"
#include "tests/support/arrow_batch.h"
#include "tests/support/checked.h"
#include "tests/support/fake_storage.h"
#include "tests/support/temp_dir.h"

#include <future>
#include <gtest/gtest.h>

namespace glamdb {
namespace {

class CoordinatorTest : public testing::Test
{
protected:
    test::TempDir temporary;
    std::filesystem::path root = temporary.path / "store";
    std::shared_ptr<test::FakeStorage> backend = std::make_shared<test::FakeStorage>(root);
    std::shared_ptr<Engine> engine;
    CreateStoreRequest create{test::store_id, root.string()};

    void SetUp() override
    {
        auto opened = detail::EngineFactory::Create(create, backend);
        ASSERT_TRUE(opened);
        engine = *opened;
        ASSERT_EQ(engine->CreateCollection({test::collection_id, "events", SchemaSpec{{{"n", FieldType::Int64, true}}}})
                      .disposition,
            CommitDisposition::Committed);
    }
    AppendRequest Request(std::string id = test::operation_id) const
    {
        return {test::collection_id, std::move(id), {}, {test::Batch()}};
    }
    Result<std::shared_ptr<Engine>> Reopen()
    {
        engine.reset();
        return detail::EngineFactory::Open({root.string(), test::store_id}, backend);
    }
};

class RetryAdmissionTest
    : public CoordinatorTest
    , public testing::WithParamInterface<std::tuple<bool, int>>
{ };

TEST_P(RetryAdmissionTest, ResourcePressureCannotChangeAnExistingOperationOutcome)
{
    const auto [already_committed, pressure] = GetParam();
    auto active_request = Request(already_committed ? "00000000-0000-0000-0000-000000000006" : test::operation_id);
    active_request.batches = {test::Batch(std::vector<std::optional<int64_t>>(256, 5))};
    Limits limits;
    limits.writer_wait = std::chrono::milliseconds(1);
    if (pressure == 1) {
        limits.max_pending_calls = 1;
    } else if (pressure == 2) {
        limits.max_owned_input_bytes = static_cast<int64_t>(
            detail::InputReservation(SchemaSpec{{{"n", FieldType::Int64, true}}}, active_request.batches).value());
    }
    engine.reset();
    auto opened =
        detail::EngineFactory::Open({root.string(), test::store_id, "local-delete-extra-v1", limits}, backend);
    ASSERT_TRUE(opened);
    engine = *opened;
    std::optional<AppendOutcome> original;
    if (already_committed) {
        original = engine->AppendDurable(Request());
        ASSERT_EQ(test::Present(original).disposition, CommitDisposition::Committed);
    }
    std::promise<void> entered;
    std::promise<void> release;
    auto released = release.get_future().share();
    backend->on_prepare = [&] {
        entered.set_value();
        released.wait();
    };
    auto write = std::async(std::launch::async, [&] {
        return engine->AppendDurable(active_request);
    });
    entered.get_future().wait();
    const auto retry_request = already_committed ? Request() : active_request;
    const auto retry = engine->AppendDurable(retry_request);
    const auto lookup = engine->FindAppendOutcome(test::collection_id, test::operation_id);
    const bool still_ready = engine->GetCollection(test::collection_id).has_value();
    release.set_value();
    const auto completed = write.get();
    backend->on_prepare = {};
    ASSERT_EQ(completed.disposition, CommitDisposition::Committed);
    EXPECT_TRUE(still_ready);
    if (already_committed) {
        EXPECT_EQ(retry, test::Present(original));
        ASSERT_TRUE(lookup);
        EXPECT_EQ(test::Present(lookup.value()), test::Present(original));
    } else {
        EXPECT_EQ(retry.disposition, CommitDisposition::CommitUnknown);
        EXPECT_EQ(test::Present(retry.error).retry_hint, RetryHint::SameIdAfterReconcile);
        EXPECT_TRUE(detail::ValidateOutcome(retry));
        ASSERT_FALSE(lookup);
        EXPECT_EQ(lookup.error().code, "OPERATION_IN_PROGRESS");
    }
    EXPECT_EQ(engine->AppendDurable(retry_request), already_committed ? test::Present(original) : completed);
    EXPECT_EQ(backend->prepares, already_committed ? 2 : 1);
}

INSTANTIATE_TEST_SUITE_P(WriterTimeoutPendingAndInputBudgets,
    RetryAdmissionTest,
    testing::Combine(testing::Bool(), testing::Values(0, 1, 2)));

TEST_F(CoordinatorTest, AppendThenNewerAppendThenRetryReturnsExactOldReceipt)
{
    const auto a = engine->AppendDurable(Request());
    ASSERT_EQ(a.disposition, CommitDisposition::Committed);
    const auto b = engine->AppendDurable(Request("00000000-0000-0000-0000-000000000006"));
    ASSERT_EQ(b.disposition, CommitDisposition::Committed);
    auto retry = Request();
    retry.expected_snapshot = {SnapshotPreconditionKind::NoSnapshot, 0};
    EXPECT_EQ(engine->AppendDurable(retry), a);
    EXPECT_EQ(backend->prepares, 2);
    EXPECT_EQ(engine->GetCollection(test::collection_id)->current_snapshot_id, test::Present(b.result).snapshot_id);
    auto opened = Reopen();
    ASSERT_TRUE(opened);
    engine = *opened;
    EXPECT_EQ(engine->AppendDurable(retry), a);
    EXPECT_EQ(test::Present(engine->FindAppendOutcome(test::collection_id, test::operation_id).value()), a);
    EXPECT_EQ(backend->prepares, 2);
}

TEST_F(CoordinatorTest, TerminalRetryDoesNotNeedPayloadCapacity)
{
    const auto original = engine->AppendDurable(Request());
    ASSERT_EQ(original.disposition, CommitDisposition::Committed);
    engine.reset();
    Limits limits;
    limits.max_operations = 1;
    limits.max_owned_input_bytes = 1;
    const auto opened =
        detail::EngineFactory::Open({root.string(), test::store_id, "local-delete-extra-v1", limits}, backend);
    ASSERT_TRUE(opened);
    engine = *opened;
    EXPECT_EQ(engine->AppendDurable(Request()), original);
    const auto next_id = "00000000-0000-0000-0000-000000000006";
    const auto rejected = engine->AppendDurable(Request(next_id));
    EXPECT_EQ(rejected.disposition, CommitDisposition::DefinitelyNotCommitted);
    EXPECT_EQ(test::Present(rejected.error).code, "RESOURCE_EXHAUSTED");
    EXPECT_FALSE(engine->FindAppendOutcome(test::collection_id, next_id)->has_value());
    EXPECT_EQ(backend->prepares, 1);
}

TEST_F(CoordinatorTest, BusyTentativeCatalogCannotLeakAnOutcomeOrExtendTheWriterWait)
{
    const auto original = engine->AppendDurable(Request());
    ASSERT_EQ(original.disposition, CommitDisposition::Committed);
    engine.reset();
    Limits limits;
    limits.writer_wait = std::chrono::milliseconds(1);
    const auto opened =
        detail::EngineFactory::Open({root.string(), test::store_id, "local-delete-extra-v1", limits}, backend);
    ASSERT_TRUE(opened);
    engine = *opened;
    std::promise<void> entered;
    std::promise<void> release;
    auto released = release.get_future().share();
    backend->on_pointer = [&] {
        entered.set_value();
        released.wait();
    };
    auto write = std::async(std::launch::async, [&] {
        return engine->AppendDurable(Request("00000000-0000-0000-0000-000000000006"));
    });
    entered.get_future().wait();
    auto retry = std::async(std::launch::async, [&] {
        return engine->AppendDurable(Request());
    });
    const auto status = retry.wait_for(std::chrono::milliseconds(200));
    release.set_value();
    const auto blocked = retry.get();
    const auto completed = write.get();
    backend->on_pointer = {};
    EXPECT_EQ(status, std::future_status::ready);
    EXPECT_EQ(blocked.disposition, CommitDisposition::CommitUnknown);
    EXPECT_TRUE(detail::ValidateOutcome(blocked));
    EXPECT_TRUE(test::Present(blocked.recovery).lookup_blocked);
    EXPECT_EQ(completed.disposition, CommitDisposition::Committed);
    EXPECT_EQ(engine->AppendDurable(Request()), original);
}

TEST_F(CoordinatorTest, BusyCreateRetryCannotTurnAnExistingCollectionIntoDefiniteFailure)
{
    engine.reset();
    Limits limits;
    limits.writer_wait = std::chrono::milliseconds(1);
    const auto opened =
        detail::EngineFactory::Open({root.string(), test::store_id, "local-delete-extra-v1", limits}, backend);
    ASSERT_TRUE(opened);
    engine = *opened;
    std::promise<void> entered;
    std::promise<void> release;
    auto released = release.get_future().share();
    backend->on_prepare = [&] {
        entered.set_value();
        released.wait();
    };
    auto write = std::async(std::launch::async, [&] {
        return engine->AppendDurable(Request());
    });
    entered.get_future().wait();
    const CreateCollectionRequest request{test::collection_id, "events", SchemaSpec{{{"n", FieldType::Int64, true}}}};
    const auto blocked = engine->CreateCollection(request);
    release.set_value();
    const auto completed = write.get();
    backend->on_prepare = {};
    EXPECT_EQ(blocked.disposition, CommitDisposition::CommitUnknown);
    EXPECT_EQ(test::Present(blocked.error).retry_hint, RetryHint::SameIdAfterReconcile);
    EXPECT_EQ(test::Present(blocked.recovery).collection_id, test::collection_id);
    EXPECT_EQ(completed.disposition, CommitDisposition::Committed);
    EXPECT_EQ(engine->CreateCollection(request).disposition, CommitDisposition::Committed);
}

TEST_F(CoordinatorTest, RegisteredFailureIsPermanentAndDifferentPayloadConflicts)
{
    backend->fail_prepare = true;
    const auto failed = engine->AppendDurable(Request());
    ASSERT_EQ(failed.disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(failed.error);
    EXPECT_EQ(test::Present(failed.error).retry_hint, RetryHint::NewOperationId);
    backend->fail_prepare = false;
    EXPECT_EQ(engine->AppendDurable(Request()), failed);
    auto conflict = Request();
    conflict.batches = {test::Batch({9})};
    EXPECT_EQ(test::Present(engine->AppendDurable(conflict).error).code, "OPERATION_CONFLICT");
    EXPECT_FALSE(engine->GetCollection(test::collection_id)->current_snapshot_id);
    auto opened = Reopen();
    ASSERT_TRUE(opened);
    engine = *opened;
    EXPECT_EQ(engine->AppendDurable(Request()), failed);
    EXPECT_EQ(backend->prepares, 1);
}

class DiagnosticTest
    : public CoordinatorTest
    , public testing::WithParamInterface<std::string>
{ };

TEST_P(DiagnosticTest, LongProducerFailureRemainsDurableAndRecoverable)
{
    const std::string prefix(2047, 'x');
    backend->on_prepare = [&] {
        throw detail::Failure({"IO_ERROR", prefix + GetParam(), RetryHint::OperatorAction});
    };
    const auto failed = engine->AppendDurable(Request());
    backend->on_prepare = {};
    EXPECT_EQ(failed.disposition, CommitDisposition::DefinitelyNotCommitted);
    EXPECT_EQ(test::Present(failed.error).code, "IO_ERROR");
    EXPECT_EQ(test::Present(failed.error).message, prefix);
    EXPECT_TRUE(detail::ValidateOutcome(failed));
    ASSERT_TRUE(engine->Reconcile());
    EXPECT_EQ(engine->AppendDurable(Request()), failed);
    auto opened = Reopen();
    ASSERT_TRUE(opened);
    engine = *opened;
    EXPECT_EQ(engine->AppendDurable(Request()), failed);
    EXPECT_EQ(backend->prepares, 1);
}

TEST_P(DiagnosticTest, LongVerifierExceptionPreservesAValidUnknownOutcome)
{
    const auto original = engine->AppendDurable(Request());
    ASSERT_EQ(original.disposition, CommitDisposition::Committed);
    const std::string prefix(2047, 'x');
    backend->on_verify = [&] {
        throw std::runtime_error(prefix + GetParam());
    };
    const auto unknown = engine->AppendDurable(Request());
    backend->on_verify = {};
    EXPECT_EQ(unknown.disposition, CommitDisposition::CommitUnknown);
    EXPECT_EQ(test::Present(unknown.error).code, "INTERNAL");
    EXPECT_EQ(test::Present(unknown.error).message, prefix);
    EXPECT_EQ(test::Present(unknown.recovery).first_error.message, prefix);
    EXPECT_TRUE(detail::ValidateOutcome(unknown));
    ASSERT_TRUE(engine->Reconcile());
    EXPECT_EQ(engine->AppendDurable(Request()), original);
}

INSTANTIATE_TEST_SUITE_P(Utf8Boundary,
    DiagnosticTest,
    testing::Values(std::string("я"), std::string("€"), std::string("😀")));

TEST_F(CoordinatorTest, EmptyInputDoesNotBindAndPreconditionFailureDoes)
{
    auto empty = Request();
    empty.batches.clear();
    EXPECT_EQ(engine->AppendDurable(empty).disposition, CommitDisposition::DefinitelyNotCommitted);
    EXPECT_FALSE(engine->FindAppendOutcome(test::collection_id, test::operation_id)->has_value());
    auto mismatch = Request();
    mismatch.expected_snapshot = {SnapshotPreconditionKind::Equals, 7};
    const auto failed = engine->AppendDurable(mismatch);
    EXPECT_EQ(test::Present(failed.error).code, "PRECONDITION_FAILED");
    EXPECT_EQ(engine->AppendDurable(Request()), failed);
    EXPECT_EQ(backend->prepares, 0);
}

TEST_F(CoordinatorTest, GuardOrInnerFailureRollsBackPointerAndStoresFailure)
{
    backend->fail_guard = true;
    const auto guarded = engine->AppendDurable(Request());
    ASSERT_EQ(guarded.disposition, CommitDisposition::DefinitelyNotCommitted);
    EXPECT_FALSE(engine->GetCollection(test::collection_id)->current_snapshot_id);
    backend->fail_guard = false;
    backend->fail_after_pointer = true;
    const auto inner = engine->AppendDurable(Request("00000000-0000-0000-0000-000000000006"));
    ASSERT_EQ(inner.disposition, CommitDisposition::DefinitelyNotCommitted);
    EXPECT_FALSE(engine->GetCollection(test::collection_id)->current_snapshot_id);
    EXPECT_TRUE(Reopen());
}

TEST_F(CoordinatorTest, CreateRetryRequiresExactNameIdAndSchema)
{
    const CreateCollectionRequest request{test::collection_id, "events", SchemaSpec{{{"n", FieldType::Int64, true}}}};
    EXPECT_EQ(engine->CreateCollection(request).disposition, CommitDisposition::Committed);
    EXPECT_EQ(engine->ResolveCollection("event").error().code, "COLLECTION_NOT_FOUND");
    auto other = request;
    other.schema.fields[0].nullable = false;
    EXPECT_EQ(test::Present(engine->CreateCollection(other).error).code, "SCHEMA_MISMATCH");
    other = request;
    other.collection_id = "00000000-0000-0000-0000-000000000006";
    EXPECT_EQ(test::Present(engine->CreateCollection(other).error).code, "ALREADY_EXISTS");
}

TEST_F(CoordinatorTest, CreateVerifierExceptionUsesRecoverableHintAndRetainsContext)
{
    const CreateCollectionRequest request{test::collection_id, "events", SchemaSpec{{{"n", FieldType::Int64, true}}}};
    backend->throw_verifier = true;
    const auto first = engine->CreateCollection(request);
    ASSERT_EQ(first.disposition, CommitDisposition::CommitUnknown);
    EXPECT_EQ(test::Present(first.error).retry_hint, RetryHint::SameIdAfterReconcile);
    const auto repeated = engine->CreateCollection(request);
    EXPECT_EQ(repeated.disposition, CommitDisposition::CommitUnknown);
    EXPECT_EQ(test::Present(repeated.recovery).attempt_id, test::Present(first.recovery).attempt_id);
    backend->throw_verifier = false;
    ASSERT_TRUE(engine->Reconcile());
    EXPECT_EQ(engine->CreateCollection(request).disposition, CommitDisposition::Committed);
}

TEST_F(CoordinatorTest, SameProcessSecondOpenIsLockedAndMissingOpenNeverCreates)
{
    const auto second = detail::EngineFactory::Open({root.string(), test::store_id}, backend);
    ASSERT_FALSE(second);
    EXPECT_EQ(second.error().code, "STATE_IN_USE");
    const auto missing = temporary.path / "missing";
    EXPECT_FALSE(OpenExisting({missing.string(), test::store_id}));
    EXPECT_FALSE(std::filesystem::exists(missing));
    auto opened = Reopen();
    ASSERT_TRUE(opened);
}

TEST_F(CoordinatorTest, CorruptOrMissingPublishedRootClosesReadiness)
{
    ASSERT_EQ(engine->AppendDurable(Request()).disposition, CommitDisposition::Committed);
    std::filesystem::remove(backend->last_root);
    const auto opened = Reopen();
    ASSERT_FALSE(opened);
    EXPECT_EQ(opened.error().code, "CORRUPTION");
    EXPECT_TRUE(std::filesystem::exists(root / "catalog.sqlite"));
}

TEST_F(CoordinatorTest, VerifierExceptionCannotTurnCommittedRetryIntoFailure)
{
    ASSERT_EQ(engine->AppendDurable(Request()).disposition, CommitDisposition::Committed);
    backend->throw_verifier = true;
    const auto retry = engine->AppendDurable(Request());
    EXPECT_EQ(retry.disposition, CommitDisposition::CommitUnknown);
    EXPECT_FALSE(engine->GetCollection(test::collection_id));
    backend->throw_verifier = false;
    EXPECT_TRUE(engine->Reconcile());
    EXPECT_EQ(engine->AppendDurable(Request()).disposition, CommitDisposition::Committed);
}

TEST_F(CoordinatorTest, DetectedCorruptionBlocksFurtherWritesInTheLiveProcess)
{
    ASSERT_EQ(engine->AppendDurable(Request()).disposition, CommitDisposition::Committed);
    std::filesystem::remove(backend->last_root);
    const auto failed = engine->AppendDurable(Request("00000000-0000-0000-0000-000000000006"));
    ASSERT_EQ(failed.disposition, CommitDisposition::DefinitelyNotCommitted);
    EXPECT_EQ(test::Present(failed.error).code, "CORRUPTION");
    EXPECT_FALSE(engine->GetCollection(test::collection_id));
    const auto blocked = engine->AppendDurable(Request("00000000-0000-0000-0000-000000000007"));
    EXPECT_EQ(test::Present(blocked.error).code, "RECOVERY_REQUIRED");
    EXPECT_EQ(backend->prepares, 2);
}

TEST_F(CoordinatorTest, ActiveLookupAndShutdownTimeoutDoNotInventFailure)
{
    std::promise<void> entered;
    std::promise<void> release;
    auto released = release.get_future().share();
    backend->on_prepare = [&] {
        entered.set_value();
        released.wait();
    };
    auto write = std::async(std::launch::async, [&] {
        return engine->AppendDurable(Request());
    });
    entered.get_future().wait();
    const auto lookup = engine->FindAppendOutcome(test::collection_id, test::operation_id);
    ASSERT_FALSE(lookup);
    EXPECT_EQ(lookup.error().code, "OPERATION_IN_PROGRESS");
    const auto shutdown = engine->Shutdown(std::chrono::milliseconds(1));
    EXPECT_FALSE(shutdown);
    EXPECT_EQ(shutdown.error().code, "DEADLINE_EXCEEDED");
    release.set_value();
    const auto result = write.get();
    EXPECT_EQ(result.disposition, CommitDisposition::DefinitelyNotCommitted);
    EXPECT_EQ(test::Present(result.error).code, "CANCELLED");
    EXPECT_TRUE(engine->Shutdown(std::chrono::milliseconds(1000)));
}

TEST_F(CoordinatorTest, ShutdownCannotTurnAPublishingRetryIntoADefiniteFailure)
{
    std::promise<void> entered;
    std::promise<void> release;
    auto released = release.get_future().share();
    backend->on_pointer = [&] {
        entered.set_value();
        released.wait();
    };
    auto write = std::async(std::launch::async, [&] {
        return engine->AppendDurable(Request());
    });
    entered.get_future().wait();
    auto retry = std::async(std::launch::async, [&] {
        return engine->AppendDurable(Request());
    });
    const auto waiting = retry.wait_for(std::chrono::milliseconds(100));
    const auto shutdown = engine->Shutdown(std::chrono::milliseconds(1));
    const auto closing = engine->AppendDurable(Request());
    release.set_value();
    const auto original = write.get();
    const auto waited = retry.get();
    backend->on_pointer = {};
    EXPECT_EQ(waiting, std::future_status::timeout);
    ASSERT_FALSE(shutdown);
    EXPECT_EQ(shutdown.error().code, "DEADLINE_EXCEEDED");
    ASSERT_EQ(original.disposition, CommitDisposition::Committed);
    for (const auto& outcome : {waited, closing}) {
        EXPECT_EQ(outcome.disposition, CommitDisposition::CommitUnknown);
        EXPECT_EQ(test::Present(outcome.error).retry_hint, RetryHint::SameIdAfterReconcile);
        EXPECT_TRUE(detail::ValidateOutcome(outcome));
        EXPECT_TRUE(test::Present(outcome.recovery).lookup_blocked);
    }
    ASSERT_TRUE(engine->Shutdown(std::chrono::milliseconds(1000)));
    const auto closed = engine->AppendDurable(Request());
    EXPECT_EQ(closed.disposition, CommitDisposition::CommitUnknown);
    EXPECT_TRUE(detail::ValidateOutcome(closed));
    auto opened = Reopen();
    ASSERT_TRUE(opened);
    engine = *opened;
    EXPECT_EQ(engine->AppendDurable(Request()), original);
    EXPECT_EQ(backend->prepares, 1);
}

TEST_F(CoordinatorTest, ShutdownCannotReclassifyAnExistingCollectionAsUncommitted)
{
    const CreateCollectionRequest request{test::collection_id, "events", SchemaSpec{{{"n", FieldType::Int64, true}}}};
    ASSERT_TRUE(engine->Shutdown(std::chrono::milliseconds(1000)));
    const auto unknown = engine->CreateCollection(request);
    EXPECT_EQ(unknown.disposition, CommitDisposition::CommitUnknown);
    EXPECT_EQ(test::Present(unknown.error).retry_hint, RetryHint::SameIdAfterReconcile);
    EXPECT_EQ(test::Present(unknown.recovery).collection_id, test::collection_id);
    auto opened = Reopen();
    ASSERT_TRUE(opened);
    engine = *opened;
    EXPECT_EQ(engine->CreateCollection(request).disposition, CommitDisposition::Committed);
}

TEST_F(CoordinatorTest, ShutdownReportsRecoveryExceptionAndRetainsOwnership)
{
    ASSERT_EQ(engine->AppendDurable(Request()).disposition, CommitDisposition::Committed);
    backend->throw_verifier = true;
    ASSERT_EQ(engine->AppendDurable(Request()).disposition, CommitDisposition::CommitUnknown);
    Status shutdown;
    EXPECT_NO_THROW(shutdown = engine->Shutdown(std::chrono::milliseconds(1000)));
    ASSERT_FALSE(shutdown);
    EXPECT_EQ(shutdown.error().code, "INTERNAL");
    const auto retry = engine->AppendDurable(Request());
    EXPECT_EQ(retry.disposition, CommitDisposition::CommitUnknown);
    EXPECT_TRUE(detail::ValidateOutcome(retry));
    EXPECT_FALSE(test::Present(retry.recovery).lookup_blocked);
    const auto other = detail::EngineFactory::Open({root.string(), test::store_id}, backend);
    ASSERT_FALSE(other);
    EXPECT_EQ(other.error().code, "STATE_IN_USE");
    backend->throw_verifier = false;
    EXPECT_TRUE(engine->Shutdown(std::chrono::milliseconds(1000)));
    EXPECT_TRUE(engine->Shutdown(std::chrono::milliseconds(1000)));
}

TEST(EngineBootstrap, NoProducerFailsClosedAndIdentityIsExplicit)
{
    test::TempDir temporary;
    const auto root = temporary.path / "store";
    // The public factory installs the real storage backend when it is built;
    // the internal factory without a backend must still fail closed.
    const auto created = detail::EngineFactory::Create({test::store_id, root.string()});
    ASSERT_TRUE(created);
    EXPECT_EQ((*created)->GetStoreInfo().store_id, test::store_id);
    const auto collection =
        (*created)->CreateCollection({test::collection_id, "events", SchemaSpec{{{"n", FieldType::Int64, true}}}});
    EXPECT_EQ(collection.disposition, CommitDisposition::DefinitelyNotCommitted);
    EXPECT_EQ(test::Present(collection.error).code, "DEPENDENCY_UNAVAILABLE");
    EXPECT_FALSE(CreateStore({test::store_id, root.string()}));
}

// Without a storage reader (FakeStorage) or without the scanner build, a scan
// fails as a dependency error and leaves readiness open.
TEST_F(CoordinatorTest, ScanWithoutStorageReaderIsDependencyUnavailable)
{
    const auto scan = engine->Scan({.collection_id = test::collection_id});
    ASSERT_FALSE(scan);
    EXPECT_EQ(scan.error().code, "DEPENDENCY_UNAVAILABLE");
    EXPECT_TRUE(engine->ListCollections());
}

#if !GLAMDB_TEST_STORAGE
// GLAMDB_BUILD_STORAGE=OFF: the public factories install no backend.
TEST(EngineBootstrap, PublicFactoriesWithoutStorageFailClosed)
{
    test::TempDir temporary;
    const auto root = temporary.path / "store";
    const auto created = CreateStore({test::store_id, root.string()});
    ASSERT_TRUE(created) << created.error().message;
    const auto collection =
        (*created)->CreateCollection({test::collection_id, "events", SchemaSpec{{{"n", FieldType::Int64, true}}}});
    EXPECT_EQ(collection.disposition, CommitDisposition::DefinitelyNotCommitted);
    EXPECT_EQ(test::Present(collection.error).code, "DEPENDENCY_UNAVAILABLE");
    const auto scan = (*created)->Scan({.collection_id = test::collection_id});
    ASSERT_FALSE(scan);
    // The scanner is not built, so no collection lookup is needed to refuse.
    EXPECT_EQ(scan.error().code, "DEPENDENCY_UNAVAILABLE");
    EXPECT_TRUE((*created)->Shutdown(std::chrono::milliseconds(1000)));
}
#endif

} // namespace
} // namespace glamdb
