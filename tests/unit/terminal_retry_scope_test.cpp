// Terminal retries reuse the verified generation; only recovery verifies history.
//
// The DISABLED_ benchmark prints coordinator-only retry latency versus store
// history with storage verification stubbed out; run it manually with
//   --gtest_also_run_disabled_tests --gtest_filter='*TerminalRetryCost*'

#include "storage/engine.h"
#include "tests/support/checked.h"

#include "core/api/binary.h"
#include "core/api/codec.h"
#include "core/engine/engine_factory.h"
#include "tests/support/arrow_batch.h"
#include "tests/support/fake_storage.h"
#include "tests/support/temp_dir.h"

#include <sqlite3.h>

#include <chrono>
#include <cstdio>
#include <iostream>
#include <mutex>
#include <set>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

namespace glamdb {
namespace {

using namespace std::chrono_literals;

constexpr char other_collection[] = "00000000-0000-0000-0000-0000000000b2";

const SchemaSpec& NumberSchema()
{
    static const SchemaSpec schema{{{"n", FieldType::Int64, true}}};
    return schema;
}

std::string Uuid(std::string_view group, unsigned value)
{
    char tail[13];
    std::snprintf(tail, sizeof(tail), "%012x", value);
    return "00000000-0000-0000-" + std::string(group) + "-" + tail;
}

std::string OperationId(unsigned value)
{
    return Uuid("0000", value);
}

AppendRequest Request(std::string collection, std::string id)
{
    return {std::move(collection), std::move(id), {}, {test::Batch()}};
}

// Delegates production to FakeStorage and records every retained-storage
// verification the coordinator asks for.
class RecordingBackend final : public detail::StorageBackend
{
public:
    struct Call
    {
        std::set<std::string> collections;
        std::set<std::string> operation_collections;
        size_t operations = 0;
    };

    explicit RecordingBackend(std::shared_ptr<test::FakeStorage> inner)
        : inner_(std::move(inner))
    { }

    Result<std::unique_ptr<detail::StagedCreate>>
    PrepareCreate(const StoreInfo& store, const CreateCollectionRequest& request, std::string_view attempt) override
    {
        return inner_->PrepareCreate(store, request, attempt);
    }

    Result<std::unique_ptr<detail::StagedAppend>> PrepareAppend(const StoreInfo& store,
        const detail::BoundOperation& binding,
        const CollectionInfo& collection,
        const detail::NormalizedInput& input,
        std::chrono::steady_clock::time_point deadline) override
    {
        return inner_->PrepareAppend(store, binding, collection, input, deadline);
    }

    Status VerifyRetained(const StoreInfo& store,
        std::span<const detail::CollectionView> collections,
        std::span<const detail::OperationRecord> operations) override
    {
        Call call;
        for (const auto& view : collections) {
            call.collections.insert(view.info.collection_id);
        }
        for (const auto& operation : operations) {
            call.operation_collections.insert(operation.binding.collection_id);
        }
        call.operations = operations.size();
        {
            std::lock_guard lock(mutex_);
            calls_.push_back(std::move(call));
        }
        if (skip_inner) {
            return {};
        }
        return inner_->VerifyRetained(store, collections, operations);
    }

    std::vector<Call> TakeCalls()
    {
        std::lock_guard lock(mutex_);
        return std::exchange(calls_, {});
    }

    // Benchmark only: isolate coordinator/catalog work from the test double,
    // whose own verification is quadratic in its tag history.
    bool skip_inner = false;

private:
    std::shared_ptr<test::FakeStorage> inner_;
    std::mutex mutex_;
    std::vector<Call> calls_;
};

class TerminalRetryScopeTest : public testing::Test
{
protected:
    test::TempDir temporary;
    std::filesystem::path root = temporary.path / "store";
    std::shared_ptr<test::FakeStorage> storage = std::make_shared<test::FakeStorage>(root);
    std::shared_ptr<RecordingBackend> backend = std::make_shared<RecordingBackend>(storage);
    std::shared_ptr<Engine> engine;

    void SetUp() override
    {
        auto created = detail::EngineFactory::Create({test::store_id, root.string()}, backend);
        ASSERT_TRUE(created) << created.error().code << ": " << created.error().message;
        engine = *created;
        ASSERT_EQ(engine->CreateCollection({test::collection_id, "events", NumberSchema()}).disposition,
            CommitDisposition::Committed);
        ASSERT_EQ(engine->CreateCollection({other_collection, "other", NumberSchema()}).disposition,
            CommitDisposition::Committed);
    }
};

TEST_F(TerminalRetryScopeTest, TerminalRetriesUseVerifiedGeneration)
{
    const auto committed_request = Request(test::collection_id, OperationId(1));
    const auto committed = engine->AppendDurable(committed_request);
    ASSERT_EQ(committed.disposition, CommitDisposition::Committed);

    // A bound, permanently failed operation: it references no storage at all.
    auto failed_request = Request(test::collection_id, OperationId(2));
    failed_request.expected_snapshot = {SnapshotPreconditionKind::NoSnapshot, 0};
    const auto failed = engine->AppendDurable(failed_request);
    ASSERT_EQ(failed.disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(failed.error.has_value());
    ASSERT_EQ(test::Present(failed.error).code, "PRECONDITION_FAILED");

    // Unrelated history in collection B.
    for (unsigned index = 0; index < 16; ++index) {
        ASSERT_EQ(engine->AppendDurable(Request(other_collection, OperationId(100 + index))).disposition,
            CommitDisposition::Committed);
    }
    (void)backend->TakeCalls();

    EXPECT_EQ(engine->AppendDurable(committed_request), committed);
    EXPECT_EQ(engine->AppendDurable(failed_request), failed);

    EXPECT_TRUE(backend->TakeCalls().empty());

    ASSERT_TRUE(engine->Reconcile());
    EXPECT_EQ(backend->TakeCalls().size(), 1U);
    EXPECT_EQ(engine->AppendDurable(committed_request), committed);
    EXPECT_EQ(engine->AppendDurable(failed_request), failed);
    EXPECT_TRUE(backend->TakeCalls().empty());
}

TEST_F(TerminalRetryScopeTest, TerminalLookupDoesNotDecodeUnrelatedOperationHistory)
{
    const auto committedRequest = Request(test::collection_id, OperationId(1));
    const auto committed = engine->AppendDurable(committedRequest);
    ASSERT_EQ(committed.disposition, CommitDisposition::Committed);
    auto failedRequest = Request(test::collection_id, OperationId(2));
    failedRequest.expected_snapshot = {SnapshotPreconditionKind::NoSnapshot, 0};
    const auto failed = engine->AppendDurable(failedRequest);
    ASSERT_EQ(failed.disposition, CommitDisposition::DefinitelyNotCommitted);
    auto unrelated = Request(other_collection, OperationId(3));
    unrelated.expected_snapshot = {SnapshotPreconditionKind::Equals, 1};
    ASSERT_EQ(engine->AppendDurable(unrelated).disposition, CommitDisposition::DefinitelyNotCommitted);

    // An unreadable unrelated row distinguishes point lookup from history decoding.
    detail::Database database;
    database.Open(root / "catalog.sqlite", SQLITE_OPEN_READWRITE, {});
    database.Execute("UPDATE operations SET hash_version=2 WHERE collection_id=?", {std::string(other_collection)});
    ASSERT_EQ(database.Changes(), 1);
    EXPECT_EQ(engine->AppendDurable(committedRequest), committed);
    EXPECT_EQ(engine->AppendDurable(failedRequest), failed);
    const auto lookup = engine->FindAppendOutcome(test::collection_id, OperationId(1));
    ASSERT_TRUE(lookup && lookup->has_value());
    EXPECT_EQ(test::Present(*lookup), committed);

    // Recovery must inspect that row and keep admission closed until it is repaired.
    EXPECT_FALSE(engine->Reconcile());
    EXPECT_FALSE(engine->GetHealth().write_ready);
    database.Execute("UPDATE operations SET hash_version=1 WHERE collection_id=?", {std::string(other_collection)});
    ASSERT_TRUE(engine->Reconcile());
    EXPECT_EQ(engine->AppendDurable(committedRequest), committed);
    EXPECT_EQ(engine->AppendDurable(failedRequest), failed);
}

// ---- Manual benchmark: coordinator-only terminal retry cost vs history ----

// Clones the single Failed operation row `copies` times with fresh IDs, in
// one transaction, so a large catalog history needs no durable appends.
void CloneFailedOperation(const std::filesystem::path& catalog, unsigned copies)
{
    sqlite3* database = nullptr;
    ASSERT_EQ(sqlite3_open_v2(catalog.c_str(), &database, SQLITE_OPEN_READWRITE, nullptr), SQLITE_OK);
    sqlite3_stmt* select = nullptr;
    ASSERT_EQ(sqlite3_prepare_v2(database,
                  "SELECT collection_id,payload_hash,base_identity,outcome_blob FROM operations WHERE state=2",
                  -1,
                  &select,
                  nullptr),
        SQLITE_OK);
    ASSERT_EQ(sqlite3_step(select), SQLITE_ROW);
    const auto text = [&](int column) {
        return std::string(static_cast<const char*>(sqlite3_column_blob(select, column)),
            static_cast<size_t>(sqlite3_column_bytes(select, column)));
    };
    const auto collection = text(0);
    const auto hash = text(1);
    const auto base = text(2);
    const auto outcome_bytes = text(3);
    sqlite3_finalize(select);
    const auto outcome = detail::DecodeOutcome(outcome_bytes);
    ASSERT_TRUE(outcome);

    ASSERT_EQ(sqlite3_exec(database, "BEGIN IMMEDIATE", nullptr, nullptr, nullptr), SQLITE_OK);
    sqlite3_stmt* insert = nullptr;
    ASSERT_EQ(sqlite3_prepare_v2(database,
                  "INSERT INTO operations(collection_id,operation_id,payload_hash,hash_version,state,"
                  "base_identity,attempt_id,receipt_version,outcome_blob) VALUES(?,?,?,1,2,?,?,1,?)",
                  -1,
                  &insert,
                  nullptr),
        SQLITE_OK);
    for (unsigned index = 1; index <= copies; ++index) {
        auto copy = *outcome;
        copy.operation_id = Uuid("1000", index);
        const auto encoded = detail::EncodeOutcome(copy);
        ASSERT_TRUE(encoded);
        const auto attempt = Uuid("2000", index);
        sqlite3_bind_text(insert, 1, collection.data(), static_cast<int>(collection.size()), SQLITE_TRANSIENT);
        sqlite3_bind_text(insert,
            2,
            copy.operation_id.data(),
            static_cast<int>(copy.operation_id.size()),
            SQLITE_TRANSIENT);
        sqlite3_bind_text(insert, 3, hash.data(), static_cast<int>(hash.size()), SQLITE_TRANSIENT);
        sqlite3_bind_blob(insert, 4, base.data(), static_cast<int>(base.size()), SQLITE_TRANSIENT);
        sqlite3_bind_text(insert, 5, attempt.data(), static_cast<int>(attempt.size()), SQLITE_TRANSIENT);
        sqlite3_bind_blob(insert, 6, encoded->data(), static_cast<int>(encoded->size()), SQLITE_TRANSIENT);
        ASSERT_EQ(sqlite3_step(insert), SQLITE_DONE);
        sqlite3_reset(insert);
        sqlite3_clear_bindings(insert);
    }
    sqlite3_finalize(insert);
    ASSERT_EQ(sqlite3_exec(database, "COMMIT", nullptr, nullptr, nullptr), SQLITE_OK);
    ASSERT_EQ(sqlite3_close(database), SQLITE_OK);
}

TEST(TerminalRetryCost, DISABLED_CoordinatorWorkScalesWithStoreHistory)
{
    for (const unsigned history : {1U, 256U, 1024U, 4096U}) {
        test::TempDir temporary;
        const auto root = temporary.path / "store";
        auto storage = std::make_shared<test::FakeStorage>(root);
        auto backend = std::make_shared<RecordingBackend>(storage);
        backend->skip_inner = true;
        const auto retried = Request(test::collection_id, OperationId(0));
        AppendOutcome failed;
        {
            auto created = detail::EngineFactory::Create({test::store_id, root.string()}, backend);
            ASSERT_TRUE(created);
            auto engine = *created;
            ASSERT_EQ(engine->CreateCollection({test::collection_id, "events", NumberSchema()}).disposition,
                CommitDisposition::Committed);
            storage->fail_prepare = true;
            failed = engine->AppendDurable(retried);
            storage->fail_prepare = false;
            ASSERT_EQ(failed.disposition, CommitDisposition::DefinitelyNotCommitted);
            ASSERT_TRUE(engine->Shutdown(1000ms));
        }
        CloneFailedOperation(root / "catalog.sqlite", history - 1);
        auto opened = detail::EngineFactory::Open({root.string(), test::store_id}, backend);
        ASSERT_TRUE(opened) << opened.error().code << ": " << opened.error().message;
        auto engine = *opened;
        constexpr int retries = 50;
        const auto start = std::chrono::steady_clock::now();
        for (int index = 0; index < retries; ++index) {
            ASSERT_EQ(engine->AppendDurable(retried), failed);
        }
        const std::chrono::duration<double, std::milli> elapsed = std::chrono::steady_clock::now() - start;
        std::cout << "operations=" << history << " terminal-retry-mean-ms=" << elapsed.count() / retries << "\n";
        ASSERT_TRUE(engine->Shutdown(1000ms));
    }
}

} // namespace
} // namespace glamdb
