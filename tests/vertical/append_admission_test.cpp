#include "tests/support/checked.h"
#include "tests/vertical/vertical_support.h"

#include "core/engine/storage_mapping.h"

#include <arrow/memory_pool.h>

#include <chrono>
#include <limits>
#include <new>

namespace glamdb {
namespace {

using namespace vertical;
using namespace std::chrono_literals;

// Fail after earlier column buffers have been allocated, so refusal also
// exercises cleanup. This wraps the real Arrow allocator.
class TFailingInputPool final : public arrow::ProxyMemoryPool
{
public:
    explicit TFailingInputPool(bool throwFailure)
        : arrow::ProxyMemoryPool(arrow::default_memory_pool())
        , Throw_(throwFailure)
    { }

    arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t** out) override
    {
        if (++Calls_ == 3) {
            if (Throw_) {
                throw std::bad_alloc();
            }
            return arrow::Status::OutOfMemory("injected input allocation failure");
        }
        return arrow::ProxyMemoryPool::Allocate(size, alignment, out);
    }

private:
    const bool Throw_;
    int Calls_ = 0;
};

class AppendAdmissionTest : public ::testing::Test
{
protected:
    test::TempDir Temporary_;
    std::filesystem::path Root_ = Temporary_.path / "store";
    std::shared_ptr<THookBackend> Hooks_;
    std::shared_ptr<Engine> Engine_;

    void Create(const SchemaSpec& schema, const detail::TLocalStorageOptions& options = {})
    {
        auto created =
            detail::EngineFactory::CreateWith({test::store_id, Root_.string()}, THookBackend::Factory(Hooks_, options));
        ASSERT_TRUE(created) << created.error().message;
        Engine_ = std::move(*created);
        ASSERT_EQ(Engine_->CreateCollection({test::collection_id, "events", schema}).disposition,
            CommitDisposition::Committed);
    }

    void Reopen(const detail::TLocalStorageOptions& options = {}, const Limits& limits = {})
    {
        ASSERT_TRUE(Engine_->Shutdown(1000ms));
        Engine_.reset();
        auto opened = detail::EngineFactory::OpenWith(
            {.absolute_root = Root_.string(), .expected_store_id = test::store_id, .limits = limits},
            THookBackend::Factory(Hooks_, options));
        ASSERT_TRUE(opened) << opened.error().message;
        Engine_ = std::move(*opened);
    }

    void ExpectUnbound(std::string_view operation)
    {
        const auto found = Engine_->FindAppendOutcome(test::collection_id, operation);
        ASSERT_TRUE(found) << found.error().message;
        EXPECT_FALSE(found->has_value());
    }
};

// Refusing the real Arrow allocation must leave the ID reusable, including
// when an existing receipt is retried through the same exhausted allocator.
TEST_F(AppendAdmissionTest, AllocationRefusalDoesNotBindAndOldRetryNeedsNoAllocation)
{
    ASSERT_NO_FATAL_FAILURE(Create(Schema));
    const AppendRequest first{test::collection_id, FirstOperation, {}, {Batch(FirstRows)}};
    const auto committed = Engine_->AppendDurable(first);
    ASSERT_EQ(committed.disposition, CommitDisposition::Committed);
    auto pool = std::make_shared<arrow::CappedMemoryPool>(arrow::default_memory_pool(), 0);
    detail::TLocalStorageOptions options;
    options.InputPool = pool;
    ASSERT_NO_FATAL_FAILURE(Reopen(options));
    EXPECT_EQ(Engine_->AppendDurable(first), committed);
    const auto files = CountRegularFiles(Root_ / "warehouse");
    const AppendRequest next{test::collection_id, SecondOperation, {}, {Batch(SecondRows)}};
    const auto refused = Engine_->AppendDurable(next);
    EXPECT_EQ(refused.disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(refused.error);
    EXPECT_EQ(test::Present(refused.error).code, "RESOURCE_EXHAUSTED");
    EXPECT_EQ(test::Present(refused.error).retry_hint, RetryHint::OperatorAction);
    EXPECT_EQ(refused.phase, CommitPhase::Validation);
    EXPECT_EQ(CountRegularFiles(Root_ / "warehouse"), files);
    ExpectUnbound(SecondOperation);
    EXPECT_TRUE(Engine_->GetHealth().write_ready);
    ASSERT_NO_FATAL_FAILURE(Reopen());
    EXPECT_EQ(Engine_->AppendDurable(next).disposition, CommitDisposition::Committed);
}

TEST_F(AppendAdmissionTest, PartialAllocationRefusalReleasesBuffersAndAdmission)
{
    ASSERT_NO_FATAL_FAILURE(Create(Schema));
    AppendRequest request{test::collection_id, FirstOperation, {}, {Batch(FirstRows)}};
    for (const bool throwFailure : {false, true}) {
        SCOPED_TRACE(throwFailure);
        auto pool = std::make_shared<TFailingInputPool>(throwFailure);
        detail::TLocalStorageOptions options;
        options.InputPool = pool;
        ASSERT_NO_FATAL_FAILURE(Reopen(options));
        const auto files = CountRegularFiles(Root_ / "warehouse");
        const auto refused = Engine_->AppendDurable(request);
        ASSERT_TRUE(refused.error);
        EXPECT_EQ(refused.disposition, CommitDisposition::DefinitelyNotCommitted);
        EXPECT_EQ(test::Present(refused.error).code, "RESOURCE_EXHAUSTED");
        EXPECT_EQ(test::Present(refused.error).retry_hint, RetryHint::OperatorAction);
        EXPECT_EQ(refused.phase, CommitPhase::Validation);
        ExpectUnbound(request.operation_id);
        EXPECT_EQ(pool->bytes_allocated(), 0);
        EXPECT_EQ(CountRegularFiles(Root_ / "warehouse"), files);
        // The same engine and ID can proceed once its allocator permits work.
        const auto committed = Engine_->AppendDurable(request);
        ASSERT_EQ(committed.disposition, CommitDisposition::Committed);
        EXPECT_EQ(pool->bytes_allocated(), 0);
        EXPECT_EQ(Engine_->AppendDurable(request), committed);
        // Use the other permanent ID in the next allocator case.
        request.operation_id = SecondOperation;
    }
}

TEST_F(AppendAdmissionTest, OwnedInputRefusalPreservesOldReceiptsAndNewIds)
{
    ASSERT_NO_FATAL_FAILURE(Create(Schema));
    const AppendRequest first{test::collection_id, FirstOperation, {}, {Batch(FirstRows)}};
    const auto committed = Engine_->AppendDurable(first);
    ASSERT_EQ(committed.disposition, CommitDisposition::Committed);
    Limits limits;
    limits.max_owned_input_bytes = 1;
    ASSERT_NO_FATAL_FAILURE(Reopen({}, limits));
    EXPECT_EQ(Engine_->AppendDurable(first), committed);
    const AppendRequest next{test::collection_id, SecondOperation, {}, {Batch(SecondRows)}};
    const auto refused = Engine_->AppendDurable(next);
    ASSERT_TRUE(refused.error);
    EXPECT_EQ(refused.disposition, CommitDisposition::DefinitelyNotCommitted);
    EXPECT_EQ(test::Present(refused.error).code, "RESOURCE_EXHAUSTED");
    EXPECT_EQ(refused.phase, CommitPhase::Validation);
    ExpectUnbound(SecondOperation);
    ASSERT_NO_FATAL_FAILURE(Reopen());
    EXPECT_EQ(Engine_->AppendDurable(next).disposition, CommitDisposition::Committed);
}

TEST_F(AppendAdmissionTest, ImpossibleDiskReservePreservesOldReceiptsAndNewIds)
{
    ASSERT_NO_FATAL_FAILURE(Create(Schema));
    const AppendRequest first{test::collection_id, FirstOperation, {}, {Batch(FirstRows)}};
    const auto committed = Engine_->AppendDurable(first);
    ASSERT_EQ(committed.disposition, CommitDisposition::Committed);
    detail::TLocalStorageOptions options;
    options.Storage.MinimumFreeBytes = std::numeric_limits<uint64_t>::max();
    ASSERT_NO_FATAL_FAILURE(Reopen(options));
    EXPECT_EQ(Engine_->AppendDurable(first), committed);
    const auto files = CountRegularFiles(Root_ / "warehouse");
    const AppendRequest next{test::collection_id, SecondOperation, {}, {Batch(SecondRows)}};
    const auto refused = Engine_->AppendDurable(next);
    ASSERT_TRUE(refused.error);
    EXPECT_EQ(refused.disposition, CommitDisposition::DefinitelyNotCommitted);
    EXPECT_EQ(test::Present(refused.error).code, "RESOURCE_EXHAUSTED");
    EXPECT_EQ(test::Present(refused.error).retry_hint, RetryHint::OperatorAction);
    EXPECT_EQ(refused.phase, CommitPhase::Validation);
    EXPECT_EQ(CountRegularFiles(Root_ / "warehouse"), files);
    ExpectUnbound(SecondOperation);
    ASSERT_NO_FATAL_FAILURE(Reopen());
    EXPECT_EQ(Engine_->AppendDurable(next).disposition, CommitDisposition::Committed);
}

// For two rows of five columns: canonical NULLs cost one byte each, while
// writer row groups charge fixed slots and string offsets even for NULLs.
TEST_F(AppendAdmissionTest, NullHeavyRowGroupExactLimitAndOneByteOver)
{
    const std::vector<TInputRow> rows(2, {"", {}, {}, {}, {}});
    detail::TLocalStorageOptions options;
    // Row: string 5 + int64 9 + double 9 + bool 2 + timestamp 9 = 34;
    // plus the final four-byte string offset.
    options.Writer.MaxRowGroupBytes = 38;
    ASSERT_NO_FATAL_FAILURE(Create(Schema, options));
    const AppendRequest first{test::collection_id, FirstOperation, {}, {Batch(rows)}};
    const auto committed = Engine_->AppendDurable(first);
    ASSERT_EQ(committed.disposition, CommitDisposition::Committed);
    options.Writer.MaxRowGroupBytes = 37;
    ASSERT_NO_FATAL_FAILURE(Reopen(options));
    EXPECT_EQ(Engine_->AppendDurable(first), committed);
    const auto refused = Engine_->AppendDurable({test::collection_id, SecondOperation, {}, {Batch(rows)}});
    ASSERT_TRUE(refused.error);
    EXPECT_EQ(test::Present(refused.error).code, "RESOURCE_EXHAUSTED");
    EXPECT_EQ(test::Present(refused.error).retry_hint, RetryHint::FixInput);
    EXPECT_EQ(refused.phase, CommitPhase::Validation);
    ExpectUnbound(SecondOperation);
}

TEST_F(AppendAdmissionTest, NullHeavyWriterRowExactLimitAndOneByteOver)
{
    const SchemaSpec schema{
        {{"a", FieldType::String, true}, {"b", FieldType::String, true}, {"n", FieldType::Int64, true}}};
    ASSERT_NO_FATAL_FAILURE(Create(schema));
    // Fixed writer slots cost 5 + 5 + 9 bytes even though n is NULL.
    // Canonical overhead is only 11, so both rows remain canonical-valid.
    const auto exact =
        detail::MakeRecordBatch(schema, {{std::string(1048566, 'x'), std::string(1048567, 'y'), std::monostate{}}});
    const auto over =
        detail::MakeRecordBatch(schema, {{std::string(1048566, 'x'), std::string(1048568, 'y'), std::monostate{}}});
    ASSERT_TRUE(exact);
    ASSERT_TRUE(over);
    const AppendRequest first{test::collection_id, FirstOperation, {}, {*exact}};
    const auto committed = Engine_->AppendDurable(first);
    ASSERT_EQ(committed.disposition, CommitDisposition::Committed);
    const auto files = CountRegularFiles(Root_ / "warehouse");
    const auto refused = Engine_->AppendDurable({test::collection_id, SecondOperation, {}, {*over}});
    ASSERT_TRUE(refused.error);
    EXPECT_EQ(refused.disposition, CommitDisposition::DefinitelyNotCommitted);
    EXPECT_EQ(test::Present(refused.error).code, "RESOURCE_EXHAUSTED");
    EXPECT_EQ(test::Present(refused.error).retry_hint, RetryHint::FixInput);
    EXPECT_EQ(refused.phase, CommitPhase::Validation);
    EXPECT_EQ(CountRegularFiles(Root_ / "warehouse"), files);
    ExpectUnbound(SecondOperation);
    EXPECT_EQ(Engine_->AppendDurable(first), committed);
    EXPECT_EQ(Engine_->AppendDurable({test::collection_id, SecondOperation, {}, {*exact}}).disposition,
        CommitDisposition::Committed);
}

TEST_F(AppendAdmissionTest, NullHeavyBatchExactLimitAndOneByteOver)
{
    const SchemaSpec schema{{{"s", FieldType::String, true}, {"n", FieldType::Int64, true}}};
    // Missing nullable columns normalize into eight NULL strings and integers.
    const auto batch = arrow::RecordBatch::Make(arrow::schema({}), 8, std::vector<std::shared_ptr<arrow::Array>>{});
    detail::TLocalStorageOptions options;
    // Strings: 9 four-byte offsets + 1 bitmap; integers: 8*8 + 1 bitmap.
    options.Writer.MaxInputBytes = 102;
    ASSERT_NO_FATAL_FAILURE(Create(schema, options));
    const AppendRequest first{test::collection_id, FirstOperation, {}, {batch}};
    const auto committed = Engine_->AppendDurable(first);
    ASSERT_EQ(committed.disposition, CommitDisposition::Committed);
    options.Writer.MaxInputBytes = 101;
    ASSERT_NO_FATAL_FAILURE(Reopen(options));
    EXPECT_EQ(Engine_->AppendDurable(first), committed);
    const auto refused = Engine_->AppendDurable({test::collection_id, SecondOperation, {}, {batch}});
    ASSERT_TRUE(refused.error);
    EXPECT_EQ(test::Present(refused.error).code, "RESOURCE_EXHAUSTED");
    EXPECT_EQ(test::Present(refused.error).retry_hint, RetryHint::FixInput);
    EXPECT_EQ(refused.phase, CommitPhase::Validation);
    ExpectUnbound(SecondOperation);
}

} // namespace
} // namespace glamdb
