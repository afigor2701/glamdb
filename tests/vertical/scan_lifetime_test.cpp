#include "tests/vertical/vertical_support.h"

#include "core/io/local_fs.h"

#include <arrow/c/bridge.h>

#include <chrono>
#include <condition_variable>
#include <future>
#include <mutex>

namespace glamdb {
namespace {

using namespace std::chrono_literals;
using namespace glamdb::vertical;
namespace NIO = NGlamDB::NIO;

// Holds a real file open at a deterministic boundary, without timing sleeps.
class TScanGateFS final : public NIO::ILocalFS
{
public:
    void Arm(std::string suffix)
    {
        std::lock_guard lock(Mutex_);
        Suffix_ = std::move(suffix);
    }

    bool WaitEntered()
    {
        std::unique_lock lock(Mutex_);
        return Changed_.wait_for(lock, 5s, [&] {
            return Entered_;
        });
    }

    void Release()
    {
        std::lock_guard lock(Mutex_);
        Released_ = true;
        Changed_.notify_all();
    }

    int DataOpens = 0;

    NIO::TResult<NIO::THandle> OpenDir(const std::filesystem::path& path) override
    {
        return Inner_.OpenDir(path);
    }
    NIO::TResult<NIO::THandle> OpenDir(const NIO::THandle& parent, std::string_view name) override
    {
        return Inner_.OpenDir(parent, name);
    }
    std::error_code MakeDir(const NIO::THandle& parent, std::string_view name) override
    {
        return Inner_.MakeDir(parent, name);
    }
    NIO::TResult<NIO::THandle>
    CreateExclusive(const NIO::THandle& parent, std::string_view name, const NIO::TIOTag& tag) override
    {
        return Inner_.CreateExclusive(parent, name, tag);
    }
    NIO::TResult<NIO::THandle> OpenRead(const NIO::THandle& parent, std::string_view name) override
    {
        {
            std::unique_lock lock(Mutex_);
            if (name.ends_with(".parquet")) {
                ++DataOpens;
            }
            if (!Suffix_.empty() && name.ends_with(Suffix_)) {
                Suffix_.clear();
                Entered_ = true;
                Changed_.notify_all();
                Changed_.wait(lock, [&] {
                    return Released_;
                });
            }
        }
        return Inner_.OpenRead(parent, name);
    }
    NIO::TResult<std::size_t> Write(const NIO::THandle& file, std::span<const std::byte> bytes) override
    {
        return Inner_.Write(file, bytes);
    }
    NIO::TResult<std::size_t> PRead(const NIO::THandle& file, std::uint64_t offset, std::span<std::byte> bytes) override
    {
        return Inner_.PRead(file, offset, bytes);
    }
    NIO::TResult<NIO::TFileStat> Stat(const NIO::THandle& handle) override
    {
        return Inner_.Stat(handle);
    }
    NIO::TResult<NIO::TSpaceInfo> StatFs(const NIO::THandle& directory) override
    {
        return Inner_.StatFs(directory);
    }
    NIO::TResult<std::uint64_t> MeasureTree(const NIO::THandle& directory, std::uint64_t limit) override
    {
        return Inner_.MeasureTree(directory, limit);
    }
    std::error_code SyncFile(const NIO::THandle& file) override
    {
        return Inner_.SyncFile(file);
    }
    std::error_code SyncDir(const NIO::THandle& directory) override
    {
        return Inner_.SyncDir(directory);
    }
    std::error_code Close(const NIO::THandle& handle) override
    {
        return Inner_.Close(handle);
    }
    std::error_code Unlink(const NIO::THandle& parent, std::string_view name) override
    {
        return Inner_.Unlink(parent, name);
    }

private:
    NIO::TPosixLocalFS Inner_;
    std::mutex Mutex_;
    std::condition_variable Changed_;
    std::string Suffix_;
    bool Entered_ = false;
    bool Released_ = false;
};

struct TExportedArray
{
    ArrowArray Value{};

    ~TExportedArray()
    {
        if (Value.release != nullptr) {
            Value.release(&Value);
        }
    }
};

class TScanLifetimeTest : public ::testing::TestWithParam<bool>
{
protected:
    test::TempDir Temporary_;
    std::filesystem::path Root_ = Temporary_.path / "store";
    std::shared_ptr<TScanGateFS> Gate_ = std::make_shared<TScanGateFS>();
    std::weak_ptr<detail::StorageBackend> Backend_;
    std::shared_ptr<Engine> Engine_;

    void SetUp() override
    {
        const detail::BackendFactory factory =
            [this](const std::filesystem::path& root,
                const StoreInfo& info) -> Result<std::shared_ptr<detail::StorageBackend>> {
            detail::TLocalStorageOptions options;
            options.Fs = Gate_;
            auto backend = detail::TLocalStorageBackend::Open(root, info, options);
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
        ASSERT_EQ(Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}}).disposition,
            CommitDisposition::Committed);
    }

    void TearDown() override
    {
        if (Engine_) {
            EXPECT_TRUE(Engine_->Shutdown(1000ms));
        }
    }

    void ExpectLocked()
    {
        const auto opened = OpenExisting({Root_.string(), test::store_id});
        ASSERT_FALSE(opened);
        EXPECT_EQ(opened.error().code, "STATE_IN_USE");
    }

    void Stop(const std::shared_ptr<ScanCursor>& cursor)
    {
        if (GetParam()) {
            EXPECT_TRUE(Engine_->Shutdown(1000ms));
        } else {
            cursor->Cancel();
        }
    }

    void StopDuringRead(std::string suffix, Predicate predicate = {})
    {
        auto cursor = Engine_->Scan({.collection_id = test::collection_id, .predicate = std::move(predicate)});
        ASSERT_TRUE(cursor) << cursor.error().message;
        Gate_->Arm(std::move(suffix));
        auto reading = std::async(std::launch::async, [&] {
            return (*cursor)->Next();
        });
        const bool entered = Gate_->WaitEntered();
        if (entered) {
            Stop(*cursor);
        }
        Gate_->Release();
        const auto result = reading.get();
        ASSERT_TRUE(entered) << "scan did not reach its I/O barrier";
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error().code, "CANCELLED");
        EXPECT_EQ((*cursor)->Profile().state, ScanState::Cancelled);
    }
};

TEST_P(TScanLifetimeTest, BuffersRetainResourcesUntilTheLastDependentHandle)
{
    ScanRequest request{.collection_id = test::collection_id};
    if (GetParam()) {
        request.predicate = Predicate::Compare("key", CompareOp::Eq, std::string("duplicate"));
    }
    auto cursor = Engine_->Scan(request);
    ASSERT_TRUE(cursor) << cursor.error().message;
    auto next = (*cursor)->Next();
    ASSERT_TRUE(next);
    auto batch = std::move(*next);
    ASSERT_NE(batch, nullptr);
    auto array = batch->column(1);
    auto slice = batch->column(0)->Slice(0, 1);
    auto buffer = array->data()->buffers[1];
    TExportedArray exported;
    ASSERT_TRUE(arrow::ExportRecordBatch(*batch, &exported.Value).ok());

    cursor->reset();
    ASSERT_TRUE(Engine_->Shutdown(1000ms));
    Engine_.reset();
    const std::weak_ptr<TScanGateFS> fileIO = Gate_;
    Gate_.reset();
    EXPECT_FALSE(Backend_.expired());
    EXPECT_FALSE(fileIO.expired());
    EXPECT_TRUE(batch->ValidateFull().ok());
    EXPECT_EQ(batch->num_rows(), GetParam() ? 2 : 3);
    EXPECT_EQ(RenderCell(*array, 0), "1");
    EXPECT_EQ(RenderCell(*slice, 0), "duplicate");
    ExpectLocked();

    batch.reset();
    ExpectLocked();
    array.reset();
    ExpectLocked();
    slice.reset();
    ExpectLocked();
    // Inspect exported values after all C++ arrays have gone away.
    ASSERT_EQ(exported.Value.n_children, 5);
    EXPECT_EQ(static_cast<const int64_t*>(exported.Value.children[1]->buffers[1])[0], 1);
    exported.Value.release(&exported.Value);
    ExpectLocked();
    buffer.reset();
    EXPECT_TRUE(Backend_.expired());
    EXPECT_TRUE(fileIO.expired());
    const auto reopened = OpenExisting({Root_.string(), test::store_id});
    ASSERT_TRUE(reopened) << reopened.error().code << ": " << reopened.error().message;
    Engine_ = *reopened;
}

TEST_P(TScanLifetimeTest, StopDuringDataReadDiscardsTheBatch)
{
    StopDuringRead(".parquet");
}

TEST_P(TScanLifetimeTest, StopDuringDataReadDiscardsEos)
{
    StopDuringRead(".parquet", Predicate::Compare("key", CompareOp::Eq, std::string("middle")));
}

TEST_P(TScanLifetimeTest, StopIsCheckedBeforePrunedEos)
{
    const auto before = Gate_->DataOpens;
    auto cursor = Engine_->Scan({
        .collection_id = test::collection_id,
        .predicate = Predicate::Compare("key", CompareOp::Eq, std::string("absent")),
    });
    ASSERT_TRUE(cursor) << cursor.error().message;
    Stop(*cursor);
    const auto next = (*cursor)->Next();
    ASSERT_FALSE(next);
    EXPECT_EQ(next.error().code, "CANCELLED");
    EXPECT_EQ(Gate_->DataOpens, before);
}

TEST_P(TScanLifetimeTest, StopBeforeIndexedReadDoesNotOpenData)
{
    auto cursor = Engine_->Scan({.collection_id = test::collection_id});
    ASSERT_TRUE(cursor) << cursor.error().message;
    const auto before = Gate_->DataOpens;
    Stop(*cursor);
    const auto next = (*cursor)->Next();
    ASSERT_FALSE(next);
    EXPECT_EQ(next.error().code, "CANCELLED");
    EXPECT_EQ((*cursor)->Profile().state, ScanState::Cancelled);
    EXPECT_EQ(Gate_->DataOpens, before);
}

TEST_P(TScanLifetimeTest, StopIsCheckedBeforeZeroLimitEos)
{
    auto cursor = Engine_->Scan({.collection_id = test::collection_id, .limit = 0});
    ASSERT_TRUE(cursor) << cursor.error().message;
    Stop(*cursor);
    const auto next = (*cursor)->Next();
    ASSERT_FALSE(next);
    EXPECT_EQ(next.error().code, "CANCELLED");
}

TEST_P(TScanLifetimeTest, StopIsCheckedBeforeInitialEmptyEos)
{
    auto cursor = Engine_->Scan({.collection_id = test::collection_id, .selector = SnapshotSelector::InitialEmpty()});
    ASSERT_TRUE(cursor) << cursor.error().message;
    Stop(*cursor);
    const auto next = (*cursor)->Next();
    ASSERT_FALSE(next);
    EXPECT_EQ(next.error().code, "CANCELLED");
}

INSTANTIATE_TEST_SUITE_P(NativeScan, TScanLifetimeTest, ::testing::Bool());

} // namespace
} // namespace glamdb
