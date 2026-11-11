// Superseded metadata roots are removed after a confirmed publication, but
// never while a scan that resolved them is still planning, and only by the
// writer, which also walks the warehouse for capacity preflight.

#include "tests/vertical/vertical_support.h"

#include "core/io/local_fs.h"

#include <chrono>
#include <condition_variable>
#include <future>
#include <mutex>
#include <thread>

namespace glamdb {
namespace {

using namespace std::chrono_literals;
using namespace glamdb::vertical;
namespace NIO = NGlamDB::NIO;

// Delegates to POSIX and can hold one thread inside its next root open, and
// the next warehouse walk at its start.
class TRootGateFS final : public NIO::ILocalFS
{
public:
    void Arm(std::thread::id reader)
    {
        std::lock_guard lock(Mutex_);
        Reader_ = reader;
        Armed_ = true;
    }

    bool WaitEntered(std::chrono::milliseconds timeout)
    {
        std::unique_lock lock(Mutex_);
        return Changed_.wait_for(lock, timeout, [&] {
            return Entered_;
        });
    }

    void Release()
    {
        std::lock_guard lock(Mutex_);
        Released_ = true;
        Changed_.notify_all();
    }

    void ArmWalk()
    {
        std::lock_guard lock(Mutex_);
        WalkArmed_ = true;
    }

    bool WaitWalkEntered(std::chrono::milliseconds timeout)
    {
        std::unique_lock lock(Mutex_);
        return Changed_.wait_for(lock, timeout, [&] {
            return WalkEntered_;
        });
    }

    void ReleaseWalk()
    {
        std::lock_guard lock(Mutex_);
        WalkReleased_ = true;
        Changed_.notify_all();
    }

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
        if (name.ends_with(".metadata.json")) {
            std::unique_lock lock(Mutex_);
            if (Armed_ && std::this_thread::get_id() == Reader_) {
                Armed_ = false;
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
        {
            std::unique_lock lock(Mutex_);
            if (WalkArmed_) {
                WalkArmed_ = false;
                WalkEntered_ = true;
                Changed_.notify_all();
                Changed_.wait(lock, [&] {
                    return WalkReleased_;
                });
            }
        }
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
    std::thread::id Reader_;
    bool Armed_ = false;
    bool Entered_ = false;
    bool Released_ = false;
    bool WalkArmed_ = false;
    bool WalkEntered_ = false;
    bool WalkReleased_ = false;
};

class SupersededRootTest : public ::testing::Test
{
protected:
    test::TempDir Temporary_;
    std::filesystem::path Root_ = Temporary_.path / "store";
    std::shared_ptr<TRootGateFS> Gate_ = std::make_shared<TRootGateFS>();
    std::shared_ptr<Engine> Engine_;

    void SetUp() override
    {
        const auto gate = Gate_;
        const detail::BackendFactory factory =
            [gate](const std::filesystem::path& path,
                const StoreInfo& info) -> Result<std::shared_ptr<detail::StorageBackend>> {
            detail::TLocalStorageOptions options;
            options.Fs = gate;
            auto backend = detail::TLocalStorageBackend::Open(path, info, options);
            if (!backend) {
                return std::unexpected(backend.error());
            }
            return std::shared_ptr<detail::StorageBackend>(std::move(*backend));
        };
        auto created = detail::EngineFactory::CreateWith({test::store_id, Root_.string()}, factory);
        ASSERT_TRUE(created) << created.error().message;
        Engine_ = std::move(*created);
        ASSERT_EQ(Engine_->CreateCollection({test::collection_id, "events", Schema}).disposition,
            CommitDisposition::Committed);
    }

    void TearDown() override
    {
        if (Engine_) {
            EXPECT_TRUE(Engine_->Shutdown(1000ms));
        }
    }

    std::vector<std::filesystem::path> Roots() const
    {
        return FilesIn(Root_ / "warehouse" / test::collection_id / "metadata", [](const auto& path) {
            return path.filename().string().ends_with(".metadata.json");
        });
    }

    AppendResult Commit(const char* operation, const std::vector<TInputRow>& rows)
    {
        const auto outcome = Engine_->AppendDurable({test::collection_id, operation, {}, {Batch(rows)}});
        EXPECT_EQ(outcome.disposition, CommitDisposition::Committed)
            << (outcome.error ? outcome.error->code + ": " + outcome.error->message : std::string());
        return outcome.result.value_or(AppendResult{});
    }

    std::vector<std::string> Read(SnapshotSelector selector = {})
    {
        auto cursor = Engine_->Scan({.collection_id = test::collection_id, .selector = selector});
        EXPECT_TRUE(cursor) << (cursor ? std::string() : cursor.error().code + ": " + cursor.error().message);
        std::vector<std::string> rows;
        while (cursor) {
            const auto batch = (*cursor)->Next();
            EXPECT_TRUE(batch) << (batch ? std::string() : batch.error().message);
            if (!batch || !*batch) {
                break;
            }
            const auto rendered = Render(**batch);
            rows.insert(rows.end(), rendered.begin(), rendered.end());
        }
        return Sorted(std::move(rows));
    }
};

TEST_F(SupersededRootTest, OnlyTheCommittedRootRemainsAndHistoryStaysReadable)
{
    const auto first = Commit(FirstOperation, FirstRows);
    Commit(SecondOperation, SecondRows);
    Commit(ThirdOperation, FirstRows);
    ASSERT_EQ(Roots().size(), 1U);
    EXPECT_EQ(Read(), Expected({&FirstRows, &SecondRows, &FirstRows}));
    EXPECT_EQ(Read(SnapshotSelector::Id(first.snapshot_id)), Expected({&FirstRows}));
    EXPECT_TRUE(Read(SnapshotSelector::InitialEmpty()).empty());

    ASSERT_TRUE(Engine_->Shutdown(1000ms));
    Engine_.reset();
    auto opened = OpenExisting({Root_.string(), test::store_id});
    ASSERT_TRUE(opened) << opened.error().code << ": " << opened.error().message;
    Engine_ = std::move(*opened);
    EXPECT_EQ(Read(SnapshotSelector::Id(first.snapshot_id)), Expected({&FirstRows}));
    // A retry still finds its receipt backed by the retained history.
    EXPECT_EQ(Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}}).result, first);
}

TEST_F(SupersededRootTest, RootStaysUntilAConcurrentScanHasPlanned)
{
    Commit(FirstOperation, FirstRows);
    const auto resolved = Roots();
    ASSERT_EQ(resolved.size(), 1U);

    std::promise<std::thread::id> readerID;
    std::promise<void> armed;
    auto reader = std::async(std::launch::async, [&] {
        readerID.set_value(std::this_thread::get_id());
        armed.get_future().wait();
        return Read();
    });
    Gate_->Arm(readerID.get_future().get());
    armed.set_value();
    // The scan resolved the first root and is about to open it.
    ASSERT_TRUE(Gate_->WaitEntered(10s));

    Commit(SecondOperation, SecondRows);
    EXPECT_TRUE(std::filesystem::exists(resolved.front())) << "superseded root removed under an active scan";
    Gate_->Release();
    EXPECT_EQ(reader.get(), Expected({&FirstRows}));
    EXPECT_TRUE(Engine_->ListCollections()) << "the scan must not observe a missing root";
    EXPECT_TRUE(std::filesystem::exists(resolved.front())) << "only the writer removes a released root";

    // Shutdown holds the writer and removes roots released after the last append.
    ASSERT_TRUE(Engine_->Shutdown(1000ms));
    Engine_.reset();
    const auto remaining = Roots();
    ASSERT_EQ(remaining.size(), 1U);
    EXPECT_NE(remaining.front(), resolved.front());
    auto opened = OpenExisting({Root_.string(), test::store_id});
    ASSERT_TRUE(opened) << opened.error().code << ": " << opened.error().message;
    Engine_ = std::move(*opened);
    EXPECT_EQ(Read(), Expected({&FirstRows, &SecondRows}));
}

TEST_F(SupersededRootTest, MetadataRetainsItsConfirmedRootAcrossAppend)
{
    const auto first = Commit(FirstOperation, FirstRows);
    const auto resolved = Roots();
    ASSERT_EQ(resolved.size(), 1U);
    std::promise<std::thread::id> readerID;
    std::promise<void> armed;
    auto reader = std::async(std::launch::async, [&] {
        readerID.set_value(std::this_thread::get_id());
        armed.get_future().wait();
        return Engine_->ExplainScan({.collection_id = test::collection_id});
    });
    Gate_->Arm(readerID.get_future().get());
    armed.set_value();
    const bool entered = Gate_->WaitEntered(10s);
    if (!entered) {
        Gate_->Release();
    }
    ASSERT_TRUE(entered);
    Commit(SecondOperation, SecondRows);
    EXPECT_TRUE(std::filesystem::exists(resolved.front()));
    Gate_->Release();
    const auto explanation = reader.get();
    ASSERT_TRUE(explanation) << explanation.error().message;
    EXPECT_EQ(explanation->info.selector_resolved, SnapshotSelector::Id(first.snapshot_id));
    EXPECT_EQ(explanation->candidates.rows.value, 3);
    const auto latest = Engine_->GetStatistics(test::collection_id);
    ASSERT_TRUE(latest);
    EXPECT_EQ(latest->rows.value, 5);
    Commit(ThirdOperation, FirstRows);
    EXPECT_FALSE(std::filesystem::exists(resolved.front())) << "metadata leaked a root lease";
    EXPECT_EQ(Roots().size(), 1U);
}

TEST_F(SupersededRootTest, FileDetailsRetainTheirConfirmedRootAcrossAppend)
{
    const auto first = Commit(FirstOperation, FirstRows);
    const auto listing = Engine_->ListFiles(test::collection_id);
    ASSERT_TRUE(listing);
    ASSERT_EQ(listing->Files.size(), 1U);
    const auto resolved = Roots();
    ASSERT_EQ(resolved.size(), 1U);
    std::promise<std::thread::id> readerID;
    std::promise<void> armed;
    auto reader = std::async(std::launch::async, [&] {
        readerID.set_value(std::this_thread::get_id());
        armed.get_future().wait();
        return Engine_->GetFileDetails(test::collection_id, listing->Files.front().FileID);
    });
    Gate_->Arm(readerID.get_future().get());
    armed.set_value();
    const bool entered = Gate_->WaitEntered(10s);
    if (!entered) {
        Gate_->Release();
    }
    ASSERT_TRUE(entered);
    Commit(SecondOperation, SecondRows);
    EXPECT_TRUE(std::filesystem::exists(resolved.front()));
    Gate_->Release();
    const auto details = reader.get();
    ASSERT_TRUE(details) << details.error().message;
    EXPECT_EQ(details->SelectorResolved, SnapshotSelector::Id(first.snapshot_id));
    EXPECT_EQ(details->File.RecordCount, 3);
    Commit(ThirdOperation, FirstRows);
    EXPECT_FALSE(std::filesystem::exists(resolved.front()));
}

// A scan that finishes while the next append walks the warehouse for capacity
// must not unlink its root under that walk: a root removed between the walk's
// readdir and fstatat failed preflight as corruption and closed the store.
TEST_F(SupersededRootTest, ReleasedRootIsNotRemovedDuringACapacityWalk)
{
    Commit(FirstOperation, FirstRows);
    const auto resolved = Roots();
    ASSERT_EQ(resolved.size(), 1U);

    std::promise<std::thread::id> readerID;
    std::promise<void> armed;
    auto reader = std::async(std::launch::async, [&] {
        readerID.set_value(std::this_thread::get_id());
        armed.get_future().wait();
        return Read();
    });
    Gate_->Arm(readerID.get_future().get());
    armed.set_value();
    ASSERT_TRUE(Gate_->WaitEntered(10s));
    Commit(SecondOperation, SecondRows);

    // The third append is inside its preflight walk when the scan finishes.
    Gate_->ArmWalk();
    auto writer = std::async(std::launch::async, [&] {
        return Engine_->AppendDurable({test::collection_id, ThirdOperation, {}, {Batch(FirstRows)}});
    });
    ASSERT_TRUE(Gate_->WaitWalkEntered(10s));
    Gate_->Release();
    EXPECT_EQ(reader.get(), Expected({&FirstRows}));
    EXPECT_TRUE(std::filesystem::exists(resolved.front())) << "a released root was removed during a capacity walk";
    Gate_->ReleaseWalk();

    const auto outcome = writer.get();
    EXPECT_EQ(outcome.disposition, CommitDisposition::Committed)
        << (outcome.error ? outcome.error->code + ": " + outcome.error->message : std::string());
    EXPECT_TRUE(Engine_->ListCollections());
    // The committing writer removed both superseded roots.
    EXPECT_EQ(Roots().size(), 1U);
    EXPECT_FALSE(std::filesystem::exists(resolved.front()));
    EXPECT_EQ(Read(), Expected({&FirstRows, &SecondRows, &FirstRows}));
}

} // namespace
} // namespace glamdb
