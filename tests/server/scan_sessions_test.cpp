#include "apps/server/routes.h"
#include "apps/server/scans.h"
#include "tests/support/checked.h"
#include "tests/vertical/vertical_support.h"

#include "storage/resources.h"

#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>

#include <condition_variable>
#include <future>
#include <mutex>
#include <set>
#include <thread>

namespace glamdb::NServer {
namespace {
using namespace std::chrono_literals;
using namespace NGlamDB::NIO;
using namespace vertical;
using namespace NTransport;

// Pause real Parquet opens without adding a production scan factory seam.
class TPausedFS final : public ILocalFS
{
public:
    TPosixLocalFS Fs;
    std::mutex Mutex;
    std::condition_variable Changed;
    bool Armed = false;
    size_t Entered = 0;
    bool Released = false;
    std::set<std::string> OpenedParquet;
    void Release()
    {
        std::lock_guard lock(Mutex);
        Released = true;
        Changed.notify_all();
    }
    TResult<THandle> OpenDir(const std::filesystem::path& path) override
    {
        return Fs.OpenDir(path);
    }
    TResult<THandle> OpenDir(const THandle& parent, std::string_view name) override
    {
        return Fs.OpenDir(parent, name);
    }
    std::error_code MakeDir(const THandle& parent, std::string_view name) override
    {
        return Fs.MakeDir(parent, name);
    }
    TResult<THandle> CreateExclusive(const THandle& parent, std::string_view name, const TIOTag& tag) override
    {
        return Fs.CreateExclusive(parent, name, tag);
    }
    TResult<THandle> OpenRead(const THandle& parent, std::string_view name) override
    {
        {
            std::unique_lock lock(Mutex);
            if (name.ends_with(".parquet")) {
                OpenedParquet.emplace(name);
            }
            if (Armed && name.ends_with(".parquet")) {
                ++Entered;
                Changed.notify_all();
                Changed.wait(lock, [&] {
                    return Released;
                });
            }
        }
        return Fs.OpenRead(parent, name);
    }
    TResult<size_t> Write(const THandle& file, std::span<const std::byte> bytes) override
    {
        return Fs.Write(file, bytes);
    }
    TResult<size_t> PRead(const THandle& file, uint64_t offset, std::span<std::byte> bytes) override
    {
        return Fs.PRead(file, offset, bytes);
    }
    TResult<TFileStat> Stat(const THandle& handle) override
    {
        return Fs.Stat(handle);
    }
    TResult<TSpaceInfo> StatFs(const THandle& handle) override
    {
        return Fs.StatFs(handle);
    }
    TResult<uint64_t> MeasureTree(const THandle& handle, uint64_t limit) override
    {
        return Fs.MeasureTree(handle, limit);
    }
    std::error_code SyncFile(const THandle& handle) override
    {
        return Fs.SyncFile(handle);
    }
    std::error_code SyncDir(const THandle& handle) override
    {
        return Fs.SyncDir(handle);
    }
    std::error_code Close(const THandle& handle) override
    {
        return Fs.Close(handle);
    }
};
class ScanSessionsTest : public ::testing::Test
{
protected:
    test::TempDir Temporary;
    std::shared_ptr<TPausedFS> Fs = std::make_shared<TPausedFS>();
    // The 64-session boundary cases reserve two maximum replay bodies each.
    std::shared_ptr<ResourceOwner> Resources = [] {
        ResourceLimits limits;
        limits.memory_bytes[0] = 4ULL << 30;
        return std::make_shared<ResourceOwner>(limits);
    }();
    std::shared_ptr<Engine> Db;
    void SetUp() override
    {
        const detail::BackendFactory factory =
            [fs = Fs, resources = Resources](const std::filesystem::path& path,
                const StoreInfo& info) -> Result<std::shared_ptr<detail::StorageBackend>> {
            detail::TLocalStorageOptions options;
            options.Fs = fs;
            options.Resources = resources;
            auto backend = detail::TLocalStorageBackend::Open(path, info, options);
            if (!backend) {
                return std::unexpected(backend.error());
            }
            return std::shared_ptr<detail::StorageBackend>(std::move(*backend));
        };
        auto created = detail::EngineFactory::CreateWith(
            {.store_id = test::store_id, .absolute_root = (Temporary.path / "db").string(), .resources = Resources},
            factory);
        ASSERT_TRUE(created);
        Db = std::move(*created);
        ASSERT_EQ(Db->CreateCollection({test::collection_id, "rows", Schema}).disposition,
            CommitDisposition::Committed);
        ASSERT_EQ(Db->CreateCollection({SecondCollection, "empty", Schema}).disposition, CommitDisposition::Committed);
        ASSERT_EQ(Db->AppendDurable({test::collection_id, FirstOperation, {}, {Batch({{"same", 1, 1.5, true, 0}})}})
                      .disposition,
            CommitDisposition::Committed);
    }
    void TearDown() override
    {
        Fs->Release();
        EXPECT_TRUE(Db->Shutdown(1s));
    }
    std::string
    Open(TScans& scans, const std::string& database = "a", const std::string& collection = test::collection_id)
    {
        return TJson::parse(scans.Open(Db, database, collection, TJson::object(), false, true).Body).at("cursor");
    }
};
TEST_F(ScanSessionsTest, SlowReadDoesNotBlockOtherSessionsOrCancellation)
{
    TScans scans;
    const auto token = Open(scans);
    {
        std::lock_guard lock(Fs->Mutex);
        Fs->Armed = true;
    }
    auto reading = std::async(std::launch::async, [&] {
        try {
            scans.Open(Db, "a", test::collection_id, {{"cursor", token}}, false, false);
            return false;
        } catch (const TFailure& error) {
            return error.Detail.code == "NOT_FOUND";
        }
    });
    bool entered;
    {
        std::unique_lock lock(Fs->Mutex);
        entered = Fs->Changed.wait_for(lock, 5s, [&] {
            return Fs->Entered;
        });
    }
    auto other = std::async(std::launch::async, [&] {
        auto response = scans.Open(Db, "b", SecondCollection, TJson::object(), false, false);
        scans.Clear("b");
        return response.Status;
    });
    auto cancelling = std::async(std::launch::async, [&] {
        return scans.Cancel("a", test::collection_id, {{"cursor", token}}).Status;
    });
    const auto otherReady = other.wait_for(2s);
    const auto cancelReady = cancelling.wait_for(2s);
    Fs->Release(); // Always unblock before assertions/future destruction.
    EXPECT_TRUE(entered);
    EXPECT_EQ(otherReady, std::future_status::ready);
    EXPECT_EQ(cancelReady, std::future_status::ready);
    EXPECT_EQ(other.get(), 200);
    EXPECT_EQ(cancelling.get(), 200);
    EXPECT_TRUE(reading.get());
    EXPECT_THROW(scans.Cancel("a", test::collection_id, {{"cursor", token}}), TFailure);
}
TEST_F(ScanSessionsTest, ServiceShutdownRejectsNewScans)
{
    TRoutes routes(Db, {}, "secret");
    const TRequest request{"POST",
        std::string("/collections/") + test::collection_id + "/scan/open",
        {{"authorization", "Bearer secret"}, {"content-type", "application/json"}},
        "{}"};
    ASSERT_EQ(routes.Handle(request).Status, 200);
    routes.CancelScans();
    const auto response = routes.Handle(request);
    EXPECT_EQ(response.Status, 503) << response.Body;
    EXPECT_EQ(TJson::parse(response.Body)["error"]["code"], "CANCELLED");
    routes.CancelScans();
    EXPECT_EQ(routes.Handle(request).Status, 503);
}

TEST_F(ScanSessionsTest, DuplicateExecutingPageRefusesBeforeAcquiringWorkMemory)
{
    TScans scans;
    const auto token = Open(scans);
    {
        std::lock_guard lock(Fs->Mutex);
        Fs->Armed = true;
    }
    auto reading = std::async(std::launch::async, [&] {
        return scans.Open(Db, "a", test::collection_id, {{"cursor", token}}, false, false);
    });
    bool entered;
    {
        std::unique_lock lock(Fs->Mutex);
        entered = Fs->Changed.wait_for(lock, 5s, [&] {
            return Fs->Entered;
        });
    }
    const auto before = Resources->GetUsage().memory_bytes;
    std::optional<EScanRejection> reason;
    try {
        scans.Open(Db, "a", test::collection_id, {{"cursor", token}}, false, false);
    } catch (const TScanAdmissionFailure& error) {
        reason = error.Reason;
    }
    const auto after = Resources->GetUsage().memory_bytes;
    Fs->Release();
    const auto response = reading.get();
    EXPECT_TRUE(entered);
    EXPECT_EQ(reason, EScanRejection::SessionBusy);
    EXPECT_EQ(before, after);
    EXPECT_EQ(scans.Open(Db, "a", test::collection_id, {{"cursor", token}}, false, false).Body, response.Body);
    EXPECT_EQ(scans.GetStatistics()["rejections"]["session_busy"], 1);
}
TEST_F(ScanSessionsTest, ServiceShutdownCancelsSlowReadsWithoutWaiting)
{
    ASSERT_EQ(Db->AppendDurable({test::collection_id,
                                    SecondOperation,
                                    {},
                                    {Batch({{"alpha", 2, 2.5, false, 1}, {"zeta", 3, 3.5, false, 2}})}})
                  .disposition,
        CommitDisposition::Committed);
    TRoutes routes(Db, {}, "secret");
    TRequest request{"POST",
        std::string("/collections/") + test::collection_id + "/scan/open",
        {{"authorization", "Bearer secret"}, {"content-type", "application/json"}},
        R"({"predicate":{"op":"eq","field":"key","value":"same"}})"};
    const auto opened = routes.Handle(request);
    ASSERT_EQ(opened.Status, 200);
    const auto token = TJson::parse(opened.Body).at("cursor");
    request.Target = std::string("/collections/") + test::collection_id + "/scan";
    request.Body = TJson{{"cursor", token}}.dump();
    {
        std::lock_guard lock(Fs->Mutex);
        Fs->Armed = true;
        Fs->OpenedParquet.clear();
    }
    auto reading = std::async(std::launch::async, [&] {
        return routes.Handle(request);
    });
    bool entered;
    {
        std::unique_lock lock(Fs->Mutex);
        entered = Fs->Changed.wait_for(lock, 5s, [&] {
            return Fs->Entered;
        });
    }
    auto stopping = std::async(std::launch::async, [&] {
        routes.CancelScans();
    });
    const auto stopped = stopping.wait_for(2s);
    const auto owner = OpenExisting({(Temporary.path / "db").string(), test::store_id});
    Fs->Release();
    stopping.get();
    const auto response = reading.get();
    EXPECT_TRUE(entered);
    EXPECT_EQ(stopped, std::future_status::ready);
    ASSERT_FALSE(owner);
    EXPECT_EQ(owner.error().code, "STATE_IN_USE");
    EXPECT_EQ(response.Status, 404) << response.Body;
    EXPECT_EQ(TJson::parse(response.Body)["error"]["code"], "NOT_FOUND");
    std::lock_guard lock(Fs->Mutex);
    EXPECT_EQ(Fs->OpenedParquet.size(), 1);
}
TEST_F(ScanSessionsTest, ReplayingCompletedPageDoesNotRefreshIdleExpiry)
{
    TScans scans(300ms, 5s);
    const auto token = Open(scans);
    const auto first = scans.Open(Db, "a", test::collection_id, {{"cursor", token}}, false, false);
    std::this_thread::sleep_for(180ms);
    EXPECT_EQ(scans.Open(Db, "a", test::collection_id, {{"cursor", token}}, false, false).Body, first.Body);
    std::this_thread::sleep_for(180ms);
    EXPECT_THROW(scans.Open(Db, "a", test::collection_id, {{"cursor", token}}, false, false), TFailure);
}
TEST_F(ScanSessionsTest, AbsoluteLifetimeExpiresEvenBeforeIdleTimeout)
{
    TScans scans(5s, 200ms);
    const auto token = Open(scans);
    std::this_thread::sleep_for(120ms);
    EXPECT_EQ(scans.Open(Db, "a", test::collection_id, {{"cursor", token}}, false, false).Status, 200);
    std::this_thread::sleep_for(120ms);
    EXPECT_THROW(scans.Open(Db, "a", test::collection_id, {{"cursor", token}}, false, false), TFailure);
}
TEST_F(ScanSessionsTest, GlobalQuotaIsBoundedAndCancellationRestoresAdmission)
{
    TScans scans;
    std::vector<std::pair<std::string, std::string>> tokens;
    for (int database = 0; database < 8; ++database) {
        const auto name = std::to_string(database);
        for (int session = 0; session < 8; ++session) {
            tokens.emplace_back(name, Open(scans, name));
        }
    }
    try {
        Open(scans, "ninth");
        FAIL() << "global session quota was not enforced";
    } catch (const TFailure& error) {
        EXPECT_EQ(error.Detail.code, "RESOURCE_EXHAUSTED");
        EXPECT_EQ(error.Detail.retry_hint, RetryHint::SameIdAfterReconcile);
    }
    scans.Cancel(tokens.front().first, test::collection_id, {{"cursor", tokens.front().second}});
    EXPECT_FALSE(Open(scans, "ninth").empty());
    scans.Clear();
    EXPECT_FALSE(Open(scans).empty());
}
TEST_F(ScanSessionsTest, ConcurrentPagesKeepReservationsBeforeReading)
{
    TScans scans;
    std::vector<std::string> tokens;
    for (size_t index = 0; index < TScans::SessionLimit; ++index) {
        tokens.push_back(Open(scans, std::to_string(index / TScans::DatabaseSessionLimit)));
    }
    {
        std::lock_guard lock(Fs->Mutex);
        Fs->Armed = true;
    }
    std::vector<std::future<TResponse>> pages;
    for (size_t index = 0; index < 8; ++index) {
        pages.push_back(std::async(std::launch::async, [&, index] {
            return scans.Open(Db, "0", test::collection_id, {{"cursor", tokens[index]}}, false, false);
        }));
    }
    bool entered;
    {
        std::unique_lock lock(Fs->Mutex);
        entered = Fs->Changed.wait_for(lock, 5s, [&] {
            return Fs->Entered == 8;
        });
    }
    auto refused = std::async(std::launch::async, [&] {
        try {
            Open(scans, "overflow");
            return std::string("admitted");
        } catch (const TFailure& error) {
            return error.Detail.code;
        }
    });
    const auto ready = refused.wait_for(1s);
    Fs->Release();
    EXPECT_TRUE(entered);
    EXPECT_EQ(ready, std::future_status::ready);
    EXPECT_EQ(refused.get(), "RESOURCE_EXHAUSTED");
    for (auto& page : pages) {
        EXPECT_EQ(page.get().Status, 200);
    }
    // Existing cursors already own their replay capacity, even at the ceiling.
    EXPECT_EQ(scans.Open(Db, "1", test::collection_id, {{"cursor", tokens[8]}}, false, false).Status, 200);
}
class ReplayAdmissionTest
    : public ScanSessionsTest
    , public ::testing::WithParamInterface<bool>
{ };
TEST_P(ReplayAdmissionTest, AdmissionRefusalPreservesCursorAndLastReplay)
{
    const std::string collection = "00000000-0000-4000-8000-0000000000c3";
    SchemaSpec schema{{{"key", FieldType::String, false}}};
    TJson row{{"key", "first"}};
    for (int column = 0; column < 9; ++column) {
        const auto name = "s" + std::to_string(column);
        schema.fields.push_back({name, FieldType::String, true});
        row[name] = std::string(75000, '\x01');
    }
    ASSERT_EQ(Db->CreateCollection({collection, "large", schema}).disposition, CommitDisposition::Committed);
    auto second = row;
    second["key"] = "second";
    const auto append =
        Db->AppendDurable({collection, SecondOperation, {}, {ParseRows(schema, TJson::array({row, second}))}});
    ASSERT_EQ(append.disposition, CommitDisposition::Committed);
    TScans scans(5min, 5min);
    auto token = Open(scans, "victim", collection);
    std::string replayToken, replayBody;
    if (GetParam()) {
        replayToken = token;
        replayBody = scans.Open(Db, "victim", collection, {{"cursor", token}}, false, false).Body;
        token = TJson::parse(replayBody).at("next_cursor");
    }
    std::vector<std::pair<std::string, std::string>> tokens;
    for (size_t index = 0; index < TScans::SessionLimit - 1; ++index) {
        const auto database = std::to_string(index / TScans::DatabaseSessionLimit);
        tokens.emplace_back(database, Open(scans, database, collection));
    }
    for (int retry = 0; retry < 2; ++retry) {
        try {
            Open(scans, "overflow", collection);
            FAIL() << "new cursor admitted without replay capacity";
        } catch (const TFailure& error) {
            EXPECT_EQ(error.Detail.code, "RESOURCE_EXHAUSTED");
            EXPECT_EQ(error.Detail.retry_hint, RetryHint::SameIdAfterReconcile);
        }
        if (!replayToken.empty()) {
            EXPECT_EQ(scans.Open(Db, "victim", collection, {{"cursor", replayToken}}, false, false).Body, replayBody);
        }
    }
    const auto owner = Db->GetResourceOwner();
    auto pressure =
        owner->Reserve(ResourceClass::Data, owner->GetLimits().memory_bytes[0] - owner->GetUsage().memory_bytes[0]);
    ASSERT_TRUE(pressure);
    try {
        scans.Open(Db, "victim", collection, {{"cursor", token}}, false, false);
        FAIL() << "page admitted while shared data memory was full";
    } catch (const TFailure& error) {
        EXPECT_EQ(error.Detail.code, "RESOURCE_EXHAUSTED");
    }
    pressure->reset();
    if (!replayToken.empty()) {
        EXPECT_EQ(scans.Open(Db, "victim", collection, {{"cursor", replayToken}}, false, false).Body, replayBody);
    }
    scans.Cancel(tokens.front().first, collection, {{"cursor", tokens.front().second}});
    EXPECT_FALSE(Open(scans, "overflow", collection).empty());
    // A later append must not enter the refused cursor's already fixed view.
    ASSERT_EQ(Db->AppendDurable({collection, ThirdOperation, {}, {ParseRows(schema, TJson::array({row}))}}).disposition,
        CommitDisposition::Committed);
    const auto resumed = scans.Open(Db, "victim", collection, {{"cursor", token}}, false, false);
    const auto page = TJson::parse(resumed.Body);
    ASSERT_EQ(page.at("rows").size(), 1);
    EXPECT_EQ(page.at("rows").at(0), GetParam() ? second : row);
    EXPECT_EQ(page.at("snapshot_id"), std::to_string(test::Present(append.result).snapshot_id));
    EXPECT_EQ(page.at("next_cursor").is_null(), GetParam());
    EXPECT_EQ(scans.Open(Db, "victim", collection, {{"cursor", token}}, false, false).Body, resumed.Body);
    scans.Clear();
    const auto reopened = Open(scans, "victim", collection);
    EXPECT_EQ(scans.Open(Db, "victim", collection, {{"cursor", reopened}}, false, false).Status, 200);
}
INSTANTIATE_TEST_SUITE_P(FirstAndNextPage, ReplayAdmissionTest, ::testing::Bool());
class LargeReplayScanSessionsTest : public ScanSessionsTest
{
    void SetUp() override
    {
        // Thirty-three retained readers each keep a 256 MiB footer envelope.
        ResourceLimits limits;
        limits.memory_bytes[0] = 4ULL << 30;
        limits.memory_bytes[1] = 16ULL << 30;
        Resources = std::make_shared<ResourceOwner>(limits);
        ScanSessionsTest::SetUp();
    }
};
TEST_F(LargeReplayScanSessionsTest, EveryAdmittedLargePageRetainsItsReplayAndNextCursor)
try {
    const std::string collection = "00000000-0000-4000-8000-0000000000c3";
    SchemaSpec schema{{{"key", FieldType::String, false}}};
    TJson row{{"key", "same"}};
    schema.fields.push_back({"payload", FieldType::String, true});
    row["payload"] = std::string(700000, '\x01');
    ASSERT_EQ(Db->CreateCollection({collection, "large", schema}).disposition, CommitDisposition::Committed);
    const auto append =
        Db->AppendDurable({collection, SecondOperation, {}, {ParseRows(schema, TJson::array({row, row}))}});
    ASSERT_EQ(append.disposition, CommitDisposition::Committed);
    TScans scans(5min, 5min);
    std::vector<std::pair<std::string, std::string>> tokens;
    for (int index = 0; index < 32; ++index) {
        const auto database = std::to_string(index / 8);
        const auto token = Open(scans, database, collection);
        const auto response = scans.Open(Db, database, collection, {{"cursor", token}}, false, false);
        ASSERT_GT(response.Body.size(), 4U * 1024 * 1024);
        const auto replay = scans.Open(Db, database, collection, {{"cursor", token}}, false, false);
        EXPECT_EQ(replay.Body, response.Body);
        const auto envelope = TJson::parse(response.Body.substr(0, response.Body.rfind(",\"rows\":")) + "}");
        tokens.emplace_back(database, envelope.at("next_cursor").get<std::string>());
    }
    const auto fifth = Open(scans, "fifth", collection);
    EXPECT_EQ(scans.Open(Db, "fifth", collection, {{"cursor", fifth}}, false, false).Status, 200);
    for (const auto& [database, token] : tokens) {
        const auto last = scans.Open(Db, database, collection, {{"cursor", token}}, false, false);
        const auto envelope = TJson::parse(last.Body.substr(0, last.Body.rfind(",\"rows\":")) + "}");
        EXPECT_TRUE(envelope.at("next_cursor").is_null());
        EXPECT_GT(last.Body.size(), 4U * 1024 * 1024);
    }
} catch (const TFailure& error) {
    const auto usage = Db->GetResourceOwner()->GetUsage();
    FAIL() << error.Detail.code << ": " << error.Detail.message << "; data peak=" << usage.memory_peak[0]
           << "; metadata peak=" << usage.memory_peak[1];
}

TEST_F(ScanSessionsTest, ActiveScansCanUseIdleExpiryWithoutAnAbsoluteLifetime)
{
    TScans scans(1s, 0ms);
    const auto token = Open(scans);
    EXPECT_EQ(scans.Open(Db, "a", test::collection_id, {{"cursor", token}}, false, false).Status, 200);
}

TEST_F(ScanSessionsTest, CompletedRetentionEvictsOldestAndReturnsAllReservations)
{
    const auto baseline = Resources->GetUsage().memory_bytes;
    TScanLimits limits;
    limits.Completed = 1;
    limits.CompletedPerDatabase = 1;
    TScans scans(limits, Resources);
    const auto first = Open(scans);
    const auto second = Open(scans);
    EXPECT_GE(Resources->GetUsage().memory_bytes[0] - baseline[0], 2 * (32ULL << 20));
    EXPECT_EQ(scans.Open(Db, "a", test::collection_id, {{"cursor", first}}, false, false).Status, 200);
    auto page = scans.Open(Db, "a", test::collection_id, {{"cursor", second}}, false, false);
    EXPECT_THROW(scans.Open(Db, "a", test::collection_id, {{"cursor", first}}, false, false), TFailure);
    EXPECT_EQ(scans.Open(Db, "a", test::collection_id, {{"cursor", second}}, false, false).Body, page.Body);
    const auto stats = scans.GetStatistics("a");
    EXPECT_EQ(stats["global"]["live_cursors"], 0);
    EXPECT_EQ(stats["global"]["completed_replays"], 1);
    EXPECT_EQ(stats["completed_evictions"], 1);
    scans.Clear();
    scans.Clear();
    // The independently owned outgoing page remains charged after cache removal.
    EXPECT_GT(Resources->GetUsage().memory_bytes[0], baseline[0]);
    EXPECT_EQ(scans.GetStatistics()["global"]["replay_reserved_bytes"], 0);
    page = {};
    EXPECT_EQ(Resources->GetUsage().memory_bytes, baseline);
}

TEST_F(ScanSessionsTest, ConcurrentEofKeepsCompletedQuotaAndReleasesAllCopies)
{
    const auto baseline = Resources->GetUsage().memory_bytes;
    TScanLimits limits;
    limits.Completed = 1;
    limits.CompletedPerDatabase = 1;
    TScans scans(limits, Resources);
    std::vector<std::string> tokens;
    tokens.reserve(8);
    for (int index = 0; index < 8; ++index) {
        tokens.push_back(Open(scans));
    }
    std::promise<void> start;
    const auto ready = start.get_future().share();
    std::vector<std::future<TResponse>> pages;
    pages.reserve(tokens.size());
    for (const auto& token : tokens) {
        pages.push_back(std::async(std::launch::async, [&, cursorToken = token] {
            ready.wait();
            return scans.Open(Db, "a", test::collection_id, {{"cursor", cursorToken}}, false, false);
        }));
    }
    start.set_value();
    for (auto& page : pages) {
        const auto response = page.get();
        EXPECT_EQ(response.Status, 200);
        EXPECT_EQ(TJson::parse(response.Body)["rows"].size(), 1);
    }
    const auto stats = scans.GetStatistics();
    EXPECT_EQ(stats["global"]["live_cursors"], 0);
    EXPECT_LE(stats["global"]["completed_replays"].get<size_t>(), 1);
    EXPECT_GE(stats["completed_evictions"].get<size_t>(), 7);
    scans.Clear();
    EXPECT_EQ(Resources->GetUsage().memory_bytes, baseline);
}

TEST_F(ScanSessionsTest, ZeroCompletedRetentionReturnsFinalPageWithoutRetainingCursor)
{
    TScanLimits limits;
    limits.Completed = 0;
    limits.CompletedPerDatabase = 0;
    TScans scans(limits, Resources);
    const auto token = Open(scans);
    const auto page = TJson::parse(scans.Open(Db, "a", test::collection_id, {{"cursor", token}}, false, false).Body);
    EXPECT_EQ(page["rows"].size(), 1);
    EXPECT_TRUE(page["next_cursor"].is_null());
    EXPECT_THROW(scans.Open(Db, "a", test::collection_id, {{"cursor", token}}, false, false), TFailure);
    EXPECT_EQ(scans.GetStatistics()["global"]["completed_replays"], 0);
}

TEST_F(ScanSessionsTest, NewAdmissionReclaimsCompletedReplaysBeforeWorkingMemoryRefusal)
{
    const auto baseline = Resources->GetUsage().memory_bytes;
    TScans scans;
    for (int index = 0; index < 6; ++index) {
        const auto token = Open(scans);
        scans.Open(Db, "a", test::collection_id, {{"cursor", token}}, false, false);
    }
    ASSERT_EQ(scans.GetStatistics()["global"]["completed_replays"], 6);
    // In addition to assembly/output/replay, the new core cursor needs its
    // independently accounted 1 MiB selection envelope.
    auto pressure = Resources->Reserve(ResourceClass::Data,
        Resources->GetLimits().memory_bytes[0] - Resources->GetUsage().memory_bytes[0] - (1ULL << 20));
    ASSERT_TRUE(pressure);
    const auto token = Open(scans);
    EXPECT_FALSE(token.empty());
    const auto stats = scans.GetStatistics();
    EXPECT_EQ(stats["global"]["completed_replays"], 0);
    EXPECT_EQ(stats["global"]["live_cursors"], 1);
    EXPECT_EQ(stats["completed_evictions"], 6);
    scans.Clear();
    pressure->reset();
    EXPECT_EQ(Resources->GetUsage().memory_bytes, baseline);
}

TEST_F(ScanSessionsTest, RouteAdmissionCanReclaimCompletedReplayMemory)
{
    TRoutes routes(Db, {}, "secret");
    const auto path = std::string("/collections/") + test::collection_id + "/scan";
    const std::map<std::string, std::string> headers{{"authorization", "Bearer secret"},
        {"content-type", "application/json"}};
    for (int index = 0; index < 6; ++index) {
        ASSERT_EQ(routes.Handle({"POST", path, headers, "{}"}).Status, 200);
    }
    auto pressure = Resources->Reserve(ResourceClass::Data,
        Resources->GetLimits().memory_bytes[0] - Resources->GetUsage().memory_bytes[0] - (1ULL << 20));
    ASSERT_TRUE(pressure);
    const auto admitted = routes.Handle({"POST", path + "/open", headers, "{}"});
    EXPECT_EQ(admitted.Status, 200) << admitted.Body;
    routes.CancelScans();
}

TEST_F(ScanSessionsTest, ProtectedCancellationWorksWithEveryOrdinaryClassFull)
{
    TRoutes routes(Db, {}, "secret");
    const auto path = std::string("/collections/") + test::collection_id + "/scan";
    const std::map<std::string, std::string> headers{{"authorization", "Bearer secret"},
        {"content-type", "application/json"}};
    const auto opened = routes.Handle({"POST", path + "/open", headers, "{}"});
    ASSERT_EQ(opened.Status, 200);
    std::vector<std::shared_ptr<ResourceReservation>> pressure;
    for (const auto resource : {ResourceClass::Data, ResourceClass::Metadata, ResourceClass::Control}) {
        const auto index = static_cast<size_t>(resource);
        pressure.push_back(Unwrap(Resources->Reserve(resource,
            Resources->GetLimits().memory_bytes[index] - Resources->GetUsage().memory_bytes[index])));
    }
    const auto response = routes.Handle(
        {"POST", path + "/cancel", headers, TJson{{"cursor", TJson::parse(opened.Body).at("cursor")}}.dump()});
    EXPECT_EQ(response.Status, 200) << response.Body;
    EXPECT_TRUE(TJson::parse(response.Body).at("cancelled"));
}

TEST_F(ScanSessionsTest, MetadataAdmissionRefusalPreservesTheOriginalCursor)
{
    TScans scans;
    const auto token = Open(scans);
    auto pressure = Resources->Reserve(ResourceClass::Metadata,
        Resources->GetLimits().memory_bytes[1] - Resources->GetUsage().memory_bytes[1]);
    ASSERT_TRUE(pressure);
    std::optional<EScanRejection> reason;
    try {
        scans.Open(Db, "a", test::collection_id, {{"cursor", token}}, false, false);
    } catch (const TFailure& error) {
        EXPECT_EQ(error.Detail.code, "RESOURCE_EXHAUSTED");
        if (const auto* admission = dynamic_cast<const TScanAdmissionFailure*>(&error)) {
            reason = admission->Reason;
        }
    }
    pressure->reset();
    EXPECT_TRUE(reason.has_value());
    EXPECT_EQ(scans.GetStatistics()["global"]["live_cursors"], 1);
    const auto result = scans.Open(Db, "a", test::collection_id, {{"cursor", token}}, false, false);
    EXPECT_EQ(TJson::parse(result.Body)["rows"].size(), 1);
    EXPECT_EQ(scans.Open(Db, "a", test::collection_id, {{"cursor", token}}, false, false).Body, result.Body);
}

class PageRollbackTest
    : public ScanSessionsTest
    , public ::testing::WithParamInterface<bool>
{ };

TEST_P(PageRollbackTest, DecoderAdmissionCommitsAvailableRowsAndPreservesContinuation)
{
    ASSERT_EQ(Db->AppendDurable({test::collection_id, SecondOperation, {}, {Batch({{"other", 2, 2.5, false, 1}})}})
                  .disposition,
        CommitDisposition::Committed);
    const bool arrow = GetParam();
    TScans scans;
    const auto opened = TJson::parse(scans.Open(Db, "a", test::collection_id, TJson::object(), arrow, true).Body);
    const auto token = opened.at("cursor").get<std::string>();
    {
        std::lock_guard lock(Fs->Mutex);
        Fs->Armed = true;
    }
    auto reading = std::async(std::launch::async, [&] {
        return scans.Open(Db, "a", test::collection_id, {{"cursor", token}}, arrow, false);
    });
    bool entered;
    {
        std::unique_lock lock(Fs->Mutex);
        entered = Fs->Changed.wait_for(lock, 5s, [&] {
            return Fs->Entered;
        });
    }
    auto pressure = Resources->Reserve(ResourceClass::Metadata,
        Resources->GetLimits().memory_bytes[1] - Resources->GetUsage().memory_bytes[1]);
    Fs->Release();
    const auto partial = reading.get();
    ASSERT_TRUE(pressure);
    pressure->reset();
    ASSERT_TRUE(entered);
    EXPECT_EQ(scans.GetStatistics()["global"]["live_cursors"], 1);
    EXPECT_EQ(scans.Open(Db, "a", test::collection_id, {{"cursor", token}}, arrow, false).Body, partial.Body);
    const auto next = arrow ? partial.Headers.at("X-GlamDB-Next-Cursor")
                            : TJson::parse(partial.Body).at("next_cursor").get<std::string>();
    const auto resumed = scans.Open(Db, "a", test::collection_id, {{"cursor", next}}, arrow, false);
    EXPECT_EQ(scans.Open(Db, "a", test::collection_id, {{"cursor", next}}, arrow, false).Body, resumed.Body);
    std::set<std::string> keys;
    size_t count = 0;
    for (const auto* response : {&partial, &resumed}) {
        if (arrow) {
            auto input = arrow::io::BufferReader::FromString(response->Body);
            const auto reader = arrow::ipc::RecordBatchStreamReader::Open(input.get());
            ASSERT_TRUE(reader.ok());
            for (;;) {
                const auto batch = (*reader)->Next();
                ASSERT_TRUE(batch.ok());
                if (!*batch) {
                    break;
                }
                const auto column = std::static_pointer_cast<arrow::StringArray>((*batch)->column(0));
                for (int64_t row = 0; row < column->length(); ++row) {
                    ++count;
                    keys.insert(column->GetString(row));
                }
            }
            EXPECT_EQ(response->Headers.at("X-GlamDB-Snapshot-Id"), opened.at("snapshot_id").get<std::string>());
        } else {
            const auto page = TJson::parse(response->Body);
            for (const auto& row : page.at("rows")) {
                ++count;
                keys.insert(row.at("key").get<std::string>());
            }
            EXPECT_EQ(page.at("snapshot_id"), opened.at("snapshot_id"));
        }
    }
    EXPECT_EQ(count, 2);
    EXPECT_EQ(keys, (std::set<std::string>{"same", "other"}));
}
INSTANTIATE_TEST_SUITE_P(JsonAndArrow, PageRollbackTest, ::testing::Bool());

class NarrowScanProfileTest : public ScanSessionsTest
{
    void SetUp() override
    {
        ResourceLimits limits;
        limits.memory_bytes[1] = 1100ULL << 20;
        Resources = std::make_shared<ResourceOwner>(limits);
        ScanSessionsTest::SetUp();
    }
};

TEST_F(NarrowScanProfileTest, TinyWideBatchesMakeProgressWithoutExternalRelease)
{
    constexpr char Collection[] = "00000000-0000-4000-8000-0000000000b3";
    SchemaSpec schema{{{"key", FieldType::String, false}}};
    std::vector<std::shared_ptr<arrow::Field>> fields{arrow::field("key", arrow::utf8(), false)};
    std::vector<std::optional<std::string>> keys;
    std::vector<std::optional<bool>> flags;
    for (size_t row = 0; row < 10000; ++row) {
        keys.emplace_back(std::to_string(row));
        flags.emplace_back(row % 2 == 0);
    }
    std::vector<std::shared_ptr<arrow::Array>> columns{Column<arrow::StringBuilder>(keys, arrow::utf8())};
    const auto values = Column<arrow::BooleanBuilder>(flags, arrow::boolean());
    for (size_t field = 1; field < 64; ++field) {
        const auto name = "f" + std::to_string(field);
        schema.fields.push_back({name, FieldType::Bool, true});
        fields.push_back(arrow::field(name, arrow::boolean()));
        columns.push_back(values);
    }
    ASSERT_EQ(Db->CreateCollection({Collection, "wide", schema}).disposition, CommitDisposition::Committed);
    const auto input = arrow::RecordBatch::Make(arrow::schema(fields), 10000, columns);
    ASSERT_EQ(Db->AppendDurable({Collection, ThirdOperation, {}, {input}}).disposition, CommitDisposition::Committed);
    const auto baseline = Resources->GetUsage().memory_bytes;
    {
        TScanLimits limits;
        limits.Live = limits.LivePerDatabase = 1;
        TScans scans(limits, Resources);
        auto token = TJson::parse(scans.Open(Db, "a", Collection, {{"batch_rows", 1}}, true, true).Body)
                         .at("cursor")
                         .get<std::string>();
        std::set<std::string> seen;
        size_t rows = 0;
        for (size_t page = 0; page < 100 && !token.empty(); ++page) {
            TResponse response;
            ASSERT_NO_THROW(response = scans.Open(Db, "a", Collection, {{"cursor", token}}, true, false));
            const auto replay = scans.Open(Db, "a", Collection, {{"cursor", token}}, true, false);
            EXPECT_EQ(replay.Body, response.Body);
            EXPECT_EQ(replay.Headers, response.Headers);
            auto stream = arrow::io::BufferReader::FromString(response.Body);
            const auto reader = arrow::ipc::RecordBatchStreamReader::Open(stream.get());
            ASSERT_TRUE(reader.ok());
            size_t pageRows = 0;
            for (;;) {
                const auto batch = (*reader)->Next();
                ASSERT_TRUE(batch.ok());
                if (!*batch) {
                    break;
                }
                const auto key = std::static_pointer_cast<arrow::StringArray>((*batch)->column(0));
                for (int64_t row = 0; row < key->length(); ++row) {
                    seen.insert(key->GetString(row));
                    ++pageRows;
                }
            }
            EXPECT_GT(pageRows, 0);
            rows += pageRows;
            const auto next = response.Headers.find("X-GlamDB-Next-Cursor");
            token = next == response.Headers.end() ? std::string{} : next->second;
        }
        EXPECT_TRUE(token.empty());
        EXPECT_EQ(rows, 10000);
        EXPECT_EQ(seen.size(), 10000);
        for (size_t row = 0; row < 10000; ++row) {
            EXPECT_TRUE(seen.contains(std::to_string(row)));
        }
        scans.Clear("a");
        EXPECT_EQ(scans.GetStatistics()["global"]["live_cursors"], 0);
    }
    EXPECT_EQ(Resources->GetUsage().memory_bytes, baseline);
}

TEST_F(ScanSessionsTest, ReplayByteBoundariesAreIndependentOfLiveQuota)
{
    TScanLimits limits;
    limits.Live = 4;
    limits.LivePerDatabase = 4;
    limits.ReplayBytes = 2 * MaxResponseBytes;
    limits.ReplayBytesPerDatabase = MaxResponseBytes;
    TScans scans(limits, Resources);
    const auto token = Open(scans);
    try {
        Open(scans);
        FAIL() << "database replay quota was not enforced";
    } catch (const TScanAdmissionFailure& error) {
        EXPECT_EQ(error.Reason, EScanRejection::DatabaseReplay);
    }
    Open(scans, "b");
    try {
        Open(scans, "c");
        FAIL() << "global replay quota was not enforced";
    } catch (const TScanAdmissionFailure& error) {
        EXPECT_EQ(error.Reason, EScanRejection::GlobalReplay);
    }
    EXPECT_EQ(scans.Open(Db, "a", test::collection_id, {{"cursor", token}}, false, false).Status, 200);
    // Completed replay reclamation never cancels the other live cursor.
    EXPECT_FALSE(Open(scans, "c").empty());
    EXPECT_EQ(scans.GetStatistics()["global"]["live_cursors"], 2);
}

TEST_F(ScanSessionsTest, CancellationAndCompletedExpiryRestoreSharedOccupancy)
{
    const auto baseline = Resources->GetUsage().memory_bytes;
    TScanLimits limits;
    limits.CompletedTtl = 100ms;
    TScans scans(limits, Resources);
    auto token = Open(scans);
    scans.Cancel("a", test::collection_id, {{"cursor", token}});
    EXPECT_EQ(Resources->GetUsage().memory_bytes, baseline);
    token = Open(scans);
    scans.Open(Db, "a", test::collection_id, {{"cursor", token}}, false, false);
    std::this_thread::sleep_for(300ms);
    EXPECT_EQ(scans.GetStatistics()["global"]["completed_replays"], 0);
    EXPECT_EQ(Resources->GetUsage().memory_bytes, baseline);
}

TEST(ScanLimits, RejectsInvalidAndInfeasibleProfiles)
{
    TScanLimits limits;
    limits.LivePerDatabase = 65;
    EXPECT_THROW((void)TScans{limits}, TFailure);
    limits = {};
    limits.ReplayBytesPerDatabase = MaxResponseBytes - 1;
    EXPECT_THROW((void)TScans{limits}, TFailure);
    limits = {};
    limits.Idle = 0ms;
    EXPECT_THROW((void)TScans{limits}, TFailure);
    limits = {};
    limits.CompletedPerDatabase = 65;
    EXPECT_THROW((void)TScans{limits}, TFailure);
    limits = {};
    limits.LivePerDatabase = 32;
    limits.ReplayBytesPerDatabase = 32 * MaxResponseBytes;
    EXPECT_THROW(TScans(limits, std::make_shared<ResourceOwner>()), TFailure);
}

TEST_F(ScanSessionsTest, ReadingAPageIsActivityAcrossTheIdleDeadline)
{
    TScans scans(150ms, 0ms);
    const auto token = Open(scans);
    {
        std::lock_guard lock(Fs->Mutex);
        Fs->Armed = true;
    }
    auto reading = std::async(std::launch::async, [&] {
        try {
            return scans.Open(Db, "a", test::collection_id, {{"cursor", token}}, false, false).Status;
        } catch (const TFailure&) {
            return 404;
        }
    });
    bool entered;
    {
        std::unique_lock lock(Fs->Mutex);
        entered = Fs->Changed.wait_for(lock, 5s, [&] {
            return Fs->Entered;
        });
    }
    std::this_thread::sleep_for(350ms);
    Fs->Release();
    EXPECT_TRUE(entered);
    EXPECT_EQ(reading.get(), 200);
    EXPECT_EQ(scans.Open(Db, "a", test::collection_id, {{"cursor", token}}, false, false).Status, 200);
}
TEST(ScanRowsJson, BatchEncodingPreservesTypesAndBudgetedSlices)
{
    auto batch = Batch({{"a\n\"", -4, 2.5, true, -1}, {"same", {}, {}, {}, {}}, {"same", {}, {}, {}, {}}});
    std::string output = "[";
    size_t consumed = 0;
    while (consumed < static_cast<size_t>(batch->num_rows())) {
        const auto rows = AppendRowsJson(*batch->Slice(static_cast<int64_t>(consumed)),
            output,
            {.MaxRows = 1, .TargetBytes = 1, .HardBytes = MaxResponseBytes});
        // Each first oversized target row is allowed only for an empty page.
        if (rows == 0) {
            break;
        }
        consumed += rows;
    }
    EXPECT_EQ(consumed, 1);
    output += ']';
    EXPECT_EQ(TJson::parse(output), EncodeRows(*batch->Slice(0, 1)));
    std::string all = "[";
    EXPECT_EQ(
        AppendRowsJson(*batch, all, {.MaxRows = 3, .TargetBytes = MaxResponseBytes, .HardBytes = MaxResponseBytes}),
        3);
    all += ']';
    EXPECT_EQ(TJson::parse(all), EncodeRows(*batch));
}
} // namespace
} // namespace glamdb::NServer
