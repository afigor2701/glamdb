#include "apps/server/routes.h"
#include "apps/server/scans.h"
#include "tests/support/checked.h"
#include "tests/vertical/vertical_support.h"

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
    std::shared_ptr<Engine> Db;
    void SetUp() override
    {
        const detail::BackendFactory factory =
            [fs = Fs](const std::filesystem::path& path,
                const StoreInfo& info) -> Result<std::shared_ptr<detail::StorageBackend>> {
            detail::TLocalStorageOptions options;
            options.Fs = fs;
            auto backend = detail::TLocalStorageBackend::Open(path, info, options);
            if (!backend) {
                return std::unexpected(backend.error());
            }
            return std::shared_ptr<detail::StorageBackend>(std::move(*backend));
        };
        auto created = detail::EngineFactory::CreateWith({test::store_id, (Temporary.path / "db").string()}, factory);
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
TEST_F(ScanSessionsTest, ServiceShutdownCancelsSlowReadsWithoutWaiting)
{
    ASSERT_EQ(Db->AppendDurable({test::collection_id, SecondOperation, {}, {Batch({{"other", 2, 2.5, false, 1}})}})
                  .disposition,
        CommitDisposition::Committed);
    TRoutes routes(Db, {}, "secret");
    TRequest request{"POST",
        std::string("/collections/") + test::collection_id + "/scan/open",
        {{"authorization", "Bearer secret"}, {"content-type", "application/json"}},
        R"({"predicate":{"op":"eq","field":"key","value":"absent"}})"};
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
TEST_F(ScanSessionsTest, ConcurrentPagesReserveReplayCapacityBeforeReading)
{
    TScans scans;
    std::vector<std::string> tokens;
    tokens.reserve(8);
    for (int index = 0; index < 8; ++index) {
        tokens.push_back(Open(scans));
    }
    const auto waiting = Open(scans, "b");
    {
        std::lock_guard lock(Fs->Mutex);
        Fs->Armed = true;
    }
    std::vector<std::future<TResponse>> pages;
    pages.reserve(tokens.size());
    for (size_t index = 0; index < tokens.size(); ++index) {
        pages.push_back(std::async(std::launch::async, [&, index] {
            return scans.Open(Db, "a", test::collection_id, {{"cursor", tokens[index]}}, false, false);
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
            scans.Open(Db, "b", test::collection_id, {{"cursor", waiting}}, false, false);
            return std::string("advanced");
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
    EXPECT_EQ(scans.Open(Db, "b", test::collection_id, {{"cursor", waiting}}, false, false).Status, 200);
}
class ReplayAdmissionTest
    : public ScanSessionsTest
    , public ::testing::WithParamInterface<bool>
{ };
TEST_P(ReplayAdmissionTest, RefusalPreservesCursorAndLastReplay)
{
    const std::string collection = "00000000-0000-4000-8000-0000000000c3";
    SchemaSpec schema{{{"key", FieldType::String, false}}};
    TJson row{{"key", "first"}};
    for (int column = 0; column < 9; ++column) {
        const auto name = "s" + std::to_string(column);
        schema.fields.push_back({name, FieldType::String, true});
        row[name] = std::string(175000, '\x01');
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
    bool full = false;
    for (int index = 0; index < 32; ++index) {
        const auto database = std::to_string(index / 8);
        try {
            const auto response = scans.Open(Db, database, collection, TJson::object(), false, false);
            ASSERT_GT(response.Body.size(), 9000000);
            tokens.emplace_back(database, TJson::parse(response.Body).at("next_cursor").get<std::string>());
        } catch (const TFailure& error) {
            EXPECT_EQ(error.Detail.code, "RESOURCE_EXHAUSTED");
            full = true;
            break;
        }
    }
    ASSERT_TRUE(full) << "retained replay byte budget was not enforced";
    ASSERT_FALSE(tokens.empty());
    for (int retry = 0; retry < 2; ++retry) {
        try {
            scans.Open(Db, "victim", collection, {{"cursor", token}}, false, false);
            FAIL() << "page advanced without replay capacity";
        } catch (const TFailure& error) {
            EXPECT_EQ(error.Detail.code, "RESOURCE_EXHAUSTED");
            EXPECT_EQ(error.Detail.retry_hint, RetryHint::SameIdAfterReconcile);
        }
        if (!replayToken.empty()) {
            EXPECT_EQ(scans.Open(Db, "victim", collection, {{"cursor", replayToken}}, false, false).Body, replayBody);
        }
    }
    scans.Cancel(tokens.front().first, collection, {{"cursor", tokens.front().second}});
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
