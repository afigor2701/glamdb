#include "apps/server/scans.h"

#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>
#include <arrow/util/byte_size.h>
#include <openssl/evp.h>

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <random>
#include <thread>
#include <unordered_map>

namespace glamdb::NServer {
using namespace NTransport;
namespace {
constexpr size_t MaxSessions = 64;
constexpr std::ptrdiff_t MaxDatabaseSessions = 8;
constexpr size_t MaxReplayBytes = size_t{128} * 1024 * 1024;
constexpr size_t PageRows = 10000;
constexpr size_t PageBytes = size_t{4} * 1024 * 1024;
int64_t Now()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}
std::string Token()
{
    std::random_device random;
    std::string value;
    constexpr char Hex[] = "0123456789abcdef";
    for (size_t i = 0; i < 32; ++i) {
        value += Hex[random() & 15];
    }
    return value;
}
std::string Checksum(std::string_view bytes)
{
    unsigned char digest[EVP_MAX_MD_SIZE]{};
    unsigned int size = 0;
    Require(EVP_Digest(bytes.data(), bytes.size(), digest, &size, EVP_sha256(), nullptr) == 1 && size == 32,
        "cannot checksum Arrow response",
        "INTERNAL");
    constexpr char Hex[] = "0123456789abcdef";
    std::string result;
    result.reserve(64);
    for (unsigned int i = 0; i < size; ++i) {
        result += Hex[digest[i] >> 4];
        result += Hex[digest[i] & 15];
    }
    return result;
}
class TBoundedOutput final : public arrow::io::OutputStream
{
public:
    arrow::Status Close() override
    {
        Closed_ = true;
        return arrow::Status::OK();
    }
    bool closed() const override
    {
        return Closed_;
    }
    arrow::Result<int64_t> Tell() const override
    {
        return static_cast<int64_t>(Bytes.size());
    }
    arrow::Status Write(const void* data, int64_t size) override
    {
        if (Closed_ || size < 0 || static_cast<uint64_t>(size) > MaxResponseBytes - Bytes.size()) {
            return arrow::Status::CapacityError("Arrow response exceeds transport limit");
        }
        Bytes.append(static_cast<const char*>(data), static_cast<size_t>(size));
        return arrow::Status::OK();
    }
    std::string Bytes;

private:
    bool Closed_ = false;
};
TJson Snapshot(const ScanInfo& info)
{
    return info.selector_resolved.kind == SnapshotSelectorKind::Id
        ? TJson(std::to_string(info.selector_resolved.snapshot_id))
        : TJson(nullptr);
}
template <class T>
T ArrowValue(arrow::Result<T> value)
{
    Require(value.ok(), "cannot assemble Arrow scan page", "RESOURCE_EXHAUSTED");
    return std::move(value).ValueUnsafe();
}
int64_t IpcSize(const arrow::RecordBatch& batch)
{
    int64_t size = 0;
    Require(arrow::ipc::GetRecordBatchSize(batch, &size).ok(), "cannot size Arrow scan page", "INTERNAL");
    return size;
}
} // namespace

struct TScans::TImpl
{
    struct TSession
    {
        std::mutex Mutex;
        std::string Database, Collection, NextToken, ReplayToken;
        bool Arrow = false, Ended = false;
        std::atomic<bool> Cancelled{false}, Complete{false};
        const int64_t Created = Now();
        std::atomic<int64_t> Touched{Created};
        // LLVM 18 supports the C++23 shared_ptr atomic free functions.
        std::shared_ptr<ScanCursor> Cursor;
        std::deque<std::shared_ptr<arrow::RecordBatch>> Pending;
        ScanInfo Info;
        TJson Schema;
        TResponse Replay;
        size_t ChargedBytes = 0;
        ~TSession()
        {
            if (const auto cursor = std::atomic_load(&Cursor)) {
                cursor->Cancel();
            }
        }
    };
    using TPtr = std::shared_ptr<TSession>;
    // Registry protection never covers planning, cursor I/O, encoding, hashing,
    // response copying or waiting for a session mutex.
    std::mutex Mutex;
    std::condition_variable Wake;
    bool Stopping = false, Accepting = true;
    size_t ReplayBytes = 0;
    std::vector<TPtr> Sessions;
    std::unordered_map<std::string, TPtr> Tokens;
    const std::chrono::milliseconds IdleTimeout, Lifetime;
    std::thread Reaper;

    struct TExpiryPolicy
    {
        std::chrono::milliseconds Idle;
        std::chrono::milliseconds Lifetime;
    };
    struct TLookup
    {
        const std::string& Database;
        const std::string& Collection;
        const std::string& Token;
    };
    explicit TImpl(const TExpiryPolicy& policy)
        : IdleTimeout(policy.Idle)
        , Lifetime(policy.Lifetime)
        , Reaper([this]() noexcept {
            for (;;) {
                try {
                    std::unique_lock lock(Mutex);
                    if (Wake.wait_for(lock, std::min(IdleTimeout, std::chrono::milliseconds(1000)), [this] {
                            return Stopping;
                        }))
                    {
                        return;
                    }
                    lock.unlock();
                    Sweep();
                } catch (...) {
                    // Allocation or backend cleanup failures must not escape a
                    // background thread. The next bounded wait retries cleanup
                    // and observes shutdown with fresh, well-defined lock ownership.
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                }
            }
        })
    { }
    ~TImpl()
    {
        {
            std::lock_guard lock(Mutex);
            Stopping = true;
        }
        Wake.notify_all();
        Reaper.join();
        // Member-owned sessions cancel through RAII after the reaper joins.
    }
    bool Expired(const TSession& session) const
    {
        const auto now = Now();
        return now - session.Created >= Lifetime.count() || now - session.Touched.load() >= IdleTimeout.count();
    }
    void CheckActive(const TSession& session) const
    {
        Require(!session.Cancelled.load() && !Expired(session), "scan cursor not found or expired", "NOT_FOUND");
    }
    void MarkLocked(const TPtr& session)
    {
        session->Cancelled = true;
        std::erase_if(Tokens, [&](const auto& item) {
            return item.second == session;
        });
    }
    // Caller owns the session mutex. Keep a shared reference until after the
    // registry lock is released, so buffer/cursor destruction cannot hold it.
    void Release(const TPtr& session)
    {
        session->Cancelled = true;
        if (const auto cursor = std::atomic_exchange(&session->Cursor, std::shared_ptr<ScanCursor>{})) {
            cursor->Cancel();
        }
        session->Pending.clear();
        session->Replay = {};
        std::lock_guard lock(Mutex);
        ReplayBytes -= session->ChargedBytes;
        session->ChargedBytes = 0;
        MarkLocked(session);
        std::erase(Sessions, session);
    }
    void Sweep()
    {
        std::array<TPtr, MaxSessions> expired;
        size_t count = 0;
        {
            std::lock_guard lock(Mutex);
            for (const auto& session : Sessions) {
                if (session->Cancelled.load() || Expired(*session)) {
                    MarkLocked(session);
                    expired[count++] = session;
                }
            }
        }
        for (size_t index = 0; index < count; ++index) {
            const auto& session = expired[index];
            std::unique_lock lock(session->Mutex, std::try_to_lock);
            if (lock.owns_lock()) {
                Release(session);
            }
        }
    }
    void Clear(const std::string* database)
    {
        std::vector<TPtr> removed;
        {
            std::lock_guard lock(Mutex);
            for (const auto& session : Sessions) {
                if (!database || session->Database == *database) {
                    MarkLocked(session);
                    removed.push_back(session);
                }
            }
        }
        for (const auto& session : removed) {
            std::lock_guard lock(session->Mutex);
            Release(session);
        }
    }
    TPtr Find(const TLookup& key)
    {
        Require(key.Token.size() == 32, "scan cursor not found or expired", "NOT_FOUND");
        std::lock_guard lock(Mutex);
        const auto found = Tokens.find(key.Token);
        Require(found != Tokens.end() && found->second->Database == key.Database &&
                found->second->Collection == key.Collection,
            "scan cursor not found or expired",
            "NOT_FOUND");
        return found->second;
    }
    void EvictCompleted(const std::string* database, const TPtr& except = {})
    {
        std::vector<TPtr> candidates;
        {
            std::lock_guard lock(Mutex);
            for (const auto& session : Sessions) {
                if (session != except && session->Complete.load() && (!database || session->Database == *database)) {
                    candidates.push_back(session);
                }
            }
        }
        for (const auto& session : candidates) {
            std::unique_lock lock(session->Mutex, std::try_to_lock);
            if (lock.owns_lock()) {
                Release(session);
            }
        }
    }
    void Register(const TPtr& session)
    {
        Sweep();
        bool localFull = false, globalFull = false;
        {
            std::lock_guard lock(Mutex);
            localFull = std::count_if(Sessions.begin(), Sessions.end(), [&](const auto& item) {
                return item->Database == session->Database;
            }) >= MaxDatabaseSessions;
            globalFull = Sessions.size() >= MaxSessions;
        }
        if (globalFull) {
            EvictCompleted(nullptr);
        } else if (localFull) {
            EvictCompleted(&session->Database);
        }
        std::lock_guard lock(Mutex);
        Require(Accepting, "server is stopping", "CANCELLED");
        const auto localCount = std::count_if(Sessions.begin(), Sessions.end(), [&](const auto& item) {
            return item->Database == session->Database;
        });
        Require(Sessions.size() < MaxSessions && localCount < MaxDatabaseSessions,
            "too many scan sessions; cancel unused cursors or retry after expiry",
            "RESOURCE_EXHAUSTED");
        Sessions.push_back(session);
        Tokens.emplace(session->NextToken, session);
    }
    void Charge(const TPtr& session, size_t bytes)
    {
        bool full = false;
        {
            std::lock_guard lock(Mutex);
            full = bytes > MaxReplayBytes - (ReplayBytes - session->ChargedBytes);
        }
        if (full) {
            EvictCompleted(nullptr, session);
        }
        std::lock_guard lock(Mutex);
        Require(bytes <= MaxReplayBytes - (ReplayBytes - session->ChargedBytes),
            "scan replay memory is full; retry after cancelling unused scans",
            "RESOURCE_EXHAUSTED");
        ReplayBytes = ReplayBytes - session->ChargedBytes + bytes;
        session->ChargedBytes = bytes;
    }
    bool Peek(TSession& session)
    {
        CheckActive(session);
        while (session.Pending.empty() && !session.Ended) {
            auto result = std::atomic_load(&session.Cursor)->Next();
            CheckActive(session);
            auto batch = Unwrap(std::move(result));
            if (!batch) {
                session.Ended = true;
            } else if (batch->num_rows() != 0) {
                session.Pending.push_back(std::move(batch));
            }
        }
        return !session.Pending.empty();
    }
    void Consume(TSession& session, int64_t count)
    {
        auto& batch = session.Pending.front();
        if (count == batch->num_rows()) {
            session.Pending.pop_front();
        } else {
            batch = batch->Slice(count);
        }
    }
    TJson Envelope(const TSession& session, const std::string& next, size_t count)
    {
        return {{"schema", session.Schema},
            {"snapshot_id", Snapshot(session.Info)},
            {"identity", EncodeIdentity(session.Info.identity)},
            {"next_cursor", next.empty() ? TJson(nullptr) : TJson(next)},
            {"profile", {{"rows_returned", std::to_string(count)}}}};
    }
    std::string JsonRows(TSession& session, size_t& count)
    {
        std::string rows = "[";
        const auto overhead = Envelope(session, std::string(32, '0'), PageRows).dump().size() + 9;
        Require(overhead < MaxResponseBytes, "scan schema exceeds response byte limit");
        while (count < PageRows && rows.size() < PageBytes && Peek(session)) {
            const auto take = AppendRowsJson(*session.Pending.front(),
                rows,
                {.MaxRows = PageRows - count, .TargetBytes = PageBytes, .HardBytes = MaxResponseBytes - overhead});
            if (take == 0) {
                break;
            }
            Consume(session, static_cast<int64_t>(take));
            count += take;
        }
        rows += ']';
        return rows;
    }
    std::string ArrowPage(TSession& session, size_t& count)
    {
        std::vector<std::shared_ptr<arrow::RecordBatch>> pieces, groups;
        size_t bytes = 0;
        while (count < PageRows && bytes < PageBytes && Peek(session)) {
            const auto& pending = session.Pending.front();
            int64_t take = std::min(pending->num_rows(), static_cast<int64_t>(PageRows - count));
            auto slice = pending->Slice(0, take);
            auto size = ArrowValue(arrow::util::ReferencedBufferSize(*slice));
            if (static_cast<uint64_t>(size) > PageBytes - bytes) {
                int64_t low = 0, high = take;
                while (low < high) {
                    const auto middle = low + (high - low + 1) / 2;
                    const auto candidate = ArrowValue(arrow::util::ReferencedBufferSize(*pending->Slice(0, middle)));
                    if (static_cast<uint64_t>(candidate) <= PageBytes - bytes) {
                        low = middle;
                    } else {
                        high = middle - 1;
                    }
                }
                take = count == 0 ? std::max<int64_t>(1, low) : low;
                if (take == 0) {
                    break;
                }
                slice = pending->Slice(0, take);
                size = ArrowValue(arrow::util::ReferencedBufferSize(*slice));
            }
            pieces.push_back(std::move(slice));
            count += static_cast<size_t>(take);
            bytes += static_cast<size_t>(size);
            Consume(session, take);
            // Bound object/control memory when core returns one filtered row
            // per batch, without repeatedly copying the whole growing page.
            if (pieces.size() == 128) {
                groups.push_back(ArrowValue(arrow::ConcatenateRecordBatches(pieces)));
                pieces.clear();
            }
        }
        if (!pieces.empty()) {
            groups.push_back(pieces.size() == 1 ? pieces.front() : ArrowValue(arrow::ConcatenateRecordBatches(pieces)));
        }
        auto sink = std::make_shared<TBoundedOutput>();
        auto writer = ArrowValue(
            arrow::ipc::MakeStreamWriter(sink, std::const_pointer_cast<arrow::Schema>(session.Info.output_schema)));
        if (!groups.empty()) {
            auto batch = groups.size() == 1 ? groups.front() : ArrowValue(arrow::ConcatenateRecordBatches(groups));
            const auto prefix =
                static_cast<size_t>(ArrowValue(arrow::ipc::SerializeSchema(*session.Info.output_schema))->size()) +
                8; // schema message plus explicit EOS
            Require(prefix < MaxResponseBytes, "Arrow schema exceeds response byte limit");
            const auto target = PageBytes > prefix ? PageBytes - prefix : 0;
            int64_t take = batch->num_rows();
            if (static_cast<uint64_t>(IpcSize(*batch)) > target) {
                int64_t low = 0, high = take;
                while (low < high) {
                    const auto middle = low + (high - low + 1) / 2;
                    if (static_cast<uint64_t>(IpcSize(*batch->Slice(0, middle))) <= target) {
                        low = middle;
                    } else {
                        high = middle - 1;
                    }
                }
                take = std::max<int64_t>(1, low);
            }
            if (take < batch->num_rows()) {
                session.Pending.push_front(batch->Slice(take));
            }
            batch = batch->Slice(0, take);
            Require(static_cast<uint64_t>(IpcSize(*batch)) <= MaxResponseBytes - prefix,
                "individual Arrow row exceeds response byte limit");
            count = static_cast<size_t>(take);
            Require(writer->WriteRecordBatch(*batch).ok(), "cannot write Arrow scan page", "INTERNAL");
        }
        Require(writer->Close().ok(), "cannot finish Arrow scan page", "INTERNAL");
        return std::move(sink->Bytes);
    }
    TResponse Page(const TPtr& session, const std::string& token)
    {
        CheckActive(*session);
        Require(token == session->ReplayToken || token == session->NextToken,
            "scan cursor not found or expired",
            "NOT_FOUND");
        if (token == session->ReplayToken) {
            auto response = session->Replay;
            CheckActive(*session);
            return response; // Replay does not extend idle or absolute lifetime.
        }
        size_t count = 0;
        auto bytes = session->Arrow ? ArrowPage(*session, count) : JsonRows(*session, count);
        const auto next = Peek(*session) ? Token() : std::string{};
        TResponse response;
        if (session->Arrow) {
            response = {200, std::move(bytes), "application/vnd.apache.arrow.stream"};
            const auto snapshot = Snapshot(session->Info);
            response.Headers["X-GlamDB-Snapshot-Id"] = snapshot.is_null() ? "initial" : snapshot.get<std::string>();
            response.Headers["X-Content-SHA256"] = Checksum(response.Body);
            if (!next.empty()) {
                response.Headers["X-GlamDB-Next-Cursor"] = next;
            }
        } else {
            auto body = Envelope(*session, next, count).dump();
            body.pop_back();
            body += ",\"rows\":";
            body += bytes;
            body += '}';
            response = {200, std::move(body)};
        }
        Require(response.Body.size() <= MaxResponseBytes, "scan response exceeds response byte limit");
        session->Replay = response;
        Charge(session, session->Replay.Body.size());
        CheckActive(*session);
        session->ReplayToken = token;
        session->NextToken = next;
        session->Touched = Now();
        if (next.empty()) {
            if (const auto cursor = std::atomic_exchange(&session->Cursor, std::shared_ptr<ScanCursor>{})) {
                cursor->Cancel();
            }
            session->Complete = true;
        }
        {
            std::lock_guard lock(Mutex);
            CheckActive(*session);
            std::erase_if(Tokens, [&](const auto& item) {
                return item.second == session;
            });
            Tokens.emplace(token, session);
            if (!next.empty()) {
                Tokens.emplace(next, session);
            }
        }
        return response;
    }
};
TScans::TScans(std::chrono::milliseconds idle, std::chrono::milliseconds lifetime)
{
    Require(idle.count() > 0 && lifetime.count() > 0, "scan lifetime limits must be positive");
    Impl_ = std::make_unique<TImpl>(TImpl::TExpiryPolicy{.Idle = idle, .Lifetime = lifetime});
}
TScans::~TScans() = default;
TResponse TScans::Open(const std::shared_ptr<Engine>& engine,
    const std::string& database,
    const std::string& collection,
    const TJson& body,
    bool arrow,
    bool openOnly)
{
    TImpl::TPtr session;
    std::string token;
    if (body.contains("cursor")) {
        Require(!openOnly, "open does not accept a cursor");
        CheckKeys(body, {"cursor"});
        token = body.at("cursor").get<std::string>();
        session = Impl_->Find({.Database = database, .Collection = collection, .Token = token});
    } else {
        session = std::make_shared<TImpl::TSession>();
        session->Database = database;
        session->Collection = collection;
        session->Arrow = arrow;
        session->NextToken = Token();
        token = session->NextToken;
    }
    std::lock_guard lock(session->Mutex);
    // A representation mismatch does not invalidate the owner's valid cursor.
    Require(session->Arrow == arrow, "cursor representation cannot change");
    Require(token == session->NextToken || token == session->ReplayToken,
        "scan cursor not found or expired",
        "NOT_FOUND");
    try {
        if (!body.contains("cursor")) {
            Impl_->Register(session);
            const auto info = Unwrap(engine->GetCollection(collection));
            const auto cursor = Unwrap(engine->Scan(ParseScan(collection, info.schema, body)));
            std::atomic_store(&session->Cursor, cursor);
            // Stop may have run while planning, before the cursor was published.
            if (session->Cancelled.load()) {
                cursor->Cancel();
            }
            session->Info = cursor->Info();
            session->Schema = EncodeSchema(*session->Info.output_schema);
            for (auto& field : session->Schema["fields"]) {
                for (size_t i = 0; i < info.schema.fields.size(); ++i) {
                    if (field["name"] == info.schema.fields[i].name) {
                        field["field_id"] = i + 1;
                    }
                }
            }
        }
        Impl_->CheckActive(*session);
        if (openOnly) {
            return {200,
                TJson{{"cursor", token},
                    {"snapshot_id", Snapshot(session->Info)},
                    {"schema", session->Schema},
                    {"identity", EncodeIdentity(session->Info.identity)}}
                    .dump()};
        }
    } catch (...) {
        Impl_->Release(session);
        throw;
    }
    // Reserve before cursor I/O or row consumption. Keep the previous replay
    // charged while its replacement is encoded; a refused existing cursor
    // must retain both tokens and the exact previous response.
    try {
        if (token != session->ReplayToken) {
            Impl_->Charge(session, session->Replay.Body.size() + MaxResponseBytes);
        }
    } catch (...) {
        if (!body.contains("cursor")) {
            Impl_->Release(session);
        }
        throw;
    }
    try {
        return Impl_->Page(session, token);
    } catch (...) {
        Impl_->Release(session);
        throw;
    }
}
TResponse TScans::Cancel(const std::string& database, const std::string& collection, const TJson& body)
{
    CheckKeys(body, {"cursor"});
    const auto token = body.at("cursor").get<std::string>();
    const auto session = Impl_->Find({.Database = database, .Collection = collection, .Token = token});
    {
        std::lock_guard lock(Impl_->Mutex);
        Impl_->CheckActive(*session);
        Impl_->MarkLocked(session);
    }
    std::unique_lock lock(session->Mutex, std::try_to_lock);
    if (lock.owns_lock()) {
        Impl_->Release(session);
    }
    return {200, R"({"cancelled":true})"};
}
void TScans::Stop()
{
    std::array<TImpl::TPtr, MaxSessions> sessions;
    size_t count = 0;
    {
        std::lock_guard lock(Impl_->Mutex);
        Impl_->Accepting = false;
        for (const auto& session : Impl_->Sessions) {
            Impl_->MarkLocked(session);
            sessions[count++] = session;
        }
    }
    // Cursor cancellation is thread-safe; destruction stays outside registry locks.
    for (size_t index = 0; index < count; ++index) {
        if (const auto cursor = std::atomic_load(&sessions[index]->Cursor)) {
            cursor->Cancel();
        }
    }
}
void TScans::Clear(const std::string& database)
{
    Impl_->Clear(&database);
}
void TScans::Clear()
{
    Impl_->Clear(nullptr);
}
} // namespace glamdb::NServer
