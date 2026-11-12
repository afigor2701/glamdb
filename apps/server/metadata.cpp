#include "apps/server/metadata.h"
#include "apps/server/scans.h"

#include "apps/transport/outcome_json.h"
#include "core/api/write_settings.h"
#include "core/scan/predicate.h"

#include <openssl/rand.h>

#include <algorithm>
#include <array>
#include <mutex>
#include <unordered_map>

namespace glamdb::NServer {
using namespace NTransport;
namespace {
constexpr size_t MaxSessions = 64;
constexpr size_t MaxDatabaseSessions = 8;
constexpr size_t MaxRetainedBytes = size_t{64} * 1024 * 1024;
constexpr size_t MaxInventoryBytes = size_t{16} * 1024 * 1024;
TJson LiteralJson(const OptionalLiteral& value)
{
    return value ? std::visit(
                       [](const auto& literal) {
                           return EncodeCell(Cell(literal));
                       },
                       *value)
                 : TJson(nullptr);
}
TJson PredicateJson(const Predicate& predicate)
{
    switch (predicate.kind()) {
        case Predicate::Kind::True:
            return {{"op", "true"}};
        case Predicate::Kind::Compare: {
            const std::array operators{"eq", "ne", "lt", "le", "gt", "ge"};
            return {{"op", operators.at(static_cast<size_t>(predicate.op()))},
                {"field", predicate.field()},
                {"value", LiteralJson(predicate.optional_value())}};
        }
        case Predicate::Kind::In: {
            auto values = TJson::array();
            for (const auto& value : predicate.values()) {
                values.push_back(LiteralJson(value));
            }
            return {{"op", "in"}, {"field", predicate.field()}, {"values", std::move(values)}};
        }
        case Predicate::Kind::IsNull:
        case Predicate::Kind::IsNotNull:
            return {{"op", predicate.kind() == Predicate::Kind::IsNull ? "is_null" : "is_not_null"},
                {"field", predicate.field()}};
        case Predicate::Kind::And:
        case Predicate::Kind::Or: {
            auto args = TJson::array();
            for (const auto& child : predicate.children()) {
                args.push_back(PredicateJson(child));
            }
            return {{"op", predicate.kind() == Predicate::Kind::And ? "and" : "or"}, {"args", std::move(args)}};
        }
        case Predicate::Kind::Not:
            return {{"op", "not"}, {"arg", PredicateJson(predicate.children().front())}};
    }
    throw TFailure({"INTERNAL", "unsupported validated predicate"});
}
int64_t Now()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}
std::string Token()
{
    std::array<unsigned char, 24> bytes{};
    Require(RAND_bytes(bytes.data(), static_cast<int>(bytes.size())) == 1, "cannot create metadata cursor", "INTERNAL");
    constexpr char Hex[] = "0123456789abcdef";
    std::string result;
    result.reserve(bytes.size() * 2);
    for (const auto byte : bytes) {
        result += Hex[byte >> 4];
        result += Hex[byte & 15];
    }
    return result;
}
} // namespace

EngineCapabilities InstalledCapabilities()
{
#if GLAMDB_HTTP_STORAGE
    return {.backend = BackendKind::LocalStorage,
        .append = true,
        .scan = true,
        .metadata = true,
        .file_pruning = true,
        .row_group_pruning = true,
        .write_settings = true,
        .parquet_codecs = detail::ParquetCodecCapabilities()};
#else
    return {};
#endif
}

namespace {
TJson CompressionJson(const ParquetCompression& compression)
{
    return {{"codec", detail::CodecName(compression.codec)},
        {"level", compression.level ? TJson(*compression.level) : TJson(nullptr)}};
}

// Parquet value compression of data files, not HTTP content coding.
TJson ParquetJson(const EngineCapabilities& capabilities)
{
    if (!capabilities.write_settings) {
        return nullptr;
    }
    TJson codecs = TJson::array();
    for (const auto& codec : capabilities.parquet_codecs) {
        TJson level = nullptr;
        if (codec.min_level && codec.max_level && codec.default_level) {
            level = {{"min", *codec.min_level}, {"max", *codec.max_level}, {"default", *codec.default_level}};
        }
        codecs.push_back({{"codec", detail::CodecName(codec.codec)}, {"level", std::move(level)}});
    }
    return {{"default_compression", CompressionJson(capabilities.default_parquet_compression)},
        {"codecs", std::move(codecs)}};
}
} // namespace

TJson CapabilitiesJson(const EngineCapabilities& capabilities,
    const std::optional<StoreHealth>& health,
    bool databaseManagement,
    const std::string& storeID,
    const TScanLimits& scans)
{
    const bool metadata = capabilities.metadata;
    const std::array backends{"unavailable", "custom", "local"};
    TJson value{{"api_version", "v0"},
        {"health", health ? EncodeHealth(*health) : TJson(nullptr)},
        {"backend", backends.at(static_cast<size_t>(capabilities.backend))},
        {"mock", false},
        {"durability_validation", capabilities.durability_qualified ? "qualified" : "designed"},
        {"durability_qualified", capabilities.durability_qualified},
        {"file_pruning", capabilities.file_pruning},
        {"row_group_pruning", capabilities.row_group_pruning},
        {"page_pruning", false},
        {"bloom_pruning", false},
        {"collection_modes", {"append"}},
        {"features",
            {{"database_management", databaseManagement},
                {"append", capabilities.append},
                {"scan", capabilities.scan},
                {"scan_pagination", capabilities.scan},
                {"scan_cancel", capabilities.scan},
                {"arrow_export", capabilities.scan},
                {"stats", metadata},
                {"snapshots", metadata},
                {"files", metadata},
                {"explain", metadata},
                {"metadata_pagination", metadata},
                {"pagination", false},
                {"zero_column_projection", true},
                {"write_settings", capabilities.write_settings}}},
        {"parquet", ParquetJson(capabilities)},
        {"predicate_ops",
            {"true", "eq", "ne", "lt", "le", "gt", "ge", "in", "is_null", "is_not_null", "and", "or", "not"}},
        {"limits",
            {{"request_bytes", MaxRequestBytes},
                {"response_bytes", MaxResponseBytes},
                {"append_rows", 10000},
                {"scan_page_rows", 10000},
                {"scan_sessions", scans.Live},
                {"scan_sessions_per_database", scans.LivePerDatabase},
                {"scan_completed_sessions", scans.Completed},
                {"scan_completed_sessions_per_database", scans.CompletedPerDatabase},
                {"scan_lifetime_seconds", scans.Lifetime.count() / 1000},
                {"scan_replay_bytes", scans.ReplayBytes},
                {"scan_replay_bytes_per_database", scans.ReplayBytesPerDatabase},
                {"scan_idle_seconds", scans.Idle.count() / 1000},
                {"scan_completed_ttl_seconds", scans.CompletedTtl.count() / 1000},
                {"metadata_page_size", 1000},
                {"metadata_sessions", MaxSessions},
                {"metadata_sessions_per_database", MaxDatabaseSessions},
                {"metadata_lifetime_seconds", 300},
                {"metadata_idle_seconds", 60},
                {"metadata_retained_json_bytes", MaxRetainedBytes},
                {"metadata_inventory_json_bytes", MaxInventoryBytes},
                {"batch_rows", 65536},
                {"predicate_depth", detail::MaxPredicateDepth},
                {"predicate_nodes", detail::MaxPredicateNodes},
                {"predicate_literals", detail::MaxPredicateLiterals}}}};
    if (!storeID.empty()) {
        value["store_id"] = storeID;
    }
    return value;
}

TJson ExplainJson(const Engine& engine, const std::string& collection, const TJson& body, const TScanLimits& scans)
{
    const auto info = Unwrap(engine.GetCollection(collection));
    const auto explanation = Unwrap(engine.ExplainScan(ParseScan(collection, info.schema, body)));
    auto value = NativeIdentity(explanation.info.identity, explanation.info.selector_resolved);
    auto output = TJson::array();
    const auto schema = EncodeSchema(*explanation.info.output_schema);
    for (const auto& field : schema.at("fields")) {
        output.push_back(field.at("name"));
    }
    auto capabilities =
        CapabilitiesJson(engine.GetCapabilities(), engine.GetHealth(), false, engine.GetStoreInfo().store_id, scans);
    capabilities["file_pruning"] = explanation.file_pruning;
    capabilities["row_group_pruning"] = explanation.row_group_pruning;
    TJson plan = TJson::array({"resolve confirmed fixed view"});
    if (explanation.file_pruning || explanation.row_group_pruning) {
        plan.push_back("prune candidate files and Parquet row groups");
    }
    plan.push_back("read committed candidate files");
    plan.push_back("exact SQL three-valued filter");
    plan.push_back("projection");
    plan.push_back("limit");
    value.update({{"schema", schema},
        {"read_columns", explanation.read_fields},
        {"output_columns", output},
        {"predicate", PredicateJson(explanation.predicate)},
        {"limit", OptionalInteger(explanation.limit)},
        {"batch_rows", std::to_string(explanation.batch_rows)},
        {"plan", std::move(plan)},
        {"candidates", Statistics(explanation.candidates)},
        {"estimates",
            {{"rows_before_filter", Value(explanation.candidates.rows)},
                {"data_files", Value(explanation.candidates.data_files)},
                {"output_rows", Value(explanation.estimated_output_rows)},
                {"compressed_bytes", Value(explanation.candidates.compressed_data_bytes)},
                {"output_rows_kind", MetricKindName(explanation.estimated_output_rows)},
                {"synthetic", false}}},
        {"actual_io", nullptr},
        {"capabilities", capabilities}});
    return value;
}

struct TMetadata::TImpl
{
    struct TSession
    {
        std::string Database, Collection, Kind, Selector;
        size_t PageSize = 100;
        size_t Bytes = 0;
        int64_t Created = 0;
        int64_t Touched = 0;
        bool Completed = false;
        TJson Value;
        std::vector<std::string> Tokens;
    };
    struct TPage
    {
        std::shared_ptr<TSession> Session;
        size_t Position = 0;
    };
    std::mutex Mutex;
    std::unordered_map<std::string, TPage> Pages;
    std::vector<std::shared_ptr<TSession>> Sessions;
    size_t Bytes = 0;
    int64_t Idle;
    int64_t Lifetime;
    TImpl(std::chrono::milliseconds idle, std::chrono::milliseconds lifetime)
        : Idle(idle.count())
        , Lifetime(lifetime.count())
    { }
    void Erase(const std::function<bool(const TSession&)>& remove)
    {
        std::erase_if(Sessions, [&](const auto& session) {
            if (!remove(*session)) {
                return false;
            }
            for (const auto& token : session->Tokens) {
                Pages.erase(token);
            }
            Bytes -= session->Bytes;
            return true;
        });
    }
    void Expire(int64_t now)
    {
        Erase([&](const auto& session) {
            return now - session.Created >= Lifetime || now - session.Touched >= Idle;
        });
    }
    void Reclaim(const std::string& database, size_t incomingBytes)
    {
        for (;;) {
            const bool databaseFull = std::count_if(Sessions.begin(), Sessions.end(), [&](const auto& session) {
                return session->Database == database;
            }) >= static_cast<std::ptrdiff_t>(MaxDatabaseSessions);
            if (!databaseFull && Sessions.size() < MaxSessions && incomingBytes <= MaxRetainedBytes - Bytes) {
                return;
            }
            const auto victim = std::find_if(Sessions.begin(), Sessions.end(), [&](const auto& session) {
                return session->Completed && (!databaseFull || session->Database == database);
            });
            if (victim == Sessions.end()) {
                return;
            }
            const auto* retired = victim->get();
            Erase([&](const auto& session) {
                return &session == retired;
            });
        }
    }
    TJson Slice(const TSession& session, size_t position) const
    {
        auto result = TJson::object();
        for (const auto& [key, value] : session.Value.items()) {
            if (key != "items") {
                result[key] = value;
            }
        }
        const auto& items = session.Value.at("items");
        const auto end = std::min(items.size(), position + session.PageSize);
        result["items"] = TJson::array();
        for (size_t i = position; i < end; ++i) {
            result["items"].push_back(items[i]);
        }
        result["next_cursor"] =
            end < items.size() ? TJson(session.Tokens.at(end / session.PageSize - 1)) : TJson(nullptr);
        Require(result.dump().size() <= MaxResponseBytes, "metadata page exceeds response limit", "RESOURCE_EXHAUSTED");
        return result;
    }
};

TMetadata::TMetadata(std::chrono::milliseconds idle, std::chrono::milliseconds lifetime)
    : Impl_(std::make_unique<TImpl>(idle, lifetime))
{ }
TMetadata::~TMetadata() = default;

TJson TMetadata::Page(const std::string& database,
    const std::string& collection,
    const std::string& kind,
    const std::map<std::string, std::string>& query,
    const std::function<TJson(SnapshotSelector)>& read)
{
    for (const auto& [key, ignored] : query) {
        (void)ignored;
        Require(key == "snapshot_id" || key == "page_size" || key == "cursor", "unknown metadata page parameter");
    }
    const auto selector = MetadataSelector(query);
    const auto selectorText = query.contains("snapshot_id") ? query.at("snapshot_id") : "latest";
    const auto sizeText = query.contains("page_size") ? query.at("page_size") : "100";
    const auto size = ParseInt64(sizeText);
    Require(size >= 1 && size <= 1000 && std::to_string(size) == sizeText, "page_size must be 1..1000");
    const auto pageSize = static_cast<size_t>(size);
    if (query.contains("cursor")) {
        const auto& cursor = query.at("cursor");
        std::lock_guard lock(Impl_->Mutex);
        const auto now = Now();
        Impl_->Expire(now);
        const auto found = Impl_->Pages.find(cursor);
        Require(cursor.size() == 48 && found != Impl_->Pages.end(),
            "metadata cursor not found or expired",
            "NOT_FOUND");
        const auto& session = *found->second.Session;
        Require(session.Database == database && session.Collection == collection,
            "metadata cursor not found or expired",
            "NOT_FOUND");
        Require(session.Kind == kind && session.Selector == selectorText && session.PageSize == pageSize,
            "metadata cursor parameters do not match");
        const auto result = Impl_->Slice(session, found->second.Position);
        if (result.at("next_cursor").is_null()) {
            found->second.Session->Completed = true;
        }
        found->second.Session->Touched = now;
        return result;
    }
    auto value = read(selector);
    const auto bytes = value.dump().size();
    Require(bytes <= MaxInventoryBytes, "metadata inventory exceeds retained limit", "RESOURCE_EXHAUSTED");
    TImpl::TSession session{.Database = database,
        .Collection = collection,
        .Kind = kind,
        .Selector = selectorText,
        .PageSize = pageSize,
        .Bytes = bytes,
        .Created = Now(),
        .Touched = Now(),
        .Value = std::move(value),
        .Tokens = {}};
    const auto count = session.Value.at("items").size();
    if (count <= pageSize) {
        return Impl_->Slice(session, 0);
    }
    // Include token overhead before reclaiming completed replay inventories.
    session.Bytes += (count - 1) / pageSize * 256;
    Require(session.Bytes <= MaxRetainedBytes, "metadata cursor capacity reached", "RESOURCE_EXHAUSTED");
    std::lock_guard lock(Impl_->Mutex);
    Impl_->Expire(Now());
    Impl_->Reclaim(database, session.Bytes);
    Require(Impl_->Sessions.size() < MaxSessions && session.Bytes <= MaxRetainedBytes - Impl_->Bytes,
        "metadata retained page capacity reached",
        "RESOURCE_EXHAUSTED");
    Require(std::count_if(Impl_->Sessions.begin(),
                Impl_->Sessions.end(),
                [&](const auto& existing) {
                    return existing->Database == database;
                }) < static_cast<std::ptrdiff_t>(MaxDatabaseSessions),
        "database metadata page capacity reached",
        "RESOURCE_EXHAUSTED");
    auto retained = std::make_shared<TImpl::TSession>(std::move(session));
    try {
        for (size_t position = pageSize; position < count; position += pageSize) {
            auto token = Token();
            while (Impl_->Pages.contains(token)) {
                token = Token();
            }
            retained->Tokens.push_back(token);
            Impl_->Pages.emplace(std::move(token), TImpl::TPage{retained, position});
        }
        const auto result = Impl_->Slice(*retained, 0);
        Impl_->Sessions.push_back(retained);
        Impl_->Bytes += retained->Bytes;
        return result;
    } catch (...) {
        for (const auto& token : retained->Tokens) {
            Impl_->Pages.erase(token);
        }
        throw;
    }
}
void TMetadata::Clear(const std::string& database)
{
    std::lock_guard lock(Impl_->Mutex);
    Impl_->Erase([&](const auto& session) {
        return session.Database == database;
    });
}
void TMetadata::Clear()
{
    std::lock_guard lock(Impl_->Mutex);
    Impl_->Pages.clear();
    Impl_->Sessions.clear();
    Impl_->Bytes = 0;
}
} // namespace glamdb::NServer
