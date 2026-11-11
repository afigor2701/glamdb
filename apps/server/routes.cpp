#include "apps/server/routes.h"
#include "apps/server/databases.h"
#include "apps/server/scans.h"
#include "core/api/binary.h"

#include "apps/transport/outcome_json.h"
#include "core/api/canonical.h"
#include "core/scan/predicate.h"

#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>

#include <fstream>

namespace glamdb::NServer {
using namespace NTransport;
namespace {
std::string Normalized(std::string_view text)
{
    const auto first = text.find_first_not_of(" \t");
    if (first == std::string_view::npos) {
        return {};
    }
    std::string result(text.substr(first, text.find_last_not_of(" \t") - first + 1));
    for (auto& c : result) {
        if (c >= 'A' && c <= 'Z') {
            c = static_cast<char>(c + ('a' - 'A'));
        }
    }
    return result;
}
bool IsJsonContentType(std::string_view text)
{
    const auto semi = text.find(';');
    if (Normalized(text.substr(0, semi)) != "application/json") {
        return false;
    }
    if (semi == std::string_view::npos) {
        return true;
    }
    const auto parameter = text.substr(semi + 1);
    const auto equals = parameter.find('=');
    if (equals == std::string_view::npos || Normalized(parameter.substr(0, equals)) != "charset") {
        return false;
    }
    const auto value = Normalized(parameter.substr(equals + 1));
    return value == "utf-8" || value == "\"utf-8\"";
}
std::optional<bool> ArrowAccepted(std::string_view text)
{
    struct TChoice
    {
        int Specificity = -1;
        int Quality = 0;
    };
    TChoice json, arrow;
    while (!text.empty()) {
        const auto comma = text.find(',');
        auto item = text.substr(0, comma);
        const auto semi = item.find(';');
        const auto media = Normalized(item.substr(0, semi));
        int quality = 1000;
        if (semi != std::string_view::npos) {
            const auto parameter = item.substr(semi + 1);
            const auto equals = parameter.find('=');
            Require(equals != std::string_view::npos && Normalized(parameter.substr(0, equals)) == "q",
                "invalid Accept parameter");
            const auto value = Normalized(parameter.substr(equals + 1));
            Require(!value.empty() && (value[0] == '0' || value[0] == '1') &&
                    (value.size() == 1 || (value.size() <= 5 && value[1] == '.')),
                "invalid Accept quality");
            quality = value[0] == '1' ? 1000 : 0;
            int scale = 100;
            for (size_t i = 2; i < value.size(); ++i) {
                Require(value[i] >= '0' && value[i] <= '9' && (value[0] != '1' || value[i] == '0'),
                    "invalid Accept quality");
                quality += (value[i] - '0') * scale;
                scale /= 10;
            }
        }
        const auto match = [&](std::string_view type, TChoice& choice) {
            const int specificity = media == type ? 2 : media == "application/*" ? 1 : media == "*/*" ? 0 : -1;
            if (specificity > choice.Specificity || (specificity == choice.Specificity && quality > choice.Quality)) {
                choice = {specificity, quality};
            }
        };
        match("application/json", json);
        match("application/vnd.apache.arrow.stream", arrow);
        if (comma == std::string_view::npos) {
            break;
        }
        text.remove_prefix(comma + 1);
    }
    if (json.Specificity < 0) {
        json.Quality = 0;
    }
    if (arrow.Specificity < 0) {
        arrow.Quality = 0;
    }
    if (json.Quality == 0 && arrow.Quality == 0) {
        return {};
    }
    return arrow.Quality > json.Quality;
}

bool IsApiPath(std::string_view path)
{
    return path == "/databases" || path.starts_with("/databases/") || path == "/capabilities" || path == "/readyz" ||
        path == "/collections" || path == "/collections:resolve" || path.starts_with("/collections/");
}
TResponse JsonResponse(const TJson& value)
{
    auto body = value.dump();
    Require(body.size() <= MaxResponseBytes, "response exceeds transport limit", "RESOURCE_EXHAUSTED");
    return {200, std::move(body)};
}
TResponse OutcomeResponse(Result<json_adapter::Response> result)
{
    auto response = Unwrap(std::move(result));
    return {response.status, std::move(response.body)};
}
TJson Collection(const CollectionInfo& info)
{
    return TJson::parse(Unwrap(json_adapter::CollectionJson(info)));
}
std::vector<std::string> Split(const std::string& path)
{
    std::vector<std::string> parts;
    size_t position = 1;
    while (position <= path.size()) {
        const auto slash = path.find('/', position);
        parts.push_back(path.substr(position, slash == std::string::npos ? slash : slash - position));
        if (slash == std::string::npos) {
            break;
        }
        position = slash + 1;
    }
    return parts;
}
} // namespace

TRoutes::TRoutes(std::shared_ptr<Engine> engine, std::filesystem::path uiRoot, std::string token)
    : Scans_(std::make_shared<TScans>())
    , Engine_(std::move(engine))
    , UIRoot_(std::move(uiRoot))
    , Token_(std::move(token))
{ }

TRoutes::TRoutes(std::shared_ptr<TDatabases> databases, std::filesystem::path uiRoot, std::string token)
    : Scans_(std::make_shared<TScans>())
    , Databases_(std::move(databases))
    , UIRoot_(std::move(uiRoot))
    , Token_(std::move(token))
{
    std::weak_ptr<TScans> weak = Scans_;
    Databases_->SetScanCanceller([weak](const std::string& database) {
        if (const auto scans = weak.lock()) {
            scans->Clear(database);
        }
    });
}

bool TRoutes::IsAuthorized(const TRequest& request) const
{
    const auto path = DecodeUrl(request.Target.substr(0, request.Target.find('?')));
    if (!IsApiPath(path)) {
        return true;
    }
    const auto found = request.Headers.find("authorization");
    if (found == request.Headers.end()) {
        return false;
    }
    const auto expected = "Bearer " + Token_;
    size_t diff = expected.size() ^ found->second.size();
    for (size_t i = 0; i < expected.size(); ++i) {
        diff |= static_cast<unsigned char>(expected[i]) ^
            static_cast<unsigned char>(i < found->second.size() ? found->second[i] : 0);
    }
    return diff == 0;
}

TResponse TRoutes::Handle(const TRequest& request) const
{
    try {
        if (!IsAuthorized(request)) {
            return ErrorResponse(401, "UNAUTHORIZED", "bearer token required");
        }
        auto response = Route(request);
        Require(response.Body.size() <= MaxResponseBytes, "response exceeds transport limit", "RESOURCE_EXHAUSTED");
        return response;
    } catch (const TFailure& error) {
        return {ErrorStatus(error.Detail), EncodeError(error.Detail).dump()};
    } catch (const detail::Failure& error) {
        return {ErrorStatus(error.detail), EncodeError(error.detail).dump()};
    } catch (const TJson::exception&) {
        return ErrorResponse(400, "INVALID_ARGUMENT", "invalid JSON request");
    } catch (const std::bad_alloc&) {
        return ErrorResponse(503,
            "RESOURCE_EXHAUSTED",
            "request allocation failed; resolve write outcome by original ID",
            RetryHint::SameIdAfterReconcile);
    } catch (const std::exception&) {
        return ErrorResponse(500,
            "INTERNAL",
            "request failed; resolve write outcome by original ID",
            RetryHint::SameIdAfterReconcile);
    }
}

TResponse TRoutes::Route(const TRequest& request) const
{
    const auto [path, query] = ParseTarget(request.Target);
    TJson body;
    if (request.Method == "POST") {
        const auto type = request.Headers.find("content-type");
        if (type == request.Headers.end() || !IsJsonContentType(type->second)) {
            return ErrorResponse(415, "UNSUPPORTED_MEDIA_TYPE", "Content-Type must be application/json");
        }
        Require(query.empty(), "POST query parameters are unsupported");
        body = ParseJson(request.Body);
        Require(body.is_object(), "request body must be an object");
    }
    return RoutePrepared(request, path, query, body);
}

TResponse TRoutes::RoutePrepared(const TRequest& request,
    const std::string& path,
    const std::map<std::string, std::string>& query,
    const TJson& body) const
{
    if (request.Method != "GET" && request.Method != "POST") {
        return ErrorResponse(405, "METHOD_NOT_ALLOWED", "use GET or POST");
    }
    if (!IsApiPath(path)) {
        Require(request.Method == "GET", "static resources require GET");
        if (path == "/healthz") {
            return JsonResponse(
                {{"status", "live"}, {"backend", "local"}, {"mock", false}, {"durability_validation", "designed"}});
        }
        return StaticFile(path);
    }
    if (request.Method == "GET" && path == "/readyz") {
        Require(query.empty(), "unknown query parameter");
        Require(static_cast<bool>(Engine_), "select a database using /databases/{id}", "DATABASE_REQUIRED");
        const auto health = Engine_->GetHealth();
        return ReadinessResponse(health);
    }
    if (request.Method == "GET" && path == "/capabilities") {
        return JsonResponse({{"api_version", "v0"},
            {"health", Engine_ ? EncodeHealth(Engine_->GetHealth()) : TJson(nullptr)},
            {"backend", "local"},
            {"mock", false},
            {"durability_validation", "designed"},
            {"collection_modes", {"append"}},
            {"features",
                {{"database_management", static_cast<bool>(Databases_)},
                    {"append", true},
                    {"scan", true},
                    {"scan_pagination", true},
                    {"scan_cancel", true},
                    {"arrow_export", true},
                    {"stats", false},
                    {"snapshots", false},
                    {"files", false},
                    {"explain", false},
                    {"pagination", false},
                    {"zero_column_projection", false}}},
            {"predicate_ops",
                {"true", "eq", "ne", "lt", "le", "gt", "ge", "in", "is_null", "is_not_null", "and", "or", "not"}},
            {"limits",
                {{"request_bytes", MaxRequestBytes},
                    {"response_bytes", MaxResponseBytes},
                    {"append_rows", 10000},
                    {"scan_page_rows", 10000},
                    {"scan_sessions", 64},
                    {"scan_sessions_per_database", 8},
                    {"scan_lifetime_seconds", 300},
                    {"scan_replay_bytes", 128ULL * 1024 * 1024},
                    {"scan_idle_seconds", 60},
                    {"batch_rows", 65536},
                    {"predicate_depth", detail::MaxPredicateDepth},
                    {"predicate_nodes", detail::MaxPredicateNodes},
                    {"predicate_literals", detail::MaxPredicateLiterals}}}});
    }
    if (path == "/databases" || path.starts_with("/databases/")) {
        Require(static_cast<bool>(Databases_), "database management requires --data-dir", "UNSUPPORTED_OPERATION");
        if (path == "/databases") {
            Require(query.empty(), "unknown query parameter");
            return JsonResponse(request.Method == "GET" ? Databases_->List() : Databases_->Create(body));
        }
        const auto parts = Split(path);
        if (parts.size() == 2 && request.Method == "GET") {
            Require(query.empty(), "unknown query parameter");
            return JsonResponse(Databases_->Get(parts[1]));
        }
        if (parts.size() == 3 && request.Method == "GET" && (parts[2] == "readyz" || parts[2] == "capabilities")) {
            Require(query.empty(), "unknown query parameter");
            const auto health = Databases_->Health(parts[1]);
            if (parts[2] == "readyz") {
                return ReadinessResponse(health);
            }
            auto response = RoutePrepared(request, "/capabilities", query, body);
            auto value = TJson::parse(response.Body);
            value["health"] = EncodeHealth(health);
            value["features"]["database_management"] = false;
            return JsonResponse(value);
        }
        if (parts.size() == 3 && request.Method == "POST" &&
            (parts[2] == "open" || parts[2] == "close" || parts[2] == "reconcile" || parts[2] == "discard"))
        {
            return JsonResponse(Databases_->Change(parts[1], parts[2], body));
        }
        Require(parts.size() >= 3 &&
                (parts[2] == "collections" || parts[2] == "collections:resolve" || parts[2] == "capabilities"),
            "route not found",
            "ROUTE_NOT_FOUND");
        // Validation and dispatch use the same once-decoded path and parsed body.
        const auto suffix = path.substr(path.find('/', std::string("/databases/").size()));
        const bool recoveryOnly = (request.Method == "GET" && parts.size() == 6 && parts[4] == "operations") ||
            (request.Method == "POST" && parts.size() == 6 && parts[4] == "scan" && parts[5] == "cancel");
        return Databases_->Use(
            parts[1],
            [&](const auto& engine) {
                auto routes = *this;
                routes.Engine_ = engine;
                routes.Databases_.reset();
                routes.DatabaseID_ = parts[1];
                return routes.RoutePrepared(request, suffix, query, body);
            },
            recoveryOnly);
    }
    Require(static_cast<bool>(Engine_), "select a database using /databases/{id}", "DATABASE_REQUIRED");
    if (request.Method == "GET" && path == "/collections:resolve") {
        Require(query.size() == 1 && query.contains("name"), "exact name is required");
        return JsonResponse(Collection(Unwrap(Engine_->ResolveCollection(query.at("name")))));
    }
    if (request.Method == "GET" && path == "/collections") {
        for (const auto& [key, value] : query) {
            (void)value;
            Require(key == "page_size" || key == "cursor", "unknown query parameter");
        }
        Require(!query.contains("cursor"), "pagination is unavailable", "UNSUPPORTED_OPERATION");
        int64_t pageSize = 100;
        if (query.contains("page_size")) {
            pageSize = ParseInt64(query.at("page_size"));
        }
        Require(pageSize >= 1 && pageSize <= 1000, "page_size must be 1..1000");
        const auto collections = Unwrap(Engine_->ListCollections());
        Require(collections.size() <= static_cast<size_t>(pageSize),
            "increase page_size; pagination is unavailable",
            "UNSUPPORTED_OPERATION");
        auto items = TJson::array();
        for (const auto& info : collections) {
            items.push_back(Collection(info));
        }
        return JsonResponse({{"items", items}, {"next_cursor", nullptr}});
    }
    if (request.Method == "POST" && path == "/collections") {
        CheckKeys(body, {"collection_id", "name", "schema", "mode"});
        Require(body.value("mode", "append") == "append",
            "only append collections are supported",
            "UNSUPPORTED_OPERATION");
        return OutcomeResponse(
            json_adapter::ToJson(Engine_->CreateCollection({body.at("collection_id").get<std::string>(),
                body.at("name").get<std::string>(),
                ParseSchema(body.at("schema"))})));
    }
    const auto parts = Split(path);
    Require(parts.size() >= 2 && parts[0] == "collections", "route not found", "ROUTE_NOT_FOUND");
    const auto& id = parts[1];
    if (request.Method == "GET" && parts.size() == 4 && parts[2] == "operations") {
        Require(query.empty(), "unknown query parameter");
        auto outcome = Unwrap(Engine_->FindAppendOutcome(id, parts[3]));
        if (!outcome.has_value()) {
            throw TFailure({"OPERATION_NOT_FOUND", "operation not found"});
        }
        return OutcomeResponse(json_adapter::ToJson(*outcome));
    }
    const std::string action = parts.size() >= 3 ? parts[2] : "";
    if (request.Method == "POST" && parts.size() == 4 && action == "scan" && parts[3] == "cancel") {
        // Releasing a scoped cursor must remain possible when an unrelated
        // core failure has fenced catalog reads pending reconciliation.
        Require(query.empty(), "unknown query parameter");
        return Scans_->Cancel(DatabaseID_, id, body);
    }
    if (request.Method == "POST" && parts.size() == 3 && action == "append") {
        Require(query.empty(), "unknown query parameter");
        CheckKeys(body, {"operation_id", "rows", "expected_snapshot"});
        AppendRequest append;
        append.collection_id = id;
        append.operation_id = body.at("operation_id").get<std::string>();
        if (body.contains("expected_snapshot")) {
            append.expected_snapshot = body["expected_snapshot"].is_null()
                ? SnapshotPrecondition{SnapshotPreconditionKind::NoSnapshot, 0}
                : SnapshotPrecondition{SnapshotPreconditionKind::Equals, ParseInt64(body["expected_snapshot"])};
        }
        append.batches.push_back(ParseRows(Unwrap(Engine_->SchemaForAppend(id)), body.at("rows")));
        return OutcomeResponse(json_adapter::ToJson(Engine_->AppendDurable(append)));
    }
    const auto info = Unwrap(Engine_->GetCollection(id));
    if ((action == "stats" || action == "snapshots" || action == "files" || action == "explain")) {
        throw TFailure({"UNSUPPORTED_OPERATION", "native metadata/explain API is not available; inspect capabilities"});
    }
    Require(query.empty(), "unknown query parameter");
    if (request.Method == "GET") {
        if (parts.size() == 2) {
            return JsonResponse(Collection(info));
        }
        if (parts.size() == 3 && action == "schema") {
            return JsonResponse(Collection(info).at("schema"));
        }
    }
    if (request.Method == "POST" && (parts.size() == 3 || (parts.size() == 4 && action == "scan"))) {
        if (action == "scan") {
            Require(parts.size() == 3 || parts[3] == "open", "route not found", "ROUTE_NOT_FOUND");
            const auto accept = request.Headers.find("accept");
            const auto media = accept == request.Headers.end() ? "application/json" : accept->second;
            const auto arrow = ArrowAccepted(media);
            if (!arrow.has_value()) {
                return ErrorResponse(406, "NOT_ACCEPTABLE", "use JSON or Arrow stream");
            }
            return Scans_->Open(Engine_, DatabaseID_, id, body, *arrow, parts.size() == 4);
        }
    }
    return ErrorResponse(404, "ROUTE_NOT_FOUND", "route not found");
}

void TRoutes::CancelScans() const
{
    Scans_->Stop();
}
void TRoutes::CancelScans(const std::string& database) const
{
    Scans_->Clear(database);
}

TResponse TRoutes::StaticFile(const std::string& path) const
{
    const auto relative = path == "/" ? "index.html"
        : path == "/workbench/config" ? "config/theme.json"
                                      : path.substr(1);
    if (UIRoot_.empty() ||
        !(relative == "index.html" || relative == "config/theme.json" || relative.starts_with("assets/")))
    {
        return ErrorResponse(404, "NOT_FOUND", "resource not found");
    }
    std::filesystem::path file = UIRoot_;
    for (const auto& component : std::filesystem::path(relative)) {
        if (component == ".." || component == "." || component.empty()) {
            return ErrorResponse(404, "NOT_FOUND", "resource not found");
        }
        file /= component;
        if (std::filesystem::is_symlink(std::filesystem::symlink_status(file))) {
            return ErrorResponse(404, "NOT_FOUND", "resource not found");
        }
    }
    if (!std::filesystem::is_regular_file(file) || std::filesystem::file_size(file) > MaxResponseBytes) {
        return ErrorResponse(404, "NOT_FOUND", "resource not found");
    }
    std::ifstream input(file, std::ios::binary);
    std::string data((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
    Require(input.good() || input.eof(), "cannot read UI asset", "IO_ERROR");
    const std::map<std::string, std::string> types{{".html", "text/html; charset=utf-8"},
        {".css", "text/css"},
        {".js", "text/javascript"},
        {".json", "application/json"},
        {".png", "image/png"},
        {".svg", "image/svg+xml"}};
    const auto type = types.find(file.extension().string());
    if (type == types.end()) {
        return ErrorResponse(404, "NOT_FOUND", "resource not found");
    }
    return {200, std::move(data), type->second};
}
} // namespace glamdb::NServer
