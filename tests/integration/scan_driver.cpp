// Fresh-process scan checks through the public Engine over the real storage
// backend. Each mode runs in its own process; see scan_process_test.py.

#include "storage/engine.h"

#include "core/backend/local_storage_backend.h"
#include "core/engine/engine_factory.h"

#include <arrow/api.h>

#include <unistd.h>

#include <algorithm>
#include <iostream>
#include <optional>
#include <string>
#include <vector>

namespace {

using namespace glamdb;

constexpr char StoreId[] = "00000000-0000-4000-8000-000000000001";
constexpr char CollectionId[] = "00000000-0000-4000-8000-000000000011";
constexpr char OperationA[] = "00000000-0000-4000-8000-0000000000a1";
constexpr char OperationB[] = "00000000-0000-4000-8000-0000000000a2";
constexpr char OperationC[] = "00000000-0000-4000-8000-0000000000a3";
constexpr char OperationD[] = "00000000-0000-4000-8000-0000000000a4";

const SchemaSpec Schema{{
    {"key", FieldType::String, false},
    {"n", FieldType::Int64, true},
    {"at", FieldType::TimestampUsUtc, true},
}};

struct TRow
{
    std::string Key;
    std::optional<int64_t> N;
    std::optional<int64_t> At;
};

const std::vector<TRow> RowsA{{"dup", 1, 10}, {"dup", 1, 10}, {"null", std::nullopt, std::nullopt}};
const std::vector<TRow> RowsB{{"b", 2, 20}};
const std::vector<TRow> RowsC{{"crash", 3, 30}};
const std::vector<TRow> RowsD{{"d", 4, 40}};

[[noreturn]] void Die(const std::string& message)
{
    std::cerr << message << '\n';
    std::exit(1);
}

template <class T>
T Check(Result<T> result, const char* context)
{
    if (!result) {
        Die(std::string(context) + ": " + result.error().code + ": " + result.error().message);
    }
    return std::move(*result);
}

std::shared_ptr<arrow::RecordBatch> Batch(const std::vector<TRow>& rows)
{
    arrow::StringBuilder keys;
    arrow::Int64Builder values;
    arrow::TimestampBuilder times(arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"), arrow::default_memory_pool());
    for (const auto& row : rows) {
        (void)keys.Append(row.Key);
        (void)(row.N ? values.Append(*row.N) : values.AppendNull());
        (void)(row.At ? times.Append(*row.At) : times.AppendNull());
    }
    std::shared_ptr<arrow::Array> a;
    std::shared_ptr<arrow::Array> b;
    std::shared_ptr<arrow::Array> c;
    (void)keys.Finish(&a);
    (void)values.Finish(&b);
    (void)times.Finish(&c);
    const auto schema = arrow::schema({arrow::field("key", arrow::utf8(), false),
        arrow::field("n", arrow::int64()),
        arrow::field("at", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"))});
    return arrow::RecordBatch::Make(schema, static_cast<int64_t>(rows.size()), {a, b, c});
}

std::string Cell(const arrow::Array& array, int64_t row)
{
    if (array.IsNull(row)) {
        return "null";
    }
    switch (array.type_id()) {
        case arrow::Type::STRING:
            return std::string(static_cast<const arrow::StringArray&>(array).GetView(row));
        case arrow::Type::INT64:
            return std::to_string(static_cast<const arrow::Int64Array&>(array).Value(row));
        case arrow::Type::TIMESTAMP:
            return std::to_string(static_cast<const arrow::TimestampArray&>(array).Value(row));
        default:
            Die("unexpected column type");
    }
}

// JSON array of sorted "key|n|at" rows.
std::string Rows(Engine& engine, SnapshotSelector selector)
{
    auto cursor = Check(engine.Scan({.collection_id = CollectionId, .selector = selector, .batch_rows = 1}), "scan");
    std::vector<std::string> rows;
    while (true) {
        auto batch = Check(cursor->Next(), "next");
        if (!batch) {
            break;
        }
        for (int64_t row = 0; row < batch->num_rows(); ++row) {
            rows.push_back(
                Cell(*batch->column(0), row) + "|" + Cell(*batch->column(1), row) + "|" + Cell(*batch->column(2), row));
        }
    }
    std::ranges::sort(rows);
    std::string json = "[";
    for (size_t index = 0; index < rows.size(); ++index) {
        json += (index == 0 ? "\"" : ",\"") + rows[index] + "\"";
    }
    return json + "]";
}

std::string Outcome(const AppendOutcome& outcome)
{
    const auto disposition = outcome.disposition == CommitDisposition::Committed ? "committed"
        : outcome.disposition == CommitDisposition::DefinitelyNotCommitted       ? "definitely_not_committed"
                                                                                 : "commit_unknown";
    return std::string("{\"outcome\":\"") + disposition + "\",\"error\":\"" +
        (outcome.error ? outcome.error->code : std::string()) + "\",\"snapshot\":\"" +
        (outcome.result ? std::to_string(outcome.result->snapshot_id) : std::string()) + "\"}";
}

// Delegates to the real backend and exits the process right after the data,
// manifests and root of an attempt are durable, before publication.
class TCrashAfterPrepare final : public detail::StorageBackend
{
public:
    explicit TCrashAfterPrepare(std::shared_ptr<detail::StorageBackend> inner)
        : Inner_(std::move(inner))
    { }

    Result<std::unique_ptr<detail::StagedCreate>>
    PrepareCreate(const StoreInfo& store, const CreateCollectionRequest& request, std::string_view attempt) override
    {
        return Inner_->PrepareCreate(store, request, attempt);
    }
    Status CheckAppendAdmissible(const StoreInfo& store,
        const CollectionInfo& collection,
        const BaseIdentity& base,
        detail::NormalizedInput& input) override
    {
        return Inner_->CheckAppendAdmissible(store, collection, base, input);
    }
    Result<std::unique_ptr<detail::StagedAppend>> PrepareAppend(const StoreInfo& store,
        const detail::BoundOperation& binding,
        const CollectionInfo& collection,
        const detail::NormalizedInput& input,
        std::chrono::steady_clock::time_point deadline) override
    {
        auto staged = Inner_->PrepareAppend(store, binding, collection, input, deadline);
        if (!staged) {
            Die("prepare failed: " + staged.error().message);
        }
        _exit(77);
    }
    void PublicationFinished(std::string_view collection, std::string_view location, CommitDisposition result) override
    {
        Inner_->PublicationFinished(collection, location, result);
    }
    void InvalidateCache() override
    {
        Inner_->InvalidateCache();
    }

    void RetireRoot(const BaseIdentity& superseded) override
    {
        Inner_->RetireRoot(superseded);
    }
    Status VerifyRetained(const StoreInfo& store,
        std::span<const detail::CollectionView> collections,
        std::span<const detail::OperationRecord> operations) override
    {
        return Inner_->VerifyRetained(store, collections, operations);
    }

    Result<std::shared_ptr<iceberg::FileIO>> ReadOnlyFileIO() override
    {
        return Inner_->ReadOnlyFileIO();
    }

private:
    std::shared_ptr<detail::StorageBackend> Inner_;
};

} // namespace

int main(int argc, char** argv)
try {
    if (argc < 3) {
        return 2;
    }
    const std::string mode = argv[1];
    const std::string root = argv[2];
    if (mode == "write") {
        auto engine = Check(CreateStore({StoreId, root}), "create store");
        if (engine->CreateCollection({CollectionId, "events", Schema}).disposition != CommitDisposition::Committed) {
            Die("create collection");
        }
        const auto a = engine->AppendDurable({CollectionId, OperationA, {}, {Batch(RowsA)}});
        const auto b = engine->AppendDurable({CollectionId, OperationB, {}, {Batch(RowsB)}});
        if (!a.result || !b.result) {
            Die("append");
        }
        std::cout << "{\"s1\":\"" << a.result->snapshot_id << "\",\"s2\":\"" << b.result->snapshot_id << "\"}" << '\n'
                  << std::flush;
        _exit(0); // No Shutdown: the next process must recover from disk alone.
    }
    if (mode == "prepared-crash") {
        const detail::BackendFactory factory =
            [](const std::filesystem::path& path,
                const StoreInfo& info) -> Result<std::shared_ptr<detail::StorageBackend>> {
            auto inner = detail::TLocalStorageBackend::Open(path, info);
            if (!inner) {
                return std::unexpected(inner.error());
            }
            return std::make_shared<TCrashAfterPrepare>(std::move(*inner));
        };
        auto engine = Check(detail::EngineFactory::OpenWith({root, StoreId}, factory), "open");
        (void)engine->AppendDurable({CollectionId, OperationC, {}, {Batch(RowsC)}});
        return 3; // Unreachable: the backend exits after PrepareAppend.
    }
    auto engine = Check(OpenExisting({root, StoreId}), "open existing");
    if (mode == "read") {
        if (argc != 5) {
            return 2;
        }
        const auto lookup = Check(engine->FindAppendOutcome(CollectionId, OperationC), "lookup");
        std::cout << "{\"latest\":" << Rows(*engine, SnapshotSelector::Latest())
                  << ",\"s1\":" << Rows(*engine, SnapshotSelector::Id(std::stoll(argv[3])))
                  << ",\"s2\":" << Rows(*engine, SnapshotSelector::Id(std::stoll(argv[4])))
                  << ",\"empty\":" << Rows(*engine, SnapshotSelector::InitialEmpty())
                  << ",\"crashed\":" << (lookup ? Outcome(*lookup) : std::string("null")) << "}" << '\n';
    } else if (mode == "append-more") {
        const auto retry = engine->AppendDurable({CollectionId, OperationC, {}, {Batch(RowsC)}});
        const auto next = engine->AppendDurable({CollectionId, OperationD, {}, {Batch(RowsD)}});
        std::cout << "{\"retry\":" << Outcome(retry) << ",\"next\":" << Outcome(next) << "}" << '\n';
    } else {
        return 2;
    }
    const auto shutdown = engine->Shutdown(std::chrono::milliseconds(1000));
    if (!shutdown) {
        Die("shutdown: " + shutdown.error().message);
    }
    return 0;
} catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 100;
}
