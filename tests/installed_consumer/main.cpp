#include <storage/engine.h>
#include <storage/scan.h>

#include <arrow/api.h>

#include <filesystem>

namespace {

constexpr char CollectionId[] = "00112233-4455-6677-8899-aabbccddeeff";

// Exercises the installed scan contract when the package includes storage.
int CheckAppendAndScan(glamdb::Engine& engine)
{
    const glamdb::SchemaSpec schema{{{"key", glamdb::FieldType::String, false}, {"n", glamdb::FieldType::Int64, true}}};
    const auto created = engine.CreateCollection({CollectionId, "events", schema});
    if (created.disposition != glamdb::CommitDisposition::Committed) {
        // A package built without storage installs no backend or scanner.
        const auto scan = engine.Scan({.collection_id = CollectionId});
        return created.error && created.error->code == "DEPENDENCY_UNAVAILABLE" && !scan &&
                scan.error().code == "DEPENDENCY_UNAVAILABLE"
            ? 0
            : 10;
    }
    arrow::StringBuilder keys;
    arrow::Int64Builder values;
    std::shared_ptr<arrow::Array> keyArray;
    std::shared_ptr<arrow::Array> valueArray;
    if (!keys.Append("a").ok() || !keys.Append("a").ok() || !values.Append(1).ok() || !values.AppendNull().ok() ||
        !keys.Finish(&keyArray).ok() || !values.Finish(&valueArray).ok())
    {
        return 11;
    }
    const auto batch = arrow::RecordBatch::Make(
        arrow::schema({arrow::field("key", arrow::utf8(), false), arrow::field("n", arrow::int64())}),
        2,
        {keyArray, valueArray});
    const auto appended = engine.AppendDurable({CollectionId, "00000000-0000-0000-0000-000000000004", {}, {batch}});
    if (appended.disposition != glamdb::CommitDisposition::Committed || !appended.result) {
        return 12;
    }
    auto cursor = engine.Scan({.collection_id = CollectionId,
        .selector = glamdb::SnapshotSelector::Id(appended.result->snapshot_id),
        .projection = std::vector<std::string>{"n"}});
    if (!cursor || (*cursor)->Info().output_schema->num_fields() != 1) {
        return 13;
    }
    int64_t rows = 0;
    int64_t nulls = 0;
    while (true) {
        const auto next = (*cursor)->Next();
        if (!next) {
            return 14;
        }
        if (!*next) {
            break;
        }
        rows += (*next)->num_rows();
        nulls += (*next)->column(0)->null_count();
    }
    return rows == 2 && nulls == 1 ? 0 : 15;
}

} // namespace

int main(int argc, char** argv)
try {
    if (argc != 2) {
        return 1;
    }
    const std::filesystem::path root = argv[1];
    const std::string id = "00000000-0000-0000-0000-000000000001";
    {
        const auto created = glamdb::CreateStore({id, root.string()});
        if (!created || (*created)->GetStoreInfo().store_id != id || !(*created)->ListCollections()->empty()) {
            return 2;
        }
        if (!(*created)->Shutdown(std::chrono::milliseconds(1000))) {
            return 3;
        }
    }
    const auto opened = glamdb::OpenExisting({root.string(), id});
    if (!opened || (*opened)->GetCollection(CollectionId).has_value()) {
        return 4;
    }
    const auto outcome = (*opened)->FindAppendOutcome(CollectionId, "00000000-0000-0000-0000-000000000003");
    if (outcome || outcome.error().code != "COLLECTION_NOT_FOUND") {
        return 6;
    }
    if (const auto scanned = CheckAppendAndScan(**opened); scanned != 0) {
        return scanned;
    }
    return (*opened)->Shutdown(std::chrono::milliseconds(1000)) ? 0 : 5;
} catch (...) {
    return 100;
}
