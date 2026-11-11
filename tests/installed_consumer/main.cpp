#include <storage/engine.h>
#include <storage/scan.h>

#include <arrow/api.h>

#include <algorithm>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

namespace {

constexpr char StoreId[] = "00000000-0000-0000-0000-000000000001";
constexpr char CollectionId[] = "00112233-4455-6677-8899-aabbccddeeff";
constexpr char OperationA[] = "00000000-0000-0000-0000-000000000004";
constexpr char OperationB[] = "00000000-0000-0000-0000-000000000005";
constexpr char UnknownOperation[] = "00000000-0000-0000-0000-000000000006";

using TRow = std::pair<std::string, std::optional<int64_t>>;
const std::vector<TRow> RowsA{{"dup", 1}, {"dup", 1}, {"null", std::nullopt}};
const std::vector<TRow> RowsB{{"b", 2}, {"dup", 3}};
const std::vector<TRow> RowsAB{{"dup", 1}, {"dup", 1}, {"null", std::nullopt}, {"b", 2}, {"dup", 3}};
const glamdb::SchemaSpec Schema{{{"key", glamdb::FieldType::String, false}, {"n", glamdb::FieldType::Int64, true}}};

void Check(bool condition, std::string_view message)
{
    if (!condition) {
        throw std::runtime_error(std::string(message));
    }
}

glamdb::Predicate CompoundDupPredicate()
{
    const auto predicate = glamdb::Predicate::And({
        glamdb::Predicate::Compare("key", glamdb::CompareOp::Eq, std::string("dup")),
        glamdb::Predicate::Or({
            glamdb::Predicate::In("n", {int64_t{1}, std::nullopt}),
            glamdb::Predicate::Not(glamdb::Predicate::IsNotNull("n")),
            glamdb::Predicate::CompareNull("n", glamdb::CompareOp::Eq),
        }),
    });
    Check(predicate.kind() == glamdb::Predicate::Kind::And && predicate.children().size() == 2U,
        "compound predicate AST accessors");
    return predicate;
}

template <class T>
T Take(glamdb::Result<T> result, std::string_view context)
{
    if (!result) {
        throw std::runtime_error(std::string(context) + ": " + result.error().code + ": " + result.error().message);
    }
    return std::move(*result);
}

std::shared_ptr<arrow::Schema> InputSchema()
{
    return arrow::schema({arrow::field("key", arrow::utf8(), false), arrow::field("n", arrow::int64())});
}

std::shared_ptr<arrow::RecordBatch> MakeBatch(const std::vector<TRow>& rows)
{
    arrow::StringBuilder keys;
    arrow::Int64Builder values;
    for (const auto& [key, value] : rows) {
        Check(keys.Append(key).ok(), "build keys");
        Check((value ? values.Append(*value) : values.AppendNull()).ok(), "build values");
    }
    std::shared_ptr<arrow::Array> keyArray;
    std::shared_ptr<arrow::Array> valueArray;
    Check(keys.Finish(&keyArray).ok() && values.Finish(&valueArray).ok(), "finish input arrays");
    return arrow::RecordBatch::Make(InputSchema(), static_cast<int64_t>(rows.size()), {keyArray, valueArray});
}

void CheckRows(glamdb::ScanCursor& cursor, std::vector<TRow> expected, glamdb::SnapshotSelector resolved)
{
    Check(cursor.Info().selector_resolved == resolved, "resolved scan selector");
    Check(cursor.Info().output_schema && cursor.Info().output_schema->Equals(*InputSchema()), "scan schema");
    std::vector<TRow> rows;
    while (const auto batch = Take(cursor.Next(), "scan next")) {
        Check(batch->ValidateFull().ok() && batch->schema()->Equals(*InputSchema()), "scan batch schema and values");
        const auto& keys = static_cast<const arrow::StringArray&>(*batch->column(0));
        const auto& values = static_cast<const arrow::Int64Array&>(*batch->column(1));
        for (int64_t row = 0; row < batch->num_rows(); ++row) {
            Check(!keys.IsNull(row), "required key");
            rows.emplace_back(keys.GetString(row),
                values.IsNull(row) ? std::nullopt : std::optional<int64_t>(values.Value(row)));
        }
    }
    std::ranges::sort(rows);
    std::ranges::sort(expected);
    Check(rows == expected, "scan rows must match the literal multiset, including duplicates and NULL");
}

void CheckScan(glamdb::Engine& engine,
    glamdb::SnapshotSelector selector,
    const std::vector<TRow>& expected,
    glamdb::SnapshotSelector resolved)
{
    auto cursor = Take(engine.Scan({.collection_id = CollectionId, .selector = selector, .batch_rows = 1}), "scan");
    CheckRows(*cursor, expected, resolved);
}

const glamdb::AppendResult& CheckReceipt(const glamdb::AppendOutcome& outcome, std::string_view operation, int64_t rows)
{
    if (!outcome.result) {
        throw std::runtime_error("committed append receipt is missing");
    }
    Check(outcome.disposition == glamdb::CommitDisposition::Committed &&
            outcome.phase == glamdb::CommitPhase::Complete && outcome.operation_id == operation && !outcome.error &&
            !outcome.recovery,
        "committed append outcome");
    const auto& receipt = *outcome.result;
    Check(receipt.collection_id == CollectionId && receipt.operation_id == operation && receipt.rows_appended == rows &&
            receipt.data_files_added == 1 && receipt.compressed_data_bytes_added > 0 && receipt.snapshot_id != 0 &&
            receipt.snapshot_created_at_us > 0 && !receipt.payload_hash.empty() && receipt.hash_version == 1 &&
            receipt.receipt_version == 1 && receipt.durability_profile == "local-delete-extra-v1",
        "complete append receipt");
    return receipt;
}

std::string RenderReceipt(const glamdb::AppendResult& receipt)
{
    std::ostringstream output;
    output << std::quoted(receipt.collection_id) << ' ' << std::quoted(receipt.operation_id) << ' '
           << std::quoted(receipt.payload_hash) << ' ' << receipt.hash_version << ' ' << receipt.snapshot_id << ' '
           << (receipt.parent_snapshot_id ? std::to_string(*receipt.parent_snapshot_id) : "none") << ' '
           << receipt.rows_appended << ' ' << receipt.data_files_added << ' ' << receipt.compressed_data_bytes_added
           << ' ' << receipt.snapshot_created_at_us << ' ' << std::quoted(receipt.durability_profile) << ' '
           << receipt.receipt_version;
    return output.str();
}

void CheckViews(glamdb::Engine& engine, int64_t snapshotA, int64_t snapshotB)
{
    CheckScan(engine, glamdb::SnapshotSelector::Latest(), RowsAB, glamdb::SnapshotSelector::Id(snapshotB));
    CheckScan(engine, glamdb::SnapshotSelector::Id(snapshotA), RowsA, glamdb::SnapshotSelector::Id(snapshotA));
    CheckScan(engine, glamdb::SnapshotSelector::InitialEmpty(), {}, glamdb::SnapshotSelector::InitialEmpty());
    auto filtered = Take(engine.Scan({.collection_id = CollectionId,
                             .selector = glamdb::SnapshotSelector::Id(snapshotB),
                             .predicate = CompoundDupPredicate(),
                             .batch_rows = 1}),
        "compound filtered scan");
    CheckRows(*filtered, {{"dup", 1}, {"dup", 1}}, glamdb::SnapshotSelector::Id(snapshotB));
    const auto capabilities = engine.GetCapabilities();
    Check(capabilities.backend == glamdb::BackendKind::LocalStorage && capabilities.metadata && capabilities.scan &&
            capabilities.append && !capabilities.file_pruning && !capabilities.row_group_pruning &&
            !capabilities.durability_qualified,
        "native capabilities do not claim pruning or qualified durability");
    const auto history = Take(engine.ListSnapshots(CollectionId), "snapshot history");
    Check(history.snapshots.size() == 2 && history.snapshots[0].snapshot_id == snapshotA &&
            history.snapshots[1].snapshot_id == snapshotB && history.snapshots[1].parent_snapshot_id == snapshotA,
        "retained history and parent identity");
    const auto a = Take(engine.FindAppendOutcome(CollectionId, OperationA), "metadata receipt A");
    const auto b = Take(engine.FindAppendOutcome(CollectionId, OperationB), "metadata receipt B");
    Check(a && b && a->result && b->result && history.snapshots[0].receipt == *a->result &&
            history.snapshots[1].receipt == *b->result,
        "history includes original durable receipts");
    for (const auto& [selector, rows, files] : {std::tuple{glamdb::SnapshotSelector::Latest(), 5, 2},
             std::tuple{glamdb::SnapshotSelector::Id(snapshotA), 3, 1},
             std::tuple{glamdb::SnapshotSelector::InitialEmpty(), 0, 0}})
    {
        const auto stats = Take(engine.GetStatistics(CollectionId, selector), "selected statistics");
        Check(stats.rows.value == rows && stats.data_files.value == files && !stats.logical_bytes.value &&
                stats.rows.kind == glamdb::MetricKind::Metadata && stats.identity == history.identity,
            "view-specific metadata with unavailable logical size");
        const auto listing = Take(engine.ListFiles(CollectionId, selector), "selected files");
        Check(listing.Identity == stats.identity && listing.SelectorResolved == stats.selector_resolved &&
                static_cast<int64_t>(listing.Files.size()) == files,
            "manifest file membership uses the selected view");
        int64_t fileRows = 0;
        for (const auto& file : listing.Files) {
            const auto details = Take(engine.GetFileDetails(CollectionId, file.FileID, selector), "file details");
            Check(details.File == file && details.SelectorResolved == listing.SelectorResolved &&
                    !details.RowGroups.empty() && file.FileID.size() == 64,
                "opaque file IDs resolve only through manifest membership");
            int64_t groupRows = 0;
            for (const auto& group : details.RowGroups) {
                groupRows += group.RecordCount;
                Check(!group.Columns.empty() && group.Columns.front().FieldID == 1,
                    "Parquet row groups retain stable field IDs");
            }
            Check(groupRows == file.RecordCount, "row-group counts match the manifest");
            fileRows += file.RecordCount;
        }
        Check(fileRows == rows, "listed files match selected rows");
        const auto explanation =
            Take(engine.ExplainScan({.collection_id = CollectionId, .selector = selector}), "explain");
        Check(explanation.info.selector_resolved == stats.selector_resolved &&
                explanation.candidates.rows.value == rows && !explanation.file_pruning &&
                !explanation.row_group_pruning && explanation.estimated_output_rows.value == rows,
            "explain uses the selected fixed view");
    }
    auto zero = Take(engine.Scan({.collection_id = CollectionId, .limit = 0}), "zero limit");
    Check(!Take(zero->Next(), "zero next") && zero->Profile().data_files_read.value == 0 &&
            zero->Profile().rows_returned.value == 0 && zero->Profile().state == glamdb::ScanState::Complete,
        "zero-limit execution reads no data files");
}

void Write(glamdb::Engine& engine, const std::string& receiptPath)
{
    const auto created = engine.CreateCollection({CollectionId, "events", Schema});
    Check(created.disposition == glamdb::CommitDisposition::Committed && created.info && !created.error &&
            !created.info->current_snapshot_id,
        "create collection with the default production backend");
    CheckScan(engine, glamdb::SnapshotSelector::Latest(), {}, glamdb::SnapshotSelector::InitialEmpty());
    const auto a = engine.AppendDurable(
        {CollectionId, OperationA, {glamdb::SnapshotPreconditionKind::NoSnapshot, 0}, {MakeBatch(RowsA)}});
    const auto& receiptA = CheckReceipt(a, OperationA, 3);
    Check(!receiptA.parent_snapshot_id, "first snapshot has no parent");
    auto fixedLatest = Take(engine.Scan({.collection_id = CollectionId, .batch_rows = 1}), "fixed latest scan");
    const auto b = engine.AppendDurable({CollectionId,
        OperationB,
        {glamdb::SnapshotPreconditionKind::Equals, receiptA.snapshot_id},
        {MakeBatch(RowsB)}});
    const auto& receiptB = CheckReceipt(b, OperationB, 2);
    Check(receiptA.snapshot_id != receiptB.snapshot_id && receiptB.parent_snapshot_id == receiptA.snapshot_id,
        "second snapshot extends the first");
    CheckRows(*fixedLatest, RowsA, glamdb::SnapshotSelector::Id(receiptA.snapshot_id));
    CheckViews(engine, receiptA.snapshot_id, receiptB.snapshot_id);
    std::ofstream output(receiptPath);
    output << RenderReceipt(receiptA) << '\n' << RenderReceipt(receiptB) << '\n';
    output.close();
    Check(!output.fail(), "save original receipts for the next process");
}

void Reopen(glamdb::Engine& engine, const std::string& receiptPath)
{
    const auto collection = Take(engine.GetCollection(CollectionId), "get reopened collection");
    Check(collection.schema == Schema && collection.name == "events", "persisted collection schema and name");
    Check(Take(engine.ListCollections(), "list reopened collections").size() == 1, "persisted collection count");
    const auto a = Take(engine.FindAppendOutcome(CollectionId, OperationA), "find receipt A");
    const auto b = Take(engine.FindAppendOutcome(CollectionId, OperationB), "find receipt B");
    if (!a || !b) {
        throw std::runtime_error("both append outcomes must survive reopen");
    }
    const auto& receiptA = CheckReceipt(*a, OperationA, 3);
    const auto& receiptB = CheckReceipt(*b, OperationB, 2);
    std::ifstream input(receiptPath);
    std::string expectedA;
    std::string expectedB;
    Check(static_cast<bool>(std::getline(input, expectedA)) && static_cast<bool>(std::getline(input, expectedB)),
        "read original receipts");
    Check(RenderReceipt(receiptA) == expectedA && RenderReceipt(receiptB) == expectedB,
        "every receipt field must survive reopen unchanged");
    Check(collection.current_snapshot_id == receiptB.snapshot_id, "persisted latest snapshot");
    Check(!Take(engine.FindAppendOutcome(CollectionId, UnknownOperation), "find unknown operation"),
        "unknown operation has no receipt");
    CheckViews(engine, receiptA.snapshot_id, receiptB.snapshot_id);
    const auto retryA = engine.AppendDurable(
        {CollectionId, OperationA, {glamdb::SnapshotPreconditionKind::NoSnapshot, 0}, {MakeBatch(RowsA)}});
    const auto retryB = engine.AppendDurable(
        {CollectionId, OperationB, {glamdb::SnapshotPreconditionKind::NoSnapshot, 0}, {MakeBatch(RowsB)}});
    Check(retryA == *a && retryB == *b, "retries return original outcomes despite stale preconditions");
    CheckViews(engine, receiptA.snapshot_id, receiptB.snapshot_id);
}

void CheckUnavailable(glamdb::Engine& engine)
{
    const auto capabilities = engine.GetCapabilities();
    Check(capabilities.backend == glamdb::BackendKind::Unavailable && !capabilities.metadata && !capabilities.scan &&
            !capabilities.append && !capabilities.durability_qualified,
        "storage-disabled capabilities");
    const auto stats = engine.GetStatistics(CollectionId);
    const auto history = engine.ListSnapshots(CollectionId);
    const auto explain = engine.ExplainScan({.collection_id = CollectionId});
    const auto files = engine.ListFiles(CollectionId);
    const auto details = engine.GetFileDetails(CollectionId, std::string(64, '0'));
    Check(!stats && stats.error().code == "DEPENDENCY_UNAVAILABLE" && !history &&
            history.error().code == "DEPENDENCY_UNAVAILABLE" && !explain &&
            explain.error().code == "DEPENDENCY_UNAVAILABLE" && !files &&
            files.error().code == "DEPENDENCY_UNAVAILABLE" && !details &&
            details.error().code == "DEPENDENCY_UNAVAILABLE",
        "storage-disabled metadata reports dependency errors");
    const auto created = engine.CreateCollection({CollectionId, "events", Schema});
    Check(created.disposition == glamdb::CommitDisposition::DefinitelyNotCommitted && !created.info && created.error &&
            created.error->code == "DEPENDENCY_UNAVAILABLE",
        "storage-disabled create must report DEPENDENCY_UNAVAILABLE");
    const auto appended = engine.AppendDurable({CollectionId, OperationA, {}, {MakeBatch(RowsA)}});
    Check(appended.disposition == glamdb::CommitDisposition::DefinitelyNotCommitted &&
            appended.phase == glamdb::CommitPhase::Validation && !appended.result && !appended.recovery &&
            appended.error && appended.error->code == "DEPENDENCY_UNAVAILABLE",
        "storage-disabled append must report DEPENDENCY_UNAVAILABLE before binding");
    const auto scan = engine.Scan({.collection_id = CollectionId, .predicate = CompoundDupPredicate()});
    Check(!scan && scan.error().code == "DEPENDENCY_UNAVAILABLE", "storage-disabled scan dependency error");
    Check(Take(engine.ListCollections(), "list storage-disabled collections").empty(), "no fabricated collection");
    const auto outcome = engine.FindAppendOutcome(CollectionId, OperationA);
    Check(!outcome && outcome.error().code == "COLLECTION_NOT_FOUND", "no fabricated append outcome");
}

} // namespace

int main(int argc, char** argv)
try {
    if (argc != 5) {
        std::cerr << "usage: consumer <write|reopen> <store-root> <receipt-file> <storage|nostorage>\n";
        return 2;
    }
    const std::string mode = argv[1];
    const std::string storage = argv[4];
    Check(mode == "write" || mode == "reopen", "unknown consumer phase");
    Check(storage == "storage" || storage == "nostorage", "explicit storage expectation required");
    auto engine = mode == "write" ? Take(glamdb::CreateStore({StoreId, argv[2]}), "create store")
                                  : Take(glamdb::OpenExisting({argv[2], StoreId}), "reopen store");
    Check(engine->GetStoreInfo().store_id == StoreId, "store identity");
    if (storage == "nostorage") {
        CheckUnavailable(*engine);
    } else if (mode == "write") {
        Check(Take(engine->ListCollections(), "list initial collections").empty(), "new store is empty");
        Write(*engine, argv[3]);
    } else {
        Reopen(*engine, argv[3]);
    }
    const auto shutdown = engine->Shutdown(std::chrono::milliseconds(1000));
    Check(shutdown.has_value(), "shutdown");
    std::cout << "Installed consumer " << storage << ' ' << mode << ": PASS\n";
    return 0;
} catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
}
