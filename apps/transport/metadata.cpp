#include "metadata.h"
#include "apps/transport/outcome_json.h"

#include <arrow/util/utf8.h>

#include <array>
#include <bit>
#include <cmath>
#include <mutex>

namespace glamdb::NTransport {
TJson OptionalInteger(std::optional<int64_t> value)
{
    return value ? TJson(std::to_string(*value)) : TJson(nullptr);
}
TJson Snapshot(SnapshotSelector selector)
{
    return selector.kind == SnapshotSelectorKind::Id ? TJson(std::to_string(selector.snapshot_id)) : TJson(nullptr);
}
TJson Value(const Metric& metric)
{
    return OptionalInteger(metric.value);
}
std::string MetricKindName(const Metric& metric)
{
    const std::array names{"unavailable", "metadata", "estimate", "measured"};
    return names.at(static_cast<size_t>(metric.kind));
}
TJson NativeIdentity(const BaseIdentity& identity, SnapshotSelector selector)
{
    return {{"identity", EncodeIdentity(identity)},
        {"snapshot_id", Snapshot(selector)},
        {"backend", "local"},
        {"mock", false},
        {"synthetic", false},
        {"durability_validation", "designed"}};
}
TJson Statistics(const CollectionStatistics& statistics)
{
    auto value = NativeIdentity(statistics.identity, statistics.selector_resolved);
    value.update({{"rows", Value(statistics.rows)},
        {"data_files", Value(statistics.data_files)},
        {"compressed_data_bytes", Value(statistics.compressed_data_bytes)},
        {"logical_bytes", Value(statistics.logical_bytes)},
        {"root_metadata_bytes", nullptr},
        {"byte_measurement", "committed_data_file_descriptors"},
        {"metric_kinds",
            {{"rows", MetricKindName(statistics.rows)},
                {"data_files", MetricKindName(statistics.data_files)},
                {"compressed_data_bytes", MetricKindName(statistics.compressed_data_bytes)},
                {"logical_bytes", MetricKindName(statistics.logical_bytes)},
                {"root_metadata_bytes", "unavailable"}}}});
    return value;
}
size_t HistorySize(const SnapshotHistory& history, SnapshotSelector selector)
{
    if (selector.kind == SnapshotSelectorKind::InitialEmpty) {
        return 0;
    }
    if (selector.kind == SnapshotSelectorKind::Latest) {
        return history.snapshots.size();
    }
    for (size_t i = 0; i < history.snapshots.size(); ++i) {
        if (history.snapshots[i].snapshot_id == selector.snapshot_id) {
            return i + 1;
        }
    }
    throw TFailure({"SNAPSHOT_NOT_FOUND", "snapshot is not in the confirmed collection history", RetryHint::FixInput});
}
using TBound = std::optional<std::vector<uint8_t>>;
TJson BinaryHex(const TBound& bound)
{
    if (!bound) {
        return nullptr;
    }
    constexpr char Hex[] = "0123456789abcdef";
    std::string result;
    result.reserve(bound->size() * 2);
    for (const auto byte : *bound) {
        result += Hex[byte >> 4];
        result += Hex[byte & 15];
    }
    return result;
}
TJson DecodeBound(const TBound& bound, FieldType type)
{
    if (!bound) {
        return nullptr;
    }
    // Supported Iceberg single values and Parquet statistics share these scalar
    // encodings. Their provenance and original bytes remain separate on the wire.
    if (type == FieldType::String) {
        const std::string text(bound->begin(), bound->end());
        static std::once_flag Utf8;
        std::call_once(Utf8, arrow::util::InitializeUTF8);
        return arrow::util::ValidateUTF8(text) ? TJson(text) : TJson(nullptr);
    }
    if (type == FieldType::Bool) {
        return bound->size() == 1 && bound->front() <= 1 ? TJson(bound->front() != 0) : TJson(nullptr);
    }
    if (bound->size() != 8) {
        return nullptr;
    }
    uint64_t bits = 0;
    for (size_t i = 0; i < bound->size(); ++i) {
        bits |= static_cast<uint64_t>((*bound)[i]) << (8 * i);
    }
    if (type == FieldType::Float64) {
        const auto value = std::bit_cast<double>(bits);
        return std::isfinite(value) ? TJson(value) : TJson(nullptr);
    }
    const auto value = std::bit_cast<int64_t>(bits);
    if (type == FieldType::TimestampUsUtc) {
        return value >= -62135596800000000LL && value <= 253402300799999999LL ? EncodeCell(TimestampUs{value})
                                                                              : TJson(nullptr);
    }
    return type == FieldType::Int64 ? TJson(std::to_string(value)) : TJson(nullptr);
}
TJson FieldStatistics(int32_t fieldID,
    const SchemaSpec& schema,
    const TBound& lower,
    const TBound& upper,
    std::string_view encoding)
{
    Require(fieldID > 0 && static_cast<size_t>(fieldID) <= schema.fields.size(),
        "metadata contains an unknown field ID",
        "CORRUPTION");
    const auto& field = schema.fields[static_cast<size_t>(fieldID) - 1];
    const std::array names{"string", "bool", "int64", "float64", "timestamp_us_utc"};
    return {{"field_id", fieldID},
        {"name", field.name},
        {"type", names.at(static_cast<size_t>(field.type) - 1)},
        {"nullable", field.nullable},
        {"distinct_count", nullptr},
        {"lower_bound", DecodeBound(lower, field.type)},
        {"upper_bound", DecodeBound(upper, field.type)},
        {"lower_bound_hex", BinaryHex(lower)},
        {"upper_bound_hex", BinaryHex(upper)},
        {"bound_encoding", encoding},
        {"lower_bound_exact", nullptr},
        {"upper_bound_exact", nullptr}};
}
TJson ManifestMetrics(const std::vector<TFileFieldMetrics>& fields, const SchemaSpec& schema)
{
    auto value = TJson::array();
    for (const auto& field : fields) {
        auto metrics =
            FieldStatistics(field.FieldID, schema, field.LowerBound, field.UpperBound, "iceberg-single-value");
        metrics.update({{"null_count", OptionalInteger(field.NullCount)},
            {"value_count", OptionalInteger(field.ValueCount)},
            {"nan_count", OptionalInteger(field.NanCount)},
            {"compressed_bytes", OptionalInteger(field.CompressedBytes)},
            {"uncompressed_bytes", nullptr}});
        value.push_back(std::move(metrics));
    }
    return value;
}
TJson File(const TDataFileMetadata& file, const SchemaSpec& schema)
{
    return {{"file_id", file.FileID},
        {"path", nullptr},
        {"content", "data"},
        {"format", file.Format},
        {"record_count", std::to_string(file.RecordCount)},
        {"compressed_bytes", std::to_string(file.CompressedBytes)},
        {"field_metrics", ManifestMetrics(file.FieldMetrics, schema)},
        {"synthetic", false},
        {"physical_file_exists", nullptr},
        {"backend", "local"},
        {"mock", false}};
}
TJson Columns(const std::vector<TColumnChunkMetadata>& columns, const SchemaSpec& schema)
{
    auto value = TJson::array();
    for (const auto& column : columns) {
        auto metrics = FieldStatistics(column.FieldID, schema, column.LowerBound, column.UpperBound, "parquet-plain");
        metrics.update({{"null_count", OptionalInteger(column.NullCount)},
            {"value_count", std::to_string(column.ValueCount)},
            {"compressed_bytes", std::to_string(column.CompressedBytes)},
            {"uncompressed_bytes", std::to_string(column.UncompressedBytes)},
            {"data_page_offset", std::to_string(column.DataPageOffset)},
            {"dictionary_page_offset", OptionalInteger(column.DictionaryPageOffset)},
            {"compression", column.Compression},
            {"indexes",
                {{"page", column.HasColumnIndex || column.HasOffsetIndex},
                    {"column", column.HasColumnIndex},
                    {"offset", column.HasOffsetIndex},
                    {"bloom", nullptr}}}});
        value.push_back(std::move(metrics));
    }
    return value;
}
SnapshotSelector MetadataSelector(const std::map<std::string, std::string>& query)
{
    const auto found = query.find("snapshot_id");
    if (found == query.end() || found->second == "latest") {
        return SnapshotSelector::Latest();
    }
    if (found->second == "initial") {
        return SnapshotSelector::InitialEmpty();
    }
    const auto id = ParseInt64(found->second);
    Require(std::to_string(id) == found->second, "snapshot_id must be a canonical decimal int64");
    return SnapshotSelector::Id(id);
}

TJson StatisticsJson(const Engine& engine,
    const std::string& collection,
    const std::map<std::string, std::string>& query)
{
    for (const auto& [key, ignored] : query) {
        (void)ignored;
        Require(key == "snapshot_id", "unknown statistics parameter");
    }
    const auto statistics = Unwrap(engine.GetStatistics(collection, MetadataSelector(query)));
    const auto history = Unwrap(engine.ListSnapshots(collection));
    // Appends may advance the second call's root; count only the history prefix
    // belonging to the first call's confirmed root. Snapshot expiration is absent.
    const auto head = statistics.identity.current_snapshot_id;
    const auto count = HistorySize(history, head ? SnapshotSelector::Id(*head) : SnapshotSelector::InitialEmpty());
    auto result = Statistics(statistics);
    result["retained_snapshots"] = std::to_string(count);
    return result;
}

TJson SnapshotsJson(const Engine& engine, const std::string& collection, SnapshotSelector selector)
{
    const auto history = Unwrap(engine.ListSnapshots(collection));
    const auto size = HistorySize(history, selector);
    const auto selected =
        size ? SnapshotSelector::Id(history.snapshots[size - 1].snapshot_id) : SnapshotSelector::InitialEmpty();
    auto result = NativeIdentity(history.identity, selected);
    auto items = TJson::array();
    for (size_t i = 0; i < size; ++i) {
        const auto& snapshot = history.snapshots[i];
        AppendOutcome outcome;
        outcome.disposition = CommitDisposition::Committed;
        outcome.phase = CommitPhase::Complete;
        outcome.operation_id = snapshot.receipt.operation_id;
        outcome.result = snapshot.receipt;
        const auto receipt = TJson::parse(Unwrap(json_adapter::ToJson(outcome)).body).at("result");
        items.push_back({{"snapshot_id", std::to_string(snapshot.snapshot_id)},
            {"parent_id", OptionalInteger(snapshot.parent_snapshot_id)},
            {"parent_snapshot_id", OptionalInteger(snapshot.parent_snapshot_id)},
            {"created_at_us", std::to_string(snapshot.created_at_us)},
            {"rows_added", std::to_string(snapshot.receipt.rows_appended)},
            {"operation_id", snapshot.receipt.operation_id},
            {"receipt", receipt},
            {"backend", "local"},
            {"mock", false}});
    }
    result["items"] = std::move(items);
    return result;
}

TJson FilesJson(const Engine& engine, const std::string& collection, SnapshotSelector selector)
{
    const auto info = Unwrap(engine.GetCollection(collection));
    const auto inventory = Unwrap(engine.ListFiles(collection, selector));
    auto result = NativeIdentity(inventory.Identity, inventory.SelectorResolved);
    auto items = TJson::array();
    for (const auto& file : inventory.Files) {
        items.push_back(File(file, info.schema));
    }
    result["items"] = std::move(items);
    return result;
}

TJson FileJson(const Engine& engine,
    const std::string& collection,
    const std::string& file,
    const std::map<std::string, std::string>& query)
{
    for (const auto& [key, ignored] : query) {
        (void)ignored;
        Require(key == "snapshot_id", "unknown file parameter");
    }
    const auto info = Unwrap(engine.GetCollection(collection));
    const auto details = Unwrap(engine.GetFileDetails(collection, file, MetadataSelector(query)));
    auto value = NativeIdentity(details.Identity, details.SelectorResolved);
    value.update(File(details.File, info.schema));
    value["physical_file_exists"] = true;
    value["footer_bytes"] = std::to_string(details.FooterBytes);
    value["row_groups"] = TJson::array();
    bool columnIndex = false, offsetIndex = false;
    for (const auto& group : details.RowGroups) {
        for (const auto& column : group.Columns) {
            columnIndex |= column.HasColumnIndex;
            offsetIndex |= column.HasOffsetIndex;
        }
        value["row_groups"].push_back({{"ordinal", group.Ordinal},
            {"record_count", std::to_string(group.RecordCount)},
            {"compressed_bytes", std::to_string(group.CompressedBytes)},
            {"uncompressed_bytes", std::to_string(group.UncompressedBytes)},
            {"field_metrics", Columns(group.Columns, info.schema)},
            {"synthetic", false}});
    }
    // A single row group's values are file-wide observations. Multiple groups
    // remain separate rather than manufacturing aggregate extrema or counts.
    value["footer_statistics"] =
        details.RowGroups.size() == 1 ? value["row_groups"].at(0).at("field_metrics") : TJson(nullptr);
    value["indexes"] = {{"page", columnIndex || offsetIndex},
        {"column", columnIndex},
        {"offset", offsetIndex},
        {"bloom", nullptr}};
    const auto capabilities = engine.GetCapabilities();
    value["execution_capabilities"] = {{"file_pruning", capabilities.file_pruning},
        {"row_group_pruning", capabilities.row_group_pruning},
        {"page_pruning", false},
        {"bloom_pruning", false}};
    return value;
}

} // namespace glamdb::NTransport
