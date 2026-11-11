#pragma once

#include "storage/scan.h"

namespace glamdb {

enum class BackendKind : uint8_t { Unavailable, Custom, LocalStorage };

// Supported operations, independent of current admission (GetHealth).
// Selecting local storage does not qualify power-loss durability.
struct EngineCapabilities
{
    BackendKind backend = BackendKind::Unavailable;
    bool append = false;
    bool scan = false;
    bool metadata = false;
    bool file_pruning = false;
    bool row_group_pruning = false;
    bool durability_qualified = false;
};

struct SnapshotMetadata
{
    int64_t snapshot_id = 0;
    std::optional<int64_t> parent_snapshot_id{};
    int64_t created_at_us = 0;
    AppendResult receipt{};
};

struct SnapshotHistory
{
    // The confirmed root from which the entire history was read.
    BaseIdentity identity{};
    // Oldest first; initial empty is a view, not a snapshot.
    std::vector<SnapshotMetadata> snapshots{};
};

struct CollectionStatistics
{
    // Root identity and selected snapshot are separate for historical views.
    BaseIdentity identity{};
    SnapshotSelector selector_resolved{};
    Metric rows{};
    Metric data_files{};
    Metric compressed_data_bytes{};
    Metric logical_bytes{};
};

struct TFileFieldMetrics
{
    int32_t FieldID = 0;
    std::optional<int64_t> CompressedBytes{};
    std::optional<int64_t> ValueCount{};
    std::optional<int64_t> NullCount{};
    std::optional<int64_t> NanCount{};
    // Iceberg single-value binary encoding; string bounds may be truncated.
    std::optional<std::vector<uint8_t>> LowerBound{};
    std::optional<std::vector<uint8_t>> UpperBound{};
    bool operator==(const TFileFieldMetrics&) const = default;
};

struct TDataFileMetadata
{
    std::string FileID{};
    std::string Format = "parquet";
    int64_t RecordCount = 0;
    int64_t CompressedBytes = 0;
    std::vector<TFileFieldMetrics> FieldMetrics{};
    bool operator==(const TDataFileMetadata&) const = default;
};

struct TFileListing
{
    BaseIdentity Identity{};
    SnapshotSelector SelectorResolved{};
    std::vector<TDataFileMetadata> Files{};
};

struct TColumnChunkMetadata
{
    int32_t FieldID = 0;
    int64_t ValueCount = 0;
    int64_t CompressedBytes = 0;
    int64_t UncompressedBytes = 0;
    int64_t DataPageOffset = 0;
    std::optional<int64_t> DictionaryPageOffset{};
    std::string Compression{};
    std::optional<int64_t> NullCount{};
    // Parquet plain encoding, independent of the manifest's Iceberg bounds.
    std::optional<std::vector<uint8_t>> LowerBound{};
    std::optional<std::vector<uint8_t>> UpperBound{};
    bool HasColumnIndex = false;
    bool HasOffsetIndex = false;
};

struct TRowGroupMetadata
{
    int32_t Ordinal = 0;
    int64_t RecordCount = 0;
    int64_t CompressedBytes = 0;
    int64_t UncompressedBytes = 0;
    std::vector<TColumnChunkMetadata> Columns{};
};

struct TFileDetails
{
    BaseIdentity Identity{};
    SnapshotSelector SelectorResolved{};
    TDataFileMetadata File{};
    std::vector<TRowGroupMetadata> RowGroups{};
};

struct ScanExplanation
{
    ScanInfo info{};
    std::vector<std::string> read_fields{};
    Predicate predicate{};
    std::optional<int64_t> limit{};
    int64_t batch_rows = 65536;
    bool file_pruning = false;
    bool row_group_pruning = false;
    // All live files of the selected view, before predicate/limit execution.
    CollectionStatistics candidates{};
    Metric estimated_output_rows{};
};

} // namespace glamdb
