#pragma once

#include "core/scan/predicate.h"

#include <cstdint>
#include <optional>
#include <span>
#include <vector>

namespace iceberg {
struct DataFile;
class Schema;
} // namespace iceberg

namespace parquet {
class RowGroupMetaData;
} // namespace parquet

namespace glamdb::detail {

struct TZoneTruth
{
    bool CanBeTrue = true;
    bool CanBeFalse = true;
    bool CanBeUnknown = true;

    bool operator==(const TZoneTruth&) const = default;
};

struct TColumnZoneStats
{
    FieldType Type = FieldType::String;
    bool Nullable = true;
    bool HasStats = false;
    std::optional<int64_t> NullCount{};
    std::optional<Literal> LowerBound{};
    std::optional<Literal> UpperBound{};
    bool LowerExact = true;
    bool UpperExact = true;
};

// Extracts Iceberg manifest column metrics for `readFields`.
// Malformed, incomplete, or contradictory per-column statistics are discarded
// (`prune nothing`) so exact column decoding and validation handle the file.
Result<std::vector<TColumnZoneStats>> ExtractFileZoneStats(const iceberg::DataFile& dataFile,
    const iceberg::Schema& icebergSchema,
    const SchemaSpec& schema,
    std::span<const size_t> readFields);

// Extracts Parquet row-group statistics for `readFields`.
// Malformed, incomplete, or contradictory per-column statistics are discarded
// (`prune nothing`); Float64 bounds require a zero manifest `nan_value_counts` entry.
Result<std::vector<TColumnZoneStats>> ExtractRowGroupZoneStats(const parquet::RowGroupMetaData& group,
    const SchemaSpec& schema,
    std::span<const size_t> readFields,
    const iceberg::DataFile* dataFile = nullptr,
    const iceberg::Schema* icebergSchema = nullptr);

// Evaluates conservative SQL three-valued truth possibilities over a zone.
// `readFields` must be sorted and paired 1:1 with `readStats`, covering `predicate.ReferencedFields`.
Result<TZoneTruth> EvaluateZonePredicate(const TBoundPredicate& predicate,
    int64_t rowCount,
    std::span<const size_t> readFields,
    std::span<const TColumnZoneStats> readStats);

// Returns true iff `EvaluateZonePredicate` proves `CanBeTrue` is false for the zone.
Result<bool> CanPruneZone(const TBoundPredicate& predicate,
    int64_t rowCount,
    std::span<const size_t> readFields,
    std::span<const TColumnZoneStats> readStats);

} // namespace glamdb::detail
