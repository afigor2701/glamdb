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

// Extracts and validates Iceberg manifest column metrics for `readFields`.
// Corrupt or out-of-domain persisted statistics fail with `CORRUPTION` before pruning.
Result<std::vector<TColumnZoneStats>> ExtractFileZoneStats(const iceberg::DataFile& dataFile,
    const iceberg::Schema& icebergSchema,
    const SchemaSpec& schema,
    std::span<const size_t> readFields);

// Extracts and validates Parquet row-group statistics for `readFields`.
// Corrupt or out-of-domain persisted statistics fail with `CORRUPTION` before pruning.
Result<std::vector<TColumnZoneStats>> ExtractRowGroupZoneStats(const parquet::RowGroupMetaData& group,
    const SchemaSpec& schema,
    std::span<const size_t> readFields);

// Evaluates conservative SQL three-valued truth possibilities over a zone.
// `readFields` must be sorted and paired 1:1 with `readStats`, covering `predicate.ReferencedFields`.
Result<TZoneTruth> EvaluateZonePredicate(const TBoundPredicate& predicate,
    int64_t rowCount,
    std::span<const size_t> readFields,
    std::span<const TColumnZoneStats> readStats);

// Returns true iff every referenced column carries zone statistics and `CanBeTrue` is false.
Result<bool> CanPruneZone(const TBoundPredicate& predicate,
    int64_t rowCount,
    std::span<const size_t> readFields,
    std::span<const TColumnZoneStats> readStats);

} // namespace glamdb::detail
