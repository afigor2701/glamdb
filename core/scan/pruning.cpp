#include "core/scan/pruning.h"

#include "core/api/canonical.h"

#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/schema.h>
#include <parquet/exception.h>
#include <parquet/metadata.h>
#include <parquet/schema.h>
#include <parquet/statistics.h>
#include <parquet/types.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstring>
#include <new>
#include <string_view>

namespace glamdb::detail {
namespace {

constexpr size_t MaxKeyBytes = 1024;
constexpr size_t MaxStringBytes = 1U << 20;
constexpr size_t DefaultIcebergStringTruncateCodePoints = 16;
constexpr int64_t MinTimestampUs = -62135596800000000LL;
constexpr int64_t MaxTimestampUs = 253402300799999999LL;

ErrorDetail InternalError(std::string_view message)
{
    return {"INTERNAL", BoundedDiagnostic(message), RetryHint::OperatorAction};
}

ErrorDetail ResourceExhausted(std::string_view message)
{
    return {"RESOURCE_EXHAUSTED", BoundedDiagnostic(message), RetryHint::OperatorAction};
}

uint64_t ReadLittleEndian64(std::span<const uint8_t> bytes) noexcept
{
    uint64_t raw = 0;
    std::memcpy(&raw, bytes.data(), sizeof(raw));
    if constexpr (std::endian::native == std::endian::big) {
        raw = std::byteswap(raw);
    }
    return raw;
}

size_t Utf8CodePoints(std::string_view text) noexcept
{
    size_t count = 0;
    for (const char ch : text) {
        if ((static_cast<uint8_t>(ch) & 0xC0U) != 0x80U) {
            ++count;
        }
    }
    return count;
}

TColumnZoneStats MakeMalformedZoneStats(const FieldSpec& spec) noexcept
{
    // Nullable is forced true when statistics are malformed so even IS NULL on a
    // required column with contradictory null_count cannot prune the zone.
    return TColumnZoneStats{
        .Type = spec.type,
        .Nullable = true,
        .HasStats = false,
        .NullCount = std::nullopt,
        .LowerBound = std::nullopt,
        .UpperBound = std::nullopt,
        .LowerExact = false,
        .UpperExact = false,
    };
}

std::optional<Literal> DecodeBoundOpt(const FieldSpec& spec, std::span<const uint8_t> bytes)
{
    switch (spec.type) {
        case FieldType::String: {
            const auto maxBytes = spec.name == "key" ? MaxKeyBytes : MaxStringBytes;
            const std::string_view text(reinterpret_cast<const char*>(bytes.data()), bytes.size());
            if (text.size() > maxBytes || !ValidUtf8(text)) {
                return std::nullopt;
            }
            return Literal{std::string(text)};
        }
        case FieldType::Bool: {
            if (bytes.size() != 1 || (bytes[0] != 0 && bytes[0] != 1)) {
                return std::nullopt;
            }
            return Literal{bytes[0] != 0};
        }
        case FieldType::Int64: {
            if (bytes.size() != sizeof(int64_t)) {
                return std::nullopt;
            }
            const auto raw = ReadLittleEndian64(bytes);
            int64_t value = 0;
            std::memcpy(&value, &raw, sizeof(value));
            return Literal{value};
        }
        case FieldType::Float64: {
            if (bytes.size() != sizeof(double)) {
                return std::nullopt;
            }
            const auto raw = ReadLittleEndian64(bytes);
            double value = 0.0;
            std::memcpy(&value, &raw, sizeof(value));
            if (!std::isfinite(value)) {
                return std::nullopt;
            }
            // Parquet statistics write -0.0 as min when the minimum value is +0.0 (PARQUET-1222).
            return Literal{value == 0.0 ? 0.0 : value};
        }
        case FieldType::TimestampUsUtc: {
            if (bytes.size() != sizeof(int64_t)) {
                return std::nullopt;
            }
            const auto raw = ReadLittleEndian64(bytes);
            int64_t micros = 0;
            std::memcpy(&micros, &raw, sizeof(micros));
            if (micros < MinTimestampUs || micros > MaxTimestampUs) {
                return std::nullopt;
            }
            return Literal{TimestampUs{micros}};
        }
    }
    return std::nullopt;
}

int CompareTyped(FieldType type, const Literal& lhs, const Literal& rhs)
{
    switch (type) {
        case FieldType::String: {
            const auto& a = std::get<std::string>(lhs);
            const auto& b = std::get<std::string>(rhs);
            const auto cmp = a <=> b;
            return cmp < 0 ? -1 : (cmp > 0 ? 1 : 0);
        }
        case FieldType::Bool: {
            const auto a = std::get<bool>(lhs) ? 1 : 0;
            const auto b = std::get<bool>(rhs) ? 1 : 0;
            return a < b ? -1 : (a > b ? 1 : 0);
        }
        case FieldType::Int64: {
            const auto a = std::get<int64_t>(lhs);
            const auto b = std::get<int64_t>(rhs);
            return a < b ? -1 : (a > b ? 1 : 0);
        }
        case FieldType::Float64: {
            const auto a = std::get<double>(lhs);
            const auto b = std::get<double>(rhs);
            return a < b ? -1 : (a > b ? 1 : 0);
        }
        case FieldType::TimestampUsUtc: {
            const auto a = std::get<TimestampUs>(lhs).value;
            const auto b = std::get<TimestampUs>(rhs).value;
            return a < b ? -1 : (a > b ? 1 : 0);
        }
    }
    return 0;
}

bool ValidateColumnZoneStats(const FieldSpec& spec, int64_t rowCount, const TColumnZoneStats& stats) noexcept
{
    if (stats.NullCount.has_value()) {
        if (*stats.NullCount < 0 || *stats.NullCount > rowCount) {
            return false;
        }
        if (!spec.nullable && *stats.NullCount != 0) {
            return false;
        }
        if (*stats.NullCount == rowCount && (stats.LowerBound.has_value() || stats.UpperBound.has_value())) {
            return false;
        }
    }
    if (rowCount == 0 && (stats.LowerBound.has_value() || stats.UpperBound.has_value())) {
        return false;
    }
    if (stats.LowerBound.has_value() && stats.UpperBound.has_value()) {
        if (CompareTyped(spec.type, *stats.LowerBound, *stats.UpperBound) > 0) {
            return false;
        }
    }
    return true;
}

bool MatchesParquetColumnOrder(FieldType type, const parquet::SchemaDescriptor* schema, int columnIndex) noexcept
{
    if (schema == nullptr || columnIndex < 0 || columnIndex >= schema->num_columns()) {
        return false;
    }
    const auto* descr = schema->Column(columnIndex);
    if (descr == nullptr) {
        return false;
    }
    switch (type) {
        case FieldType::String:
            return descr->physical_type() == parquet::Type::BYTE_ARRAY &&
                descr->sort_order() == parquet::SortOrder::UNSIGNED;
        case FieldType::Bool:
            return descr->physical_type() == parquet::Type::BOOLEAN;
        case FieldType::Int64:
        case FieldType::TimestampUsUtc:
            return descr->physical_type() == parquet::Type::INT64 && descr->sort_order() == parquet::SortOrder::SIGNED;
        case FieldType::Float64:
            return descr->physical_type() == parquet::Type::DOUBLE && descr->sort_order() == parquet::SortOrder::SIGNED;
    }
    return false;
}

ETruthValue AndTruth(ETruthValue lhs, ETruthValue rhs) noexcept
{
    if (lhs == ETruthValue::False || rhs == ETruthValue::False) {
        return ETruthValue::False;
    }
    if (lhs == ETruthValue::True && rhs == ETruthValue::True) {
        return ETruthValue::True;
    }
    return ETruthValue::Unknown;
}

ETruthValue OrTruth(ETruthValue lhs, ETruthValue rhs) noexcept
{
    if (lhs == ETruthValue::True || rhs == ETruthValue::True) {
        return ETruthValue::True;
    }
    if (lhs == ETruthValue::False && rhs == ETruthValue::False) {
        return ETruthValue::False;
    }
    return ETruthValue::Unknown;
}

bool HasTruth(const TZoneTruth& zone, ETruthValue truth) noexcept
{
    switch (truth) {
        case ETruthValue::False:
            return zone.CanBeFalse;
        case ETruthValue::Unknown:
            return zone.CanBeUnknown;
        case ETruthValue::True:
            return zone.CanBeTrue;
    }
    return false;
}

void AddTruth(TZoneTruth& zone, ETruthValue truth) noexcept
{
    switch (truth) {
        case ETruthValue::False:
            zone.CanBeFalse = true;
            break;
        case ETruthValue::Unknown:
            zone.CanBeUnknown = true;
            break;
        case ETruthValue::True:
            zone.CanBeTrue = true;
            break;
    }
}

template <typename TCombine>
TZoneTruth CombineBinaryZone(const TZoneTruth& lhs, const TZoneTruth& rhs, TCombine&& combine) noexcept
{
    constexpr std::array<ETruthValue, 3> AllTruths{
        ETruthValue::False,
        ETruthValue::Unknown,
        ETruthValue::True,
    };
    TZoneTruth out{.CanBeTrue = false, .CanBeFalse = false, .CanBeUnknown = false};
    for (const auto a : AllTruths) {
        if (!HasTruth(lhs, a)) {
            continue;
        }
        for (const auto b : AllTruths) {
            if (HasTruth(rhs, b)) {
                AddTruth(out, combine(a, b));
            }
        }
    }
    return out;
}

TZoneTruth EvalCompareZone(const TBoundOp& op, const TColumnZoneStats& stats, int64_t rowCount)
{
    const bool canHaveNull = stats.Nullable && (!stats.NullCount.has_value() || *stats.NullCount > 0);
    const bool canHaveNonNull = !stats.NullCount.has_value() || *stats.NullCount < rowCount;
    TZoneTruth out{.CanBeTrue = false, .CanBeFalse = false, .CanBeUnknown = canHaveNull};
    if (!canHaveNonNull) {
        return out;
    }

    const auto& lower = stats.LowerBound;
    const auto& upper = stats.UpperBound;
    const auto& value = op.Value;
    const bool singleExactPoint = lower.has_value() && upper.has_value() && stats.LowerExact && stats.UpperExact &&
        CompareTyped(op.Type, *lower, *upper) == 0;
    const bool valueInRange = (!lower.has_value() || CompareTyped(op.Type, value, *lower) >= 0) &&
        (!upper.has_value() || CompareTyped(op.Type, value, *upper) <= 0);

    switch (op.Op) {
        case CompareOp::Eq:
            if (valueInRange) {
                out.CanBeTrue = true;
            }
            if (!(singleExactPoint && CompareTyped(op.Type, *lower, value) == 0)) {
                out.CanBeFalse = true;
            }
            break;
        case CompareOp::Ne:
            if (!(singleExactPoint && CompareTyped(op.Type, *lower, value) == 0)) {
                out.CanBeTrue = true;
            }
            if (valueInRange) {
                out.CanBeFalse = true;
            }
            break;
        case CompareOp::Lt:
            if (!lower.has_value() || CompareTyped(op.Type, *lower, value) < 0) {
                out.CanBeTrue = true;
            }
            if (!upper.has_value() || CompareTyped(op.Type, *upper, value) >= 0) {
                out.CanBeFalse = true;
            }
            break;
        case CompareOp::Le:
            if (!lower.has_value() || CompareTyped(op.Type, *lower, value) <= 0) {
                out.CanBeTrue = true;
            }
            if (!upper.has_value() || CompareTyped(op.Type, *upper, value) > 0) {
                out.CanBeFalse = true;
            }
            break;
        case CompareOp::Gt:
            if (!upper.has_value() || CompareTyped(op.Type, *upper, value) > 0) {
                out.CanBeTrue = true;
            }
            if (!lower.has_value() || CompareTyped(op.Type, *lower, value) <= 0) {
                out.CanBeFalse = true;
            }
            break;
        case CompareOp::Ge:
            if (!upper.has_value() || CompareTyped(op.Type, *upper, value) >= 0) {
                out.CanBeTrue = true;
            }
            if (!lower.has_value() || CompareTyped(op.Type, *lower, value) < 0) {
                out.CanBeFalse = true;
            }
            break;
    }
    return out;
}

template <typename TElement, typename TBoundValue = TElement>
void EvalSortedInRange(std::span<const TElement> sortedValues,
    const std::optional<TBoundValue>& lower,
    const std::optional<TBoundValue>& upper,
    bool singleExactPoint,
    bool exhaustiveTwoValueDomain,
    bool& canMatchOut,
    bool& canMissOut)
{
    if (sortedValues.empty()) {
        canMatchOut = false;
        canMissOut = true;
        return;
    }
    const auto it =
        lower.has_value() ? std::ranges::lower_bound(sortedValues, *lower, std::less<>{}) : sortedValues.begin();
    canMatchOut = it != sortedValues.end() && (!upper.has_value() || !std::less<>{}(*upper, *it));
    if (exhaustiveTwoValueDomain) {
        canMissOut = false;
        return;
    }
    if (singleExactPoint && lower.has_value() && std::ranges::binary_search(sortedValues, *lower, std::less<>{})) {
        canMissOut = false;
        return;
    }
    canMissOut = true;
}

TZoneTruth EvalInZone(const TBoundOp& op, const TBoundInSet& set, const TColumnZoneStats& stats, int64_t rowCount)
{
    const bool canHaveNull = stats.Nullable && (!stats.NullCount.has_value() || *stats.NullCount > 0);
    const bool canHaveNonNull = !stats.NullCount.has_value() || *stats.NullCount < rowCount;
    TZoneTruth out{.CanBeTrue = false, .CanBeFalse = false, .CanBeUnknown = canHaveNull};
    if (!canHaveNonNull) {
        return out;
    }

    const bool singleExactPoint = stats.LowerBound.has_value() && stats.UpperBound.has_value() && stats.LowerExact &&
        stats.UpperExact && CompareTyped(op.Type, *stats.LowerBound, *stats.UpperBound) == 0;

    bool canMatch = false;
    bool canMiss = true;
    switch (op.Type) {
        case FieldType::String: {
            const auto lower = stats.LowerBound
                ? std::optional<std::string_view>(std::get<std::string>(*stats.LowerBound))
                : std::optional<std::string_view>{};
            const auto upper = stats.UpperBound
                ? std::optional<std::string_view>(std::get<std::string>(*stats.UpperBound))
                : std::optional<std::string_view>{};
            EvalSortedInRange<std::string, std::string_view>(set.Strings,
                lower,
                upper,
                singleExactPoint,
                false,
                canMatch,
                canMiss);
            break;
        }
        case FieldType::Bool: {
            const auto lower = stats.LowerBound
                ? std::optional<uint8_t>(std::get<bool>(*stats.LowerBound) ? uint8_t{1} : uint8_t{0})
                : std::optional<uint8_t>{};
            const auto upper = stats.UpperBound
                ? std::optional<uint8_t>(std::get<bool>(*stats.UpperBound) ? uint8_t{1} : uint8_t{0})
                : std::optional<uint8_t>{};
            EvalSortedInRange<uint8_t>(set.Bools,
                lower,
                upper,
                singleExactPoint,
                set.Bools.size() == 2,
                canMatch,
                canMiss);
            break;
        }
        case FieldType::Int64: {
            const auto lower =
                stats.LowerBound ? std::optional(std::get<int64_t>(*stats.LowerBound)) : std::optional<int64_t>{};
            const auto upper =
                stats.UpperBound ? std::optional(std::get<int64_t>(*stats.UpperBound)) : std::optional<int64_t>{};
            EvalSortedInRange<int64_t>(set.Ints, lower, upper, singleExactPoint, false, canMatch, canMiss);
            break;
        }
        case FieldType::Float64: {
            const auto lower =
                stats.LowerBound ? std::optional(std::get<double>(*stats.LowerBound)) : std::optional<double>{};
            const auto upper =
                stats.UpperBound ? std::optional(std::get<double>(*stats.UpperBound)) : std::optional<double>{};
            EvalSortedInRange<double>(set.Doubles, lower, upper, singleExactPoint, false, canMatch, canMiss);
            break;
        }
        case FieldType::TimestampUsUtc: {
            const auto lower = stats.LowerBound ? std::optional(std::get<TimestampUs>(*stats.LowerBound).value)
                                                : std::optional<int64_t>{};
            const auto upper = stats.UpperBound ? std::optional(std::get<TimestampUs>(*stats.UpperBound).value)
                                                : std::optional<int64_t>{};
            EvalSortedInRange<int64_t>(set.Ints, lower, upper, singleExactPoint, false, canMatch, canMiss);
            break;
        }
    }

    if (canMatch) {
        out.CanBeTrue = true;
    }
    if (canMiss) {
        if (set.HasNull) {
            out.CanBeUnknown = true;
        } else {
            out.CanBeFalse = true;
        }
    }
    return out;
}

const TColumnZoneStats* FindZoneStats(size_t fieldIndex,
    std::span<const size_t> readFields,
    std::span<const TColumnZoneStats> readStats) noexcept
{
    const auto it = std::ranges::lower_bound(readFields, fieldIndex);
    if (it == readFields.end() || *it != fieldIndex) {
        return nullptr;
    }
    return &readStats[static_cast<size_t>(it - readFields.begin())];
}

} // namespace

Result<std::vector<TColumnZoneStats>> ExtractFileZoneStats(const iceberg::DataFile& dataFile,
    const iceberg::Schema& icebergSchema,
    const SchemaSpec& schema,
    std::span<const size_t> readFields)
{
    try {
        for (const auto fieldIndex : readFields) {
            if (fieldIndex >= schema.fields.size()) {
                return std::unexpected(InternalError("read field index is out of range"));
            }
        }
        std::vector<TColumnZoneStats> out;
        out.reserve(readFields.size());
        const auto icebergFields = icebergSchema.fields();
        if (dataFile.record_count <= 0 || icebergFields.size() != schema.fields.size()) {
            for (const auto fieldIndex : readFields) {
                out.push_back(MakeMalformedZoneStats(schema.fields[fieldIndex]));
            }
            return out;
        }
        for (const auto fieldIndex : readFields) {
            const auto& spec = schema.fields[fieldIndex];
            const auto fieldID = icebergFields[fieldIndex].field_id();
            TColumnZoneStats stats{
                .Type = spec.type,
                .Nullable = spec.nullable,
                // Iceberg string bounds may use truncate(N) outer prefixes unless configured full.
                .LowerExact = spec.type != FieldType::String,
                .UpperExact = spec.type != FieldType::String,
            };

            bool malformed = false;
            if (const auto it = dataFile.value_counts.find(fieldID); it != dataFile.value_counts.end()) {
                if (it->second != dataFile.record_count) {
                    malformed = true;
                } else {
                    stats.HasStats = true;
                }
            }
            if (const auto it = dataFile.null_value_counts.find(fieldID); it != dataFile.null_value_counts.end()) {
                stats.NullCount = it->second;
                stats.HasStats = true;
            }
            bool floatsWithoutNaN = false;
            if (const auto it = dataFile.nan_value_counts.find(fieldID); it != dataFile.nan_value_counts.end()) {
                if (it->second != 0) {
                    malformed = true;
                } else {
                    floatsWithoutNaN = true;
                }
            }
            if (const auto it = dataFile.lower_bounds.find(fieldID); it != dataFile.lower_bounds.end()) {
                auto decoded = DecodeBoundOpt(spec, it->second);
                if (!decoded) {
                    malformed = true;
                } else {
                    stats.LowerBound = std::move(*decoded);
                    stats.HasStats = true;
                }
            }
            if (const auto it = dataFile.upper_bounds.find(fieldID); it != dataFile.upper_bounds.end()) {
                auto decoded = DecodeBoundOpt(spec, it->second);
                if (!decoded) {
                    malformed = true;
                } else {
                    stats.UpperBound = std::move(*decoded);
                    stats.HasStats = true;
                }
            }

            if (malformed || !ValidateColumnZoneStats(spec, dataFile.record_count, stats)) {
                out.push_back(MakeMalformedZoneStats(spec));
                continue;
            }
            if (spec.type == FieldType::Float64 && !floatsWithoutNaN) {
                stats.LowerBound.reset();
                stats.UpperBound.reset();
            }
            // Non-key strings use write.metadata.metrics.default = truncate(16). Drop upper bounds
            // with >= 16 UTF-8 code points in case a writer truncated the prefix without incrementing;
            // exact Parquet row-group bounds still prune after opening the file.
            if (spec.type == FieldType::String && spec.name != "key" && stats.UpperBound.has_value() &&
                Utf8CodePoints(std::get<std::string>(*stats.UpperBound)) >= DefaultIcebergStringTruncateCodePoints)
            {
                stats.UpperBound.reset();
            }
            out.push_back(std::move(stats));
        }
        return out;
    } catch (const std::bad_alloc& error) {
        return std::unexpected(ResourceExhausted(error.what()));
    } catch (const std::exception& error) {
        return std::unexpected(InternalError(error.what()));
    }
}

Result<std::vector<TColumnZoneStats>> ExtractRowGroupZoneStats(const parquet::RowGroupMetaData& group,
    const SchemaSpec& schema,
    std::span<const size_t> readFields,
    const iceberg::DataFile* dataFile,
    const iceberg::Schema* icebergSchema)
{
    try {
        for (const auto fieldIndex : readFields) {
            if (fieldIndex >= schema.fields.size()) {
                return std::unexpected(InternalError("read field index is out of range"));
            }
        }
        std::vector<TColumnZoneStats> out;
        out.reserve(readFields.size());
        const auto rowCount = group.num_rows();
        if (rowCount < 0 || group.num_columns() != static_cast<int>(schema.fields.size())) {
            for (const auto fieldIndex : readFields) {
                out.push_back(MakeMalformedZoneStats(schema.fields[fieldIndex]));
            }
            return out;
        }
        for (const auto fieldIndex : readFields) {
            const auto& spec = schema.fields[fieldIndex];
            try {
                if (!MatchesParquetColumnOrder(spec.type, group.schema(), static_cast<int>(fieldIndex))) {
                    out.push_back(MakeMalformedZoneStats(spec));
                    continue;
                }
                const auto chunk = group.ColumnChunk(static_cast<int>(fieldIndex));
                if (!chunk || chunk->num_values() != rowCount) {
                    out.push_back(MakeMalformedZoneStats(spec));
                    continue;
                }

                bool malformed = false;
                bool floatsWithoutNaN = false;
                if (spec.type == FieldType::Float64 && dataFile != nullptr && icebergSchema != nullptr) {
                    const auto icebergFields = icebergSchema->fields();
                    if (fieldIndex < icebergFields.size()) {
                        const auto fieldID = icebergFields[fieldIndex].field_id();
                        if (const auto nanIt = dataFile->nan_value_counts.find(fieldID);
                            nanIt != dataFile->nan_value_counts.end())
                        {
                            if (nanIt->second != 0) {
                                malformed = true;
                            } else {
                                floatsWithoutNaN = true;
                            }
                        }
                    }
                }

                TColumnZoneStats stats{
                    .Type = spec.type,
                    .Nullable = spec.nullable,
                    .LowerExact = true,
                    .UpperExact = true,
                };

                if (const auto encoded = chunk->encoded_statistics()) {
                    if (encoded->has_null_count) {
                        stats.NullCount = encoded->null_count;
                        stats.HasStats = true;
                    }
                    if (encoded->all_null_value) {
                        if (encoded->has_null_count && encoded->null_count != rowCount) {
                            malformed = true;
                        } else {
                            stats.NullCount = rowCount;
                            stats.HasStats = true;
                        }
                    }
                    if (encoded->has_min) {
                        const std::span<const uint8_t> minBytes(reinterpret_cast<const uint8_t*>(encoded->min().data()),
                            encoded->min().size());
                        auto decoded = DecodeBoundOpt(spec, minBytes);
                        if (!decoded) {
                            malformed = true;
                        } else {
                            stats.LowerBound = std::move(*decoded);
                            stats.LowerExact = !encoded->is_min_value_exact.has_value() || *encoded->is_min_value_exact;
                            stats.HasStats = true;
                        }
                    }
                    if (encoded->has_max) {
                        const std::span<const uint8_t> maxBytes(reinterpret_cast<const uint8_t*>(encoded->max().data()),
                            encoded->max().size());
                        auto decoded = DecodeBoundOpt(spec, maxBytes);
                        if (!decoded) {
                            malformed = true;
                        } else {
                            stats.UpperBound = std::move(*decoded);
                            stats.UpperExact = !encoded->is_max_value_exact.has_value() || *encoded->is_max_value_exact;
                            stats.HasStats = true;
                        }
                    }
                }

                if (malformed || !ValidateColumnZoneStats(spec, rowCount, stats)) {
                    out.push_back(MakeMalformedZoneStats(spec));
                    continue;
                }
                if (!stats.LowerExact) {
                    stats.LowerBound.reset();
                }
                if (!stats.UpperExact) {
                    stats.UpperBound.reset();
                }
                if (spec.type == FieldType::Float64 && !floatsWithoutNaN) {
                    stats.LowerBound.reset();
                    stats.UpperBound.reset();
                }
                out.push_back(std::move(stats));
            } catch (const std::bad_alloc&) {
                throw;
            } catch (const std::exception&) {
                out.push_back(MakeMalformedZoneStats(spec));
            }
        }
        return out;
    } catch (const std::bad_alloc& error) {
        return std::unexpected(ResourceExhausted(error.what()));
    } catch (const std::exception& error) {
        return std::unexpected(InternalError(error.what()));
    }
}

Result<TZoneTruth> EvaluateZonePredicate(const TBoundPredicate& predicate,
    int64_t rowCount,
    std::span<const size_t> readFields,
    std::span<const TColumnZoneStats> readStats)
{
    try {
        if (readFields.size() != readStats.size() || !std::ranges::is_sorted(readFields) || predicate.Ops.empty()) {
            return std::unexpected(InternalError("invalid zone predicate inputs"));
        }
        if (rowCount <= 0) {
            return TZoneTruth{.CanBeTrue = false, .CanBeFalse = false, .CanBeUnknown = false};
        }

        std::vector<TZoneTruth> stack;
        stack.reserve(std::min(predicate.Ops.size(), MaxPredicateDepth + 2));

        for (const auto& op : predicate.Ops) {
            switch (op.Kind) {
                case EBoundOpKind::PushTrue:
                    stack.push_back({.CanBeTrue = true, .CanBeFalse = false, .CanBeUnknown = false});
                    break;
                case EBoundOpKind::PushCompareNull: {
                    const auto* stats = FindZoneStats(op.FieldIndex, readFields, readStats);
                    if (stats == nullptr || stats->Type != op.Type) {
                        return std::unexpected(InternalError("missing zone statistics for predicate field"));
                    }
                    stack.push_back({.CanBeTrue = false, .CanBeFalse = false, .CanBeUnknown = true});
                    break;
                }
                case EBoundOpKind::PushIsNull:
                case EBoundOpKind::PushIsNotNull: {
                    const auto* stats = FindZoneStats(op.FieldIndex, readFields, readStats);
                    if (stats == nullptr || stats->Type != op.Type) {
                        return std::unexpected(InternalError("missing zone statistics for predicate field"));
                    }
                    const bool canHaveNull =
                        stats->Nullable && (!stats->NullCount.has_value() || *stats->NullCount > 0);
                    const bool canHaveNonNull = !stats->NullCount.has_value() || *stats->NullCount < rowCount;
                    if (op.Kind == EBoundOpKind::PushIsNull) {
                        stack.push_back(
                            {.CanBeTrue = canHaveNull, .CanBeFalse = canHaveNonNull, .CanBeUnknown = false});
                    } else {
                        stack.push_back(
                            {.CanBeTrue = canHaveNonNull, .CanBeFalse = canHaveNull, .CanBeUnknown = false});
                    }
                    break;
                }
                case EBoundOpKind::PushCompare: {
                    const auto* stats = FindZoneStats(op.FieldIndex, readFields, readStats);
                    if (stats == nullptr || stats->Type != op.Type) {
                        return std::unexpected(InternalError("missing zone statistics for predicate field"));
                    }
                    stack.push_back(EvalCompareZone(op, *stats, rowCount));
                    break;
                }
                case EBoundOpKind::PushIn: {
                    const auto* stats = FindZoneStats(op.FieldIndex, readFields, readStats);
                    if (stats == nullptr || stats->Type != op.Type || op.InSetIndex >= predicate.InSets.size()) {
                        return std::unexpected(InternalError("missing zone statistics or IN set for predicate field"));
                    }
                    stack.push_back(EvalInZone(op, predicate.InSets[op.InSetIndex], *stats, rowCount));
                    break;
                }
                case EBoundOpKind::ApplyNot: {
                    if (stack.empty()) {
                        return std::unexpected(InternalError("zone predicate stack underflow on NOT"));
                    }
                    const auto child = stack.back();
                    stack.back() = {
                        .CanBeTrue = child.CanBeFalse,
                        .CanBeFalse = child.CanBeTrue,
                        .CanBeUnknown = child.CanBeUnknown,
                    };
                    break;
                }
                case EBoundOpKind::FoldAnd:
                case EBoundOpKind::FoldOr: {
                    if (stack.size() < 2) {
                        return std::unexpected(InternalError("zone predicate stack underflow on binary operator"));
                    }
                    const auto rhs = stack.back();
                    stack.pop_back();
                    const auto lhs = stack.back();
                    stack.back() = op.Kind == EBoundOpKind::FoldAnd ? CombineBinaryZone(lhs, rhs, AndTruth)
                                                                    : CombineBinaryZone(lhs, rhs, OrTruth);
                    break;
                }
            }
        }

        if (stack.size() != 1) {
            return std::unexpected(InternalError("zone predicate stack did not reduce to one value"));
        }
        return stack.front();
    } catch (const std::bad_alloc& error) {
        return std::unexpected(ResourceExhausted(error.what()));
    } catch (const std::exception& error) {
        return std::unexpected(InternalError(error.what()));
    }
}

Result<bool> CanPruneZone(const TBoundPredicate& predicate,
    int64_t rowCount,
    std::span<const size_t> readFields,
    std::span<const TColumnZoneStats> readStats)
{
    if (readFields.size() != readStats.size() || !std::ranges::is_sorted(readFields) || predicate.Ops.empty()) {
        return std::unexpected(InternalError("invalid zone predicate inputs"));
    }
    if (predicate.IsAlwaysTrue()) {
        return false;
    }
    if (rowCount <= 0) {
        return true;
    }
    for (const auto fieldIndex : predicate.ReferencedFields) {
        const auto* stats = FindZoneStats(fieldIndex, readFields, readStats);
        if (stats == nullptr) {
            return std::unexpected(InternalError("missing zone statistics for referenced predicate field"));
        }
        if (!stats->HasStats) {
            return false;
        }
    }
    auto truth = EvaluateZonePredicate(predicate, rowCount, readFields, readStats);
    if (!truth) {
        return std::unexpected(std::move(truth.error()));
    }
    return !truth->CanBeTrue;
}

} // namespace glamdb::detail
