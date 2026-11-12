#include "core/engine/storage_mapping.h"
#include "core/scan/predicate.h"

#include <gtest/gtest.h>

#if GLAMDB_TEST_STORAGE

#include "core/scan/pruning.h"
#include "core/storage/table_profile.h"

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/schema.h>
#include <parquet/arrow/writer.h>
#include <parquet/file_reader.h>
#include <parquet/metadata.h>
#include <parquet/properties.h>
#include <parquet/statistics.h>

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstring>
#include <functional>
#include <limits>
#include <map>
#include <numeric>
#include <optional>
#include <string>
#include <vector>

namespace glamdb::detail {
namespace {

const SchemaSpec FullSchema{{
    {"key", FieldType::String, false},
    {"label", FieldType::String, true},
    {"flag", FieldType::Bool, true},
    {"count", FieldType::Int64, true},
    {"score", FieldType::Float64, true},
    {"at", FieldType::TimestampUsUtc, true},
}};

std::vector<uint8_t> LeBytes64(uint64_t value)
{
    if constexpr (std::endian::native == std::endian::big) {
        value = std::byteswap(value);
    }
    std::vector<uint8_t> out(sizeof(value));
    std::memcpy(out.data(), &value, sizeof(value));
    return out;
}

std::vector<uint8_t> EncodeInt64(int64_t value)
{
    uint64_t raw = 0;
    std::memcpy(&raw, &value, sizeof(raw));
    return LeBytes64(raw);
}

std::vector<uint8_t> EncodeDouble(double value)
{
    uint64_t raw = 0;
    std::memcpy(&raw, &value, sizeof(raw));
    return LeBytes64(raw);
}

std::vector<uint8_t> EncodeString(std::string_view value)
{
    return {reinterpret_cast<const uint8_t*>(value.data()),
        reinterpret_cast<const uint8_t*>(value.data()) + value.size()};
}

int CompareSameType(FieldType type, const Literal& lhs, const Literal& rhs)
{
    switch (type) {
        case FieldType::String: {
            const auto cmp = std::get<std::string>(lhs) <=> std::get<std::string>(rhs);
            return cmp < 0 ? -1 : (cmp > 0 ? 1 : 0);
        }
        case FieldType::Bool: {
            const int a = std::get<bool>(lhs) ? 1 : 0;
            const int b = std::get<bool>(rhs) ? 1 : 0;
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

Literal CellToLiteral(const Cell& cell)
{
    return std::visit(
        [](const auto& value) -> Literal {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, std::monostate>) {
                return Literal{int64_t{0}};
            } else {
                return Literal{value};
            }
        },
        cell);
}

std::vector<TColumnZoneStats> ExactStatsFromRows(const SchemaSpec& schema, const std::vector<Row>& rows)
{
    std::vector<TColumnZoneStats> out;
    out.reserve(schema.fields.size());
    for (size_t col = 0; col < schema.fields.size(); ++col) {
        const auto& spec = schema.fields[col];
        TColumnZoneStats stats{
            .Type = spec.type,
            .Nullable = spec.nullable,
            .HasStats = true,
            .NullCount = 0,
            .LowerExact = true,
            .UpperExact = true,
        };
        int64_t nulls = 0;
        for (const auto& row : rows) {
            const auto& cell = row[col];
            if (std::holds_alternative<std::monostate>(cell)) {
                ++nulls;
                continue;
            }
            auto lit = CellToLiteral(cell);
            if (spec.type == FieldType::Float64 && std::get<double>(lit) == 0.0) {
                lit = Literal{0.0};
            }
            if (!stats.LowerBound || CompareSameType(spec.type, lit, *stats.LowerBound) < 0) {
                stats.LowerBound = lit;
            }
            if (!stats.UpperBound || CompareSameType(spec.type, lit, *stats.UpperBound) > 0) {
                stats.UpperBound = lit;
            }
        }
        stats.NullCount = nulls;
        out.push_back(std::move(stats));
    }
    return out;
}

void VerifySoundnessAgainstRowOracle(const SchemaSpec& schema,
    const std::vector<Row>& rows,
    const std::vector<TColumnZoneStats>& stats,
    const Predicate& predicate)
{
    const auto batchRes = MakeRecordBatch(schema, rows);
    ASSERT_TRUE(batchRes) << batchRes.error().message;
    const auto& batch = *batchRes;

    const auto boundTrue = BindPredicate(predicate, schema);
    ASSERT_TRUE(boundTrue) << boundTrue.error().message;
    const auto selectedTrue = SelectRows(*boundTrue, *batch);
    ASSERT_TRUE(selectedTrue) << selectedTrue.error().message;

    const auto boundFalse = BindPredicate(Predicate::Not(predicate), schema);
    ASSERT_TRUE(boundFalse) << boundFalse.error().message;
    const auto selectedFalse = SelectRows(*boundFalse, *batch);
    ASSERT_TRUE(selectedFalse) << selectedFalse.error().message;

    std::vector<size_t> allFields(schema.fields.size());
    std::iota(allFields.begin(), allFields.end(), size_t{0});

    const auto zone = EvaluateZonePredicate(*boundTrue, static_cast<int64_t>(rows.size()), allFields, stats);
    ASSERT_TRUE(zone) << zone.error().message;

    const int64_t trueCount = static_cast<int64_t>(selectedTrue->size());
    const int64_t falseCount = static_cast<int64_t>(selectedFalse->size());
    const int64_t unknownCount = static_cast<int64_t>(rows.size()) - trueCount - falseCount;
    ASSERT_GE(unknownCount, 0);

    if (trueCount > 0) {
        EXPECT_TRUE(zone->CanBeTrue);
    }
    if (falseCount > 0) {
        EXPECT_TRUE(zone->CanBeFalse);
    }
    if (unknownCount > 0) {
        EXPECT_TRUE(zone->CanBeUnknown);
    }

    const auto prunable = CanPruneZone(*boundTrue, static_cast<int64_t>(rows.size()), allFields, stats);
    ASSERT_TRUE(prunable) << prunable.error().message;
    if (trueCount > 0) {
        EXPECT_FALSE(*prunable);
    }
}

TEST(ScanPruningUnitTest, ThreeValuedZoneTruthMatchesRowOracleAcrossZonesAndRelaxations)
{
    const std::vector<std::vector<Row>> zones{
        // Zone 0: mixed values + NULLs across all nullable columns.
        {
            {std::string("alpha"), std::string("abcdefghijklmnop_1"), true, int64_t{10}, 0.0, TimestampUs{100}},
            {std::string("beta"),
                std::monostate{},
                std::monostate{},
                std::monostate{},
                std::monostate{},
                std::monostate{}},
            {std::string("omega"), std::string("abcdefghijklmnop_9"), false, int64_t{20}, 2.5, TimestampUs{300}},
        },
        // Zone 1: all nullable columns are NULL.
        {
            {std::string("k1"),
                std::monostate{},
                std::monostate{},
                std::monostate{},
                std::monostate{},
                std::monostate{}},
            {std::string("k2"),
                std::monostate{},
                std::monostate{},
                std::monostate{},
                std::monostate{},
                std::monostate{}},
        },
        // Zone 2: single constant value per column (no NULLs).
        {
            {std::string("same"), std::string("const-label-0123456789"), true, int64_t{42}, -3.5, TimestampUs{500}},
            {std::string("same"), std::string("const-label-0123456789"), true, int64_t{42}, -3.5, TimestampUs{500}},
        },
        // Zone 3: non-null extremes (both booleans, negative/positive floats & timestamps).
        {
            {std::string("A"), std::string("aaa"), false, int64_t{-100}, -10.0, TimestampUs{-62135596800000000LL}},
            {std::string("Z"), std::string("zzz"), true, int64_t{100}, 0.0, TimestampUs{253402300799999999LL}},
        },
    };

    const std::vector<Predicate> leafPredicates{
        Predicate::True(),
        Predicate::Compare("key", CompareOp::Eq, std::string("alpha")),
        Predicate::Compare("key", CompareOp::Eq, std::string("same")),
        Predicate::Compare("key", CompareOp::Eq, std::string("000")),
        Predicate::Compare("key", CompareOp::Ne, std::string("same")),
        Predicate::Compare("key", CompareOp::Lt, std::string("alpha")),
        Predicate::Compare("key", CompareOp::Le, std::string("alpha")),
        Predicate::Compare("key", CompareOp::Gt, std::string("omega")),
        Predicate::Compare("key", CompareOp::Ge, std::string("omega")),
        Predicate::Compare("label", CompareOp::Eq, std::string("const-label-0123456789")),
        Predicate::Compare("label", CompareOp::Ne, std::string("const-label-0123456789")),
        Predicate::Compare("label", CompareOp::Gt, std::string("abcdefghijklmnop_9")),
        Predicate::Compare("flag", CompareOp::Eq, true),
        Predicate::Compare("flag", CompareOp::Ne, true),
        Predicate::Compare("count", CompareOp::Eq, int64_t{42}),
        Predicate::Compare("count", CompareOp::Ne, int64_t{42}),
        Predicate::Compare("count", CompareOp::Lt, int64_t{10}),
        Predicate::Compare("count", CompareOp::Le, int64_t{10}),
        Predicate::Compare("count", CompareOp::Gt, int64_t{20}),
        Predicate::Compare("count", CompareOp::Ge, int64_t{20}),
        Predicate::Compare("score", CompareOp::Eq, -0.0),
        Predicate::Compare("score", CompareOp::Le, 0.0),
        Predicate::Compare("score", CompareOp::Gt, 0.0),
        Predicate::Compare("at", CompareOp::Eq, TimestampUs{100}),
        Predicate::Compare("at", CompareOp::Lt, TimestampUs{100}),
        Predicate::Compare("at", CompareOp::Gt, TimestampUs{500}),
        Predicate::CompareNull("count", CompareOp::Eq),
        Predicate::CompareNull("key", CompareOp::Ne),
        Predicate::IsNull("count"),
        Predicate::IsNotNull("count"),
        Predicate::IsNull("key"),
        Predicate::IsNotNull("key"),
        Predicate::In("count", {}),
        Predicate::In("count", {std::nullopt}),
        Predicate::In("count", {int64_t{1}, int64_t{5}}),
        Predicate::In("count", {int64_t{15}}),
        Predicate::In("count", {int64_t{42}}),
        Predicate::In("count", {int64_t{42}, std::nullopt}),
        Predicate::In("count", {int64_t{99}, std::nullopt}),
        Predicate::In("flag", {false, true}),
        Predicate::In("flag", {true, std::nullopt}),
        Predicate::In("score", {-0.0, 5.0}),
        Predicate::In("label", {std::string("const-label-0123456789")}),
    };

    std::vector<Predicate> allPredicates = leafPredicates;
    for (const auto& leaf : leafPredicates) {
        allPredicates.push_back(Predicate::Not(leaf));
    }
    for (size_t i = 0; i + 1 < leafPredicates.size(); i += 3) {
        const auto& a = leafPredicates[i];
        const auto& b = leafPredicates[i + 1];
        allPredicates.push_back(Predicate::And({a, b}));
        allPredicates.push_back(Predicate::Or({a, b}));
        allPredicates.push_back(Predicate::Not(Predicate::And({a, Predicate::Not(b)})));
        allPredicates.push_back(Predicate::Not(Predicate::Or({Predicate::Not(a), b})));
    }

    for (size_t z = 0; z < zones.size(); ++z) {
        SCOPED_TRACE(z);
        const auto exact = ExactStatsFromRows(FullSchema, zones[z]);

        // Variant 1: exact statistics.
        auto missingUpper = exact;
        for (auto& col : missingUpper) {
            col.UpperBound.reset();
        }

        // Variant 2: missing lower bounds and missing null counts.
        auto missingLowerAndNulls = exact;
        for (auto& col : missingLowerAndNulls) {
            col.LowerBound.reset();
            col.NullCount.reset();
        }

        // Variant 3: truncated UTF-8 outer string bounds (16-byte prefix / incremented upper).
        auto truncatedStrings = exact;
        for (auto& col : truncatedStrings) {
            if (col.Type == FieldType::String) {
                col.LowerExact = false;
                col.UpperExact = false;
                if (col.LowerBound) {
                    auto s = std::get<std::string>(*col.LowerBound);
                    if (s.size() > 16) {
                        s.resize(16);
                    }
                    col.LowerBound = Literal{s};
                }
                if (col.UpperBound) {
                    auto s = std::get<std::string>(*col.UpperBound);
                    if (s.size() > 16) {
                        s.resize(16);
                        ++s.back();
                    }
                    col.UpperBound = Literal{s};
                }
            }
        }

        const std::vector<TColumnZoneStats>* variants[]{
            &exact,
            &missingUpper,
            &missingLowerAndNulls,
            &truncatedStrings,
        };
        for (const auto* statsVariant : variants) {
            for (const auto& pred : allPredicates) {
                VerifySoundnessAgainstRowOracle(FullSchema, zones[z], *statsVariant, pred);
            }
        }
    }
}

TEST(ScanPruningUnitTest, PrunesProvenImpossibleZonesAndPreservesTruncatedNeSafety)
{
    std::vector<size_t> fields{0, 1, 3};
    std::vector<TColumnZoneStats> stats{
        // key: exact ["d", "m"], required
        {.Type = FieldType::String,
            .Nullable = false,
            .HasStats = true,
            .NullCount = 0,
            .LowerBound = Literal{std::string("d")},
            .UpperBound = Literal{std::string("m")},
            .LowerExact = true,
            .UpperExact = true},
        // label: truncated ["aaaaaaaaaaaaaaaa", "aaaaaaaaaaaaaaaa"], non-exact!
        {.Type = FieldType::String,
            .Nullable = true,
            .HasStats = true,
            .NullCount = 0,
            .LowerBound = Literal{std::string(16, 'a')},
            .UpperBound = Literal{std::string(16, 'a')},
            .LowerExact = false,
            .UpperExact = false},
        // count: exact [10, 20], 0 nulls
        {.Type = FieldType::Int64,
            .Nullable = true,
            .HasStats = true,
            .NullCount = 0,
            .LowerBound = Literal{int64_t{10}},
            .UpperBound = Literal{int64_t{20}},
            .LowerExact = true,
            .UpperExact = true},
    };

    const auto checkPruned = [&](const Predicate& pred, bool expectedPruned) {
        const auto bound = BindPredicate(pred, FullSchema);
        ASSERT_TRUE(bound) << bound.error().message;
        const auto pruned = CanPruneZone(*bound, 8, fields, stats);
        ASSERT_TRUE(pruned) << pruned.error().message;
        EXPECT_EQ(*pruned, expectedPruned);
    };

    checkPruned(Predicate::Compare("key", CompareOp::Lt, std::string("d")), true);
    checkPruned(Predicate::Compare("key", CompareOp::Le, std::string("d")), false);
    checkPruned(Predicate::Compare("key", CompareOp::Gt, std::string("m")), true);
    checkPruned(Predicate::Compare("key", CompareOp::Ge, std::string("m")), false);
    checkPruned(Predicate::IsNull("count"), true);
    checkPruned(Predicate::In("count", {int64_t{1}, int64_t{9}, int64_t{21}, int64_t{50}}), true);
    checkPruned(Predicate::In("count", {int64_t{1}, int64_t{15}, int64_t{50}}), false);

    // SQL: `count NOT IN (5, NULL)` is NEVER TRUE (5 != count -> UNKNOWN), so it CAN prune!
    checkPruned(Predicate::Not(Predicate::In("count", {int64_t{15}, std::nullopt})), true);

    // Non-exact truncated string bounds ["aaa...", "aaa..."] MUST NOT prune `label != "aaa..."`!
    checkPruned(Predicate::Compare("label", CompareOp::Ne, std::string(16, 'a')), false);
    // Range miss on truncated prefix ("bbb" > "aaa...") DOES safely prune `label == "bbb"`!
    checkPruned(Predicate::Compare("label", CompareOp::Eq, std::string("bbb")), true);

    // Partial valid statistics (`HasStats == true`, bounds omitted e.g. >4 KiB strings or missing nan_value_counts)
    // do not prune range predicates on that column, while still allowing valid sibling columns in `AND` to prune.
    auto partialStats = stats;
    partialStats[2].LowerBound.reset();
    partialStats[2].UpperBound.reset();
    {
        const auto boundRange = BindPredicate(Predicate::Compare("count", CompareOp::Gt, int64_t{100}), FullSchema);
        ASSERT_TRUE(boundRange);
        const auto rangePruned = CanPruneZone(*boundRange, 8, fields, partialStats);
        ASSERT_TRUE(rangePruned);
        EXPECT_FALSE(*rangePruned);

        const auto boundSiblingAnd = BindPredicate(Predicate::And({
                                                       Predicate::Compare("key", CompareOp::Lt, std::string("d")),
                                                       Predicate::Compare("count", CompareOp::Gt, int64_t{100}),
                                                   }),
            FullSchema);
        ASSERT_TRUE(boundSiblingAnd);
        const auto siblingAndPruned = CanPruneZone(*boundSiblingAnd, 8, fields, partialStats);
        ASSERT_TRUE(siblingAndPruned);
        EXPECT_TRUE(*siblingAndPruned);
    }

    // Missing or malformed statistics (`HasStats == false`) on a referenced predicate column disable
    // pruning for the entire zone (including `CompareNull` and `AND`), ensuring `ValidateColumn` runs.
    auto unstat = stats;
    unstat[2] = TColumnZoneStats{
        .Type = FieldType::Int64,
        .Nullable = true,
        .HasStats = false,
    };
    for (const auto& pred : {
             Predicate::Compare("count", CompareOp::Gt, int64_t{100}),
             Predicate::CompareNull("count", CompareOp::Eq),
             Predicate::IsNull("count"),
             Predicate::IsNotNull("count"),
             Predicate::And({
                 Predicate::Compare("key", CompareOp::Lt, std::string("d")),
                 Predicate::Compare("count", CompareOp::Gt, int64_t{100}),
             }),
         })
    {
        const auto bound = BindPredicate(pred, FullSchema);
        ASSERT_TRUE(bound);
        const auto unstatPruned = CanPruneZone(*bound, 8, fields, unstat);
        ASSERT_TRUE(unstatPruned);
        EXPECT_FALSE(*unstatPruned);
    }
}

TEST(ScanPruningUnitTest, DiscardsMalformedAndContradictoryManifestAndParquetStatisticsWithoutPruning)
{
    const auto profile = NGlamDB::NStorage::MakeTableProfile(ToStorageSchema(FullSchema));
    ASSERT_TRUE(profile.has_value());
    const auto& icebergSchema = *profile->Schema;

    iceberg::DataFile validFile;
    validFile.record_count = 4;
    validFile.value_counts = {{1, 4}, {2, 4}, {3, 4}, {4, 4}, {5, 4}, {6, 4}};
    validFile.null_value_counts = {{1, 0}, {2, 1}, {3, 0}, {4, 0}, {5, 0}, {6, 0}};
    validFile.nan_value_counts = {{5, 0}};
    validFile.lower_bounds[1] = EncodeString("c");
    validFile.upper_bounds[1] = EncodeString("x");
    validFile.lower_bounds[2] = EncodeString("lbl_c");
    validFile.upper_bounds[2] = EncodeString("lbl_x");
    validFile.lower_bounds[3] = {0};
    validFile.upper_bounds[3] = {1};
    validFile.lower_bounds[4] = EncodeInt64(10);
    validFile.upper_bounds[4] = EncodeInt64(20);
    validFile.lower_bounds[5] = EncodeDouble(0.0);
    validFile.upper_bounds[5] = EncodeDouble(1.5);
    validFile.lower_bounds[6] = EncodeInt64(100);
    validFile.upper_bounds[6] = EncodeInt64(200);

    const std::vector<size_t> allFields{0, 1, 2, 3, 4, 5};
    const auto validStats = ExtractFileZoneStats(validFile, icebergSchema, FullSchema, allFields);
    ASSERT_TRUE(validStats) << validStats.error().message;

    const auto checkZonePruned =
        [&](const std::vector<TColumnZoneStats>& zoneStats, const Predicate& pred, bool expected) {
            const auto bound = BindPredicate(pred, FullSchema);
            ASSERT_TRUE(bound) << bound.error().message;
            const auto pruned = CanPruneZone(*bound, validFile.record_count, allFields, zoneStats);
            ASSERT_TRUE(pruned) << pruned.error().message;
            EXPECT_EQ(*pruned, expected);
        };

    checkZonePruned(*validStats, Predicate::Compare("key", CompareOp::Lt, std::string("c")), true);
    checkZonePruned(*validStats, Predicate::Compare("count", CompareOp::Gt, int64_t{20}), true);
    checkZonePruned(*validStats, Predicate::Compare("score", CompareOp::Gt, 2.0), true);
    checkZonePruned(*validStats, Predicate::Compare("at", CompareOp::Gt, TimestampUs{200}), true);

    const auto expectManifestColumnReset = [&](size_t colIdx, auto mutate, const Predicate& unprunedPred) {
        auto bad = validFile;
        mutate(bad);
        const auto res = ExtractFileZoneStats(bad, icebergSchema, FullSchema, allFields);
        ASSERT_TRUE(res) << res.error().message;
        EXPECT_FALSE((*res)[colIdx].HasStats);
        EXPECT_FALSE((*res)[colIdx].NullCount.has_value());
        EXPECT_FALSE((*res)[colIdx].LowerBound.has_value());
        EXPECT_FALSE((*res)[colIdx].UpperBound.has_value());
        checkZonePruned(*res, unprunedPred, false);
        const Predicate siblingPred = (colIdx == 0) ? Predicate::Compare("count", CompareOp::Gt, int64_t{100})
                                                    : Predicate::Compare("key", CompareOp::Lt, std::string("b"));
        // Referencing the malformed column in `AND` also refuses to prune so `ValidateColumn` runs on it,
        // whereas predicates referencing only valid columns still prune even when the malformed column is projected!
        checkZonePruned(*res, Predicate::And({unprunedPred, siblingPred}), false);
        checkZonePruned(*res, siblingPred, true);
    };

    // Contradictory counts, NaN counts, required nulls, all-null with bounds:
    expectManifestColumnReset(
        4,
        [](iceberg::DataFile& f) {
            f.nan_value_counts[5] = 1;
        },
        Predicate::Compare("score", CompareOp::Gt, 10.0));
    expectManifestColumnReset(
        4,
        [](iceberg::DataFile& f) {
            f.nan_value_counts[5] = 1;
        },
        Predicate::IsNull("score"));
    expectManifestColumnReset(
        0,
        [](iceberg::DataFile& f) {
            f.value_counts[1] = 3;
        },
        Predicate::Compare("key", CompareOp::Lt, std::string("a")));
    expectManifestColumnReset(
        0,
        [](iceberg::DataFile& f) {
            f.null_value_counts[1] = 1; // required column -> even IS NULL must NOT prune!
        },
        Predicate::IsNull("key"));
    expectManifestColumnReset(
        1,
        [](iceberg::DataFile& f) {
            f.null_value_counts[2] = 5; // > record_count
        },
        Predicate::Compare("label", CompareOp::Lt, std::string("a")));
    expectManifestColumnReset(
        1,
        [](iceberg::DataFile& f) {
            f.null_value_counts[2] = -1;
        },
        Predicate::IsNull("label"));
    expectManifestColumnReset(
        3,
        [](iceberg::DataFile& f) {
            f.null_value_counts[4] = 4; // all-null with bounds present
        },
        Predicate::IsNotNull("count"));

    // Inverted bounds (lower > upper) across all 5 types:
    expectManifestColumnReset(
        0,
        [](iceberg::DataFile& f) {
            f.lower_bounds[1] = EncodeString("z");
            f.upper_bounds[1] = EncodeString("a");
        },
        Predicate::Compare("key", CompareOp::Eq, std::string("m")));
    expectManifestColumnReset(
        2,
        [](iceberg::DataFile& f) {
            f.lower_bounds[3] = {1};
            f.upper_bounds[3] = {0};
        },
        Predicate::Compare("flag", CompareOp::Eq, true));
    expectManifestColumnReset(
        3,
        [](iceberg::DataFile& f) {
            f.lower_bounds[4] = EncodeInt64(100);
            f.upper_bounds[4] = EncodeInt64(10);
        },
        Predicate::Compare("count", CompareOp::Eq, int64_t{50}));
    expectManifestColumnReset(
        4,
        [](iceberg::DataFile& f) {
            f.lower_bounds[5] = EncodeDouble(10.0);
            f.upper_bounds[5] = EncodeDouble(1.0);
        },
        Predicate::Compare("score", CompareOp::Eq, 5.0));
    expectManifestColumnReset(
        5,
        [](iceberg::DataFile& f) {
            f.lower_bounds[6] = EncodeInt64(500);
            f.upper_bounds[6] = EncodeInt64(100);
        },
        Predicate::Compare("at", CompareOp::Eq, TimestampUs{250}));

    // Invalid encodings, invalid UTF-8 (0xC3), NaN/Inf, wrong byte widths, domain overflows:
    expectManifestColumnReset(
        1,
        [](iceberg::DataFile& f) {
            f.lower_bounds[2] = {0xC3}; // truncated 2-byte UTF-8 lead byte before "é" (0xC3 0xA9)
        },
        Predicate::Compare("label", CompareOp::Eq, std::string("\xC3\xA9")));
    expectManifestColumnReset(
        1,
        [](iceberg::DataFile& f) {
            f.upper_bounds[2] = {0xC3};
        },
        Predicate::Compare("label", CompareOp::Eq, std::string("\xC3\xA9")));
    expectManifestColumnReset(
        2,
        [](iceberg::DataFile& f) {
            f.lower_bounds[3] = {2};
        },
        Predicate::Compare("flag", CompareOp::Eq, false));
    expectManifestColumnReset(
        3,
        [](iceberg::DataFile& f) {
            f.lower_bounds[4] = {1, 2, 3}; // wrong byte width for Int64
        },
        Predicate::Compare("count", CompareOp::Gt, int64_t{100}));
    expectManifestColumnReset(
        4,
        [](iceberg::DataFile& f) {
            f.lower_bounds[5] = EncodeDouble(std::numeric_limits<double>::quiet_NaN());
        },
        Predicate::Compare("score", CompareOp::Gt, 10.0));
    expectManifestColumnReset(
        4,
        [](iceberg::DataFile& f) {
            f.upper_bounds[5] = EncodeDouble(std::numeric_limits<double>::infinity());
        },
        Predicate::Compare("score", CompareOp::Lt, -10.0));
    expectManifestColumnReset(
        5,
        [](iceberg::DataFile& f) {
            f.upper_bounds[6] = EncodeInt64(253402300800000000LL);
        },
        Predicate::Compare("at", CompareOp::Gt, TimestampUs{500}));
    expectManifestColumnReset(
        0,
        [](iceberg::DataFile& f) {
            f.upper_bounds[1] = std::vector<uint8_t>(1025, 'k');
        },
        Predicate::Compare("key", CompareOp::Gt, std::string("z")));

    // Missing `nan_value_counts` drops Float64 bounds while keeping valid NullCount;
    // -0.0 in manifest lower_bounds normalizes to +0.0.
    {
        auto missingNanCount = validFile;
        missingNanCount.nan_value_counts.erase(5);
        const auto res = ExtractFileZoneStats(missingNanCount, icebergSchema, FullSchema, allFields);
        ASSERT_TRUE(res) << res.error().message;
        EXPECT_FALSE((*res)[4].LowerBound.has_value());
        EXPECT_FALSE((*res)[4].UpperBound.has_value());
        EXPECT_EQ((*res)[4].NullCount, 0);
        checkZonePruned(*res, Predicate::Compare("score", CompareOp::Gt, 100.0), false);
        checkZonePruned(*res, Predicate::IsNull("score"), true);

        auto negZeroManifest = validFile;
        negZeroManifest.lower_bounds[5] = EncodeDouble(-0.0);
        const auto ok = ExtractFileZoneStats(negZeroManifest, icebergSchema, FullSchema, allFields);
        ASSERT_TRUE(ok) << ok.error().message;
        ASSERT_TRUE((*ok)[4].LowerBound.has_value());
        EXPECT_FALSE(std::signbit(std::get<double>(*(*ok)[4].LowerBound)));
    }

    // `truncate(16)` on non-key String (`label`, fieldID 2) drops >= 16-codepoint upper_bound
    // at manifest level while keeping lower_bound; `key` (`full`, fieldID 1) keeps >= 16-codepoint upper_bound.
    {
        auto truncManifest = validFile;
        const std::string key20 = "key_prefix_012345678"; // 20 code points
        const std::string ascii16 = "shared_prefix_16";   // 16 ASCII code points
        // 16 Cyrillic code points (32 UTF-8 bytes)
        const std::string utf8_16 = "\xD0\xB0\xD0\xB1\xD0\xB2\xD0\xB3\xD0\xB4\xD0\xB5\xD0\xB6\xD0\xB7"
                                    "\xD0\xB8\xD0\xB9\xD0\xBA\xD0\xBB\xD0\xBC\xD0\xBD\xD0\xBE\xD0\xBF";
        truncManifest.lower_bounds[1] = EncodeString("key_a");
        truncManifest.upper_bounds[1] = EncodeString(key20);
        truncManifest.lower_bounds[2] = EncodeString(ascii16);
        truncManifest.upper_bounds[2] = EncodeString(ascii16);
        const auto resAscii = ExtractFileZoneStats(truncManifest, icebergSchema, FullSchema, allFields);
        ASSERT_TRUE(resAscii) << resAscii.error().message;
        EXPECT_TRUE((*resAscii)[0].UpperBound.has_value());
        EXPECT_TRUE((*resAscii)[1].LowerBound.has_value());
        EXPECT_FALSE((*resAscii)[1].UpperBound.has_value());
        checkZonePruned(*resAscii, Predicate::Compare("key", CompareOp::Gt, key20), true);
        checkZonePruned(*resAscii, Predicate::Compare("label", CompareOp::Eq, ascii16 + "_tail"), false);
        checkZonePruned(*resAscii, Predicate::Compare("label", CompareOp::Lt, ascii16), true);

        truncManifest.lower_bounds[2] = EncodeString("a");
        truncManifest.upper_bounds[2] = EncodeString(utf8_16);
        const auto resUtf8 = ExtractFileZoneStats(truncManifest, icebergSchema, FullSchema, allFields);
        ASSERT_TRUE(resUtf8) << resUtf8.error().message;
        EXPECT_FALSE((*resUtf8)[1].UpperBound.has_value());
        checkZonePruned(*resUtf8, Predicate::Compare("label", CompareOp::Eq, utf8_16 + "_tail"), false);
    }

    // Build real Parquet metadata via FileMetaDataBuilder to test corrupt/contradictory row-group statistics.
    const auto templateBatch = MakeRecordBatch(FullSchema,
        std::vector<Row>{{std::string("k"), std::string("lbl"), true, int64_t{10}, 1.0, TimestampUs{100}}});
    ASSERT_TRUE(templateBatch);
    const auto templateTable = arrow::Table::FromRecordBatches({*templateBatch}).ValueOrDie();
    auto templateSink = arrow::io::BufferOutputStream::Create().ValueOrDie();
    ASSERT_TRUE(parquet::arrow::WriteTable(*templateTable, arrow::default_memory_pool(), templateSink, 16).ok());
    const auto templateBytes = templateSink->Finish().ValueOrDie();
    auto templateReader = parquet::ParquetFileReader::Open(std::make_shared<arrow::io::BufferReader>(templateBytes));
    const auto* parquetSchema = templateReader->metadata()->schema();

    const auto makeBytesString = [](std::span<const uint8_t> bytes) {
        return std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    };

    const auto buildCustomRowGroupMeta =
        [&](int64_t numRows,
            const std::function<void(int col, int64_t& numValues, parquet::EncodedStatistics& enc)>& customize) {
            auto props = parquet::default_writer_properties();
            auto fb = parquet::FileMetaDataBuilder::Make(parquetSchema, props);
            auto* rgb = fb->AppendRowGroup();
            rgb->set_num_rows(numRows);
            const std::map<parquet::Encoding::type, int32_t> emptyEnc{};
            for (int col = 0; col < parquetSchema->num_columns(); ++col) {
                auto* ccb = rgb->NextColumnChunk();
                parquet::EncodedStatistics enc;
                enc.set_null_count(0);
                switch (col) {
                    case 0:
                        enc.set_min("c").set_max("x");
                        break;
                    case 1:
                        enc.set_min("lbl_c").set_max("lbl_x");
                        break;
                    case 2:
                        enc.set_min(std::string("\x00", 1)).set_max(std::string("\x01", 1));
                        break;
                    case 3:
                        enc.set_min(makeBytesString(EncodeInt64(10))).set_max(makeBytesString(EncodeInt64(20)));
                        break;
                    case 4:
                        enc.set_min(makeBytesString(EncodeDouble(0.0))).set_max(makeBytesString(EncodeDouble(1.5)));
                        break;
                    case 5:
                        enc.set_min(makeBytesString(EncodeInt64(100))).set_max(makeBytesString(EncodeInt64(200)));
                        break;
                }
                int64_t numValues = numRows;
                customize(col, numValues, enc);
                ccb->SetStatistics(enc);
                ccb->Finish(numValues, 0, -1, 4, 32, 32, false, false, emptyEnc, emptyEnc);
            }
            rgb->Finish(192, 0);
            return fb->Finish();
        };

    // Baseline valid custom row group prunes across all columns when validFile (`nan_value_counts[5] == 0`) is passed.
    {
        const auto okMeta = buildCustomRowGroupMeta(4, [](int, int64_t&, parquet::EncodedStatistics&) {
        });
        const auto rgStats =
            ExtractRowGroupZoneStats(*okMeta->RowGroup(0), FullSchema, allFields, &validFile, &icebergSchema);
        ASSERT_TRUE(rgStats) << rgStats.error().message;
        checkZonePruned(*rgStats, Predicate::Compare("key", CompareOp::Lt, std::string("c")), true);
        checkZonePruned(*rgStats, Predicate::Compare("score", CompareOp::Gt, 2.0), true);

        // Without `dataFile` or with `nan_value_counts` missing, Float64 row-group bounds are dropped (`prune
        // nothing`).
        const auto noDataFileStats = ExtractRowGroupZoneStats(*okMeta->RowGroup(0), FullSchema, allFields);
        ASSERT_TRUE(noDataFileStats) << noDataFileStats.error().message;
        EXPECT_FALSE((*noDataFileStats)[4].LowerBound.has_value());
        EXPECT_FALSE((*noDataFileStats)[4].UpperBound.has_value());
        checkZonePruned(*noDataFileStats, Predicate::Compare("score", CompareOp::Gt, 2.0), false);
    }

    const auto expectParquetColumnReset = [&](int targetCol,
                                              auto customizeCol,
                                              const Predicate& unprunedPred,
                                              const iceberg::DataFile* df = nullptr) {
        const iceberg::DataFile* activeDf = df ? df : &validFile;
        const auto meta = buildCustomRowGroupMeta(4, [&](int col, int64_t& numValues, parquet::EncodedStatistics& enc) {
            if (col == targetCol) {
                customizeCol(numValues, enc);
            }
        });
        const auto res = ExtractRowGroupZoneStats(*meta->RowGroup(0), FullSchema, allFields, activeDf, &icebergSchema);
        ASSERT_TRUE(res) << res.error().message;
        EXPECT_FALSE((*res)[static_cast<size_t>(targetCol)].LowerBound.has_value());
        EXPECT_FALSE((*res)[static_cast<size_t>(targetCol)].UpperBound.has_value());
        checkZonePruned(*res, unprunedPred, false);
    };

    // Reversed bounds (min > max) across all 5 types in Parquet row-group statistics:
    expectParquetColumnReset(
        0,
        [](int64_t&, parquet::EncodedStatistics& enc) {
            enc.set_min("z").set_max("a");
        },
        Predicate::Compare("key", CompareOp::Eq, std::string("m")));
    expectParquetColumnReset(
        2,
        [](int64_t&, parquet::EncodedStatistics& enc) {
            enc.set_min(std::string("\x01", 1)).set_max(std::string("\x00", 1));
        },
        Predicate::Compare("flag", CompareOp::Eq, true));
    expectParquetColumnReset(
        3,
        [&](int64_t&, parquet::EncodedStatistics& enc) {
            enc.set_min(makeBytesString(EncodeInt64(50))).set_max(makeBytesString(EncodeInt64(10)));
        },
        Predicate::Compare("count", CompareOp::Eq, int64_t{30}));
    expectParquetColumnReset(
        4,
        [&](int64_t&, parquet::EncodedStatistics& enc) {
            enc.set_min(makeBytesString(EncodeDouble(9.0))).set_max(makeBytesString(EncodeDouble(1.0)));
        },
        Predicate::Compare("score", CompareOp::Eq, 5.0));
    expectParquetColumnReset(
        5,
        [&](int64_t&, parquet::EncodedStatistics& enc) {
            enc.set_min(makeBytesString(EncodeInt64(900))).set_max(makeBytesString(EncodeInt64(100)));
        },
        Predicate::Compare("at", CompareOp::Eq, TimestampUs{500}));

    // Invalid UTF-8 (0xC3), out-of-range timestamp, NaN/Inf, mismatched num_values, contradictory all_null_value,
    // required column null_count > 0, and non-exact upper bound (`is_max_value_exact = false`):
    expectParquetColumnReset(
        1,
        [](int64_t&, parquet::EncodedStatistics& enc) {
            enc.set_min(std::string("\xC3", 1)).set_max("zzz");
        },
        Predicate::Compare("label", CompareOp::Eq, std::string("\xC3\xA9")));
    expectParquetColumnReset(
        1,
        [](int64_t&, parquet::EncodedStatistics& enc) {
            enc.set_min("aaa").set_max(std::string("\xC3", 1));
        },
        Predicate::Compare("label", CompareOp::Eq, std::string("\xC3\xA9")));
    expectParquetColumnReset(
        5,
        [&](int64_t&, parquet::EncodedStatistics& enc) {
            enc.set_max(makeBytesString(EncodeInt64(253402300800000000LL)));
        },
        Predicate::Compare("at", CompareOp::Gt, TimestampUs{500}));
    expectParquetColumnReset(
        4,
        [&](int64_t&, parquet::EncodedStatistics& enc) {
            enc.set_min(makeBytesString(EncodeDouble(std::numeric_limits<double>::quiet_NaN())));
        },
        Predicate::Compare("score", CompareOp::Gt, 10.0));
    expectParquetColumnReset(
        3,
        [](int64_t& numValues, parquet::EncodedStatistics&) {
            numValues = 3; // != rowCount (4)
        },
        Predicate::Compare("count", CompareOp::Gt, int64_t{100}));
    expectParquetColumnReset(
        3,
        [](int64_t&, parquet::EncodedStatistics& enc) {
            enc.set_null_count(4); // all-null row group with non-null min/max bounds present
        },
        Predicate::IsNotNull("count"));
    expectParquetColumnReset(
        3,
        [](int64_t&, parquet::EncodedStatistics& enc) {
            enc.set_null_count(5); // null_count > rowCount
        },
        Predicate::IsNull("count"));
    expectParquetColumnReset(
        3,
        [](int64_t&, parquet::EncodedStatistics& enc) {
            enc.set_null_count(-1);
        },
        Predicate::IsNull("count"));
    expectParquetColumnReset(
        0,
        [](int64_t&, parquet::EncodedStatistics& enc) {
            enc.set_null_count(1); // required column -> even IS NULL must NOT prune
        },
        Predicate::IsNull("key"));
    expectParquetColumnReset(
        1,
        [](int64_t&, parquet::EncodedStatistics& enc) {
            enc.is_max_value_exact = false;
            enc.is_min_value_exact = false;
        },
        Predicate::Compare("label", CompareOp::Gt, std::string("zzz")));

    auto nanManifestFile = validFile;
    nanManifestFile.nan_value_counts[5] = 1;
    expectParquetColumnReset(
        4,
        [](int64_t&, parquet::EncodedStatistics&) {
        },
        Predicate::Compare("score", CompareOp::Gt, 10.0),
        &nanManifestFile);
    expectParquetColumnReset(
        4,
        [](int64_t&, parquet::EncodedStatistics&) {
        },
        Predicate::IsNull("score"),
        &nanManifestFile);
}

} // namespace
} // namespace glamdb::detail

#endif // GLAMDB_TEST_STORAGE
