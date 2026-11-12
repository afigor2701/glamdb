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

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstring>
#include <limits>
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

    // Missing statistics (`HasStats == false`) conservatively disables pruning.
    auto unstat = stats;
    unstat[2].HasStats = false;
    const auto boundCompareNull = BindPredicate(Predicate::CompareNull("count", CompareOp::Eq), FullSchema);
    ASSERT_TRUE(boundCompareNull);
    const auto unstatPruned = CanPruneZone(*boundCompareNull, 8, fields, unstat);
    ASSERT_TRUE(unstatPruned);
    EXPECT_FALSE(*unstatPruned);
}

TEST(ScanPruningUnitTest, RejectsCorruptManifestAndParquetStatisticsBeforePruning)
{
    const auto profile = NGlamDB::NStorage::MakeTableProfile(ToStorageSchema(FullSchema));
    ASSERT_TRUE(profile.has_value());
    const auto& icebergSchema = *profile->Schema;

    iceberg::DataFile validFile;
    validFile.record_count = 4;
    validFile.value_counts = {{1, 4}, {2, 4}, {3, 4}, {4, 4}, {5, 4}, {6, 4}};
    validFile.null_value_counts = {{1, 0}, {2, 1}, {3, 0}, {4, 0}, {5, 0}, {6, 0}};
    validFile.nan_value_counts = {{5, 0}};
    validFile.lower_bounds[1] = EncodeString("a");
    validFile.upper_bounds[1] = EncodeString("z");
    validFile.lower_bounds[3] = {0};
    validFile.upper_bounds[3] = {1};
    validFile.lower_bounds[4] = EncodeInt64(1);
    validFile.upper_bounds[4] = EncodeInt64(9);
    validFile.lower_bounds[5] = EncodeDouble(0.0);
    validFile.upper_bounds[5] = EncodeDouble(1.5);
    validFile.lower_bounds[6] = EncodeInt64(10);
    validFile.upper_bounds[6] = EncodeInt64(20);

    const std::vector<size_t> allFields{0, 1, 2, 3, 4, 5};
    ASSERT_TRUE(ExtractFileZoneStats(validFile, icebergSchema, FullSchema, allFields));

    const auto expectCorruptManifest = [&](auto mutate) {
        auto bad = validFile;
        mutate(bad);
        const auto res = ExtractFileZoneStats(bad, icebergSchema, FullSchema, allFields);
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().code, "CORRUPTION") << res.error().message;
    };

    expectCorruptManifest([](iceberg::DataFile& f) {
        f.nan_value_counts[5] = 1;
    });
    expectCorruptManifest([](iceberg::DataFile& f) {
        f.value_counts[1] = 3;
    });
    expectCorruptManifest([](iceberg::DataFile& f) {
        f.null_value_counts[1] = 1;
    }); // required column
    expectCorruptManifest([](iceberg::DataFile& f) {
        f.null_value_counts[2] = 5;
    }); // > record_count
    expectCorruptManifest([](iceberg::DataFile& f) {
        f.null_value_counts[4] = 4;
    }); // all-null with bounds present
    expectCorruptManifest([](iceberg::DataFile& f) {
        f.lower_bounds[4] = EncodeInt64(100);
    }); // lower > upper
    expectCorruptManifest([](iceberg::DataFile& f) {
        f.lower_bounds[3] = {2};
    }); // invalid bool byte
    expectCorruptManifest([](iceberg::DataFile& f) {
        f.lower_bounds[5] = EncodeDouble(std::numeric_limits<double>::quiet_NaN());
    });
    expectCorruptManifest([](iceberg::DataFile& f) {
        f.upper_bounds[5] = EncodeDouble(std::numeric_limits<double>::infinity());
    });
    expectCorruptManifest([](iceberg::DataFile& f) {
        f.upper_bounds[6] = EncodeInt64(253402300800000000LL);
    });
    expectCorruptManifest([](iceberg::DataFile& f) {
        f.lower_bounds[1] = {0xFF, 0xFE};
    }); // invalid UTF-8
    expectCorruptManifest([](iceberg::DataFile& f) {
        f.upper_bounds[1] = std::vector<uint8_t>(1025, 'k');
    });

    // Parquet writers emit -0.0 as min for +0.0 (PARQUET-1222); ExtractFileZoneStats
    // normalizes -0.0 to +0.0, while out-of-range timestamps in Parquet stats fail CORRUPTION.
    {
        auto negZeroManifest = validFile;
        negZeroManifest.lower_bounds[5] = EncodeDouble(-0.0);
        const auto ok = ExtractFileZoneStats(negZeroManifest, icebergSchema, FullSchema, allFields);
        ASSERT_TRUE(ok) << ok.error().message;
        ASSERT_TRUE((*ok)[4].LowerBound.has_value());
        EXPECT_FALSE(std::signbit(std::get<double>(*(*ok)[4].LowerBound)));
    }

    const auto batchRes = MakeRecordBatch(FullSchema,
        std::vector<Row>{{std::string("k"), std::string("lbl"), true, int64_t{1}, 0.0, TimestampUs{10}}});
    ASSERT_TRUE(batchRes);
    arrow::TimestampBuilder badTsBuilder(arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"), arrow::default_memory_pool());
    ASSERT_TRUE(badTsBuilder.Append(253402300800000000LL).ok());
    std::shared_ptr<arrow::Array> badTsArray;
    ASSERT_TRUE(badTsBuilder.Finish(&badTsArray).ok());
    const auto badBatch = arrow::RecordBatch::Make((*batchRes)->schema(),
        1,
        {(*batchRes)->column(0),
            (*batchRes)->column(1),
            (*batchRes)->column(2),
            (*batchRes)->column(3),
            (*batchRes)->column(4),
            badTsArray});
    const auto badTable = arrow::Table::FromRecordBatches({badBatch}).ValueOrDie();
    auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
    ASSERT_TRUE(parquet::arrow::WriteTable(*badTable, arrow::default_memory_pool(), sink, 16).ok());
    const auto parquetBytes = sink->Finish().ValueOrDie();
    auto parquetReader = parquet::ParquetFileReader::Open(std::make_shared<arrow::io::BufferReader>(parquetBytes));
    const auto rg = parquetReader->metadata()->RowGroup(0);
    const auto rgRes = ExtractRowGroupZoneStats(*rg, FullSchema, allFields);
    ASSERT_FALSE(rgRes);
    EXPECT_EQ(rgRes.error().code, "CORRUPTION") << rgRes.error().message;
}

} // namespace
} // namespace glamdb::detail

#endif // GLAMDB_TEST_STORAGE
