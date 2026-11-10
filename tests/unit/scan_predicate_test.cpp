#include "core/engine/storage_mapping.h"
#include "core/scan/predicate.h"

#include <arrow/api.h>
#include <gtest/gtest.h>

#include <limits>

namespace glamdb::detail {
namespace {

const SchemaSpec Schema{{
    {"key", FieldType::String, false},
    {"flag", FieldType::Bool, true},
    {"count", FieldType::Int64, true},
    {"score", FieldType::Float64, true},
    {"at", FieldType::TimestampUsUtc, true},
}};

std::shared_ptr<arrow::RecordBatch> Batch()
{
    const std::vector<Row> rows{
        {std::string("B"), true, int64_t{1}, -0.0, TimestampUs{10}},
        {std::string("a"), false, int64_t{2}, 1.5, TimestampUs{20}},
        {std::string("\xC3\xA9"), std::monostate{}, std::monostate{}, std::monostate{}, std::monostate{}},
        {std::string("a"), true, int64_t{3}, 2.5, TimestampUs{30}},
    };
    auto batch = MakeRecordBatch(Schema, rows);
    EXPECT_TRUE(batch) << batch.error().message;
    return *batch;
}

std::vector<int64_t> Select(const std::string& field, CompareOp op, Literal value)
{
    const auto bound = BindPredicate(Predicate::Compare(field, op, std::move(value)), Schema);
    EXPECT_TRUE(bound) << bound.error().message;
    if (!bound) {
        return {};
    }
    const auto selected = SelectRows(*bound, *Batch()->column(static_cast<int>(bound->FieldIndex)));
    EXPECT_TRUE(selected) << selected.error().message;
    return selected ? *selected : std::vector<int64_t>{};
}

TEST(ScanPredicate, TrueSelectsEveryRow)
{
    const auto bound = BindPredicate(Predicate::True(), Schema);
    ASSERT_TRUE(bound);
    const auto selected = SelectRows(*bound, *Batch()->column(0));
    ASSERT_TRUE(selected);
    EXPECT_EQ(*selected, (std::vector<int64_t>{0, 1, 2, 3}));
}

TEST(ScanPredicate, RejectsUnknownFieldsAndMismatchedLiterals)
{
    const auto rejected = [](const Predicate& predicate) {
        const auto bound = BindPredicate(predicate, Schema);
        return !bound && bound.error().code == "INVALID_ARGUMENT";
    };
    EXPECT_TRUE(rejected(Predicate::Compare("missing", CompareOp::Eq, int64_t{1})));
    EXPECT_TRUE(rejected(Predicate::Compare("count", CompareOp::Eq, 1.0)));
    EXPECT_TRUE(rejected(Predicate::Compare("at", CompareOp::Eq, int64_t{1})));
    EXPECT_TRUE(rejected(Predicate::Compare("key", CompareOp::Eq, false)));
    EXPECT_TRUE(rejected(Predicate::Compare("key", CompareOp::Eq, std::string("\xFF"))));
    EXPECT_TRUE(rejected(Predicate::Compare("score", CompareOp::Lt, std::numeric_limits<double>::quiet_NaN())));
    EXPECT_TRUE(rejected(Predicate::Compare("score", CompareOp::Lt, std::numeric_limits<double>::infinity())));
    EXPECT_TRUE(rejected(Predicate::Compare("at", CompareOp::Lt, TimestampUs{253402300800000000LL})));
}

TEST(ScanPredicate, NullNeverMatchesAnyComparison)
{
    for (const auto op : {CompareOp::Eq, CompareOp::Ne, CompareOp::Lt, CompareOp::Le, CompareOp::Gt, CompareOp::Ge}) {
        for (const auto row : Select("count", op, int64_t{2})) {
            EXPECT_NE(row, 2) << static_cast<int>(op);
        }
    }
    EXPECT_EQ(Select("count", CompareOp::Ne, int64_t{2}), (std::vector<int64_t>{0, 3}));
    EXPECT_EQ(Select("flag", CompareOp::Ne, true), (std::vector<int64_t>{1}));
}

TEST(ScanPredicate, ComparesEveryType)
{
    EXPECT_EQ(Select("count", CompareOp::Ge, int64_t{2}), (std::vector<int64_t>{1, 3}));
    EXPECT_EQ(Select("count", CompareOp::Lt, int64_t{2}), (std::vector<int64_t>{0}));
    EXPECT_EQ(Select("flag", CompareOp::Eq, true), (std::vector<int64_t>{0, 3}));
    EXPECT_EQ(Select("score", CompareOp::Eq, 0.0), (std::vector<int64_t>{0}));
    EXPECT_EQ(Select("score", CompareOp::Eq, -0.0), (std::vector<int64_t>{0}));
    EXPECT_EQ(Select("score", CompareOp::Gt, 1.5), (std::vector<int64_t>{3}));
    EXPECT_EQ(Select("at", CompareOp::Le, TimestampUs{20}), (std::vector<int64_t>{0, 1}));
    EXPECT_EQ(Select("key", CompareOp::Eq, std::string("a")), (std::vector<int64_t>{1, 3}));
}

TEST(ScanPredicate, StringsUseBinaryUtf8Order)
{
    // "B" (0x42) < "a" (0x61) < "\xC3\xA9" (U+00E9) by unsigned bytes.
    EXPECT_EQ(Select("key", CompareOp::Lt, std::string("a")), (std::vector<int64_t>{0}));
    EXPECT_EQ(Select("key", CompareOp::Gt, std::string("a")), (std::vector<int64_t>{2}));
}

TEST(ScanPredicate, TakeRowsPreservesValuesAndNulls)
{
    const auto batch = Batch();
    const std::vector<int64_t> rows{3, 2};
    for (int column = 0; column < batch->num_columns(); ++column) {
        const auto taken = TakeRows(*batch->column(column), rows);
        ASSERT_TRUE(taken) << taken.error().message;
        ASSERT_EQ((*taken)->length(), 2);
        EXPECT_TRUE((*taken)->type()->Equals(*batch->column(column)->type()));
        EXPECT_TRUE((*taken)->RangeEquals(0, 1, 3, batch->column(column)));
        EXPECT_EQ((*taken)->IsNull(1), batch->column(column)->IsNull(2));
    }
    EXPECT_FALSE(TakeRows(*batch->column(0), std::vector<int64_t>{4}));
}

} // namespace
} // namespace glamdb::detail
