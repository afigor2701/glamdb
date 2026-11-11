#include "core/engine/storage_mapping.h"
#include "core/scan/predicate.h"

#include <arrow/api.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <vector>

namespace glamdb::detail {
namespace {

const SchemaSpec Schema{{
    {"key", FieldType::String, false},
    {"label", FieldType::String, true},
    {"flag", FieldType::Bool, true},
    {"count", FieldType::Int64, true},
    {"score", FieldType::Float64, true},
    {"at", FieldType::TimestampUsUtc, true},
}};

std::shared_ptr<arrow::RecordBatch> Batch()
{
    const std::vector<Row> rows{
        {std::string("B"), std::string("x"), true, int64_t{1}, -0.0, TimestampUs{10}},
        {std::string("a"), std::string("y"), false, int64_t{2}, 1.5, TimestampUs{20}},
        {std::string("\xC3\xA9"),
            std::monostate{},
            std::monostate{},
            std::monostate{},
            std::monostate{},
            std::monostate{}},
        {std::string("a"), std::string("z"), true, int64_t{3}, 2.5, TimestampUs{30}},
    };
    auto batch = MakeRecordBatch(Schema, rows);
    EXPECT_TRUE(batch) << batch.error().message;
    return *batch;
}

std::vector<int64_t> Eval(const Predicate& predicate, const std::shared_ptr<arrow::RecordBatch>& batch = Batch())
{
    const auto bound = BindPredicate(predicate, Schema);
    EXPECT_TRUE(bound) << bound.error().message;
    if (!bound) {
        return {};
    }
    const auto selected = SelectRows(*bound, *batch);
    EXPECT_TRUE(selected) << selected.error().message;
    return selected ? *selected : std::vector<int64_t>{};
}

std::vector<int64_t> Select(const std::string& field, CompareOp op, Literal value)
{
    return Eval(Predicate::Compare(field, op, std::move(value)));
}

TEST(ScanPredicate, TrueSelectsEveryRow)
{
    const auto bound = BindPredicate(Predicate::True(), Schema);
    ASSERT_TRUE(bound);
    EXPECT_TRUE(bound->IsAlwaysTrue());
    EXPECT_TRUE(bound->ReferencedFields.empty());
    const auto selected = SelectRows(*bound, *Batch());
    ASSERT_TRUE(selected);
    EXPECT_EQ(*selected, (std::vector<int64_t>{0, 1, 2, 3}));
}

TEST(ScanPredicate, PublicAstAccessorsAndSingleInOverload)
{
    const auto t = Predicate::True();
    EXPECT_EQ(t.kind(), Predicate::Kind::True);
    EXPECT_TRUE(t.field().empty());
    EXPECT_FALSE(t.optional_value().has_value());
    EXPECT_TRUE(t.values().empty());
    EXPECT_TRUE(t.children().empty());

    const auto cmp = Predicate::Compare("count", CompareOp::Ge, int64_t{5});
    EXPECT_EQ(cmp.kind(), Predicate::Kind::Compare);
    EXPECT_EQ(cmp.field(), "count");
    EXPECT_EQ(cmp.op(), CompareOp::Ge);
    ASSERT_TRUE(cmp.optional_value().has_value());
    EXPECT_EQ(std::get<int64_t>(cmp.value()), 5);

    const auto cmpNull = Predicate::CompareNull("count", CompareOp::Eq);
    EXPECT_EQ(cmpNull.kind(), Predicate::Kind::Compare);
    EXPECT_EQ(cmpNull.field(), "count");
    EXPECT_FALSE(cmpNull.optional_value().has_value());

    const auto inEmpty = Predicate::In("count", {});
    EXPECT_EQ(inEmpty.kind(), Predicate::Kind::In);
    EXPECT_TRUE(inEmpty.values().empty());

    const auto inValues = Predicate::In("count", {int64_t{1}, std::nullopt, int64_t{2}});
    ASSERT_EQ(inValues.values().size(), 3U);
    EXPECT_TRUE(inValues.values()[0].has_value());
    EXPECT_FALSE(inValues.values()[1].has_value());

    const auto isNull = Predicate::IsNull("label");
    const auto isNotNull = Predicate::IsNotNull("label");
    const auto neg = Predicate::Not(isNull);
    const auto conj = Predicate::And({cmp, isNotNull});
    const auto disj = Predicate::Or({conj, neg});
    EXPECT_EQ(neg.kind(), Predicate::Kind::Not);
    ASSERT_EQ(neg.children().size(), 1U);
    EXPECT_EQ(conj.kind(), Predicate::Kind::And);
    ASSERT_EQ(conj.children().size(), 2U);
    EXPECT_EQ(disj.kind(), Predicate::Kind::Or);
    ASSERT_EQ(disj.children().size(), 2U);
}

TEST(ScanPredicate, RejectsUnknownFieldsAndMismatchedLiterals)
{
    const auto rejected = [](const Predicate& predicate) {
        const auto bound = BindPredicate(predicate, Schema);
        return !bound && bound.error().code == "INVALID_ARGUMENT" && bound.error().retry_hint == RetryHint::FixInput;
    };
    EXPECT_TRUE(rejected(Predicate::Compare("missing", CompareOp::Eq, int64_t{1})));
    EXPECT_TRUE(rejected(Predicate::CompareNull("missing", CompareOp::Eq)));
    EXPECT_TRUE(rejected(Predicate::In("missing", {int64_t{1}})));
    EXPECT_TRUE(rejected(Predicate::IsNull("missing")));
    EXPECT_TRUE(rejected(Predicate::IsNotNull("missing")));
    EXPECT_TRUE(rejected(Predicate::Compare("count", CompareOp::Eq, 1.0)));
    EXPECT_TRUE(rejected(Predicate::Compare("at", CompareOp::Eq, int64_t{1})));
    EXPECT_TRUE(rejected(Predicate::Compare("key", CompareOp::Eq, false)));
    EXPECT_TRUE(rejected(Predicate::Compare("key", CompareOp::Eq, std::string("\xFF"))));
    EXPECT_TRUE(rejected(Predicate::Compare("score", CompareOp::Lt, std::numeric_limits<double>::quiet_NaN())));
    EXPECT_TRUE(rejected(Predicate::Compare("score", CompareOp::Lt, std::numeric_limits<double>::infinity())));
    EXPECT_TRUE(rejected(Predicate::Compare("at", CompareOp::Lt, TimestampUs{253402300800000000LL})));
    EXPECT_TRUE(rejected(Predicate::Compare("at", CompareOp::Ge, TimestampUs{-62135596800000001LL})));
    EXPECT_TRUE(rejected(Predicate::In("count", {int64_t{1}, std::string("bad")})));
    EXPECT_TRUE(rejected(Predicate::And({})));
    EXPECT_TRUE(rejected(Predicate::Or({})));
    // Invalid nested leaf inside a short-circuitable branch must still fail at BindPredicate.
    EXPECT_TRUE(rejected(Predicate::Or({Predicate::True(), Predicate::Compare("missing", CompareOp::Eq, int64_t{1})})));
}

TEST(ScanPredicate, EnforcesStringByteLimitsForKeyAndValueFields)
{
    EXPECT_TRUE(BindPredicate(Predicate::Compare("key", CompareOp::Eq, std::string(1024, 'k')), Schema).has_value());
    const auto tooLongKey = BindPredicate(Predicate::Compare("key", CompareOp::Eq, std::string(1025, 'k')), Schema);
    ASSERT_FALSE(tooLongKey);
    EXPECT_EQ(tooLongKey.error().code, "INVALID_ARGUMENT");

    const auto tooLongKeyIn = BindPredicate(Predicate::In("key", {std::string(1025, 'k')}), Schema);
    ASSERT_FALSE(tooLongKeyIn);
    EXPECT_EQ(tooLongKeyIn.error().code, "INVALID_ARGUMENT");

    EXPECT_TRUE(BindPredicate(Predicate::Compare("label", CompareOp::Eq, std::string(1025, 'v')), Schema).has_value());
    EXPECT_TRUE(
        BindPredicate(Predicate::Compare("label", CompareOp::Eq, std::string(1U << 20, 'v')), Schema).has_value());
    const auto tooLongLabel =
        BindPredicate(Predicate::Compare("label", CompareOp::Eq, std::string((1U << 20) + 1, 'v')), Schema);
    ASSERT_FALSE(tooLongLabel);
    EXPECT_EQ(tooLongLabel.error().code, "INVALID_ARGUMENT");
}

TEST(ScanPredicate, NullNeverMatchesAnyComparisonAndCompareNullIsUnknown)
{
    for (const auto op : {CompareOp::Eq, CompareOp::Ne, CompareOp::Lt, CompareOp::Le, CompareOp::Gt, CompareOp::Ge}) {
        for (const auto row : Select("count", op, int64_t{2})) {
            EXPECT_NE(row, 2) << static_cast<int>(op);
        }
        EXPECT_TRUE(Eval(Predicate::CompareNull("count", op)).empty());
        EXPECT_TRUE(Eval(Predicate::Not(Predicate::CompareNull("count", op))).empty());
        EXPECT_TRUE(Eval(Predicate::CompareNull("key", op)).empty());
    }
    // Unknown OR True == True; Unknown AND True == Unknown.
    EXPECT_EQ(Eval(Predicate::Or({
                  Predicate::CompareNull("count", CompareOp::Eq),
                  Predicate::Compare("count", CompareOp::Eq, int64_t{2}),
              })),
        (std::vector<int64_t>{1}));
    EXPECT_TRUE(Eval(Predicate::And({
                         Predicate::CompareNull("count", CompareOp::Eq),
                         Predicate::True(),
                     }))
                    .empty());
    // CompareNull still records the field in ReferencedFields for cursor column validation.
    const auto boundNull = BindPredicate(Predicate::CompareNull("score", CompareOp::Eq), Schema);
    ASSERT_TRUE(boundNull);
    EXPECT_EQ(boundNull->ReferencedFields, (std::vector<size_t>{4}));

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

TEST(ScanPredicate, IsNullAndIsNotNullOnNullableAndRequiredFields)
{
    EXPECT_EQ(Eval(Predicate::IsNull("count")), (std::vector<int64_t>{2}));
    EXPECT_EQ(Eval(Predicate::IsNotNull("count")), (std::vector<int64_t>{0, 1, 3}));
    EXPECT_EQ(Eval(Predicate::Not(Predicate::IsNull("count"))), (std::vector<int64_t>{0, 1, 3}));
    EXPECT_EQ(Eval(Predicate::Not(Predicate::IsNotNull("count"))), (std::vector<int64_t>{2}));

    // Required field "key" is allowed in IsNull / IsNotNull.
    EXPECT_TRUE(Eval(Predicate::IsNull("key")).empty());
    EXPECT_EQ(Eval(Predicate::IsNotNull("key")), (std::vector<int64_t>{0, 1, 2, 3}));
}

TEST(ScanPredicate, InHandlesEmptyListDuplicatesNullsAndNegativeZero)
{
    // Row 2 has count = NULL:
    // - For non-NULL rows (0, 1, 3), count IN [] is FALSE -> NOT(count IN []) is TRUE.
    // - For NULL row (2), count IN [] is UNKNOWN -> NOT(count IN []) is UNKNOWN (excluded!).
    EXPECT_TRUE(Eval(Predicate::In("count", {})).empty());
    EXPECT_EQ(Eval(Predicate::Not(Predicate::In("count", {}))), (std::vector<int64_t>{0, 1, 3}));

    // Duplicates in IN list are deduplicated and matched accurately.
    EXPECT_EQ(Eval(Predicate::In("count", {int64_t{3}, int64_t{1}, int64_t{3}})), (std::vector<int64_t>{0, 3}));
    EXPECT_EQ(Eval(Predicate::Not(Predicate::In("count", {int64_t{3}, int64_t{1}}))), (std::vector<int64_t>{1}));

    // IN list containing NULL:
    // - Matching non-null value -> TRUE (row 0 has count=1).
    // - Non-matching value when NULL is in list -> UNKNOWN (rows 1, 2, 3), so NOT(...) is empty!
    EXPECT_EQ(Eval(Predicate::In("count", {int64_t{1}, std::nullopt})), (std::vector<int64_t>{0}));
    EXPECT_TRUE(Eval(Predicate::Not(Predicate::In("count", {int64_t{1}, std::nullopt}))).empty());
    EXPECT_TRUE(Eval(Predicate::In("count", {std::nullopt})).empty());
    EXPECT_TRUE(Eval(Predicate::Not(Predicate::In("count", {std::nullopt}))).empty());

    // Negative zero normalization and other types in IN:
    EXPECT_EQ(Eval(Predicate::In("score", {-0.0, 2.5})), (std::vector<int64_t>{0, 3}));
    EXPECT_EQ(Eval(Predicate::In("key", {std::string("B"), std::string("\xC3\xA9")})), (std::vector<int64_t>{0, 2}));
    EXPECT_EQ(Eval(Predicate::In("flag", {false})), (std::vector<int64_t>{1}));
    EXPECT_EQ(Eval(Predicate::In("at", {TimestampUs{20}, TimestampUs{30}})), (std::vector<int64_t>{1, 3}));
}

TEST(ScanPredicate, ExhaustiveThreeValuedTruthTablesAndDeMorganLaws)
{
    const SchemaSpec pairSchema{{
        {"key", FieldType::String, false},
        {"a", FieldType::Bool, true},
        {"b", FieldType::Bool, true},
    }};
    // 9 rows covering all {True, Unknown(NULL), False}^2 combinations:
    // 0: (T, T), 1: (T, U), 2: (T, F)
    // 3: (U, T), 4: (U, U), 5: (U, F)
    // 6: (F, T), 7: (F, U), 8: (F, F)
    const std::vector<Row> rows{
        {std::string("0"), true, true},
        {std::string("1"), true, std::monostate{}},
        {std::string("2"), true, false},
        {std::string("3"), std::monostate{}, true},
        {std::string("4"), std::monostate{}, std::monostate{}},
        {std::string("5"), std::monostate{}, false},
        {std::string("6"), false, true},
        {std::string("7"), false, std::monostate{}},
        {std::string("8"), false, false},
    };
    const auto batchRes = MakeRecordBatch(pairSchema, rows);
    ASSERT_TRUE(batchRes) << batchRes.error().message;
    const auto& batch = *batchRes;

    const auto evalPair = [&](const Predicate& p) {
        const auto bound = BindPredicate(p, pairSchema);
        EXPECT_TRUE(bound) << bound.error().message;
        const auto sel = SelectRows(*bound, *batch);
        EXPECT_TRUE(sel) << sel.error().message;
        return *sel;
    };

    const auto aTrue = Predicate::Compare("a", CompareOp::Eq, true);
    const auto bTrue = Predicate::Compare("b", CompareOp::Eq, true);

    // a is TRUE on {0, 1, 2}; NOT a is TRUE on {6, 7, 8} (rows 3, 4, 5 are UNKNOWN).
    EXPECT_EQ(evalPair(aTrue), (std::vector<int64_t>{0, 1, 2}));
    EXPECT_EQ(evalPair(Predicate::Not(aTrue)), (std::vector<int64_t>{6, 7, 8}));

    // a AND b: TRUE only on {0}; FALSE on {2, 5, 6, 7, 8}; UNKNOWN on {1, 3, 4}.
    const auto aAndB = Predicate::And({aTrue, bTrue});
    EXPECT_EQ(evalPair(aAndB), (std::vector<int64_t>{0}));
    // NOT(a AND b) is TRUE where (a AND b) is FALSE: {2, 5, 6, 7, 8}.
    // Note rows 5 (U, F) and 7 (F, U) are TRUE because FALSE AND UNKNOWN == FALSE!
    EXPECT_EQ(evalPair(Predicate::Not(aAndB)), (std::vector<int64_t>{2, 5, 6, 7, 8}));

    // a OR b: TRUE on {0, 1, 2, 3, 6}; FALSE on {8}; UNKNOWN on {4, 5, 7}.
    // Note rows 1 (T, U) and 3 (U, T) are TRUE because TRUE OR UNKNOWN == TRUE!
    const auto aOrB = Predicate::Or({aTrue, bTrue});
    EXPECT_EQ(evalPair(aOrB), (std::vector<int64_t>{0, 1, 2, 3, 6}));
    // NOT(a OR b) is TRUE where (a OR b) is FALSE: {8}.
    EXPECT_EQ(evalPair(Predicate::Not(aOrB)), (std::vector<int64_t>{8}));

    // De Morgan's laws under SQL 3VL:
    // 1) NOT(a AND b) === (NOT a) OR (NOT b)
    const auto notAOrNotB = Predicate::Or({Predicate::Not(aTrue), Predicate::Not(bTrue)});
    EXPECT_EQ(evalPair(Predicate::Not(aAndB)), evalPair(notAOrNotB));
    // 2) NOT(a OR b) === (NOT a) AND (NOT b)
    const auto notAAndNotB = Predicate::And({Predicate::Not(aTrue), Predicate::Not(bTrue)});
    EXPECT_EQ(evalPair(Predicate::Not(aOrB)), evalPair(notAAndNotB));
    // 3) Double negation: NOT(NOT a) === a
    EXPECT_EQ(evalPair(Predicate::Not(Predicate::Not(aTrue))), evalPair(aTrue));
}

TEST(ScanPredicate, EnforcesDepthNodeAndLiteralLimits)
{
    auto makeDepth = [](size_t depth) {
        Predicate p = Predicate::Compare("count", CompareOp::Eq, int64_t{1});
        for (size_t d = 0; d < depth; ++d) {
            p = Predicate::Not(std::move(p));
        }
        return p;
    };
    EXPECT_TRUE(BindPredicate(makeDepth(MaxPredicateDepth), Schema).has_value());
    EXPECT_EQ(Eval(makeDepth(MaxPredicateDepth)), (std::vector<int64_t>{0}));
    const auto tooDeep = BindPredicate(makeDepth(MaxPredicateDepth + 1), Schema);
    ASSERT_FALSE(tooDeep);
    EXPECT_EQ(tooDeep.error().code, "INVALID_ARGUMENT");

    // MaxPredicateNodes = 1024: root And (1) + 1023 leaves = 1024 nodes (valid).
    std::vector<Predicate> children;
    children.reserve(MaxPredicateNodes);
    for (size_t i = 0; i < MaxPredicateNodes - 1; ++i) {
        children.push_back(Predicate::Compare("count", CompareOp::Ge, int64_t{1}));
    }
    EXPECT_TRUE(BindPredicate(Predicate::And(children), Schema).has_value());
    children.push_back(Predicate::Compare("count", CompareOp::Ge, int64_t{1}));
    const auto tooManyNodes = BindPredicate(Predicate::And(std::move(children)), Schema);
    ASSERT_FALSE(tooManyNodes);
    EXPECT_EQ(tooManyNodes.error().code, "INVALID_ARGUMENT");

    // MaxPredicateLiterals = 4096: In with 4096 values is valid; 4097 is rejected.
    std::vector<OptionalLiteral> values(MaxPredicateLiterals, OptionalLiteral{int64_t{1}});
    EXPECT_TRUE(BindPredicate(Predicate::In("count", values), Schema).has_value());
    values.emplace_back(int64_t{2});
    const auto tooManyLiterals = BindPredicate(Predicate::In("count", std::move(values)), Schema);
    ASSERT_FALSE(tooManyLiterals);
    EXPECT_EQ(tooManyLiterals.error().code, "INVALID_ARGUMENT");
}

TEST(ScanPredicate, WideConjunctionAcrossMultipleEvaluationStrips)
{
    // 4500 rows (> 2 * EvalStripRows = 4096) and 1000 children in And.
    constexpr int64_t RowCount = 4500;
    std::vector<Row> rows;
    rows.reserve(static_cast<size_t>(RowCount));
    for (int64_t i = 0; i < RowCount; ++i) {
        rows.push_back({std::to_string(i), std::string("lbl"), true, i, 1.0, TimestampUs{i}});
    }
    const auto batchRes = MakeRecordBatch(Schema, rows);
    ASSERT_TRUE(batchRes);

    std::vector<Predicate> args;
    args.reserve(1000);
    for (int64_t i = 0; i < 999; ++i) {
        args.push_back(Predicate::Compare("count", CompareOp::Ge, i));
    }
    args.push_back(Predicate::Compare("count", CompareOp::Lt, int64_t{1002}));

    const auto selected = Eval(Predicate::And(std::move(args)), *batchRes);
    EXPECT_EQ(selected, (std::vector<int64_t>{998, 999, 1000, 1001}));
}

TEST(ScanPredicate, DeepTreeAndDagDestructionIsStackSafe)
{
    // 200,000-deep unary chain must not overflow the call stack on destruction.
    {
        Predicate p = Predicate::True();
        for (int i = 0; i < 200'000; ++i) {
            p = Predicate::Not(std::move(p));
        }
        EXPECT_EQ(p.kind(), Predicate::Kind::Not);
    }
    // Deep branching DAG with shared subtrees and multi-child nodes.
    {
        Predicate p = Predicate::Compare("count", CompareOp::Eq, int64_t{1});
        for (int i = 0; i < 50'000; ++i) {
            Predicate leaf = Predicate::IsNotNull("key");
            p = Predicate::And({std::move(p), leaf, std::move(leaf)});
        }
        EXPECT_EQ(p.kind(), Predicate::Kind::And);
    }
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
