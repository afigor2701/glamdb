#include "core/api/canonical.h"

#include <arrow/api.h>
#include <gtest/gtest.h>
#include <limits>

namespace glamdb::detail {
namespace {

std::shared_ptr<arrow::RecordBatch> Integers(const std::vector<std::optional<int64_t>>& values)
{
    arrow::Int64Builder builder;
    for (const auto value : values) {
        const auto status = value ? builder.Append(*value) : builder.AppendNull();
        EXPECT_TRUE(status.ok());
    }
    const auto array = builder.Finish();
    EXPECT_TRUE(array.ok());
    return arrow::RecordBatch::Make(arrow::schema({arrow::field("n", arrow::int64())}),
        static_cast<int64_t>(values.size()),
        {*array});
}

TEST(ArrowInput, ChunkBoundariesDoNotChangeCanonicalIdentity)
{
    const SchemaSpec schema{{{"n", FieldType::Int64, true}}};
    const auto single = NormalizeBatches(schema,
        "00112233-4455-6677-8899-aabbccddeeff",
        {Integers({9007199254740993LL, std::nullopt, 9007199254740993LL})});
    const auto chunked = NormalizeBatches(schema,
        "00112233-4455-6677-8899-aabbccddeeff",
        {Integers({9007199254740993LL}), Integers({std::nullopt}), Integers({9007199254740993LL})});
    ASSERT_TRUE(single);
    ASSERT_TRUE(chunked);
    EXPECT_EQ(single->payload_hash, "e9cbe7131f25c3f2932c48f755529fd10370a936c5182e2bca621d5f9e3c0fd7");
    EXPECT_EQ(chunked->payload_hash, single->payload_hash);
    EXPECT_EQ(chunked->rows, single->rows);
    const auto fingerprint = FingerprintBatches(schema,
        "00112233-4455-6677-8899-aabbccddeeff",
        {Integers({9007199254740993LL}), Integers({std::nullopt}), Integers({9007199254740993LL})});
    ASSERT_TRUE(fingerprint);
    EXPECT_EQ(*fingerprint, single->payload_hash);
}

TEST(ArrowInput, MissingNullableFieldsBecomeNullButUnknownAndNullBatchReject)
{
    const SchemaSpec schema{{{"n", FieldType::Int64, true}, {"extra", FieldType::Bool, true}}};
    const auto result = NormalizeBatches(schema, "00112233-4455-6677-8899-aabbccddeeff", {Integers({1})});
    ASSERT_TRUE(result);
    EXPECT_TRUE(std::holds_alternative<std::monostate>(result->rows[0][1]));
    EXPECT_FALSE(NormalizeBatches(SchemaSpec{{{"other", FieldType::Int64, true}}},
        "00112233-4455-6677-8899-aabbccddeeff",
        {Integers({1})}));
    EXPECT_FALSE(NormalizeBatches(schema, "00112233-4455-6677-8899-aabbccddeeff", {nullptr}));
}

TEST(ArrowInput, InputValidationErrorsSuggestFixingTheInput)
{
    const std::vector<std::pair<SchemaSpec, std::shared_ptr<arrow::RecordBatch>>> cases{
        {SchemaSpec{{{"n", FieldType::Bool, true}}}, Integers({1})},
        {SchemaSpec{{{"n", FieldType::Int64, false}}}, Integers({std::nullopt})},
        {SchemaSpec{{{"other", FieldType::Int64, true}}}, Integers({1})},
        {SchemaSpec{{{"n", FieldType::Int64, true}}}, nullptr}};
    for (const auto& [schema, batch] : cases) {
        const auto reservation = InputReservation(schema, {batch});
        ASSERT_FALSE(reservation);
        EXPECT_EQ(reservation.error().retry_hint, RetryHint::FixInput);
        const auto normalized = NormalizeBatches(schema, "00112233-4455-6677-8899-aabbccddeeff", {batch});
        ASSERT_FALSE(normalized);
        EXPECT_EQ(normalized.error().retry_hint, RetryHint::FixInput);
        const auto fingerprint = FingerprintBatches(schema, "00112233-4455-6677-8899-aabbccddeeff", {batch});
        ASSERT_FALSE(fingerprint);
        EXPECT_EQ(fingerprint.error().retry_hint, RetryHint::FixInput);
    }
}

// Canonical limits count NULL as one validity byte, independently of the
// writer's fixed slots. Exact row and batch boundaries must agree in the
// streaming fingerprint, reservation inspector and owned normalization.
TEST(ArrowInput, NullHeavyCanonicalLimitsAgreeAcrossInputPaths)
{
    const SchemaSpec schema{
        {{"a", FieldType::String, true}, {"b", FieldType::String, true}, {"n", FieldType::Int64, true}}};
    for (const auto rows : {1, 16}) {
        for (const auto extra : {0, 1}) {
            SCOPED_TRACE(::testing::Message() << "rows=" << rows << " extra=" << extra);
            // 2 * (validity + four-byte length) + NULL validity = 11.
            // One row reaches 2 MiB; sixteen 1 MiB rows reach the batch cap
            // while every row remains below its independent cap.
            const size_t firstBytes = rows == 1 ? 1048570 : 524282;
            const size_t secondBytes = rows == 1 ? 1048571 : 524283;
            std::vector<Row> input(static_cast<size_t>(rows),
                Row{std::string(firstBytes, 'x'), std::string(secondBytes, 'y'), std::monostate{}});
            std::get<std::string>(input.back()[0]).append(static_cast<size_t>(extra), 'z');
            arrow::StringBuilder first;
            arrow::StringBuilder second;
            for (const auto& row : input) {
                ASSERT_TRUE(first.Append(std::get<std::string>(row[0])).ok());
                ASSERT_TRUE(second.Append(std::get<std::string>(row[1])).ok());
            }
            const auto a = first.Finish();
            const auto b = second.Finish();
            ASSERT_TRUE(a.ok());
            ASSERT_TRUE(b.ok());
            // Omit n: every input path must account for its implicit NULL.
            const auto batch = arrow::RecordBatch::Make(
                arrow::schema({arrow::field("a", arrow::utf8()), arrow::field("b", arrow::utf8())}),
                rows,
                {*a, *b});
            const auto normalized = NormalizeRows(schema, "00112233-4455-6677-8899-aabbccddeeff", input);
            const auto fromArrow = NormalizeBatches(schema, "00112233-4455-6677-8899-aabbccddeeff", {batch});
            const auto fingerprint = FingerprintBatches(schema, "00112233-4455-6677-8899-aabbccddeeff", {batch});
            const auto reservation = InputReservation(schema, {batch});
            if (extra == 0) {
                ASSERT_TRUE(normalized);
                ASSERT_TRUE(fromArrow);
                ASSERT_TRUE(fingerprint);
                ASSERT_TRUE(reservation);
                EXPECT_EQ(normalized->decoded_bytes, rows == 1 ? 2097152U : 16777216U);
                EXPECT_EQ(fromArrow->payload_hash, normalized->payload_hash);
                EXPECT_EQ(*fingerprint, normalized->payload_hash);
            } else {
                ASSERT_FALSE(normalized);
                ASSERT_FALSE(fromArrow);
                ASSERT_FALSE(fingerprint);
                ASSERT_FALSE(reservation);
                EXPECT_EQ(normalized.error().code, "INPUT_TOO_LARGE");
                EXPECT_EQ(fromArrow.error().code, "INPUT_TOO_LARGE");
                EXPECT_EQ(fingerprint.error().code, "INPUT_TOO_LARGE");
                EXPECT_EQ(reservation.error().code, "INPUT_TOO_LARGE");
            }
        }
    }
}

TEST(ArrowInput, StreamingFingerprintMatchesIndependentAllTypesGolden)
{
    const SchemaSpec schema{{{"s", FieldType::String, false},
        {"b", FieldType::Bool, true},
        {"i", FieldType::Int64, true},
        {"f", FieldType::Float64, true},
        {"t", FieldType::TimestampUsUtc, true}}};
    arrow::StringBuilder strings;
    arrow::BooleanBuilder booleans;
    arrow::Int64Builder integers;
    arrow::DoubleBuilder floats;
    arrow::TimestampBuilder times(arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"), arrow::default_memory_pool());
    ASSERT_TRUE(strings.Append("é").ok());
    ASSERT_TRUE(strings.Append("dup").ok());
    ASSERT_TRUE(strings.Append("dup").ok());
    ASSERT_TRUE(booleans.Append(true).ok());
    ASSERT_TRUE(booleans.Append(false).ok());
    ASSERT_TRUE(booleans.AppendNull().ok());
    ASSERT_TRUE(integers.Append(std::numeric_limits<int64_t>::min()).ok());
    ASSERT_TRUE(integers.Append(std::numeric_limits<int64_t>::max()).ok());
    ASSERT_TRUE(integers.AppendNull().ok());
    ASSERT_TRUE(floats.Append(-0.0).ok());
    ASSERT_TRUE(floats.Append(1.5).ok());
    ASSERT_TRUE(floats.AppendNull().ok());
    ASSERT_TRUE(times.Append(-62135596800000000LL).ok());
    ASSERT_TRUE(times.Append(253402300799999999LL).ok());
    ASSERT_TRUE(times.AppendNull().ok());
    const auto finish = [](auto& builder) {
        const auto array = builder.Finish();
        if (!array.ok()) {
            throw std::runtime_error(array.status().ToString());
        }
        return *array;
    };
    const auto batch =
        arrow::RecordBatch::Make(arrow::schema({arrow::field("s", arrow::utf8(), false),
                                     arrow::field("b", arrow::boolean()),
                                     arrow::field("i", arrow::int64()),
                                     arrow::field("f", arrow::float64()),
                                     arrow::field("t", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"))}),
            3,
            {finish(strings), finish(booleans), finish(integers), finish(floats), finish(times)});
    const auto fingerprint =
        FingerprintBatches(schema, "00112233-4455-6677-8899-aabbccddeeff", {batch->Slice(0, 1), batch->Slice(1, 2)});
    ASSERT_TRUE(fingerprint);
    EXPECT_EQ(*fingerprint, "72b0d7fbd4d8097a8a848de273fbf13cffa462d20be481e87d8c788f64feef1b");
}

TEST(ArrowInput, FingerprintRejectsNonfiniteFloatsAndOutOfRangeTimestamps)
{
    const auto check = [](FieldType type,
                           const std::shared_ptr<arrow::DataType>& arrow_type,
                           const std::shared_ptr<arrow::Array>& array) {
        const auto batch = arrow::RecordBatch::Make(arrow::schema({arrow::field("n", arrow_type)}), 1, {array});
        const auto result =
            FingerprintBatches(SchemaSpec{{{"n", type, false}}}, "00112233-4455-6677-8899-aabbccddeeff", {batch});
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error().retry_hint, RetryHint::FixInput);
    };
    for (const double value : {std::numeric_limits<double>::infinity(), std::numeric_limits<double>::quiet_NaN()}) {
        arrow::DoubleBuilder builder;
        ASSERT_TRUE(builder.Append(value).ok());
        const auto array = builder.Finish();
        ASSERT_TRUE(array.ok());
        check(FieldType::Float64, arrow::float64(), *array);
    }
    const auto type = arrow::timestamp(arrow::TimeUnit::MICRO, "UTC");
    for (const int64_t value : {-62135596800000001LL, 253402300800000000LL}) {
        arrow::TimestampBuilder builder(type, arrow::default_memory_pool());
        ASSERT_TRUE(builder.Append(value).ok());
        const auto array = builder.Finish();
        ASSERT_TRUE(array.ok());
        check(FieldType::TimestampUsUtc, type, *array);
    }
}

} // namespace
} // namespace glamdb::detail
