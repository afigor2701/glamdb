#include "core/engine/storage_mapping.h"

#include <arrow/api.h>
#include <arrow/util/byte_size.h>
#include <gtest/gtest.h>

namespace glamdb::detail {
namespace {

const SchemaSpec AllTypes{{
    {"key", FieldType::String, false},
    {"flag", FieldType::Bool, true},
    {"count", FieldType::Int64, true},
    {"score", FieldType::Float64, true},
    {"at", FieldType::TimestampUsUtc, true},
}};

TEST(StorageMapping, PreservesFieldOrderTypesAndNullability)
{
    const auto storage = ToStorageSchema(AllTypes);
    ASSERT_EQ(storage.Fields.size(), AllTypes.fields.size());
    EXPECT_EQ(storage.Fields[0].Name, "key");
    EXPECT_EQ(storage.Fields[0].Type, NGlamDB::EFieldType::String);
    EXPECT_FALSE(storage.Fields[0].Nullable);
    EXPECT_EQ(storage.Fields[1].Type, NGlamDB::EFieldType::Bool);
    EXPECT_EQ(storage.Fields[2].Type, NGlamDB::EFieldType::Int64);
    EXPECT_EQ(storage.Fields[3].Type, NGlamDB::EFieldType::Float64);
    EXPECT_EQ(storage.Fields[4].Type, NGlamDB::EFieldType::TimestampUsUtc);
    EXPECT_TRUE(storage.Fields[4].Nullable);
}

TEST(StorageMapping, ArrowSchemaMatchesTheFixedSchema)
{
    const auto schema = MakeArrowSchema(AllTypes);
    const auto expected = arrow::schema({
        arrow::field("key", arrow::utf8(), false),
        arrow::field("flag", arrow::boolean(), true),
        arrow::field("count", arrow::int64(), true),
        arrow::field("score", arrow::float64(), true),
        arrow::field("at", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"), true),
    });
    EXPECT_TRUE(schema->Equals(*expected)) << schema->ToString();
}

TEST(StorageMapping, BuildsOwnedBatchWithNullsAndBoundaries)
{
    const std::vector<Row> rows{
        {std::string("duplicate"), true, int64_t{9007199254740993LL}, 0.0, TimestampUs{-62135596800000000LL}},
        {std::string("duplicate"), std::monostate{}, std::monostate{}, std::monostate{}, std::monostate{}},
        {std::string("\xD0\xBA\xD0\xBB\xD1\x8E\xD1\x87"), false, int64_t{-1}, -2.5, TimestampUs{253402300799999999LL}},
    };
    const auto batch = MakeRecordBatch(AllTypes, rows);
    ASSERT_TRUE(batch) << batch.error().message;
    ASSERT_EQ((*batch)->num_rows(), 3);
    EXPECT_TRUE((*batch)->schema()->Equals(*MakeArrowSchema(AllTypes)));

    const auto& keys = static_cast<const arrow::StringArray&>(*(*batch)->column(0));
    EXPECT_EQ(keys.GetView(0), "duplicate");
    EXPECT_EQ(keys.GetView(1), "duplicate");
    EXPECT_EQ(keys.GetView(2), "\xD0\xBA\xD0\xBB\xD1\x8E\xD1\x87");
    const auto& counts = static_cast<const arrow::Int64Array&>(*(*batch)->column(2));
    EXPECT_EQ(counts.Value(0), 9007199254740993LL);
    EXPECT_TRUE(counts.IsNull(1));
    const auto& times = static_cast<const arrow::TimestampArray&>(*(*batch)->column(4));
    EXPECT_EQ(times.Value(0), -62135596800000000LL);
    EXPECT_EQ(times.Value(2), 253402300799999999LL);
    for (int column = 1; column < 5; ++column) {
        EXPECT_TRUE((*batch)->column(column)->IsNull(1)) << column;
    }
    EXPECT_TRUE((*batch)->ValidateFull().ok());
}

TEST(StorageMapping, RejectsCellsThatDoNotMatchTheSchema)
{
    const SchemaSpec schema{{{"key", FieldType::String, false}, {"count", FieldType::Int64, true}}};
    EXPECT_FALSE(MakeRecordBatch(schema, {{std::string("a"), 1.5}}));
    EXPECT_FALSE(MakeRecordBatch(schema, {{std::monostate{}, int64_t{1}}}));
    EXPECT_FALSE(MakeRecordBatch(schema, {{std::string("a")}}));
    const auto empty = MakeRecordBatch(schema, {});
    ASSERT_TRUE(empty);
    EXPECT_EQ((*empty)->num_rows(), 0);
}

// The writer preflight uses the estimate instead of building a batch; it must
// never be below the buffers WriteDataFile later receives.
TEST(StorageMapping, RecordBatchByteEstimateBoundsArrowBuffers)
{
    const auto check = [](const std::vector<Row>& rows) {
        const auto batch = MakeRecordBatch(AllTypes, rows);
        ASSERT_TRUE(batch) << batch.error().message;
        const auto actual = arrow::util::TotalBufferSize(**batch);
        const auto estimate = EstimateRecordBatchBytes(AllTypes, rows);
        EXPECT_GE(estimate, actual);
        // Only absent validity bitmaps may separate them.
        const auto bitmap = (static_cast<int64_t>(rows.size()) + 7) / 8;
        EXPECT_LE(estimate - actual, static_cast<int64_t>(AllTypes.fields.size()) * bitmap);
    };
    const Row full{std::string("k"), true, int64_t{1}, 1.5, TimestampUs{1}};
    const Row nulls{std::string(""), std::monostate{}, std::monostate{}, std::monostate{}, std::monostate{}};
    check({full});
    check({nulls});
    check({full, nulls, full});
    check({Row{std::string(1U << 20, 'x'), false, int64_t{-1}, -2.0, TimestampUs{-5}}});
    std::vector<Row> many;
    for (int index = 0; index < 10000; ++index) {
        many.push_back(index % 3 == 0
                ? nulls
                : Row{std::string(index % 17, 'a'), index % 2 == 0, int64_t{index}, 0.5 * index, TimestampUs{index}});
    }
    check(many);
    std::vector<Row> dense(10000, full);
    check(dense);
}

} // namespace
} // namespace glamdb::detail
