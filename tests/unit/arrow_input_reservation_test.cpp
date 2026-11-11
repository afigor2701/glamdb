// A maximum documented append (10,000 rows, 64 fields, at most 16 MiB of
// canonical input) fits the default owned-input budget on every supported
// standard library.

#include "core/api/canonical.h"
#include "core/engine/storage_mapping.h"

#include <arrow/api.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <string>

namespace glamdb::detail {
namespace {

std::shared_ptr<arrow::Array> Strings(int64_t rows, size_t length, char fill)
{
    arrow::StringBuilder builder;
    EXPECT_TRUE(builder.Reserve(rows).ok());
    EXPECT_TRUE(builder.ReserveData(rows * static_cast<int64_t>(length)).ok());
    std::string value(length, fill);
    for (int64_t row = 0; row < rows; ++row) {
        const auto digits = std::to_string(row);
        std::ranges::copy(digits, value.end() - static_cast<std::ptrdiff_t>(digits.size()));
        EXPECT_TRUE(builder.Append(value).ok());
    }
    std::shared_ptr<arrow::Array> array;
    EXPECT_TRUE(builder.Finish(&array).ok());
    return array;
}

TEST(ArrowInputReservation, NullHeavyReservationCoversOwnedRowsAndWriterAllocation)
{
    SchemaSpec schema;
    for (int column = 0; column < 64; ++column) {
        schema.fields.push_back({"n" + std::to_string(column), FieldType::Int64, true});
    }
    const auto batch = arrow::RecordBatch::Make(arrow::schema({}), 10000, std::vector<std::shared_ptr<arrow::Array>>{});
    const auto reservation = InputReservation(schema, {batch});
    ASSERT_TRUE(reservation);
    const auto normalized = NormalizeBatches(schema, "00112233-4455-6677-8899-aabbccddeeff", {batch});
    ASSERT_TRUE(normalized);
    arrow::ProxyMemoryPool pool(arrow::default_memory_pool());
    const auto writerBatch = MakeRecordBatch(schema, normalized->rows, &pool);
    ASSERT_TRUE(writerBatch);
    size_t ownedBytes = normalized->rows.capacity() * sizeof(Row);
    for (const auto& row : normalized->rows) {
        ownedBytes += row.capacity() * sizeof(Cell);
    }
    // The reservation must at least cover these simultaneous live allocations;
    // the canonical NULL encoding alone does not measure the writer's slots.
    EXPECT_GE(*reservation, ownedBytes + static_cast<size_t>(pool.max_memory()));
    EXPECT_LE(*reservation, static_cast<size_t>(Limits{}.max_owned_input_bytes));
}

TEST(ArrowInputReservation, MaximumWideAppendFitsTheDefaultOwnedInputBudget)
{
    constexpr int64_t Rows = 10000;
    constexpr size_t Fields = 64;
    constexpr size_t KeyBytes = 34;
    constexpr size_t ValueBytes = 21;

    SchemaSpec schema;
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::Array>> columns;
    schema.fields.push_back({"key", FieldType::String, false});
    fields.push_back(arrow::field("key", arrow::utf8(), false));
    columns.push_back(Strings(Rows, KeyBytes, 'k'));
    for (size_t index = 1; index < Fields; ++index) {
        const auto name = "s" + std::to_string(index);
        schema.fields.push_back({name, FieldType::String, true});
        fields.push_back(arrow::field(name, arrow::utf8()));
        columns.push_back(Strings(Rows, ValueBytes, static_cast<char>('a' + index % 26)));
    }
    const auto batch = arrow::RecordBatch::Make(arrow::schema(fields), Rows, columns);

    // Per row: 64 presence bytes + 64 length prefixes + 34 + 63 * 21 value bytes = 1677.
    constexpr size_t Decoded = static_cast<size_t>(Rows) * (Fields * 5 + KeyBytes + (Fields - 1) * ValueBytes);
    static_assert(Decoded <= 16ULL * 1024 * 1024, "within the 16 MiB canonical per-append limit");

    // InputReservation applies the same shape limits as the canonical encoder
    // (rows, key, string, row and total bytes), so success means the append is
    // admissible. Hashing it as well would add nothing but minutes under MSan.
    const auto reservation = InputReservation(schema, {batch});
    ASSERT_TRUE(reservation) << reservation.error().message;
    const auto budget = static_cast<size_t>(Limits{}.max_owned_input_bytes);
    EXPECT_LE(*reservation, budget) << "sizeof(Cell)=" << sizeof(Cell) << " sizeof(Row)=" << sizeof(Row)
                                    << ": a maximum documented append can never be admitted with default Limits";
    // Standard libraries differ in sizeof(Cell): libstdc++ cells are 8 bytes
    // larger than libc++ ones. Charging cells per normalized copy must leave
    // room for that on whichever library this test is built with.
    constexpr size_t CellHeadroom = 16;
    EXPECT_LE(*reservation + static_cast<size_t>(Rows) * Fields * CellHeadroom, budget)
        << "the reservation leaves no room for a standard library with larger cells";
}

} // namespace
} // namespace glamdb::detail
