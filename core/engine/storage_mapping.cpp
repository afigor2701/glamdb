#include "core/engine/storage_mapping.h"

#include "core/api/canonical.h"

#include <arrow/api.h>

#include <cstddef>
#include <string>
#include <type_traits>
#include <variant>

namespace glamdb::detail {
namespace {

static_assert(static_cast<int>(FieldType::String) == static_cast<int>(NGlamDB::EFieldType::String));
static_assert(static_cast<int>(FieldType::Bool) == static_cast<int>(NGlamDB::EFieldType::Bool));
static_assert(static_cast<int>(FieldType::Int64) == static_cast<int>(NGlamDB::EFieldType::Int64));
static_assert(static_cast<int>(FieldType::Float64) == static_cast<int>(NGlamDB::EFieldType::Float64));
static_assert(static_cast<int>(FieldType::TimestampUsUtc) == static_cast<int>(NGlamDB::EFieldType::TimestampUsUtc));

NGlamDB::EFieldType ToStorageType(FieldType type)
{
    switch (type) {
        case FieldType::String:
            return NGlamDB::EFieldType::String;
        case FieldType::Bool:
            return NGlamDB::EFieldType::Bool;
        case FieldType::Int64:
            return NGlamDB::EFieldType::Int64;
        case FieldType::Float64:
            return NGlamDB::EFieldType::Float64;
        case FieldType::TimestampUsUtc:
            return NGlamDB::EFieldType::TimestampUsUtc;
    }
    return NGlamDB::EFieldType{};
}

ErrorDetail ArrowError(const arrow::Status& status)
{
    return {"INTERNAL",
        BoundedDiagnostic("Arrow batch construction failed: " + status.ToString()),
        RetryHint::OperatorAction};
}

ErrorDetail CellMismatch(size_t row, size_t column)
{
    return {"INTERNAL",
        "normalized cell does not match the schema at row " + std::to_string(row) + ", column " +
            std::to_string(column),
        RetryHint::OperatorAction};
}

template <class TBuilder, class TValue>
arrow::Status AppendCell(TBuilder& builder, const Cell& cell, bool& mismatch)
{
    if (std::holds_alternative<std::monostate>(cell)) {
        return builder.AppendNull();
    }
    const auto* value = std::get_if<TValue>(&cell);
    if (value == nullptr) {
        mismatch = true;
        return arrow::Status::OK();
    }
    if constexpr (std::is_same_v<TValue, TimestampUs>) {
        return builder.Append(value->value);
    } else {
        return builder.Append(*value);
    }
}

template <class TBuilder, class TValue>
Result<std::shared_ptr<arrow::Array>>
BuildTypedColumn(const FieldSpec& field, const std::vector<Row>& rows, size_t column)
{
    TBuilder builder(MakeArrowType(field.type), arrow::default_memory_pool());
    if (const auto status = builder.Reserve(static_cast<int64_t>(rows.size())); !status.ok()) {
        return std::unexpected(ArrowError(status));
    }
    // MakeRecordBatch has already checked every row width.
    for (size_t row = 0; row < rows.size(); ++row) {
        const auto& cell = rows[row][column];
        if (!field.nullable && std::holds_alternative<std::monostate>(cell)) {
            return std::unexpected(CellMismatch(row, column));
        }
        bool mismatch = false;
        if (const auto status = AppendCell<TBuilder, TValue>(builder, cell, mismatch); !status.ok()) {
            return std::unexpected(ArrowError(status));
        }
        if (mismatch) {
            return std::unexpected(CellMismatch(row, column));
        }
    }
    std::shared_ptr<arrow::Array> array;
    if (const auto status = builder.Finish(&array); !status.ok()) {
        return std::unexpected(ArrowError(status));
    }
    return array;
}

Result<std::shared_ptr<arrow::Array>> BuildColumn(const FieldSpec& field, const std::vector<Row>& rows, size_t column)
{
    switch (field.type) {
        case FieldType::String:
            return BuildTypedColumn<arrow::StringBuilder, std::string>(field, rows, column);
        case FieldType::Bool:
            return BuildTypedColumn<arrow::BooleanBuilder, bool>(field, rows, column);
        case FieldType::Int64:
            return BuildTypedColumn<arrow::Int64Builder, int64_t>(field, rows, column);
        case FieldType::Float64:
            return BuildTypedColumn<arrow::DoubleBuilder, double>(field, rows, column);
        case FieldType::TimestampUsUtc:
            return BuildTypedColumn<arrow::TimestampBuilder, TimestampUs>(field, rows, column);
    }
    return std::unexpected(CellMismatch(0, column));
}

} // namespace

NGlamDB::TSchemaSpec ToStorageSchema(const SchemaSpec& schema)
{
    NGlamDB::TSchemaSpec result;
    result.Fields.reserve(schema.fields.size());
    for (const auto& field : schema.fields) {
        result.Fields.push_back({field.name, ToStorageType(field.type), field.nullable});
    }
    return result;
}

std::shared_ptr<arrow::DataType> MakeArrowType(FieldType type)
{
    switch (type) {
        case FieldType::String:
            return arrow::utf8();
        case FieldType::Bool:
            return arrow::boolean();
        case FieldType::Int64:
            return arrow::int64();
        case FieldType::Float64:
            return arrow::float64();
        case FieldType::TimestampUsUtc:
            return arrow::timestamp(arrow::TimeUnit::MICRO, "UTC");
    }
    return {};
}

std::shared_ptr<arrow::Schema> MakeArrowSchema(const SchemaSpec& schema)
{
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(schema.fields.size());
    for (const auto& field : schema.fields) {
        fields.push_back(arrow::field(field.name, MakeArrowType(field.type), field.nullable));
    }
    return arrow::schema(std::move(fields));
}

Result<std::shared_ptr<arrow::RecordBatch>> MakeRecordBatch(const SchemaSpec& schema, const std::vector<Row>& rows)
{
    for (size_t row = 0; row < rows.size(); ++row) {
        if (rows[row].size() != schema.fields.size()) {
            return std::unexpected(CellMismatch(row, rows[row].size()));
        }
    }
    std::vector<std::shared_ptr<arrow::Array>> columns;
    columns.reserve(schema.fields.size());
    for (size_t column = 0; column < schema.fields.size(); ++column) {
        auto array = BuildColumn(schema.fields[column], rows, column);
        if (!array) {
            return std::unexpected(array.error());
        }
        columns.push_back(std::move(*array));
    }
    return arrow::RecordBatch::Make(MakeArrowSchema(schema), static_cast<int64_t>(rows.size()), std::move(columns));
}

int64_t EstimateRecordBatchBytes(const SchemaSpec& schema, const std::vector<Row>& rows)
{
    const auto count = static_cast<int64_t>(rows.size());
    const auto bitmap = (count + 7) / 8;
    int64_t total = 0;
    for (size_t column = 0; column < schema.fields.size(); ++column) {
        total += bitmap;
        switch (schema.fields[column].type) {
            case FieldType::String:
                total += 4 * (count + 1);
                for (const auto& row : rows) {
                    if (const auto* text = column < row.size() ? std::get_if<std::string>(&row[column]) : nullptr) {
                        total += static_cast<int64_t>(text->size());
                    }
                }
                break;
            case FieldType::Bool:
                total += bitmap;
                break;
            case FieldType::Int64:
            case FieldType::Float64:
            case FieldType::TimestampUsUtc:
                total += 8 * count;
                break;
        }
    }
    return total;
}

} // namespace glamdb::detail
