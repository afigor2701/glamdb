#include "core/api/canonical.h"

#include "core/api/binary.h"

#include <arrow/api.h>

#include <cmath>
#include <memory>
#include <unordered_map>
#include <vector>

namespace glamdb::detail {
namespace {

// `validated`: Inspect already ran ValidateFull on this batch in the same call.
std::vector<int> MapFields(const SchemaSpec& schema, const arrow::RecordBatch& batch, bool validated = false)
{
    Require(batch.num_columns() <= static_cast<int>(schema.fields.size()) && (validated || batch.ValidateFull().ok()),
        "INVALID_ARGUMENT",
        "invalid Arrow batch");
    std::unordered_map<std::string, int> names;
    for (int i = 0; i < batch.num_columns(); ++i) {
        const auto& name = batch.schema()->field(i)->name();
        Require(names.emplace(name, i).second &&
                std::ranges::any_of(schema.fields,
                    [&](const auto& field) {
                        return field.name == name;
                    }),
            "INVALID_ARGUMENT",
            "unknown or duplicate Arrow field");
    }
    std::vector<int> columns;
    for (const auto& field : schema.fields) {
        const auto found = names.find(field.name);
        if (found == names.end()) {
            Require(field.nullable, "INVALID_ARGUMENT", "required Arrow field is missing");
            columns.push_back(-1);
            continue;
        }
        const auto& type = batch.column(found->second)->type();
        bool matches = false;
        switch (field.type) {
            case FieldType::String:
                matches = type->id() == arrow::Type::STRING;
                break;
            case FieldType::Bool:
                matches = type->id() == arrow::Type::BOOL;
                break;
            case FieldType::Int64:
                matches = type->id() == arrow::Type::INT64;
                break;
            case FieldType::Float64:
                matches = type->id() == arrow::Type::DOUBLE;
                break;
            case FieldType::TimestampUsUtc:
                if (type->id() == arrow::Type::TIMESTAMP) {
                    const auto& timestamp = static_cast<const arrow::TimestampType&>(*type);
                    matches = timestamp.unit() == arrow::TimeUnit::MICRO && timestamp.timezone() == "UTC";
                }
                break;
        }
        Require(matches, "INVALID_ARGUMENT", "Arrow field type does not match fixed schema");
        columns.push_back(found->second);
    }
    return columns;
}

struct Shape
{
    size_t rows = 0;
    size_t decoded_bytes = 0;
    size_t null_slot_bytes = 0;
};

// Boxes each mapped column once per batch: RecordBatch::column() locks and
// copies a shared_ptr on every call, which per-cell loops must not repeat.
std::vector<std::shared_ptr<arrow::Array>> Arrays(const arrow::RecordBatch& batch, const std::vector<int>& columns)
{
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrays.reserve(columns.size());
    for (const int column : columns) {
        arrays.push_back(column < 0 ? nullptr : batch.column(column));
    }
    return arrays;
}

Shape Inspect(const SchemaSpec& schema, const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches)
{
    const auto fingerprint = SchemaFingerprint(schema);
    if (!fingerprint) {
        throw Failure(fingerprint.error());
    }
    Require(!batches.empty() && batches.size() <= 10000, "INVALID_ARGUMENT", "append requires bounded Arrow batches");
    Shape shape;
    for (const auto& batch : batches) {
        Require(batch != nullptr && batch->num_rows() >= 0 && batch->num_rows() <= 10000 &&
                shape.rows + static_cast<size_t>(batch->num_rows()) <= 10000,
            "INVALID_ARGUMENT",
            "append requires at most 10000 rows");
        shape.rows += static_cast<size_t>(batch->num_rows());
        const auto arrays = Arrays(*batch, MapFields(schema, *batch));
        for (int64_t row = 0; row < batch->num_rows(); ++row) {
            size_t bytes = schema.fields.size();
            for (size_t field = 0; field < arrays.size(); ++field) {
                const auto* array = arrays[field].get();
                if (array == nullptr || array->IsNull(row)) {
                    Require(schema.fields[field].nullable, "INVALID_ARGUMENT", "NULL in required Arrow field");
                    const auto type = schema.fields[field].type;
                    shape.null_slot_bytes += type == FieldType::String ? 4 : type == FieldType::Bool ? 1 : 8;
                    continue;
                }
                if (schema.fields[field].type == FieldType::String) {
                    const auto& strings = static_cast<const arrow::StringArray&>(*array);
                    const auto length = strings.value_length(row);
                    Require(length <= (schema.fields[field].name == "key" ? 1024 : 1024 * 1024),
                        "INVALID_ARGUMENT",
                        "oversized Arrow string");
                    bytes += 4 + static_cast<size_t>(length);
                } else {
                    bytes += schema.fields[field].type == FieldType::Bool ? 1 : 8;
                }
            }
            shape.decoded_bytes += bytes;
            Require(bytes <= 2ULL * 1024 * 1024 && shape.decoded_bytes <= 16ULL * 1024 * 1024,
                "INPUT_TOO_LARGE",
                "Arrow input exceeds decoded byte limit");
        }
    }
    Require(shape.rows != 0, "INVALID_ARGUMENT", "empty append is not supported");
    return shape;
}

Cell Read(const arrow::Array& array, int64_t row, FieldType type)
{
    if (array.IsNull(row)) {
        return std::monostate{};
    }
    switch (type) {
        case FieldType::String:
            return std::string(static_cast<const arrow::StringArray&>(array).GetView(row));
        case FieldType::Bool:
            return static_cast<const arrow::BooleanArray&>(array).Value(row);
        case FieldType::Int64:
            return static_cast<const arrow::Int64Array&>(array).Value(row);
        case FieldType::Float64:
            return static_cast<const arrow::DoubleArray&>(array).Value(row);
        case FieldType::TimestampUsUtc:
            return TimestampUs{static_cast<const arrow::TimestampArray&>(array).Value(row)};
    }
    throw Failure({"INVALID_ARGUMENT", "unsupported Arrow field", RetryHint::FixInput});
}

} // namespace

Result<size_t> InputReservation(const SchemaSpec& schema,
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches)
{
    try {
        const auto shape = Inspect(schema, batches);
        // Payload bytes: the owned rows, the canonical string while hashing and
        // the Arrow batch with builder growth (4 copies). STL cells, including
        // tiny/null values whose buffers alone undercount RAM, exist once;
        // row headers twice while the extracted rows are moved into the
        // normalized input. NULL encodings omit typed slots, but Arrow still
        // allocates them; reserve their builder growth separately.
        return shape.decoded_bytes * 4 + shape.null_slot_bytes * 2 +
            shape.rows * (sizeof(Row) * 2 + schema.fields.size() * sizeof(Cell)) + 65536;
    } catch (const Failure& error) {
        auto detail = error.detail;
        detail.retry_hint = RetryHint::FixInput;
        return std::unexpected(std::move(detail));
    }
}

Result<std::string> FingerprintBatches(const SchemaSpec& schema,
    std::string_view collection_id,
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches)
{
    try {
        const auto shape = Inspect(schema, batches);
        const auto header = EncodeAppendHeader(schema, collection_id, shape.rows);
        if (!header) {
            throw Failure(header.error());
        }
        Sha256State hash;
        hash.Update(*header);
        const auto number = [&hash]<class T>(T value) {
            Encoder encoded;
            encoded.Number(value);
            hash.Update(encoded.bytes);
        };
        for (const auto& batch : batches) {
            const auto arrays = Arrays(*batch, MapFields(schema, *batch, true));
            for (int64_t row = 0; row < batch->num_rows(); ++row) {
                for (size_t field = 0; field < arrays.size(); ++field) {
                    if (arrays[field] == nullptr || arrays[field]->IsNull(row)) {
                        number(uint8_t{0});
                        continue;
                    }
                    number(uint8_t{1});
                    const auto& array = *arrays[field];
                    switch (schema.fields[field].type) {
                        case FieldType::String: {
                            const auto value = static_cast<const arrow::StringArray&>(array).GetView(row);
                            Require(ValidUtf8(value), "INVALID_ARGUMENT", "invalid UTF-8 string");
                            number(static_cast<uint32_t>(value.size()));
                            hash.Update(value);
                            break;
                        }
                        case FieldType::Bool:
                            number(static_cast<uint8_t>(
                                static_cast<const arrow::BooleanArray&>(array).Value(row) ? 1 : 0));
                            break;
                        case FieldType::Int64:
                            number(static_cast<const arrow::Int64Array&>(array).Value(row));
                            break;
                        case FieldType::Float64: {
                            auto value = static_cast<const arrow::DoubleArray&>(array).Value(row);
                            Require(std::isfinite(value), "INVALID_ARGUMENT", "non-finite float");
                            if (value == 0) {
                                value = 0.0;
                            }
                            number(std::bit_cast<uint64_t>(value));
                            break;
                        }
                        case FieldType::TimestampUsUtc: {
                            const auto value = static_cast<const arrow::TimestampArray&>(array).Value(row);
                            Require(value >= -62135596800000000LL && value <= 253402300799999999LL,
                                "INVALID_ARGUMENT",
                                "timestamp outside years 0001..9999");
                            number(value);
                            break;
                        }
                    }
                }
            }
        }
        return hash.Digest();
    } catch (const Failure& error) {
        auto detail = error.detail;
        detail.retry_hint = RetryHint::FixInput;
        return std::unexpected(std::move(detail));
    }
}

Result<NormalizedInput> NormalizeBatches(const SchemaSpec& schema,
    std::string_view collection_id,
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches)
{
    try {
        const auto shape = Inspect(schema, batches);
        std::vector<Row> rows;
        rows.reserve(shape.rows);
        for (const auto& batch : batches) {
            const auto arrays = Arrays(*batch, MapFields(schema, *batch, true));
            for (int64_t index = 0; index < batch->num_rows(); ++index) {
                Row row;
                row.reserve(schema.fields.size());
                for (size_t field = 0; field < arrays.size(); ++field) {
                    row.push_back(arrays[field] == nullptr ? Cell(std::monostate{})
                                                           : Read(*arrays[field], index, schema.fields[field].type));
                }
                rows.push_back(std::move(row));
            }
        }
        return NormalizeOwnedRows(schema, collection_id, std::move(rows));
    } catch (const Failure& error) {
        auto detail = error.detail;
        detail.retry_hint = RetryHint::FixInput;
        return std::unexpected(std::move(detail));
    }
}

} // namespace glamdb::detail
