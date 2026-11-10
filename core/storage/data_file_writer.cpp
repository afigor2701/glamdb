#include "core/storage/data_file_writer.h"

#include "core/storage/format_registry.h"
#include "core/storage/format_verifier.h"
#include "core/storage/iceberg_error_boundary.h"

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/byte_size.h>
#include <iceberg/file_io.h>
#include <iceberg/file_writer.h>
#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/metrics_config.h>
#include <iceberg/partition_spec.h>
#include <iceberg/schema.h>
#include <iceberg/table_metadata.h>
#include <iceberg/type.h>
#include <iceberg/util/macros.h>

#include <algorithm>
#include <cmath>
#include <span>
#include <utility>

namespace NGlamDB::NStorage {
using iceberg::Error;

namespace {

struct TInputShape
{
    std::int64_t Rows = 0;
    std::int64_t RowGroupRows = 0;
};

bool MatchesType(const iceberg::Type& expected, const arrow::DataType& actual)
{
    switch (expected.type_id()) {
        case iceberg::TypeId::kString:
            return actual.id() == arrow::Type::STRING;
        case iceberg::TypeId::kBoolean:
            return actual.id() == arrow::Type::BOOL;
        case iceberg::TypeId::kLong:
            return actual.id() == arrow::Type::INT64;
        case iceberg::TypeId::kDouble:
            return actual.id() == arrow::Type::DOUBLE;
        case iceberg::TypeId::kTimestampTz:
            return actual.id() == arrow::Type::TIMESTAMP &&
                static_cast<const arrow::TimestampType&>(actual).unit() == arrow::TimeUnit::MICRO &&
                static_cast<const arrow::TimestampType&>(actual).timezone() == "UTC";
        default:
            return false;
    }
}

iceberg::Result<std::int64_t>
CheckRow(const arrow::RecordBatch& batch, std::span<const iceberg::SchemaField> fields, std::int64_t row)
{
    std::int64_t size = 0;
    for (std::size_t column = 0; column < fields.size(); ++column) {
        const auto& array = *batch.column(static_cast<int>(column));
        size += 1 + (array.type_id() == arrow::Type::STRING ? 4 : array.type_id() == arrow::Type::BOOL ? 1 : 8);
        if (array.IsNull(row)) {
            continue;
        }
        switch (array.type_id()) {
            case arrow::Type::STRING: {
                const auto value = static_cast<const arrow::StringArray&>(array).GetView(row);
                const auto maxLength = fields[column].name() == "key" ? 1024U : (1U << 20);
                if (value.size() > maxLength) {
                    return iceberg::InvalidArgument("String value exceeds its byte limit.");
                }
                size += static_cast<std::int64_t>(value.size());
                break;
            }
            case arrow::Type::BOOL:
                break;
            case arrow::Type::DOUBLE:
                if (!std::isfinite(static_cast<const arrow::DoubleArray&>(array).Value(row))) {
                    return iceberg::InvalidArgument("Floating-point values must be finite.");
                }
                break;
            case arrow::Type::TIMESTAMP: {
                const auto value = static_cast<const arrow::TimestampArray&>(array).Value(row);
                if (value < -62135596800000000LL || value > 253402300799999999LL) {
                    return iceberg::InvalidArgument("Timestamp is outside the supported UTC range.");
                }
                break;
            }
            default:
                break;
        }
    }
    if (size > (2LL << 20)) {
        return iceberg::InvalidArgument("Row exceeds its byte limit.");
    }
    return size;
}

iceberg::Result<TInputShape> CheckBatches(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
    const iceberg::Schema& schema,
    const TDataWriterLimits& limits)
{
    if (limits.MaxInputBytes <= 0 || limits.MaxRows <= 0 || limits.MaxRowGroupBytes <= 0 || limits.MaxFileBytes <= 0) {
        return iceberg::InvalidArgument("Data writer limits must be positive.");
    }
    TInputShape shape;
    std::int64_t inputBytes = 0;
    std::int64_t maxRowBytes = 1;
    const auto fields = schema.fields();
    if (fields.empty() || fields.size() > 64) {
        return iceberg::InvalidArgument("Data schema must contain 1..64 fields.");
    }
    for (const auto& batch : batches) {
        if (!batch || batch->num_rows() < 0 || batch->num_rows() > limits.MaxRows - shape.Rows ||
            batch->num_columns() != static_cast<int>(fields.size()))
        {
            return iceberg::InvalidArgument("Invalid batch shape or row limit exceeded.");
        }
        const auto valid = batch->ValidateFull();
        if (!valid.ok()) {
            return iceberg::InvalidArgument(valid.ToString());
        }
        const auto bytes = arrow::util::TotalBufferSize(*batch);
        if (bytes < 0 || bytes > limits.MaxInputBytes - inputBytes) {
            return iceberg::InvalidArgument("Input buffers exceed their byte limit.");
        }
        inputBytes += bytes;
        shape.Rows += batch->num_rows();
        for (std::size_t column = 0; column < fields.size(); ++column) {
            const auto index = static_cast<int>(column);
            if (batch->column_name(index) != fields[column].name() ||
                !MatchesType(*fields[column].type(), *batch->column(index)->type()) ||
                (!fields[column].optional() && batch->column(index)->null_count() != 0))
            {
                return iceberg::InvalidArgument("Batch field does not match the table schema.");
            }
        }
        for (std::int64_t row = 0; row < batch->num_rows(); ++row) {
            ICEBERG_ASSIGN_OR_RAISE(auto rowBytes, CheckRow(*batch, fields, row));
            maxRowBytes = std::max(maxRowBytes, rowBytes);
        }
    }
    const auto offsetBytes = 4 * static_cast<std::int64_t>(std::ranges::count_if(fields, [](const auto& field) {
        return field.type()->type_id() == iceberg::TypeId::kString;
    }));
    if (shape.Rows == 0 || limits.MaxRowGroupBytes < offsetBytes || maxRowBytes > limits.MaxRowGroupBytes - offsetBytes)
    {
        return iceberg::InvalidArgument("Input is empty or a row exceeds the row group byte limit.");
    }
    shape.RowGroupRows = std::min(shape.Rows, (limits.MaxRowGroupBytes - offsetBytes) / maxRowBytes);
    return shape;
}

iceberg::Status WriteBatches(iceberg::Writer& writer,
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
    const std::shared_ptr<NIO::TIOAttempt>& attempt,
    std::int64_t maxFileBytes)
{
    for (const auto& batch : batches) {
        if (auto error = attempt->FirstError()) {
            return iceberg::IOError(error.message());
        }
        if (batch->num_rows() == 0) {
            continue;
        }
        struct TExportedArray
        {
            ArrowArray Array{};
            ~TExportedArray()
            {
                if (Array.release != nullptr) {
                    Array.release(&Array);
                }
            }
        } exported;
        auto status = arrow::ExportRecordBatch(*batch, &exported.Array);
        if (!status.ok()) {
            return iceberg::InvalidArrowData(status.ToString());
        }
        ICEBERG_RETURN_UNEXPECTED(writer.Write(&exported.Array));
        if (auto error = attempt->FirstError()) {
            return iceberg::IOError(error.message());
        }
        ICEBERG_ASSIGN_OR_RAISE(auto length, writer.length());
        if (length < 0 || length > maxFileBytes) {
            return iceberg::InvalidArgument("CAPACITY: Parquet file exceeds its byte limit.");
        }
    }
    return {};
}

iceberg::Result<std::shared_ptr<iceberg::DataFile>> MakeDataFile(iceberg::Writer& writer,
    const iceberg::TableMetadata& metadata,
    const std::string& path,
    const TInputShape& shape,
    const NIO::TDurableFileReceipt& receipt)
{
    ICEBERG_ASSIGN_OR_RAISE(auto metrics, writer.metrics());
    if (!metrics.row_count || *metrics.row_count != shape.Rows) {
        return iceberg::Invalid("Writer metrics do not match the input row count.");
    }
    auto result = std::make_shared<iceberg::DataFile>();
    result->file_path = path;
    result->record_count = shape.Rows;
    result->file_size_in_bytes = static_cast<std::int64_t>(receipt.Length);
    result->partition_spec_id = metadata.default_spec_id;
    result->sort_order_id = metadata.default_sort_order_id;
    result->column_sizes = {metrics.column_sizes.begin(), metrics.column_sizes.end()};
    result->value_counts = {metrics.value_counts.begin(), metrics.value_counts.end()};
    result->null_value_counts = {metrics.null_value_counts.begin(), metrics.null_value_counts.end()};
    result->nan_value_counts = {metrics.nan_value_counts.begin(), metrics.nan_value_counts.end()};
    for (const auto& [id, literal] : metrics.lower_bounds) {
        ICEBERG_ASSIGN_OR_RAISE(result->lower_bounds[id], literal.Serialize());
    }
    for (const auto& [id, literal] : metrics.upper_bounds) {
        ICEBERG_ASSIGN_OR_RAISE(result->upper_bounds[id], literal.Serialize());
    }
    result->split_offsets = writer.split_offsets();
    return result;
}

}

iceberg::Result<TDataWriteResult> WriteDataFile(std::shared_ptr<iceberg::FileIO> fileIO,
    std::shared_ptr<NIO::TIOAttempt> attempt,
    const iceberg::TableMetadata& metadata,
    std::string path,
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches,
    TDataWriterLimits limits)
{
    auto result = CatchIcebergExceptions([&]() -> iceberg::Result<TDataWriteResult> {
        if (!fileIO || !attempt || metadata.format_version != 2) {
            return iceberg::InvalidArgument("Data writes require FileIO, an attempt, and a v2 table.");
        }
        ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());
        ICEBERG_ASSIGN_OR_RAISE(auto spec, metadata.PartitionSpec());
        if (!spec->IsUnpartitioned() || metadata.default_sort_order_id != 0) {
            return iceberg::InvalidArgument("Data writes require an unpartitioned, unsorted table.");
        }
        ICEBERG_ASSIGN_OR_RAISE(auto shape, CheckBatches(batches, *schema, limits));
        if (auto error = attempt->DeclareDataFile(path)) {
            return iceberg::InvalidArgument(error.message());
        }
        RegisterStorageFormats();
        iceberg::WriterOptions options;
        options.path = path;
        options.io = fileIO;
        options.schema = schema;
        ICEBERG_ASSIGN_OR_RAISE(options.metrics_config, iceberg::MetricsConfig::Make(metadata.properties.configs()));
        options.properties = iceberg::WriterProperties::FromMap(
            {{"write.parquet.max-row-group-rows", std::to_string(shape.RowGroupRows)}});
        ICEBERG_ASSIGN_OR_RAISE(auto writer,
            iceberg::WriterFactoryRegistry::Open(iceberg::FileFormatType::kParquet, options));
        auto written = CatchIcebergExceptions([&] {
            return WriteBatches(*writer, batches, attempt, limits.MaxFileBytes);
        });
        if (!written) {
            attempt->FailFile(path, std::make_error_code(std::errc::io_error));
        }
        auto closed = CatchIcebergExceptions([&] {
            return writer->Close();
        });
        ICEBERG_RETURN_UNEXPECTED(written);
        if (auto error = attempt->FirstError()) {
            return iceberg::IOError(error.message());
        }
        ICEBERG_RETURN_UNEXPECTED(closed);
        const auto record = attempt->Record(path);
        if (!record || !record.Value().Receipt) {
            return iceberg::Invalid("Parquet file has no valid durable receipt.");
        }
        if (record.Value().Receipt->Length > static_cast<std::uint64_t>(limits.MaxFileBytes)) {
            return iceberg::InvalidArgument("CAPACITY: Parquet file exceeds its byte limit.");
        }
        const auto& receipt = *record.Value().Receipt;
        ICEBERG_ASSIGN_OR_RAISE(auto dataFile, MakeDataFile(*writer, metadata, path, shape, receipt));
        ICEBERG_ASSIGN_OR_RAISE(auto input, fileIO->NewInputFile(path));
        ICEBERG_ASSIGN_OR_RAISE(auto verified,
            CheckParquet(*input,
                *schema,
                {static_cast<std::int64_t>(receipt.Length), shape.Rows},
                {limits.MaxFileBytes}));
        if (auto error = attempt->MarkFormatVerified(path)) {
            return iceberg::IOError(error.message());
        }
        return TDataWriteResult{std::move(dataFile), receipt, verified.FooterBytes};
    });
    if (!result && attempt) {
        attempt->FailFile(path, std::make_error_code(std::errc::io_error));
    }
    return result;
}

}
