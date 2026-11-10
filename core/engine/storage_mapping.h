#pragma once

#include "storage/schema.h"
#include "storage/types.h"

#include <memory>
#include <vector>

namespace arrow {
class DataType;
class RecordBatch;
class Schema;
} // namespace arrow

namespace glamdb::detail {

// Field order, names and nullability are preserved; field IDs are 1..N on both sides.
NGlamDB::TSchemaSpec ToStorageSchema(const SchemaSpec& schema);

std::shared_ptr<arrow::DataType> MakeArrowType(FieldType type);

// The single Arrow representation of a fixed collection schema.
std::shared_ptr<arrow::Schema> MakeArrowSchema(const SchemaSpec& schema);

// Builds one owned, immutable batch from normalized rows. Values must already
// satisfy the schema; a mismatch is an internal error, not a cast.
Result<std::shared_ptr<arrow::RecordBatch>> MakeRecordBatch(const SchemaSpec& schema, const std::vector<Row>& rows);

// Upper bound of arrow::util::TotalBufferSize(*MakeRecordBatch(schema, rows))
// computed without building the batch: every column is counted with a
// validity bitmap. Equal when every column contains a NULL.
int64_t EstimateRecordBatchBytes(const SchemaSpec& schema, const std::vector<Row>& rows);

} // namespace glamdb::detail
