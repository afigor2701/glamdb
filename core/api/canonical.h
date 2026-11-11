#pragma once

#include "storage/types.h"

#include <array>
#include <string_view>

namespace glamdb::detail {

struct NormalizedInput
{
    std::vector<Row> rows;
    std::string payload_hash;
    size_t decoded_bytes = 0;
    // Built during backend admission and retained until this append finishes.
    std::shared_ptr<arrow::RecordBatch> writer_batch;
};

// Fingerprinting uses fixed-size state, independent of owned payload admission.
class Sha256State
{
public:
    void Update(std::string_view bytes);
    std::string Digest() const;

private:
    void Transform();
    std::array<uint32_t, 8>
        hash_{0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19};
    std::array<char, 64> block_{};
    size_t used_ = 0;
    uint64_t bytes_ = 0;
};

bool ValidUuid(std::string_view value);
bool ValidUtf8(std::string_view value);
std::string BoundedDiagnostic(std::string_view message);
bool ValidDigest(std::string_view value);
std::string Sha256(std::string_view input);
Result<std::string> EncodeAppendHeader(const SchemaSpec& schema, std::string_view collection_id, size_t rows);
Result<std::string> EncodeSchema(const SchemaSpec& schema);
Result<SchemaSpec> DecodeSchema(std::string_view bytes);
Result<std::string> SchemaFingerprint(const SchemaSpec& schema);
Result<NormalizedInput>
NormalizeRows(const SchemaSpec& schema, std::string_view collection_id, const std::vector<Row>& rows);
// Normalizes rows the caller already owns by moving them: no second copy.
Result<NormalizedInput>
NormalizeOwnedRows(const SchemaSpec& schema, std::string_view collection_id, std::vector<Row>&& rows);
Result<NormalizedInput> NormalizeBatches(const SchemaSpec& schema,
    std::string_view collection_id,
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches);
Result<std::string> FingerprintBatches(const SchemaSpec& schema,
    std::string_view collection_id,
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches);
Result<size_t> InputReservation(const SchemaSpec& schema,
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches);

} // namespace glamdb::detail
