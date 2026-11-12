#pragma once

#include <iceberg/result.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

namespace iceberg {
class FileIO;
class InputFile;
class Schema;
class SeekableInputStream;
struct TableMetadata;
}

namespace parquet {
class FileMetaData;
}

namespace NGlamDB::NStorage {

struct TParquetLimits
{
    std::int64_t MaxFileBytes = 32LL << 20;
    std::uint32_t MaxFooterBytes = 32U << 20;
    std::int32_t MaxThriftContainerElements = 16384;
};

struct TParquetExpectation
{
    std::int64_t Length;
    std::int64_t RowCount;
};

struct TAvroLimits
{
    std::int64_t MaxBytes = 8LL << 20;
    std::int64_t MaxRecords = 16384;
    std::int64_t MaxDecodedBytes = 8LL << 20;
};

struct TParquetFileInfo
{
    std::uint32_t FooterBytes;
    std::int64_t RowCount;
    std::int32_t RowGroupCount;
};

struct TAvroContainerInfo
{
    std::int64_t RecordCount = 0;
    std::int64_t BlockCount = 0;
};

iceberg::Result<TParquetFileInfo> CheckParquet(iceberg::InputFile& file,
    const iceberg::Schema& schema,
    TParquetExpectation expected,
    TParquetLimits limits = {});

iceberg::Result<std::shared_ptr<parquet::FileMetaData>> ReadParquetFooter(iceberg::InputFile& file,
    const iceberg::Schema& schema,
    TParquetExpectation expected,
    TParquetLimits limits = {});

iceberg::Result<std::shared_ptr<parquet::FileMetaData>> ReadParquetFooterFromStream(
    iceberg::SeekableInputStream& stream,
    std::int64_t actualSize,
    const iceberg::Schema& schema,
    TParquetExpectation expected,
    TParquetLimits limits = {},
    std::uint64_t* bytesReadOut = nullptr);

// Checks OCF framing and bounded decoding; manifest semantics require the full Iceberg reader.
iceberg::Result<TAvroContainerInfo>
CheckAvroContainer(iceberg::InputFile& file, std::int64_t expectedLength, TAvroLimits limits = {});

iceberg::Result<std::unique_ptr<iceberg::TableMetadata>> ReadTableMetadata(iceberg::FileIO& io,
    const std::string& path,
    std::optional<std::string_view> expectedBytes = std::nullopt,
    std::int64_t maxBytes = 16LL << 20);

iceberg::Result<std::unique_ptr<iceberg::TableMetadata>> ReadTableMetadataFromInput(iceberg::InputFile& file,
    const std::string& path,
    std::int64_t actualSize,
    std::int64_t maxBytes = 16LL << 20);

// The exact bytes iceberg::TableMetadataUtil::Write writes for `metadata`.
iceberg::Result<std::string> SerializeTableMetadata(const iceberg::TableMetadata& metadata);

// Parses root bytes with the same depth bound and parser as ReadTableMetadata.
iceberg::Result<std::unique_ptr<iceberg::TableMetadata>> ParseTableMetadata(std::string bytes);

// Reads a durable root and requires exactly `expectedBytes` within the same
// size and nesting bounds as ReadTableMetadata, without parsing it.
iceberg::Status VerifyTableMetadataBytes(iceberg::FileIO& io,
    const std::string& path,
    std::string_view expectedBytes,
    std::int64_t maxBytes = 16LL << 20);

// Number of root JSON parses (ReadTableMetadata and ParseTableMetadata) in
// this process; cost tests compare it around an operation.
std::uint64_t TableMetadataParses() noexcept;
}
