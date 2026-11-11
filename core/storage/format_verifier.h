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

// Checks OCF framing and bounded decoding; manifest semantics require the full Iceberg reader.
iceberg::Result<TAvroContainerInfo>
CheckAvroContainer(iceberg::InputFile& file, std::int64_t expectedLength, TAvroLimits limits = {});

iceberg::Result<std::unique_ptr<iceberg::TableMetadata>> ReadTableMetadata(iceberg::FileIO& io,
    const std::string& path,
    std::optional<std::string_view> expectedBytes = std::nullopt,
    std::int64_t maxBytes = 16LL << 20);

}
