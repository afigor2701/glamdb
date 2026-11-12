#include "core/storage/format_verifier.h"

#include "core/storage/iceberg_error_boundary.h"

#include <arrow/util/utf8.h>
#include <iceberg/file_io.h>
#include <iceberg/schema.h>
#include <iceberg/table_metadata.h>
#include <iceberg/type.h>
#include <iceberg/util/macros.h>
#include <parquet/exception.h>
#include <parquet/metadata.h>
#include <parquet/properties.h>
#include <parquet/schema.h>
#include <zlib.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <limits>
#include <span>
#include <unordered_set>
#include <utility>
#include <vector>

namespace NGlamDB::NStorage {
using iceberg::Error;

namespace {

template <typename T, class Function>
iceberg::Result<T> WithStream(iceberg::InputFile& file, Function&& function)
{
    ICEBERG_ASSIGN_OR_RAISE(auto stream, file.Open());
    auto result = CatchIcebergExceptions([&] {
        return function(*stream);
    });
    auto closed = CatchIcebergExceptions([&] {
        return stream->Close();
    });
    if (!result) {
        return std::unexpected(std::move(result.error()));
    }
    ICEBERG_RETURN_UNEXPECTED(closed);
    return result;
}

iceberg::Result<std::int64_t>
CheckSize(iceberg::InputFile& file, std::optional<std::int64_t> expectedLength, std::int64_t maxBytes)
{
    if (maxBytes < 0 || (expectedLength && *expectedLength < 0)) {
        return iceberg::InvalidArgument("File size limits must be nonnegative.");
    }
    ICEBERG_ASSIGN_OR_RAISE(auto size, file.Size());
    if (size < 0 || (expectedLength && size != *expectedLength)) {
        return iceberg::Invalid("CORRUPTION: File length differs from its expected length.");
    }
    if (size > maxBytes) {
        return iceberg::InvalidArgument("File exceeds its byte limit.");
    }
    return size;
}

iceberg::Result<std::string>
ReadBounded(iceberg::InputFile& file, std::optional<std::int64_t> expectedLength, std::int64_t maxBytes)
{
    ICEBERG_ASSIGN_OR_RAISE(auto size, CheckSize(file, expectedLength, maxBytes));
    return WithStream<std::string>(file, [&](iceberg::SeekableInputStream& stream) -> iceberg::Result<std::string> {
        std::string bytes(static_cast<std::size_t>(size), '\0');
        ICEBERG_RETURN_UNEXPECTED(stream.ReadFully(0, std::as_writable_bytes(std::span(bytes))));
        return bytes;
    });
}

bool MatchesType(const parquet::schema::Node& node, const iceberg::Type& type)
{
    if (!node.is_primitive()) {
        return false;
    }
    auto logical = parquet::LogicalType::None();
    auto physical = parquet::Type::UNDEFINED;
    switch (type.type_id()) {
        case iceberg::TypeId::kBoolean:
            physical = parquet::Type::BOOLEAN;
            break;
        case iceberg::TypeId::kLong:
            physical = parquet::Type::INT64;
            break;
        case iceberg::TypeId::kDouble:
            physical = parquet::Type::DOUBLE;
            break;
        case iceberg::TypeId::kString:
            physical = parquet::Type::BYTE_ARRAY;
            logical = parquet::LogicalType::String();
            break;
        case iceberg::TypeId::kTimestampTz:
            physical = parquet::Type::INT64;
            logical = parquet::LogicalType::Timestamp(true, parquet::LogicalType::TimeUnit::MICROS);
            break;
        default:
            return false;
    }
    return static_cast<const parquet::schema::PrimitiveNode&>(node).physical_type() == physical &&
        node.logical_type()->Equals(*logical);
}

iceberg::Result<std::shared_ptr<parquet::FileMetaData>> ParseFooter(std::span<const std::byte> bytes,
    std::int64_t expectedRows,
    const iceberg::Schema& schema,
    const TParquetLimits& limits)
{
    auto parsedLength = static_cast<std::uint32_t>(bytes.size());
    parquet::ReaderProperties properties;
    properties.set_thrift_string_size_limit(static_cast<std::int32_t>(bytes.size()));
    properties.set_thrift_container_size_limit(limits.MaxThriftContainerElements);
    try {
        const auto metadata = parquet::FileMetaData::Make(bytes.data(), &parsedLength, properties);
        if (metadata->is_encryption_algorithm_set()) {
            return iceberg::Invalid("CORRUPTION: Encrypted Parquet files are not supported.");
        }
        const auto fields = schema.fields();
        if (parsedLength != bytes.size() || metadata->num_rows() != expectedRows ||
            metadata->num_columns() != static_cast<int>(fields.size()) ||
            metadata->schema()->group_node()->field_count() != static_cast<int>(fields.size()))
        {
            return iceberg::Invalid("CORRUPTION: Parquet footer does not match the "
                                    "expected rows or schema.");
        }
        for (std::size_t index = 0; index < fields.size(); ++index) {
            const auto& node = metadata->schema()->group_node()->field(static_cast<int>(index));
            if (node->is_repeated() || !MatchesType(*node, *fields[index].type()) ||
                node->field_id() != fields[index].field_id() || node->name() != fields[index].name() ||
                node->is_optional() != fields[index].optional())
            {
                return iceberg::Invalid("CORRUPTION: Parquet field does not match its schema.");
            }
        }
        std::int64_t rowCount = 0;
        for (int index = 0; index < metadata->num_row_groups(); ++index) {
            const auto group = metadata->RowGroup(index);
            const auto rows = group->num_rows();
            if (rows < 0 || rows > expectedRows - rowCount || group->num_columns() != metadata->num_columns()) {
                return iceberg::Invalid("CORRUPTION: Invalid Parquet row group count.");
            }
            for (int column = 0; column < group->num_columns(); ++column) {
                const auto chunk = group->ColumnChunk(column);
                const auto* descriptor = metadata->schema()->Column(column);
                if (chunk->type() != descriptor->physical_type() ||
                    chunk->path_in_schema()->ToDotVector() != descriptor->path()->ToDotVector())
                {
                    return iceberg::Invalid("CORRUPTION: Parquet column chunk does not match its schema.");
                }
            }
            rowCount += rows;
        }
        if (rowCount != expectedRows) {
            return iceberg::Invalid("CORRUPTION: Parquet row groups do not match the total row count.");
        }
        return metadata;
    } catch (const parquet::ParquetException& error) {
        return iceberg::Invalid("CORRUPTION: Invalid Parquet footer: {}", error.what());
    }
}

class TAvroCursor
{
public:
    explicit TAvroCursor(std::string_view bytes)
        : Bytes_(bytes)
    { }

    std::size_t GetRemaining() const
    {
        return Bytes_.size();
    }

    iceberg::Result<std::string_view> Take(std::size_t count)
    {
        if (count > Bytes_.size()) {
            return iceberg::Invalid("CORRUPTION: Truncated Avro container.");
        }
        const auto result = Bytes_.substr(0, count);
        Bytes_.remove_prefix(count);
        return result;
    }

    iceberg::Result<std::int64_t> ReadLong()
    {
        std::uint64_t value = 0;
        for (unsigned shift = 0; shift < 70; shift += 7) {
            ICEBERG_ASSIGN_OR_RAISE(auto byte, Take(1));
            const auto current = static_cast<unsigned char>(byte[0]);
            if (shift == 63 && current > 1) {
                return iceberg::Invalid("CORRUPTION: Overflowing Avro integer.");
            }
            value |= static_cast<std::uint64_t>(current & 0x7f) << shift;
            if ((current & 0x80) == 0) {
                return static_cast<std::int64_t>(value >> 1) ^ -static_cast<std::int64_t>(value & 1);
            }
        }
        return iceberg::Invalid("CORRUPTION: Invalid Avro integer.");
    }

    iceberg::Result<std::string_view> ReadBytes()
    {
        ICEBERG_ASSIGN_OR_RAISE(auto count, ReadLong());
        if (count < 0) {
            return iceberg::Invalid("CORRUPTION: Negative Avro byte count.");
        }
        return Take(static_cast<std::size_t>(count));
    }

private:
    std::string_view Bytes_;
};

enum class EAvroCodec : std::uint8_t { Null, Deflate };

iceberg::Result<EAvroCodec> ReadAvroMetadata(TAvroCursor& cursor)
{
    std::unordered_set<std::string_view> keys;
    bool hasSchema = false;
    auto codec = EAvroCodec::Null;
    arrow::util::InitializeUTF8();
    while (true) {
        ICEBERG_ASSIGN_OR_RAISE(auto count, cursor.ReadLong());
        if (count == 0) {
            break;
        }
        std::optional<std::int64_t> blockBytes;
        if (count < 0) {
            if (count == std::numeric_limits<std::int64_t>::min()) {
                return iceberg::Invalid("CORRUPTION: Overflowing Avro metadata count.");
            }
            count = -count;
            ICEBERG_ASSIGN_OR_RAISE(blockBytes, cursor.ReadLong());
            if (*blockBytes < 0 || static_cast<std::uint64_t>(*blockBytes) > cursor.GetRemaining()) {
                return iceberg::Invalid("CORRUPTION: Invalid Avro metadata block size.");
            }
        }
        const auto before = cursor.GetRemaining();
        if (static_cast<std::uint64_t>(count) > before / 2) {
            return iceberg::Invalid("CORRUPTION: Invalid Avro metadata count.");
        }
        for (std::int64_t index = 0; index < count; ++index) {
            ICEBERG_ASSIGN_OR_RAISE(auto key, cursor.ReadBytes());
            ICEBERG_ASSIGN_OR_RAISE(auto value, cursor.ReadBytes());
            if (!arrow::util::ValidateUTF8(key) || !keys.insert(key).second) {
                return iceberg::Invalid("CORRUPTION: Invalid or duplicate Avro metadata key.");
            }
            if (key == "avro.schema") {
                hasSchema = !value.empty();
            } else if (key == "avro.codec") {
                if (value == "deflate") {
                    codec = EAvroCodec::Deflate;
                } else if (value != "null") {
                    return iceberg::NotSupported("Unsupported Avro codec.");
                }
            }
        }
        if (blockBytes && static_cast<std::uint64_t>(*blockBytes) != before - cursor.GetRemaining()) {
            return iceberg::Invalid("CORRUPTION: Avro metadata block size does "
                                    "not match its contents.");
        }
    }
    if (!hasSchema) {
        return iceberg::Invalid("CORRUPTION: Avro schema is missing.");
    }
    return codec;
}

iceberg::Result<std::int64_t> CheckDecodedBlock(std::string_view block, EAvroCodec codec, std::int64_t remaining)
{
    if (codec == EAvroCodec::Null) {
        if (block.size() > static_cast<std::uint64_t>(remaining)) {
            return iceberg::InvalidArgument("Avro container exceeds its decoded byte limit.");
        }
        return static_cast<std::int64_t>(block.size());
    }
    if (block.size() > std::numeric_limits<uInt>::max()) {
        return iceberg::InvalidArgument("Avro compressed block exceeds the decoder input limit.");
    }
    struct TInflater
    {
        z_stream State{};
        bool Initialized = false;

        ~TInflater()
        {
            if (Initialized) {
                inflateEnd(&State);
            }
        }
    } inflater;
    if (inflateInit2(&inflater.State, -MAX_WBITS) != Z_OK) {
        return iceberg::UnknownError("Cannot initialize the Avro deflate decoder.");
    }
    inflater.Initialized = true;
    inflater.State.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(block.data()));
    inflater.State.avail_in = static_cast<uInt>(block.size());
    std::array<Bytef, 32768> output;
    std::int64_t decoded = 0;
    while (true) {
        const auto capacity = std::min(output.size(), static_cast<std::size_t>(remaining - decoded) + 1);
        inflater.State.next_out = output.data();
        inflater.State.avail_out = static_cast<uInt>(capacity);
        const auto inputBefore = inflater.State.avail_in;
        const auto status = inflate(&inflater.State, Z_NO_FLUSH);
        const auto produced = static_cast<std::int64_t>(capacity - inflater.State.avail_out);
        if (produced > remaining - decoded) {
            return iceberg::InvalidArgument("Avro container exceeds its decoded byte limit.");
        }
        decoded += produced;
        if (status == Z_STREAM_END) {
            if (inflater.State.avail_in != 0) {
                return iceberg::Invalid("CORRUPTION: Trailing bytes in an Avro deflate block.");
            }
            return decoded;
        }
        if (status != Z_OK || (produced == 0 && inputBefore == inflater.State.avail_in)) {
            return iceberg::Invalid("CORRUPTION: Invalid or truncated Avro deflate block.");
        }
    }
}

bool HasBoundedJsonDepth(std::string_view bytes)
{
    unsigned depth = 0;
    bool inString = false;
    bool escaped = false;
    for (char value : bytes) {
        if (inString) {
            if (escaped) {
                escaped = false;
            } else if (value == '\\') {
                escaped = true;
            } else if (value == '"') {
                inString = false;
            }
        } else if (value == '"') {
            inString = true;
        } else if (value == '{' || value == '[') {
            if (++depth > 64) {
                return false;
            }
        } else if ((value == '}' || value == ']') && depth != 0) {
            --depth;
        }
    }
    return true;
}

class TMetadataBytes : public iceberg::FileIO
{
public:
    explicit TMetadataBytes(std::string bytes)
        : Bytes_(std::move(bytes))
    { }

    iceberg::Result<std::string> ReadFile(const std::string&, std::optional<std::size_t>) override
    {
        return std::move(Bytes_);
    }

private:
    std::string Bytes_;
};

// Captures the bytes TableMetadataUtil::Write produces without writing them.
class TCapturedBytes : public iceberg::FileIO
{
public:
    iceberg::Status WriteFile(const std::string&, std::string_view content) override
    {
        Bytes.assign(content);
        return {};
    }

    std::string Bytes;
};

std::atomic<std::uint64_t> MetadataParses{0};

iceberg::Result<std::unique_ptr<iceberg::TableMetadata>> ParseBoundedMetadata(std::string bytes,
    const std::string& path)
{
    if (!HasBoundedJsonDepth(bytes)) {
        return iceberg::Invalid("CORRUPTION: Metadata JSON exceeds its nesting depth limit.");
    }
    MetadataParses.fetch_add(1, std::memory_order_relaxed);
    TMetadataBytes loader(std::move(bytes));
    return iceberg::TableMetadataUtil::Read(loader, path);
}

}

iceberg::Result<TParquetFileInfo> CheckParquet(iceberg::InputFile& file,
    const iceberg::Schema& schema,
    TParquetExpectation expected,
    TParquetLimits limits)
{
    ICEBERG_ASSIGN_OR_RAISE(auto metadata, ReadParquetFooter(file, schema, expected, limits));
    return TParquetFileInfo{metadata->size(), metadata->num_rows(), metadata->num_row_groups()};
}

iceberg::Result<std::shared_ptr<parquet::FileMetaData>> ReadParquetFooterFromStream(
    iceberg::SeekableInputStream& stream,
    std::int64_t actualSize,
    const iceberg::Schema& schema,
    TParquetExpectation expected,
    TParquetLimits limits,
    std::uint64_t* bytesReadOut)
{
    const auto expectedLength = expected.Length;
    const auto expectedRows = expected.RowCount;
    return CatchIcebergExceptions([&]() -> iceberg::Result<std::shared_ptr<parquet::FileMetaData>> {
        if (expectedRows < 0 || limits.MaxFooterBytes > std::numeric_limits<std::int32_t>::max() ||
            limits.MaxThriftContainerElements <= 0)
        {
            return iceberg::InvalidArgument("Invalid Parquet verification limits.");
        }
        if (limits.MaxFileBytes < 0 || expectedLength < 0) {
            return iceberg::InvalidArgument("File size limits must be nonnegative.");
        }
        if (actualSize < 0 || actualSize != expectedLength) {
            return iceberg::Invalid("CORRUPTION: File length differs from its expected length.");
        }
        if (actualSize > limits.MaxFileBytes) {
            return iceberg::InvalidArgument("File exceeds its byte limit.");
        }
        if (actualSize < 12) {
            return iceberg::Invalid("CORRUPTION: Parquet file is truncated.");
        }
        std::array<std::byte, 4> header;
        std::array<std::byte, 8> trailer;
        ICEBERG_RETURN_UNEXPECTED(stream.ReadFully(0, header));
        ICEBERG_RETURN_UNEXPECTED(stream.ReadFully(actualSize - 8, trailer));
        const auto magic = std::as_bytes(std::span("PAR1", 4));
        if (!std::ranges::equal(header, magic) || !std::ranges::equal(std::span(trailer).subspan(4), magic)) {
            return iceberg::Invalid("CORRUPTION: Invalid Parquet magic.");
        }
        std::uint32_t footerLength = 0;
        for (unsigned index = 0; index < 4; ++index) {
            footerLength |= std::to_integer<std::uint32_t>(trailer[index]) << (index * 8);
        }
        if (footerLength == 0 || footerLength > static_cast<std::uint64_t>(actualSize - 12)) {
            return iceberg::Invalid("CORRUPTION: Invalid Parquet footer length.");
        }
        if (footerLength > limits.MaxFooterBytes) {
            return iceberg::InvalidArgument("Parquet footer exceeds its byte limit.");
        }
        std::vector<std::byte> footer(footerLength);
        ICEBERG_RETURN_UNEXPECTED(stream.ReadFully(actualSize - 8 - footerLength, footer));
        auto parsed = ParseFooter(footer, expectedRows, schema, limits);
        if (parsed && bytesReadOut != nullptr) {
            *bytesReadOut += header.size() + trailer.size() + footer.size();
        }
        return parsed;
    });
}

iceberg::Result<std::shared_ptr<parquet::FileMetaData>> ReadParquetFooter(iceberg::InputFile& file,
    const iceberg::Schema& schema,
    TParquetExpectation expected,
    TParquetLimits limits)
{
    const auto expectedLength = expected.Length;
    const auto expectedRows = expected.RowCount;
    return CatchIcebergExceptions([&]() -> iceberg::Result<std::shared_ptr<parquet::FileMetaData>> {
        if (expectedRows < 0 || limits.MaxFooterBytes > std::numeric_limits<std::int32_t>::max() ||
            limits.MaxThriftContainerElements <= 0)
        {
            return iceberg::InvalidArgument("Invalid Parquet verification limits.");
        }
        ICEBERG_ASSIGN_OR_RAISE(auto size, CheckSize(file, expectedLength, limits.MaxFileBytes));
        if (size < 12) {
            return iceberg::Invalid("CORRUPTION: Parquet file is truncated.");
        }
        return WithStream<std::shared_ptr<parquet::FileMetaData>>(file, [&](iceberg::SeekableInputStream& stream) {
            return ReadParquetFooterFromStream(stream, size, schema, expected, limits);
        });
    });
}

iceberg::Result<TAvroContainerInfo>
CheckAvroContainer(iceberg::InputFile& file, std::int64_t expectedLength, TAvroLimits limits)
{
    const auto maxBytes = limits.MaxBytes;
    const auto maxRecords = limits.MaxRecords;
    return CatchIcebergExceptions([&]() -> iceberg::Result<TAvroContainerInfo> {
        if (maxRecords < 0 || limits.MaxDecodedBytes <= 0) {
            return iceberg::InvalidArgument("Invalid Avro record or decoded byte limit.");
        }
        ICEBERG_ASSIGN_OR_RAISE(auto bytes, ReadBounded(file, expectedLength, maxBytes));
        TAvroCursor cursor(bytes);
        ICEBERG_ASSIGN_OR_RAISE(auto magic, cursor.Take(4));
        if (magic != "Obj\1") {
            return iceberg::Invalid("CORRUPTION: Invalid Avro magic.");
        }
        ICEBERG_ASSIGN_OR_RAISE(auto codec, ReadAvroMetadata(cursor));
        ICEBERG_ASSIGN_OR_RAISE(auto sync, cursor.Take(16));
        TAvroContainerInfo info;
        std::int64_t decodedBytes = 0;
        while (cursor.GetRemaining() != 0) {
            ICEBERG_ASSIGN_OR_RAISE(auto count, cursor.ReadLong());
            if (count <= 0) {
                return iceberg::Invalid("CORRUPTION: Invalid Avro record count.");
            }
            if (count > maxRecords - info.RecordCount) {
                return iceberg::InvalidArgument("Avro container exceeds its record limit.");
            }
            ICEBERG_ASSIGN_OR_RAISE(auto block, cursor.ReadBytes());
            ICEBERG_ASSIGN_OR_RAISE(auto marker, cursor.Take(16));
            if (marker != sync) {
                return iceberg::Invalid("CORRUPTION: Avro sync marker does not match.");
            }
            ICEBERG_ASSIGN_OR_RAISE(auto decoded,
                CheckDecodedBlock(block, codec, limits.MaxDecodedBytes - decodedBytes));
            decodedBytes += decoded;
            info.RecordCount += count;
            ++info.BlockCount;
        }
        return info;
    });
}

iceberg::Result<std::unique_ptr<iceberg::TableMetadata>> ReadTableMetadata(iceberg::FileIO& io,
    const std::string& path,
    std::optional<std::string_view> expectedBytes,
    std::int64_t maxBytes)
{
    return CatchIcebergExceptions([&]() -> iceberg::Result<std::unique_ptr<iceberg::TableMetadata>> {
        if (!path.ends_with(".metadata.json")) {
            return iceberg::InvalidArgument("Metadata verification requires an uncompressed JSON file.");
        }
        ICEBERG_ASSIGN_OR_RAISE(auto file, io.NewInputFile(path));
        ICEBERG_ASSIGN_OR_RAISE(auto bytes, ReadBounded(*file, std::nullopt, maxBytes));
        if (expectedBytes && bytes != *expectedBytes) {
            return iceberg::Invalid("CORRUPTION: Metadata bytes differ from the written content.");
        }
        return ParseBoundedMetadata(std::move(bytes), path);
    });
}

iceberg::Result<std::unique_ptr<iceberg::TableMetadata>> ReadTableMetadataFromInput(iceberg::InputFile& file,
    const std::string& path,
    std::int64_t actualSize,
    std::int64_t maxBytes)
{
    return CatchIcebergExceptions([&]() -> iceberg::Result<std::unique_ptr<iceberg::TableMetadata>> {
        if (!path.ends_with(".metadata.json")) {
            return iceberg::InvalidArgument("Metadata verification requires an uncompressed JSON file.");
        }
        if (maxBytes < 0) {
            return iceberg::InvalidArgument("File size limits must be nonnegative.");
        }
        if (actualSize < 0) {
            return iceberg::Invalid("CORRUPTION: File length differs from its expected length.");
        }
        if (actualSize > maxBytes) {
            return iceberg::InvalidArgument("File exceeds its byte limit.");
        }
        ICEBERG_ASSIGN_OR_RAISE(auto bytes,
            WithStream<std::string>(file, [&](iceberg::SeekableInputStream& stream) -> iceberg::Result<std::string> {
                std::string buffer(static_cast<std::size_t>(actualSize), '\0');
                ICEBERG_RETURN_UNEXPECTED(stream.ReadFully(0, std::as_writable_bytes(std::span(buffer))));
                return buffer;
            }));
        return ParseBoundedMetadata(std::move(bytes), path);
    });
}

iceberg::Result<std::string> SerializeTableMetadata(const iceberg::TableMetadata& metadata)
{
    return CatchIcebergExceptions([&]() -> iceberg::Result<std::string> {
        TCapturedBytes captured;
        ICEBERG_RETURN_UNEXPECTED(iceberg::TableMetadataUtil::Write(captured, "memory.metadata.json", metadata));
        return std::move(captured.Bytes);
    });
}

iceberg::Result<std::unique_ptr<iceberg::TableMetadata>> ParseTableMetadata(std::string bytes)
{
    return CatchIcebergExceptions([&]() -> iceberg::Result<std::unique_ptr<iceberg::TableMetadata>> {
        return ParseBoundedMetadata(std::move(bytes), "memory.metadata.json");
    });
}

iceberg::Status VerifyTableMetadataBytes(iceberg::FileIO& io,
    const std::string& path,
    std::string_view expectedBytes,
    std::int64_t maxBytes)
{
    return CatchIcebergExceptions([&]() -> iceberg::Status {
        if (!path.ends_with(".metadata.json")) {
            return iceberg::InvalidArgument("Metadata verification requires an uncompressed JSON file.");
        }
        ICEBERG_ASSIGN_OR_RAISE(auto file, io.NewInputFile(path));
        ICEBERG_ASSIGN_OR_RAISE(auto bytes, ReadBounded(*file, std::nullopt, maxBytes));
        if (bytes != expectedBytes) {
            return iceberg::Invalid("CORRUPTION: Metadata bytes differ from the written content.");
        }
        if (!HasBoundedJsonDepth(bytes)) {
            return iceberg::Invalid("CORRUPTION: Metadata JSON exceeds its nesting depth limit.");
        }
        return {};
    });
}

std::uint64_t TableMetadataParses() noexcept
{
    return MetadataParses.load(std::memory_order_relaxed);
}
}
