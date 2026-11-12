#include "core/storage/format_verifier.h"

#include <arrow/array/builder_primitive.h>
#include <arrow/c/bridge.h>
#include <arrow/io/memory.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <gtest/gtest.h>
#include <iceberg/arrow/arrow_io_util.h>
#include <iceberg/avro/avro_register.h>
#include <iceberg/file_io.h>
#include <iceberg/file_writer.h>
#include <iceberg/parquet/parquet_register.h>
#include <iceberg/partition_spec.h>
#include <iceberg/schema.h>
#include <iceberg/schema_field.h>
#include <iceberg/sort_order.h>
#include <iceberg/table_metadata.h>
#include <iceberg/type.h>
#include <parquet/column_writer.h>
#include <parquet/file_reader.h>
#include <parquet/file_writer.h>
#include <parquet/metadata.h>
#include <parquet/schema.h>
#include <zlib.h>

#include <array>
#include <limits>
#include <stdexcept>
#include <string>
#include <utility>

namespace NGlamDB::NStorage {
namespace {

class TFormatVerifierTest : public ::testing::Test
{
protected:
    std::shared_ptr<iceberg::FileIO> IO_ = iceberg::arrow::MakeMockFileIO();
    std::shared_ptr<iceberg::Schema> Schema_ = std::make_shared<iceberg::Schema>(
        std::vector{iceberg::SchemaField::MakeRequired(1, "value", iceberg::int64())});

    static void SetUpTestSuite()
    {
        iceberg::parquet::RegisterAll();
        iceberg::avro::RegisterAll();
    }

    void WriteRows(iceberg::FileFormatType format, const std::string& path)
    {
        iceberg::WriterOptions options;
        options.io = IO_;
        options.path = path;
        options.schema = Schema_;
        options.properties = iceberg::WriterProperties::FromMap({{"write.parquet.max-row-group-rows", "2"}});
        auto writer = iceberg::WriterFactoryRegistry::Open(format, options);
        ASSERT_TRUE(writer) << writer.error().message;
        arrow::Int64Builder builder;
        ASSERT_TRUE(builder.AppendValues({1, 2, 3}).ok());
        auto array = builder.Finish();
        ASSERT_TRUE(array.ok());
        auto batch =
            arrow::RecordBatch::Make(arrow::schema({arrow::field("value", arrow::int64(), false)}), 3, {*array});
        ArrowArray exported{};
        ASSERT_TRUE(arrow::ExportRecordBatch(*batch, &exported).ok());
        const auto written = (*writer)->Write(&exported);
        if (exported.release != nullptr) {
            exported.release(&exported);
        }
        ASSERT_TRUE(written) << written.error().message;
        const auto closed = (*writer)->Close();
        ASSERT_TRUE(closed) << closed.error().message;
    }

    std::string ReadBytes(const std::string& path)
    {
        auto bytes = IO_->ReadFile(path, std::nullopt);
        EXPECT_TRUE(bytes);
        return bytes ? *bytes : std::string{};
    }

    iceberg::Result<TParquetFileInfo>
    CheckData(const std::string& path, std::int64_t rows = 3, TParquetLimits limits = {})
    {
        auto file = IO_->NewInputFile(path);
        if (!file) {
            return std::unexpected(file.error());
        }
        auto size = (*file)->Size();
        if (!size) {
            return std::unexpected(size.error());
        }
        return CheckParquet(**file, *Schema_, {*size, rows}, limits);
    }

    iceberg::Result<TAvroContainerInfo> CheckAvro(const std::string& bytes, TAvroLimits limits = {})
    {
        auto written = IO_->WriteFile("input.avro", bytes);
        if (!written) {
            return std::unexpected(written.error());
        }
        auto file = IO_->NewInputFile("input.avro");
        if (!file) {
            return std::unexpected(file.error());
        }
        return CheckAvroContainer(**file, static_cast<std::int64_t>(bytes.size()), limits);
    }
};

TEST_F(TFormatVerifierTest, ReadsRealParquetFooterAndChecksRowsFieldIdsAndLimits)
{
    ASSERT_NO_FATAL_FAILURE(WriteRows(iceberg::FileFormatType::kParquet, "data.parquet"));
    const auto checked = CheckData("data.parquet");
    ASSERT_TRUE(checked) << checked.error().message;
    EXPECT_EQ(checked->RowCount, 3);
    EXPECT_EQ(checked->RowGroupCount, 2);
    EXPECT_GT(checked->FooterBytes, 0);
    EXPECT_FALSE(CheckData("data.parquet", 4));
    auto file = IO_->NewInputFile("data.parquet");
    ASSERT_TRUE(file);
    auto size = (*file)->Size();
    ASSERT_TRUE(size);
    EXPECT_FALSE(CheckParquet(**file, *Schema_, {*size + 1, 3}));
    iceberg::Schema wrongID({iceberg::SchemaField::MakeRequired(2, "value", iceberg::int64())});
    EXPECT_FALSE(CheckParquet(**file, wrongID, {*size, 3}));
    for (const auto& type : std::array<std::shared_ptr<iceberg::Type>, 4>{iceberg::string(),
             iceberg::boolean(),
             iceberg::float64(),
             iceberg::timestamp_tz()})
    {
        iceberg::Schema wrongType({iceberg::SchemaField::MakeRequired(1, "value", type)});
        EXPECT_FALSE(CheckParquet(**file, wrongType, {*size, 3}));
    }
    EXPECT_TRUE(CheckData("data.parquet", 3, {*size, checked->FooterBytes}));
    EXPECT_FALSE(CheckData("data.parquet", 3, {*size - 1, checked->FooterBytes}));
    EXPECT_FALSE(CheckData("data.parquet", 3, {*size, checked->FooterBytes - 1}));
}

TEST_F(TFormatVerifierTest, RejectsTruncatedMalformedAndTrailingParquetFooterBytes)
{
    ASSERT_NO_FATAL_FAILURE(WriteRows(iceberg::FileFormatType::kParquet, "data.parquet"));
    const auto original = ReadBytes("data.parquet");
    ASSERT_GT(original.size(), 12);
    for (int fault = 0; fault < 6; ++fault) {
        auto bytes = original;
        switch (fault) {
            case 0:
                bytes.resize(11);
                break;
            case 1:
                bytes.front() = 'X';
                break;
            case 2:
                bytes.back() = 'X';
                break;
            case 3:
                bytes[bytes.size() - 8] = '\xff';
                bytes[bytes.size() - 7] = '\xff';
                bytes[bytes.size() - 6] = '\xff';
                bytes[bytes.size() - 5] = '\xff';
                break;
            case 4:
                bytes.erase(bytes.size() - 9, 1);
                break;
            case 5: {
                const auto checked = CheckData("data.parquet");
                ASSERT_TRUE(checked);
                bytes.insert(bytes.size() - 8, 1, '\0');
                const auto length = checked->FooterBytes + 1;
                for (unsigned index = 0; index < 4; ++index) {
                    bytes[bytes.size() - 8 + index] = static_cast<char>((length >> (index * 8)) & 0xff);
                }
                break;
            }
            default:
                FAIL();
        }
        ASSERT_TRUE(IO_->WriteFile("broken.parquet", bytes));
        EXPECT_FALSE(CheckData("broken.parquet")) << fault;
    }
}

TEST_F(TFormatVerifierTest, ReadsRealCompressedAvroAndRejectsBrokenFraming)
{
    ASSERT_NO_FATAL_FAILURE(WriteRows(iceberg::FileFormatType::kAvro, "data.avro"));
    const auto original = ReadBytes("data.avro");
    const auto checked = CheckAvro(original);
    ASSERT_TRUE(checked) << checked.error().message;
    EXPECT_EQ(checked->RecordCount, 3);
    EXPECT_GE(checked->BlockCount, 1);
    EXPECT_FALSE(CheckAvro(original, {.MaxRecords = 2}));
    for (auto size : {std::size_t{0}, std::size_t{3}, original.size() - 1, original.size() - 16}) {
        EXPECT_FALSE(CheckAvro(original.substr(0, size))) << size;
    }
    auto corrupted = original;
    corrupted.back() ^= 1;
    EXPECT_FALSE(CheckAvro(corrupted));
    EXPECT_FALSE(CheckAvro(original + "garbage"));
    auto file = IO_->NewInputFile("data.avro");
    ASSERT_TRUE(file);
    const auto size = static_cast<std::int64_t>(original.size());
    EXPECT_TRUE(CheckAvroContainer(**file, size, {size}));
    EXPECT_FALSE(CheckAvroContainer(**file, size, {size - 1}));
    EXPECT_FALSE(CheckAvroContainer(**file, size + 1, {size + 1}));
}

std::string EncodeLong(std::int64_t value)
{
    auto bits = (static_cast<std::uint64_t>(value) << 1) ^ static_cast<std::uint64_t>(-(value < 0));
    std::string result;
    do {
        const auto byte = static_cast<unsigned char>(bits & 0x7f);
        bits >>= 7;
        result += static_cast<char>(byte | (bits == 0 ? 0 : 0x80));
    } while (bits != 0);
    return result;
}

std::string EncodeBytes(std::string_view value)
{
    return EncodeLong(static_cast<std::int64_t>(value.size())) + std::string(value);
}

std::string DeflateRaw(const std::string& bytes)
{
    z_stream stream{};
    EXPECT_EQ(deflateInit2(&stream, Z_BEST_COMPRESSION, Z_DEFLATED, -MAX_WBITS, 8, Z_DEFAULT_STRATEGY), Z_OK);
    std::string output(compressBound(static_cast<uLong>(bytes.size())), '\0');
    stream.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(bytes.data()));
    stream.avail_in = static_cast<uInt>(bytes.size());
    stream.next_out = reinterpret_cast<Bytef*>(output.data());
    stream.avail_out = static_cast<uInt>(output.size());
    EXPECT_EQ(deflate(&stream, Z_FINISH), Z_STREAM_END);
    output.resize(stream.total_out);
    EXPECT_EQ(deflateEnd(&stream), Z_OK);
    return output;
}

std::string AvroHeader(std::string_view codec)
{
    return std::string("Obj\1") + EncodeLong(2) + EncodeBytes("avro.schema") + EncodeBytes("\"bytes\"") +
        EncodeBytes("avro.codec") + EncodeBytes(codec) + EncodeLong(0) + std::string(16, 's');
}

std::string AvroBlock(std::string_view bytes)
{
    return EncodeLong(1) + EncodeBytes(bytes) + std::string(16, 's');
}

TEST_F(TFormatVerifierTest, BoundsDecodedAvroBytesAcrossCompressedAndNullBlocks)
{
    const auto plain = EncodeBytes(std::string(128ULL * 1024, 'x'));
    const auto compressed = DeflateRaw(plain);
    ASSERT_LT(compressed.size(), 1024);
    const auto file = AvroHeader("deflate") + AvroBlock(compressed);
    const auto size = static_cast<std::int64_t>(plain.size());
    EXPECT_TRUE(CheckAvro(file, {.MaxDecodedBytes = size}));
    EXPECT_FALSE(CheckAvro(file, {.MaxDecodedBytes = size - 1}));
    const auto twice = file + AvroBlock(compressed);
    EXPECT_TRUE(CheckAvro(twice, {.MaxDecodedBytes = 2 * size}));
    EXPECT_FALSE(CheckAvro(twice, {.MaxDecodedBytes = 2 * size - 1}));
    const auto uncompressed = AvroHeader("null") + AvroBlock(plain);
    EXPECT_TRUE(CheckAvro(uncompressed, {.MaxDecodedBytes = size}));
    EXPECT_FALSE(CheckAvro(uncompressed, {.MaxDecodedBytes = size - 1}));
    EXPECT_FALSE(CheckAvro(file, {.MaxDecodedBytes = 0}));
}

TEST_F(TFormatVerifierTest, RejectsInvalidTruncatedTrailingAndUnknownAvroCompression)
{
    const auto compressed = DeflateRaw(EncodeBytes("value"));
    const auto header = AvroHeader("deflate");
    EXPECT_FALSE(CheckAvro(header + AvroBlock(compressed.substr(0, compressed.size() - 1))));
    EXPECT_FALSE(CheckAvro(header + AvroBlock(compressed + "trailing")));
    EXPECT_FALSE(CheckAvro(header + AvroBlock("garbage")));
    const auto unknown = CheckAvro(AvroHeader("snappy") + AvroBlock(compressed));
    ASSERT_FALSE(unknown);
    EXPECT_EQ(unknown.error().kind, iceberg::ErrorKind::kNotSupported);
}

TEST_F(TFormatVerifierTest, HandlesNegativeMetadataBlocksEmptyContainersAndIntegerBounds)
{
    const auto entry = EncodeBytes("avro.schema") + EncodeBytes("\"null\"");
    const auto marker = std::string(16, 's');
    const auto header = std::string("Obj\1") + EncodeLong(-1) + EncodeLong(static_cast<std::int64_t>(entry.size())) +
        entry + EncodeLong(0) + marker;
    const auto empty = CheckAvro(header);
    ASSERT_TRUE(empty);
    EXPECT_EQ(empty->RecordCount, 0);
    EXPECT_EQ(empty->BlockCount, 0);
    const auto valid = header + EncodeLong(3) + EncodeLong(0) + marker;
    EXPECT_EQ(CheckAvro(valid)->RecordCount, 3);
    EXPECT_FALSE(CheckAvro(header + EncodeLong(-1) + EncodeLong(0) + marker));
    EXPECT_FALSE(CheckAvro(header + EncodeLong(1) + EncodeLong(-1) + marker));
    EXPECT_FALSE(CheckAvro(header + EncodeLong(std::numeric_limits<std::int64_t>::max()) + EncodeLong(0) + marker));
    EXPECT_FALSE(CheckAvro(std::string("Obj\1") + EncodeLong(std::numeric_limits<std::int64_t>::min())));
    EXPECT_FALSE(CheckAvro(std::string("Obj\1") + std::string(10, '\xff')));
    EXPECT_FALSE(CheckAvro(std::string("Obj\1") + EncodeLong(0) + marker));
    EXPECT_FALSE(CheckAvro(std::string("Obj\1") + EncodeLong(2) + entry + entry + EncodeLong(0) + marker));
    EXPECT_FALSE(CheckAvro(std::string("Obj\1") + EncodeLong(-1) + EncodeLong(1) + entry + EncodeLong(0) + marker));
}

TEST_F(TFormatVerifierTest, LoadsRealMetadataAndChecksExactBytesAndSizeBeforeParsing)
{
    auto metadata = iceberg::TableMetadata::Make(*Schema_,
        *iceberg::PartitionSpec::Unpartitioned(),
        *iceberg::SortOrder::Unsorted(),
        "warehouse",
        {{"commit.retry.num-retries", "0"}});
    ASSERT_TRUE(metadata) << metadata.error().message;
    (*metadata)->properties.mutable_configs()["quoted"] =
        std::string(100, '[') + "\" \\\\ \\\" " + std::string(100, ']');
    const std::string path = "table.metadata.json";
    const auto written = iceberg::TableMetadataUtil::Write(*IO_, path, **metadata);
    ASSERT_TRUE(written) << written.error().message;
    const auto bytes = ReadBytes(path);
    const auto parsed = ReadTableMetadata(*IO_, path, bytes, static_cast<std::int64_t>(bytes.size()));
    ASSERT_TRUE(parsed) << parsed.error().message;
    EXPECT_EQ((*parsed)->format_version, 2);
    EXPECT_EQ((*parsed)->table_uuid, (*metadata)->table_uuid);
    EXPECT_EQ((*parsed)->location, "warehouse");
    EXPECT_FALSE(ReadTableMetadata(*IO_, path, bytes + " "));
    EXPECT_FALSE(ReadTableMetadata(*IO_, path, std::nullopt, static_cast<std::int64_t>(bytes.size()) - 1));
    EXPECT_FALSE(ReadTableMetadata(*IO_, path + ".gz"));
    auto atLimit = bytes;
    atLimit.insert(atLimit.find_last_of('}'), ",\"unknown\":" + std::string(63, '[') + "0" + std::string(63, ']'));
    ASSERT_TRUE(IO_->WriteFile("depth.metadata.json", atLimit));
    EXPECT_TRUE(ReadTableMetadata(*IO_, "depth.metadata.json"));
    std::string nestedObject = "0";
    for (int depth = 0; depth < 65; ++depth) {
        nestedObject.insert(0, "{\"x\":");
        nestedObject += '}';
    }
    for (const auto& deep : {nestedObject, std::string(65, '[') + "0" + std::string(65, ']')}) {
        ASSERT_TRUE(IO_->WriteFile("deep.metadata.json", deep));
        const auto rejected = ReadTableMetadata(*IO_, "deep.metadata.json");
        ASSERT_FALSE(rejected);
        EXPECT_NE(rejected.error().message.find("nesting depth"), std::string::npos);
    }
    for (const std::string invalid : {"", "{", "{}", "[]", "null"}) {
        ASSERT_TRUE(IO_->WriteFile("bad.metadata.json", invalid));
        EXPECT_FALSE(ReadTableMetadata(*IO_, "bad.metadata.json")) << invalid;
    }
}

class TFailingInputFile : public iceberg::InputFile
{
public:
    std::int64_t ReportedSize = 0;
    int OpenCount = 0;

    std::string_view location() const override
    {
        return "failing";
    }
    iceberg::Result<std::int64_t> Size() const override
    {
        return ReportedSize;
    }
    iceberg::Result<std::unique_ptr<iceberg::SeekableInputStream>> Open() override
    {
        ++OpenCount;
        return iceberg::IOError("original open error");
    }
};

TEST_F(TFormatVerifierTest, RejectsSizeBeforeOpeningAndPreservesIoErrors)
{
    TFailingInputFile file;
    file.ReportedSize = std::numeric_limits<std::int64_t>::max();
    EXPECT_FALSE(CheckParquet(file, *Schema_, {file.ReportedSize, 0}));
    EXPECT_FALSE(CheckAvroContainer(file, file.ReportedSize, {1024}));
    EXPECT_EQ(file.OpenCount, 0);
    file.ReportedSize = 12;
    const auto result = CheckParquet(file, *Schema_, {12, 0});
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().kind, iceberg::ErrorKind::kIOError);
    EXPECT_EQ(result.error().message, "original open error");
    EXPECT_EQ(file.OpenCount, 1);
}

TEST_F(TFormatVerifierTest, RejectsRepeatedPrimitiveForRequiredScalar)
{
    auto output = arrow::io::BufferOutputStream::Create();
    ASSERT_TRUE(output.ok());
    auto field = parquet::schema::PrimitiveNode::Make("value",
        parquet::Repetition::REPEATED,
        parquet::LogicalType::None(),
        parquet::Type::INT64,
        /*primitiveLength*/ -1,
        /*fieldID*/ 1);
    auto schema = parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, {field});
    auto writer =
        parquet::ParquetFileWriter::Open(*output, std::static_pointer_cast<parquet::schema::GroupNode>(schema));
    writer->Close();
    auto bytes = (*output)->Finish();
    ASSERT_TRUE(bytes.ok());
    ASSERT_TRUE(IO_->WriteFile("repeated.parquet", (*bytes)->ToString()));
    EXPECT_FALSE(CheckData("repeated.parquet", 0));
}

TEST_F(TFormatVerifierTest, AcceptsNativeDecimalAndDateFootersAndRejectsWrongParameters)
{
    struct TNativeType
    {
        std::shared_ptr<iceberg::Type> Iceberg;
        std::shared_ptr<const parquet::LogicalType> Logical;
        parquet::Type::type Physical;
        int Width = -1;
    };
    const std::array cases{
        TNativeType{iceberg::decimal(9, 2), parquet::LogicalType::Decimal(9, 2), parquet::Type::INT32},
        TNativeType{iceberg::decimal(18, 6), parquet::LogicalType::Decimal(18, 6), parquet::Type::INT64},
        TNativeType{iceberg::decimal(20, 0),
            parquet::LogicalType::Decimal(20, 0),
            parquet::Type::FIXED_LEN_BYTE_ARRAY,
            9},
        TNativeType{iceberg::decimal(38, 38),
            parquet::LogicalType::Decimal(38, 38),
            parquet::Type::FIXED_LEN_BYTE_ARRAY,
            16},
        TNativeType{iceberg::date(), parquet::LogicalType::Date(), parquet::Type::INT32}};
    for (const auto& test : cases) {
        SCOPED_TRACE(test.Iceberg->ToString());
        auto output = arrow::io::BufferOutputStream::Create();
        ASSERT_TRUE(output.ok());
        auto node = parquet::schema::PrimitiveNode::Make("value",
            parquet::Repetition::REQUIRED,
            test.Logical,
            test.Physical,
            test.Width,
            1);
        auto schema = parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, {node});
        auto writer =
            parquet::ParquetFileWriter::Open(*output, std::static_pointer_cast<parquet::schema::GroupNode>(schema));
        writer->Close();
        auto bytes = (*output)->Finish();
        ASSERT_TRUE(bytes.ok());
        ASSERT_TRUE(IO_->WriteFile("native.parquet", (*bytes)->ToString()));
        Schema_ = std::make_shared<iceberg::Schema>(
            std::vector{iceberg::SchemaField::MakeRequired(1, "value", test.Iceberg)});
        const auto checked = CheckData("native.parquet", 0);
        EXPECT_TRUE(checked) << checked.error().message;
        for (const auto& wrong : std::array<std::shared_ptr<iceberg::Type>, 5>{iceberg::decimal(8, 2),
                 iceberg::decimal(18, 5),
                 iceberg::decimal(37, 37),
                 iceberg::int32(),
                 iceberg::string()})
        {
            Schema_ =
                std::make_shared<iceberg::Schema>(std::vector{iceberg::SchemaField::MakeRequired(1, "value", wrong)});
            EXPECT_FALSE(CheckData("native.parquet", 0));
        }
    }
}

TEST_F(TFormatVerifierTest, RejectsNativeBoundsOutsideDecimalPrecisionAndCalendarRange)
{
    for (const bool date : {false, true}) {
        for (const std::int32_t value : date ? std::array{-719163, 2932897} : std::array{-1000000000, 1000000000}) {
            auto output = arrow::io::BufferOutputStream::Create();
            ASSERT_TRUE(output.ok());
            auto node = parquet::schema::PrimitiveNode::Make("value",
                parquet::Repetition::REQUIRED,
                date ? parquet::LogicalType::Date() : parquet::LogicalType::Decimal(9, 2),
                parquet::Type::INT32,
                -1,
                1);
            auto schema = parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, {node});
            auto writer =
                parquet::ParquetFileWriter::Open(*output, std::static_pointer_cast<parquet::schema::GroupNode>(schema));
            auto* column = static_cast<parquet::Int32Writer*>(writer->AppendRowGroup()->NextColumn());
            column->WriteBatch(1, nullptr, nullptr, &value);
            writer->Close();
            auto bytes = (*output)->Finish();
            ASSERT_TRUE(bytes.ok());
            ASSERT_TRUE(IO_->WriteFile("overflow.parquet", (*bytes)->ToString()));
            Schema_ = std::make_shared<iceberg::Schema>(std::vector{iceberg::SchemaField::MakeRequired(1,
                "value",
                date ? std::shared_ptr<iceberg::Type>(iceberg::date()) : iceberg::decimal(9, 2))});
            EXPECT_FALSE(CheckData("overflow.parquet", 1));
        }
    }
}

TEST_F(TFormatVerifierTest, RejectsDecimalPhysicalTypesAndWidthsOutsideTheFixedProfile)
{
    struct TPhysicalType
    {
        int Precision;
        parquet::Type::type Physical;
        int Width;
    };
    for (const auto test : std::array{TPhysicalType{9, parquet::Type::INT64, -1},
             TPhysicalType{9, parquet::Type::FIXED_LEN_BYTE_ARRAY, 4},
             TPhysicalType{18, parquet::Type::FIXED_LEN_BYTE_ARRAY, 8},
             TPhysicalType{20, parquet::Type::FIXED_LEN_BYTE_ARRAY, 10},
             TPhysicalType{38, parquet::Type::BYTE_ARRAY, -1}})
    {
        auto output = arrow::io::BufferOutputStream::Create();
        ASSERT_TRUE(output.ok());
        auto field = parquet::schema::PrimitiveNode::Make("value",
            parquet::Repetition::REQUIRED,
            parquet::LogicalType::Decimal(test.Precision, 2),
            test.Physical,
            test.Width,
            1);
        auto schema = parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, {field});
        auto writer =
            parquet::ParquetFileWriter::Open(*output, std::static_pointer_cast<parquet::schema::GroupNode>(schema));
        writer->Close();
        auto bytes = (*output)->Finish();
        ASSERT_TRUE(bytes.ok());
        ASSERT_TRUE(IO_->WriteFile("physical.parquet", (*bytes)->ToString()));
        Schema_ = std::make_shared<iceberg::Schema>(
            std::vector{iceberg::SchemaField::MakeRequired(1, "value", iceberg::decimal(test.Precision, 2))});
        EXPECT_FALSE(CheckData("physical.parquet", 0));
    }
}

TEST_F(TFormatVerifierTest, RejectsMinimumPhysicalDecimalBound)
{
    auto output = arrow::io::BufferOutputStream::Create();
    ASSERT_TRUE(output.ok());
    auto field = parquet::schema::PrimitiveNode::Make("value",
        parquet::Repetition::REQUIRED,
        parquet::LogicalType::Decimal(38, 2),
        parquet::Type::FIXED_LEN_BYTE_ARRAY,
        16,
        1);
    auto schema = parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, {field});
    auto writer =
        parquet::ParquetFileWriter::Open(*output, std::static_pointer_cast<parquet::schema::GroupNode>(schema));
    auto* column = static_cast<parquet::FixedLenByteArrayWriter*>(writer->AppendRowGroup()->NextColumn());
    std::array<std::uint8_t, 16> coefficient{0x80};
    const parquet::FixedLenByteArray value(coefficient.data());
    column->WriteBatch(1, nullptr, nullptr, &value);
    writer->Close();
    const auto bytes = (*output)->Finish();
    ASSERT_TRUE(bytes.ok());
    ASSERT_TRUE(IO_->WriteFile("minimum.parquet", (*bytes)->ToString()));
    Schema_ = std::make_shared<iceberg::Schema>(
        std::vector{iceberg::SchemaField::MakeRequired(1, "value", iceberg::decimal(38, 2))});
    EXPECT_FALSE(CheckData("minimum.parquet", 1));
}

class TCloseFailureInput : public iceberg::InputFile
{
public:
    int CloseCount = 0;
    bool ThrowOnClose = false;

    TCloseFailureInput(std::unique_ptr<iceberg::InputFile> file, bool failRead)
        : File_(std::move(file))
        , FailRead_(failRead)
    { }

    std::string_view location() const override
    {
        return File_->location();
    }
    iceberg::Result<std::int64_t> Size() const override
    {
        return File_->Size();
    }
    iceberg::Result<std::unique_ptr<iceberg::SeekableInputStream>> Open() override
    {
        auto stream = File_->Open();
        if (!stream) {
            return std::unexpected(stream.error());
        }
        return std::make_unique<TStream>(std::move(*stream), *this);
    }

private:
    class TStream : public iceberg::SeekableInputStream
    {
    public:
        TStream(std::unique_ptr<iceberg::SeekableInputStream> stream, TCloseFailureInput& owner)
            : Stream_(std::move(stream))
            , Owner_(owner)
        { }
        iceberg::Result<std::int64_t> Position() const override
        {
            return Stream_->Position();
        }
        iceberg::Status Seek(std::int64_t position) override
        {
            return Stream_->Seek(position);
        }
        iceberg::Result<std::int64_t> Read(std::span<std::byte> bytes) override
        {
            return Stream_->Read(bytes);
        }
        iceberg::Status ReadFully(std::int64_t position, std::span<std::byte> bytes) override
        {
            return Owner_.FailRead_ ? iceberg::IOError("original read error") : Stream_->ReadFully(position, bytes);
        }
        iceberg::Status Close() override
        {
            ++Owner_.CloseCount;
            auto closed = Stream_->Close();
            if (Owner_.ThrowOnClose) {
                throw std::runtime_error("close error");
            }
            return closed ? iceberg::IOError("close error") : closed;
        }

    private:
        std::unique_ptr<iceberg::SeekableInputStream> Stream_;
        TCloseFailureInput& Owner_;
    };
    std::unique_ptr<iceberg::InputFile> File_;
    bool FailRead_;
};

TEST_F(TFormatVerifierTest, ClosesReadStreamsAndPreservesTheFirstError)
{
    ASSERT_NO_FATAL_FAILURE(WriteRows(iceberg::FileFormatType::kParquet, "data.parquet"));
    for (bool throwOnClose : {false, true}) {
        for (bool failRead : {false, true}) {
            auto source = IO_->NewInputFile("data.parquet");
            ASSERT_TRUE(source);
            TCloseFailureInput file(std::move(*source), failRead);
            file.ThrowOnClose = throwOnClose;
            auto size = file.Size();
            ASSERT_TRUE(size);
            const auto result = CheckParquet(file, *Schema_, {*size, 3});
            ASSERT_FALSE(result);
            EXPECT_EQ(result.error().message, failRead ? "original read error" : "close error");
            EXPECT_EQ(file.CloseCount, 1);
        }
    }
}

std::string EncodeCompactZigZagVarint(std::int64_t value)
{
    auto zigzag =
        (static_cast<std::uint64_t>(value) << 1) ^ static_cast<std::uint64_t>(-static_cast<std::int64_t>(value < 0));
    std::string out;
    do {
        std::uint8_t byte = static_cast<std::uint8_t>(zigzag & 0x7fU);
        zigzag >>= 7U;
        if (zigzag != 0) {
            byte |= 0x80U;
        }
        out.push_back(static_cast<char>(byte));
    } while (zigzag != 0);
    return out;
}

TEST_F(TFormatVerifierTest, RejectsDataPageOffsetOutsideDictionaryColumnChunkRange)
{
    // Write 6 rows across 3 row groups (max-row-group-rows = 2) so row group 1's
    // dictionary-encoded chunk has dictionary_page_offset >= 64, data_page_offset >= 64,
    // and dictionary_page_offset + total_compressed_size < dataEnd (all 2-byte Compact
    // Thrift varints). Also test the 2-row-group WriteRows fixture directly.
    ASSERT_NO_FATAL_FAILURE(WriteRows(iceberg::FileFormatType::kParquet, "two_groups.parquet"));
    {
        const auto original = ReadBytes("two_groups.parquet");
        auto reader = parquet::ParquetFileReader::Open(
            std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(original)));
        const auto metadata = reader->metadata();
        ASSERT_EQ(metadata->num_row_groups(), 2);
        const auto chunk = metadata->RowGroup(1)->ColumnChunk(0);
        ASSERT_TRUE(chunk->has_dictionary_page());
        const auto dictOffset = chunk->dictionary_page_offset();
        const auto dataOffset = chunk->data_page_offset();
        ASSERT_LT(dictOffset, dataOffset);
        const std::string needle = std::string("\x26", 1) + EncodeCompactZigZagVarint(dataOffset) +
            std::string("\x26", 1) + EncodeCompactZigZagVarint(dictOffset);
        const auto footerStart = original.size() - 8 - static_cast<std::size_t>(metadata->size());
        const auto pos = original.find(needle, footerStart);
        ASSERT_NE(pos, std::string::npos);

        for (const std::int64_t badDataOffset : {dictOffset - 1, dictOffset}) {
            const std::string replacement = std::string("\x26", 1) + EncodeCompactZigZagVarint(badDataOffset) +
                std::string("\x26", 1) + EncodeCompactZigZagVarint(dictOffset);
            ASSERT_EQ(replacement.size(), needle.size());
            auto mutated = original;
            mutated.replace(pos, needle.size(), replacement);
            ASSERT_TRUE(IO_->WriteFile("bad_two_groups.parquet", mutated));
            EXPECT_FALSE(CheckData("bad_two_groups.parquet", 3)) << badDataOffset;
        }
    }

    iceberg::WriterOptions options;
    options.io = IO_;
    options.path = "three_groups.parquet";
    options.schema = Schema_;
    options.properties = iceberg::WriterProperties::FromMap({{"write.parquet.max-row-group-rows", "2"}});
    auto writer = iceberg::WriterFactoryRegistry::Open(iceberg::FileFormatType::kParquet, options);
    ASSERT_TRUE(writer) << writer.error().message;
    arrow::Int64Builder builder;
    ASSERT_TRUE(builder.AppendValues({1, 2, 3, 4, 5, 6}).ok());
    auto array = builder.Finish();
    ASSERT_TRUE(array.ok());
    auto batch = arrow::RecordBatch::Make(arrow::schema({arrow::field("value", arrow::int64(), false)}), 6, {*array});
    ArrowArray exported{};
    ASSERT_TRUE(arrow::ExportRecordBatch(*batch, &exported).ok());
    const auto written = (*writer)->Write(&exported);
    if (exported.release != nullptr) {
        exported.release(&exported);
    }
    ASSERT_TRUE(written) << written.error().message;
    ASSERT_TRUE((*writer)->Close());

    const auto original = ReadBytes("three_groups.parquet");
    auto reader = parquet::ParquetFileReader::Open(
        std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(original)));
    const auto metadata = reader->metadata();
    ASSERT_EQ(metadata->num_row_groups(), 3);
    const auto chunk = metadata->RowGroup(1)->ColumnChunk(0);
    ASSERT_TRUE(chunk->has_dictionary_page());
    const auto dictOffset = chunk->dictionary_page_offset();
    const auto dataOffset = chunk->data_page_offset();
    const auto chunkEnd = dictOffset + chunk->total_compressed_size();
    const auto dataEnd = static_cast<std::int64_t>(original.size()) - metadata->size() - 8;
    ASSERT_LT(dictOffset, dataOffset);
    ASSERT_LT(dataOffset, chunkEnd);
    ASSERT_LT(chunkEnd, dataEnd);

    const std::string needle = std::string("\x26", 1) + EncodeCompactZigZagVarint(dataOffset) + std::string("\x26", 1) +
        EncodeCompactZigZagVarint(dictOffset);
    const auto footerStart = original.size() - 8 - static_cast<std::size_t>(metadata->size());
    const auto pos = original.find(needle, footerStart);
    ASSERT_NE(pos, std::string::npos);

    for (const std::int64_t badDataOffset : {dictOffset - 1, dictOffset, chunkEnd, chunkEnd + 1}) {
        const std::string replacement = std::string("\x26", 1) + EncodeCompactZigZagVarint(badDataOffset) +
            std::string("\x26", 1) + EncodeCompactZigZagVarint(dictOffset);
        ASSERT_EQ(replacement.size(), needle.size());
        auto mutated = original;
        mutated.replace(pos, needle.size(), replacement);
        ASSERT_TRUE(IO_->WriteFile("bad_three_groups.parquet", mutated));
        const auto checked = CheckData("bad_three_groups.parquet", 6);
        ASSERT_FALSE(checked) << badDataOffset;
        EXPECT_NE(checked.error().message.find("CORRUPTION"), std::string::npos);
    }
}

}
}
