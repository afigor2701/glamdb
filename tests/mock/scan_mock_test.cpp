// Planner/cursor tests over an in-memory iceberg::FileIO: real Parquet, Avro
// and metadata JSON are encoded in RAM; no filesystem access.

#include "core/backend/inert_catalog.h"
#include "core/backend/storage_errors.h"
#include "core/engine/storage_mapping.h"
#include "core/scan/scan_cursor.h"
#include "core/scan/scan_planner.h"
#include "core/storage/format_registry.h"
#include "core/storage/table_profile.h"
#include "tests/support/memory_file_io.h"

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <gtest/gtest.h>
#include <iceberg/file_writer.h>
#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/metrics_config.h>
#include <iceberg/partition_spec.h>
#include <iceberg/snapshot.h>
#include <iceberg/sort_order.h>
#include <iceberg/table.h>
#include <iceberg/table_identifier.h>
#include <iceberg/table_metadata.h>
#include <iceberg/transaction.h>
#include <iceberg/update/fast_append.h>

#include <algorithm>

namespace glamdb::detail {
namespace {

using test::TMemoryFileIO;

constexpr char StoreId[] = "00000000-0000-4000-8000-000000000001";
constexpr char CollectionId[] = "00000000-0000-4000-8000-000000000011";

const SchemaSpec Schema{{
    {"key", FieldType::String, false},
    {"flag", FieldType::Bool, true},
    {"count", FieldType::Int64, true},
    {"score", FieldType::Float64, true},
    {"at", FieldType::TimestampUsUtc, true},
}};

const std::vector<Row> FirstRows{
    {std::string("B"), true, int64_t{1}, 0.0, TimestampUs{10}},
    {std::string("a"), false, int64_t{2}, 1.5, TimestampUs{20}},
    {std::string("\xC3\xA9"), std::monostate{}, std::monostate{}, std::monostate{}, std::monostate{}},
    {std::string("a"), true, int64_t{3}, 2.5, TimestampUs{30}},
};
const std::vector<Row> SecondRows{
    {std::string("a"), true, int64_t{3}, 2.5, TimestampUs{30}},
    {std::string("z"), false, int64_t{9}, -4.0, TimestampUs{90}},
};

std::string RenderCell(const Cell& cell)
{
    return std::visit(
        [](const auto& value) -> std::string {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, std::monostate>) {
                return "null";
            } else if constexpr (std::is_same_v<T, std::string>) {
                return value;
            } else if constexpr (std::is_same_v<T, bool>) {
                return value ? "true" : "false";
            } else if constexpr (std::is_same_v<T, TimestampUs>) {
                return "ts:" + std::to_string(value.value);
            } else {
                return std::to_string(value);
            }
        },
        cell);
}

// Independent oracle: renders input rows without Arrow.
std::vector<std::string> Expected(std::initializer_list<const std::vector<Row>*> inputs, std::vector<size_t> fields)
{
    std::vector<std::string> rows;
    for (const auto* input : inputs) {
        for (const auto& row : *input) {
            std::string text;
            for (size_t index = 0; index < fields.size(); ++index) {
                text += (index == 0 ? "" : "|") + RenderCell(row[fields[index]]);
            }
            rows.push_back(std::move(text));
        }
    }
    std::ranges::sort(rows);
    return rows;
}

std::string RenderArrow(const arrow::Array& array, int64_t row)
{
    if (array.IsNull(row)) {
        return "null";
    }
    switch (array.type_id()) {
        case arrow::Type::STRING:
            return std::string(static_cast<const arrow::StringArray&>(array).GetView(row));
        case arrow::Type::BOOL:
            return static_cast<const arrow::BooleanArray&>(array).Value(row) ? "true" : "false";
        case arrow::Type::INT64:
            return std::to_string(static_cast<const arrow::Int64Array&>(array).Value(row));
        case arrow::Type::DOUBLE:
            return std::to_string(static_cast<const arrow::DoubleArray&>(array).Value(row));
        case arrow::Type::TIMESTAMP:
            return "ts:" + std::to_string(static_cast<const arrow::TimestampArray&>(array).Value(row));
        default:
            return "?";
    }
}

std::shared_ptr<iceberg::DataFile> WriteMemoryParquet(const std::shared_ptr<TMemoryFileIO>& io,
    const iceberg::TableMetadata& metadata,
    const std::string& path,
    const std::vector<Row>& rows)
{
    NGlamDB::NStorage::RegisterStorageFormats();
    const auto batch = MakeRecordBatch(Schema, rows);
    EXPECT_TRUE(batch) << batch.error().message;
    auto schema = metadata.Schema();
    EXPECT_TRUE(schema.has_value());
    iceberg::WriterOptions options;
    options.path = path;
    options.io = io;
    options.schema = *schema;
    auto metricsConfig = iceberg::MetricsConfig::Make(metadata.properties.configs());
    EXPECT_TRUE(metricsConfig.has_value());
    options.metrics_config = *metricsConfig;
    auto writer = iceberg::WriterFactoryRegistry::Open(iceberg::FileFormatType::kParquet, options);
    EXPECT_TRUE(writer.has_value());
    ArrowArray exported{};
    EXPECT_TRUE(arrow::ExportRecordBatch(**batch, &exported).ok());
    EXPECT_TRUE((*writer)->Write(&exported).has_value());
    if (exported.release != nullptr) {
        exported.release(&exported);
    }
    EXPECT_TRUE((*writer)->Close().has_value());

    auto file = std::make_shared<iceberg::DataFile>();
    file->file_path = path;
    file->file_format = iceberg::FileFormatType::kParquet;
    file->content = iceberg::DataFile::Content::kData;
    file->record_count = static_cast<int64_t>(rows.size());
    file->file_size_in_bytes = static_cast<int64_t>(io->Files.at(path).size());
    file->partition_spec_id = metadata.default_spec_id;
    file->sort_order_id = metadata.default_sort_order_id;
    return file;
}

// Two committed snapshots and one unreferenced orphan data file, all in RAM.
class ScanMockTest : public ::testing::Test
{
protected:
    const std::string Table_ = std::string("store://") + StoreId + "/warehouse/" + CollectionId;
    const std::string DataPath1_ = Table_ + "/data/op1-a1-00000.parquet";
    const std::string DataPath2_ = Table_ + "/data/op2-a2-00000.parquet";
    const std::string OrphanPath_ = Table_ + "/data/orphan-00000.parquet";
    std::shared_ptr<TMemoryFileIO> IO_ = std::make_shared<TMemoryFileIO>();
    BaseIdentity Base_;
    CollectionInfo Collection_;
    int64_t Snap1_ = 0;
    int64_t Snap2_ = 0;

    void SetUp() override
    {
        auto profile = NGlamDB::NStorage::MakeTableProfile(ToStorageSchema(Schema));
        ASSERT_TRUE(profile.has_value());
        auto meta0 = iceberg::TableMetadata::Make(*profile->Schema,
            *iceberg::PartitionSpec::Unpartitioned(),
            *iceberg::SortOrder::Unsorted(),
            Table_,
            profile->Properties);
        ASSERT_TRUE(meta0.has_value());
        const auto root0 = Table_ + "/metadata/00000-a0.metadata.json";
        ASSERT_TRUE(iceberg::TableMetadataUtil::Write(*IO_, root0, **meta0).has_value());

        const auto meta1 = Append(**meta0, root0, WriteMemoryParquet(IO_, **meta0, DataPath1_, FirstRows));
        const auto root1 = Table_ + "/metadata/00001-a1.metadata.json";
        ASSERT_TRUE(iceberg::TableMetadataUtil::Write(*IO_, root1, *meta1).has_value());
        Snap1_ = meta1->current_snapshot_id;

        (void)WriteMemoryParquet(IO_, *meta1, OrphanPath_, FirstRows);
        const auto meta2 = Append(*meta1, root1, WriteMemoryParquet(IO_, *meta1, DataPath2_, SecondRows));
        const auto root2 = Table_ + "/metadata/00002-a2.metadata.json";
        ASSERT_TRUE(iceberg::TableMetadataUtil::Write(*IO_, root2, *meta2).has_value());
        Snap2_ = meta2->current_snapshot_id;

        Base_ = {StoreId, CollectionId, meta2->table_uuid, root2, Snap2_, 0, 0};
        Collection_ = {CollectionId, "events", meta2->table_uuid, Schema, 1, Snap2_};
    }

    std::shared_ptr<iceberg::TableMetadata>
    Append(const iceberg::TableMetadata& base, const std::string& root, std::shared_ptr<iceberg::DataFile> file)
    {
        const iceberg::TableIdentifier ident{.ns = iceberg::Namespace{.levels = {"columnar"}}, .name = "events"};
        auto table = iceberg::Table::Make(ident,
            std::make_shared<iceberg::TableMetadata>(base),
            root,
            IO_,
            std::make_shared<TInertCatalog>());
        EXPECT_TRUE(table.has_value());
        auto transaction = iceberg::Transaction::Make(std::move(*table), iceberg::TransactionKind::kUpdate);
        EXPECT_TRUE(transaction.has_value());
        auto append = (*transaction)->NewFastAppend();
        EXPECT_TRUE(append.has_value());
        (*append)->AppendFile(std::move(file));
        EXPECT_TRUE((*append)->Commit().has_value());
        return std::make_shared<iceberg::TableMetadata>((*transaction)->current());
    }

    TScanContext Context(std::function<void(const ErrorDetail&)> onError = {}) const
    {
        return {.Collection = Collection_,
            .Base = Base_,
            .FileIO = IO_,
            .Closing = nullptr,
            .Lease = nullptr,
            .OnError = std::move(onError)};
    }

    std::shared_ptr<ScanCursor> Open(ScanRequest request, std::function<void(const ErrorDetail&)> onError = {})
    {
        request.collection_id = CollectionId;
        auto cursor = OpenScan(Context(std::move(onError)), request);
        EXPECT_TRUE(cursor) << (cursor ? std::string() : cursor.error().message);
        return cursor ? *cursor : nullptr;
    }

    std::vector<std::string> Drain(ScanCursor& cursor)
    {
        std::vector<std::string> rows;
        while (true) {
            const auto batch = cursor.Next();
            EXPECT_TRUE(batch) << (batch ? std::string() : batch.error().message);
            if (!batch || !*batch) {
                break;
            }
            for (int64_t row = 0; row < (*batch)->num_rows(); ++row) {
                std::string text;
                for (int column = 0; column < (*batch)->num_columns(); ++column) {
                    text += (column == 0 ? "" : "|") + RenderArrow(*(*batch)->column(column), row);
                }
                rows.push_back(std::move(text));
            }
        }
        std::ranges::sort(rows);
        return rows;
    }
};

TEST_F(ScanMockTest, PlansHistoricalSnapshotsThroughManifestsOnly)
{
    const auto empty = PlanScan(IO_, Base_, SnapshotSelector::InitialEmpty());
    ASSERT_TRUE(empty) << empty.error().message;
    EXPECT_FALSE(empty->SnapshotID);
    EXPECT_TRUE(empty->Files.empty());

    const auto historical = PlanScan(IO_, Base_, SnapshotSelector::Id(Snap1_));
    ASSERT_TRUE(historical) << historical.error().message;
    ASSERT_EQ(historical->Files.size(), 1U);
    EXPECT_EQ(historical->Files[0].Path, DataPath1_);

    const auto latest = PlanScan(IO_, Base_, SnapshotSelector::Latest());
    ASSERT_TRUE(latest) << latest.error().message;
    ASSERT_EQ(latest->SnapshotID, std::optional<int64_t>(Snap2_));
    ASSERT_EQ(latest->Files.size(), 2U);
    for (const auto& file : latest->Files) {
        EXPECT_NE(file.Path, OrphanPath_);
    }

    const auto unknown = PlanScan(IO_, Base_, SnapshotSelector::Id(Snap2_ + 1));
    ASSERT_FALSE(unknown);
    EXPECT_EQ(unknown.error().code, "NOT_FOUND");
}

TEST_F(ScanMockTest, ReturnsExactRowsProjectionAndNulls)
{
    const auto all = Open({});
    ASSERT_NE(all, nullptr);
    EXPECT_EQ(Drain(*all), Expected({&FirstRows, &SecondRows}, {0, 1, 2, 3, 4}));

    const auto historical = Open({.selector = SnapshotSelector::Id(Snap1_)});
    ASSERT_NE(historical, nullptr);
    EXPECT_EQ(Drain(*historical), Expected({&FirstRows}, {0, 1, 2, 3, 4}));

    const auto projected = Open({.projection = std::vector<std::string>{"at", "key"},
        .predicate = Predicate::Compare("count", CompareOp::Ge, int64_t{3})});
    ASSERT_NE(projected, nullptr);
    EXPECT_EQ(Drain(*projected), (std::vector<std::string>{"ts:30|a", "ts:30|a", "ts:90|z"}));

    const auto empty = Open({.selector = SnapshotSelector::InitialEmpty()});
    ASSERT_NE(empty, nullptr);
    EXPECT_TRUE(Drain(*empty).empty());

    const auto zero = Open({.limit = 0});
    ASSERT_NE(zero, nullptr);
    const auto eos = zero->Next();
    ASSERT_TRUE(eos);
    EXPECT_EQ(*eos, nullptr);
}

TEST_F(ScanMockTest, CancelInsideReadIsDeterministic)
{
    const auto cursor = Open({.selector = SnapshotSelector::Id(Snap1_)});
    ASSERT_NE(cursor, nullptr);
    IO_->OnRead = [&](const std::string& path) {
        if (path == DataPath1_) {
            cursor->Cancel();
        }
    };
    const auto next = cursor->Next();
    IO_->OnRead = nullptr;
    ASSERT_FALSE(next);
    EXPECT_EQ(next.error().code, "CANCELLED");
}

TEST_F(ScanMockTest, MissingOrTruncatedDataIsStickyCorruption)
{
    for (const bool truncate : {false, true}) {
        SCOPED_TRACE(truncate ? "truncated" : "missing");
        std::vector<ErrorDetail> reported;
        const auto cursor = Open({.selector = SnapshotSelector::Id(Snap1_)}, [&](const ErrorDetail& error) {
            reported.push_back(error);
        });
        ASSERT_NE(cursor, nullptr);
        const auto saved = IO_->Files.at(DataPath1_);
        if (truncate) {
            IO_->Files[DataPath1_] = saved.substr(0, saved.size() / 2);
        } else {
            IO_->Files.erase(DataPath1_);
        }
        const auto first = cursor->Next();
        IO_->Files[DataPath1_] = saved;
        ASSERT_FALSE(first);
        EXPECT_EQ(first.error().code, "CORRUPTION");
        ASSERT_EQ(reported.size(), 1U);
        EXPECT_EQ(reported.front().code, "CORRUPTION");
        // Failures are sticky, never EOS, even after the file reappears.
        const auto second = cursor->Next();
        ASSERT_FALSE(second);
        EXPECT_EQ(second.error().code, "CORRUPTION");
    }
}

TEST_F(ScanMockTest, MissingOrMalformedManifestIsCorruption)
{
    // Latest planning reads every data manifest, so any of them will do.
    std::string manifest;
    for (const auto& [path, bytes] : IO_->Files) {
        if (path.ends_with(".avro") && path.find("/snap-") == std::string::npos) {
            manifest = path;
        }
    }
    ASSERT_FALSE(manifest.empty());
    const auto saved = IO_->Files.at(manifest);

    IO_->Files[manifest] = "X" + saved.substr(1);
    const auto malformed = PlanScan(IO_, Base_, SnapshotSelector::Latest());
    ASSERT_FALSE(malformed);
    EXPECT_EQ(malformed.error().code, "CORRUPTION");

    IO_->Files[manifest] = saved.substr(0, saved.size() / 2);
    const auto truncated = PlanScan(IO_, Base_, SnapshotSelector::Latest());
    ASSERT_FALSE(truncated);
    EXPECT_EQ(truncated.error().code, "CORRUPTION");

    IO_->Files.erase(manifest);
    const auto missing = PlanScan(IO_, Base_, SnapshotSelector::Latest());
    ASSERT_FALSE(missing);
    EXPECT_EQ(missing.error().code, "CORRUPTION");
}

// ---- Error classification table (see SCANNER.md) ----

TEST(StorageErrorClassification, FilesystemErrors)
{
    using enum std::errc;
    const auto code = [](std::errc error, EStoragePath path) {
        return std::string(ClassifyFsError(std::make_error_code(error), path));
    };
    for (const auto path : {EStoragePath::Read, EStoragePath::Write, EStoragePath::Preflight}) {
        EXPECT_EQ(code(state_not_recoverable, path), "CORRUPTION");
        EXPECT_EQ(code(too_many_symbolic_link_levels, path), "CORRUPTION");
        EXPECT_EQ(code(not_a_directory, path), "CORRUPTION");
        EXPECT_EQ(code(cross_device_link, path), "CORRUPTION");
        EXPECT_EQ(code(no_such_file_or_directory, path), "CORRUPTION");
        EXPECT_EQ(code(no_space_on_device, path), "RESOURCE_EXHAUSTED");
        EXPECT_EQ(code(value_too_large, path), "RESOURCE_EXHAUSTED");
        EXPECT_EQ(code(not_enough_memory, path), "RESOURCE_EXHAUSTED");
        EXPECT_EQ(code(io_error, path), "IO_ERROR");
        EXPECT_EQ(code(read_only_file_system, path), "IO_ERROR");
        EXPECT_EQ(code(permission_denied, path), "IO_ERROR");
    }
    EXPECT_EQ(code(invalid_argument, EStoragePath::Preflight), "CORRUPTION");
    EXPECT_EQ(code(invalid_argument, EStoragePath::Read), "CORRUPTION");
    EXPECT_EQ(code(invalid_argument, EStoragePath::Write), "IO_ERROR");
    EXPECT_EQ(code(file_too_large, EStoragePath::Read), "CORRUPTION");
    EXPECT_EQ(code(file_too_large, EStoragePath::Write), "RESOURCE_EXHAUSTED");
    EXPECT_EQ(StorageError(std::make_error_code(no_space_on_device), EStoragePath::Write, "x").retry_hint,
        RetryHint::OperatorAction);
}

TEST(StorageErrorClassification, IcebergErrors)
{
    using iceberg::ErrorKind;
    const auto code = [](ErrorKind kind, std::string message, EStoragePath path) {
        return StorageError(iceberg::Error{kind, std::move(message)}, path, "ctx", "IO_ERROR").code;
    };
    for (const auto kind : {ErrorKind::kInvalid,
             ErrorKind::kInvalidArgument,
             ErrorKind::kNotFound,
             ErrorKind::kValidationFailed,
             ErrorKind::kDecompressError,
             ErrorKind::kUnknownError,
             ErrorKind::kInvalidManifest,
             ErrorKind::kInvalidManifestList,
             ErrorKind::kInvalidSchema,
             ErrorKind::kJsonParseError})
    {
        EXPECT_EQ(code(kind, "bad", EStoragePath::Read), "CORRUPTION") << static_cast<int>(kind);
    }
    EXPECT_EQ(code(ErrorKind::kIOError, "eio", EStoragePath::Read), "IO_ERROR");
    EXPECT_EQ(code(ErrorKind::kValidationFailed, "CAPACITY: full", EStoragePath::Read), "CORRUPTION");
    EXPECT_EQ(code(ErrorKind::kValidationFailed, "CAPACITY: full", EStoragePath::Write), "RESOURCE_EXHAUSTED");
    EXPECT_EQ(code(ErrorKind::kInvalidArgument, "CAPACITY: full", EStoragePath::Write), "RESOURCE_EXHAUSTED");
    EXPECT_EQ(code(ErrorKind::kIOError, "CORRUPTION: moved", EStoragePath::Write), "CORRUPTION");
    EXPECT_EQ(code(ErrorKind::kInvalid, "no receipt", EStoragePath::Write), "IO_ERROR");
    EXPECT_EQ(code(ErrorKind::kValidationFailed, "bad candidate", EStoragePath::Write), "IO_ERROR");
}

} // namespace
} // namespace glamdb::detail
