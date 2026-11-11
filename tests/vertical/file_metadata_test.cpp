#include "tests/vertical/vertical_support.h"

#include "core/storage/format_verifier.h"

#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/manifest/manifest_reader.h>
#include <iceberg/snapshot.h>
#include <iceberg/table_metadata.h>
#include <parquet/file_reader.h>
#include <parquet/statistics.h>

#include <chrono>

namespace glamdb {
namespace {

using namespace vertical;
using namespace std::chrono_literals;

class TFileMetadataTest : public ::testing::Test
{
protected:
    test::TempDir Temporary_;
    std::filesystem::path Root_ = Temporary_.path / "store";
    std::shared_ptr<THookBackend> Backend_;
    std::shared_ptr<Engine> Engine_;

    void SetUp() override
    {
        detail::TLocalStorageOptions options;
        options.Writer.MaxRowGroupBytes = 4 + 3 * 40;
        auto created = detail::EngineFactory::CreateWith({test::store_id, Root_.string()},
            THookBackend::Factory(Backend_, options));
        ASSERT_TRUE(created) << created.error().message;
        Engine_ = *created;
        ASSERT_EQ(Engine_->CreateCollection({test::collection_id, "events", Schema}).disposition,
            CommitDisposition::Committed);
    }

    void TearDown() override
    {
        EXPECT_TRUE(Engine_->Shutdown(1000ms));
    }

    AppendResult Append(const char* operation, const std::vector<TInputRow>& rows)
    {
        const auto result = Engine_->AppendDurable({test::collection_id, operation, {}, {Batch(rows)}});
        EXPECT_EQ(result.disposition, CommitDisposition::Committed);
        return result.result.value_or(AppendResult{});
    }
};

TEST_F(TFileMetadataTest, ListsStableFilesFromTheSelectedSnapshot)
{
    const auto empty = Engine_->ListFiles(test::collection_id);
    ASSERT_TRUE(empty) << empty.error().message;
    EXPECT_TRUE(empty->Files.empty());
    EXPECT_EQ(empty->SelectorResolved, SnapshotSelector::InitialEmpty());

    const auto a = Append(FirstOperation, FirstRows);
    const auto first = Engine_->ListFiles(test::collection_id);
    ASSERT_TRUE(first) << first.error().message;
    ASSERT_EQ(first->Files.size(), 1U);
    const auto file = first->Files.front();
    EXPECT_EQ(file.RecordCount, 3);
    EXPECT_EQ(file.CompressedBytes, a.compressed_data_bytes_added);
    EXPECT_EQ(file.FileID.size(), 64U);
    EXPECT_EQ(file.Format, "parquet");

    const auto b = Append(SecondOperation, SecondRows);
    const auto latest = Engine_->ListFiles(test::collection_id);
    const auto old = Engine_->ListFiles(test::collection_id, SnapshotSelector::Id(a.snapshot_id));
    ASSERT_TRUE(latest);
    ASSERT_TRUE(old);
    ASSERT_EQ(latest->Files.size(), 2U);
    EXPECT_EQ(old->Files, first->Files);
    EXPECT_EQ(old->Identity, latest->Identity);
    EXPECT_EQ(old->SelectorResolved, SnapshotSelector::Id(a.snapshot_id));
    EXPECT_EQ(latest->SelectorResolved, SnapshotSelector::Id(b.snapshot_id));
    EXPECT_EQ(latest->Identity.current_snapshot_id, b.snapshot_id);
    const auto dataDirectory = Root_ / "warehouse" / test::collection_id / "data";
    std::filesystem::copy_file(FilesIn(dataDirectory).front(), dataDirectory / "orphan-00000.parquet");
    const auto withOrphan = Engine_->ListFiles(test::collection_id);
    ASSERT_TRUE(withOrphan);
    EXPECT_EQ(withOrphan->Files, latest->Files);
    const auto newer = std::ranges::find_if(latest->Files, [&](const auto& item) {
        return item.FileID != file.FileID;
    });
    ASSERT_NE(newer, latest->Files.end());
    const auto outside =
        Engine_->GetFileDetails(test::collection_id, newer->FileID, SnapshotSelector::Id(a.snapshot_id));
    ASSERT_FALSE(outside);
    EXPECT_EQ(outside.error().code, "NOT_FOUND");
    const auto initial = Engine_->GetFileDetails(test::collection_id, file.FileID, SnapshotSelector::InitialEmpty());
    ASSERT_FALSE(initial);
    EXPECT_EQ(initial.error().code, "NOT_FOUND");
    const auto missing = Engine_->ListFiles(test::collection_id, SnapshotSelector::Id(-7));
    ASSERT_FALSE(missing);
    EXPECT_EQ(missing.error().code, "NOT_FOUND");

    ASSERT_TRUE(Engine_->Shutdown(1000ms));
    Engine_.reset();
    Backend_.reset();
    auto reopened = OpenExisting({Root_.string(), test::store_id});
    ASSERT_TRUE(reopened) << reopened.error().message;
    Engine_ = *reopened;
    const auto retained = Engine_->ListFiles(test::collection_id, SnapshotSelector::Id(a.snapshot_id));
    ASSERT_TRUE(retained);
    EXPECT_EQ(retained->Files, first->Files);
    EXPECT_TRUE(Engine_->GetFileDetails(test::collection_id, file.FileID));
}

TEST_F(TFileMetadataTest, MatchesIndependentManifestAndMultiRowGroupFooterInspection)
{
    std::vector<TInputRow> rows;
    rows.reserve(9);
    for (int index = 0; index < 9; ++index) {
        rows.push_back({"key-0" + std::to_string(index), index, std::nullopt, true, index});
    }
    Append(FirstOperation, rows);
    const auto listing = Engine_->ListFiles(test::collection_id);
    ASSERT_TRUE(listing) << listing.error().message;
    ASSERT_EQ(listing->Files.size(), 1U);
    const auto& file = listing->Files.front();
    const auto details = Engine_->GetFileDetails(test::collection_id, file.FileID);
    ASSERT_TRUE(details) << details.error().message;
    EXPECT_EQ(details->File, file);
    EXPECT_EQ(details->Identity, listing->Identity);
    EXPECT_EQ(details->SelectorResolved, listing->SelectorResolved);

    const auto io = Backend_->ReadOnlyFileIO();
    ASSERT_TRUE(io);
    const auto root = NGlamDB::NStorage::ReadTableMetadata(**io, listing->Identity.metadata_location);
    ASSERT_TRUE(root);
    const auto snapshot = (*root)->Snapshot();
    ASSERT_TRUE(snapshot);
    const iceberg::SnapshotReader snapshotReader(snapshot->get());
    const auto manifests = snapshotReader.Manifests(*io);
    ASSERT_TRUE(manifests);
    ASSERT_EQ(manifests->size(), 1U);
    const auto reader = iceberg::ManifestReader::Make(manifests->front(),
        *io,
        (*root)->Schema().value(),
        (*root)->PartitionSpec().value());
    ASSERT_TRUE(reader);
    const auto entries = (*reader)->LiveEntries();
    ASSERT_TRUE(entries) << entries.error().message;
    ASSERT_EQ(entries->size(), 1U);
    const auto& data = *entries->front().data_file;
    EXPECT_EQ(file.RecordCount, data.record_count);
    EXPECT_EQ(file.CompressedBytes, data.file_size_in_bytes);
    ASSERT_EQ(file.FieldMetrics.size(), 5U);
    for (const auto& metric : file.FieldMetrics) {
        EXPECT_EQ(metric.CompressedBytes, data.column_sizes.at(metric.FieldID));
        EXPECT_EQ(metric.ValueCount, data.value_counts.at(metric.FieldID));
        EXPECT_EQ(metric.NullCount, data.null_value_counts.at(metric.FieldID));
        if (data.lower_bounds.contains(metric.FieldID)) {
            EXPECT_EQ(metric.LowerBound, data.lower_bounds.at(metric.FieldID));
            EXPECT_EQ(metric.UpperBound, data.upper_bounds.at(metric.FieldID));
        } else {
            EXPECT_FALSE(metric.LowerBound);
            EXPECT_FALSE(metric.UpperBound);
        }
    }
    EXPECT_EQ(file.FieldMetrics[2].NullCount, 9);
    EXPECT_FALSE(file.FieldMetrics[2].LowerBound);
    EXPECT_EQ(file.FieldMetrics[2].NanCount, 0);
    EXPECT_FALSE(file.FieldMetrics[0].NanCount);

    const auto path =
        Root_ / "warehouse" / test::collection_id / "data" / std::filesystem::path(data.file_path).filename();
    EXPECT_EQ(file.CompressedBytes, static_cast<int64_t>(std::filesystem::file_size(path)));
    const auto parquet = parquet::ParquetFileReader::OpenFile(path.string());
    const auto footer = parquet->metadata();
    ASSERT_EQ(footer->num_row_groups(), 3);
    ASSERT_EQ(details->RowGroups.size(), 3U);
    for (int ordinal = 0; ordinal < footer->num_row_groups(); ++ordinal) {
        const auto group = footer->RowGroup(ordinal);
        const auto& reported = details->RowGroups[ordinal];
        EXPECT_EQ(reported.Ordinal, ordinal);
        EXPECT_EQ(reported.RecordCount, 3);
        EXPECT_EQ(reported.RecordCount, group->num_rows());
        EXPECT_EQ(reported.UncompressedBytes, group->total_byte_size());
        ASSERT_EQ(reported.Columns.size(), 5U);
        int64_t compressed = 0;
        for (int column = 0; column < group->num_columns(); ++column) {
            const auto chunk = group->ColumnChunk(column);
            const auto& actual = reported.Columns[column];
            EXPECT_EQ(actual.FieldID, footer->schema()->Column(column)->schema_node()->field_id());
            EXPECT_EQ(actual.FieldID, column + 1);
            EXPECT_EQ(actual.ValueCount, chunk->num_values());
            EXPECT_EQ(actual.CompressedBytes, chunk->total_compressed_size());
            EXPECT_EQ(actual.UncompressedBytes, chunk->total_uncompressed_size());
            EXPECT_EQ(actual.DataPageOffset, chunk->data_page_offset());
            EXPECT_EQ(actual.DictionaryPageOffset,
                chunk->has_dictionary_page() ? std::optional(chunk->dictionary_page_offset()) : std::nullopt);
            EXPECT_FALSE(actual.Compression.empty());
            EXPECT_EQ(actual.HasColumnIndex, chunk->GetColumnIndexLocation().has_value());
            EXPECT_EQ(actual.HasOffsetIndex, chunk->GetOffsetIndexLocation().has_value());
            if (const auto stats = chunk->encoded_statistics()) {
                EXPECT_EQ(actual.NullCount, stats->has_null_count ? std::optional(stats->null_count) : std::nullopt);
                EXPECT_EQ(actual.LowerBound,
                    stats->has_min ? std::optional(std::vector<uint8_t>(stats->min().begin(), stats->min().end()))
                                   : std::nullopt);
                EXPECT_EQ(actual.UpperBound,
                    stats->has_max ? std::optional(std::vector<uint8_t>(stats->max().begin(), stats->max().end()))
                                   : std::nullopt);
            }
            compressed += chunk->total_compressed_size();
        }
        EXPECT_EQ(reported.CompressedBytes, compressed);
        EXPECT_EQ(reported.Columns[2].NullCount, 3);
        EXPECT_FALSE(reported.Columns[2].LowerBound);
        EXPECT_FALSE(reported.Columns[2].UpperBound);
    }
}

TEST_F(TFileMetadataTest, RejectsClientPathsUnknownIDsAndForeignCollectionFiles)
{
    Append(FirstOperation, FirstRows);
    ASSERT_EQ(Engine_->CreateCollection({SecondCollection, "other", Schema}).disposition, CommitDisposition::Committed);
    ASSERT_EQ(Engine_->AppendDurable({SecondCollection, FirstOperation, {}, {Batch(FirstRows)}}).disposition,
        CommitDisposition::Committed);
    const auto foreign = Engine_->ListFiles(SecondCollection);
    ASSERT_TRUE(foreign);
    ASSERT_EQ(foreign->Files.size(), 1U);
    const auto rejected = Engine_->GetFileDetails(test::collection_id, foreign->Files.front().FileID);
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error().code, "NOT_FOUND");
    for (const auto& path : {std::string(),
             std::string("../catalog.sqlite"),
             Root_.string(),
             std::string("store://") + test::store_id + "/warehouse/" + test::collection_id + "/data/a.parquet"})
    {
        const auto invalid = Engine_->GetFileDetails(test::collection_id, path);
        ASSERT_FALSE(invalid);
        EXPECT_EQ(invalid.error().code, "INVALID_ARGUMENT");
    }
    const auto unknown = Engine_->GetFileDetails(test::collection_id, std::string(64, '0'));
    ASSERT_FALSE(unknown);
    EXPECT_EQ(unknown.error().code, "NOT_FOUND");
    EXPECT_TRUE(Engine_->GetHealth().read_available);
}

TEST_F(TFileMetadataTest, DetectsChangedFileLengthAndFooterCorruption)
{
    Append(FirstOperation, FirstRows);
    const auto listing = Engine_->ListFiles(test::collection_id);
    ASSERT_TRUE(listing);
    const auto path = FilesIn(Root_ / "warehouse" / test::collection_id / "data").front();
    const auto original = ReadBytes(path);
    for (const bool truncate : {false, true}) {
        auto damaged = original;
        if (truncate) {
            damaged.pop_back();
        } else {
            damaged[0] = 'X';
        }
        WriteBytes(path, damaged);
        const auto failed = Engine_->GetFileDetails(test::collection_id, listing->Files.front().FileID);
        ASSERT_FALSE(failed);
        EXPECT_EQ(failed.error().code, "CORRUPTION");
        EXPECT_FALSE(Engine_->GetHealth().read_available);
        WriteBytes(path, original);
        ASSERT_TRUE(Engine_->Reconcile());
    }
}

TEST_F(TFileMetadataTest, RejectsColumnChunkPathsAndTypesThatDifferFromTheSchema)
{
    Append(FirstOperation, FirstRows);
    const auto listing = Engine_->ListFiles(test::collection_id);
    ASSERT_TRUE(listing);
    const auto path = FilesIn(Root_ / "warehouse" / test::collection_id / "data").front();
    const auto original = ReadBytes(path);
    const auto reader = parquet::ParquetFileReader::OpenFile(path.string());
    const auto footerStart = original.size() - 8 - reader->metadata()->size();
    // Compact Thrift: field 3 is a one-element binary list (path_in_schema).
    const auto chunkPath = original.find(std::string("\x19\x18\x03") + "key", footerStart);
    ASSERT_NE(chunkPath, std::string::npos);
    // ColumnMetaData starts with BYTE_ARRAY (6, zigzag 12), then encodings.
    const auto chunkType = original.rfind(std::string("\x15\x0c\x19"), chunkPath);
    ASSERT_NE(chunkType, std::string::npos);
    ASSERT_GE(chunkType, footerStart);
    for (const bool changeType : {false, true}) {
        auto damaged = original;
        if (changeType) {
            damaged[chunkType + 1] = '\x04'; // INT64 (2, zigzag 4)
        } else {
            damaged.replace(chunkPath + 3, 3, "xxx");
        }
        WriteBytes(path, damaged);
        const auto altered = parquet::ParquetFileReader::OpenFile(path.string())->metadata();
        const auto group = altered->RowGroup(0);
        const auto chunk = group->ColumnChunk(0);
        EXPECT_EQ(altered->schema()->Column(0)->physical_type(), parquet::Type::BYTE_ARRAY);
        EXPECT_EQ(chunk->type(), changeType ? parquet::Type::INT64 : parquet::Type::BYTE_ARRAY);
        EXPECT_EQ(chunk->path_in_schema()->ToDotString(), changeType ? "key" : "xxx");
        const auto failed = Engine_->GetFileDetails(test::collection_id, listing->Files.front().FileID);
        EXPECT_FALSE(failed);
        if (!failed) {
            EXPECT_EQ(failed.error().code, "CORRUPTION");
        }
        EXPECT_FALSE(Engine_->GetHealth().read_available);
        WriteBytes(path, original);
        ASSERT_TRUE(Engine_->Reconcile());
    }
}

} // namespace
} // namespace glamdb
