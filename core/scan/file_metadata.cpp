#include "core/scan/file_metadata.h"

#include "core/api/binary.h"
#include "core/api/canonical.h"
#include "core/backend/storage_errors.h"
#include "core/io/store_uri.h"
#include "core/scan/scan_planner.h"
#include "core/storage/format_verifier.h"

#include <arrow/util/compression.h>
#include <iceberg/file_io.h>
#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/schema.h>
#include <parquet/exception.h>
#include <parquet/metadata.h>
#include <parquet/statistics.h>
#include <parquet/types.h>

#include <algorithm>
#include <limits>
#include <map>
#include <set>
#include <type_traits>

namespace glamdb::detail {
namespace {

template <class T>
T Take(Result<T> result)
{
    if (!result) {
        throw Failure(result.error());
    }
    return std::move(*result);
}

template <class T>
T TakeStorage(iceberg::Result<T> result, std::string_view context)
{
    if (!result) {
        throw Failure(StorageError(result.error(), context, EStoragePath::Read, "IO_ERROR"));
    }
    return std::move(*result);
}

template <class T>
std::optional<T> Lookup(const std::map<int32_t, T>& values, int32_t fieldID)
{
    const auto it = values.find(fieldID);
    return it == values.end() ? std::nullopt : std::optional(it->second);
}

SnapshotSelector ResolveSelector(const TScanManifests& plan)
{
    return plan.SnapshotID ? SnapshotSelector::Id(*plan.SnapshotID) : SnapshotSelector::InitialEmpty();
}

std::string MakeFileID(const BaseIdentity& base, const iceberg::DataFile& file)
{
    const auto uri = NGlamDB::NIO::TStoreUri::Parse(file.file_path, base.store_id);
    Require(uri && uri.Value().CollectionId() == base.collection_id &&
            uri.Value().Kind() == NGlamDB::NIO::EFileKind::Data,
        "CORRUPTION",
        "Manifest data file does not belong to this collection.");
    Encoder encoded;
    encoded.Text("glamdb-file-v1");
    encoded.Text(base.table_uuid);
    encoded.Text(file.file_path);
    return Sha256(encoded.bytes);
}

template <class T>
void CheckFieldIDs(const std::map<int32_t, T>& values, const iceberg::Schema& schema)
{
    for (const auto& entry : values) {
        const auto fieldID = entry.first;
        const auto& value = entry.second;
        Require(std::ranges::any_of(schema.fields(),
                    [&](const auto& field) {
                        return field.field_id() == fieldID;
                    }),
            "CORRUPTION",
            "Manifest metrics contain an unknown field ID.");
        if constexpr (std::is_same_v<T, int64_t>) {
            Require(value >= 0, "CORRUPTION", "Manifest metrics contain a negative count.");
        }
    }
}

TDataFileMetadata DescribeFile(const BaseIdentity& base, const iceberg::DataFile& file, const iceberg::Schema& schema)
{
    TDataFileMetadata result{.FileID = MakeFileID(base, file),
        .RecordCount = file.record_count,
        .CompressedBytes = file.file_size_in_bytes};
    CheckFieldIDs(file.column_sizes, schema);
    CheckFieldIDs(file.value_counts, schema);
    CheckFieldIDs(file.null_value_counts, schema);
    CheckFieldIDs(file.nan_value_counts, schema);
    CheckFieldIDs(file.lower_bounds, schema);
    CheckFieldIDs(file.upper_bounds, schema);
    for (const auto& field : schema.fields()) {
        const auto id = field.field_id();
        TFileFieldMetrics metrics{.FieldID = id,
            .CompressedBytes = Lookup(file.column_sizes, id),
            .ValueCount = Lookup(file.value_counts, id),
            .NullCount = Lookup(file.null_value_counts, id),
            .NanCount = Lookup(file.nan_value_counts, id),
            .LowerBound = Lookup(file.lower_bounds, id),
            .UpperBound = Lookup(file.upper_bounds, id)};
        Require((!metrics.CompressedBytes || *metrics.CompressedBytes <= file.file_size_in_bytes) &&
                (!metrics.ValueCount || *metrics.ValueCount == file.record_count) &&
                (!metrics.NullCount || *metrics.NullCount <= file.record_count) &&
                (!metrics.NanCount || *metrics.NanCount <= file.record_count - metrics.NullCount.value_or(0)),
            "CORRUPTION",
            "Manifest field metrics do not match the file.");
        result.FieldMetrics.push_back(std::move(metrics));
    }
    return result;
}

void AddBytes(int64_t& total, int64_t bytes)
{
    Require(bytes >= 0 && total <= std::numeric_limits<int64_t>::max() - bytes,
        "CORRUPTION",
        "Parquet byte counts overflow.");
    total += bytes;
}

void CheckRange(int64_t offset, int64_t length, int64_t end)
{
    Require(offset >= 4 && offset < end && length >= 0 && length <= end - offset,
        "CORRUPTION",
        "Parquet column range is outside the file data.");
}

TColumnChunkMetadata DescribeColumn(const parquet::ColumnChunkMetaData& chunk,
    const parquet::ColumnDescriptor& column,
    const TRowGroupMetadata& group,
    int64_t dataEnd)
{
    const auto rows = group.RecordCount;
    TColumnChunkMetadata result{.FieldID = column.schema_node()->field_id(),
        .ValueCount = chunk.num_values(),
        .CompressedBytes = chunk.total_compressed_size(),
        .UncompressedBytes = chunk.total_uncompressed_size(),
        .DataPageOffset = chunk.data_page_offset(),
        .Compression = arrow::util::Codec::GetCodecAsString(chunk.compression()),
        .HasColumnIndex = chunk.GetColumnIndexLocation().has_value(),
        .HasOffsetIndex = chunk.GetOffsetIndexLocation().has_value()};
    Require(chunk.file_path().empty() && result.ValueCount == rows && result.UncompressedBytes >= 0,
        "CORRUPTION",
        "Parquet column metadata does not match the row group.");
    if (chunk.has_dictionary_page()) {
        result.DictionaryPageOffset = chunk.dictionary_page_offset();
    }
    CheckRange(result.DataPageOffset, 0, dataEnd);
    CheckRange(result.DictionaryPageOffset.value_or(result.DataPageOffset), result.CompressedBytes, dataEnd);
    for (const auto& index : {chunk.GetColumnIndexLocation(), chunk.GetOffsetIndexLocation()}) {
        if (index) {
            CheckRange(index->offset, index->length, dataEnd);
        }
    }
    if (const auto stats = chunk.encoded_statistics()) {
        if (stats->has_null_count) {
            Require(stats->null_count >= 0 && stats->null_count <= rows,
                "CORRUPTION",
                "Parquet null count is outside the row group.");
            result.NullCount = stats->null_count;
        }
        if (stats->has_min) {
            result.LowerBound = std::vector<uint8_t>(stats->min().begin(), stats->min().end());
        }
        if (stats->has_max) {
            result.UpperBound = std::vector<uint8_t>(stats->max().begin(), stats->max().end());
        }
    }
    return result;
}

std::vector<TRowGroupMetadata> DescribeRowGroups(const parquet::FileMetaData& footer, const TDataFileMetadata& file)
{
    const auto dataEnd = file.CompressedBytes - footer.size() - 8;
    std::vector<TRowGroupMetadata> groups;
    std::vector<int64_t> compressed(file.FieldMetrics.size(), 0);
    std::vector<std::optional<int64_t>> nulls(file.FieldMetrics.size(), 0);
    for (int ordinal = 0; ordinal < footer.num_row_groups(); ++ordinal) {
        const auto group = footer.RowGroup(ordinal);
        TRowGroupMetadata result{.Ordinal = ordinal,
            .RecordCount = group->num_rows(),
            .UncompressedBytes = group->total_byte_size()};
        int64_t uncompressed = 0;
        for (int column = 0; column < group->num_columns(); ++column) {
            const auto chunk = group->ColumnChunk(column);
            auto descriptor = DescribeColumn(*chunk, *footer.schema()->Column(column), result, dataEnd);
            AddBytes(result.CompressedBytes, descriptor.CompressedBytes);
            AddBytes(uncompressed, descriptor.UncompressedBytes);
            AddBytes(compressed[column], descriptor.CompressedBytes);
            auto& nullCount = nulls[column];
            if (nullCount && descriptor.NullCount) {
                AddBytes(*nullCount, *descriptor.NullCount);
            } else {
                nullCount.reset();
            }
            result.Columns.push_back(std::move(descriptor));
        }
        Require(result.UncompressedBytes == uncompressed,
            "CORRUPTION",
            "Parquet row group byte counts differ from its columns.");
        groups.push_back(std::move(result));
    }
    for (size_t column = 0; column < file.FieldMetrics.size(); ++column) {
        const auto& metrics = file.FieldMetrics[column];
        Require((!metrics.CompressedBytes || *metrics.CompressedBytes == compressed[column]) &&
                (!metrics.NullCount || !nulls[column] || metrics.NullCount == nulls[column]),
            "CORRUPTION",
            "Parquet field metrics differ from the manifest.");
    }
    return groups;
}

} // namespace

TFileListing ReadFiles(const TScanContext& context, const SnapshotSelector& selector)
{
    const auto plan = Take(PlanManifests(context.FileIO, context.Base, selector, context.Cache.get()));
    TFileListing listing{.Identity = context.Base, .SelectorResolved = ResolveSelector(plan)};
    std::set<std::string> seen;
    for (size_t index = 0; index < plan.Manifests.size(); ++index) {
        const auto files = Take(ReadManifestDataFiles(context.FileIO, plan, index, context.Cache.get()));
        for (const auto& file : *files) {
            auto descriptor = DescribeFile(context.Base, *file, *plan.Schema);
            Require(seen.insert(descriptor.FileID).second,
                "CORRUPTION",
                "Selected snapshot contains a duplicate data file.");
            listing.Files.push_back(std::move(descriptor));
        }
    }
    std::ranges::sort(listing.Files, {}, &TDataFileMetadata::FileID);
    return listing;
}

TFileDetails ReadFileDetails(const TScanContext& context, std::string_view fileID, const SnapshotSelector& selector)
{
    Require(fileID.size() == 64 &&
            std::ranges::all_of(fileID,
                [](char c) {
                    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
                }),
        "INVALID_ARGUMENT",
        "File ID must be an opaque ID returned by ListFiles.");
    const auto plan = Take(PlanManifests(context.FileIO, context.Base, selector, context.Cache.get()));
    std::shared_ptr<const iceberg::DataFile> selected;
    for (size_t index = 0; index < plan.Manifests.size(); ++index) {
        const auto files = Take(ReadManifestDataFiles(context.FileIO, plan, index, context.Cache.get()));
        for (const auto& file : *files) {
            if (MakeFileID(context.Base, *file) == fileID) {
                Require(!selected, "CORRUPTION", "Selected snapshot contains a duplicate data file.");
                selected = file;
            }
        }
    }
    Require(selected != nullptr, "NOT_FOUND", "File does not belong to the selected snapshot.");
    TFileDetails result{.Identity = context.Base,
        .SelectorResolved = ResolveSelector(plan),
        .File = DescribeFile(context.Base, *selected, *plan.Schema)};
    try {
        auto input = TakeStorage(context.FileIO->NewInputFile(selected->file_path), "open selected data file");
        const auto footer = TakeStorage(NGlamDB::NStorage::ReadParquetFooter(*input,
                                            *plan.Schema,
                                            {selected->file_size_in_bytes, selected->record_count}),
            "read selected Parquet footer");
        result.FooterBytes = footer->size();
        result.RowGroups = DescribeRowGroups(*footer, result.File);
    } catch (const parquet::ParquetException& error) {
        throw Failure({"CORRUPTION", BoundedDiagnostic(error.what()), RetryHint::OperatorAction});
    }
    return result;
}

} // namespace glamdb::detail
