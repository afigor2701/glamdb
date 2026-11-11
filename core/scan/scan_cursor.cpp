#include "core/scan/scan_cursor.h"

#include "core/api/binary.h"
#include "core/api/canonical.h"
#include "core/backend/storage_errors.h"
#include "core/engine/storage_mapping.h"
#include "core/scan/buffer_file.h"
#include "core/scan/predicate.h"
#include "core/scan/scan_planner.h"
#include "core/storage/format_verifier.h"
#include "core/storage/table_profile.h"

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <iceberg/file_io.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>
#include <parquet/metadata.h>
#include <parquet/schema.h>

#include <algorithm>
#include <cmath>
#include <cstring>
#include <deque>
#include <iterator>
#include <mutex>
#include <new>
#include <numeric>
#include <span>

namespace glamdb::detail {
namespace {

constexpr int64_t MaxBatchRows = 65536;

void RequireInput(bool condition, std::string_view message)
{
    if (!condition) {
        throw Failure({"INVALID_ARGUMENT", std::string(message), RetryHint::FixInput});
    }
}

[[noreturn]] void Fail(std::string_view code, std::string_view context, std::string_view message)
{
    throw Failure({std::string(code),
        BoundedDiagnostic(std::string(context) + ": " + std::string(message)),
        RetryHint::OperatorAction});
}

// Allocation failure is resource pressure, not a property of the decoded file.
void CheckArrow(const arrow::Status& status, std::string_view code, std::string_view context)
{
    if (!status.ok()) {
        Fail(status.IsOutOfMemory() ? "RESOURCE_EXHAUSTED" : code, context, status.ToString());
    }
}

template <class T>
T TakeArrow(arrow::Result<T> result, std::string_view code, std::string_view context)
{
    CheckArrow(result.status(), code, context);
    return std::move(result).ValueUnsafe();
}

// Every path a cursor reads comes from committed metadata.
template <class T>
T TakeIceberg(iceberg::Result<T> result, std::string_view code, std::string_view context)
{
    if (!result) {
        throw Failure(StorageError(result.error(), context, EStoragePath::Read, code));
    }
    return std::move(*result);
}

// Limits of the canonical input (see core/api/canonical.cpp); stored values
// outside them cannot have been written by this engine.
constexpr size_t MaxKeyBytes = 1024;
constexpr size_t MaxStringBytes = 1U << 20;
constexpr int64_t MinTimestampUs = -62135596800000000LL;
constexpr int64_t MaxTimestampUs = 253402300799999999LL;

// Decoded values of one column must satisfy the canonical input domain.
void ValidateColumn(const FieldSpec& field, const arrow::Array& array)
{
    // Checks offsets and, for utf8, that every value is valid UTF-8; the
    // Parquet reader decodes strings without validation.
    CheckArrow(array.ValidateFull(), "CORRUPTION", "validate decoded column");
    if (!field.nullable && array.null_count() != 0) {
        Fail("CORRUPTION", "read Parquet batch", "required column contains NULL");
    }
    switch (field.type) {
        case FieldType::String: {
            const auto& strings = static_cast<const arrow::StringArray&>(array);
            const auto limit = field.name == "key" ? MaxKeyBytes : MaxStringBytes;
            for (int64_t row = 0; row < strings.length(); ++row) {
                if (!strings.IsNull(row) && strings.GetView(row).size() > limit) {
                    Fail("CORRUPTION", "read Parquet batch", "string value exceeds its byte limit");
                }
            }
            break;
        }
        case FieldType::Float64: {
            const auto& doubles = static_cast<const arrow::DoubleArray&>(array);
            for (int64_t row = 0; row < doubles.length(); ++row) {
                const auto value = doubles.IsNull(row) ? 0.0 : doubles.Value(row);
                if (!std::isfinite(value) || (value == 0.0 && std::signbit(value))) {
                    Fail("CORRUPTION", "read Parquet batch", "stored float is not canonical");
                }
            }
            break;
        }
        case FieldType::TimestampUsUtc: {
            const auto& times = static_cast<const arrow::TimestampArray&>(array);
            for (int64_t row = 0; row < times.length(); ++row) {
                if (!times.IsNull(row) && (times.Value(row) < MinTimestampUs || times.Value(row) > MaxTimestampUs)) {
                    Fail("CORRUPTION", "read Parquet batch", "stored timestamp is outside the supported UTC range");
                }
            }
            break;
        }
        case FieldType::Bool:
        case FieldType::Int64:
            break;
    }
}

template <class TFunction>
decltype(auto) DecodeParquet(std::string_view context, TFunction&& fn)
{
    try {
        return fn();
    } catch (const Failure&) {
        throw;
    } catch (const std::bad_alloc& error) {
        Fail("RESOURCE_EXHAUSTED", context, error.what());
    } catch (const std::exception& error) {
        Fail("CORRUPTION", context, error.what());
    }
}

template <class T>
T Take(Result<T> result)
{
    if (!result) {
        throw Failure(result.error());
    }
    return std::move(*result);
}

class TLeaseBuffer final : public arrow::Buffer
{
public:
    TLeaseBuffer(std::shared_ptr<arrow::Buffer> inner, std::shared_ptr<const void> lease)
        : arrow::Buffer(inner->data(), inner->size())
        , Lease_(std::move(lease))
        , Inner_(std::move(inner))
    { }

private:
    // Destroy Inner_ before Lease_ so buffer memory is freed while the
    // store lease is still held.
    std::shared_ptr<const void> Lease_;
    std::shared_ptr<arrow::Buffer> Inner_;
};

std::shared_ptr<arrow::ArrayData> AttachLease(const std::shared_ptr<arrow::ArrayData>& data,
    const std::shared_ptr<const void>& lease)
{
    if (!data || !lease) {
        return data;
    }
    auto copy = data->Copy();
    for (auto& buffer : copy->buffers) {
        if (buffer) {
            buffer = std::make_shared<TLeaseBuffer>(std::move(buffer), lease);
        }
    }
    for (auto& child : copy->child_data) {
        child = AttachLease(child, lease);
    }
    if (copy->dictionary) {
        copy->dictionary = AttachLease(copy->dictionary, lease);
    }
    return copy;
}

std::vector<size_t> ResolveProjection(const SchemaSpec& schema, const std::optional<std::vector<std::string>>& names)
{
    std::vector<size_t> fields;
    if (!names) {
        fields.resize(schema.fields.size());
        std::iota(fields.begin(), fields.end(), size_t{0});
        return fields;
    }
    RequireInput(!names->empty(), "projection must select at least one field");
    for (const auto& name : *names) {
        const auto it = std::ranges::find_if(schema.fields, [&](const auto& field) {
            return field.name == name;
        });
        RequireInput(it != schema.fields.end(), "projection field does not exist");
        const auto index = static_cast<size_t>(it - schema.fields.begin());
        RequireInput(std::ranges::find(fields, index) == fields.end(), "projection repeats a field");
        fields.push_back(index);
    }
    return fields;
}

class TScanCursor final : public ScanCursor
{
public:
    TScanCursor(TScanContext context,
        std::shared_ptr<iceberg::Schema> fileSchema,
        std::shared_ptr<arrow::Schema> outputSchema,
        TScanManifests plan,
        TBoundPredicate predicate,
        std::vector<size_t> outputFields,
        std::vector<size_t> readFields,
        int64_t batchRows,
        std::optional<int64_t> limit)
        : Context_(std::move(context))
        , FileSchema_(std::move(fileSchema))
        , OutputSchema_(std::move(outputSchema))
        , Info_{OutputSchema_,
              Context_.Base,
              plan.SnapshotID ? SnapshotSelector::Id(*plan.SnapshotID) : SnapshotSelector::InitialEmpty()}
        , Plan_(std::move(plan))
        , Predicate_(std::move(predicate))
        , OutputFields_(std::move(outputFields))
        , ReadFields_(std::move(readFields))
        , BatchRows_(batchRows)
        , Remaining_(limit)
    { }

    const ScanInfo& Info() const override
    {
        return Info_;
    }

    Result<std::shared_ptr<arrow::RecordBatch>> Next() override
    {
        std::lock_guard lock(Mutex_);
        if (Failed_) {
            return std::unexpected(*Failed_);
        }
        try {
            return ReadNext();
        } catch (const Failure& error) {
            Release();
            if (State_ == ScanState::Open) {
                State_ = error.detail.code == "CANCELLED" ? ScanState::Cancelled : ScanState::Failed;
            }
            if (Context_.OnError) {
                Context_.OnError(error.detail);
            }
            if (error.detail.code != "CANCELLED") {
                Failed_ = error.detail;
            }
            return std::unexpected(error.detail);
        } catch (const std::bad_alloc& error) {
            Release();
            if (State_ == ScanState::Open) {
                State_ = ScanState::Failed;
            }
            Failed_ = ErrorDetail{"RESOURCE_EXHAUSTED", BoundedDiagnostic(error.what()), RetryHint::OperatorAction};
            return std::unexpected(*Failed_);
        } catch (const std::exception& error) {
            Release();
            if (State_ == ScanState::Open) {
                State_ = ScanState::Failed;
            }
            Failed_ = ErrorDetail{"INTERNAL", BoundedDiagnostic(error.what()), RetryHint::OperatorAction};
            return std::unexpected(*Failed_);
        }
    }

    void Cancel() override
    {
        Cancelled_ = true;
    }

    ScanProfile Profile() const override
    {
        std::lock_guard lock(Mutex_);
        ScanProfile profile{.state = State_,
            .manifests_read = {ManifestsRead_, MetricKind::Measured},
            .data_files_read = {DataFilesRead_, MetricKind::Measured},
            .data_bytes_read = {DataBytesRead_, MetricKind::Measured},
            .rows_decoded = {RowsDecoded_, MetricKind::Measured},
            .rows_returned = {RowsReturned_, MetricKind::Measured}};
        if (profile.state == ScanState::Open && (Cancelled_.load() || (Context_.Closing && Context_.Closing->load()))) {
            profile.state = ScanState::Cancelled;
        }
        return profile;
    }

private:
    const TScanContext Context_;
    // The canonical Iceberg schema of every data file of this collection.
    const std::shared_ptr<iceberg::Schema> FileSchema_;
    const std::shared_ptr<arrow::Schema> OutputSchema_;
    const ScanInfo Info_;
    // Manifests are read on demand, so a limited scan opens only what it needs.
    const TScanManifests Plan_;
    const TBoundPredicate Predicate_;
    // Schema field indices in output order, and every field read from files.
    const std::vector<size_t> OutputFields_;
    const std::vector<size_t> ReadFields_;
    const int64_t BatchRows_;

    mutable std::mutex Mutex_;
    ScanState State_ = ScanState::Open;
    int64_t ManifestsRead_ = 0;
    int64_t DataFilesRead_ = 0;
    int64_t DataBytesRead_ = 0;
    int64_t RowsDecoded_ = 0;
    int64_t RowsReturned_ = 0;
    std::atomic<bool> Cancelled_{false};
    std::optional<ErrorDetail> Failed_;
    size_t NextManifest_ = 0;
    std::deque<TScanFile> PendingFiles_;
    // A RecordBatchReader must not outlive its FileReader; members are
    // destroyed in reverse order.
    std::unique_ptr<parquet::arrow::FileReader> FileReader_;
    std::unique_ptr<arrow::RecordBatchReader> BatchReader_;
    std::optional<int64_t> Remaining_;

    void CheckActive() const
    {
        if (Cancelled_.load()) {
            throw Failure({"CANCELLED", "scan was cancelled", RetryHint::OperatorAction});
        }
        if (Context_.Closing && Context_.Closing->load()) {
            throw Failure({"CANCELLED", "engine is closing", RetryHint::OperatorAction});
        }
    }

    void Release()
    {
        BatchReader_.reset();
        FileReader_.reset();
    }

    Result<std::shared_ptr<arrow::RecordBatch>> ReadNext()
    {
        CheckActive();
        while (!Remaining_ || *Remaining_ > 0) {
            CheckActive();
            if (!BatchReader_) {
                while (PendingFiles_.empty() && NextManifest_ < Plan_.Manifests.size()) {
                    CheckActive();
                    auto files = Take(ManifestFiles(Context_.FileIO, Plan_, NextManifest_++));
                    ++ManifestsRead_;
                    PendingFiles_.insert(PendingFiles_.end(),
                        std::make_move_iterator(files.begin()),
                        std::make_move_iterator(files.end()));
                }
                if (PendingFiles_.empty()) {
                    break;
                }
                const auto file = std::move(PendingFiles_.front());
                PendingFiles_.pop_front();
                OpenFile(file);
            }
            std::shared_ptr<arrow::RecordBatch> batch;
            DecodeParquet("read Parquet batch", [&] {
                CheckArrow(BatchReader_->ReadNext(&batch), "CORRUPTION", "read Parquet batch");
            });
            if (!batch) {
                Release();
                continue;
            }
            RowsDecoded_ += batch->num_rows();
            if (auto produced = Produce(*batch)) {
                CheckActive();
                RowsReturned_ += produced->num_rows();
                return produced;
            }
        }
        Release();
        CheckActive();
        State_ = ScanState::Complete;
        return std::shared_ptr<arrow::RecordBatch>{};
    }

    void OpenFile(const TScanFile& file)
    {
        // Writer limits bound one data file to 32 MiB in this profile.
        auto bytes = TakeIceberg(Context_.FileIO->ReadFile(file.Path, static_cast<size_t>(file.SizeBytes)),
            "IO_ERROR",
            "read data file");
        ++DataFilesRead_;
        DataBytesRead_ += static_cast<int64_t>(bytes.size());
        if (static_cast<int64_t>(bytes.size()) != file.SizeBytes) {
            Fail("CORRUPTION", "read data file", "file length differs from its manifest");
        }
        // The same strict footer check as recovery: field IDs, names,
        // repetition, physical/logical types and row counts per row group.
        TBufferInputFile buffered({.Location = file.Path, .Bytes = bytes});
        (void)TakeIceberg(NGlamDB::NStorage::CheckParquet(buffered, *FileSchema_, {file.SizeBytes, file.RecordCount}),
            "CORRUPTION",
            "verify Parquet file");
        DecodeParquet("open Parquet file", [&] {
            auto source = std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(std::move(bytes)));
            auto reader = TakeArrow(parquet::arrow::OpenFile(source, arrow::default_memory_pool()),
                "CORRUPTION",
                "open Parquet file");
            reader->set_batch_size(BatchRows_);
            std::vector<int> rowGroups(static_cast<size_t>(reader->num_row_groups()));
            std::iota(rowGroups.begin(), rowGroups.end(), 0);
            std::vector<int> columns;
            columns.reserve(ReadFields_.size());
            for (const auto field : ReadFields_) {
                columns.push_back(static_cast<int>(field));
            }
            BatchReader_ =
                TakeArrow(reader->GetRecordBatchReader(rowGroups, columns), "CORRUPTION", "open Parquet batches");
            FileReader_ = std::move(reader);
        });
    }

    std::shared_ptr<arrow::Array> Column(const arrow::RecordBatch& batch, size_t field) const
    {
        const auto& spec = Context_.Collection.schema.fields[field];
        auto array = batch.GetColumnByName(spec.name);
        if (!array) {
            Fail("CORRUPTION", "read Parquet batch", "batch lacks a requested column");
        }
        const auto expected = MakeArrowType(spec.type);
        if (!array->type()->Equals(*expected)) {
            const bool sameTimestamp = expected->id() == arrow::Type::TIMESTAMP &&
                array->type_id() == arrow::Type::TIMESTAMP &&
                static_cast<const arrow::TimestampType&>(*array->type()).unit() == arrow::TimeUnit::MICRO;
            if (!sameTimestamp) {
                Fail("CORRUPTION", "read Parquet batch", "column type differs from the collection schema");
            }
            array = TakeArrow(array->View(expected), "CORRUPTION", "normalize timestamp type");
        }
        if (!spec.nullable && array->null_count() != 0) {
            Fail("CORRUPTION", "read Parquet batch", "required column contains NULL");
        }
        return array;
    }

    // Filters, projects and applies the limit. Returns nullptr for no rows.
    std::shared_ptr<arrow::RecordBatch> Produce(const arrow::RecordBatch& batch)
    {
        // Validate every decoded column before any value can be evaluated or returned.
        std::vector<std::shared_ptr<arrow::Array>> readColumns;
        readColumns.reserve(ReadFields_.size());
        for (const auto field : ReadFields_) {
            auto col = Column(batch, field);
            ValidateColumn(Context_.Collection.schema.fields[field], *col);
            readColumns.push_back(std::move(col));
        }
        const auto findReadColumn = [&](size_t field) -> const std::shared_ptr<arrow::Array>& {
            const auto it = std::ranges::lower_bound(ReadFields_, field);
            if (it == ReadFields_.end() || *it != field) {
                Fail("INTERNAL", "read Parquet batch", "missing validated column");
            }
            return readColumns[static_cast<size_t>(it - ReadFields_.begin())];
        };
        const auto selected = [&]() -> std::optional<std::vector<int64_t>> {
            if (!Predicate_.IsAlwaysTrue()) {
                std::vector<std::shared_ptr<arrow::Array>> predicateColumns;
                predicateColumns.reserve(Predicate_.ReferencedFields.size());
                for (const auto field : Predicate_.ReferencedFields) {
                    predicateColumns.push_back(findReadColumn(field));
                }
                auto rows = Take(SelectRows(Predicate_, batch.num_rows(), predicateColumns));
                if (static_cast<int64_t>(rows.size()) != batch.num_rows()) {
                    return rows;
                }
            }
            return std::nullopt;
        }();
        auto count = selected ? static_cast<int64_t>(selected->size()) : batch.num_rows();
        if (Remaining_) {
            count = std::min(count, *Remaining_);
        }
        if (count == 0) {
            return nullptr;
        }
        std::vector<std::shared_ptr<arrow::Array>> columns;
        columns.reserve(OutputFields_.size());
        for (const auto field : OutputFields_) {
            auto array = findReadColumn(field);
            if (selected) {
                const auto rows = std::span<const int64_t>(*selected).first(static_cast<size_t>(count));
                array = Take(TakeRows(*array, rows));
            } else if (count != array->length()) {
                array = array->Slice(0, count);
            }
            columns.push_back(arrow::MakeArray(AttachLease(array->data(), Context_.Lease)));
        }
        if (Remaining_) {
            *Remaining_ -= count;
        }
        return arrow::RecordBatch::Make(OutputSchema_, count, std::move(columns));
    }
};

} // namespace

Result<std::shared_ptr<ScanCursor>> OpenScan(TScanContext context, const ScanRequest& request)
{
    try {
        RequireInput(request.batch_rows >= 1 && request.batch_rows <= MaxBatchRows,
            "batch_rows must be within 1..65536");
        RequireInput(!request.limit || *request.limit >= 0, "limit must not be negative");
        const auto& schema = context.Collection.schema;
        auto outputFields = ResolveProjection(schema, request.projection);
        auto predicate = Take(BindPredicate(request.predicate, schema));

        auto readFields = outputFields;
        readFields.insert(readFields.end(), predicate.ReferencedFields.begin(), predicate.ReferencedFields.end());
        std::ranges::sort(readFields);
        readFields.erase(std::unique(readFields.begin(), readFields.end()), readFields.end());

        const auto fullSchema = MakeArrowSchema(schema);
        std::vector<std::shared_ptr<arrow::Field>> fields;
        fields.reserve(outputFields.size());
        for (const auto field : outputFields) {
            fields.push_back(fullSchema->field(static_cast<int>(field)));
        }
        auto profile = NGlamDB::NStorage::MakeTableProfile(ToStorageSchema(schema));
        if (!profile) {
            throw Failure({"CORRUPTION",
                BoundedDiagnostic("collection schema profile: " + profile.error().message),
                RetryHint::OperatorAction});
        }
        auto plan = Take(PlanManifests(context.FileIO, context.Base, request.selector));
        return std::make_shared<TScanCursor>(std::move(context),
            std::move(profile->Schema),
            arrow::schema(std::move(fields)),
            std::move(plan),
            std::move(predicate),
            std::move(outputFields),
            std::move(readFields),
            request.batch_rows,
            request.limit);
    } catch (const Failure& error) {
        return std::unexpected(error.detail);
    } catch (const std::bad_alloc& error) {
        return std::unexpected(
            ErrorDetail{"RESOURCE_EXHAUSTED", BoundedDiagnostic(error.what()), RetryHint::OperatorAction});
    } catch (const std::exception& error) {
        return std::unexpected(ErrorDetail{"INTERNAL", BoundedDiagnostic(error.what()), RetryHint::OperatorAction});
    }
}

} // namespace glamdb::detail
