#include "core/scan/scan_cursor.h"
#include "core/telemetry/metrics.h"

#include "core/api/binary.h"
#include "core/api/canonical.h"
#include "core/api/scalar_validation.h"
#include "core/engine/storage_mapping.h"
#include "core/scan/metadata_cache.h"
#include "core/scan/parquet_input.h"
#include "core/scan/predicate.h"
#include "core/scan/pruning.h"
#include "core/scan/scan_planner.h"
#include "core/storage/format_verifier.h"
#include "core/storage/table_profile.h"

#include "core/resources/arrow_pool.h"
#include "core/resources/envelopes.h"
#include "storage/resources.h"

#include <arrow/api.h>
#include <iceberg/file_io.h>
#include <parquet/arrow/reader.h>
#include <parquet/exception.h>
#include <parquet/file_reader.h>
#include <parquet/metadata.h>
#include <parquet/properties.h>
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
        if (status.IsCancelled()) {
            throw Failure({"CANCELLED", status.ToString(), RetryHint::OperatorAction});
        }
        Fail(status.IsOutOfMemory() ? "RESOURCE_EXHAUSTED" : code, context, status.ToString());
    }
}

template <class T>
T TakeArrow(arrow::Result<T> result, std::string_view code, std::string_view context)
{
    CheckArrow(result.status(), code, context);
    return std::move(result).ValueUnsafe();
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
        case FieldType::Decimal: {
            const auto& values = static_cast<const arrow::Decimal128Array&>(array);
            for (int64_t row = 0; row < values.length(); ++row) {
                if (!values.IsNull(row) &&
                    !FitsDecimalPrecision(arrow::Decimal128(values.GetValue(row)), field.precision))
                {
                    Fail("CORRUPTION", "read Parquet batch", "stored decimal exceeds its precision");
                }
            }
            break;
        }
        case FieldType::Date: {
            const auto& values = static_cast<const arrow::Date32Array&>(array);
            for (int64_t row = 0; row < values.length(); ++row) {
                if (!values.IsNull(row) && (values.Value(row) < -719162 || values.Value(row) > 2932896)) {
                    Fail("CORRUPTION", "read Parquet batch", "stored date is outside the supported range");
                }
            }
            break;
        }
    }
}

template <typename TFunction>
decltype(auto)
DecodeParquetWithSource(const std::shared_ptr<TScanRandomAccessFile>& source, std::string_view context, TFunction&& fn)
{
    const auto rethrowSourceError = [&] {
        if (source) {
            if (auto err = source->LastError()) {
                throw Failure(std::move(*err));
            }
        }
    };
    try {
        return std::forward<TFunction>(fn)();
    } catch (const Failure&) {
        rethrowSourceError();
        throw;
    } catch (const parquet::ParquetStatusException& error) {
        rethrowSourceError();
        CheckArrow(error.status(), "CORRUPTION", context);
        Fail("CORRUPTION", context, error.what());
    } catch (const std::bad_alloc& error) {
        Fail("RESOURCE_EXHAUSTED", context, error.what());
    } catch (const std::exception& error) {
        rethrowSourceError();
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
    // allocator and engine resources are still held.
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

std::shared_ptr<arrow::Schema> RetainSchema(std::shared_ptr<arrow::Schema> schema,
    std::shared_ptr<ResourceReservation> memory)
{
    struct TSchemaLease
    {
        std::shared_ptr<ResourceReservation> Memory;
        std::shared_ptr<arrow::Schema> Schema;
    };
    auto* retainedSchema = schema.get();
    auto lease = std::make_shared<TSchemaLease>(std::move(memory), std::move(schema));
    return std::shared_ptr<arrow::Schema>(std::move(lease), retainedSchema);
}

struct TOutputUsage
{
    std::atomic<int64_t> Bytes{0};
    std::atomic<int64_t> Peak{0};
};

struct TOutputLease
{
    // Output schemas retain this lease even for zero-column batch slices.
    std::shared_ptr<const void> Cursor;
    std::shared_ptr<ResourceReservation> Memory;
    std::shared_ptr<TOutputUsage> Usage;
    std::shared_ptr<arrow::Schema> Schema;

    TOutputLease(std::shared_ptr<const void> cursor,
        std::shared_ptr<ResourceReservation> memory,
        std::shared_ptr<TOutputUsage> usage,
        std::shared_ptr<arrow::Schema> schema)
        : Cursor(std::move(cursor))
        , Memory(std::move(memory))
        , Usage(std::move(usage))
        , Schema(std::move(schema))
    {
        const auto bytes =
            Usage->Bytes.fetch_add(static_cast<int64_t>(Memory->GetBytes())) + static_cast<int64_t>(Memory->GetBytes());
        auto peak = Usage->Peak.load();
        while (peak < bytes && !Usage->Peak.compare_exchange_weak(peak, bytes)) {
        }
    }

    ~TOutputLease()
    {
        Usage->Bytes.fetch_sub(static_cast<int64_t>(Memory->GetBytes()));
    }
};

std::vector<size_t> ResolveProjection(const SchemaSpec& schema, const std::optional<std::vector<std::string>>& names)
{
    std::vector<size_t> fields;
    if (!names) {
        fields.resize(schema.fields.size());
        std::iota(fields.begin(), fields.end(), size_t{0});
        return fields;
    }
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
        std::shared_ptr<ResourceReservation> planningMemory,
        std::shared_ptr<ResourceReservation> controlMemory,
        std::shared_ptr<iceberg::Schema> fileSchema,
        std::shared_ptr<arrow::Schema> outputSchema,
        TScanManifests plan,
        TBoundPredicate predicate,
        std::vector<size_t> outputFields,
        std::vector<size_t> readFields,
        int64_t batchRows,
        std::optional<int64_t> limit)
        : PlanningMemory_(std::move(planningMemory))
        , CursorMemory_(Take(context.Resources->Reserve(ResourceClass::Metadata, 16ULL << 20)))
        , SelectionMemory_(Take(context.Resources->Reserve(ResourceClass::Data, 1ULL << 20)))
        , ControlMemory_(std::move(controlMemory))
        , Context_(std::move(context))
        , Pool_(MakeResourcePool(Context_.Resources, Context_.Pool))
        , BufferLease_(std::make_shared<TBufferLease>(Context_.Lease, Context_.FileIO, Pool_, CursorMemory_))
        , FileSchema_(std::move(fileSchema))
        , OutputSchema_(RetainSchema(std::move(outputSchema), CursorMemory_))
        , Info_{OutputSchema_,
              Context_.Base,
              plan.SnapshotID ? SnapshotSelector::Id(*plan.SnapshotID) : SnapshotSelector::InitialEmpty()}
        , Plan_(std::move(plan))
        , Predicate_(std::move(predicate))
        , OutputFields_(std::move(outputFields))
        , ReadFields_(std::move(readFields))
        , BatchRows_(batchRows)
        , Remaining_(limit)
    {
        NGlamDB::NTelemetry::AdjustRetainedCursors(1);
    }

    ~TScanCursor() override
    {
        NGlamDB::NTelemetry::AdjustRetainedCursors(-1);
    }

    const ScanInfo& Info() const override
    {
        return Info_;
    }

    Result<std::shared_ptr<arrow::RecordBatch>> Next() override
    {
        NGlamDB::NTelemetry::TReadOperationScope metrics(NGlamDB::NTelemetry::EReadOperation::ScanNext);
        std::lock_guard lock(Mutex_);
        if (Failed_) {
            metrics.Error(Failed_->code);
            return std::unexpected(*Failed_);
        }
        try {
            auto result = ReadNext();
            const auto rows = result && *result ? static_cast<std::uint64_t>((*result)->num_rows()) : 0;
            return metrics.Finish(std::move(result), rows);
        } catch (const Failure& error) {
            // Explicit admission fails before consuming the next batch/file.
            // Preserve pending descriptors and the current decoder for retry.
            if (error.detail.code == "RESOURCE_EXHAUSTED" && error.detail.retry_hint == RetryHint::SameIdAfterReconcile)
            {
                return std::unexpected(error.detail);
            }
            Release();
            if (State_ == ScanState::Open) {
                if (error.detail.code == "CANCELLED") {
                    CancellationStopped_ = true;
                    State_ = ScanState::Cancelled;
                } else {
                    State_ = ScanState::Failed;
                }
            }
            if (Context_.OnError) {
                Context_.OnError(error.detail);
            }
            if (error.detail.code != "CANCELLED") {
                Failed_ = error.detail;
            }
            metrics.Error(error.detail.code);
            return std::unexpected(error.detail);
        } catch (const std::bad_alloc& error) {
            Release();
            if (State_ == ScanState::Open) {
                State_ = ScanState::Failed;
            }
            Failed_ = ErrorDetail{"RESOURCE_EXHAUSTED", BoundedDiagnostic(error.what()), RetryHint::OperatorAction};
            metrics.Error(Failed_->code);
            return std::unexpected(*Failed_);
        } catch (const std::exception& error) {
            Release();
            if (State_ == ScanState::Open) {
                State_ = ScanState::Failed;
            }
            Failed_ = ErrorDetail{"INTERNAL", BoundedDiagnostic(error.what()), RetryHint::OperatorAction};
            metrics.Error(Failed_->code);
            return std::unexpected(*Failed_);
        }
    }

    void Cancel() override
    {
        NGlamDB::NTelemetry::TReadOperationScope metrics(NGlamDB::NTelemetry::EReadOperation::ScanCancel);
        Cancelled_ = true;
        metrics.Finish();
    }

    ScanProfile Profile() const override
    {
        std::lock_guard lock(Mutex_);
        ScanProfile profile{.state = State_,
            .manifests_read = {ManifestsRead_, MetricKind::Measured},
            .data_files_read = {DataFilesRead_, MetricKind::Measured},
            .data_bytes_read = {DataBytesRead_->load(std::memory_order_relaxed), MetricKind::Measured},
            .rows_decoded = {RowsDecoded_, MetricKind::Measured},
            .rows_returned = {RowsReturned_, MetricKind::Measured},
            .rows_filtered = {RowsFiltered_, MetricKind::Measured},
            .rows_limited = {RowsLimited_, MetricKind::Measured},
            .limit_stops = {LimitStopped_ ? 1 : 0, MetricKind::Measured},
            .cancellation_stops = {CancellationStopped_ ? 1 : 0, MetricKind::Measured},
            .data_files_pruned = {DataFilesPruned_, MetricKind::Measured},
            .row_groups_pruned = {RowGroupsPruned_, MetricKind::Measured},
            .arrow_bytes = {Pool_->bytes_allocated(), MetricKind::Measured},
            .peak_arrow_bytes = {Pool_->max_memory(), MetricKind::Measured},
            .output_metadata_bytes = {OutputUsage_->Bytes.load(), MetricKind::Measured},
            .peak_output_metadata_bytes = {OutputUsage_->Peak.load(), MetricKind::Measured}};
        if (profile.state == ScanState::Open && (Cancelled_.load() || (Context_.Closing && Context_.Closing->load()))) {
            profile.state = ScanState::Cancelled;
        }
        return profile;
    }

private:
    struct TBufferLease
    {
        std::shared_ptr<const void> Store;
        std::shared_ptr<iceberg::FileIO> FileIO;
        std::shared_ptr<arrow::MemoryPool> Pool;
        std::shared_ptr<ResourceReservation> Metadata;
    };
    // Reservations outlive the context, plan, predicate and decoder objects.
    const std::shared_ptr<ResourceReservation> PlanningMemory_, CursorMemory_, SelectionMemory_, ControlMemory_;
    const TScanContext Context_;
    const std::shared_ptr<arrow::MemoryPool> Pool_;
    const std::shared_ptr<TOutputUsage> OutputUsage_ = std::make_shared<TOutputUsage>();
    const std::shared_ptr<TBufferLease> BufferLease_;
    std::shared_ptr<ResourceReservation> FileMemory_;
    // The canonical Iceberg schema of every data file of this collection.
    const std::shared_ptr<iceberg::Schema> FileSchema_;
    const std::shared_ptr<arrow::Schema> OutputSchema_;
    const ScanInfo Info_;
    // Production plans pin verified membership; custom backends can retain
    // on-demand manifest planning.
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
    const std::shared_ptr<std::atomic<int64_t>> DataBytesRead_ = std::make_shared<std::atomic<int64_t>>(0);
    int64_t RowsDecoded_ = 0;
    int64_t RowsReturned_ = 0;
    int64_t RowsFiltered_ = 0;
    int64_t RowsLimited_ = 0;
    int64_t DataFilesPruned_ = 0;
    int64_t RowGroupsPruned_ = 0;
    bool LimitStopped_ = false;
    bool CancellationStopped_ = false;
    std::atomic<bool> Cancelled_{false};
    std::optional<ErrorDetail> Failed_;
    size_t NextManifest_ = 0;
    size_t NextIndexedFile_ = 0;
    std::shared_ptr<ResourceReservation> PendingMemory_;
    std::deque<TScanFile> PendingFiles_;
    std::shared_ptr<TScanRandomAccessFile> FileSource_;
    // A RecordBatchReader must not outlive its FileReader; members are
    // destroyed in reverse order.
    std::unique_ptr<parquet::arrow::FileReader> FileReader_;
    std::unique_ptr<arrow::RecordBatchReader> BatchReader_;
    std::optional<int64_t> Remaining_;

    void CheckActive()
    {
        if (Cancelled_.load() || (Context_.Closing && Context_.Closing->load())) {
            if (State_ == ScanState::Open) {
                CancellationStopped_ = true;
            }
            throw Failure({"CANCELLED", "scan was cancelled or engine is closing", RetryHint::OperatorAction});
        }
    }

    void ReleaseFile()
    {
        BatchReader_.reset();
        FileReader_.reset();
        if (FileSource_) {
            (void)FileSource_->Abort();
            FileSource_.reset();
        }
        FileMemory_.reset();
    }

    void FinishFile()
    {
        BatchReader_.reset();
        FileReader_.reset();
        if (FileSource_) {
            auto source = std::move(FileSource_);
            CheckActive();
            if (auto err = source->LastError()) {
                throw Failure(std::move(*err));
            }
            auto closed = source->CloseStream();
            if (!closed) {
                throw Failure(std::move(closed.error()));
            }
            CheckActive();
        }
        FileMemory_.reset();
    }

    void Release()
    {
        ReleaseFile();
        PendingFiles_.clear();
        PendingMemory_.reset();
    }

    std::shared_ptr<ResourceReservation> ReserveAdmission(ResourceClass resource, uint64_t bytes)
    {
        auto result = Context_.Resources->Reserve(resource, bytes);
        if (!result) {
            auto error = result.error();
            if (error.code == "RESOURCE_EXHAUSTED") {
                error.retry_hint = RetryHint::SameIdAfterReconcile;
            }
            throw Failure(std::move(error));
        }
        return std::move(*result);
    }

    Result<std::shared_ptr<arrow::RecordBatch>> ReadNext()
    {
        CheckActive();
        while (!Remaining_ || *Remaining_ > 0) {
            CheckActive();
            std::shared_ptr<ResourceReservation> outputMemory;
            if (!BatchReader_) {
                if (PendingFiles_.empty() && Plan_.Index && NextIndexedFile_ < Plan_.FileCount) {
                    PendingFiles_.push_back(Plan_.Index->Files[Plan_.FileCount - ++NextIndexedFile_]);
                }
                while (PendingFiles_.empty() && NextManifest_ < Plan_.Manifests.size()) {
                    CheckActive();
                    auto memory = ReserveAdmission(ResourceClass::Metadata, 256ULL << 20);
                    auto files = Take(ManifestFiles(Context_.FileIO, Plan_, NextManifest_++, Context_.Cache.get()));
                    ++ManifestsRead_;
                    CheckActive();
                    PendingMemory_ = std::move(memory);
                    PendingFiles_.insert(PendingFiles_.end(),
                        std::make_move_iterator(files.begin()),
                        std::make_move_iterator(files.end()));
                }
                if (PendingFiles_.empty()) {
                    break;
                }
                if (PendingFiles_.front().DataFile != nullptr) {
                    const auto fileStats = Take(ExtractFileZoneStats(*PendingFiles_.front().DataFile,
                        *FileSchema_,
                        Context_.Collection.schema,
                        ReadFields_));
                    if (Context_.EnableFilePruning &&
                        Take(CanPruneZone(Predicate_, PendingFiles_.front().RecordCount, ReadFields_, fileStats)))
                    {
                        ++DataFilesPruned_;
                        PendingFiles_.pop_front();
                        if (PendingFiles_.empty()) {
                            PendingMemory_.reset();
                        }
                        CheckActive();
                        continue;
                    }
                }
                outputMemory = ReserveAdmission(ResourceClass::Metadata,
                    4096 + static_cast<uint64_t>(OutputFields_.size()) * 2048);
                OpenFile(PendingFiles_.front());
                PendingFiles_.pop_front();
                if (PendingFiles_.empty()) {
                    PendingMemory_.reset();
                }
                if (!BatchReader_) {
                    continue;
                }
            }
            if (!outputMemory) {
                outputMemory = ReserveAdmission(ResourceClass::Metadata,
                    4096 + static_cast<uint64_t>(OutputFields_.size()) * 2048);
            }
            std::shared_ptr<arrow::RecordBatch> batch;
            DecodeParquetWithSource(FileSource_, "read Parquet batch", [&] {
                CheckArrow(BatchReader_->ReadNext(&batch), "CORRUPTION", "read Parquet batch");
            });
            if (FileSource_) {
                if (auto err = FileSource_->LastError()) {
                    throw Failure(std::move(*err));
                }
            }
            if (!batch) {
                FinishFile();
                continue;
            }
            RowsDecoded_ += batch->num_rows();
            CheckActive();
            if (auto produced = Produce(*batch, std::move(outputMemory))) {
                CheckActive();
                if (Remaining_) {
                    *Remaining_ -= produced->num_rows();
                    LimitStopped_ = *Remaining_ == 0;
                }
                RowsReturned_ += produced->num_rows();
                return produced;
            }
        }
        // Normal completion (either all files were read or Remaining_ reached 0
        // inside an open file) closes the open stream through the checked path so
        // close errors surface instead of returning EOS. Cancellation is checked
        // first so cancelled scans still clean up best-effort via Release().
        CheckActive();
        FinishFile();
        Release();
        CheckActive();
        LimitStopped_ = Remaining_ && *Remaining_ == 0;
        State_ = ScanState::Complete;
        return std::shared_ptr<arrow::RecordBatch>{};
    }

    void OpenFile(const TScanFile& file)
    {
        RequireInput(file.SizeBytes >= 0 && file.SizeBytes <= (32LL << 20), "invalid data file size");
        auto fileMemory = ReserveAdmission(ResourceClass::Metadata, 256ULL << 20);
        auto source = Take(TScanRandomAccessFile::Open(*Context_.FileIO,
            file.Path,
            file.SizeBytes,
            TScanInputOptions{
                .Pool = Pool_.get(),
                .Cancelled = &Cancelled_,
                .Closing = Context_.Closing,
                .TotalBytesRead = DataBytesRead_,
            }));
        FileMemory_ = std::move(fileMemory);
        // The same strict footer check as recovery: field IDs, names,
        // repetition, physical/logical types and row counts per row group.
        std::shared_ptr<parquet::FileMetaData> verifiedFooter;
        if (Context_.Cache) {
            verifiedFooter = Context_.Cache->FindFooter(file.Path, file.SizeBytes, file.RecordCount);
        }
        if (!verifiedFooter) {
            verifiedFooter = Take(source->ReadFooter(*FileSchema_, {file.SizeBytes, file.RecordCount}));
            if (Context_.Cache) {
                Context_.Cache->TryPutFooter(file.Path, file.SizeBytes, file.RecordCount, verifiedFooter);
            }
        }
        ++DataFilesRead_;
        CheckActive();
        const int numRowGroups = verifiedFooter->num_row_groups();
        std::vector<int> rowGroups;
        rowGroups.reserve(static_cast<size_t>(std::max(0, numRowGroups)));
        for (int ordinal = 0; ordinal < numRowGroups; ++ordinal) {
            std::unique_ptr<parquet::RowGroupMetaData> group;
            DecodeParquetWithSource(source, "inspect Parquet row group", [&] {
                group = verifiedFooter->RowGroup(ordinal);
            });
            const auto groupRows = group->num_rows();
            const auto groupStats = Take(ExtractRowGroupZoneStats(*group,
                Context_.Collection.schema,
                ReadFields_,
                file.DataFile.get(),
                FileSchema_.get()));
            if (groupRows == 0) {
                continue;
            }
            if (Context_.EnableRowGroupPruning && Take(CanPruneZone(Predicate_, groupRows, ReadFields_, groupStats))) {
                ++RowGroupsPruned_;
                continue;
            }
            rowGroups.push_back(ordinal);
        }
        CheckActive();
        if (rowGroups.empty()) {
            auto closed = source->CloseStream();
            if (!closed) {
                throw Failure(std::move(closed.error()));
            }
            CheckActive();
            FileMemory_.reset();
            return;
        }
        DecodeParquetWithSource(source, "open Parquet file", [&] {
            const NGlamDB::NStorage::TParquetLimits parquetLimits{};
            parquet::ReaderProperties readerProps(Pool_.get());
            readerProps.set_thrift_string_size_limit(static_cast<int32_t>(parquetLimits.MaxFooterBytes));
            readerProps.set_thrift_container_size_limit(parquetLimits.MaxThriftContainerElements);
            auto parquetReader = parquet::ParquetFileReader::Open(source, readerProps, std::move(verifiedFooter));
            parquet::ArrowReaderProperties arrowProps(/*use_threads=*/false);
            arrowProps.set_pre_buffer(false);
            arrowProps.set_batch_size(BatchRows_);
            arrowProps.set_io_context(arrow::io::IOContext(Pool_.get()));
            auto reader = TakeArrow(parquet::arrow::FileReader::Make(Pool_.get(), std::move(parquetReader), arrowProps),
                "CORRUPTION",
                "open Parquet file");
            std::vector<int> columns;
            columns.reserve(ReadFields_.size());
            for (const auto field : ReadFields_) {
                columns.push_back(static_cast<int>(field));
            }
            BatchReader_ =
                TakeArrow(reader->GetRecordBatchReader(rowGroups, columns), "CORRUPTION", "open Parquet batches");
            FileSource_ = std::move(source);
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
        const auto expected = MakeArrowType(spec);
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
    std::shared_ptr<arrow::RecordBatch> Produce(const arrow::RecordBatch& batch,
        std::shared_ptr<ResourceReservation> outputMemory)
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
        RowsFiltered_ += batch.num_rows() - count;
        if (Remaining_) {
            const auto limited = std::min(count, *Remaining_);
            RowsLimited_ += count - limited;
            count = limited;
        }
        if (count == 0) {
            return nullptr;
        }
        // Flat output arrays need ArrayData, buffer wrappers and vector nodes.
        // Reserve before constructing any retained descriptors. The shared
        // cursor envelope separately covers schema fields and allocator state.
        auto lease = std::make_shared<TOutputLease>(BufferLease_, std::move(outputMemory), OutputUsage_, OutputSchema_);
        auto schema = std::shared_ptr<arrow::Schema>(lease, OutputSchema_.get());
        std::vector<std::shared_ptr<arrow::Array>> columns;
        columns.reserve(OutputFields_.size());
        for (const auto field : OutputFields_) {
            auto array = findReadColumn(field);
            if (selected) {
                const auto rows = std::span<const int64_t>(*selected).first(static_cast<size_t>(count));
                array = Take(TakeRows(*array, rows, Pool_.get()));
            } else if (count != array->length()) {
                array = array->Slice(0, count);
            }
            columns.push_back(arrow::MakeArray(AttachLease(array->data(), lease)));
        }
        return arrow::RecordBatch::Make(std::move(schema), count, std::move(columns));
    }
};

} // namespace

Result<std::shared_ptr<ScanCursor>> OpenScan(TScanContext context, const ScanRequest& request)
{
    try {
        context.Resources = context.Resources ? context.Resources : ResourceOwner::ProcessDefault();
        auto controlMemory = Take(context.Resources->Reserve(ResourceClass::Control, ControlMemoryBytes));
        auto planningMemory = context.PlanningMemory
            ? std::move(context.PlanningMemory)
            : Take(context.Resources->Reserve(ResourceClass::Metadata, MetadataWorkBytes));
        RequireInput(request.batch_rows >= 1 && request.batch_rows <= MaxBatchRows,
            "batch_rows must be within 1..65536");
        RequireInput(!request.limit || *request.limit >= 0, "limit must not be negative");
        const auto& schema = context.Collection.schema;
        auto outputFields = ResolveProjection(schema, request.projection);
        auto predicate = Take(BindPredicate(request.predicate, schema, context.Resources));

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
        auto plan = Take(context.VerifiedState
                ? PlanVerifiedScan(*context.VerifiedState, context.Base, request.selector)
                : PlanManifests(context.FileIO, context.Base, request.selector, context.Cache.get()));
        // Uncached lists/schemas remain materialized in the plan. Verified
        // membership already retains the backend's shared generation lease.
        auto retainedPlan = context.VerifiedState ? nullptr : planningMemory;
        return std::make_shared<TScanCursor>(std::move(context),
            std::move(retainedPlan),
            std::move(controlMemory),
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
