#include "core/scan/parquet_input.h"

#include "core/api/canonical.h"
#include "core/backend/storage_errors.h"

#include <arrow/buffer.h>
#include <arrow/memory_pool.h>
#include <arrow/status.h>
#include <iceberg/file_io.h>

#include <algorithm>
#include <cstddef>
#include <new>
#include <span>
#include <utility>

namespace glamdb::detail {
namespace {

constexpr int64_t MaxDataFileBytes = 32LL << 20;

std::optional<ErrorDetail> CheckStopped(const std::atomic<bool>* cancelled,
    const std::shared_ptr<const std::atomic<bool>>& closing)
{
    if (cancelled != nullptr && cancelled->load()) {
        return ErrorDetail{"CANCELLED", "scan was cancelled", RetryHint::OperatorAction};
    }
    if (closing && closing->load()) {
        return ErrorDetail{"CANCELLED", "engine is closing", RetryHint::OperatorAction};
    }
    return std::nullopt;
}

} // namespace

TScanRandomAccessFile::TScanRandomAccessFile(std::string path,
    int64_t size,
    TScanInputOptions options,
    std::unique_ptr<iceberg::InputFile> file,
    std::unique_ptr<iceberg::SeekableInputStream> stream)
    : Path_(std::move(path))
    , Size_(size)
    , IOContext_(options.Pool != nullptr ? options.Pool : arrow::default_memory_pool())
    , Options_(std::move(options))
    , File_(std::move(file))
    , Stream_(std::move(stream))
{ }

TScanRandomAccessFile::~TScanRandomAccessFile()
{
    try {
        if (!Closed_ && Stream_) {
            [[maybe_unused]] const auto closed = Stream_->Close();
        }
    } catch (...) {
        Closed_ = true;
    }
    Closed_ = true;
    Stream_.reset();
    File_.reset();
}

Result<std::shared_ptr<TScanRandomAccessFile>> TScanRandomAccessFile::Open(iceberg::FileIO& fileIO,
    const std::string& path,
    int64_t expectedSize,
    TScanInputOptions options)
{
    try {
        if (auto stopped = CheckStopped(options.Cancelled, options.Closing)) {
            return std::unexpected(std::move(*stopped));
        }
        if (expectedSize <= 0 || expectedSize > MaxDataFileBytes) {
            return std::unexpected(ErrorDetail{"CORRUPTION",
                BoundedDiagnostic("open data file: invalid manifest file size"),
                RetryHint::OperatorAction});
        }
        auto input = fileIO.NewInputFile(path, static_cast<size_t>(expectedSize));
        if (auto stopped = CheckStopped(options.Cancelled, options.Closing)) {
            return std::unexpected(std::move(*stopped));
        }
        if (!input) {
            return std::unexpected(StorageError(input.error(), "open data file", EStoragePath::Read, "IO_ERROR"));
        }
        auto actualSize = (*input)->Size();
        if (auto stopped = CheckStopped(options.Cancelled, options.Closing)) {
            return std::unexpected(std::move(*stopped));
        }
        if (!actualSize) {
            return std::unexpected(StorageError(actualSize.error(), "stat data file", EStoragePath::Read, "IO_ERROR"));
        }
        if (*actualSize != expectedSize) {
            return std::unexpected(ErrorDetail{"CORRUPTION",
                BoundedDiagnostic("open data file: file length differs from its manifest"),
                RetryHint::OperatorAction});
        }
        auto stream = (*input)->Open();
        if (auto stopped = CheckStopped(options.Cancelled, options.Closing)) {
            if (stream) {
                [[maybe_unused]] const auto closed = (*stream)->Close();
            }
            return std::unexpected(std::move(*stopped));
        }
        if (!stream) {
            return std::unexpected(StorageError(stream.error(), "open data file", EStoragePath::Read, "IO_ERROR"));
        }
        return std::shared_ptr<TScanRandomAccessFile>(
            new TScanRandomAccessFile(path, expectedSize, std::move(options), std::move(*input), std::move(*stream)));
    } catch (const std::bad_alloc& error) {
        return std::unexpected(
            ErrorDetail{"RESOURCE_EXHAUSTED", BoundedDiagnostic(error.what()), RetryHint::OperatorAction});
    } catch (const std::exception& error) {
        return std::unexpected(ErrorDetail{"INTERNAL", BoundedDiagnostic(error.what()), RetryHint::OperatorAction});
    }
}

Result<std::shared_ptr<parquet::FileMetaData>> TScanRandomAccessFile::ReadFooter(const iceberg::Schema& schema,
    NGlamDB::NStorage::TParquetExpectation expected,
    NGlamDB::NStorage::TParquetLimits limits)
{
    std::lock_guard lock(Mutex_);
    if (auto stopped = StoppedErrorLocked()) {
        RecordErrorLocked(*stopped);
        return std::unexpected(std::move(*stopped));
    }
    if (Closed_ || !Stream_) {
        ErrorDetail closed{"INTERNAL", "Parquet input stream is closed", RetryHint::OperatorAction};
        RecordErrorLocked(closed);
        return std::unexpected(std::move(closed));
    }
    std::uint64_t footerBytesRead = 0;
    auto footer =
        NGlamDB::NStorage::ReadParquetFooterFromStream(*Stream_, Size_, schema, expected, limits, &footerBytesRead);
    if (auto stopped = StoppedErrorLocked()) {
        RecordErrorLocked(*stopped);
        return std::unexpected(std::move(*stopped));
    }
    if (!footer) {
        auto detail = StorageError(footer.error(), "verify Parquet file", EStoragePath::Read, "IO_ERROR");
        RecordErrorLocked(detail);
        return std::unexpected(std::move(detail));
    }
    if (Options_.TotalBytesRead) {
        Options_.TotalBytesRead->fetch_add(static_cast<int64_t>(footerBytesRead), std::memory_order_relaxed);
    }
    return std::move(*footer);
}

Status TScanRandomAccessFile::CloseStream()
{
    std::lock_guard lock(Mutex_);
    if (Closed_) {
        if (FirstError_) {
            return std::unexpected(*FirstError_);
        }
        return {};
    }
    Closed_ = true;
    if (!Stream_) {
        File_.reset();
        return {};
    }
    try {
        auto closed = Stream_->Close();
        Stream_.reset();
        File_.reset();
        if (!closed) {
            auto detail = StorageError(closed.error(), "close data file", EStoragePath::Read, "IO_ERROR");
            RecordErrorLocked(detail);
            return std::unexpected(std::move(detail));
        }
        return {};
    } catch (const std::bad_alloc& error) {
        Stream_.reset();
        File_.reset();
        ErrorDetail detail{"RESOURCE_EXHAUSTED", BoundedDiagnostic(error.what()), RetryHint::OperatorAction};
        RecordErrorLocked(detail);
        return std::unexpected(std::move(detail));
    } catch (const std::exception& error) {
        Stream_.reset();
        File_.reset();
        ErrorDetail detail{"IO_ERROR", BoundedDiagnostic(error.what()), RetryHint::OperatorAction};
        RecordErrorLocked(detail);
        return std::unexpected(std::move(detail));
    }
}

std::optional<ErrorDetail> TScanRandomAccessFile::LastError() const
{
    std::lock_guard lock(Mutex_);
    return FirstError_;
}

std::optional<ErrorDetail> TScanRandomAccessFile::StoppedErrorLocked() const
{
    return CheckStopped(Options_.Cancelled, Options_.Closing);
}

arrow::Status TScanRandomAccessFile::CheckActiveStatusLocked()
{
    if (auto stopped = StoppedErrorLocked()) {
        const auto& saved = RecordErrorLocked(std::move(*stopped));
        return arrow::Status::Cancelled(saved.message);
    }
    return arrow::Status::OK();
}

const ErrorDetail& TScanRandomAccessFile::RecordErrorLocked(ErrorDetail detail)
{
    if (!FirstError_) {
        FirstError_ = std::move(detail);
    }
    return *FirstError_;
}

arrow::Status TScanRandomAccessFile::ToArrowStatusLocked(ErrorDetail detail)
{
    const auto& saved = RecordErrorLocked(std::move(detail));
    if (saved.code == "RESOURCE_EXHAUSTED") {
        return arrow::Status::OutOfMemory(saved.message);
    }
    if (saved.code == "CANCELLED") {
        return arrow::Status::Cancelled(saved.message);
    }
    return arrow::Status::IOError(saved.message);
}

arrow::Status TScanRandomAccessFile::Close()
{
    auto status = CloseStream();
    if (!status) {
        if (status.error().code == "RESOURCE_EXHAUSTED") {
            return arrow::Status::OutOfMemory(status.error().message);
        }
        return arrow::Status::IOError(status.error().message);
    }
    return arrow::Status::OK();
}

arrow::Status TScanRandomAccessFile::Abort()
{
    std::lock_guard lock(Mutex_);
    if (!Closed_ && Stream_) {
        try {
            [[maybe_unused]] const auto closed = Stream_->Close();
        } catch (...) {
            Closed_ = true;
        }
    }
    Closed_ = true;
    Stream_.reset();
    File_.reset();
    return arrow::Status::OK();
}

arrow::Result<int64_t> TScanRandomAccessFile::Tell() const
{
    std::lock_guard lock(Mutex_);
    if (Closed_) {
        return arrow::Status::Invalid("Parquet input stream is closed");
    }
    return Position_;
}

bool TScanRandomAccessFile::closed() const
{
    std::lock_guard lock(Mutex_);
    return Closed_;
}

arrow::Status TScanRandomAccessFile::Seek(int64_t position)
{
    std::lock_guard lock(Mutex_);
    ARROW_RETURN_NOT_OK(CheckActiveStatusLocked());
    if (Closed_) {
        return arrow::Status::Invalid("Parquet input stream is closed");
    }
    if (position < 0 || position > Size_) {
        return ToArrowStatusLocked({"CORRUPTION",
            BoundedDiagnostic("seek data file: position outside Parquet file bounds"),
            RetryHint::OperatorAction});
    }
    Position_ = position;
    return arrow::Status::OK();
}

const arrow::io::IOContext& TScanRandomAccessFile::io_context() const
{
    return IOContext_;
}

arrow::Result<int64_t> TScanRandomAccessFile::GetSize()
{
    std::lock_guard lock(Mutex_);
    ARROW_RETURN_NOT_OK(CheckActiveStatusLocked());
    if (Closed_) {
        return arrow::Status::Invalid("Parquet input stream is closed");
    }
    return Size_;
}

arrow::Result<int64_t> TScanRandomAccessFile::ReadAtLocked(arrow::io::ReadRange range, bool allowShortRead, void* out)
{
    ARROW_RETURN_NOT_OK(CheckActiveStatusLocked());
    if (Closed_ || !Stream_) {
        return arrow::Status::Invalid("Parquet input stream is closed");
    }
    if (range.offset < 0 || range.length < 0 || range.offset > Size_) {
        return ToArrowStatusLocked({"CORRUPTION",
            BoundedDiagnostic("read data file: range outside Parquet file bounds"),
            RetryHint::OperatorAction});
    }
    const int64_t available = Size_ - range.offset;
    if (!allowShortRead && range.length > available) {
        return ToArrowStatusLocked({"CORRUPTION",
            BoundedDiagnostic("read data file: truncated Parquet read range"),
            RetryHint::OperatorAction});
    }
    const int64_t toRead = std::min(range.length, available);
    if (toRead == 0) {
        return int64_t{0};
    }
    if (out == nullptr) {
        return ToArrowStatusLocked({"INTERNAL", "read data file: null destination buffer", RetryHint::OperatorAction});
    }
    try {
        auto status = Stream_->ReadFully(range.offset,
            std::span<std::byte>(static_cast<std::byte*>(out), static_cast<size_t>(toRead)));
        if (!status) {
            return ToArrowStatusLocked(StorageError(status.error(), "read data file", EStoragePath::Read, "IO_ERROR"));
        }
        if (Options_.TotalBytesRead) {
            Options_.TotalBytesRead->fetch_add(toRead, std::memory_order_relaxed);
        }
        ARROW_RETURN_NOT_OK(CheckActiveStatusLocked());
        return toRead;
    } catch (const std::bad_alloc& error) {
        return ToArrowStatusLocked({"RESOURCE_EXHAUSTED", BoundedDiagnostic(error.what()), RetryHint::OperatorAction});
    } catch (const std::exception& error) {
        return ToArrowStatusLocked({"IO_ERROR", BoundedDiagnostic(error.what()), RetryHint::OperatorAction});
    }
}

arrow::Result<int64_t> TScanRandomAccessFile::ReadAt(int64_t position, int64_t nbytes, bool allow_short_read, void* out)
{
    std::lock_guard lock(Mutex_);
    return ReadAtLocked({.offset = position, .length = nbytes}, allow_short_read, out);
}

arrow::Result<int64_t> TScanRandomAccessFile::ReadAt(int64_t position, int64_t nbytes, void* out)
{
    return ReadAt(position, nbytes, /*allow_short_read=*/true, out);
}

arrow::Result<std::shared_ptr<arrow::Buffer>>
TScanRandomAccessFile::ReadAt(int64_t position, int64_t nbytes, bool allow_short_read)
{
    std::lock_guard lock(Mutex_);
    ARROW_RETURN_NOT_OK(CheckActiveStatusLocked());
    if (Closed_ || !Stream_) {
        return arrow::Status::Invalid("Parquet input stream is closed");
    }
    if (position < 0 || nbytes < 0 || position > Size_) {
        return ToArrowStatusLocked({"CORRUPTION",
            BoundedDiagnostic("read data file: range outside Parquet file bounds"),
            RetryHint::OperatorAction});
    }
    const int64_t available = Size_ - position;
    if (!allow_short_read && nbytes > available) {
        return ToArrowStatusLocked({"CORRUPTION",
            BoundedDiagnostic("read data file: truncated Parquet read range"),
            RetryHint::OperatorAction});
    }
    const int64_t toRead = std::min(nbytes, available);
    auto bufferResult = arrow::AllocateResizableBuffer(toRead, IOContext_.pool());
    if (!bufferResult.ok()) {
        const auto code = bufferResult.status().IsOutOfMemory() ? "RESOURCE_EXHAUSTED" : "INTERNAL";
        return ToArrowStatusLocked(
            {code, BoundedDiagnostic(bufferResult.status().ToString()), RetryHint::OperatorAction});
    }
    auto buffer = std::move(bufferResult).ValueUnsafe();
    if (toRead > 0) {
        ARROW_ASSIGN_OR_RAISE(const auto actual,
            ReadAtLocked({.offset = position, .length = toRead}, allow_short_read, buffer->mutable_data()));
        if (actual != toRead) {
            auto shrink = buffer->Resize(actual);
            if (!shrink.ok()) {
                return shrink;
            }
        }
    }
    return std::shared_ptr<arrow::Buffer>(std::move(buffer));
}

arrow::Result<std::shared_ptr<arrow::Buffer>> TScanRandomAccessFile::ReadAt(int64_t position, int64_t nbytes)
{
    return ReadAt(position, nbytes, /*allow_short_read=*/true);
}

arrow::Result<int64_t> TScanRandomAccessFile::Read(int64_t nbytes, void* out)
{
    std::lock_guard lock(Mutex_);
    ARROW_ASSIGN_OR_RAISE(const auto read,
        ReadAtLocked({.offset = Position_, .length = nbytes}, /*allowShortRead=*/true, out));
    Position_ += read;
    return read;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> TScanRandomAccessFile::Read(int64_t nbytes)
{
    std::lock_guard lock(Mutex_);
    ARROW_RETURN_NOT_OK(CheckActiveStatusLocked());
    if (Closed_ || !Stream_) {
        return arrow::Status::Invalid("Parquet input stream is closed");
    }
    if (nbytes < 0) {
        return ToArrowStatusLocked(
            {"CORRUPTION", BoundedDiagnostic("read data file: negative byte count"), RetryHint::OperatorAction});
    }
    const int64_t toRead = std::min(nbytes, Size_ - Position_);
    auto bufferResult = arrow::AllocateResizableBuffer(toRead, IOContext_.pool());
    if (!bufferResult.ok()) {
        const auto code = bufferResult.status().IsOutOfMemory() ? "RESOURCE_EXHAUSTED" : "INTERNAL";
        return ToArrowStatusLocked(
            {code, BoundedDiagnostic(bufferResult.status().ToString()), RetryHint::OperatorAction});
    }
    auto buffer = std::move(bufferResult).ValueUnsafe();
    if (toRead > 0) {
        ARROW_ASSIGN_OR_RAISE(const auto actual,
            ReadAtLocked({.offset = Position_, .length = toRead}, /*allowShortRead=*/true, buffer->mutable_data()));
        Position_ += actual;
        if (actual != toRead) {
            auto shrink = buffer->Resize(actual);
            if (!shrink.ok()) {
                return shrink;
            }
        }
    }
    return std::shared_ptr<arrow::Buffer>(std::move(buffer));
}

} // namespace glamdb::detail
