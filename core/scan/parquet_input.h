#pragma once

#include "core/storage/format_verifier.h"
#include "storage/types.h"

#include <arrow/io/interfaces.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

namespace arrow {
class MemoryPool;
} // namespace arrow

namespace iceberg {
class FileIO;
class InputFile;
class Schema;
class SeekableInputStream;
} // namespace iceberg

namespace parquet {
class FileMetaData;
} // namespace parquet

namespace glamdb::detail {

struct TScanInputOptions
{
    arrow::MemoryPool* Pool = nullptr;
    const std::atomic<bool>* Cancelled = nullptr;
    std::shared_ptr<const std::atomic<bool>> Closing{};
    std::shared_ptr<std::atomic<int64_t>> TotalBytesRead{};
};

// Positional Arrow RandomAccessFile over a store-scoped iceberg::InputFile.
// Preserves the first storage or cancellation ErrorDetail across upstream
// Parquet status/exception wrappers so transient I/O errors are never reported
// as persisted file corruption.
class TScanRandomAccessFile final : public arrow::io::RandomAccessFile
{
public:
    static Result<std::shared_ptr<TScanRandomAccessFile>>
    Open(iceberg::FileIO& fileIO, const std::string& path, int64_t expectedSize, TScanInputOptions options);

    ~TScanRandomAccessFile() override;

    Result<std::shared_ptr<parquet::FileMetaData>> ReadFooter(const iceberg::Schema& schema,
        NGlamDB::NStorage::TParquetExpectation expected,
        NGlamDB::NStorage::TParquetLimits limits = {});

    Status CloseStream();
    std::optional<ErrorDetail> LastError() const;

    arrow::Status Close() override;
    arrow::Status Abort() override;
    arrow::Result<int64_t> Tell() const override;
    bool closed() const override;
    arrow::Status Seek(int64_t position) override;
    arrow::Result<int64_t> Read(int64_t nbytes, void* out) override;
    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override;
    const arrow::io::IOContext& io_context() const override;
    arrow::Result<int64_t> GetSize() override;
    arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, bool allow_short_read, void* out) override;
    arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override;
    arrow::Result<std::shared_ptr<arrow::Buffer>>
    ReadAt(int64_t position, int64_t nbytes, bool allow_short_read) override;
    arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t position, int64_t nbytes) override;

private:
    const std::string Path_;
    const int64_t Size_ = 0;
    const arrow::io::IOContext IOContext_;
    const TScanInputOptions Options_;

    mutable std::mutex Mutex_;
    std::unique_ptr<iceberg::InputFile> File_;
    std::unique_ptr<iceberg::SeekableInputStream> Stream_;
    int64_t Position_ = 0;
    bool Closed_ = false;
    std::optional<ErrorDetail> FirstError_;

    TScanRandomAccessFile(std::string path,
        int64_t size,
        TScanInputOptions options,
        std::unique_ptr<iceberg::InputFile> file,
        std::unique_ptr<iceberg::SeekableInputStream> stream);

    std::optional<ErrorDetail> StoppedErrorLocked() const;
    arrow::Status CheckActiveStatusLocked();
    const ErrorDetail& RecordErrorLocked(ErrorDetail detail);
    arrow::Status ToArrowStatusLocked(ErrorDetail detail);
    arrow::Result<int64_t> ReadAtLocked(arrow::io::ReadRange range, bool allowShortRead, void* out);
};

} // namespace glamdb::detail
