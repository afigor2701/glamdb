#include "core/storage/durable_local_file_io.h"

#include "core/io/durable_file.h"
#include "core/io/store_uri.h"
#include "core/storage/iceberg_error_boundary.h"

#include <algorithm>
#include <atomic>
#include <limits>
#include <mutex>
#include <new>
#include <thread>
#include <utility>

namespace NGlamDB::NStorage {
namespace {

using TAttempt = std::shared_ptr<NIO::TIOAttempt>;

// Identity violations of store paths are corruption: a symlink (ELOOP with
// O_NOFOLLOW), a non-directory or a cross-device entry where the store layout
// requires its own directory or file.
bool IsLayoutViolation(std::error_code error)
{
    return error == std::errc::state_not_recoverable || error == std::errc::too_many_symbolic_link_levels ||
        error == std::errc::not_a_directory || error == std::errc::cross_device_link;
}

std::unexpected<iceberg::Error> Failure(std::error_code error, std::string_view path, const TAttempt& attempt = {})
{
    if (attempt) {
        attempt->FailFile(path, error);
    }
    auto kind = iceberg::ErrorKind::kIOError;
    if (IsLayoutViolation(error)) {
        kind = iceberg::ErrorKind::kInvalid;
    } else if (error == std::errc::file_exists) {
        kind = iceberg::ErrorKind::kAlreadyExists;
    } else if (error == std::errc::no_such_file_or_directory) {
        kind = iceberg::ErrorKind::kNotFound;
    } else if (error == std::errc::invalid_argument || error == std::errc::value_too_large) {
        kind = iceberg::ErrorKind::kInvalidArgument;
    } else if (error == std::errc::operation_not_permitted || error == std::errc::operation_canceled) {
        kind = iceberg::ErrorKind::kNotAllowed;
    }
    std::string message = kind == iceberg::ErrorKind::kInvalid ? std::string(CorruptionPrefix) : std::string();
    message += std::string(path) + ": " + error.message();
    return std::unexpected(iceberg::Error{kind, std::move(message)});
}

// Opening an existing store file: a non-regular file (invalid_argument from
// OpenRead) or one larger than its file-kind limit cannot have been published.
std::unexpected<iceberg::Error> ReadFailure(std::error_code error, std::string_view path, const TAttempt& attempt)
{
    if (error == std::errc::invalid_argument || error == std::errc::file_too_large) {
        if (attempt) {
            attempt->FailFile(path, error);
        }
        return std::unexpected(iceberg::Error{iceberg::ErrorKind::kInvalid,
            std::string(CorruptionPrefix) + std::string(path) + ": " + error.message()});
    }
    return Failure(error, path, attempt);
}

template <typename Function>
auto Boundary(const TAttempt& attempt, std::string_view path, Function function) -> decltype(function())
{
    try {
        return function();
    } catch (const std::system_error& error) {
        return Failure(error.code(), path, attempt);
    } catch (const std::bad_alloc&) {
        return Failure(std::make_error_code(std::errc::not_enough_memory), path, attempt);
    } catch (...) {
        return Failure(std::make_error_code(std::errc::io_error), path, attempt);
    }
}

std::error_code Code(std::errc error)
{
    return std::make_error_code(error);
}

}

struct TLocalFileIOState
{
    std::shared_ptr<NIO::TStoreDirs> Directories;
    std::string StoreId;
    TFileIOLimits Limits;
    std::mutex Mutex;
    TAttempt Active;
    std::thread::id Owner;
    bool ReadOnly = false;
    std::shared_ptr<TLocalFileIOState> WriteAuthority;
    std::atomic<std::uint64_t> DeniedDeletes{0};

    TAttempt Current()
    {
        std::lock_guard lock(Mutex);
        return Active;
    }

    TAttempt CurrentRead()
    {
        std::lock_guard lock(Mutex);
        return Owner == std::this_thread::get_id() ? Active : TAttempt{};
    }

    std::error_code CheckReadable(std::string_view path)
    {
        const auto attempt = WriteAuthority ? WriteAuthority->Current() : Current();
        return attempt ? attempt->CanRead(path) : std::error_code{};
    }

    std::uint64_t Limit(NIO::EFileKind kind) const
    {
        switch (kind) {
            case NIO::EFileKind::Data:
                return Limits.DataFileBytes;
            case NIO::EFileKind::Metadata:
                return Limits.MetadataBytes;
            case NIO::EFileKind::Manifest:
                return Limits.ManifestBytes;
            case NIO::EFileKind::ManifestList:
                return Limits.ManifestListBytes;
            case NIO::EFileKind::Other:
                return 0;
        }
        return 0;
    }

    iceberg::Result<NIO::TStoreUri> Parse(const std::string& path, const TAttempt& attempt)
    {
        auto uri = NIO::TStoreUri::Parse(path, StoreId);
        if (!uri) {
            return Failure(uri.Error(), path, attempt);
        }
        if (attempt && (attempt->StoreId() != StoreId || attempt->CollectionId() != uri.Value().CollectionId())) {
            return Failure(Code(std::errc::invalid_argument), path, attempt);
        }
        return std::move(uri).Value();
    }

    iceberg::Result<NIO::THandle> Directory(const NIO::TStoreUri& uri, const TAttempt& attempt)
    {
        auto collection = Directories->OpenCollection(uri.CollectionId());
        if (!collection) {
            return Failure(collection.Error(), uri.String(), attempt);
        }
        return uri.Kind() == NIO::EFileKind::Data ? collection.Value().Data : collection.Value().Metadata;
    }
};

namespace {

struct TInputContext
{
    std::shared_ptr<TLocalFileIOState> State;
    NIO::THandle Directory;
    std::string Path;
    std::string Leaf;
    NIO::EFileKind Kind;
    NIO::TFileStat Identity;
    TAttempt Attempt;

    iceberg::Result<std::int64_t> Size(const NIO::THandle& file) const
    {
        if (auto error = State->CheckReadable(Path)) {
            return Failure(error, Path, Attempt);
        }
        if (Attempt) {
            if (auto error = Attempt->CanRead(Path)) {
                return Failure(error, Path, Attempt);
            }
        }
        auto info = State->Directories->Fs()->Stat(file);
        if (!info) {
            return Failure(info.Error(), Path, Attempt);
        }
        if (info.Value().Type != NIO::EFileType::Regular || info.Value().Size != Identity.Size ||
            info.Value().Device != Identity.Device || info.Value().Inode != Identity.Inode)
        {
            return Failure(Code(std::errc::state_not_recoverable), Path, Attempt);
        }
        if (info.Value().Size > State->Limit(Kind)) {
            return ReadFailure(Code(std::errc::file_too_large), Path, Attempt);
        }
        return static_cast<std::int64_t>(info.Value().Size);
    }
};

class TLocalInputStream final : public iceberg::SeekableInputStream
{
public:
    TLocalInputStream(std::shared_ptr<TInputContext> context, NIO::THandle file)
        : Context_(std::move(context))
        , File_(std::move(file))
    { }

    iceberg::Result<std::int64_t> Position() const override
    {
        std::lock_guard lock(Mutex_);
        if (Closed_) {
            return Failure(Code(std::errc::bad_file_descriptor), Context_->Path, Context_->Attempt);
        }
        return Position_;
    }

    iceberg::Status Seek(std::int64_t position) override
    {
        std::lock_guard lock(Mutex_);
        return Boundary(Context_->Attempt, Context_->Path, [&]() -> iceberg::Status {
            auto size = GetSize();
            if (!size) {
                return std::unexpected(size.error());
            }
            if (position < 0 || position > *size) {
                return Failure(Code(std::errc::invalid_argument), Context_->Path, Context_->Attempt);
            }
            Position_ = position;
            return {};
        });
    }

    iceberg::Result<std::int64_t> Read(std::span<std::byte> out) override
    {
        std::lock_guard lock(Mutex_);
        return Boundary(Context_->Attempt, Context_->Path, [&]() -> iceberg::Result<std::int64_t> {
            auto size = GetSize();
            if (!size) {
                return std::unexpected(size.error());
            }
            const auto length = std::min<std::uint64_t>(out.size(), *size - Position_);
            return ReadBytes(out.first(static_cast<std::size_t>(length)));
        });
    }

    iceberg::Status ReadFully(std::int64_t position, std::span<std::byte> out) override
    {
        std::lock_guard lock(Mutex_);
        return Boundary(Context_->Attempt, Context_->Path, [&]() -> iceberg::Status {
            auto size = GetSize();
            if (!size) {
                return std::unexpected(size.error());
            }
            if (position < 0) {
                return Failure(Code(std::errc::invalid_argument), Context_->Path, Context_->Attempt);
            }
            if (position > *size || out.size() > static_cast<std::uint64_t>(*size - position)) {
                return Failure(Code(std::errc::state_not_recoverable), Context_->Path, Context_->Attempt);
            }
            Position_ = position;
            auto read = ReadBytes(out);
            return read ? iceberg::Status{} : std::unexpected(read.error());
        });
    }

    iceberg::Status Close() override
    {
        std::lock_guard lock(Mutex_);
        if (Closed_) {
            return CloseError_ ? iceberg::Status(std::unexpected(*CloseError_)) : iceberg::Status{};
        }
        Closed_ = true;
        auto result = Boundary(Context_->Attempt, Context_->Path, [&]() -> iceberg::Status {
            const auto error = Context_->State->Directories->Fs()->Close(File_);
            return error ? iceberg::Status(Failure(error, Context_->Path, Context_->Attempt)) : iceberg::Status{};
        });
        File_.reset();
        if (!result) {
            CloseError_ = result.error();
        }
        return result;
    }

private:
    std::shared_ptr<TInputContext> Context_;
    NIO::THandle File_;
    mutable std::mutex Mutex_;
    std::int64_t Position_ = 0;
    bool Closed_ = false;
    std::optional<iceberg::Error> CloseError_;

    iceberg::Result<std::int64_t> GetSize() const
    {
        if (Closed_) {
            return Failure(Code(std::errc::bad_file_descriptor), Context_->Path, Context_->Attempt);
        }
        return Context_->Size(File_);
    }

    iceberg::Result<std::int64_t> ReadBytes(std::span<std::byte> bytes)
    {
        const auto requested = bytes.size();
        while (!bytes.empty()) {
            if (Context_->Attempt) {
                if (auto error = Context_->Attempt->ChargeRead(bytes.size())) {
                    return Failure(error, Context_->Path, Context_->Attempt);
                }
            }
            auto read = Context_->State->Directories->Fs()->PRead(File_, Position_, bytes);
            if (!read) {
                if (read.Error() == std::errc::interrupted) {
                    continue;
                }
                return Failure(read.Error(), Context_->Path, Context_->Attempt);
            }
            if (read.Value() == 0 || read.Value() > bytes.size()) {
                return Failure(Code(std::errc::state_not_recoverable), Context_->Path, Context_->Attempt);
            }
            Position_ += static_cast<std::int64_t>(read.Value());
            bytes = bytes.subspan(read.Value());
        }
        return static_cast<std::int64_t>(requested);
    }
};

class TLocalInputFile final : public iceberg::InputFile
{
public:
    TLocalInputFile(std::shared_ptr<TInputContext> context, NIO::THandle file)
        : Context_(std::move(context))
        , File_(std::move(file))
    { }
    std::string_view location() const override
    {
        return Context_->Path;
    }
    iceberg::Result<std::int64_t> Size() const override
    {
        return Boundary(Context_->Attempt, Context_->Path, [&] {
            return Context_->Size(File_);
        });
    }
    iceberg::Result<std::unique_ptr<iceberg::SeekableInputStream>> Open() override
    {
        return Boundary(Context_->Attempt,
            Context_->Path,
            [&]() -> iceberg::Result<std::unique_ptr<iceberg::SeekableInputStream>> {
                auto opened = Context_->State->Directories->Fs()->OpenRead(Context_->Directory, Context_->Leaf);
                if (!opened) {
                    return Failure(opened.Error(), Context_->Path, Context_->Attempt);
                }
                auto size = Context_->Size(opened.Value());
                if (!size) {
                    return std::unexpected(size.error());
                }
                return std::make_unique<TLocalInputStream>(Context_, std::move(opened).Value());
            });
    }

private:
    std::shared_ptr<TInputContext> Context_;
    NIO::THandle File_;
};

class TLocalOutputStream final : public iceberg::PositionOutputStream
{
public:
    TLocalOutputStream(std::shared_ptr<NIO::TDurableFile> file, TAttempt attempt, std::string path, std::uint64_t limit)
        : File_(std::move(file))
        , Attempt_(std::move(attempt))
        , Path_(std::move(path))
        , Limit_(limit)
    { }

    iceberg::Result<std::int64_t> Position() const override
    {
        return Boundary(Attempt_, Path_, [&]() -> iceberg::Result<std::int64_t> {
            auto position = File_->Position();
            if (!position) {
                return Failure(position.Error(), Path_, Attempt_);
            }
            if (position.Value() > std::numeric_limits<std::int64_t>::max()) {
                return Failure(Code(std::errc::value_too_large), Path_, Attempt_);
            }
            return static_cast<std::int64_t>(position.Value());
        });
    }
    iceberg::Result<std::int64_t> StoredLength() const override
    {
        return static_cast<std::int64_t>(File_->StoredLength());
    }
    iceberg::Status Write(std::span<const std::byte> bytes) override
    {
        std::lock_guard lock(WriteMutex_);
        try {
            auto position = File_->Position();
            if (!position || position.Value() > Limit_ || bytes.size() > Limit_ - position.Value()) {
                Attempt_->FailFile(Path_, Code(std::errc::file_too_large));
            }
            const auto error = File_->Write(bytes);
            if (error) {
                Attempt_->FailFile(Path_, error);
            }
        } catch (...) {
            Attempt_->FailFile(Path_, Code(std::errc::io_error));
        }
        return {};
    }
    iceberg::Status Flush() override
    {
        try {
            const auto error = File_->Flush();
            if (error) {
                Attempt_->FailFile(Path_, error);
            }
        } catch (...) {
            Attempt_->FailFile(Path_, Code(std::errc::io_error));
        }
        return {};
    }
    iceberg::Status Close() override
    {
        return Boundary(Attempt_, Path_, [&]() -> iceberg::Status {
            const auto error = File_->Close();
            return error ? iceberg::Status(Failure(error, Path_, Attempt_)) : iceberg::Status{};
        });
    }

private:
    std::shared_ptr<NIO::TDurableFile> File_;
    TAttempt Attempt_;
    std::string Path_;
    std::uint64_t Limit_;
    std::mutex WriteMutex_;
};

class TLocalOutputFile final : public iceberg::OutputFile
{
public:
    TLocalOutputFile(std::shared_ptr<TLocalFileIOState> state, NIO::TStoreUri uri, TAttempt attempt)
        : State_(std::move(state))
        , Uri_(std::move(uri))
        , Attempt_(std::move(attempt))
    { }
    std::string_view location() const override
    {
        return Uri_.String();
    }
    iceberg::Result<std::unique_ptr<iceberg::PositionOutputStream>> Create() override
    {
        return Boundary(Attempt_,
            Uri_.String(),
            [&]() -> iceberg::Result<std::unique_ptr<iceberg::PositionOutputStream>> {
                if (State_->Current() != Attempt_) {
                    return Failure(Code(std::errc::operation_canceled), Uri_.String(), Attempt_);
                }
                if (auto error = State_->Directories->CheckIdentity()) {
                    return Failure(error, Uri_.String(), Attempt_);
                }
                auto directory = State_->Directory(Uri_, Attempt_);
                if (!directory) {
                    return std::unexpected(directory.error());
                }
                auto file = NIO::TDurableFile::Create(State_->Directories->Fs(),
                    *directory,
                    Uri_.String(),
                    State_->Directories->Latch(),
                    Attempt_);
                if (!file) {
                    return Failure(file.Error(), Uri_.String(), Attempt_);
                }
                return std::make_unique<TLocalOutputStream>(std::move(file).Value(),
                    Attempt_,
                    Uri_.String(),
                    State_->Limit(Uri_.Kind()));
            });
    }
    iceberg::Result<std::unique_ptr<iceberg::PositionOutputStream>> CreateOrOverwrite() override
    {
        return Create();
    }

private:
    std::shared_ptr<TLocalFileIOState> State_;
    NIO::TStoreUri Uri_;
    TAttempt Attempt_;
};

}

TAttemptScope::TAttemptScope(std::shared_ptr<TLocalFileIOState> state, TAttempt attempt)
    : State_(std::move(state))
    , Attempt_(std::move(attempt))
{ }
TAttemptScope::TAttemptScope(TAttemptScope&& other) noexcept = default;
TAttemptScope& TAttemptScope::operator=(TAttemptScope&& other) noexcept
{
    if (this != &other) {
        Reset();
        State_ = std::move(other.State_);
        Attempt_ = std::move(other.Attempt_);
    }
    return *this;
}
TAttemptScope::~TAttemptScope()
{
    Reset();
}
void TAttemptScope::Reset() noexcept
{
    if (State_ && Attempt_) {
        std::lock_guard lock(State_->Mutex);
        if (State_->Active == Attempt_) {
            State_->Active.reset();
            State_->Owner = {};
        }
        Attempt_->Deactivate();
    }
    Attempt_.reset();
    State_.reset();
}

TDurableLocalFileIO::TDurableLocalFileIO(std::shared_ptr<TLocalFileIOState> state)
    : State_(std::move(state))
{ }

iceberg::Result<std::shared_ptr<TDurableLocalFileIO>>
TDurableLocalFileIO::Make(std::shared_ptr<NIO::TStoreDirs> directories, std::string storeId, TFileIOLimits limits)
{
    return Boundary({}, "FileIO", [&]() -> iceberg::Result<std::shared_ptr<TDurableLocalFileIO>> {
        if (!directories || !NIO::IsCanonicalUuid(storeId)) {
            return iceberg::InvalidArgument("Invalid FileIO store identity");
        }
        for (auto limit : {limits.DataFileBytes, limits.MetadataBytes, limits.ManifestBytes, limits.ManifestListBytes})
        {
            if (limit > std::numeric_limits<std::int64_t>::max()) {
                return iceberg::InvalidArgument("FileIO limit exceeds signed file size range");
            }
        }
        auto state = std::make_shared<TLocalFileIOState>();
        state->Directories = std::move(directories);
        state->StoreId = std::move(storeId);
        state->Limits = limits;
        return std::shared_ptr<TDurableLocalFileIO>(new TDurableLocalFileIO(std::move(state)));
    });
}

iceberg::Result<TAttemptScope> TDurableLocalFileIO::BindAttempt(TAttempt attempt)
{
    return Boundary(attempt, {}, [&]() -> iceberg::Result<TAttemptScope> {
        if (State_->ReadOnly) {
            return iceberg::NotAllowed("Read-only FileIO cannot bind a write attempt");
        }
        if (!attempt || attempt->StoreId() != State_->StoreId || attempt->Latch() != State_->Directories->Latch()) {
            return iceberg::InvalidArgument("Attempt belongs to a different store or latch");
        }
        if (auto error = attempt->CheckBinding()) {
            return Failure(error, {}, attempt);
        }
        std::lock_guard lock(State_->Mutex);
        if (State_->Active) {
            return iceberg::NotAllowed("FileIO already has an active attempt");
        }
        State_->Active = attempt;
        State_->Owner = std::this_thread::get_id();
        return TAttemptScope(State_, std::move(attempt));
    });
}

iceberg::Result<std::shared_ptr<TDurableLocalFileIO>> TDurableLocalFileIO::ReadOnlyView() const
{
    return Boundary({}, "FileIO", [&]() -> iceberg::Result<std::shared_ptr<TDurableLocalFileIO>> {
        auto view = Make(State_->Directories, State_->StoreId, State_->Limits);
        if (view) {
            (*view)->State_->ReadOnly = true;
            (*view)->State_->WriteAuthority = State_->WriteAuthority ? State_->WriteAuthority : State_;
        }
        return view;
    });
}

iceberg::Result<std::unique_ptr<iceberg::InputFile>>
TDurableLocalFileIO::Input(std::string location, std::optional<std::uint64_t> length, TAttempt attempt)
{
    return Boundary(attempt, location, [&]() -> iceberg::Result<std::unique_ptr<iceberg::InputFile>> {
        auto uri = State_->Parse(location, attempt);
        if (!uri) {
            return std::unexpected(uri.error());
        }
        if (auto error = State_->CheckReadable(location)) {
            return Failure(error, location, attempt);
        }
        if (attempt) {
            if (auto error = attempt->CanRead(location)) {
                return Failure(error, location, attempt);
            }
        }
        auto directory = State_->Directory(*uri, attempt);
        if (!directory) {
            return std::unexpected(directory.error());
        }
        auto file = State_->Directories->Fs()->OpenRead(*directory, uri->Leaf());
        if (!file) {
            return ReadFailure(file.Error(), location, attempt);
        }
        auto info = State_->Directories->Fs()->Stat(file.Value());
        if (!info) {
            return Failure(info.Error(), location, attempt);
        }
        if (info.Value().Type != NIO::EFileType::Regular || (length && *length != info.Value().Size)) {
            return Failure(Code(std::errc::state_not_recoverable), location, attempt);
        }
        if (info.Value().Size > State_->Limit(uri->Kind())) {
            return ReadFailure(Code(std::errc::file_too_large), location, attempt);
        }
        auto context = std::make_shared<TInputContext>(
            TInputContext{State_, *directory, location, std::string(uri->Leaf()), uri->Kind(), info.Value(), attempt});
        return std::make_unique<TLocalInputFile>(std::move(context), std::move(file).Value());
    });
}

iceberg::Result<std::unique_ptr<iceberg::InputFile>> TDurableLocalFileIO::NewInputFile(std::string location)
{
    return Input(std::move(location), std::nullopt, State_->CurrentRead());
}
iceberg::Result<std::unique_ptr<iceberg::InputFile>> TDurableLocalFileIO::NewInputFile(std::string location,
    std::size_t length)
{
    return Input(std::move(location), length, State_->CurrentRead());
}
iceberg::Result<std::unique_ptr<iceberg::OutputFile>> TDurableLocalFileIO::NewOutputFile(std::string location)
{
    const auto attempt = State_->Current();
    return Boundary(attempt, location, [&]() -> iceberg::Result<std::unique_ptr<iceberg::OutputFile>> {
        if (State_->ReadOnly || !attempt) {
            return iceberg::NotAllowed("Writing requires an active attempt");
        }
        auto uri = State_->Parse(location, attempt);
        if (!uri) {
            return std::unexpected(uri.error());
        }
        return std::make_unique<TLocalOutputFile>(State_, std::move(*uri), attempt);
    });
}
iceberg::Result<std::string> TDurableLocalFileIO::ReadFile(const std::string& location,
    std::optional<std::size_t> length)
{
    return Read(location, length, State_->CurrentRead());
}
iceberg::Result<std::string>
TDurableLocalFileIO::Read(const std::string& location, std::optional<std::size_t> length, TAttempt attempt)
{
    return Boundary(attempt, location, [&]() -> iceberg::Result<std::string> {
        auto file = Input(location, length, attempt);
        if (!file) {
            return std::unexpected(file.error());
        }
        auto size = (*file)->Size();
        if (!size) {
            return std::unexpected(size.error());
        }
        std::string content(static_cast<std::size_t>(*size), '\0');
        auto stream = (*file)->Open();
        if (!stream) {
            return std::unexpected(stream.error());
        }
        auto read = (*stream)->ReadFully(0, std::as_writable_bytes(std::span(content.data(), content.size())));
        auto closed = (*stream)->Close();
        if (!read) {
            return std::unexpected(read.error());
        }
        if (!closed) {
            return std::unexpected(closed.error());
        }
        return content;
    });
}
iceberg::Status TDurableLocalFileIO::WriteFile(const std::string& location, std::string_view content)
{
    const auto attempt = State_->Current();
    return Boundary(attempt, location, [&]() -> iceberg::Status {
        if (State_->ReadOnly) {
            return iceberg::NotAllowed("Read-only FileIO cannot write files");
        }
        auto uri = State_->Parse(location, attempt);
        if (!uri) {
            return std::unexpected(uri.error());
        }
        if (uri->Kind() != NIO::EFileKind::Metadata || content.size() > State_->Limits.MetadataBytes) {
            return Failure(Code(std::errc::invalid_argument), location, attempt);
        }
        auto file = NewOutputFile(location);
        if (!file) {
            return std::unexpected(file.error());
        }
        auto stream = (*file)->Create();
        if (!stream) {
            return std::unexpected(stream.error());
        }
        auto written = (*stream)->Write(std::as_bytes(std::span(content.data(), content.size())));
        auto closed = (*stream)->Close();
        if (!written) {
            return written;
        }
        if (!closed) {
            return closed;
        }
        auto actual = Read(location, content.size(), attempt);
        if (!actual) {
            return std::unexpected(actual.error());
        }
        if (*actual != content) {
            return Failure(Code(std::errc::state_not_recoverable), location, attempt);
        }
        return {};
    });
}
iceberg::Status TDurableLocalFileIO::DeleteFile(const std::string&)
{
    State_->DeniedDeletes.fetch_add(1, std::memory_order_relaxed);
    return iceberg::NotAllowed("Deleting immutable storage files is disabled");
}
iceberg::Status TDurableLocalFileIO::DeleteFiles(const std::vector<std::string>&)
{
    State_->DeniedDeletes.fetch_add(1, std::memory_order_relaxed);
    return iceberg::NotAllowed("Deleting immutable storage files is disabled");
}
std::uint64_t TDurableLocalFileIO::DeniedDeleteCount() const noexcept
{
    return State_->DeniedDeletes.load(std::memory_order_relaxed);
}

}
