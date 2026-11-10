#include "core/storage/publication_guard.h"
#include "core/io/io_attempt.h"
#include "core/io/store_dirs.h"
#include "core/io/store_uri.h"
#include "core/storage/format_verifier.h"
#include "core/storage/iceberg_error_boundary.h"
#include "core/storage/metadata_validation.h"

#include <iceberg/file_io.h>
#include <iceberg/table_metadata.h>

#include <limits>

namespace NGlamDB::NStorage {
using iceberg::Error;

TStoragePublicationGuard::TStoragePublicationGuard(TPublicationContext context)
    : Context_(std::move(context))
{ }

std::shared_ptr<const TBaseState> TStoragePublicationGuard::GetCandidate() const
{
    return Candidate_;
}

iceberg::Status TStoragePublicationGuard::CheckContext(const TPointerChange& change) const
{
    if (!Context_.Directories || !Context_.FileIO || !Context_.Attempt ||
        Context_.Attempt->Latch() != Context_.Directories->Latch() || change.Namespace != Context_.Namespace ||
        change.Name != Context_.Name ||
        Context_.Limits.RootBytes > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()))
    {
        return iceberg::InvalidArgument("Invalid publication context.");
    }
    const auto isAppend = Context_.Attempt->Kind() == NIO::EAttemptKind::Append;
    if (isAppend != static_cast<bool>(Context_.Base) ||
        (isAppend &&
            (change.Op != EPointerOp::UpdateLocation ||
                change.ExpectedCurrentLocation != Context_.Base->MetadataLocation)) ||
        (!isAppend && (change.Op != EPointerOp::InsertTable || change.ExpectedCurrentLocation)))
    {
        return iceberg::ValidationFailed("Publication does not match the prepared base.");
    }
    const auto uri = NIO::TStoreUri::Parse(change.NewLocation, Context_.Attempt->StoreId());
    if (!uri || uri.Value().Kind() != NIO::EFileKind::Metadata ||
        uri.Value().CollectionId() != Context_.Attempt->CollectionId() ||
        (Context_.Base && (change.NewLocation == Context_.Base->MetadataLocation || !Context_.Base->Metadata)))
    {
        return iceberg::ValidationFailed("Invalid candidate metadata location.");
    }
    if (Context_.Attempt->FirstError() || Context_.Directories->Latch()->IsSet()) {
        return iceberg::IOError("Publication attempt has failed.");
    }
    if (const auto error = Context_.Directories->CheckIdentity()) {
        return iceberg::Invalid("CORRUPTION: Store root identity changed: {}.", error.message());
    }
    return {};
}

iceberg::Status TStoragePublicationGuard::CheckCandidate(const TPointerChange& change)
{
    const auto record = Context_.Attempt->Record(change.NewLocation);
    if (!record) {
        return iceberg::ValidationFailed("Candidate metadata has no durable receipt.");
    }
    const auto& entry = record.Value();
    const auto& receipt = entry.Receipt;
    if (!receipt || entry.Kind != NIO::EFileKind::Metadata || entry.State != NIO::EFileState::DirSynced ||
        entry.FirstError || receipt->AttemptId != Context_.Attempt->Id())
    {
        return iceberg::ValidationFailed("Candidate metadata has no durable receipt.");
    }
    if (entry.Length > Context_.Limits.RootBytes) {
        return iceberg::ValidationFailed("CAPACITY: Candidate metadata exceeds its byte limit.");
    }
    ICEBERG_ASSIGN_OR_RAISE(auto input,
        Context_.FileIO->NewInputFile(change.NewLocation, static_cast<std::int64_t>(record.Value().Length)));
    ICEBERG_ASSIGN_OR_RAISE(auto rootSize, input->Size());
    if (rootSize < 0 || static_cast<std::uint64_t>(rootSize) != record.Value().Length) {
        return iceberg::ValidationFailed("Candidate metadata length differs from its receipt.");
    }
    ICEBERG_ASSIGN_OR_RAISE(auto metadata,
        ReadTableMetadata(*Context_.FileIO,
            change.NewLocation,
            std::nullopt,
            static_cast<std::int64_t>(Context_.Limits.RootBytes)));
    TBaseState candidate;
    candidate.MetadataLocation = change.NewLocation;
    candidate.Metadata = std::shared_ptr<iceberg::TableMetadata>(std::move(metadata));
    if (Context_.Base) {
        ICEBERG_RETURN_UNEXPECTED(
            CheckAppendMetadata(*Context_.Base->Metadata, *candidate.Metadata, Context_.ExpectedSummary));
        candidate.Usage = Context_.Base->Usage;
    } else {
        const auto location = "store://" + std::string(Context_.Attempt->StoreId()) + "/warehouse/" +
            std::string(Context_.Attempt->CollectionId());
        ICEBERG_RETURN_UNEXPECTED(CheckCreateMetadata(*candidate.Metadata, Context_.Profile, location));
    }
    candidate.Usage.RootBytes = record.Value().Length;
    candidate.Usage.Snapshots = candidate.Metadata->snapshots.size();
    candidate.Usage.SnapshotLogEntries = candidate.Metadata->snapshot_log.size();
    if (candidate.Metadata->metadata_log.size() > Context_.Limits.MetadataLogEntries) {
        return iceberg::ValidationFailed("CAPACITY: Metadata history exceeds storage capacity.");
    }
    if (Context_.Base) {
        ICEBERG_ASSIGN_OR_RAISE(candidate,
            CheckAppendClosure(Context_.FileIO,
                std::move(candidate),
                Context_.Base->CurrentManifests,
                Context_.Attempt,
                Context_.Limits));
    }
    const auto total = ReplaceUsage(Context_.StoreUsage,
        {.Previous = Context_.Base ? Context_.Base->Usage : TStorageUsage{}, .Next = candidate.Usage});
    if (!total || CheckStorageCapacity(total.Value(), Context_.Limits)) {
        return iceberg::ValidationFailed("CAPACITY: Candidate exceeds store capacity.");
    }
    if (const auto error = Context_.Attempt->MarkFormatVerified(change.NewLocation)) {
        return iceberg::IOError("Cannot verify candidate metadata: {}.", error.message());
    }
    if (Context_.Attempt->FirstError() || Context_.Directories->Latch()->IsSet()) {
        return iceberg::IOError("Publication attempt failed during verification.");
    }
    Candidate_ = std::make_shared<const TBaseState>(std::move(candidate));
    return {};
}

iceberg::Status TStoragePublicationGuard::BeforePointerChange(const TPointerChange& change)
{
    auto status = CatchIcebergExceptions([&]() -> iceberg::Status {
        if (Invoked_) {
            return iceberg::ValidationFailed("Publication guard can only be invoked once.");
        }
        Invoked_ = true;
        ICEBERG_RETURN_UNEXPECTED(CheckContext(change));
        if (const auto error = Context_.Attempt->Seal()) {
            return iceberg::IOError("Cannot seal publication attempt: {}.", error.message());
        }
        return CheckCandidate(change);
    });
    if (!status) {
        Candidate_.reset();
        if (Context_.Attempt) {
            Context_.Attempt->FailFile(change.NewLocation, std::make_error_code(std::errc::state_not_recoverable));
        }
    }
    return status;
}

}
