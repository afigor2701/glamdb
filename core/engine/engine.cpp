#include "storage/engine.h"
#include "core/telemetry/structured_log.h"

#include "core/api/binary.h"
#include "core/api/canonical.h"
#include "core/api/codec.h"
#include "core/api/write_settings.h"
#include "core/engine/engine_factory.h"
#include "core/engine/metadata_pages.h"
#include "core/engine/storage_mapping.h"
#include "core/engine/store_lease.h"
#include "core/telemetry/metrics.h"

#if GLAMDB_HAS_STORAGE
#include "core/backend/local_storage_backend.h"
#include "core/scan/file_metadata.h"
#include "core/scan/metadata.h"
#include "core/scan/scan_cursor.h"
#endif

#include "core/resources/envelopes.h"
#include "core/resources/metadata_result.h"
#include "storage/resources.h"

#include <algorithm>
#include <atomic>
#include <limits>
#include <map>
#include <mutex>
#include <new>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <arrow/type.h>

namespace glamdb {
namespace {

using namespace detail;

template <class T>
T Take(Result<T> result)
{
    if (!result) {
        throw Failure(result.error());
    }
    return std::move(*result);
}

ErrorDetail ExceptionError(const std::exception& error)
{
    return {dynamic_cast<const std::bad_alloc*>(&error) != nullptr ? "RESOURCE_EXHAUSTED" : "INTERNAL",
        BoundedDiagnostic(error.what()),
        RetryHint::OperatorAction};
}

AppendOutcome Rejected(std::string operation_id, CommitPhase phase, ErrorDetail error)
{
    return {std::move(operation_id),
        CommitDisposition::DefinitelyNotCommitted,
        phase,
        std::nullopt,
        std::move(error),
        std::nullopt};
}

void ValidateConfig(const StoreInfo& info, const Limits& limits)
{
    Require(ValidUuid(info.store_id) && info.profile_id == "local-delete-extra-v1",
        "INVALID_ARGUMENT",
        "explicit canonical store UUID and supported profile are required");
    Require(limits.max_pending_calls >= 1 && limits.max_pending_calls <= 16 && limits.max_owned_input_bytes >= 1 &&
            limits.max_owned_input_bytes <= 128LL * 1024 * 1024 && limits.writer_wait.count() >= 0 &&
            limits.writer_wait <= std::chrono::milliseconds(5000) && limits.append_deadline.count() > 0 &&
            limits.append_deadline <= std::chrono::milliseconds(60000),
        "INVALID_ARGUMENT",
        "invalid writer resource limits");
    // Catalog limits are also enforced by the catalog itself; checking them
    // here rejects a configuration before the store root is created.
    Require(limits.max_catalog_bytes >= 65536 && limits.max_catalog_bytes <= 256LL * 1024 * 1024 &&
            limits.max_operations > 0 && limits.max_operations <= 4096 && limits.max_collections > 0 &&
            limits.max_collections <= 8,
        "INVALID_ARGUMENT",
        "invalid catalog limits");
}

RecoveryContext Context(const BoundOperation& binding, const ErrorDetail& first, bool publication)
{
    return {binding.base.store_id,
        binding.collection_id,
        binding.operation_id,
        binding.payload_hash,
        binding.hash_version,
        binding.base,
        binding.attempt_id,
        std::nullopt,
        std::nullopt,
        first,
        publication};
}

AppendOutcome UnresolvedAppend(RecoveryContext context, std::string_view payload_hash)
{
    if (context.lookup_blocked) {
        context.payload_hash = payload_hash;
    } else {
        Require(payload_hash == context.payload_hash,
            "OPERATION_CONFLICT",
            "operation ID is bound to different content");
    }
    auto error = context.first_error;
    error.retry_hint = RetryHint::SameIdAfterReconcile;
    return {context.operation_id,
        CommitDisposition::CommitUnknown,
        CommitPhase::Reconciling,
        std::nullopt,
        std::move(error),
        std::move(context)};
}

std::shared_ptr<StorageBackend>
MakeBackend(const BackendFactory& factory, const std::filesystem::path& root, const StoreInfo& info)
{
    if (!factory) {
        return {};
    }
    return Take(factory(root, info));
}

BackendFactory DefaultBackendFactory(std::shared_ptr<ResourceOwner> resources)
{
#if GLAMDB_HAS_STORAGE
    return [resources = std::move(resources)](const std::filesystem::path& root,
               const StoreInfo& info) -> Result<std::shared_ptr<StorageBackend>> {
        auto backend = TLocalStorageBackend::Open(root, info, {.Resources = resources});
        if (!backend) {
            return std::unexpected(backend.error());
        }
        return std::shared_ptr<StorageBackend>(std::move(*backend));
    };
#else
    (void)resources;
    return {};
#endif
}

BackendFactory FixedBackend(std::shared_ptr<StorageBackend> backend)
{
    if (!backend) {
        return {};
    }
    return [backend = std::move(backend)](const std::filesystem::path&, const StoreInfo&) {
        return Result<std::shared_ptr<StorageBackend>>(backend);
    };
}

} // namespace

struct Engine::Impl
{
    std::shared_ptr<ResourceOwner> resources;
    std::vector<std::shared_ptr<ResourceReservation>> store_resources;
    std::vector<std::shared_ptr<ResourceReservation>> attempt_resources;
    StoreInfo info;
    Limits limits;
    std::shared_ptr<StoreLease> lease;
    std::shared_ptr<DurableSqliteCatalogStore> catalog;
    std::shared_ptr<StorageBackend> backend;
    mutable std::mutex state_mutex;
    // Orders corruption reports against a publication decision. The writer
    // takes it immediately before its final health check, inside the catalog
    // guard, and holds it through the outer COMMIT or rollback. A reader that
    // detects corruption takes it before closing readiness, so its error
    // either prevents that publication or is surfaced after it completes.
    // Lock order: catalog mutex -> commit_gate -> state_mutex. The writer
    // never reports corruption while holding it.
    std::mutex commit_gate;
    std::timed_mutex writer;
    std::atomic<bool> closing{false};
    bool ready = false;
    bool recovering = false;
    bool closed = false;
    uint64_t health_generation = 0;
    int64_t pending_calls = 0;
    size_t owned_input_bytes = 0;
    std::vector<CollectionInfo> collections;
    std::vector<CollectionView> confirmed_views;
    TMetadataPages metadata_pages;
    // Roots that scans resolved from confirmed_views and are still reading,
    // and superseded roots waiting for those readers (all under state_mutex).
    std::map<std::string, std::size_t, std::less<>> root_readers;
    std::vector<BaseIdentity> retired_roots;
    std::optional<BoundOperation> active;
    std::optional<RecoveryContext> recovery;
    std::optional<AppendOutcome> pending_failure;
    std::optional<CreateOutcome> unresolved_create;

    void ReserveStoreMemory(const std::shared_ptr<ResourceOwner>& supplied)
    {
        resources = supplied ? supplied : ResourceOwner::ProcessDefault();
        store_resources.push_back(Take(resources->Reserve(ResourceClass::Metadata, CatalogMemoryBytes)));
        store_resources.push_back(Take(resources->Reserve(ResourceClass::Recovery, RecoveryMemoryBytes)));
    }

    void ReserveStoreDevice(const std::string& root)
    {
        // Catalog growth/journal and a bounded recovery workspace on this
        // exact device stay protected for the entire engine lifetime.
        store_resources.push_back(Take(resources->ReserveDevice(root,
            2 * static_cast<uint64_t>(limits.max_catalog_bytes) + WorkspaceDiskBytes + (8ULL << 20),
            32)));
    }

    void ReserveAttempt()
    {
        std::vector<std::shared_ptr<ResourceReservation>> reserved;
        reserved.push_back(Take(resources->Reserve(ResourceClass::Control, ControlMemoryBytes)));
        reserved.push_back(Take(resources->Reserve(ResourceClass::Completion, CompletionMemoryBytes)));
        reserved.push_back(Take(resources->Reserve(ResourceClass::Data, WriteDataMemoryBytes)));
        reserved.push_back(Take(resources->Reserve(ResourceClass::Metadata, MetadataWorkBytes)));
        reserved.push_back(Take(resources->ReserveDevice(lease->Root().string(), AttemptDiskBytes, 64)));
        attempt_resources = std::move(reserved);
    }

    struct TAttemptRelease
    {
        Impl& Owner;
        std::unique_lock<std::timed_mutex>& Writer;
        ~TAttemptRelease()
        {
            if (!Writer.owns_lock()) {
                return;
            }
            std::lock_guard lock(Owner.state_mutex);
            // Uncertain work transfers its protected capacity to recovery.
            if (!Owner.recovery && !Owner.unresolved_create) {
                Owner.attempt_resources.clear();
            }
        }
    };

    ~Impl()
    {
        closing = true;
        std::lock_guard writer_lock(writer);
        if (catalog) {
            try {
                catalog->Close();
            } catch (...) {
                // No background work exists. A subsequent Open must recover.
                ready = false;
            }
        }
    }

    void CheckReady() const
    {
        Require(!closing.load(), "CANCELLED", "engine is closing");
        Require(ready && !closed, "RECOVERY_REQUIRED", "authoritative reconciliation is required");
    }

    Status BackendWriteHealth() const
    {
        if (!backend) {
            return std::unexpected(ErrorDetail{"DEPENDENCY_UNAVAILABLE",
                "durable storage producer is not installed",
                RetryHint::OperatorAction});
        }
        const auto health = backend->GetHealth();
        if (health.write_failure_latched) {
            return std::unexpected(ErrorDetail{"IO_ERROR",
                "a durable storage write failed in this engine instance; shut down and reopen the store",
                RetryHint::OperatorAction});
        }
        if (!health.write_available) {
            return std::unexpected(ErrorDetail{"DEPENDENCY_UNAVAILABLE",
                "durable storage producer is unavailable",
                RetryHint::OperatorAction});
        }
        return {};
    }

    // Non-default settings need a backend that applies them with a codec of
    // this build; defaults never depend on the backend.
    void CheckSettingsSupported(const WriteSettings& settings) const
    {
        if (IsDefault(settings)) {
            return;
        }
        const auto capabilities = backend ? backend->GetCapabilities() : EngineCapabilities{};
        Require(capabilities.write_settings,
            "DEPENDENCY_UNAVAILABLE",
            "the storage backend does not support write settings");
        if (const auto supported = CheckWriteSettingsSupported(settings, capabilities.parquet_codecs); !supported) {
            throw Failure(supported.error());
        }
    }

    void CheckHealthy(uint64_t expected_generation) const
    {
        CheckReady();
        Require(health_generation == expected_generation,
            "RECOVERY_REQUIRED",
            "authoritative reconciliation is required");
        if (const auto health = BackendWriteHealth(); !health) {
            throw Failure(health.error());
        }
    }

    // The caller holds commit_gate. Final decision of a publication guard.
    Status PublicationHealth(uint64_t expected_generation) const
    {
        std::lock_guard lock(state_mutex);
        if (closing.load()) {
            return std::unexpected(ErrorDetail{"CANCELLED", "engine is closing", RetryHint::OperatorAction});
        }
        if (!ready || closed || health_generation != expected_generation) {
            return std::unexpected(ErrorDetail{"RECOVERY_REQUIRED",
                "authoritative reconciliation is required",
                RetryHint::SameIdAfterReconcile});
        }
        return BackendWriteHealth();
    }

    // The caller holds state_mutex. Recheck at every admission/writer boundary:
    // another writer may close readiness while this request is waiting.
    std::optional<RecoveryContext> BlockedAppend(const AppendRequest& request) const
    {
        const bool stopped = closed || closing.load();
        if (ready && !stopped) {
            return std::nullopt;
        }
        if (recovery && recovery->collection_id == request.collection_id &&
            recovery->operation_id == request.operation_id)
        {
            return recovery;
        }
        const auto view = std::ranges::find_if(confirmed_views, [&](const auto& item) {
            return item.info.collection_id == request.collection_id;
        });
        Require(view != confirmed_views.end(), "RECOVERY_REQUIRED", "collection lookup is blocked by recovery");
        const ErrorDetail error{stopped ? "CANCELLED" : "RECOVERY_REQUIRED",
            stopped ? "engine is closing or closed; reopen the store to resolve this operation"
                    : "resolve the store before consulting this operation; this call did not publish data",
            RetryHint::SameIdAfterReconcile};
        auto context =
            Context({request.collection_id, request.operation_id, {}, 1, view->base, NewUuid()}, error, false);
        context.lookup_blocked = true;
        return context;
    }

    // The caller holds state_mutex. A rejected retry cannot erase the first
    // uncertain create, including its exact identity, candidates and attempt.
    std::optional<CreateOutcome> BlockedCreate(const CreateRecoveryContext& context) const
    {
        auto pending = unresolved_create;
        if (pending) {
            const auto recovery = pending->recovery;
            if (!recovery) {
                throw Failure({"INTERNAL", "uncertain create lost its context", RetryHint::OperatorAction});
            }
            if (recovery->collection_id == context.collection_id || recovery->exact_name == context.exact_name) {
                Require(recovery->collection_id == context.collection_id && recovery->exact_name == context.exact_name,
                    "ALREADY_EXISTS",
                    "collection identity belongs to an unresolved creation");
                Require(recovery->schema_hash == context.schema_hash,
                    "SCHEMA_MISMATCH",
                    "schema differs from unresolved creation");
                return pending;
            }
        }
        const bool stopped = closed || closing.load();
        if (!ready || stopped) {
            return CreateOutcome{CommitDisposition::CommitUnknown,
                CommitPhase::Reconciling,
                std::nullopt,
                ErrorDetail{stopped ? "CANCELLED" : "RECOVERY_REQUIRED",
                    stopped ? "engine is closing or closed; reopen the store to resolve this creation"
                            : "collection lookup is blocked by recovery",
                    RetryHint::SameIdAfterReconcile},
                context};
        }
        return std::nullopt;
    }

    // The caller holds state_mutex. This context describes a lookup, never a
    // new publication of the requested operation.
    RecoveryContext
    AppendLookupContext(const AppendRequest& request, std::string_view payload_hash, const ErrorDetail& error) const
    {
        const auto view = std::ranges::find_if(confirmed_views, [&](const auto& item) {
            return item.info.collection_id == request.collection_id;
        });
        Require(view != confirmed_views.end(), "CORRUPTION", "confirmed collection view is missing");
        auto context =
            Context({request.collection_id, request.operation_id, std::string(payload_hash), 1, view->base, NewUuid()},
                error,
                false);
        context.lookup_blocked = true;
        return context;
    }

    AppendOutcome
    ResolveUnadmitted(const AppendRequest& request, std::string_view payload_hash, const ErrorDetail& reason)
    {
        RecoveryContext context;
        {
            std::lock_guard lock(state_mutex);
            const auto blocked = BlockedAppend(request);
            if (blocked) {
                return UnresolvedAppend(*blocked, payload_hash);
            }
            context = AppendLookupContext(request, payload_hash, reason);
        }
        std::optional<OperationRecord> previous;
        try {
            previous = catalog->TryFindOperation(request.collection_id, request.operation_id);
        } catch (const Failure& error) {
            CloseOnCorruption(error.detail);
            if (error.detail.code != "OPERATION_IN_PROGRESS") {
                context.first_error = error.detail;
            }
            return UnresolvedAppend(context, payload_hash);
        } catch (const std::exception& error) {
            context.first_error = ExceptionError(error);
            return UnresolvedAppend(context, payload_hash);
        }
        {
            std::lock_guard lock(state_mutex);
            const auto blocked = BlockedAppend(request);
            if (blocked) {
                return UnresolvedAppend(*blocked, payload_hash);
            }
        }
        if (previous) {
            Require(previous->binding.payload_hash == payload_hash,
                "OPERATION_CONFLICT",
                "operation ID is permanently bound to different content");
            if (previous->outcome) {
                return *previous->outcome;
            }
        }
        // No writer ownership: absence here cannot rule out another admitted
        // call binding the same ID. Keep readiness and the real recovery target.
        return UnresolvedAppend(context, payload_hash);
    }

    CreateOutcome UnknownCreate(CreateRecoveryContext context, CommitPhase phase, ErrorDetail error)
    {
        NGlamDB::NTelemetry::TLogScope::Emit(NGlamDB::NTelemetry::ELogEvent::RecoveryRequired,
            NGlamDB::NTelemetry::ELogOutcome::Unknown);
        if (backend) {
            backend->InvalidateCache();
        }
        error.retry_hint = RetryHint::SameIdAfterReconcile;
        CreateOutcome outcome{CommitDisposition::CommitUnknown,
            phase,
            std::nullopt,
            std::move(error),
            std::move(context)};
        std::lock_guard lock(state_mutex);
        ready = false;
        ++health_generation;
        unresolved_create = outcome;
        return outcome;
    }

    CollectionInfo CachedCollection(std::string_view id) const
    {
        const auto it = std::ranges::find_if(collections, [&](const auto& item) {
            return item.collection_id == id;
        });
        Require(it != collections.end(), "COLLECTION_NOT_FOUND", "collection does not exist");
        return *it;
    }

    template <typename TPredicate>
    CollectionInfo ReadCollection(const TPredicate& matches)
    {
        std::unique_lock stateLock(state_mutex);
        CheckReady();
        auto collectionIt = std::ranges::find_if(collections, matches);
        if (collectionIt != collections.end()) {
            return *collectionIt;
        }
        // A cache miss cannot prove absence while a writer may still publish.
        stateLock.unlock();
        std::unique_lock writerLock(writer, std::try_to_lock);
        if (!writerLock.owns_lock()) {
            throw Failure({"OPERATION_IN_PROGRESS",
                "Collection absence is unresolved while a writer is active.",
                RetryHint::SameIdAfterReconcile});
        }
        stateLock.lock();
        CheckReady();
        collectionIt = std::ranges::find_if(collections, matches);
        Require(collectionIt != collections.end(), "COLLECTION_NOT_FOUND", "Collection does not exist.");
        return *collectionIt;
    }

    template <typename T, typename TLoad>
    Result<T> ReadMetadataPage(const TMetadataPageRequest& request, std::string_view parameters, TLoad&& load)
    {
        try {
            uint64_t admissionGeneration = 0;
            {
                std::lock_guard lock(state_mutex);
                CheckReady();
                admissionGeneration = metadata_pages.GetGeneration();
            }
            auto page = metadata_pages.Read(
                request,
                parameters,
                [&]() -> Result<TMetadataPages::TListing> {
                    auto listing = load();
                    if (!listing) {
                        return std::unexpected(listing.error());
                    }
                    return TMetadataPages::TListing(std::move(*listing));
                },
                TMetadataPages::TClock::now(),
                admissionGeneration,
                resources);
            if (!page) {
                return std::unexpected(page.error());
            }
            std::lock_guard lock(state_mutex);
            CheckReady();
            Require(metadata_pages.GetGeneration() == admissionGeneration,
                "INVALID_CURSOR",
                "Metadata cursor admission was invalidated.");
            return std::get<T>(std::move(*page));
        } catch (const Failure& error) {
            return std::unexpected(error.detail);
        } catch (const std::exception& error) {
            return std::unexpected(ExceptionError(error));
        }
    }

    // Writer only. After a committed append replaced `superseded` in
    // confirmed_views: queue its root and remove every queued root that no
    // planning scan still holds. Removal stays on the writer because capacity
    // preflight walks the warehouse on the writer, and a file unlinked between
    // that walk's readdir and fstatat would fail it as corruption.
    void RetireRoot(const BaseIdentity& superseded)
    {
        {
            std::lock_guard lock(state_mutex);
            retired_roots.push_back(superseded);
        }
        RemoveReleasedRoots();
    }

    // Writer only (append or shutdown holds `writer`).
    void RemoveReleasedRoots()
    {
        std::vector<BaseIdentity> released;
        {
            std::lock_guard lock(state_mutex);
            const auto unleased = std::ranges::partition(retired_roots, [&](const auto& item) {
                return root_readers.contains(item.metadata_location);
            });
            released.assign(unleased.begin(), unleased.end());
            retired_roots.erase(unleased.begin(), unleased.end());
        }
        if (backend) {
            for (const auto& superseded : released) {
                backend->RetireRoot(superseded);
            }
        }
    }

    // A scan finished planning from `location`. Only the lease ends here: the
    // root, if superseded, is removed by the next writer that retires roots.
    void ReleaseRoot(const std::string& location)
    {
        std::lock_guard lock(state_mutex);
        const auto reader = root_readers.find(location);
        if (reader != root_readers.end() && --reader->second == 0) {
            root_readers.erase(reader);
        }
    }

#if GLAMDB_HAS_STORAGE
    template <class T, class TRead>
    Result<T> ReadView(std::string_view id, TRead&& read, bool useCache = true)
    {
        std::string root;
        bool leased = false;
        const auto release = [&] {
            if (std::exchange(leased, false)) {
                ReleaseRoot(root);
            }
        };
        try {
            Require(ValidUuid(id), "INVALID_ARGUMENT", "metadata requires a canonical collection UUID");
            TScanContext context;
            {
                std::lock_guard lock(state_mutex);
                CheckReady();
                context.Collection = CachedCollection(id);
                const auto view = std::ranges::find_if(confirmed_views, [&](const auto& item) {
                    return item.info.collection_id == id;
                });
                Require(view != confirmed_views.end(), "CORRUPTION", "confirmed collection view is missing");
                context.Base = view->base;
                root = context.Base.metadata_location;
                ++root_readers[root];
                leased = true;
            }
            Require(backend != nullptr, "DEPENDENCY_UNAVAILABLE", "storage reader is not installed");
            auto memory = Take(resources->Reserve(ResourceClass::Metadata, MetadataWorkBytes));
            const auto capabilities = backend->GetCapabilities();
            context.Resources = resources;
            context.FileIO = Take(backend->ReadOnlyFileIO());
            context.Cache = useCache ? backend->ReadMetadataCache() : nullptr;
            context.EnableFilePruning = capabilities.file_pruning;
            context.EnableRowGroupPruning = capabilities.row_group_pruning;
            auto result = read(context);
            if (const auto retained = RetainMetadata(resources, result); !retained) {
                throw Failure(retained.error());
            }
            release();
            return result;
        } catch (const Failure& error) {
            release();
            CloseOnCorruption(error.detail);
            return std::unexpected(error.detail);
        } catch (const std::bad_alloc& error) {
            release();
            return std::unexpected(
                ErrorDetail{"RESOURCE_EXHAUSTED", BoundedDiagnostic(error.what()), RetryHint::OperatorAction});
        } catch (const std::exception& error) {
            release();
            return std::unexpected(ExceptionError(error));
        }
    }
#endif

    void RefreshCache()
    {
        auto current = catalog->Collections();
        std::vector<CollectionView> views;
        views.reserve(current.size());
        for (const auto& collection : current) {
            CollectionView view{collection, catalog->Base(collection.collection_id)};
            if (backend) {
                view.scan_state = Take(backend->VerifiedScanState(view.base));
            }
            views.push_back(std::move(view));
        }
        std::lock_guard lock(state_mutex);
        collections = std::move(current);
        confirmed_views = std::move(views);
    }

    void CloseOnCorruption(const ErrorDetail& error)
    {
        using namespace NGlamDB::NTelemetry;
        if (error.code == "IO_ERROR" || error.code == "CORRUPTION" || error.code == "UNSUPPORTED_STORE_VERSION") {
            TLogScope::Emit(ELogEvent::StorageFailed);
        }
        if (error.code == "CORRUPTION" || error.code == "UNSUPPORTED_STORE_VERSION") {
            std::lock_guard gate(commit_gate);
            if (backend) {
                backend->InvalidateCache();
            }
            std::lock_guard lock(state_mutex);
            ready = false;
            ++health_generation;
            TLogScope::Emit(ELogEvent::RecoveryRequired);
        }
    }

    void VerifyStorage()
    {
        try {
            catalog->ValidateInventory();
            std::vector<CollectionView> views;
            for (const auto& collection : catalog->Collections()) {
                views.push_back({collection, catalog->Base(collection.collection_id)});
            }
            Require(views.empty() || backend != nullptr,
                "DEPENDENCY_UNAVAILABLE",
                "storage recovery validator is not installed");
            if (backend) {
                const auto verified = backend->VerifyRetained(info, views, catalog->Operations());
                if (!verified) {
                    throw Failure(verified.error());
                }
            }
        } catch (...) {
            if (backend) {
                backend->InvalidateCache();
            }
            std::lock_guard lock(state_mutex);
            ready = false;
            ++health_generation;
            throw;
        }
    }

    void Recover(bool reopen)
    {
        {
            std::lock_guard lock(state_mutex);
            recovering = true;
        }
        try {
            RecoverStore(reopen);
        } catch (const Failure& error) {
            using namespace NGlamDB::NTelemetry;
            if (error.detail.code == "IO_ERROR" || error.detail.code == "CORRUPTION" ||
                error.detail.code == "UNSUPPORTED_STORE_VERSION")
            {
                TLogScope::Emit(ELogEvent::StorageFailed);
            }
            TLogScope::Emit(ELogEvent::RecoveryRequired);
            std::lock_guard lock(state_mutex);
            recovering = false;
            throw;
        } catch (...) {
            NGlamDB::NTelemetry::TLogScope::Emit(NGlamDB::NTelemetry::ELogEvent::RecoveryRequired);
            std::lock_guard lock(state_mutex);
            recovering = false;
            throw;
        }
        std::lock_guard lock(state_mutex);
        recovering = false;
    }

    void RecoverStore(bool reopen)
    {
        uint64_t start_generation = 0;
        {
            std::lock_guard lock(state_mutex);
            ready = false;
            start_generation = ++health_generation;
            // Recovery replaces these states. Only active cursors must keep
            // the old generations and their metadata reservations pinned.
            for (auto& view : confirmed_views) {
                view.scan_state.reset();
            }
            metadata_pages.Clear();
        }
        if (reopen) {
            if (backend) {
                backend->InvalidateCache();
            }
            catalog->Reopen();
        }
        VerifyStorage();
        catalog->DurabilityFence();
        for (const auto& operation : catalog->Operations()) {
            if (operation.state != OperationState::Bound) {
                continue;
            }
            AppendOutcome failure = Rejected(operation.binding.operation_id,
                CommitPhase::Reconciling,
                {"INTERRUPTED", "registered append was interrupted before publication", RetryHint::NewOperationId});
            {
                std::lock_guard lock(state_mutex);
                if (pending_failure && pending_failure->operation_id == operation.binding.operation_id && recovery &&
                    recovery->collection_id == operation.binding.collection_id)
                {
                    failure = *pending_failure;
                } else if (recovery && recovery->collection_id == operation.binding.collection_id &&
                    recovery->operation_id == operation.binding.operation_id)
                {
                    failure.error = recovery->first_error;
                    failure.error->retry_hint = RetryHint::NewOperationId;
                }
            }
            catalog->RecordFailure(operation.binding, failure);
        }
        catalog->ValidateInventory();
        RefreshCache();
        std::lock_guard lock(state_mutex);
        if (health_generation != start_generation) {
            if (backend) {
                backend->InvalidateCache();
            }
            throw Failure({"CORRUPTION", "concurrent corruption detected during recovery", RetryHint::OperatorAction});
        }
        active.reset();
        recovery.reset();
        pending_failure.reset();
        attempt_resources.clear();
        unresolved_create.reset();
        ready = true;
    }

    AppendOutcome Unknown(RecoveryContext context,
        CommitPhase phase,
        ErrorDetail error,
        std::optional<AppendOutcome> failure = std::nullopt)
    {
        NGlamDB::NTelemetry::TLogScope::Emit(NGlamDB::NTelemetry::ELogEvent::RecoveryRequired,
            NGlamDB::NTelemetry::ELogOutcome::Unknown);
        if (backend) {
            backend->InvalidateCache();
        }
        error.retry_hint = RetryHint::SameIdAfterReconcile;
        std::lock_guard lock(state_mutex);
        ready = false;
        ++health_generation;
        active.reset();
        recovery = context;
        pending_failure = std::move(failure);
        return {context.operation_id,
            CommitDisposition::CommitUnknown,
            phase,
            std::nullopt,
            std::move(error),
            std::move(context)};
    }

    AppendOutcome
    FailBound(const BoundOperation& binding, CommitPhase phase, ErrorDetail error, bool publication_started = false)
    {
        if (error.code == "IO_ERROR") {
            NGlamDB::NTelemetry::TLogScope::Emit(NGlamDB::NTelemetry::ELogEvent::StorageFailed);
        }
        error.message = BoundedDiagnostic(error.message);
        // The bound ID is consumed either way. Capacity, corruption and an
        // unhealthy backend need operator action before a new ID can succeed.
        const bool operatorAction = error.code == "RESOURCE_EXHAUSTED" || error.code == "CORRUPTION" ||
            error.code == "UNSUPPORTED_STORE_VERSION" || !BackendWriteHealth();
        error.retry_hint = operatorAction ? RetryHint::OperatorAction : RetryHint::NewOperationId;
        auto failure = Rejected(binding.operation_id, phase, error);
        try {
            catalog->RecordFailure(binding, failure);
            if (error.code == "CORRUPTION" || error.code == "UNSUPPORTED_STORE_VERSION") {
                if (backend) {
                    backend->InvalidateCache();
                }
            }
            std::lock_guard lock(state_mutex);
            active.reset();
            if (error.code == "CORRUPTION" || error.code == "UNSUPPORTED_STORE_VERSION") {
                ready = false;
                ++health_generation;
            }
            return failure;
        } catch (const Failure& record_error) {
            return Unknown(Context(binding, error, publication_started),
                CommitPhase::RecordingFailure,
                record_error.detail,
                failure);
        } catch (const std::exception& record_error) {
            return Unknown(Context(binding, error, publication_started),
                CommitPhase::RecordingFailure,
                ExceptionError(record_error),
                failure);
        }
    }
};

namespace detail {

Result<std::shared_ptr<Engine>> EngineFactory::Create(const CreateStoreRequest& request,
    std::shared_ptr<StorageBackend> backend,
    std::string sqlite_vfs)
{
    return CreateWith(request, FixedBackend(std::move(backend)), std::move(sqlite_vfs));
}

Result<std::shared_ptr<Engine>>
EngineFactory::Open(const OpenStoreRequest& request, std::shared_ptr<StorageBackend> backend, std::string sqlite_vfs)
{
    return OpenWith(request, FixedBackend(std::move(backend)), std::move(sqlite_vfs));
}

Result<std::shared_ptr<Engine>>
EngineFactory::CreateWith(const CreateStoreRequest& request, const BackendFactory& backend, std::string sqlite_vfs)
{
    NGlamDB::NTelemetry::TReadOperationScope metrics(NGlamDB::NTelemetry::EReadOperation::CreateStore);
    try {
        auto impl = std::make_shared<Engine::Impl>();
        impl->info = {request.store_id, request.profile_id, 1};
        impl->limits = request.limits;
        ValidateConfig(impl->info, impl->limits);
        impl->ReserveStoreMemory(request.resources);
        impl->ReserveStoreDevice(request.absolute_root);
        impl->lease = StoreLease::Create(request.absolute_root, impl->info);
        impl->backend = MakeBackend(backend, impl->lease->Root(), impl->info);
        impl->catalog = DurableSqliteCatalogStore::Create(impl->lease->Root() / "catalog.sqlite",
            impl->info,
            impl->limits,
            std::move(sqlite_vfs));
        impl->Recover(false);
        impl->lease->MarkReady(impl->info);
        return metrics.Finish(Result<std::shared_ptr<Engine>>(std::shared_ptr<Engine>(new Engine(std::move(impl)))));
    } catch (const Failure& error) {
        metrics.Error(error.detail.code);
        return std::unexpected(error.detail);
    } catch (const std::exception& error) {
        const auto detail = ExceptionError(error);
        metrics.Error(detail.code);
        return std::unexpected(detail);
    }
}

Result<std::shared_ptr<Engine>>
EngineFactory::OpenWith(const OpenStoreRequest& request, const BackendFactory& backend, std::string sqlite_vfs)
{
    NGlamDB::NTelemetry::TReadOperationScope metrics(NGlamDB::NTelemetry::EReadOperation::OpenStore);
    try {
        auto impl = std::make_shared<Engine::Impl>();
        impl->info = {request.expected_store_id, request.profile_id, 1};
        impl->limits = request.limits;
        ValidateConfig(impl->info, impl->limits);
        impl->ReserveStoreMemory(request.resources);
        impl->lease = StoreLease::Open(request.absolute_root, impl->info);
        // Exclusive open reclaims interrupted workspaces before their retained
        // bytes/inodes can prevent admission of the next recovery workspace.
        impl->ReserveStoreDevice(impl->lease->Root().string());
        impl->backend = MakeBackend(backend, impl->lease->Root(), impl->info);
        impl->catalog = DurableSqliteCatalogStore::Open(impl->lease->Root() / "catalog.sqlite",
            impl->info,
            impl->limits,
            std::move(sqlite_vfs));
        impl->Recover(false);
        return metrics.Finish(Result<std::shared_ptr<Engine>>(std::shared_ptr<Engine>(new Engine(std::move(impl)))));
    } catch (const Failure& error) {
        metrics.Error(error.detail.code);
        return std::unexpected(error.detail);
    } catch (const std::exception& error) {
        const auto detail = ExceptionError(error);
        metrics.Error(detail.code);
        return std::unexpected(detail);
    }
}

} // namespace detail

Engine::Engine(std::shared_ptr<Impl> implementation)
    : impl_(std::move(implementation))
{ }
std::shared_ptr<ResourceOwner> Engine::GetResourceOwner() const
{
    return impl_->resources;
}

Engine::~Engine() = default;

Result<std::shared_ptr<Engine>> CreateStore(const CreateStoreRequest& request)
{
    return detail::EngineFactory::CreateWith(request, DefaultBackendFactory(request.resources));
}
Result<std::shared_ptr<Engine>> OpenExisting(const OpenStoreRequest& request)
{
    return detail::EngineFactory::OpenWith(request, DefaultBackendFactory(request.resources));
}
const StoreInfo& Engine::GetStoreInfo() const
{
    return impl_->info;
}

StoreHealth Engine::GetHealth() const
{
    const auto impl = impl_;
    std::lock_guard lock(impl->state_mutex);
    const auto backend =
        impl->backend ? impl->backend->GetHealth() : detail::StorageBackend::Health{false, false, false};
    StoreHealth health;
    health.storage_failure_latched = backend.write_failure_latched;
    if (impl->closed) {
        health.state = StoreState::Closed;
    } else if (impl->closing.load()) {
        health.state = StoreState::Stopping;
    } else if (impl->recovering) {
        health.state = StoreState::Recovering;
    } else if (!impl->ready) {
        health.state = StoreState::RecoveryRequired;
    } else {
        health.read_available = backend.read_available;
        health.write_ready = backend.write_available && !backend.write_failure_latched;
        health.state = backend.write_failure_latched ? StoreState::StorageFailed
            : health.write_ready                     ? StoreState::Ready
                                                     : StoreState::Unavailable;
    }
    return health;
}

Result<CollectionInfo> Engine::GetCollection(std::string_view id) const
{
    try {
        const auto impl = impl_;
        return impl->ReadCollection([&](const auto& collection) {
            return collection.collection_id == id;
        });
    } catch (const Failure& error) {
        return std::unexpected(error.detail);
    }
}

Result<CollectionInfo> Engine::ResolveCollection(std::string_view name) const
{
    try {
        const auto impl = impl_;
        return impl->ReadCollection([&](const auto& collection) {
            return collection.name == name;
        });
    } catch (const Failure& error) {
        return std::unexpected(error.detail);
    }
}

Result<std::vector<CollectionInfo>> Engine::ListCollections() const
{
    try {
        const auto impl = impl_;
        std::lock_guard lock(impl->state_mutex);
        impl->CheckReady();
        return impl->collections;
    } catch (const Failure& error) {
        return std::unexpected(error.detail);
    }
}

Result<TCollectionPage> Engine::ListCollectionsPage(const TMetadataPageRequest& request) const
{
    return impl_->ReadMetadataPage<TCollectionPage>(request, "collections-v1", [&]() -> Result<TCollectionPage> {
        auto collections = ListCollections();
        if (!collections) {
            return std::unexpected(collections.error());
        }
        const auto highWater = collections->empty() ? 0 : collections->back().creation_sequence;
        return TCollectionPage{.CreationHighWaterMark = highWater, .Collections = std::move(*collections)};
    });
}

Result<SnapshotHistory> Engine::ListSnapshotsPage(std::string_view collectionID,
    const TMetadataPageRequest& request) const
{
    try {
        Require(ValidUuid(collectionID), "INVALID_ARGUMENT", "Collection ID must be a canonical UUID.");
        return impl_->ReadMetadataPage<SnapshotHistory>(request,
            std::string("snapshots-v1:") + std::string(collectionID),
            [&] {
                return ListSnapshots(collectionID);
            });
    } catch (const Failure& error) {
        return std::unexpected(error.detail);
    } catch (const std::exception& error) {
        return std::unexpected(ExceptionError(error));
    }
}

Result<TFileListing> Engine::ListFilesPage(std::string_view collectionID,
    const TMetadataPageRequest& request,
    SnapshotSelector selector) const
{
    try {
        Require(ValidUuid(collectionID), "INVALID_ARGUMENT", "Collection ID must be a canonical UUID.");
        Encoder parameters;
        parameters.Text("files-v1");
        parameters.Text(collectionID);
        parameters.Number(static_cast<uint8_t>(selector.kind));
        parameters.Number(selector.snapshot_id);
        return impl_->ReadMetadataPage<TFileListing>(request, parameters.bytes, [&] {
            return ListFiles(collectionID, selector);
        });
    } catch (const Failure& error) {
        return std::unexpected(error.detail);
    } catch (const std::exception& error) {
        return std::unexpected(ExceptionError(error));
    }
}

Result<std::shared_ptr<const arrow::Schema>> Engine::Schema(std::string_view id) const
{
    const auto collection = GetCollection(id);
    if (!collection) {
        return std::unexpected(collection.error());
    }
    return MakeArrowSchema(collection->schema);
}

Result<SchemaSpec> Engine::SchemaForAppend(std::string_view id) const
{
    try {
        const auto impl = impl_;
        std::lock_guard lock(impl->state_mutex);
        if (impl->closed || impl->closing.load()) {
            throw Failure({"CANCELLED", "engine is closing or closed", RetryHint::SameIdAfterReconcile});
        }
        if (!impl->ready && std::ranges::none_of(impl->confirmed_views, [&](const auto& view) {
                return view.info.collection_id == id;
            }))
        {
            throw Failure(
                {"RECOVERY_REQUIRED", "collection lookup is blocked by recovery", RetryHint::SameIdAfterReconcile});
        }
        return impl->CachedCollection(id).schema;
    } catch (const Failure& error) {
        return std::unexpected(error.detail);
    }
}

CreateOutcome Engine::CreateCollection(const CreateCollectionRequest& request)
{
    NGlamDB::NTelemetry::TOperationScope<CreateOutcome> metrics(NGlamDB::NTelemetry::EOperation::CreateCollection);
    const auto impl = impl_;
    std::unique_lock writer_lock(impl->writer, std::defer_lock);
    Impl::TAttemptRelease release{*impl, writer_lock};
    std::optional<CreateRecoveryContext> context;
    bool publication_started = false;
    bool committed = false;
    try {
        Require(ValidUuid(request.collection_id) && !request.exact_name.empty() && request.exact_name.size() <= 128 &&
                ValidUtf8(request.exact_name) && request.exact_name.find('\0') == std::string::npos,
            "INVALID_ARGUMENT",
            "invalid collection ID or exact name");
        const auto schema_hash = Take(SchemaFingerprint(request.schema));
        const auto settings = Take(CanonicalWriteSettings(request.schema, request.write_settings));
        context = CreateRecoveryContext{impl->info.store_id,
            request.collection_id,
            request.exact_name,
            schema_hash,
            NewUuid(),
            std::nullopt,
            std::nullopt,
            false};
        {
            std::lock_guard state_lock(impl->state_mutex);
            const auto blocked = impl->BlockedCreate(*context);
            if (blocked) {
                return metrics.Finish(*blocked);
            }
        }
        if (!writer_lock.try_lock_for(impl->limits.writer_wait)) {
            std::lock_guard state_lock(impl->state_mutex);
            const auto blocked = impl->BlockedCreate(*context);
            if (blocked) {
                return metrics.Finish(*blocked);
            }
            return metrics.Finish({CommitDisposition::CommitUnknown,
                CommitPhase::Reconciling,
                std::nullopt,
                ErrorDetail{"RESOURCE_EXHAUSTED",
                    "writer busy; original creation outcome was not resolved",
                    RetryHint::SameIdAfterReconcile},
                context});
        }
        uint64_t expected_generation = 0;
        {
            std::lock_guard state_lock(impl->state_mutex);
            const auto blocked = impl->BlockedCreate(*context);
            if (blocked) {
                return metrics.Finish(*blocked);
            }
            expected_generation = impl->health_generation;
        }
        const auto existing = impl->catalog->Collections();
        for (const auto& collection : existing) {
            if (collection.collection_id == request.collection_id || collection.name == request.exact_name) {
                Require(collection.collection_id == request.collection_id && collection.name == request.exact_name,
                    "ALREADY_EXISTS",
                    "collection ID or name belongs to another definition");
                Require(collection.schema == request.schema,
                    "SCHEMA_MISMATCH",
                    "fixed schema differs from original creation");
                Require(impl->catalog->CreatedWriteSettings(collection.collection_id) == settings,
                    "WRITE_SETTINGS_MISMATCH",
                    "write settings differ from original creation");
                context = CreateRecoveryContext{impl->info.store_id,
                    collection.collection_id,
                    collection.name,
                    schema_hash,
                    NewUuid(),
                    collection.table_uuid,
                    std::nullopt,
                    true};
                committed = true;
                impl->VerifyStorage();
                return metrics.Finish(
                    {CommitDisposition::Committed, CommitPhase::Complete, collection, std::nullopt, std::nullopt});
            }
        }
        Require(existing.size() < static_cast<size_t>(impl->limits.max_collections),
            "RESOURCE_EXHAUSTED",
            "permanent catalog capacity is full");
        Require(impl->backend != nullptr,
            "DEPENDENCY_UNAVAILABLE",
            "durable storage producer and closure guard are not installed");
        if (const auto health = impl->BackendWriteHealth(); !health) {
            throw Failure(health.error());
        }
        impl->CheckSettingsSupported(settings);
        const auto attempt = NewUuid();
        context = CreateRecoveryContext{impl->info.store_id,
            request.collection_id,
            request.exact_name,
            schema_hash,
            attempt,
            std::nullopt,
            std::nullopt,
            false};
        impl->ReserveAttempt();
        auto canonical = request;
        canonical.write_settings = settings;
        metrics.EnterPhase(CommitPhase::Data);
        auto staged = Take(impl->backend->PrepareCreate(impl->info, canonical, attempt));
        Require(staged != nullptr, "IO_ERROR", "producer returned no staged collection");
        auto info = staged->Info();
        Require(info.collection_id == request.collection_id && info.name == request.exact_name &&
                info.schema == request.schema && info.write_settings == settings && info.write_settings_version == 0,
            "IO_ERROR",
            "producer changed collection identity, schema or write settings");
        context->candidate_table_uuid = info.table_uuid;
        {
            std::lock_guard state_lock(impl->state_mutex);
            impl->CheckHealthy(expected_generation);
        }
        publication_started = true;
        context->publication_started = true;
        // Held from the final health check through the outer COMMIT or
        // rollback; see Impl::commit_gate.
        std::unique_lock gate(impl->commit_gate, std::defer_lock);
        metrics.EnterPhase(CommitPhase::CatalogTransaction);
        auto created = impl->catalog->PublishCreate(
            info,
            [&](std::string_view location) -> Status {
                context->candidate_metadata_location = std::string(location);
                auto verified = staged->VerifyClosure(location);
                if (!verified) {
                    return verified;
                }
                if (!gate.owns_lock()) {
                    gate.lock();
                }
                return impl->PublicationHealth(expected_generation);
            },
            [&] {
                return staged->Publish(*impl->catalog);
            });
        committed = true;
        if (gate.owns_lock()) {
            if (context->candidate_metadata_location) {
                impl->backend->PublicationFinished(created.collection_id,
                    *context->candidate_metadata_location,
                    CommitDisposition::Committed);
            }
        }
        // Pin the confirmed storage generation before corruption can revoke
        // it after COMMIT. Revocation must not change a known committed result.
        impl->RefreshCache();
        if (gate.owns_lock()) {
            gate.unlock();
        }
        return metrics.Finish(
            {CommitDisposition::Committed, CommitPhase::Complete, created, std::nullopt, std::nullopt});
    } catch (const Failure& error) {
        if (publication_started && context && context->candidate_metadata_location) {
            impl->backend->PublicationFinished(context->collection_id,
                *context->candidate_metadata_location,
                error.uncertain || committed ? CommitDisposition::CommitUnknown
                                             : CommitDisposition::DefinitelyNotCommitted);
        }
        impl->CloseOnCorruption(error.detail);
        if ((error.uncertain || committed) && context) {
            return metrics.Finish(impl->UnknownCreate(*context,
                committed ? CommitPhase::Reconciling : error.phase.value_or(CommitPhase::CatalogTransaction),
                error.detail));
        }
        return metrics.Finish({CommitDisposition::DefinitelyNotCommitted,
            publication_started ? CommitPhase::CatalogTransaction : CommitPhase::Validation,
            std::nullopt,
            error.detail,
            std::nullopt});
    } catch (const std::exception& error) {
        if (publication_started && context && context->candidate_metadata_location) {
            impl->backend->PublicationFinished(context->collection_id,
                *context->candidate_metadata_location,
                CommitDisposition::CommitUnknown);
        }
        if ((publication_started || committed) && context) {
            return metrics.Finish(impl->UnknownCreate(*context, CommitPhase::Reconciling, ExceptionError(error)));
        }
        return metrics.Finish({CommitDisposition::DefinitelyNotCommitted,
            CommitPhase::Validation,
            std::nullopt,
            ExceptionError(error),
            std::nullopt});
    }
}

Result<CollectionInfo> Engine::UpdateWriteSettings(const UpdateWriteSettingsRequest& request)
{
    const auto impl = impl_;
    // Appends hold the writer from binding through publication, so a bound
    // append keeps the settings it read and the next one sees the new ones.
    std::unique_lock writer_lock(impl->writer, std::defer_lock);
    try {
        Require(ValidUuid(request.collection_id), "INVALID_ARGUMENT", "collection ID must be a canonical UUID");
        // The next version must be representable: expected + 1 never overflows.
        if (request.expected_version < 0 || request.expected_version == std::numeric_limits<int64_t>::max()) {
            throw Failure(
                {"INVALID_ARGUMENT", "expected write settings version must be in 0..2^63-2", RetryHint::FixInput});
        }
        // A cache miss cannot prove absence while another writer may publish.
        const auto collection = impl->ReadCollection([&](const auto& item) {
            return item.collection_id == request.collection_id;
        });
        const auto settings = Take(CanonicalWriteSettings(collection.schema, request.write_settings));
        if (!writer_lock.try_lock_for(impl->limits.writer_wait)) {
            throw Failure({"RESOURCE_EXHAUSTED",
                "writer busy; write settings were not changed",
                RetryHint::SameIdAfterReconcile});
        }
        uint64_t expected_generation = 0;
        {
            std::lock_guard state_lock(impl->state_mutex);
            impl->CheckReady();
            expected_generation = impl->health_generation;
        }
        // Resetting to defaults is always possible; anything else must be
        // applicable by this build before it can bind later appends.
        impl->CheckSettingsSupported(settings);
        {
            std::lock_guard state_lock(impl->state_mutex);
            impl->CheckHealthy(expected_generation);
        }
        CollectionInfo updated;
        try {
            updated = impl->catalog->UpdateWriteSettings(request.collection_id, request.expected_version, settings);
        } catch (const Failure& error) {
            if (!error.uncertain) {
                throw;
            }
            // COMMIT may or may not have persisted. After reconciliation the
            // same request either applies or is recognized as applied.
            if (impl->backend) {
                impl->backend->InvalidateCache();
            }
            {
                std::lock_guard state_lock(impl->state_mutex);
                impl->ready = false;
                ++impl->health_generation;
            }
            return std::unexpected(ErrorDetail{"RECOVERY_REQUIRED",
                "write settings outcome is unknown; reconcile, then repeat the same request",
                RetryHint::SameIdAfterReconcile});
        }
        impl->RefreshCache();
        return updated;
    } catch (const Failure& error) {
        impl->CloseOnCorruption(error.detail);
        return std::unexpected(error.detail);
    } catch (const std::exception& error) {
        return std::unexpected(ExceptionError(error));
    }
}

AppendOutcome Engine::AppendDurable(const AppendRequest& request)
{
    NGlamDB::NTelemetry::TOperationScope<AppendOutcome> metrics(NGlamDB::NTelemetry::EOperation::Append);
    const auto impl = impl_;
    const auto deadline = std::chrono::steady_clock::now() + impl->limits.append_deadline;
    // Retain the writer across catch handlers: recording a failure and setting
    // an unknown barrier are part of the serialized attempt, too.
    std::unique_lock writer_lock(impl->writer, std::defer_lock);
    Impl::TAttemptRelease release{*impl, writer_lock};
    struct Admission
    {
        std::shared_ptr<Impl> owner;
        size_t bytes = 0;
        std::shared_ptr<ResourceReservation> shared_input;
        ~Admission()
        {
            if (owner) {
                std::lock_guard lock(owner->state_mutex);
                --owner->pending_calls;
                owner->owned_input_bytes -= bytes;
                NGlamDB::NTelemetry::AdjustAdmission(-1, -static_cast<int64_t>(bytes));
            }
        }
    } admission;
    std::optional<BoundOperation> binding;
    CommitPhase phase = CommitPhase::Validation;
    bool publication_started = false;
    RecoveryContext context;
    try {
        Require(ValidUuid(request.collection_id) && ValidUuid(request.operation_id) &&
                request.expected_snapshot.kind <= SnapshotPreconditionKind::Equals,
            "INVALID_ARGUMENT",
            "append requires canonical UUIDs and a valid precondition");
        Require(impl->backend != nullptr, "DEPENDENCY_UNAVAILABLE", "durable storage producer is not installed");
        CollectionInfo collection;
        std::optional<RecoveryContext> unresolved;
        {
            std::lock_guard lock(impl->state_mutex);
            unresolved = impl->BlockedAppend(request);
            collection = impl->CachedCollection(request.collection_id);
        }
        const auto payload_hash = Take(FingerprintBatches(collection.schema, request.collection_id, request.batches));
        if (unresolved) {
            return metrics.Finish(UnresolvedAppend(*unresolved, payload_hash));
        }
        if (writer_lock.try_lock()) {
            // The writer holder is an admitted call as well. Count it before
            // its outcome lookup: a definite refusal after the lookup could
            // contradict a queued call with the same ID that later publishes.
            bool full = false;
            {
                std::lock_guard lock(impl->state_mutex);
                full = impl->pending_calls >= impl->limits.max_pending_calls;
                if (!full) {
                    ++impl->pending_calls;
                    NGlamDB::NTelemetry::AdjustAdmission(1, 0);
                    admission.owner = impl;
                }
            }
            if (full) {
                writer_lock.unlock();
                return metrics.Finish(impl->ResolveUnadmitted(request,
                    payload_hash,
                    {"RESOURCE_EXHAUSTED",
                        "pending budget is full; original operation outcome was not resolved",
                        RetryHint::SameIdAfterReconcile}));
            }
        } else {
            bool full = false;
            {
                std::lock_guard lock(impl->state_mutex);
                unresolved = impl->BlockedAppend(request);
                full = impl->pending_calls >= impl->limits.max_pending_calls;
                if (!unresolved && !full) {
                    ++impl->pending_calls;
                    NGlamDB::NTelemetry::AdjustAdmission(1, 0);
                    admission.owner = impl;
                }
            }
            if (unresolved) {
                return metrics.Finish(UnresolvedAppend(*unresolved, payload_hash));
            }
            if (full) {
                return metrics.Finish(impl->ResolveUnadmitted(request,
                    payload_hash,
                    {"RESOURCE_EXHAUSTED",
                        "pending budget is full; original operation outcome was not resolved",
                        RetryHint::SameIdAfterReconcile}));
            }
            if (!writer_lock.try_lock_until(
                    std::min(deadline, std::chrono::steady_clock::now() + impl->limits.writer_wait)))
            {
                return metrics.Finish(impl->ResolveUnadmitted(request,
                    payload_hash,
                    {"RESOURCE_EXHAUSTED",
                        "writer wait expired; original operation outcome was not resolved",
                        RetryHint::SameIdAfterReconcile}));
            }
        }
        uint64_t expected_generation = 0;
        {
            std::lock_guard state_lock(impl->state_mutex);
            unresolved = impl->BlockedAppend(request);
            collection = impl->CachedCollection(request.collection_id);
            if (!unresolved) {
                expected_generation = impl->health_generation;
                context = impl->AppendLookupContext(request,
                    payload_hash,
                    {"RECOVERY_REQUIRED", "outcome lookup is unresolved", RetryHint::SameIdAfterReconcile});
            }
        }
        if (unresolved) {
            return metrics.Finish(UnresolvedAppend(*unresolved, payload_hash));
        }
        std::optional<OperationRecord> previous;
        try {
            previous = impl->catalog->FindOperation(request.collection_id, request.operation_id);
        } catch (const Failure& error) {
            context.first_error = error.detail;
            return metrics.Finish(impl->Unknown(context, CommitPhase::Reconciling, error.detail));
        } catch (const std::exception& error) {
            context.first_error = ExceptionError(error);
            return metrics.Finish(impl->Unknown(context, CommitPhase::Reconciling, context.first_error));
        }
        if (previous) {
            Require(previous->binding.payload_hash == payload_hash,
                "OPERATION_CONFLICT",
                "operation ID is permanently bound to different content");
            if (previous->outcome) {
                try {
                    // Startup/recovery verified the retained history; confirmed
                    // publications preserve it. A terminal lookup is not a scrub.
                    std::lock_guard state_lock(impl->state_mutex);
                    impl->CheckReady();
                    Require(impl->health_generation == expected_generation,
                        "RECOVERY_REQUIRED",
                        "authoritative reconciliation is required");
                } catch (const Failure& error) {
                    return metrics.Finish(impl->Unknown(Context(previous->binding, error.detail, true),
                        CommitPhase::Reconciling,
                        error.detail));
                } catch (const std::exception& error) {
                    const auto detail = ExceptionError(error);
                    return metrics.Finish(
                        impl->Unknown(Context(previous->binding, detail, true), CommitPhase::Reconciling, detail));
                }
                return metrics.Finish(*previous->outcome);
            }
            const ErrorDetail error{"RECOVERY_REQUIRED",
                "pending binding requires reconciliation",
                RetryHint::SameIdAfterReconcile};
            return metrics.Finish(
                impl->Unknown(Context(previous->binding, error, false), CommitPhase::Reconciling, error));
        }
        // Only a new operation needs owned payload memory. A retry has already
        // returned its original terminal receipt, even under a lower input cap.
        const auto reservation = Take(InputReservation(collection.schema, request.batches));
        {
            std::lock_guard lock(impl->state_mutex);
            impl->CheckHealthy(expected_generation);
            Require(reservation <= static_cast<size_t>(impl->limits.max_owned_input_bytes) - impl->owned_input_bytes,
                "RESOURCE_EXHAUSTED",
                "owned input budget is full");
            impl->owned_input_bytes += reservation;
            NGlamDB::NTelemetry::AdjustAdmission(0, static_cast<int64_t>(reservation));
            admission.bytes = reservation;
        }
        admission.shared_input = Take(impl->resources->Reserve(ResourceClass::Data, reservation));
        impl->ReserveAttempt();
        auto input = Take(NormalizeBatches(collection.schema, request.collection_id, request.batches));
        Require(input.payload_hash == payload_hash, "INVALID_ARGUMENT", "Arrow input changed during append");
        const auto base = impl->catalog->Base(request.collection_id);
        // The store may have been opened by a build without a stored codec.
        impl->CheckSettingsSupported(collection.write_settings);
        if (impl->backend) {
            const auto admissible = impl->backend->CheckAppendAdmissible(impl->info, collection, base, input);
            if (!admissible) {
                throw Failure(admissible.error());
            }
        }
        {
            std::lock_guard state_lock(impl->state_mutex);
            impl->CheckHealthy(expected_generation);
        }
        binding = BoundOperation{request.collection_id, request.operation_id, input.payload_hash, 1, base, NewUuid()};
        context = Context(*binding,
            {"RECOVERY_REQUIRED", "append outcome is unresolved", RetryHint::SameIdAfterReconcile},
            false);
        phase = CommitPhase::Binding;
        metrics.EnterPhase(CommitPhase::Binding);
        try {
            impl->catalog->BindOperation(*binding);
        } catch (const Failure& error) {
            if (error.uncertain) {
                context.first_error = error.detail;
                return metrics.Finish(impl->Unknown(context, phase, error.detail));
            }
            binding.reset();
            throw;
        }
        {
            std::lock_guard state_lock(impl->state_mutex);
            impl->active = binding;
        }
        if (request.expected_snapshot.kind == SnapshotPreconditionKind::NoSnapshot) {
            Require(!binding->base.current_snapshot_id, "PRECONDITION_FAILED", "collection already has a snapshot");
        } else if (request.expected_snapshot.kind == SnapshotPreconditionKind::Equals) {
            Require(binding->base.current_snapshot_id == request.expected_snapshot.snapshot_id,
                "PRECONDITION_FAILED",
                "snapshot precondition does not match");
        }
        {
            std::lock_guard state_lock(impl->state_mutex);
            impl->CheckHealthy(expected_generation);
        }
        Require(std::chrono::steady_clock::now() < deadline,
            "DEADLINE_EXCEEDED",
            "append deadline expired before execution");
        phase = CommitPhase::Data;
        metrics.EnterPhase(CommitPhase::Data);
        auto staged = Take(impl->backend->PrepareAppend(impl->info, *binding, collection, input, deadline));
        Require(staged != nullptr, "IO_ERROR", "producer returned no staged append");
        const auto receipt = staged->Receipt();
        const AppendOutcome committed{binding->operation_id,
            CommitDisposition::Committed,
            CommitPhase::Complete,
            receipt,
            std::nullopt,
            std::nullopt};
        Require(ValidateOutcome(committed).has_value() &&
                receipt.rows_appended == static_cast<int64_t>(input.rows.size()),
            "IO_ERROR",
            "producer returned an invalid receipt or row count");
        {
            std::lock_guard state_lock(impl->state_mutex);
            impl->CheckHealthy(expected_generation);
        }
        Require(std::chrono::steady_clock::now() < deadline,
            "DEADLINE_EXCEEDED",
            "append deadline expired before publication");
        phase = CommitPhase::CatalogTransaction;
        metrics.EnterPhase(CommitPhase::CatalogTransaction);
        publication_started = true;
        context.publication_started = true;
        context.candidate_snapshot_id = receipt.snapshot_id;
        // Held from the final health check through the outer COMMIT or
        // rollback; see Impl::commit_gate.
        std::unique_lock gate(impl->commit_gate, std::defer_lock);
        impl->catalog->PublishAppend(
            *binding,
            receipt,
            [&](std::string_view location) -> Status {
                context.candidate_metadata_location = std::string(location);
                auto verified = staged->VerifyClosure(location);
                if (!verified) {
                    return verified;
                }
                if (!gate.owns_lock()) {
                    gate.lock();
                }
                return impl->PublicationHealth(expected_generation);
            },
            [&] {
                return staged->Publish(*impl->catalog);
            });
        // Count a confirmed new publication once. Receipt replay paths never
        // reach this boundary; a lost/unknown COMMIT response is not counted.
        NGlamDB::NTelemetry::RecordPublished(static_cast<std::uint64_t>(receipt.rows_appended),
            static_cast<std::uint64_t>(receipt.compressed_data_bytes_added));
        phase = CommitPhase::Complete;
        if (gate.owns_lock()) {
            if (context.candidate_metadata_location) {
                impl->backend->PublicationFinished(binding->collection_id,
                    *context.candidate_metadata_location,
                    CommitDisposition::Committed);
            }
        }
        // Publish the matching index/view while the commit gate still protects
        // the successful handoff from concurrent corruption invalidation.
        impl->RefreshCache();
        if (gate.owns_lock()) {
            gate.unlock();
        }
        metrics.EnterPhase(CommitPhase::Complete);
        {
            std::lock_guard state_lock(impl->state_mutex);
            impl->active.reset();
        }
        // New scans now resolve the new root; the exact prepared base is superseded.
        if (context.candidate_metadata_location &&
            *context.candidate_metadata_location != binding->base.metadata_location)
        {
            impl->RetireRoot(binding->base);
        }
        return metrics.Finish(committed);
    } catch (const Failure& error) {
        if (publication_started && binding && context.candidate_metadata_location) {
            impl->backend->PublicationFinished(binding->collection_id,
                *context.candidate_metadata_location,
                error.uncertain || phase == CommitPhase::Complete ? CommitDisposition::CommitUnknown
                                                                  : CommitDisposition::DefinitelyNotCommitted);
        }
        impl->CloseOnCorruption(error.detail);
        if (!binding) {
            return metrics.Finish(Rejected(request.operation_id, phase, error.detail));
        }
        context.first_error = error.detail;
        if (error.uncertain || phase == CommitPhase::Complete) {
            return metrics.Finish(impl->Unknown(context, error.phase.value_or(CommitPhase::Reconciling), error.detail));
        }
        return metrics.Finish(impl->FailBound(*binding, phase, error.detail, publication_started));
    } catch (const std::exception& error) {
        const auto detail = ExceptionError(error);
        if (publication_started && binding && context.candidate_metadata_location) {
            impl->backend->PublicationFinished(binding->collection_id,
                *context.candidate_metadata_location,
                CommitDisposition::CommitUnknown);
        }
        if (!binding) {
            return metrics.Finish(Rejected(request.operation_id, phase, detail));
        }
        if (publication_started) {
            context.first_error = detail;
            return metrics.Finish(impl->Unknown(context, CommitPhase::Reconciling, detail));
        }
        return metrics.Finish(impl->FailBound(*binding, phase, detail));
    }
}

Result<std::optional<AppendOutcome>> Engine::FindAppendOutcome(std::string_view collection_id,
    std::string_view operation_id) const
{
    const auto impl = impl_;
    try {
        Require(ValidUuid(collection_id) && ValidUuid(operation_id),
            "INVALID_ARGUMENT",
            "lookup requires canonical UUIDs");
        {
            std::lock_guard lock(impl->state_mutex);
            const auto recovery = impl->recovery;
            if (!impl->ready && recovery && recovery->collection_id == collection_id &&
                recovery->operation_id == operation_id)
            {
                auto error = recovery->first_error;
                error.retry_hint = RetryHint::SameIdAfterReconcile;
                return std::optional(AppendOutcome{std::string(operation_id),
                    CommitDisposition::CommitUnknown,
                    CommitPhase::Reconciling,
                    std::nullopt,
                    error,
                    recovery});
            }
            impl->CheckReady();
            (void)impl->CachedCollection(collection_id);
            const auto active = impl->active;
            if (active && active->collection_id == collection_id && active->operation_id == operation_id) {
                throw Failure(
                    {"OPERATION_IN_PROGRESS", "registered append is still executing", RetryHint::SameIdAfterReconcile});
            }
        }
        // Publication owns the catalog lock through durable file work. Outcome
        // lookup must leave the service's control workers available meanwhile.
        const auto record = impl->catalog->TryFindOperation(collection_id, operation_id);
        {
            std::lock_guard lock(impl->state_mutex);
            impl->CheckReady();
        }
        if (!record) {
            return std::optional<AppendOutcome>{};
        }
        if (!record->outcome) {
            throw Failure(
                {"OPERATION_IN_PROGRESS", "registered append is still executing", RetryHint::SameIdAfterReconcile});
        }
        return record->outcome;
    } catch (const Failure& error) {
        impl->CloseOnCorruption(error.detail);
        return std::unexpected(error.detail);
    }
}

EngineCapabilities Engine::GetCapabilities() const
{
    return impl_->backend ? impl_->backend->GetCapabilities() : EngineCapabilities{};
}

Result<SnapshotHistory> Engine::ListSnapshots(std::string_view id) const
{
    const auto impl = impl_;
#if GLAMDB_HAS_STORAGE
    return impl->ReadView<SnapshotHistory>(
        id,
        [&](const TScanContext& context) {
            std::vector<AppendResult> receipts;
            for (const auto& record : impl->catalog->Operations()) {
                if (record.state == OperationState::Committed && record.outcome && record.outcome->result) {
                    receipts.push_back(*record.outcome->result);
                }
            }
            return ReadHistory(context, receipts);
        },
        false);
#else
    (void)id;
    return std::unexpected(
        ErrorDetail{"DEPENDENCY_UNAVAILABLE", "storage metadata is not built", RetryHint::OperatorAction});
#endif
}

Result<CollectionStatistics> Engine::GetStatistics(std::string_view id, SnapshotSelector selector) const
{
    const auto impl = impl_;
#if GLAMDB_HAS_STORAGE
    return impl->ReadView<CollectionStatistics>(id, [&](const TScanContext& context) {
        return ReadStatistics(context, selector);
    });
#else
    (void)id;
    (void)selector;
    return std::unexpected(
        ErrorDetail{"DEPENDENCY_UNAVAILABLE", "storage metadata is not built", RetryHint::OperatorAction});
#endif
}

Result<TFileListing> Engine::ListFiles(std::string_view collectionID, SnapshotSelector selector) const
{
#if GLAMDB_HAS_STORAGE
    return impl_->ReadView<TFileListing>(collectionID, [&](const TScanContext& context) {
        return ReadFiles(context, selector);
    });
#else
    (void)collectionID;
    (void)selector;
    return std::unexpected(
        ErrorDetail{"DEPENDENCY_UNAVAILABLE", "storage metadata is not built", RetryHint::OperatorAction});
#endif
}

// The IDs have distinct validated UUID and SHA-256 formats.
// NOLINTBEGIN(bugprone-easily-swappable-parameters)
Result<TFileDetails>
Engine::GetFileDetails(std::string_view collectionID, std::string_view fileID, SnapshotSelector selector) const
{
#if GLAMDB_HAS_STORAGE
    return impl_->ReadView<TFileDetails>(
        collectionID,
        [&](const TScanContext& context) {
            return ReadFileDetails(context, fileID, selector);
        },
        false);
#else
    (void)collectionID;
    (void)fileID;
    (void)selector;
    return std::unexpected(
        ErrorDetail{"DEPENDENCY_UNAVAILABLE", "storage metadata is not built", RetryHint::OperatorAction});
#endif
}
// NOLINTEND(bugprone-easily-swappable-parameters)

Result<ScanExplanation> Engine::ExplainScan(const ScanRequest& request) const
{
    const auto impl = impl_;
#if GLAMDB_HAS_STORAGE
    return impl->ReadView<ScanExplanation>(request.collection_id, [&](const TScanContext& context) {
        return detail::ExplainScan(context, request);
    });
#else
    (void)request;
    return std::unexpected(
        ErrorDetail{"DEPENDENCY_UNAVAILABLE", "storage metadata is not built", RetryHint::OperatorAction});
#endif
}

Result<std::shared_ptr<ScanCursor>> Engine::Scan(const ScanRequest& request) const
{
    NGlamDB::NTelemetry::TReadOperationScope metrics(NGlamDB::NTelemetry::EReadOperation::ScanOpen);
    const auto impl = impl_;
    // The root this scan resolved; held until planning has read it, so a
    // concurrent append does not remove it underneath the scan.
    std::optional<std::string> leased_root;
    const auto release_root = [&] {
        if (leased_root) {
            impl->ReleaseRoot(*std::exchange(leased_root, std::nullopt));
        }
    };
    try {
        Require(ValidUuid(request.collection_id), "INVALID_ARGUMENT", "scan requires a canonical collection UUID");
#if GLAMDB_HAS_STORAGE
        TScanContext context;
        {
            // Latest is resolved here exactly once, from the confirmed view;
            // no SQLite read transaction is held while files are read.
            std::lock_guard lock(impl->state_mutex);
            impl->CheckReady();
            context.Collection = impl->CachedCollection(request.collection_id);
            const auto view = std::ranges::find_if(impl->confirmed_views, [&](const auto& item) {
                return item.info.collection_id == request.collection_id;
            });
            Require(view != impl->confirmed_views.end(), "CORRUPTION", "confirmed collection view is missing");
            context.Base = view->base;
            context.VerifiedState = view->scan_state;
            ++impl->root_readers[context.Base.metadata_location];
            leased_root = context.Base.metadata_location;
        }
        Require(impl->backend != nullptr, "DEPENDENCY_UNAVAILABLE", "storage reader is not installed");
        const auto capabilities = impl->backend->GetCapabilities();
        context.Resources = impl->resources;
        context.FileIO = Take(impl->backend->ReadOnlyFileIO());
        context.Cache = impl->backend->ReadMetadataCache();
        context.EnableFilePruning = capabilities.file_pruning;
        context.EnableRowGroupPruning = capabilities.row_group_pruning;
        context.Closing = std::shared_ptr<const std::atomic<bool>>(impl, &impl->closing);
        context.Lease = impl;
        context.OnError = [impl](const ErrorDetail& error) {
            impl->CloseOnCorruption(error);
        };
        auto cursor = OpenScan(std::move(context), request);
        // Planning pinned membership or read the uncached root. The cursor now
        // needs only files that are never removed.
        release_root();
        if (!cursor) {
            impl->CloseOnCorruption(cursor.error());
        }
        return metrics.Finish(std::move(cursor));
#else
        throw Failure({"DEPENDENCY_UNAVAILABLE", "storage scanner is not built", RetryHint::OperatorAction});
#endif
    } catch (const Failure& error) {
        release_root();
        impl->CloseOnCorruption(error.detail);
        metrics.Error(error.detail.code);
        return std::unexpected(error.detail);
    } catch (const std::exception& error) {
        release_root();
        const auto detail = ExceptionError(error);
        metrics.Error(detail.code);
        return std::unexpected(detail);
    }
}

Status Engine::Reconcile()
{
    NGlamDB::NTelemetry::TReadOperationScope metrics(NGlamDB::NTelemetry::EReadOperation::Reconcile);
    const auto impl = impl_;
    std::unique_lock lock(impl->writer, std::try_to_lock);
    if (!lock.owns_lock()) {
        metrics.Error("OPERATION_IN_PROGRESS");
        return std::unexpected(
            ErrorDetail{"OPERATION_IN_PROGRESS", "writer is still executing", RetryHint::SameIdAfterReconcile});
    }
    try {
        Require(!impl->closed, "CANCELLED", "engine is closed");
        impl->Recover(true);
        metrics.Finish();
        return {};
    } catch (const Failure& error) {
        metrics.Error(error.detail.code);
        return std::unexpected(error.detail);
    } catch (const std::exception& error) {
        const auto detail = ExceptionError(error);
        metrics.Error(detail.code);
        return std::unexpected(detail);
    }
}

Status Engine::Shutdown(std::chrono::milliseconds timeout)
{
    NGlamDB::NTelemetry::TReadOperationScope metrics(NGlamDB::NTelemetry::EReadOperation::CloseStore);
    const auto impl = impl_;
    impl->closing = true;
    impl->metadata_pages.Clear();
    std::unique_lock lock(impl->writer, std::defer_lock);
    if (!lock.try_lock_for(std::max(timeout, std::chrono::milliseconds(0)))) {
        metrics.Error("DEADLINE_EXCEEDED");
        return std::unexpected(ErrorDetail{"DEADLINE_EXCEEDED",
            "writer has not finished; resources remain owned",
            RetryHint::OperatorAction});
    }
    try {
        bool ready = false;
        {
            std::lock_guard state_lock(impl->state_mutex);
            if (impl->closed) {
                metrics.Finish();
                return {};
            }
            ready = impl->ready;
        }
        if (!ready) {
            impl->Recover(true);
        }
        // Roots whose scans finished after the last append.
        impl->RemoveReleasedRoots();
        if (impl->backend) {
            impl->backend->InvalidateCache();
        }
        impl->catalog->Close();
        std::lock_guard state_lock(impl->state_mutex);
        impl->ready = false;
        impl->closed = true;
        metrics.Finish();
        return {};
    } catch (const Failure& error) {
        metrics.Error(error.detail.code);
        return std::unexpected(error.detail);
    } catch (const std::exception& error) {
        const auto detail = ExceptionError(error);
        metrics.Error(detail.code);
        return std::unexpected(detail);
    }
}

} // namespace glamdb
