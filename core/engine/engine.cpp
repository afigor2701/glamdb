#include "storage/engine.h"

#include "core/api/binary.h"
#include "core/api/canonical.h"
#include "core/api/codec.h"
#include "core/engine/engine_factory.h"
#include "core/engine/storage_mapping.h"
#include "core/engine/store_lease.h"

#if GLAMDB_HAS_STORAGE
#include "core/backend/local_storage_backend.h"
#include "core/scan/scan_cursor.h"
#endif

#include <algorithm>
#include <atomic>
#include <mutex>

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
    return {"INTERNAL", BoundedDiagnostic(error.what()), RetryHint::OperatorAction};
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

BackendFactory DefaultBackendFactory()
{
#if GLAMDB_HAS_STORAGE
    return [](const std::filesystem::path& root, const StoreInfo& info) -> Result<std::shared_ptr<StorageBackend>> {
        auto backend = TLocalStorageBackend::Open(root, info);
        if (!backend) {
            return std::unexpected(backend.error());
        }
        return std::shared_ptr<StorageBackend>(std::move(*backend));
    };
#else
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
    bool closed = false;
    uint64_t health_generation = 0;
    int64_t pending_calls = 0;
    size_t owned_input_bytes = 0;
    std::vector<CollectionInfo> collections;
    std::vector<CollectionView> confirmed_views;
    std::optional<BoundOperation> active;
    std::optional<RecoveryContext> recovery;
    std::optional<AppendOutcome> pending_failure;
    std::optional<CreateOutcome> unresolved_create;

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

    void CheckHealthy(uint64_t expected_generation) const
    {
        CheckReady();
        Require(health_generation == expected_generation,
            "RECOVERY_REQUIRED",
            "authoritative reconciliation is required");
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
        return {};
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

    void RefreshCache()
    {
        auto current = catalog->Collections();
        std::vector<CollectionView> views;
        views.reserve(current.size());
        for (const auto& collection : current) {
            views.push_back({collection, catalog->Base(collection.collection_id)});
        }
        std::lock_guard lock(state_mutex);
        collections = std::move(current);
        confirmed_views = std::move(views);
    }

    void CloseOnCorruption(const ErrorDetail& error)
    {
        if (error.code == "CORRUPTION" || error.code == "UNSUPPORTED_STORE_VERSION") {
            std::lock_guard gate(commit_gate);
            if (backend) {
                backend->InvalidateCache();
            }
            std::lock_guard lock(state_mutex);
            ready = false;
            ++health_generation;
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
        uint64_t start_generation = 0;
        {
            std::lock_guard lock(state_mutex);
            ready = false;
            start_generation = ++health_generation;
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
        unresolved_create.reset();
        ready = true;
    }

    AppendOutcome Unknown(RecoveryContext context,
        CommitPhase phase,
        ErrorDetail error,
        std::optional<AppendOutcome> failure = std::nullopt)
    {
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
        error.message = BoundedDiagnostic(error.message);
        // The bound ID is consumed either way. Capacity and corruption need
        // operator action before any new operation can succeed.
        const bool operatorAction = error.code == "RESOURCE_EXHAUSTED" || error.code == "CORRUPTION" ||
            error.code == "UNSUPPORTED_STORE_VERSION";
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
    try {
        auto impl = std::make_shared<Engine::Impl>();
        impl->info = {request.store_id, request.profile_id, 1};
        impl->limits = request.limits;
        ValidateConfig(impl->info, impl->limits);
        impl->lease = StoreLease::Create(request.absolute_root, impl->info);
        impl->backend = MakeBackend(backend, impl->lease->Root(), impl->info);
        impl->catalog = DurableSqliteCatalogStore::Create(impl->lease->Root() / "catalog.sqlite",
            impl->info,
            impl->limits,
            std::move(sqlite_vfs));
        impl->Recover(false);
        impl->lease->MarkReady(impl->info);
        return std::shared_ptr<Engine>(new Engine(std::move(impl)));
    } catch (const Failure& error) {
        return std::unexpected(error.detail);
    } catch (const std::exception& error) {
        return std::unexpected(ExceptionError(error));
    }
}

Result<std::shared_ptr<Engine>>
EngineFactory::OpenWith(const OpenStoreRequest& request, const BackendFactory& backend, std::string sqlite_vfs)
{
    try {
        auto impl = std::make_shared<Engine::Impl>();
        impl->info = {request.expected_store_id, request.profile_id, 1};
        impl->limits = request.limits;
        ValidateConfig(impl->info, impl->limits);
        impl->lease = StoreLease::Open(request.absolute_root, impl->info);
        impl->backend = MakeBackend(backend, impl->lease->Root(), impl->info);
        impl->catalog = DurableSqliteCatalogStore::Open(impl->lease->Root() / "catalog.sqlite",
            impl->info,
            impl->limits,
            std::move(sqlite_vfs));
        impl->Recover(false);
        return std::shared_ptr<Engine>(new Engine(std::move(impl)));
    } catch (const Failure& error) {
        return std::unexpected(error.detail);
    } catch (const std::exception& error) {
        return std::unexpected(ExceptionError(error));
    }
}

} // namespace detail

Engine::Engine(std::shared_ptr<Impl> implementation)
    : impl_(std::move(implementation))
{ }
Engine::~Engine() = default;

Result<std::shared_ptr<Engine>> CreateStore(const CreateStoreRequest& request)
{
    return detail::EngineFactory::CreateWith(request, DefaultBackendFactory());
}
Result<std::shared_ptr<Engine>> OpenExisting(const OpenStoreRequest& request)
{
    return detail::EngineFactory::OpenWith(request, DefaultBackendFactory());
}
const StoreInfo& Engine::GetStoreInfo() const
{
    return impl_->info;
}

Result<CollectionInfo> Engine::GetCollection(std::string_view id) const
{
    try {
        const auto impl = impl_;
        std::lock_guard lock(impl->state_mutex);
        impl->CheckReady();
        return impl->CachedCollection(id);
    } catch (const Failure& error) {
        return std::unexpected(error.detail);
    }
}

Result<CollectionInfo> Engine::ResolveCollection(std::string_view name) const
{
    try {
        const auto impl = impl_;
        std::lock_guard lock(impl->state_mutex);
        impl->CheckReady();
        const auto it = std::ranges::find_if(impl->collections, [&](const auto& item) {
            return item.name == name;
        });
        Require(it != impl->collections.end(), "COLLECTION_NOT_FOUND", "exact collection name does not exist");
        return *it;
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

Result<std::shared_ptr<const arrow::Schema>> Engine::Schema(std::string_view id) const
{
    const auto collection = GetCollection(id);
    if (!collection) {
        return std::unexpected(collection.error());
    }
    return MakeArrowSchema(collection->schema);
}

CreateOutcome Engine::CreateCollection(const CreateCollectionRequest& request)
{
    const auto impl = impl_;
    std::unique_lock writer_lock(impl->writer, std::defer_lock);
    std::optional<CreateRecoveryContext> context;
    bool publication_started = false;
    bool committed = false;
    try {
        Require(ValidUuid(request.collection_id) && !request.exact_name.empty() && request.exact_name.size() <= 128 &&
                ValidUtf8(request.exact_name) && request.exact_name.find('\0') == std::string::npos,
            "INVALID_ARGUMENT",
            "invalid collection ID or exact name");
        const auto schema_hash = Take(SchemaFingerprint(request.schema));
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
                return *blocked;
            }
        }
        if (!writer_lock.try_lock_for(impl->limits.writer_wait)) {
            std::lock_guard state_lock(impl->state_mutex);
            const auto blocked = impl->BlockedCreate(*context);
            if (blocked) {
                return *blocked;
            }
            return {CommitDisposition::CommitUnknown,
                CommitPhase::Reconciling,
                std::nullopt,
                ErrorDetail{"RESOURCE_EXHAUSTED",
                    "writer busy; original creation outcome was not resolved",
                    RetryHint::SameIdAfterReconcile},
                context};
        }
        uint64_t expected_generation = 0;
        {
            std::lock_guard state_lock(impl->state_mutex);
            const auto blocked = impl->BlockedCreate(*context);
            if (blocked) {
                return *blocked;
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
                return {CommitDisposition::Committed, CommitPhase::Complete, collection, std::nullopt, std::nullopt};
            }
        }
        Require(existing.size() < static_cast<size_t>(impl->limits.max_collections),
            "RESOURCE_EXHAUSTED",
            "permanent catalog capacity is full");
        Require(impl->backend != nullptr,
            "DEPENDENCY_UNAVAILABLE",
            "durable storage producer and closure guard are not installed");
        const auto attempt = NewUuid();
        context = CreateRecoveryContext{impl->info.store_id,
            request.collection_id,
            request.exact_name,
            schema_hash,
            attempt,
            std::nullopt,
            std::nullopt,
            false};
        auto staged = Take(impl->backend->PrepareCreate(impl->info, request, attempt));
        Require(staged != nullptr, "IO_ERROR", "producer returned no staged collection");
        auto info = staged->Info();
        Require(info.collection_id == request.collection_id && info.name == request.exact_name &&
                info.schema == request.schema,
            "IO_ERROR",
            "producer changed collection identity or schema");
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
                impl->backend->ConfirmPublication(created.collection_id, *context->candidate_metadata_location);
            }
            gate.unlock();
        }
        impl->RefreshCache();
        return {CommitDisposition::Committed, CommitPhase::Complete, created, std::nullopt, std::nullopt};
    } catch (const Failure& error) {
        impl->CloseOnCorruption(error.detail);
        if ((error.uncertain || committed) && context) {
            return impl->UnknownCreate(*context,
                committed ? CommitPhase::Reconciling : error.phase.value_or(CommitPhase::CatalogTransaction),
                error.detail);
        }
        return {CommitDisposition::DefinitelyNotCommitted,
            publication_started ? CommitPhase::CatalogTransaction : CommitPhase::Validation,
            std::nullopt,
            error.detail,
            std::nullopt};
    } catch (const std::exception& error) {
        if ((publication_started || committed) && context) {
            return impl->UnknownCreate(*context, CommitPhase::Reconciling, ExceptionError(error));
        }
        return {CommitDisposition::DefinitelyNotCommitted,
            CommitPhase::Validation,
            std::nullopt,
            ExceptionError(error),
            std::nullopt};
    }
}

AppendOutcome Engine::AppendDurable(const AppendRequest& request)
{
    const auto impl = impl_;
    const auto deadline = std::chrono::steady_clock::now() + impl->limits.append_deadline;
    // Retain the writer across catch handlers: recording a failure and setting
    // an unknown barrier are part of the serialized attempt, too.
    std::unique_lock writer_lock(impl->writer, std::defer_lock);
    struct Admission
    {
        std::shared_ptr<Impl> owner;
        size_t bytes = 0;
        ~Admission()
        {
            if (owner) {
                std::lock_guard lock(owner->state_mutex);
                --owner->pending_calls;
                owner->owned_input_bytes -= bytes;
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
        CollectionInfo collection;
        std::optional<RecoveryContext> unresolved;
        {
            std::lock_guard lock(impl->state_mutex);
            unresolved = impl->BlockedAppend(request);
            collection = impl->CachedCollection(request.collection_id);
        }
        const auto payload_hash = Take(FingerprintBatches(collection.schema, request.collection_id, request.batches));
        if (unresolved) {
            return UnresolvedAppend(*unresolved, payload_hash);
        }
        if (!writer_lock.try_lock()) {
            bool full = false;
            {
                std::lock_guard lock(impl->state_mutex);
                unresolved = impl->BlockedAppend(request);
                full = impl->pending_calls >= impl->limits.max_pending_calls;
                if (!unresolved && !full) {
                    ++impl->pending_calls;
                    admission.owner = impl;
                }
            }
            if (unresolved) {
                return UnresolvedAppend(*unresolved, payload_hash);
            }
            if (full) {
                return impl->ResolveUnadmitted(request,
                    payload_hash,
                    {"RESOURCE_EXHAUSTED",
                        "pending budget is full; original operation outcome was not resolved",
                        RetryHint::SameIdAfterReconcile});
            }
            if (!writer_lock.try_lock_until(
                    std::min(deadline, std::chrono::steady_clock::now() + impl->limits.writer_wait)))
            {
                return impl->ResolveUnadmitted(request,
                    payload_hash,
                    {"RESOURCE_EXHAUSTED",
                        "writer wait expired; original operation outcome was not resolved",
                        RetryHint::SameIdAfterReconcile});
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
            return UnresolvedAppend(*unresolved, payload_hash);
        }
        std::optional<OperationRecord> previous;
        try {
            previous = impl->catalog->FindOperation(request.collection_id, request.operation_id);
        } catch (const Failure& error) {
            context.first_error = error.detail;
            return impl->Unknown(context, CommitPhase::Reconciling, error.detail);
        } catch (const std::exception& error) {
            context.first_error = ExceptionError(error);
            return impl->Unknown(context, CommitPhase::Reconciling, context.first_error);
        }
        if (previous) {
            Require(previous->binding.payload_hash == payload_hash,
                "OPERATION_CONFLICT",
                "operation ID is permanently bound to different content");
            if (previous->outcome) {
                try {
                    impl->VerifyStorage();
                } catch (const Failure& error) {
                    return impl->Unknown(Context(previous->binding, error.detail, true),
                        CommitPhase::Reconciling,
                        error.detail);
                } catch (const std::exception& error) {
                    const auto detail = ExceptionError(error);
                    return impl->Unknown(Context(previous->binding, detail, true), CommitPhase::Reconciling, detail);
                }
                return *previous->outcome;
            }
            const ErrorDetail error{"RECOVERY_REQUIRED",
                "pending binding requires reconciliation",
                RetryHint::SameIdAfterReconcile};
            return impl->Unknown(Context(previous->binding, error, false), CommitPhase::Reconciling, error);
        }
        // Only a new operation needs owned payload memory. A retry has already
        // returned its original terminal receipt, even under a lower input cap.
        const auto reservation = Take(InputReservation(collection.schema, request.batches));
        {
            std::lock_guard lock(impl->state_mutex);
            impl->CheckHealthy(expected_generation);
            if (!admission.owner) {
                Require(impl->pending_calls < impl->limits.max_pending_calls,
                    "RESOURCE_EXHAUSTED",
                    "pending budget is full");
                ++impl->pending_calls;
                admission.owner = impl;
            }
            Require(reservation <= static_cast<size_t>(impl->limits.max_owned_input_bytes) - impl->owned_input_bytes,
                "RESOURCE_EXHAUSTED",
                "owned input budget is full");
            impl->owned_input_bytes += reservation;
            admission.bytes = reservation;
        }
        const auto input = Take(NormalizeBatches(collection.schema, request.collection_id, request.batches));
        Require(input.payload_hash == payload_hash, "INVALID_ARGUMENT", "Arrow input changed during append");
        const auto base = impl->catalog->Base(request.collection_id);
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
        try {
            impl->catalog->BindOperation(*binding);
        } catch (const Failure& error) {
            if (error.uncertain) {
                context.first_error = error.detail;
                return impl->Unknown(context, phase, error.detail);
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
        Require(impl->backend != nullptr, "DEPENDENCY_UNAVAILABLE", "durable storage producer is not installed");
        phase = CommitPhase::Data;
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
        phase = CommitPhase::Complete;
        if (gate.owns_lock()) {
            if (context.candidate_metadata_location) {
                impl->backend->ConfirmPublication(binding->collection_id, *context.candidate_metadata_location);
            }
            gate.unlock();
        }
        impl->RefreshCache();
        {
            std::lock_guard state_lock(impl->state_mutex);
            impl->active.reset();
        }
        return committed;
    } catch (const Failure& error) {
        impl->CloseOnCorruption(error.detail);
        if (!binding) {
            return Rejected(request.operation_id, phase, error.detail);
        }
        context.first_error = error.detail;
        if (error.uncertain || phase == CommitPhase::Complete) {
            return impl->Unknown(context, error.phase.value_or(CommitPhase::Reconciling), error.detail);
        }
        return impl->FailBound(*binding, phase, error.detail, publication_started);
    } catch (const std::exception& error) {
        const auto detail = ExceptionError(error);
        if (!binding) {
            return Rejected(request.operation_id, phase, detail);
        }
        if (publication_started) {
            context.first_error = detail;
            return impl->Unknown(context, CommitPhase::Reconciling, detail);
        }
        return impl->FailBound(*binding, phase, detail);
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
            Require(!active || active->collection_id != collection_id || active->operation_id != operation_id,
                "OPERATION_IN_PROGRESS",
                "registered append is still executing");
        }
        const auto record = impl->catalog->FindOperation(collection_id, operation_id);
        {
            std::lock_guard lock(impl->state_mutex);
            impl->CheckReady();
        }
        if (!record) {
            return std::optional<AppendOutcome>{};
        }
        Require(record->outcome.has_value(), "OPERATION_IN_PROGRESS", "registered append is still executing");
        return record->outcome;
    } catch (const Failure& error) {
        impl->CloseOnCorruption(error.detail);
        return std::unexpected(error.detail);
    }
}

Result<std::shared_ptr<ScanCursor>> Engine::Scan(const ScanRequest& request) const
{
    const auto impl = impl_;
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
        }
        Require(impl->backend != nullptr, "DEPENDENCY_UNAVAILABLE", "storage reader is not installed");
        context.FileIO = Take(impl->backend->ReadOnlyFileIO());
        context.Closing = std::shared_ptr<const std::atomic<bool>>(impl, &impl->closing);
        context.Lease = impl->lease;
        context.OnError = [impl](const ErrorDetail& error) {
            impl->CloseOnCorruption(error);
        };
        auto cursor = OpenScan(std::move(context), request);
        if (!cursor) {
            impl->CloseOnCorruption(cursor.error());
        }
        return cursor;
#else
        throw Failure({"DEPENDENCY_UNAVAILABLE", "storage scanner is not built", RetryHint::OperatorAction});
#endif
    } catch (const Failure& error) {
        impl->CloseOnCorruption(error.detail);
        return std::unexpected(error.detail);
    } catch (const std::exception& error) {
        return std::unexpected(ExceptionError(error));
    }
}

Status Engine::Reconcile()
{
    const auto impl = impl_;
    std::unique_lock lock(impl->writer, std::try_to_lock);
    if (!lock.owns_lock()) {
        return std::unexpected(
            ErrorDetail{"OPERATION_IN_PROGRESS", "writer is still executing", RetryHint::SameIdAfterReconcile});
    }
    try {
        Require(!impl->closed, "CANCELLED", "engine is closed");
        impl->Recover(true);
        return {};
    } catch (const Failure& error) {
        return std::unexpected(error.detail);
    } catch (const std::exception& error) {
        return std::unexpected(ExceptionError(error));
    }
}

Status Engine::Shutdown(std::chrono::milliseconds timeout)
{
    const auto impl = impl_;
    impl->closing = true;
    std::unique_lock lock(impl->writer, std::defer_lock);
    if (!lock.try_lock_for(std::max(timeout, std::chrono::milliseconds(0)))) {
        return std::unexpected(ErrorDetail{"DEADLINE_EXCEEDED",
            "writer has not finished; resources remain owned",
            RetryHint::OperatorAction});
    }
    try {
        bool ready = false;
        {
            std::lock_guard state_lock(impl->state_mutex);
            if (impl->closed) {
                return {};
            }
            ready = impl->ready;
        }
        if (!ready) {
            impl->Recover(true);
        }
        impl->catalog->Close();
        std::lock_guard state_lock(impl->state_mutex);
        impl->ready = false;
        impl->closed = true;
        return {};
    } catch (const Failure& error) {
        return std::unexpected(error.detail);
    } catch (const std::exception& error) {
        return std::unexpected(ExceptionError(error));
    }
}

} // namespace glamdb
