# Shared resource budgets

The embedded engine and native HTTP service need shared budgets as well as
individual request limits. This contract defines the accounting units, owners,
reservation lifetimes and refusal rules for the synchronous append profile.
It complements the [HTTP contract](HTTP_SERVER.md) and its
[v0 wire fixtures](../../fixtures/http/v0.json), established by issue #29.

The integration baseline is merged [PR #19](https://github.com/glamourdb/glamdb/pull/19):
final head `ee480ec9a10ab3d6baabf9b5296accf57a82adba`, merge
`87b557e4685331c99666370c52a40aaa505f0d71`. The matrices below were checked
against the subsequent health/API baseline `4e53c89`. **Current** means an
implemented local check, not a qualified simultaneous resource guarantee.
**Required** identifies enforcement still needed before a profile can claim a
bounded process. [The shared owner](SHARED_RESOURCES.md) now implements finite
reservation classes and device admission through public `ResourceLimits`.
Full-engine RSS, capacity and durability qualification remain open.

## Accounting units

MiB means 2^20 bytes; GiB means 2^30; TiB means 2^40. Counts, durations,
logical bytes, allocated memory and physical disk usage are separate units.

| Quantity | Definition and boundary |
| --- | --- |
| Canonical decoded bytes | Per field: one validity byte; for a non-NULL value add one byte for bool, eight for int64/float64/timestamp, or four length bytes plus UTF-8 string bytes. Sum fields into rows and rows into the append; exclude the canonical hash header. Omitted nullable fields count as NULL. Independent of Arrow chunking and HTTP encoding. |
| Owned-input reservation | `4 * decoded_bytes + 2 * null_slot_bytes + rows * (2 * sizeof(Row) + fields * sizeof(Cell)) + 65536`, from `InputReservation`; NULL slots add four bytes for string offsets, one for bool or eight for numeric/timestamp fields, including omitted fields. A conservative admission estimate for normalized rows, hashing and rebuilt Arrow input; not measured RSS or a writer/reader allocation cap. |
| Physical memory | Allocated capacities, allocator rounding and live parent buffers, including Arrow validity/offset/value buffers, builder growth, STL cells/strings, codecs, Parquet, Avro, Iceberg, JSON and SQLite. Slices can retain an entire parent allocation. |
| Writer input bytes | The backend estimates the Arrow batch it will build; the writer separately checks its buffers and row groups. NULL fixed-width slots, offsets and bitmaps differ from canonical bytes. Its 16 MiB cap does not redefine canonical input size. |
| Wire bytes | UTF-8 HTTP request/response body bytes, including JSON keys, escaping and decimal strings, or a complete Arrow IPC page with schema and EOS. Header bytes have a separate cap. Compressed and chunked requests are rejected in the current profile. |
| Disk bytes | Warehouse traversal counts the larger of logical length and allocated blocks for regular files, directory blocks, and hard links per entry; includes orphans. Catalog pages, rollback journal and recovery workspace are separate charges. |

The [canonical inspector and reservation](../../core/api/arrow_input.cpp),
[backend preflight](../../core/backend/local_storage_backend.cpp) and
[writer](../../core/storage/data_file_writer.cpp) enforce different checks.
A canonical batch that fits can still fail the writer or transport cap. The
HTTP adapter allocates JSON and Arrow input before `AppendDurable`; the core's
owned-input reservation does not cover those allocations. Embedded callers own
their immutable input buffers for the synchronous call; the host application
must budget those buffers separately.

## Shared memory and admission matrix

A bounded deployment must have a process owner (the embedding host or HTTP
service) whose budgets cover every open store and every surviving lease. Store
sub-budgets cannot each independently spend the full process allowance. The symbols below describe accounting classes, separate from per-engine `Limits`.
`ResourceLimits` configures finite shared envelopes; their conservative upstream
estimates still need workload-specific calibration and RSS measurements.

| Budget / unit | Scope and owner | Reservation, enforcement and release | Status |
| --- | --- | --- | --- |
| Data memory `D`, allocated bytes | Process parent; engine writer and scanner children; transport decode/encode children | Reserve before allocation for input copies, Parquet buffers, decompression, filtering, output batches and IPC/JSON assembly. Keep simultaneous copies charged. Transfer accounting with ownership; release only when the last owning buffer is freed. | Current tracked Arrow pools, owned-input and writer/transport envelopes share the parent. RSS qualification remains open. |
| Metadata memory `M`, allocated bytes | Process parent; catalog, backend, planner, metadata cache and recovery children | Charge materialized roots/history, manifest lists/entries, schemas, metrics, footer parsing, caches and SQLite memory before loading or growing them. Retain charges with plans/caches; lazy file iteration alone is insufficient. | Current cached-root charges, per-store 32 MiB (`ReadCacheMemoryBytes`) immutable metadata/footer cache (`TSnapshotMetadataCache`) and catalog/planner/footer/recovery envelopes share the parent. Upstream expansion estimates remain conservative. |
| Control memory `C`, allocated bytes | Process parent; engine coordinator and service admission children | Charge operation/recovery contexts, pending-call descriptors, tasks, timers and error/receipt serialization. Partition ordinary admission, completion and recovery capacity; ordinary requests cannot borrow the protected partitions. Release with the last descriptor/context. | Current descriptor envelopes and protected completion/recovery classes complement the worker/call counts. |
| Completion reserve, bytes and slots within `C` plus execution memory/disk | Coordinator owns an admitted attempt; catalog and backend consume its child reservations | Before durable binding, reserve enough to finish or durably record failure, retain an unknown context, perform checked close and emit a bounded outcome. Keep it through publication/failure recording or transfer it to reconciliation. Disconnect never releases an executing attempt's reserve. | Current 64 KiB completion reservation plus execution/device envelopes before binding; uncertainty retains them. The existing 24,576-byte catalog headroom check also applies. |
| Parquet codec scratch, allocated bytes | The 256 MiB write data envelope reserved per attempt | Compression contexts are allocated outside Arrow pools for one page at a time (pages are at most 1 MiB, row groups 8 MiB). Measured peak RSS growth of one 10,000-row write: uncompressed 1.2 MiB, lz4_raw/gzip/zstd 1–3 at most 2 MiB, snappy 5.8 MiB, zstd 9 11.2 MiB, zstd 15–22 18.0 MiB. Collection write settings therefore stay within the existing envelope; no separate reservation. | Measured with the pinned Arrow codecs through pyarrow; RSS qualification remains open. |
| Recovery reserve, memory bytes, workspace disk bytes and control slots | Process parent grants an engine recovery child before open/reconciliation | Protect root/catalog validation, retained-history traversal, durability fence and resolution of unfinished bindings from normal data admission. Retain through recovery, including uncertainty; release temporary workspace after its last handle closes. | Current 768 MiB protected memory per engine plus a reserved 256 MiB workspace on the store device; the workspace uses eight 4 KiB cache pages. |
| Verified scan index, descriptor and boundary capacities within `M` | Backend confirmed/tentative generations, engine confirmed views and pinned cursors | One descriptor per verified append file and one boundary per retained snapshot. Publication copies a generation; recovery builds one index. Invalidation revokes admission; memory releases with the last generation owner. | The backend retained-root reservation covers its index and stays pinned with the state. Index capacity is measured by the [scan benchmark](SCAN_INDEX.md); the shared envelopes remain conservative. |
| Shared pending calls `P`, count | Process admission owner with per-store children and separate control/recovery slots | Reserve before posting or entering work; include queued and executing calls across every API/store. Hold until the underlying call finishes, even if its caller disconnects. Data calls cannot spend protected lookup/recovery slots. | Required across independent embedded engines; current HTTP dispatch counts are listed below. |
| Owned input, estimated bytes | Per store, shared across collections; `Engine::Impl` | `max_owned_input_bytes` defaults to 128 MiB. Reserve under the state mutex after writer acquisition and original-outcome lookup, before normalization; the append admission guard releases it when the synchronous call exits. | Current; only new operations consume this reservation. |
| Pending append calls, count | Per store, shared across collections; coordinator | `max_pending_calls` defaults to 16, including the writer and queued calls. Admission guard decrements on exit, including failures. Fingerprinting precedes this admission and is not process-wide concurrency control. | Current; shared admission for other embedded calls and across stores is required. |
| Writer wait / append deadline, milliseconds | Per store configuration; coordinator | Defaults/maxima: 5,000 / 60,000. Bound writer waiting and check the append deadline before execution/publication. Neither forcibly interrupts OS I/O nor proves rollback. | Current. |
| Returned buffers and active scans, allocated bytes / cursor count | Scanner and caller share a lease; process/store data and metadata children | Charge retained results, slices and C Data exports even after cursor cancellation, engine shutdown or release of the engine object. Charge backing allocations once; charge copies separately. Release buffer charges only on final buffer destruction. | Current engine/store, FileIO, tracked Arrow pool and metadata leases survive cancellation/close through buffers, slices and C Data exports, including filtered output. Compound predicate compilation reserves 4 MiB plus twice its measured tree capacity and retains that reservation with the program. Uncached plans and queued manifest descriptors retain their working admission; each original output has a separate metadata reservation, including zero-column slices. Each cursor holds 16 KiB ordinary control admission. Parent bytes bound concurrent scans; no separate embedded count quota. |
| Public metadata calls/results, allocated bytes | Engine read call and caller-owned DTOs; process/store metadata children | History materializes catalog operations, receipts and a root; statistics/explain/file APIs materialize the selected root/list and one manifest at a time. File listing retains descriptor DTOs; details retain one verified bounded Parquet footer and row-group DTOs. Release temporary structures when the call ends and DTO allocations when their caller releases them. The confirmed-root lease lasts through the call. | Current file/history caps, shared metadata working envelopes and capacity-sized leases for original returned results. Independently copied caller DTOs belong to the host budget. |
| Fixed metadata pages, sessions and estimated retained bytes | Per Engine `TMetadataPages` | At most 32 retained/loading listings; retained DTO capacities and conservative token/tree charges share 16 MiB. Reserve a session before loading; check retained-byte admission before caching. Five-minute absolute expiry is reclaimed on the next request; reconciliation/shutdown clears the cache. Empty/single-page results retain no session. | Current local cache limits plus shared metadata working, descriptor/token and retained DTO reservations. Returned pages keep independent leases after engine close; pressure preserves continuations and replay. |

[`Limits`](../../public/storage/types.h),
[`Engine::AppendDurable`](../../core/engine/engine.cpp),
[`DurableSqliteCatalogStore`](../../core/catalog/durable_sqlite_catalog.cpp),
[`TRecoveryVisited`](../../core/storage/recovery_visited.cpp) and
[`TLeaseBuffer`](../../core/scan/scan_cursor.cpp) are the current enforcement
and lifetime boundaries. The host must retain the accounting parent as long as
any dependent allocation survives, including after a database is closed.

Reservations must be atomic at the shared parent, overflow checked and released
on every refusal/exception path. Use one class owner per allocation; transfers
must not create an uncharged interval or double-credit the parent. Conservatively
reserve an upper bound when an upstream allocator cannot be charged directly,
and refuse before starting that work if the bound is unavailable. A qualified
process-memory profile must include simultaneous data, metadata, control and
untracked runtime overhead; `max_owned_input_bytes` alone is not that limit.

## Input and transport matrix

All rows below are **Current** local checks. Their native owners also use
the shared reservation envelopes above. CLI, SDK and UI requests
use these same server checks; their own client memory belongs to their process.

| Limit / unit | Bound and scope | Owner and enforcement / release |
| --- | --- | --- |
| Canonical input, rows / batches / bytes | Per append: 1..10,000 rows; at most 10,000 Arrow batches; 2 MiB per row; 16 MiB per append; strings 1 MiB, `key` 1,024 bytes | Core `Inspect`/fingerprint before binding. Deterministic validation, not a reservation. |
| Writer row / input / row group / output file, bytes | Per append: separate 2 MiB row estimate; 16 MiB Arrow input; 8 MiB row group target/bound; 32 MiB Parquet file | Backend preflight counts a validity byte plus typed slot bytes even for NULL fields, unlike canonical rows. It checks estimated input and allocates the normalized Arrow batch before binding, retaining it under the owned-input reservation through the call; writer and durable FileIO check production. Each row group's summed row estimates plus string offsets stay within the bound (the writer keeps 8 bytes per input row of running sums to choose the group size). File-size failure after binding retains a failed operation. Not a cap on transient writer allocations. |
| Scan batch, rows | `batch_rows` 1..65,536 per cursor | Core scan creation validates; cursor reads Parquet ranges positionally (`TScanRandomAccessFile`) without whole-file buffering and charges decoded/filtered Arrow buffers to its shared data pool. A row cap is not by itself a byte reservation. |
| HTTP header / body, bytes | 16 KiB header; 16 MiB request body | Session header parsing, Content-Length validation and bounded body read. Release buffers when the session and outstanding worker references finish. JSON parsing adds up to 160 nesting levels and 1,300,100 parser events for the 64-field schema, not a DOM-memory quota. |
| HTTP response / page, bytes and rows | 16 MiB body; scan page at most 10,000 rows, soft target 4 MiB | Route serializers prepare the complete body before success headers. The shared IPC codec measures schema, batch and EOS before allocating its fixed output buffer within the scan reservations. A single row may exceed the soft target if the complete envelope fits. Hold outgoing copies through write completion/timeout. |
| HTTP connections / header readers / bodies, counts | Per service: 96 connections by default, including overload replies; ordinary header allowances 64 globally / 32 per peer; 16 concurrent body readers, at most 12 ordinary | `TState`/`TSession`; excess headers get a 100 ms probe within the connection ceiling. Authentication precedes body admission. Four readers are reserved from ordinary uploads for database lifecycle and scan cancellation; public/static POSTs are ordinary. Additional bodies wait within that ceiling and the original read deadline; eligible controls can pass ordinary waiters. Body slots release at read completion, disconnect, timeout or shutdown. Accept pauses at the connection ceiling. Counts exclude kernel backlog/buffer accounting. |
| HTTP active calls, counts | Readers 4 / 2 per database; publishers 4 / 1 per database; controls 2 / 1 per database; lifecycle 4 / 1 per database | Separate pools; release only when the worker finishes. FIFO queues rotate across databases. Core remains the only serialized publisher. Health/capabilities observations run on the I/O thread. Shared protected completion memory and body/output leases remain reserved through ownership. |
| HTTP queued calls / retained request bytes | Per pool: 64 calls / 64 MiB; per database within each pool: 16 calls / 32 MiB | `TFairQueue` reserves before enqueue, releases on dispatch, expiry, observed reset/socket error or shutdown. A complete request's send-side EOF preserves its queue slot and response socket. Queued calls use the request timeout and never enter core after refusal. Active bodies and decoded allocations are separate. |
| HTTP scan sessions, count / time | Configurable live and completed global/per-database limits; defaults 64/8 for each; 60 s live idle/completed TTL; no default absolute lifetime | `TScans`; count and shared reservation admission precede planning. EOF destroys cursor/schema leases and retains only completed replay. Expiry, cancellation and lifecycle/stop revoke tokens once; returned/in-flight resources can outlive removal. Replay refreshes no deadline. [Policy and sufficient 32-reader profile](SCAN_SESSIONS.md). |
| Retained replay bodies, serialized bytes / shared data memory | Configurable; default 1 GiB globally, 128 MiB per database; 16 MiB logical bytes and 32 MiB + 64 KiB shared data per session | `TScans::Register` reserves both before planning or consumption, including open-only cursors. The shared lease covers replay/replacement copies; page assembly, outgoing copies, Arrow and core metadata retain separate parent charges. Completed eviction is oldest-first within the pressured scope and observable; live cursors are preserved. |
| HTTP metadata pages, serialized JSON bytes / count / time | 16 MiB per fixed inventory, 64 MiB retained JSON; 64 sessions, eight per database; 60 s idle / 300 s lifetime | `TMetadata`; binds original selector/kind/page size and immutable identity, rechecks core read admission, clears on lifecycle changes. Admission reclaims the oldest completed replay inventories before refusing session/byte capacity; unread inventories remain retained. Library DOM and temporary materialization overhead are additional; this is not a shared heap governor. |
| Database descriptors, count | 64 per managed service | `TDatabases::Create` under its shared mutex; persisted registry entries include closed/error databases. This is not a cap on the total memory or disk of their stores. |

Sources: [HTTP session admission](../../apps/server/server.cpp),
[codecs](../../apps/transport/codec.cpp),
[body constants](../../apps/transport/codec.h),
[scan sessions](../../apps/server/scans.cpp),
[database registry](../../apps/server/databases.cpp) and
[writer limits](../../core/storage/data_file_writer.h).
Sixteen 16 MiB request bodies alone can occupy 256 MiB of logical payload;
decoded JSON, Arrow input, active responses and other databases add to that.
The replay cap cannot be used as a bound on total scan or process memory.

## Persistent capacity and disk matrix

These are **Current** development defaults, shared across collections where
marked store-wide; the final row describes shared device admission.
Only public `Limits` fields are configurable through the
HTTP database `limits` object; private storage/FileIO/attempt limits are not
additional HTTP settings. Persistent charges do not disappear when a call ends.

| Limit / unit | Bound and scope | Owner and enforcement / release |
| --- | --- | --- |
| Catalog collections / operations, counts | Store-wide `max_collections=8`, `max_operations=4096`; operations include bound, committed and failed records | Catalog capacity check before insert, plus reopen validation. Bindings/outcomes do not expire; closing a store does not refund them. An operation ID is collection-scoped but the cap covers all collections. |
| Catalog database, page bytes | Store-wide `max_catalog_bytes=256 MiB`, rounded down to whole SQLite pages; minimum configuration 64 KiB | Working connection `max_page_count` and page-count check on open; 24,576 bytes of freelist/growth headroom before a binding or collection creation. Excludes journal, service registry and SQLite heap/cache; catalog cache target is 8 MiB per connection. |
| Individual metadata files, serialized bytes | Root JSON 16 MiB; manifest 8 MiB; manifest list 16 MiB | Durable FileIO size checks and publication/recovery validation. Keep the disk charge for retained files and orphans. |
| Aggregate current roots, bytes | Store-wide 64 MiB | Backend aggregate usage and publication guard; root replacement subtracts the previous root before adding the candidate. Superseded roots held by a planning scan still count as physical warehouse usage. |
| Retained snapshots / snapshot-log entries / manifests / data files, counts | Store-wide 4,096 / 4,096 / 4,096 / 16,384 | Publication guard and `VerifyRetained` aggregate `TStorageUsage` across collections. Per-collection metadata log cap 4,096, at most 8 new manifests per commit. Retained history is not expired to make room. |
| Retained manifest / manifest-list / footer bytes | Store-wide 256 MiB / 512 MiB / 256 MiB | Guard and recovery count retained serialized structures. These are persisted-structure limits, not shared metadata-memory reservations. |
| Commit verification, bytes | 32 MiB per commit | Publication guard and recovery use matching commit accounting. Recovery charges each retained commit's manifests/list and only the final current root to its current commit, not all history to one commit. |
| Attempt new-file / I/O volume, bytes | 256 MiB new files; 512 MiB requested reads plus writes per attempt, including retries | `TIOAttempt` reserves requested writes before I/O and settles new-file usage to accepted bytes, refunding unused file capacity. Requested I/O stays charged even on failure. Handles retain their originating attempt. Attempt completion does not refund physical files left behind. Not a process memory or device-space reservation. |
| Warehouse, physical bytes | 1 TiB per store, including orphans; admit only if another 256 MiB attempt fits | Backend preflight measures warehouse before binding and reuses that measurement for the same append, rechecking identity/free space before production. The publication guard rechecks the cap after production from that measurement plus the attempt's durable files and directory growth (creation walks again); growth by others after admission is caught by the next admission; recovery checks retained physical usage without requiring a future attempt reserve. Successful removal of an eligible superseded root releases its disk use; no general orphan cleanup. |
| Filesystem headroom / attempt / journal, available bytes | `max(1 GiB, ceil(filesystem_capacity / 20)) + 256 MiB + 256 MiB` | `CheckDiskReserve` compares against available space. This is a preflight check, not filesystem preallocation or a shared reservation across databases/processes. Preserve journal/recovery room when rejecting new writes. |
| Recovery workspace, page bytes | Temporary SQLite database at most 256 MiB per verifier | Native recovery creates it in the store root on the reserved device; isolated storage callers may retain the temporary-filesystem default. Close removes it; exclusive, identity-validated reopen reclaims interrupted regular workspaces before device admission. Workspace exhaustion fails recovery. |
| Shared device capacity, allocated bytes and inodes | Finite process/device reservation profile covering stores, catalog/journals, service registry and recovery workspaces | The shared owner atomically subtracts outstanding byte/inode reservations from current filesystem availability. Open engines protect catalog/journal/workspace growth; attempts reserve growth before binding. Retained files remain charged by filesystem allocation after reservations end. |

Sources: [catalog limits](../../core/catalog/durable_sqlite_catalog.cpp),
[storage limits and usage](../../core/storage/capacity.h),
[disk checks](../../core/storage/capacity.cpp),
[FileIO limits](../../core/storage/durable_local_file_io.h),
[attempt reservations](../../core/io/io_attempt.cpp) and
[storage validation](STORAGE_MODULE.md).
Shared per-device byte/inode reservations now cover stores, attempts, catalog
and journals, the service registry and recovery workspaces. The owner samples
filesystem availability again after restart and atomically accounts for pending growth;
external ENOSPC/EIO must still propagate through the durable outcome protocol.

## Admission, release and errors

For new work the required order is process/store admission, transport decode
reservation, core validation and original-outcome lookup, execution and
completion/recovery reservations, durable operation binding, production, then
guarded publication. The current coordinator already returns a known terminal
retry before reserving owned input, checking a new snapshot precondition or
consuming another permanent operation slot. Keep that ordering when adding
shared budgets. Lookup and reconciliation need protected control capacity.

Returning, cancelling or disconnecting releases only resources actually no
longer owned. The writer owns an admitted publication through its known outcome
or recovery-required state. Retained Arrow buffers and HTTP replay/outgoing
copies keep their own charges. Failure does not delete operation bindings,
receipts, retained snapshots or orphan files. Resource pressure by itself does
not latch corruption or change readiness.

| Refusal boundary | Core/transport result and HTTP mapping | Identity and retry rule |
| --- | --- | --- |
| Header/body/canonical size or invalid shape before binding | `HEADERS_TOO_LARGE` 431; `PAYLOAD_TOO_LARGE` / `INPUT_TOO_LARGE` 413 with `FixInput`; invalid row/batch/string shape `INVALID_ARGUMENT` 400 | Reject this input without binding an ID. A rejection of this call says nothing about a previous call with the same ID. |
| Writer row/input estimate before binding | Current `RESOURCE_EXHAUSTED` with `FixInput` maps to 413, including a canonical-valid row/batch that exceeds the separate writer estimate | Reduce/change input; this is a deterministic input refusal, not transient shared admission. No ID is bound. Follow the core hint rather than assuming every 429 permits unchanged input. |
| Transient shared admission | `RESOURCE_EXHAUSTED` / `DATABASE_BUSY` 429 where no outcome override applies; HTTP worker/body/connection overload `SERVER_BUSY` 503 with `Retry-After: 1` when a response can be admitted | Preserve original identity. Pending-call or writer-wait refusal first tries authoritative outcome lookup; unresolved lookup is `CommitUnknown` (503), not proof of absence. |
| Owned-input, catalog, disk or produced-file capacity / allocation failure | Core `RESOURCE_EXHAUSTED` with `OperatorAction` maps to 507; `CAPACITY_EXCEEDED` also maps to 507 | Before binding, no new binding; after binding, a proven failure is permanently recorded. Same ID returns that terminal failure, not a fresh attempt. Do not treat every 507 as retryable overload. |
| Transport response/replay capacity | Oversized responses use `INPUT_TOO_LARGE` (413). Replay capacity is reserved at session admission; refusal (429) occurs before planning and cannot invalidate another admitted cursor. Other failed scan pages invalidate their sessions | Cancel unused scans before opening another. This is not a core append disposition. Preserve a write's ID and resolve its outcome if its response is lost or cannot be encoded. |
| Cannot prove binding/publication/failure-recording outcome | `CommitUnknown`, normally 503 with `SameIdAfterReconcile`, complete recovery context | Hold the write barrier and reconcile/fence before new writes. Exhausting a reserve, timing out or losing a socket cannot turn uncertainty into `DefinitelyNotCommitted`. |
| Open under insufficient configured limits | Catalog `CONFIG_TOO_SMALL` (HTTP 400); persisted storage violations follow the [storage error classification](SCANNER.md#error-classification) | Fail closed without pruning history, dropping receipts or rewriting the store. Restore an adequate profile and reopen; failure does not authorize creating an empty replacement. |

Mappings are owned by [the transport error policy](../../apps/transport/errors.h)
and [durable outcome serializer](../../apps/transport/outcome_json.cpp).
Core retry hints and authoritative dispositions take precedence over a generic
status-code retry rule.

## Publication and reopening

An accepted committed state must reopen under the same profile and limits,
including all collections, retained snapshots, permanent failed outcomes and
required recovery workspace. Validate the complete candidate against the same
units on both paths before changing the catalog pointer. Replacing a root is
not an extra retained root; validating retained history is not one giant new
commit. Equality at a limit must be accepted wherever publication accepted it.

The managed service persists public engine limits in its registry and reuses
them on restart. It changes limits only while a database is closed. Embedded
callers must supply the same limits on `OpenExisting`; the profile ID alone
does not persist their custom `Limits` values. Lower limits may reject open,
but must preserve the original state and identity. The shared owner reserves
recovery capacity before opening stores, admits data only after recovery, and
retains parents for buffers surviving close.

## Validation boundaries

Documentation checks should verify source constants, ownership and local links.
Existing component coverage is separated below. Shared-owner tests and measured
HTTP/cold-reopen evidence are in [the resource validation report](SHARED_RESOURCES_VALIDATION.md).
Neither logical counters nor these samples establish a universal peak-RSS bound.

| Evidence layer | Existing checks |
| --- | --- |
| Coordinator with test backend, real SQLite | `tests/unit/coordinator_test.cpp`, `tests/fault/pending_budget_owner_test.cpp`: pending writer accounting, owned-input refusal, terminal retry under lower limits, uncertainty during admission. The backend is a double, not a native Parquet writer. |
| Native storage components | `StorageCapacity`, `TWriteCapacityTest`, `IoAttempt`, publication/recovery tests: exact caps, cross-collection usage, arithmetic overflow, orphan accounting, attempt/journal reserves and same-limit reopening. |
| Real public engine and dependencies | `AppendAdmissionTest`: exact/over-limit NULL-heavy writer inputs, impossible free-space reserve, allocation refusal, owned-input refusal and old retries; `ArrowInput.NullHeavyCanonicalLimitsAgreeAcrossInputPaths`: canonical row/batch boundaries; `EngineVerticalTest` retained-buffer and multi-collection capacity cases; `BackendRecoveryLimitsTest.ReopenAcceptsHistoryPublishedUnderTheSameCommitBytes`: real Parquet/Avro/JSON and SQLite, lease lifetime, publication/reopen accounting. |
| Real HTTP service | `glamdb.database_limits`, `glamdb.http_review`, `glamdb.native_http_fault`, `glamdb.scan_http`: persisted/lowered profiles, capacity status, socket admission, HTTP/verified-HTTPS body reserves, page/replay/session limits, cancellation and expiry. TLS terminates at a byte-forwarding test proxy; requests reach the native listener and real storage. SDK/Arrow cases require their Python dependencies. |

For a full configured dependency build, discover and run the focused cases with:

```sh
cmake --build --preset local --target glamdb_unit glamdb_fault glamdb_io_test \
    glamdb_storage_test glamdb_vertical glamdb_server glamdb_cli --parallel 2
ctest --preset local --output-on-failure --no-tests=error \
    -R 'Coordinator|PendingBudget|StorageCapacity|WriteCapacity|IoAttempt|BackendRecoveryLimits|EngineVerticalTest|glamdb\.(database_limits|http_review|scan_http)$'
```

Shared-owner acceptance includes deterministic combined-load tests across
stores and collections: simultaneous writer/reader/transport allocations,
retained slices after shutdown, ordinary admission exhausted while completion
and recovery succeed, and colliding disk/workspace reservations on one device.
Measure actual allocation peaks/RSS and cold reopen under the same profile.
Mock-only or independent per-request checks cannot close those gates.
