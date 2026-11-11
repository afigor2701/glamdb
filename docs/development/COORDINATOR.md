# Catalog and coordinator MVP

The embedded facade in `public/storage/engine.h` implements explicit store
creation/opening, collection identity/schema lookup, synchronous append outcomes,
operation lookup, reconciliation and bounded shutdown. `glamdb::core` installs
as a CMake package. The CLI provides `create-store` and `inspect-store` with an
absolute root and caller-retained store UUID.

Read/configuration APIs use `Result<T> = std::expected<T, ErrorDetail>` and
`Status = Result<void>`. `ListCollections()` returns the full vector within the
eight-collection limit; this MVP has no collection pagination.

The bridge from the coordinator to the [durable storage primitives](STORAGE_MODULE.md)
and the scanner are described in [SCANNER.md](SCANNER.md); they are built with
`GLAMDB_BUILD_STORAGE=ON` and tested against the pinned Arrow, Parquet, Iceberg
and SQLite build (see its validation status). Without them, public factories
can create/open an empty catalog, and collection creation and scans report
`DEPENDENCY_UNAVAILABLE`. Opening a populated store
requires the storage validator. Coordinator test doubles are confined to
`tests/`; they produce no real Parquet or Avro data. The separate storage
component tests do exercise real Parquet, Avro and JSON files. The
[native HTTP server](HTTP_SERVER.md) adapts this same facade outside the embedded
component. Payload journaling, background jobs, whole-store backup and general
automatic file cleanup remain outside this MVP.

## Persistence and retry rules

- One directory lease owns the store across threads/processes. Opening never
  uses SQLite CREATE or repairs missing identity, readiness or catalog files.
- `DurableSqliteCatalogStore` implements Iceberg's public `CatalogStore` with
  one serialized SQLite connection. It checks the locked SQLite source ID and
  actual `DELETE`/`EXTRA` profile. Open validates schema/version, integrity,
  foreign keys, collection registry and operation records.
- The coordinator validates and fingerprints Arrow input without copying the
  payload. It copies into owned normalized rows only for a new operation, after
  acquiring the writer and resolving any previous outcome. Caller-owned Arrow
  buffers must remain immutable throughout the synchronous call. Backend admission
  checks disk/write health and writer limits, then builds the normalized writer
  batch before binding. Allocation refusal leaves the ID unbound; production
  reuses the admitted batch. Final publication still rechecks changing capacity.
  Field IDs follow fixed schema order from 1; omitted nullable values become
  NULL, duplicate rows remain, float negative zero normalizes, nonfinite floats
  fail, and timestamps are UTC microseconds in years 0001 through 9999. SHA-256
  hashes use versioned canonical bytes, independent of batch chunking.
- A permanent collection-scoped operation binding commits before file work.
  Its exact base includes store/collection/table identities, metadata location,
  optional snapshot, schema and partition-spec IDs. Retrying identical content
  returns the original terminal result before evaluating a new precondition,
  reserving owned input or admitting another operation. Healthy terminal lookups
  query only the requested catalog record, reusing the verified generation from
  open/reconciliation and confirmed publications. They neither enumerate retained
  operations nor scrub warehouse files. Observed corruption or uncertain
  publication closes this path until authoritative recovery and its durability
  fence succeed; a write-only I/O latch still permits old receipts. Different content
  conflicts: the public API rejects that request as `DefinitelyNotCommitted`
  with `OPERATION_CONFLICT`, which describes only the conflicting request, not
  the bound operation (which may be committed or unknown). Outcome JSON
  therefore emits it as a plain HTTP 409 error envelope without `outcome`.
  Arrow input errors use `FixInput`. A bound operation that fails
  with `RESOURCE_EXHAUSTED`, `CORRUPTION` or `UNSUPPORTED_STORE_VERSION` keeps
  its permanent failed outcome with `OperatorAction`; other bound failures use
  `NewOperationId`.
- Publication owns `BEGIN IMMEDIATE` through final COMMIT. Inner Iceberg
  transactions join that scope. A guard runs before the single INSERT/CAS;
  registry/pointer, snapshot identity and terminal receipt commit atomically.
  Failed inner transactions poison the outer scope even if their error is
  ignored. Ordinary failure records a permanent failed outcome. Bounded error
  diagnostics retain a valid UTF-8 prefix of at most 2048 bytes, so truncation
  cannot make a failure outcome unencodable or block live reconciliation.
- Detected corruption, from any thread, closes readiness and advances a health
  generation. A write admitted under an older generation does not publish:
  the final health decision runs inside the catalog guard while a commit gate
  is held through the outer COMMIT, so a concurrent corruption report either
  prevents that publication or is surfaced after it completes.
- After the outer pointer/receipt transaction resolves, `PublicationFinished`
  hands its disposition and exact candidate root to the backend. Success promotes
  that candidate; rollback discards it; uncertainty invalidates the verified
  generation until reconciliation. Storage admission/preparation cannot promote
  candidates by observing a later pointer or reload a changed prepared base.
- An uncertain binding/publication/failure-recording result closes readiness.
  Reconciliation closes the old handle, reopens authoritative SQLite state,
  validates retained storage in both directions, and commits a real fence
  increment before resolving unfinished bindings. It never replays payloads.
  Missing/corrupt committed state fails closed.
- The pending-call budget counts every admitted append, including the call
  that holds the writer; that call is counted when it acquires the writer,
  before its outcome lookup, so it is never refused definitely while a queued
  call with the same ID can still publish.
- Writer timeout or pending-call exhaustion first attempts a nonblocking lookup
  of an authoritative terminal outcome. It never reads through a tentative
  catalog transaction. If the lookup cannot resolve the original operation,
  append returns `commit_unknown`, not a new definite failure. Absence without
  writer ownership cannot exclude a concurrent call with the same ID.
- An unresolved lookup uses `recovery.lookup_blocked=true`. Its base is the last
  confirmed collection view and its attempt UUID identifies the lookup call;
  that call binds/publishes nothing and does not replace the original recovery
  target. Resource pressure alone does not close readiness. A global recovery
  barrier, including one raised while a retry waits, also uses this response.
- Shutdown closes admission without declaring earlier operations uncommitted.
  Queued retries and calls on a closing/closed engine return an unknown lookup
  outcome and retain any matching original recovery context. Reopen the store
  after releasing the closed engine to resolve the original ID; do not retry
  under a new ID because admission closed.
- An uncertain collection creation retains its complete outcome and context
  until reconciliation succeeds. An identical name/UUID/schema retry returns
  that outcome; conflicting identity/schema fails without erasing it. Unknown
  creation outcomes, including verifier exceptions and writer timeouts, use
  `SameIdAfterReconcile`.

Candidate bounds are eight collections, 4096 permanent operations, 256 MiB of
catalog pages, 16 pending calls and 128 MiB aggregate owned input. Per append:
10,000 rows, 16 MiB canonical input, 2 MiB per row, 1 MiB per string and 1024
bytes per key. Writer wait is at most five seconds; append deadline at most
60 seconds. Configuration may lower these bounds. These are admission limits,
not measured simultaneous RSS or full-storage capacity guarantees. All limits,
including the catalog limits, are validated before `CreateStore` creates the
root, so a rejected configuration leaves the root and store UUID reusable.

See the [shared resource budget contract](RESOURCE_BUDGETS.md) for the limit
matrix, reservation/release ownership, retained-buffer lifetimes and protected
completion/recovery requirements. The owned-input estimate does not cover HTTP
decoding, all writer allocations or scan buffers; the current checks are
store-local rather than a shared process-memory governor.

## Storage backend integration

`core/engine/storage_backend.h` is the private integration seam, injected by
`EngineFactory`. Implement `PrepareCreate`, `PrepareAppend` and `VerifyRetained`.
Preparation supplies the actual generated table UUID or complete append receipt;
`Publish` must use the supplied catalog and exact bound base with Iceberg retries
disabled and metadata cleanup disabled. Every new file class must use the same
durable FileIO. Do not substitute the test backend or Iceberg's plain local
FileIO in production.

The staged closure guard must verify the final root and all reachable new
files, durable receipts and sticky errors, reused-base membership and candidate
reopen limits before pointer mutation. `VerifyRetained` must independently match
every committed operation to exactly one retained snapshot/tag and vice versa;
Bound/Failed operations must have none. Validate UUID, schema, ancestry and file
closure. Catalog checks alone cannot establish these storage properties.

## SDK/UI compatibility

Outcome JSON in `apps/transport/outcome_json.*` keeps transport dependencies outside
the public API. All int64 quantities are decimal strings; version numbers are
integers. Append replies carry complete receipt/recovery fields. Unknown is
HTTP 503 with `SameIdAfterReconcile`. Create success is a direct collection
object, including `current_snapshot_id`, ordered `field_id` values and append
mode, matching the SDK/UI contract in
[PR #4](https://github.com/glamourdb/glamdb/pull/4). An `OPERATION_CONFLICT`
append is the plain error envelope `{"error":{...}}` with HTTP 409, as in the
mock contract; clients raise it instead of returning a terminal outcome.

Recommended client adjustments:

- UI create should parse outcome envelopes on error HTTP statuses
  (`allowOutcome: true`) and handle authoritative `definitely_not_committed`
  before status-based recovery. Otherwise a definite 503 failure freezes the
  create UUID/schema as an unresolved request.
- SDK unknown handling should require and validate a complete consistent
  recovery object and error before treating it as a resumable outcome. Retain
  optional extension fields such as `lookup_blocked`.
- Future file listings need a stable `file_id`. Snapshot ordering/pagination
  must use ancestry or sequence, since snapshot IDs are opaque integers.

The immutable reviewed client head is
`ee7da3993d10144cacaa4ad5c5cb629d6db11b0d`. Native JSON fixtures passed six
checks through that unmodified SDK with HTTPX mock transport. This tests wire
shapes and precision; socket-based SDK/UI integration awaits an HTTP adapter.

## Validation evidence

The coordinator baseline, before combining the storage primitives, was recorded
on Linux x86-64 with Clang 18.1.3/libc++ 18.1.3, Arrow/Parquet 25.0.0,
Iceberg 0.4.0 (`64dc66f159bd62c9817a96d74cbce767268934d0`) and locked SQLite
3.54.1, using disposable stores:

| Executed check | Result |
| --- | --- |
| Normal build + `ctest --test-dir build/coordinator --output-on-failure` | 66/66 PASS |
| Release build + `ctest --test-dir build/coordinator-release --output-on-failure` | 66/66 PASS |
| Debug ASan/UBSan/LeakSanitizer + CTest | 65/65 PASS; installed consumer tested separately above |
| Unmodified SDK parsing native outcome JSON through mock transport | 6/6 PASS |
| LLVM 18 formatting / analyzer | 36 files / 20 translation units PASS |
| Formatting and analyzer negative controls | Both deliberate violations rejected |

The analyzer checked translation units in parallel with the repository's
unchanged checks, header filter, compile database and warnings-as-errors flags.
Corrected units were rechecked with the same flags.

The sanitizer run enabled leak detection, allocation/deallocation mismatch
checks and halt-on-error, with no diagnostic suppression. It used static
libc++/libc++abi/libunwind because the packaged dynamic exception runtime also
failed a standalone ASan throw/catch control. Independent negative controls
still detected a deliberate mismatch, use-after-free and leak.

Use [BUILDING.md](BUILDING.md) for build, CTest and quality commands. Tests cover
literal canonical hash vectors; A/B/retry-A with exact recovered receipts;
permanent failures; exact metadata-only bases; nested transaction rollback;
lost COMMIT and fence results through a real SQLite VFS; global-barrier races;
writer/pending/input pressure without changing old outcomes; tentative-catalog
lookup isolation; retained uncertain-create contexts; incremental SHA-256
padding boundaries and all-type Arrow fingerprint goldens; shutdown during
publication with queued retries; bounded multibyte diagnostics and live recovery
after losing failure-recording results; fresh-process
operation recovery, hot rollback journals and store leases; and the installed
public package.

`glamdb.real_sqlcatalog` additionally uses real Iceberg `StageCreateTable`,
`Transaction::Commit` and `UpdateProperties` with completed JSON roots. It
observes close-before-guard, tentative pointer isolation on a second SQLite
connection, outer rollback, metadata-only stale-base rejection and a separate
process reopening/loading the retained pointer. Its property-only transaction
is deliberately rolled back before receipt insertion; a synthetic append
receipt is never committed. The VFS tests separately exercise terminal receipt
publication. The local test FileIO proves ordering, not file synchronization.

With storage enabled, `glamdb.durable_storage_catalog` also exercises the real
durable FileIO and guard inside this catalog's publication transaction, followed
by catalog-selected recovery and receipt/row checks in a fresh process. See the
[storage guide](STORAGE_MODULE.md#tests-and-evidence) for its cases and boundaries.

Independent catalog, coordinator and SDK reviews exercised these boundaries;
their confirmed correctness defects have regression tests. Production
coordinator-to-storage appends and scans are exercised in-process by
`glamdb_vertical` and in fresh processes by `glamdb.scan_process` (see
[SCANNER.md](SCANNER.md#validation-status)). `BackendPublicationTest` exercises
explicit promotion, rollback discard, uncertain-result invalidation and
metadata-only base changes over the real backend. `BackendPublicationIntegration`
uses the real SQLite journal fault VFS with Parquet/Iceberg files: scans during
the tentative transaction and retained cursors after a lost COMMIT result keep
the confirmed snapshot; new scans block until reconciliation. This injected
result loss does not simulate loss of the OS page cache. Restore, combined capacity/RSS and
OS-cache-loss/power-cut qualification remain NOT RUN.

## Public health observation

`Engine::GetHealth()` returns a point-in-time `StoreHealth` without catalog I/O
or waiting for the writer. Recovery and corruption fences close new reads and
writes. A local durable-I/O latch separately closes writes while confirmed reads
remain available; `Reconcile()` does not clear it. Shutdown takes precedence over
recovery in the reported state, including after a shutdown timeout. Closed
engines remain observable. A missing storage backend advertises neither scans
nor writes. The storage seam explicitly reports reader availability, so the
coordinator test backend does not claim to support scans.

The coordinator checks backend write health after resolving terminal retries,
before staging a new collection or binding a new append, and again at the final
publication decision inside the commit gate. An unavailable writer refuses new
work without consuming an operation slot. A latched failure returns `IO_ERROR`
with operator action required; it leaves confirmed reads, old receipts and
reconciliation available, but only reopening can replace that backend instance.

The [HTTP contract](HTTP_SERVER.md) defines the state table, lifecycle overlays,
and 200/503 readiness mapping. Every health snapshot remains durability-unqualified;
backend availability and successful recovery are not qualification evidence.
