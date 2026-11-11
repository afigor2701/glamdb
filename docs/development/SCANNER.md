# Storage backend and scanner MVP

`core/backend/` connects the coordinator seam in `core/engine/storage_backend.h`
to the [durable storage primitives](STORAGE_MODULE.md). `core/scan/` implements
the public `Engine::Scan` declared in `public/storage/scan.h`. Both are built
when `GLAMDB_BUILD_STORAGE=ON`, the default unless `GLAMDB_IO_ONLY=ON`. Without
them the public factories install no backend and collection creation, appends
and scans report `DEPENDENCY_UNAVAILABLE`.

> [!IMPORTANT]
> The bridge and scanner are built and tested against the pinned Arrow, Parquet,
> Iceberg, and SQLite dependencies (`glamdb_unit` and `glamdb_vertical`).
> Full-engine sanitizer runs and power-loss qualification remain separate steps.
> See [Validation status](#validation-status).

## Write path

`CreateStore`/`OpenExisting` construct `TLocalStorageBackend` after the store
lease has created or validated `root/` and `warehouse/`
(`EngineFactory::CreateWith/OpenWith`). The internal factories that take a
ready backend remain for coordinator tests.

The installed consumer uses these public factories and only installed headers
and CMake targets. Its independent write/reopen processes append A/B, compare
latest/A/initial views with literal duplicate/NULL rows, and verify every
persisted receipt field and idempotent retry. The runner passes the build's
storage expectation explicitly: a missing production backend cannot pass the
storage-enabled contour. A storage-disabled package must report
`DEPENDENCY_UNAVAILABLE` for create, append and scan before and after reopen;
append checks availability before collection lookup or operation binding.
This real-file baseline is separate from the in-memory mock scan suite and
does not qualify physical power-loss durability.

- **Create:** `MakeTableProfile` maps the fixed `SchemaSpec` (field IDs 1..N).
  The backend creates the collection directories, binds a `CreateCollection`
  attempt, writes `metadata/00000-<attempt>.metadata.json` through the durable
  FileIO and returns the Iceberg table UUID. `Publish` inserts the
  `columnar.<exact name>` pointer; the catalog guard runs
  `TStoragePublicationGuard` before that insert.
- **Append:** normalized coordinator rows are converted into one owned Arrow
  batch, so stored values are exactly the hashed canonical content. The backend
  writes `data/<operation>-<attempt>-00000.parquet`, stages a fast append with
  summary keys `glamdb.operation-id` and `glamdb.content-hash`, and writes the
  next numbered root. `Publish` is one CAS from the exact prepared root. The
  receipt carries the staged snapshot ID, parent, row count, one data file, its
  size and the snapshot timestamp in microseconds.
- `iceberg::Table` requires a catalog; the backend passes `TInertCatalog`,
  whose every operation fails, so no Iceberg commit can bypass the coordinator.
- Guard candidates are tentative until the coordinator confirms the outer
  SQLite pointer/receipt `COMMIT` via `PublicationFinished(Committed)`.
  `DefinitelyNotCommitted` discards the candidate without changing the confirmed
  base; `CommitUnknown` invalidates the verified generation. A missing or
  mismatched success candidate also invalidates it. Admission and preparation
  accept only the exact explicitly confirmed base, including its metadata
  location; they never promote a tentative root or silently reload another base.
  `VerifyRetained` rebuilds the generation after reconciliation.
- `VerifyRetained` runs the full `VerifyReachable` closure check for every
  collection root (manifest lists, manifests, data file lengths and Parquet
  footers), its schema, a one-to-one mapping between retained snapshots and
  committed receipts, and the aggregate multi-collection store usage against
  `TStorageLimits::Store`. It runs on open, `Reconcile()` and every identical
  create retry. Healthy append retries and `FindAppendOutcome` use direct catalog
  lookups within the verified generation, without enumerating operation history
  or rereading warehouse files. Confirmed publications preserve that generation;
  observed corruption and uncertain publication invalidate it. Receipt lookup
  does not scrub for unobserved file damage. A scan or explicit `Reconcile()`
  detects damage; missing or inconsistent committed state is `CORRUPTION` and
  closes readiness. Full recovery verification and a successful durability fence
  are required before terminal lookups resume. A durable write latch alone still
  permits old receipt lookup.
- After a committed append the backend removes the superseded root
  (`RetireRoot`). The engine defers it while a scan that resolved that root is
  still planning; a root that cannot be removed remains an orphan. Removal
  runs only on the writer (after the next committed append, or at shutdown):
  capacity preflight walks the warehouse on the writer, and a root unlinked
  mid-walk would fail that walk as corruption. A finishing scan only ends its
  lease.
- Corruption reports and publication are ordered by the engine's commit gate.
  The writer takes it immediately before the guard's final health decision and
  holds it through the outer `COMMIT`; a reader that detects corruption takes
  it before closing readiness. Corruption observed before that decision rolls
  the publication back; corruption observed after it is surfaced only once the
  publication has completed.

`CheckAppendAdmissible` runs before the coordinator binds the operation in
SQLite, so a rejection consumes no operation ID and writes no file. It checks
writer row/byte limits (1..10000 rows, 2 MiB per row including fixed slot
headers for `NULL` cells, 16 MiB Arrow buffers, estimated without building a
batch), the warehouse preflight, the exact base root against
`TStorageLimits::RootBytes` and `MetadataLogEntries`, and the minimum store
usage increment of one append. After these checks it allocates exactly one
normalized Arrow batch before binding and retains it in the call-owned input;
`PrepareAppend` reuses that batch. Allocation refusal is `RESOURCE_EXHAUSTED`
with `OperatorAction`, consumes no ID and releases partial buffers. The owned-input
reservation remains charged until the call and its batch finish.
Limits that only the produced files reveal (`MaxFileBytes`, candidate root
size, aggregate usage) fail after binding as `RESOURCE_EXHAUSTED` with
`OperatorAction`; the ID stays permanently failed. The default storage limits
require free-space reserves (see `TStorageLimits`). Orphan files from failed
attempts remain.

A failed durable write sets the backend's write latch for this engine
instance, as does identity evidence from the store-root probe (a replaced,
missing or redirected root). Transient resource errors of that probe, such as
descriptor or memory exhaustion, fail only the current call. The latch blocks
further writes with `IO_ERROR`/`OperatorAction` before binding; verification,
scans, `Reconcile()` and `Shutdown()` remain available. Shut down and reopen
the store to write again.

### Error classification

Storage failures map to engine codes as follows. Read paths only use locations
taken from committed metadata, so malformed or missing persisted state there
is corruption; environmental read failures (I/O errors, descriptor or memory
exhaustion) keep their own codes and do not close readiness. Manifest lists
and manifests are read whole through the FileIO before Avro decoding, so a
read error cannot surface as a truncated, apparently malformed file. Every
`CORRUPTION` closes readiness.

| Source | Condition | Engine code |
| --- | --- | --- |
| `state_not_recoverable`, ELOOP (symlink with `O_NOFOLLOW`), `not_a_directory`, `cross_device_link`, `no_such_file_or_directory` | any store path | `CORRUPTION` |
| `invalid_argument` | warehouse preflight (symlink or special file) or opening a committed file that is not regular | `CORRUPTION` |
| `file_too_large` | reading a committed file / write budget | `CORRUPTION` / `RESOURCE_EXHAUSTED` |
| `no_space_on_device`, `value_too_large`, `not_enough_memory` | any | `RESOURCE_EXHAUSTED` |
| write latch, `io_error`, `read_only_file_system`, `permission_denied`, other | any | `IO_ERROR` |
| Iceberg `kIOError` without a prefix | read paths | `IO_ERROR` |
| Iceberg `kInvalid`, `kInvalidArgument`, `kNotFound`, `kValidationFailed`, `kDecompressError`, `kUnknownError`, `kInvalidManifest(List)`, `kInvalidSchema`, `kJsonParseError`; decoder exceptions | read paths | `CORRUPTION` |
| `std::bad_alloc`, Arrow `OutOfMemory`, message prefix `RESOURCE: ` (including a full recovery workspace) | any | `RESOURCE_EXHAUSTED` |
| message prefix `CORRUPTION: ` | any | `CORRUPTION` |
| message prefix `CAPACITY: ` | write paths / read paths | `RESOURCE_EXHAUSTED` / `CORRUPTION` |

An `invalid_argument` from a `statfs` anomaly in the preflight is not
distinguishable from a planted special file and also fails closed.

## Scan path

The facade also exposes capabilities, retained snapshot history, selected-view
statistics, snapshot-bound file/row-group details and scan explain. Cursor profiles
measure completed execution work.
See [public metadata](METADATA.md) for provenance, unknown counters and limits.

`Engine::Scan` validates the collection, resolves the confirmed SQLite view
once, and opens a cursor through the backend's read-only FileIO view; it never
joins a writer attempt or holds a SQLite read transaction.

- `SnapshotSelector`: `Latest` (resolved at open), `Id`, `InitialEmpty`.
  `ScanInfo.selector_resolved` reports `Id(snapshot)` or `InitialEmpty`.
- Opening a scan reads the committed root and the selected manifest list; the
  cursor reads each manifest's live entries only when it needs the next data
  file, so `limit = 0` reads no manifest and a small limit reads only the
  manifests it consumes. A malformed manifest therefore surfaces from
  `Next()` (still `CORRUPTION`, closing readiness). Warehouse directories are
  never listed. Delete manifests,
  partitioned specs and non-Parquet files are `UNSUPPORTED_TABLE_FEATURE`.
- `Predicate` supports `True`, comparisons (`Eq`, `Ne`, `Lt`, `Le`, `Gt`, `Ge`)
  of a field with a literal of the exact field type or an explicit `NULL`
  (`CompareNull`), `In` with an array of optional literals (`std::nullopt` for
  SQL `NULL`), `IsNull`, `IsNotNull`, and recursive `And`, `Or`, `Not` trees
  (depth `<= 64`, nodes `<= 1024`, literals `<= 4096`; empty `And`/`Or`
  children are `INVALID_ARGUMENT`). Evaluation uses full SQL three-valued logic
  (`False`, `True`, `Unknown`): a `NULL` cell or `NULL` comparison literal yields
  `Unknown`, `In` with an empty list yields `False` for non-`NULL` cells (and
  `Unknown` for `NULL` cells), `In` with `NULL` yields `True` on a match and
  `Unknown` on a non-match, `False AND Unknown == False`, `True OR Unknown ==
  True`, `NOT Unknown == Unknown`, and only `== True` at the root survives.
  Strings compare by unsigned UTF-8 bytes (`key` literals `<= 1024` bytes, other
  strings `<= 1 MiB`); literal `-0.0` normalizes to `0.0`; NaN/Inf and
  out-of-range timestamps are `INVALID_ARGUMENT`.
- Each file is read whole (at most 32 MiB in this profile) and its footer is
  checked by the same verifier as recovery: exact field IDs 1..N, names,
  repetition, physical/logical types, row group counts and the manifest row
  count. All columns referenced by `projection` and `predicate` are deduplicated
  and read from Parquet; before `SelectRows` evaluates the predicate or any
  value is returned, every decoded projection and filter-only column is
  validated: Arrow `ValidateFull` (offsets and UTF-8), string limits (1024 bytes
  for `key`, 1 MiB otherwise), finite non-negative-zero floats and timestamps in
  years 0001..9999. Then rows are filtered in bounded strips, projected (dropping
  non-projected filter columns) and limited. Batches have at most `batch_rows`
  rows and the exact `Info().output_schema`. `limit = 0` and empty views return
  `nullptr` from the first `Next()`.
- Both the cursor and returned Arrow buffers retain the store lease until the
  last dependent handle (including array slices and C Data exports) is
  released. `Cancel()` or `Engine::Shutdown()` makes `Next()` return
  `CANCELLED`; `Info()` remains valid. Detected corruption during `Next()`
  closes engine readiness. Read failures are errors, never EOS.

Not implemented: file/row-group pruning, reuse of the backend's parsed root by
scans, runtime memory leases and quotas,
page CRC verification, per-device physical/inode budgets and in-flight
reservations, and measured attempt-volume/capacity qualification.
The [shared resource budget contract](RESOURCE_BUDGETS.md) defines the required
memory accounting lifetime for retained buffers and the process/store ownership
of reader, writer, metadata and recovery reservations.

## Validation status

| Check | Status |
| --- | --- |
| `cmake --build --preset local` (Clang 18.1.3, `GLAMDB_BUILD_STORAGE=ON`) and `format-check` | PASS |
| `ctest --preset local` (unit `ScanPredicate.*`, mock `ScanMockTest.*`, vertical `EngineVerticalTest.*`, `glamdb.scan_process`, `glamdb.transport.*`, `glamdb.native_http`, `glamdb.http_review`, `glamdb.native_http_fault`, installed consumer) | PASS |
| `glamdb_vertical` (`engine_vertical_test.cpp`, `engine_integrity_test.cpp`) | PASS: commit-gate boundaries, layout violations, retry verification, crafted same-length Parquet corruption (including filter-only columns), compound 3VL predicates across multiple Parquet files, quotas, write latch |
| `glamdb_mock` (label `mock`, in-memory FileIO) | PASS, including compound 3VL truth tables, De Morgan laws with `NULL`, multi-column filter-only validation, and read-only working/temporary directory |
| Separate `GLAMDB_BUILD_STORAGE=OFF` build (`build/nostorage`) and CTest (`ScanPredicate.*`, `Transport.*`, `installed_consumer`, `EngineBootstrap.*`) | PASS |
| `tests/verification/test_oracle.py` against `glamdb_verify` | PASS |
| `tidy-check`, sanitizer presets, TSan, Python `pytest`/`httpx`/`pyarrow`/`jsonschema` client & schema runner, Node/Playwright workbench checks, power-loss qualification | NOT RUN in this environment |

Value validation reads only decoded columns of the projection and predicate;
latent corruption in unread columns or unread data pages is not detected by
`VerifyReachable`, which checks footers rather than every page.
