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
  SQLite `COMMIT` via `ConfirmPublication` (or a later binding/recovery
  observes the committed root). Rollback discards the tentative candidate.
- `VerifyRetained` runs the full `VerifyReachable` closure check for every
  collection root (manifest lists, manifests, data file lengths and Parquet
  footers), its schema, a one-to-one mapping between retained snapshots and
  committed receipts, and the aggregate multi-collection store usage against
  `TStorageLimits::Store`. It runs on open, `Reconcile()` and every idempotent
  retry of a committed create or append, so a retry never returns a committed
  outcome over missing or corrupt files. Missing or inconsistent committed
  state is `CORRUPTION`, which closes readiness.
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
usage increment of one append. Exactly one Arrow batch is built per append.
Limits that only the produced files reveal (`MaxFileBytes`, candidate root
size, aggregate usage) fail after binding as `RESOURCE_EXHAUSTED` with
`OperatorAction`; the ID stays permanently failed. The default storage limits
require free-space reserves (see `TStorageLimits`). Orphan files from failed
attempts remain.

A failed durable write sets the backend's write latch for this engine
instance. It blocks further writes with `IO_ERROR`/`OperatorAction` before
binding; verification, scans, `Reconcile()` and `Shutdown()` remain available.
Shut down and reopen the store to write again.

### Error classification

Storage failures map to engine codes as follows. Read paths only use locations
taken from committed metadata, so malformed or missing persisted state there
is corruption. Every `CORRUPTION` closes readiness.

| Source | Condition | Engine code |
| --- | --- | --- |
| `state_not_recoverable`, ELOOP (symlink with `O_NOFOLLOW`), `not_a_directory`, `cross_device_link`, `no_such_file_or_directory` | any store path | `CORRUPTION` |
| `invalid_argument` | warehouse preflight (symlink or special file) or opening a committed file that is not regular | `CORRUPTION` |
| `file_too_large` | reading a committed file / write budget | `CORRUPTION` / `RESOURCE_EXHAUSTED` |
| `no_space_on_device`, `value_too_large`, `not_enough_memory` | any | `RESOURCE_EXHAUSTED` |
| write latch, `io_error`, `read_only_file_system`, `permission_denied`, other | any | `IO_ERROR` |
| Iceberg `kInvalid`, `kInvalidArgument`, `kNotFound`, `kValidationFailed`, `kDecompressError`, `kUnknownError`, `kInvalidManifest(List)`, `kInvalidSchema`, `kJsonParseError`; decoder exceptions | read paths | `CORRUPTION` |
| message prefix `CORRUPTION: ` | any | `CORRUPTION` |
| message prefix `CAPACITY: ` | write paths / read paths | `RESOURCE_EXHAUSTED` / `CORRUPTION` |

An `invalid_argument` from a `statfs` anomaly in the preflight is not
distinguishable from a planted special file and also fails closed.

## Scan path

`Engine::Scan` validates the collection, resolves the confirmed SQLite view
once, and opens a cursor through the backend's read-only FileIO view; it never
joins a writer attempt or holds a SQLite read transaction.

- `SnapshotSelector`: `Latest` (resolved at open), `Id`, `InitialEmpty`.
  `ScanInfo.selector_resolved` reports `Id(snapshot)` or `InitialEmpty`.
- Planning reads the committed root, the selected manifest list and live
  manifest entries. Warehouse directories are never listed. Delete manifests,
  partitioned specs and non-Parquet files are `UNSUPPORTED_TABLE_FEATURE`.
- `Predicate` supports `True` and one comparison (`Eq`, `Ne`, `Lt`, `Le`, `Gt`,
  `Ge`) of a field with a literal of exactly the field type. A NULL cell is
  UNKNOWN and never returned, which equals SQL three-valued logic for a single
  comparison. Strings compare by unsigned UTF-8 bytes; literal `-0.0` matches
  stored `0.0`; NaN/Inf and out-of-range timestamps are `INVALID_ARGUMENT`.
- Each file is read whole (at most 32 MiB in this profile) and its footer is
  checked by the same verifier as recovery: exact field IDs 1..N, names,
  repetition, physical/logical types, row group counts and the manifest row
  count. Before any value is returned, every decoded column is validated:
  Arrow `ValidateFull` (offsets and UTF-8), string limits (1024 bytes for
  `key`, 1 MiB otherwise), finite non-negative-zero floats and timestamps in
  years 0001..9999. Then rows are filtered, projected and limited. Batches have at most
  `batch_rows` rows and the exact `Info().output_schema`. `limit = 0` and empty
  views return `nullptr` from the first `Next()`.
- Both the cursor and returned Arrow buffers retain the store lease until the
  last dependent handle (including array slices and C Data exports) is
  released. `Cancel()` or `Engine::Shutdown()` makes `Next()` return
  `CANCELLED`; `Info()` remains valid. Detected corruption during `Next()`
  closes engine readiness. Read failures are errors, never EOS.

Not implemented: file/row-group pruning, compound predicates (`And`, `Or`,
`Not`, `In`, `IsNull`), streaming planning, runtime memory leases and quotas,
page CRC verification, per-device physical/inode budgets and in-flight
reservations, and measured attempt-volume/capacity qualification.

## Validation status

| Check | Status |
| --- | --- |
| `cmake --build --preset local` (Clang 18.1.3, `GLAMDB_BUILD_STORAGE=ON`) and `format-check`, `quality-negative-check` | PASS |
| `ctest --preset local` (unit, mock, vertical, fault, integration including `glamdb.scan_process`, installed consumer, tooling, smoke) | PASS, see the PR description for counts; the standalone `RecoveryChildProcess` helper is skipped as expected |
| `glamdb_vertical` (`engine_vertical_test.cpp`, `engine_integrity_test.cpp`) | PASS: commit-gate boundaries, layout violations, retry verification, crafted same-length Parquet corruption, quotas, write latch |
| `glamdb_mock` (label `mock`, in-memory FileIO) | PASS, also from a read-only working/temporary directory |
| Separate `GLAMDB_BUILD_STORAGE=OFF` build and CTest | PASS |
| Full-repo `tidy-check`, sanitizer presets, TSan, power-loss qualification | NOT RUN in this change |

Value validation reads only decoded columns of the projection and predicate;
latent corruption in unread columns or unread data pages is not detected by
`VerifyReachable`, which checks footers rather than every page.
