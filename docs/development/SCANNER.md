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
- Omitted projection selects every field; an empty projection selects no
  columns while preserving matching row cardinality, duplicates and limits.
  Filter-only fields remain hidden. `ScanInfo` fixes the output schema and view
  before `Next()`, including empty views and zero limits. Without projected or
  filter fields, verified Parquet row-group counts supply batch cardinality.
- Native scans pin the confirmed generation's [verified membership index](SCAN_INDEX.md).
  Opening and repeated planning read no root, manifest list or manifest, and each
  indexed `TScanDataFile` retains its verified `iceberg::DataFile` descriptor so
  manifest-level file pruning also performs zero metadata I/O.
  `limit = 0` opens no data file. Recovery and publication derive the index
  through the same strict closure verifier; observed corruption invalidates it.
  Custom backends without an index retain the non-indexed planner and lazy manifest
  reads. Uncached planning, metadata inspection (`GetStatistics`, `ListFiles`,
  `ExplainScan`) and Parquet footer reads share a bounded per-store
  `TSnapshotMetadataCache` (`32 MiB` under `ResourceClass::Metadata`, disabled when
  `ReadCacheBytes = 0`) keyed by immutable logical store paths and evicted/cleared
  on `RetireRoot`, `VerifyRetained`, observed corruption and shutdown.
  Warehouse directories are never listed for membership. Delete manifests,
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
- Before opening a candidate Parquet file, conservative SQL three-valued zone
  pruning (`TZoneTruth` in `core/scan/pruning.h`) evaluates the predicate over
  manifest column bounds and null/value counts (`ExtractFileZoneStats`). Only
  zones proven `AlwaysFalse` or `AlwaysUnknown` at the predicate root are
  pruned (`CanPruneZone`); Iceberg 16-byte string upper-bound truncation ties are
  treated conservatively (`UpperExact = false`, so `Ne` never falsely prunes
  a truncated tie). Statistics for every field in `ReadFields_` (`projection ∪
  predicate`) are domain-checked first (inconsistent counts, inverted bounds,
  NaN/Inf floats, `-0.0` normalization, out-of-range timestamps, `key > 1024` or
  `string > 1 MiB`); malformed, incomplete or contradictory statistics on a
  column are conservatively discarded (`prune nothing`) so exact column decoding
  and `ValidateColumn` inspect the data rather than failing or skipping rows at
  the pruning stage. Non-`key` manifest string upper bounds with `>= 16` UTF-8
  code points (`truncate(16)`) are also omitted at the manifest level while exact
  Parquet row-group statistics still prune, and `Float64` bounds require a
  confirmed zero manifest `nan_value_counts` entry.
- Surviving files open through positional `TScanRandomAccessFile` (`pread` over
  `IReadOnlyFileIO`, recording exact `BytesRead` and validating `Close()`,
  including when `limit` completes a scan inside an open file; only cancellation
  and already-failed scans close best-effort),
  eliminating whole-file 32 MiB buffers. On a footer cache miss,
  `ReadParquetFooterFromStream` reads only the magic/tail/footer ranges and
  verifies the footer with the same rules as recovery: unencrypted metadata,
  exact field IDs 1..N, names, repetition, physical/logical types, row-group
  counts and the manifest row count. The verified `parquet::FileMetaData` is
  handed directly to `parquet::ParquetFileReader::Open` (single-pass footer
  verification) and inserted into `TSnapshotMetadataCache`.
- After footer verification, `ExtractRowGroupZoneStats` applies the same
  domain-checked conservative SQL-3VL zone evaluator per row group. If all row
  groups in a file are pruned, no column chunks are read. For surviving row
  groups, `ReadFields_` (`projection ∪ predicate`) are deduplicated and read
  positionally via `ReadTable(survivingRowGroups, readColumns)` (when
  `ReadFields_` is empty on an unfiltered zero-column scan, empty-column batches
  are synthesized directly from verified row-group row counts without reading
  column pages). Before `SelectRows` evaluates the predicate or any
  value is returned, every decoded projection and filter-only column is
  validated: Arrow `ValidateFull` (offsets and UTF-8), string limits (1024 bytes
  for `key`, 1 MiB otherwise), finite non-negative-zero floats and timestamps in
  years 0001..9999. Then rows are filtered in bounded strips, projected (dropping
  non-projected filter columns) and limited. Batches have at most `batch_rows`
  rows and the exact `Info().output_schema`. `limit = 0` and empty views return
  `nullptr` from the first `Next()`.
- The cursor and returned Arrow backing buffers retain the engine (including
  the store lock), read-only FileIO and owned Arrow allocator/accounting pool
  until the last batch, extracted array/buffer, slice or C Data export is
  released. Decoding and filtered copies use the same per-scan resource pool,
  which charges the shared data budget and retains any supplied upstream pool.
  This does not bound total process memory. `Cancel()` or `Engine::Shutdown()`
  makes `Next()` return `CANCELLED`; checks after I/O prevent further file reads, and checks before
  delivery cover batches and EOS, including empty views and `limit = 0`.
  `Info()` remains valid. Detected corruption during `Next()`
  closes engine readiness. Read failures are errors, never EOS.

Explicit manifest, file-source/footer and output-descriptor reservation refusal
returns `RESOURCE_EXHAUSTED` with `SameIdAfterReconcile` before consuming the
next batch. That cursor remains open: release pressure and retry it. A file
descriptor remains queued until `OpenFile` succeeds. Actual decode, I/O and
allocator failures remain terminal. Native [HTTP page assembly](SCAN_SESSIONS.md)
keeps rollback ownership of fetched batches. Explicit admission refusal after
collecting rows finishes a nonempty page with a continuation, freeing retained
descriptors before decoding resumes. A refusal before the first row preserves
the fixed snapshot, cursor tokens and previous replay. Pressure never means EOS.

Not implemented: Parquet page-index/Bloom pruning, query-result/decoded-array caching, page CRC verification and qualification-scale attempt-volume/capacity measurements.
The [process resource owner](SHARED_RESOURCES.md) now supplies scan working
reservations, separately charged output descriptors, tracked Arrow allocation capacity and leases retained through
slices and nonempty C Data exports. Zero-column slices retain their output schema lease.
Uncached plans and queued manifest descriptors keep reservations for their whole
lifetime; cursor control admission shares the ordinary control budget. Profiles
separate filtering, limit/cancellation stops and pruning, and expose live/peak
Arrow allocations and output metadata reservations. Compiled compound predicates retain metadata
reservations sized before literal copying; explanation predicate subtrees keep
their result lease when extracted. Reader, writer, metadata, recovery and device
reservations share the same parent; their ownership is listed in the
[budget contract](RESOURCE_BUDGETS.md). The
[validation report](SHARED_RESOURCES_VALIDATION.md) records saturation, lifetime
sanitizers and a small multi-database HTTP/reopen campaign.

## Validation status

These checks were run locally after integration with official main `d61aeaa`.
The [resource validation report](SHARED_RESOURCES_VALIDATION.md) records exact
commands, counts and the remaining qualification limits.

| Check | Status |
| --- | --- |
| `cmake --build build/local` (Clang 18.1.3, `GLAMDB_BUILD_STORAGE=ON`) and `format-check`, `quality-negative-check` | PASS |
| Full native CTest suite (unit, mock, vertical, fault, integration including `glamdb.scan_process`, installed consumer, tooling, smoke) | PASS: 674 passed, three standalone helper skips, one existing disabled benchmark |
| `glamdb_vertical` (`engine_vertical_test.cpp`, `engine_integrity_test.cpp`) | PASS: commit-gate boundaries, layout violations, retry verification, crafted same-length Parquet corruption (including filter-only columns), compound 3VL predicates across multiple Parquet files, quotas, write latch |
| `glamdb_mock` (label `mock`, in-memory FileIO) | PASS, including compound 3VL truth tables, De Morgan laws with `NULL`, multi-column filter-only validation, and read-only working/temporary directory |
| Separate `GLAMDB_BUILD_STORAGE=OFF` build and CTest | PASS: 332 passed, one existing disabled benchmark |
| Fixed-view pages and shared metadata admission | PASS in normal and ASan/UBSan/LSan builds: retained page copies after close, replay/continuation under pressure and expiry reclamation |
| Shared resource tests under ASan/UBSan/LSan | PASS; see the [resource validation report](SHARED_RESOURCES_VALIDATION.md) |
| `TScanLifetimeTest` (real files and production backend) | PASS in normal and ASan/UBSan/LSan builds: batch/array/buffer/slice/C Data retention, final-release reopen, deterministic cancel/shutdown barriers before batch/EOS, empty and zero-limit views |
| `ScanMockTest` (in-memory FileIO, real Arrow/Parquet) | PASS in normal and ASan/UBSan/LSan builds: extracted-buffer FileIO/owner retention, decoded/filtered Arrow pool accounting and compound predicates |
| Full-repo `tidy-check`, full sanitizer presets, TSan, power-loss qualification | NOT RUN in this change |


Zero-column projection was revalidated after integration with official main
`9b4d30a`: the local Debug build and full CTest suite passed (695 passed, three
child-process helper skips and one existing disabled benchmark). The full Python
suite passed 716 tests, and the workbench unit suite passed 131. Native HTTP
checks include empty-output JSON/Arrow pagination and replay, plus `/explain`
wire-schema validation for initial, historical and latest views with hidden
predicate fields. The explain regression failed on the nonempty-schema constraint
before the contract fix and passed afterward. Sanitizer profiles and browser E2E were not rerun for this integration.

Positional Parquet I/O, conservative SQL-3VL manifest/row-group pruning and
`TSnapshotMetadataCache` were validated after rebasing onto official main
`47cc955`: `ScanPruningUnitTest` (exhaustive SQL-3VL zone truth tables against
the row oracle, truncated-string `Ne` and `truncate(16)` upper-bound safety,
`Float64` `nan_value_counts` gating, and conservative `prune nothing` fallback on
malformed/contradictory manifest and Parquet statistics),
`SnapshotMetadataCacheUnitTest` and `SnapshotMetadataCacheGlobalOomTest`
(per-kind/byte LRU caps, oversize bypass, zero-allocation lookups, strong
exception safety under `std::bad_alloc`, eviction and thread safety),
`ScanPruningVerticalTest` (differential parity with `EnablePruning = false` across
600+ generated 3VL predicates, multi-row-group `> 4096`-byte string manifest-bound
omission, on-disk corrupt footer statistics `prune nothing`, zero Parquet opens on
manifest-pruned files, byte-exact positional `PRead` accounting, warm metadata/footer
cache hits, and `ENOMEM` column-read classification), `ScanReadErrorClassificationTest`
(including checked close on `limit` completion vs best-effort close on `Cancel()`),
`glamdb_scan_data_reads_bench`, `glamdb.installed_consumer`, the
`GLAMDB_BUILD_STORAGE=OFF` contour, and `NativeMetadataHTTP` wire-schema checks
passed locally with `format-check` (`tidy-check` and sanitizer presets were not
run in this change).

Value validation reads only decoded columns of the projection and predicate;
latent corruption in unread columns or unread data pages is not detected by
`VerifyReachable`, which checks footers rather than every page.

## Selected-view metadata integration

`Engine::ListFiles`/`GetFileDetails` reuse committed manifest planning and confirmed-root
leases. Inventory never lists warehouse directories; details validate actual
immutable Parquet bytes, field IDs, typed footer statistics and row groups.
Manifest and row-group bounds enable conservative file and row-group pruning;
page-index and Bloom presence remain observational only (page/Bloom pruning is
not implemented). HTTP metadata, SDK
and workbench use the same facade. The [installed system recipe](NATIVE_ACCEPTANCE.md)
checks latest=1,010, historical A=1,000, initial=0, JSON/Arrow/EOS and retained
batches, then repeats them after restart and source-independent restore.
