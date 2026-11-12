# Public metadata

`storage/engine.h` exposes native metadata through transport-independent DTOs
in `storage/metadata.h`. Adapters must use these methods instead of reading the
catalog or warehouse themselves. These APIs require `GLAMDB_BUILD_STORAGE=ON`;
disabled builds return `DEPENDENCY_UNAVAILABLE`.

| API | Result |
| --- | --- |
| `GetCapabilities()` | Backend identity and supported append, scan, metadata, file-pruning and row-group-pruning operations; no I/O or writer wait. Custom backends identify themselves explicitly. |
| `GetHealth()` | Current admission/recovery/shutdown state, separate from supported operations. |
| `ListSnapshots(collection)` | Retained snapshots, oldest first, joined with their original committed append receipts. Initial empty is not a snapshot. |
| `GetStatistics(collection, selector)` | Rows, live data-file count and compressed data-file bytes from the selected view's manifests. |
| `ListFiles(collection, selector)` | Stable opaque file IDs, lengths, row counts and field-ID metrics from the selected view's live manifest entries. |
| `ListCollectionsPage(request)` | Creation-ordered collections at one creation high-water mark. |
| `ListSnapshotsPage(collection, request)` | Pages of one retained append ancestry, oldest first. |
| `ListFilesPage(collection, request, selector)` | File-ID-ordered pages of one resolved view. |
| `GetFileDetails(collection, file_id, selector)` | Manifest descriptor and verified physical Parquet row-group/column metadata for a member of the selected view. |
| `ExplainScan(request)` | Validated projection, physical read fields, predicate, batch size, limit, pruning capabilities, resolved view and pre-execution candidate statistics. No Parquet reads. |
| `ScanCursor::Profile()` | Thread-safe cumulative completed work, available after completion, cancellation and errors. |

Each metadata call admits a read against one confirmed root and holds that
root through planning, including concurrent appends. No SQLite read transaction
is held while reading files. History reads durable receipts through the engine's
catalog interface; receipts for concurrent, newer snapshots are excluded.
missing or inconsistent committed metadata reports `CORRUPTION` and closes
readiness. Environmental read errors retain their scan error classification.
`GetStatistics`, `ListFiles` and `ExplainScan` share the backend's bounded
immutable `TSnapshotMetadataCache` (roots, manifest lists and live manifest
`DataFile` vectors); `ListSnapshots` and `GetFileDetails` bypass that cache and
read authoritative files directly on each call.

`identity` describes the confirmed current root; `selector_resolved` identifies
the chosen historical or initial view separately. Latest resolves exactly once
per call. Independent calls may observe different roots. Retained snapshots and
receipts survive reopen, including after superseded root files are removed.

`TFileListing` and `TFileDetails` keep `Identity` (the confirmed root) separate
from `SelectorResolved` (the selected view). Listings are sorted by `FileID`.
An ID is a lowercase SHA-256 digest of a versioned encoding of the table UUID
and committed logical store URI. IDs remain stable across snapshots, reopen and
moving a whole store; they are opaque identifiers, not content hashes or paths.
Only IDs from the selected view authorize details. Unknown IDs, files from
another collection, and newer files queried through an older view return
`NOT_FOUND`. Paths and malformed IDs return `INVALID_ARGUMENT`. The engine
resolves logical URIs through its store-scoped read-only FileIO; clients never
supply filesystem paths. Orphans are absent even when present on disk.

File metrics come from Avro manifests, keyed by stable field ID in schema order.
Optional counts and bounds remain absent when the manifest omits them. Iceberg
bounds retain their single-value binary encoding and any configured string
truncation; they are not decoded or promoted to exact extrema. File length is
the complete compressed Parquet file, including its footer. Listing reads no
Parquet data. Details check the actual file length, magic, bounded footer,
schema/field IDs and total/row-group counts before returning footer metadata.
Present column sizes and null counts must also agree with manifest metrics.
Only the header, trailer and footer are read; data pages are not decoded.

Each row group reports its ordinal, rows, summed compressed column bytes,
uncompressed bytes and column chunks. Columns include field ID, values, optional
null count/bounds, compression (the codec the footer records, e.g. `zstd`,
`lz4_raw`; independent of the collection's current write settings), page
offsets and index presence. Column bounds
retain Parquet plain encoding, distinct from Iceberg manifest bounds. Missing
statistics stay null; an all-null column need not have bounds. Index presence
does not imply scan pruning. These are stored metadata observations, not a
verification of every data page. Corrupt/missing committed files close readiness;
environmental read/close failures retain `IO_ERROR` without closing readiness.

File listings and details materialize DTOs under existing local file/footer
limits and shared metadata working reservations. Original results retain
capacity-sized leases for file descriptors, bounds and row-group/column data
until their final owner releases them, including after engine close. Independently
copied caller DTOs belong to the host budget. Conservative working envelopes
are not an RSS guarantee. See
[shared resources](SHARED_RESOURCES.md) for the accounting contract.

## Fixed-view pages

Pass `TMetadataPageRequest` with `PageSize` (1–1024, default 100), an initially
empty `PageToken`, and an adapter-supplied `AuthorizationContext`. Continue with
the returned `NextPageToken` and the same arguments until that token is empty.
An empty next token is terminal; an empty input token always starts a new view.
The unpaged methods remain available.

The first call materializes one verified listing. File and history pages retain
its root identity and resolved snapshot; subsequent appends cannot add or
remove entries from it. Collection pages retain the collection descriptors and
creation high-water mark captured at admission. Replaying any continuation
returns the same entries and next token. Materialized listings need no further
file reads, so superseded root JSON files can retire normally between pages.

Tokens are 256-bit random bearer capabilities authenticated by server-held
lookup; they contain no caller-editable offsets or paths. Each continuation is
bound to the API, collection, original selector, page size and authorization
context. Adapters must derive that context from trusted authentication state,
including the authorization generation/scope; accepting client-supplied context
would not enforce authorization. Empty context is for a single embedded caller.
Changed, malformed, foreign, expired and pre-restart tokens return
`INVALID_CURSOR`. Invalid first-page arguments return `INVALID_ARGUMENT`.
Tokens belong to one Engine instance and are invalidated on reconciliation or
shutdown. Existing readiness errors still apply to cached pages.

Each engine admits at most 32 retained or loading listings and 16 MiB of
estimated retained DTO/token allocations. Admission refuses excess with
`RESOURCE_EXHAUSTED`; it does not evict live cursors. Listings expire five
minutes after admission, without renewal on replay; the next page request
reclaims expired entries. Shutdown releases retained listings. Empty and
single-page results retain no cursor. Listing construction, cached descriptors
and tokens, and every returned page use the engine's shared metadata parent.
Returned pages retain independent capacity-sized leases after engine close.
Pressure refuses a page without consuming its continuation; retry and replay
remain available after capacity returns. These envelopes are not an RSS limit.

Every `Metric` contains an optional integer and a `MetricKind`:

- `Metadata`: counts summed from committed live-file descriptors. These are
  exact within the append-only model, but do not measure every data page.
- `Estimate`: planned output rows for an unfiltered view, bounded by its limit;
  zero is also known for a zero limit or empty view.
- `Measured`: completed execution work observed by this cursor.
- `Unavailable`: null. Logical/uncompressed size, elapsed time and nontrivial
  predicate selectivity remain unavailable; unknown does not mean zero.

Explain lists snapshot candidate files before execution without opening Parquet
files; `file_pruning` and `row_group_pruning` reflect the backend's conservative
zone-pruning capabilities (`true` on `TLocalStorageBackend` by default, `false`
on custom backends; page and Bloom pruning remain unsupported and false). A small
limit may stop execution early; this is not pruning. Projection includes
predicate fields in physical reads. During execution, positional Parquet reads
fetch only the header/footer (on a footer cache miss) and the column chunks of
surviving row groups for `projection ∪ predicate` (zero-column unfiltered scans
read no column chunks).

Profile counters count successfully decoded manifests, opened Parquet files whose
footer was verified (`data_files_read`, including files whose row groups were all
pruned), actual positional
Parquet `pread` bytes (`data_bytes_read`, excluding cached footers, metadata,
failed reads after the failing syscall and OS page-cache hits), decoded rows
before filtering/limit, and rows returned after filtering/limit. `Complete` means
the caller reached EOS, including limit exhaustion; it does not mean every
candidate was read. Open profiles are partial. Cancellation is observable
before another `Next()`; terminal complete/failed states are retained through
shutdown. Custom cursors may leave unsupported counters unavailable.

Native profiles separate `rows_filtered` (decoded rows rejected by the exact
predicate) and `rows_limited` (matching decoded rows discarded by the limit).
Unread rows are not estimated. `limit_stops` and `cancellation_stops` are each
zero or one: the former records delivering the requested limit (or completing a zero limit),
the latter records cancellation observed by `Next()` while open. Repeated EOS
or cancellation calls do not increment them. `data_files_pruned` counts
candidate files skipped by conservative manifest pruning before opening Parquet,
and `row_groups_pruned` counts row groups skipped by conservative Parquet footer
pruning before reading column chunks; files or row groups left unread because of
`limit` or cancellation are never counted as pruned.

`arrow_bytes` and `peak_arrow_bytes` measure live/high-water allocation sizes in
that cursor's Arrow pool, covering decoding and retained output; they exclude
allocation padding, accounting nodes, whole-file copies and non-Arrow envelopes.
`output_metadata_bytes` and `peak_output_metadata_bytes` measure reservations for
original output descriptors. Live counters can fall when another thread releases
a retained result; peaks persist. Neither is process RSS. `ResourceOwner::GetUsage()`
remains the authority for combined class reservations. Custom cursors leave
unsupported measurements unavailable. These added counters are public C++ API
observations; HTTP scan responses retain their existing wire contract.

[HTTP metadata routes](HTTP_SERVER.md) adapt these methods into the existing
SDK/workbench types. Counts/identities use decimal strings; unavailable values
remain null with explicit metric kinds. File details distinguish descriptor
metadata from verified actual bytes. Presence of an index does not imply scan
pruning. Root leases cover footer inspection through concurrent append cleanup.
No snapshot expiration, directory-based inventory, format change or durability
qualification is introduced.

Metadata history/file pages retain the first resolved view. Opaque tokens bind
to database, collection, kind, original selector and page size; tampering,
expiry, lifecycle invalidation and changed read admission fail explicitly.
The [installed acceptance guide](NATIVE_ACCEPTANCE.md) independently verifies
Parquet/Avro, API membership, reopen and whole-store restore.

Validation uses real dependencies in `NativeMetadata`, metadata cases in
`ScanReadErrorClassificationTest` and `SupersededRootTest`, and the installed
consumer's separate write/reopen processes. The consumer checks explicit
dependency errors in the storage-disabled contour. In-memory scan tests remain
separate evidence. See the PR for executed commands and results.

`TFileMetadataTest` independently reads Avro manifests and physical Parquet
footers, including multiple row groups and all-null fields. It checks historical
membership, orphan exclusion, stable IDs across reopen and footer corruption.
File cases in the read-error and root-retirement suites check bounded footer
reads, classification and concurrent append. The installed consumer exercises
the file APIs in separate write/reopen processes and checks dependency errors
with storage disabled. These native tests are distinct from synthetic mock/UI
descriptors and do not qualify power-loss durability.

`TMetadataPagesTest` in the native vertical suite covers fixed membership across
writes, parameter/authorization binding, foreign and reopened engines, historical
and initial views. Its unit counterpart covers replay, absolute expiry, quota
reclamation and admission failures with an explicit clock. The installed consumer
carries a cursor between separate write/reopen processes to verify rejection.
