# Public metadata

`storage/engine.h` exposes native metadata through transport-independent DTOs
in `storage/metadata.h`. Adapters must use these methods instead of reading the
catalog or warehouse themselves. These APIs require `GLAMDB_BUILD_STORAGE=ON`;
disabled builds return `DEPENDENCY_UNAVAILABLE`.

| API | Result |
| --- | --- |
| `GetCapabilities()` | Backend identity and supported append, scan and metadata operations; no I/O or writer wait. Custom backends identify themselves explicitly. |
| `GetHealth()` | Current admission/recovery/shutdown state, separate from supported operations. |
| `ListSnapshots(collection)` | Retained snapshots, oldest first, joined with their original committed append receipts. Initial empty is not a snapshot. |
| `GetStatistics(collection, selector)` | Rows, live data-file count and compressed data-file bytes from the selected view's manifests. |
| `ListFiles(collection, selector)` | Stable opaque file IDs, lengths, row counts and field-ID metrics from the selected view's live manifest entries. |
| `GetFileDetails(collection, file_id, selector)` | Manifest descriptor and verified physical Parquet row-group/column metadata for a member of the selected view. |
| `ExplainScan(request)` | Validated projection, physical read fields, predicate, batch size, limit, resolved view and candidate statistics. No Parquet reads. |
| `ScanCursor::Profile()` | Thread-safe cumulative completed work, available after completion, cancellation and errors. |

Each metadata call admits a read against one confirmed root and holds that
root through planning, including concurrent appends. No SQLite read transaction
is held while reading files. History reads durable receipts through the engine's
catalog interface; receipts for concurrent, newer snapshots are excluded.
Missing or inconsistent committed metadata reports `CORRUPTION` and closes
readiness. Environmental read errors retain their scan error classification.

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
null count/bounds, compression, page offsets and index presence. Column bounds
retain Parquet plain encoding, distinct from Iceberg manifest bounds. Missing
statistics stay null; an all-null column need not have bounds. Index presence
does not imply scan pruning. These are stored metadata observations, not a
verification of every data page. Corrupt/missing committed files close readiness;
environmental read/close failures retain `IO_ERROR` without closing readiness.

File listings and details materialize caller-owned DTOs under existing local
file/footer limits. Shared metadata reservations and fixed-view pagination are
separate work; these APIs do not provide a process-wide memory bound.

Every `Metric` contains an optional integer and a `MetricKind`:

- `Metadata`: counts summed from committed live-file descriptors. These are
  exact within the append-only model, but do not measure every data page.
- `Estimate`: planned output rows for an unfiltered view, bounded by its limit;
  zero is also known for a zero limit or empty view.
- `Measured`: completed execution work observed by this cursor.
- `Unavailable`: null. Logical/uncompressed size, elapsed time and nontrivial
  predicate selectivity remain unavailable; unknown does not mean zero.

Explain lists all candidate files before execution. File and row-group pruning
are unsupported and explicitly false. A small limit may stop execution early;
this is not pruning. Projection includes predicate fields in physical reads.
The scanner reads entire Parquet files, then decodes only requested columns.

Profile counters count successfully decoded manifests, successful whole-file
reads and their bytes, decoded rows before filtering/limit, and rows returned
after filtering/limit. Bytes include footers and unprojected column bytes but
exclude metadata, failed/partial reads and device/cache I/O. `Complete` means
the caller reached EOS, including limit exhaustion; it does not mean every
candidate was read. Open profiles are partial. Cancellation is observable
before another `Next()`; terminal complete/failed states are retained through
shutdown. Custom cursors may leave unsupported counters unavailable.

No persistent format changes, snapshot expiration, directory inventory, HTTP
routes, SDK/UI integration or durability qualification are introduced here.
The existing HTTP metadata/explain routes still return `501`.

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
