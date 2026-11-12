# Verified scan membership

The local backend derives scan membership during strict Iceberg closure
verification. Iceberg roots, manifest lists and manifests remain authoritative;
there is no persisted auxiliary index or storage migration.

Each immutable `TBaseState` binds the exact metadata location, parsed root,
verified usage and a `TScanMembership`. The index stores each data-file
descriptor once in chronological order, retaining the verified
`std::shared_ptr<const iceberg::DataFile>` (while keeping membership equality on
`{Path, SizeBytes, RecordCount}`) so indexed scans can evaluate manifest column
bounds and null counts without reopening manifests. A sorted snapshot-ID table identifies
each retained snapshot's prefix boundary. IDs are opaque; sorting IDs is only
for lookup. Scanning reverses the selected prefix to preserve the manifest
order produced by fast append. Initial-empty views select zero files.

This works only for the current append-only, unpartitioned, fixed-schema
profile. Deletes, branches, compaction, snapshot expiration and schema evolution
need a different representation and validation contract.

## Verification and lifetime

`CheckManifest` records a descriptor only after the existing strict manifest
entry/counter/path checks and Parquet length, row-count, schema and field-ID
verification succeed. Recovery builds one index while verifying every retained
transition. Publication copies the verified base index and checks only the new
closure; that candidate remains tentative until the outer SQLite commit.

- Confirmed publication installs the exact candidate.
- Definite rollback discards the candidate and retains the previous index.
- Uncertainty, observed corruption and reconciliation invalidate backend admission.
- A failed rebuild leaves no usable backend generation. Successful recovery
  rebuilds from authoritative files and the engine completes its durability fence.

The engine caches the index with its confirmed collection view. A scan pins both
under the same state lock, including the interval between backend confirmation
and engine-view refresh. Later appends cannot change its files or selected view.
The cursor and returned buffers retain their owned Arrow pool, FileIO and engine
resources until the last dependent handle is released, including after filtering.
New scans are blocked while recovery is required. Existing pinned cursors retain
their immutable membership and normal data-read/error behavior.

Healthy `Engine::Scan` planning performs no root, manifest-list or manifest I/O.
It selects a boundary in O(log retained snapshots), without copying the file
prefix. `limit=0` opens no data file, and candidate files proven non-matching by
manifest bounds are pruned (`data_files_pruned`) before opening Parquet. Surviving
data-file footers (reusing the bounded per-store `TSnapshotMetadataCache` on
repeated scans) and decoded-value checks still run during execution. Cursor `manifests_read` is measured zero for indexed
scans. The non-indexed planner remains available for custom backends and component
tests. `ListSnapshots` and `GetFileDetails` still read authoritative files directly,
while `GetStatistics`, `ListFiles` and `ExplainScan` may reuse immutable entries in
`TSnapshotMetadataCache`; neither cached planning nor recovery is a full data-page scrub.

## Cost and reproduction

The current development caps bound snapshots and files. A live generation takes
O(files + snapshots) index space; publication copies that space, and cursors may
retain previous generations. The backend's shared metadata reservation stays
with the parsed root and its index, including generations pinned by cursors.
Recovery adds descriptors to one mutable builder before publishing it. The
[shared resource envelopes](SHARED_RESOURCES.md) remain conservative admission
estimates, not a bound on total process memory.

Build and run the native benchmark:

```sh
cmake --build --preset release --target glamdb_scan_index_bench --parallel 2
build/release/tests/glamdb_scan_index_bench 32 1000
build/release/tests/glamdb_scan_index_bench 128 1000
build/release/tests/glamdb_scan_index_bench 256 1000
```

Each run creates a disposable native store with duplicate/NULL rows, compares
indexed and uncached planning, counts metadata opens, then closes and reopens
the engine. Reopen is cold with respect to engine state; the OS page cache is
not flushed. `index_capacity_bytes` counts `TScanMembership`'s own object, vector
and path-string capacities, excluding allocator rounding, shared-pointer control
blocks, shared `iceberg::DataFile` descriptors (emitted separately as
`datafile_descriptor_bytes`), parsed roots/manifests, FileIO instrumentation and process RSS.

These measurements use the initial implementation on official base `1f48601`.

Measured on 2026-11-12 with Linux x86-64, Clang 18 and a Release build using
the pinned native dependencies. Timings are observations on a shared host,
not latency guarantees. Each warm scan measurement contains 1,000 zero-limit
scan opens; each uncached planning measurement contains five full membership
plans. Counts include every metadata `OpenRead` through the injected native FS.

| Appends | Indexed plan, us | Uncached plan, ms | Warm scan, us | Warm metadata opens | Uncached metadata opens / plan | Reopen, ms | Rebuilt index capacity, bytes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 32 | 0.166 | 25.7 | 3.62 | 0 | 68 | 83.7 | 7,984 |
| 128 | 0.127 | 115.0 | 4.54 | 0 | 260 | 235.0 | 31,792 |
| 256 | 0.130 | 234.9 | 7.60 | 0 | 516 | 513.7 | 63,536 |

Reopen performs authoritative metadata reads (259 / 1,027 / 2,051 respectively)
and reconstructs membership. The benchmark does not establish device-cold
latency, total recovery RAM, a process memory cap or power-loss qualification.

`ScanIndexTest` compares indexed and uncached native membership and literal
duplicate/NULL rows for latest, historical and initial-empty selectors, keeps
orphans invisible, and checks rebuild/invalidation. `BackendPublicationTest`
checks tentative/confirmed, rollback, uncertainty and failed-rebuild transitions.
`BackendPublicationIntegration` exercises publication handoff and lost COMMIT
results with real SQLite/Parquet/Iceberg. The existing uncached `ScanMockTest`
suite uses in-memory FileIO and remains separate evidence.

## Executed checks

On official base `0382261`, the normal Release build and native CTest pass with
694 discovered cases (one existing disabled benchmark and three child-only skips).
Only normal tests were rerun locally for this merge:

```sh
cmake --build build/release --parallel 8
GLAMDB_REQUIRE_NATIVE_CLIENTS=1 GLAMDB_VALIDATE_SCHEMAS=1 \
  ctest --test-dir build/release --output-on-failure --no-tests=error -j 6
```

The metadata-pressure regression failed with `RESOURCE_EXHAUSTED` before the fix.
Recovery now releases engine-owned generation pins after closing admission and
before rebuilding; active cursors retain their own states and reservations.
The independent critical review found this issue and reported no remaining
findings after the fix.
