# Durable storage module

The module supplies local durable files and Arrow/Parquet/Iceberg integration
for the append-store MVP. The catalog/coordinator and installed public API are
separate components; `core/backend/` bridges them to these primitives and
`core/scan/` reads them, as described in [SCANNER.md](SCANNER.md). See
[the coordinator guide](COORDINATOR.md) for the integration contract.

`glamdb_io` contains the independent local I/O components. `glamdb_storage`
adds the pinned dependency adapters; `GLAMDB_BUILD_STORAGE` enables it and
defaults to `ON` unless `GLAMDB_IO_ONLY=ON`. See [Building](BUILDING.md#storage-adapters) for the
Docker runner and [dependency validation](DEPENDENCY_VALIDATION.md) for the
installed-prefix consumer and toolchain constraints.

## Integration sequence

The coordinator retains the process/store ownership lock and serializes writers.
It opens or creates directories through `core/io/store_dirs.h`, constructs the
adapter in `core/storage/durable_local_file_io.h`, and retains the confirmed base
from `VerifyReachable` or a previously confirmed publication.

1. Run `CheckWriteCapacity` before binding a new operation. It checks the root
   identity, measures the warehouse including orphan files, and reserves space
   for the next attempt, journal, and filesystem headroom. The catalog owner
   supplies aggregate storage usage separately to the publication guard.
2. The coordinator owns operation identity, canonical hashing, durable binding,
   and input normalization. Pass owned, immutable, normalized Arrow batches to
   `WriteDataFile`; their buffers must remain unchanged until it returns. The
   writer validates bounded input against the loaded table schema and derives
   metrics from its properties. It does not perform the coordinator's copy or
   canonical-hash step.
3. Create an attempt and hold its `BindAttempt` scope across data writing,
   `StageAppend`, metadata writing, and publication checking. The attempt owns
   sticky errors, budgets, file state, durable receipts, and format verification.
   Delayed handles retain their original attempt; releasing the scope deactivates it.
4. `StageAppend` commits the Iceberg append update to its transaction and returns
   the staged transaction, snapshot identity, and manifest paths. It does not
   perform the outer transaction commit. The catalog/coordinator owner passes
   expected operation fields in the summary map and the guard's `ExpectedSummary`;
   this module does not define that owner's persistent receipt schema.
5. The catalog calls `BeforePointerChange` after the candidate metadata JSON is
   durable and before changing the catalog pointer. The guard seals the attempt,
   verifies the exact prepared base, metadata delta, reachable new files,
   retained manifests, expected summary fields, and capacity limits.
6. `GetCandidate()` returns a tentative verified state. Install it as the next
   confirmed base only after the caller confirms its outer catalog COMMIT.
   Atomic pointer/outcome publication, uncertain-commit recovery, fencing, and
   client acknowledgements remain catalog/coordinator responsibilities.

For collection creation, `MakeTableProfile` supplies the schema and exact table
properties used with the public Iceberg create API. The adapter and guard check
the resulting durable root. Public collection lifecycle and catalog registration
remain external. Schema names are nonempty UTF-8, at most 128 bytes, and contain
no NUL; the optional `key` field must be a required string. Field IDs are stable
1..N, with no Iceberg identifier fields. Publication and recovery both require the
canonical schema and exact properties returned by `MakeTableProfile`; caller
property extensions and changes to that profile are rejected before publication.
Snapshot-log timestamps refer to their snapshots. The root update timestamp is
independent and is preserved on recovery, including when the wall clock moves.

Failures that the engine must classify carry stable message prefixes from
`core/storage/iceberg_error_boundary.h`: `CORRUPTION: ` marks a violated storage
invariant and `CAPACITY: ` a configured storage or file limit (candidate store
usage, candidate root size, append accumulators and the Parquet file limit).
The durable FileIO reports store-layout violations as `kInvalid` with the
corruption prefix: a symlink (ELOOP with `O_NOFOLLOW`), a non-directory or
cross-device entry, and, when opening an existing file, a non-regular file or
one larger than its file-kind limit. See
[SCANNER.md](SCANNER.md#error-classification) for the engine mapping.

## Read, recovery, and copy boundaries

Reads on the binding thread participate in its active attempt and I/O budget.
Scan callers must retain an independent `ReadOnlyView()`; they must not reuse
the writer's implicit binding. The adapter rejects writes through that view and
keeps its filesystem resources alive. Snapshot selection, planning, pruning,
filtering, and returned Arrow buffer ownership belong to the scanner owner.

`VerifyReachable` validates a selected root without requiring receipts from the
creating process. It checks the supported table profile, retained snapshot chain,
manifest lists/manifests, data file lengths and Parquet footers, and storage
usage. It registers the required format readers in a fresh process. The catalog
owner selects the authoritative root and performs SQLite recovery/fencing.
This verification does not checksum every byte of historical data pages.

Recovery checks snapshot transitions with a two-snapshot window and retains the
loaded root for the final consistency check. Previously seen manifest-list and
data paths use a private temporary SQLite database capped at 256 MiB, with an
eight-page cache. It does not modify the catalog or warehouse; workspace I/O or
capacity failures fail recovery. SQLite must use disk-backed temporary databases
and its temporary directory must have sufficient space on local disk. The
bounded snapshot-ID set and current manifest list remain in memory; total RSS
and reference-host recovery time still require qualification.

`CopyDurableFile` copies an exact known length through a fixed-size buffer,
checks source identity/size and source close, and finalizes the target durably.
The caller must hold an offline snapshot/lock and prepare a durable destination
directory. The primitive does not orchestrate a whole-store backup or publish
backup readiness.

Warehouse accounting uses the larger of logical size and allocated blocks for
regular files, plus directory blocks; hard links are conservatively counted per
entry. The walk rejects symlinks, special files, cross-device entries, and depth
above 64. It creates no files and performs no orphan cleanup. Limits are
configurable development candidates pending measured qualification.

## Tests and evidence

The following files under `tests/unit/` cover the corresponding implementation.
They are registered with CTest label `unit`; storage tests require the optional
target.

| Component | Tests |
| --- | --- |
| Filesystem, URI, directory identity, SHA-256 | `io/local_fs_test.cpp`, `io/store_uri_test.cpp`, `io/store_dirs_test.cpp`, `io/sha256_test.cpp` |
| Attempt state, durable close, first errors, receipts | `io/io_attempt_test.cpp`, `io/durable_file_test.cpp` |
| Fault matching and durability ordering | `io/fault_fs_test.cpp`, `io/durability_trace_test.cpp` |
| Durable copy and source/target failure boundaries | `io/durable_copy_test.cpp` |
| Schema/profile and permitted metadata changes | `storage/table_profile_test.cpp`, `storage/metadata_validation_test.cpp` |
| FileIO binding, reads, denied deletes, lifetime | `storage/durable_local_file_io_test.cpp` |
| Real formats, bounded decoding, writer metrics and failures | `storage/format_verifier_test.cpp`, `storage/data_file_writer_test.cpp` |
| Writer compatibility and entirely absent footer statistics | `storage/writer_compatibility_test.cpp` |
| Staging, manifest round trips, no implicit publication | `storage/append_stager_test.cpp` |
| Guard, closure/recovery, retained history, invalid candidates | `storage/publication_guard_test.cpp` |
| Recovery visited index, exact duplicates, disk spill and workspace failures | `storage/recovery_visited_test.cpp` |
| Capacity boundaries, orphans, preflight faults | `storage/capacity_test.cpp`, `io/local_fs_test.cpp` |

The fault adapter also injects errors after a successful native file or directory
sync. Nested traces distinguish the successful syscall from the reported error;
all four file kinds retain the first error, issue no receipt and block further
writes. This exercises conservative error handling, not power loss.

`TManifestMetricsTest.RoundTripsIndependentMetricsThroughRealParquetAndAvro`
checks serialized field IDs, value/null/NaN counts, numeric bounds, full ASCII
and 1024-byte UTF-8 key bounds, and column sizes in a real Avro manifest against
independent row expectations and its Parquet footer. An all-NULL field exercises
omitted min/max bounds.

`TWriterCompatibilityTest` compares the complete no-key DataFile, physical bytes
and decoded rows against the upstream DataWriter. Its second test uses the public
Parquet API to disable statistics for every column, then checks format validation,
decoded rows, Avro round-trip of absent metrics and recovery. The production
writer has no test-only statistics switch. These checks passed with ASan/UBSan
and LLVM 18 analysis; writer RSS measurements remain pending. Run them with:

```sh
ctest --preset asan-ubsan --output-on-failure --no-tests=error \
    -R 'TManifestMetricsTest|TWriterCompatibilityTest'
```

`WritesTwoAppendsAndReopensEveryRetainedFile` in the publication guard suite writes
1,000 and then 10 rows with duplicate keys through the real Parquet writer,
stages real Avro manifests/lists, writes metadata, checks publication, and reopens
the closure in a fresh Linux process. Its upstream in-memory catalog is a test
double for the catalog integration boundary. The child helper is skipped when CTest
invokes it alone and is explicitly executed by this pipeline test.

With the full build and storage enabled, `glamdb.durable_storage_catalog` combines
the durable FileIO, real writers and publication guard with the real SqlCatalog
and SQLite catalog store. It verifies create, appends of 1000 and 10 duplicate
rows, guard rejection, outer rollback and three actual intervening base changes.
An independent SQLite connection checks that inner commits remain tentative.
An observing catalog forwards real commits unchanged and checks the emitted
table UUID and `main` snapshot requirements, including the first append's absent
snapshot. A competing staged create is rejected before its publication body or
guard can run, preserving the winning name, UUID and root.
A new process selects its root from the reopened catalog, compares exact stored
outcomes with caller-result fixtures, and reads historical/current rows through
the manifests. Failed candidates remain unreachable; this raw catalog test leaves
their bindings unresolved rather than claiming coordinator reconciliation.
It does not implement the production StorageBackend or scanner. Run it with:

```sh
ctest --preset asan-ubsan --output-on-failure --no-tests=error -R '^glamdb.durable_storage_catalog$'
```

The full Linux ARM64 Docker baseline registered 380 tests: 379 passed with ASan/UBSan,
and the child-process dispatcher skipped its standalone invocation. The parent
pipeline executed it successfully in a fresh process. Regressions for delayed
Iceberg commits and unsupported profile extensions failed against the original
implementation and passed after the fixes. LLVM 18 formatting checked 109 project
files; changed C++ analyzer checks passed. The Release installed-package consumer
was verified separately. Run the current checkout with:

```sh
./scripts/test-storage-in-docker.sh asan-ubsan
./scripts/test-installed-deps-in-docker.sh
```

The storage sanitizer profile has the documented libc++ compatibility restriction
in [Building](BUILDING.md#storage-adapters); it is not a fully instrumented
dependency qualification. Product integration, load/capacity measurement,
long-running fault campaigns, and Linux x86-64/ext4 power-loss qualification are
deferred. Simulated failures and fresh-process recovery do not establish
power-loss durability.

Recovery tests cover the temporary index, nine retained-history cases and reopening
at exact usage limits; reducing each positive usage limit rejects the same store
without modifying it. Collection-copy tests use real Parquet/Avro/JSON files,
remove their disposable source and reopen old/current roots in new processes.
Interrupted copies fail on the injected Parquet corruption. These component tests
do not cover catalog backup, backup leases or restore through the engine API.
Both copy cases also passed with source temporary files on tmpfs and the destination
on a different filesystem; deleting the source does not require a cross-device rename.
The follow-up publication/recovery run registered 57 tests: 56 passed; the child
dispatcher skipped its standalone invocation and ran through its parent tests.
The real catalog integration also passed separately.
The bounded file-I/O probe and its separately checked syscall trace are described in
[qualification preparation](QUALIFICATION.md#bounded-file-io-probe).

Raw generated logs include `artifacts/qualification-full-engine-check.log`,
`artifacts/after-success-io.log` and `artifacts/p-io-final-evidence.log`.
Earlier regression and installed-package evidence is retained in
`artifacts/review-regression-before.log` and `artifacts/merged-install.log`.
