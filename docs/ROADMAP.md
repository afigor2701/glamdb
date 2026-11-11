# Roadmap

The first release target is an embedded C++23 columnar append store using Arrow,
Parquet, Iceberg, and SQLite. Its critical path is durable append, reopen in a
new process, fixed-snapshot reads, and fault qualification. Append preserves
duplicate keys; it is not yet the key-value API.

## Current status

This section describes `main` as of 2026-11-11. *Implemented* means the code is
on `main`. *Executed* means a named test or recorded run passed; the pull
request that added a test records its run. A gate is complete only when all of
its required evidence has been executed. No gate is marked complete.

The local append path is implemented end to end. The coordinator publishes
Parquet data files, manifests, manifest lists and metadata JSON through the
production storage backend before the outer SQLite commit. `Engine::Scan` reads
fixed snapshots, and `Engine::GetHealth` reports read and write admission. The facade also exposes
[capabilities, snapshot history, selected-view statistics, file/row-group details, explain and cursor profiles](development/METADATA.md). The
native HTTP server (`glamdb --data-dir`), the network CLI (`glamdb-cli`), the
Python SDK and the browser workbench use that same core. The disposable mock and
the static browser demo remain separate profiles; passing them is not native
evidence.

| Gate | Status | Executed evidence on `main` | Not implemented or not run |
| --- | --- | --- | --- |
| G0 | Contracts and fixtures in place | Public headers without private or transport dependencies; canonical golden fixtures; route, health-state and shared outcome/scalar fixtures in `fixtures/http/`, consumed by native, SDK, browser and mock tests; the [resource budget matrix](development/RESOURCE_BUDGETS.md) (documentation). | The candidate limits have not been measured together (G4). |
| G1 | Partial | One pinned Arrow/Parquet/Iceberg/SQLite installation with exported targets; durable I/O and publication tests for all four file classes; catalog commit, rollback and lost-result tests through the SQLite VFS; a recorded iceberg-cpp patch with a linear-cost regression test. | Measured metadata and control allocations; combined boundary states reopened under the same limits. |
| G2 | Executed at small scale | `glamdb_vertical` real-file tests: A/B appends, a held cursor, the initial empty view, retries, reopen and zero-limit cancellation. `glamdb.scan_process`. `glamdb.installed_consumer` writes and reopens in separate processes, in storage-enabled and storage-disabled builds; it checks native history/statistics/explain, file details and zero-limit profiles. `TFileMetadataTest` compares manifest metrics and physical Parquet row groups independently and rejects files outside the selected view. | The full 1,000 + 10-row scenario as an automated public-API test (`glamdb.durable_storage_catalog` checks those row counts at the storage/catalog layer, and the scenario has been run by hand over HTTP); filtering with pruning on and off (no pruning exists). |
| G3 | Partial | Three-outcome classification, A/B/retry-A, conflicting retries, exact-base rejection, corruption closing readiness, latched write failures, retained Arrow buffers across shutdown, snapshot timestamp range checks and admission before binding. SQLite VFS faults with fresh-process recovery, on a test backend and on native storage over HTTP. Native CI is configured to run these suites under normal, ASan, UBSan and TSan on pull requests, plus MSan and LSan on `main`. `AppendAdmissionTest` covers native pre-binding capacity/allocation refusal and matching retries. | One fault harness combining file-system and SQLite failures on the real backend; quiesced backup and restore; a unified power-loss model that includes SQLite and detects omitted catalog syncs. |
| G4 | **NOT RUN** | None for this gate. Preparation tools exist: an environment collector, an I/O probe and an abstract ordering model; see [qualification preparation](development/QUALIFICATION.md). | Model and physical power-loss campaigns; the 5,000 × 1,000-row ingestion run; combined capacity and cold-reopen measurements; an installed service, CLI and UI run from a clean prefix. |
| N | Implemented, partly validated | Real-socket suites `glamdb.native_http`, `glamdb.http_review`, `glamdb.database_limits`, `glamdb.cli_network`, `glamdb.scan_http`, `glamdb.native_http_fault` and `glamdb.http_lifecycle` (the last on a test backend). Native shutdown cases cover selective scan cancellation, ownership after write deadlines, and graceful or forced restarts with uncertain COMMIT results. Native CI requires SDK, Arrow and schema checks in every profile and native browser acceptance in the `normal` profile. | HTTP adapters for native metadata, history and explain, plus file inventory (the routes return `501`); compound predicates and empty projection; collection and history pagination; open server findings from the native HTTP review; any network durability claim (needs G4). |
| K, S | Not started | None. | Everything. |
| Optional asynchronous admission | Outlined below only | None. | Decision record, interfaces, prototype and measurements. |

Power-loss durability remains **NOT VALIDATED** until G4's physical evidence
exists. Process kills, injected faults and restarts do not establish it. The
remaining work is tracked in [issue #28](https://github.com/glamourdb/glamdb/issues/28).

## Scope and dependencies

G0 through G4 establish local append before the KV release. Build packaging,
fixtures, client mocks, and test harnesses can develop alongside the core.
Network integration can begin after G3, but a network durability claim still
requires G4. S3 experiments can begin once the storage interfaces are defined;
the full remote qualification does not delay the initial local release.

| Stage | Completion criterion |
| --- | --- |
| G0 | Public contracts, canonical fixtures, and candidate environment/resource profiles are explicit. |
| G1 | Bounded dependency, I/O, writer, catalog, and memory experiments verify the chosen integration. |
| G2 | The embedded API creates, appends, closes, reopens in a new process, and reads current and historical views. |
| G3 | Recovery, failure outcomes, idempotency, corruption handling, ownership, and backup are validated. |
| G4 | The named Linux environment passes power-loss, capacity, ingestion, packaging, and quality qualification. |
| K | Explicit KV mode passes mutation correctness and its measured workload, bulk, and compaction requirements. |
| N | The HTTP adapter, Python network SDK, and browser UI preserve core semantics and resource lifetimes. |
| S | S3 and the persistent remote catalog pass their own commit and recovery qualification. |

Asynchronous admission is optional and has a separate acceptance condition; it
is not a prerequisite for N. Snapshot expiration, garbage collection, snapshot
rollback, merge-on-read, an LSM row-data path, replication, multiple writers, and
schema evolution are outside these baseline milestones. Extending that scope
requires an explicit design and validation change.

The [architecture](ARCHITECTURE.md) defines the shared correctness boundaries.
The sections below describe required evidence, not already available commands
or executables.

## G0: Contracts and fixtures

Define creation versus opening, collection identity, schemas, typed predicates,
snapshot selection, and the three write outcomes. Public headers must cover the
embedded API without private or transport headers. Fix canonical content hashing
and golden byte fixtures, including NULLs, duplicate rows, stable field IDs,
int64 precision, and timestamp boundaries.

Record candidate input, memory, metadata, history, disk, and concurrency limits,
and the environment in which durability will be qualified. Candidate limits are
design inputs; their simultaneous feasibility remains to be measured. Connect
each contract to an observable test and its release gate.

## G1: Integration experiments

Use small, isolated experiments to verify the dependency interfaces before
claiming an integrated engine:

- Build one C++23 dependency installation with one Arrow/Parquet runtime, the
  pinned SQLite library, and usable exported public CMake targets.
- Trace all four file classes through durable I/O: Parquet, manifests, manifest
  lists, and metadata JSON. Verify finalization, file/directory synchronization,
  exclusive creation, and preservation of the first error after repeated close.
- Verify the writer's actual Iceberg field IDs, counts, bounds, and allocation
  behavior. Parquet footer statistics alone do not validate manifest metrics.
- Verify the working catalog connection, exact prepared base, final outer
  transaction, atomic pointer/outcome update, rollback, and lost commit results.
- Measure metadata and control allocations as well as Arrow buffers. Candidate
  states must reopen under the same limits, including combined boundary cases.

Choose one tested writer/catalog path. If an upstream interface needs a narrow
adapter or pinned patch, repeat the relevant experiment; a dependency build
alone does not establish the required behavior. See the
[dependency guide](../third_party/README.md) for the integration boundaries.

## G2: Local embedded behavior

The acceptance scenario creates a store and fixed-schema collection, appends
1,000 rows as operation A, holds an A cursor, then appends 10 rows as B. The
latest view contains 1,010 rows while A still contains 1,000. Duplicate keys
across the appends remain separate rows. After all handles are released, a new
process opens the same store identity, reads both snapshots, and retrieves A's
original outcome.

Also verify empty-append rejection, an initial empty view that remains empty,
schema/snapshot information before the first cursor read, and exact filtering
with pruning enabled and disabled. Orphan files cannot enter a snapshot.
Missing stores or catalogs fail to open instead of creating replacements.
Exercise the public API from an installed consumer without source-tree or
private headers, including a zero-limit scan.

## G3: Recovery and operational correctness

- Inject failures around every file kind's write, format finalization, flush,
  close, file sync, and directory sync, plus SQLite journal and commit boundaries.
  Cover short writes, interrupted calls, disk exhaustion, and rollback errors.
- Distinguish definite failure from unknown outcomes using recovered state.
  Unknown outcomes hold the writer barrier; lost responses and restart must
  recover the original receipt without a second append.
- Test A, then B, then retry A: A's original outcome returns and B stays latest.
  Include durable failures, different-content conflicts, and retries at capacity.
- Reject changed commit bases, including metadata-only changes with the same
  snapshot ID. Detect corrupt or missing published state and unsupported format
  versions without silently repairing, truncating, or recreating the store.
- Validate cursor/buffer ownership and shutdown with ASan/UBSan, shared quotas,
  retained slices, and cancellation. A shutdown timeout cannot relabel an
  uncertain publication as a definite failure.
- Verify a quiesced backup and restore to a different physical root with the
  original volume unavailable. The source stays locked through copy and
  verification; retained snapshots and all operation outcomes survive.

Calibrate a unified power-loss model covering data bytes, directory entries,
and SQLite state. It must detect deliberately broken variants that omit data,
directory, or catalog synchronization. A model that misses these failures cannot
support the next gate.

## G4: Local release qualification

Qualify Linux x86-64 on a dedicated local ext4 volume using `data=ordered`,
enabled barriers/flush, and a device/controller that honors flush/FUA. Catalog
and warehouse share the qualified persistent volume. Record kernel, mount,
device, firmware/cache, SQLite VFS/build, and dependency details. NFS, tmpfs,
ephemeral container layers, network disks, and macOS are outside this profile.

Power-loss evidence requires at least 100 deterministic model cuts plus 100
seeded schedules, and at least 20 controlled power-cut cycles on the dedicated
test volume. Keep the received-ACK ledger on a separate durable controller.
Every acknowledged operation must survive exactly once; an unacknowledged
operation may be absent or wholly committed, never partially visible. Process
kills, simulated cuts, and physical power cuts are reported separately. Without
physical qualification, power-loss durability remains **NOT VALIDATED**.

Validate data volume, small-file counts, retained history, metadata growth,
operation outcomes, and cold reopen independently and together. Include a real
ingestion run of 5,000 attempts of 1,000 rows, with measurements every 100
successful appends, until a configured limit rejects further writes. At a hard
limit, old reads and retries remain usable. A component-generated large-data
fixture tests scanning; it does not demonstrate public bulk ingestion.

Release evidence also includes Linux debug/release builds, sanitizer lifetime
tests, an installed-library consumer, and passing quality checks with negative
controls. Publish the measured supported limits and environment alongside the
results; neither a successful compile nor the presence of `fsync` is sufficient.

## K: Key-value behavior, bulk ingestion, and compaction

KV collections have an explicit mode. Upsert is full-row replacement, with the
last occurrence of a key in the input winning; omitted nullable fields become
NULL. Delete accepts duplicate or missing keys. Copy-on-write preserves
untouched rows and publishes one atomic overwrite. No-ops create no snapshot
but still have a durable outcome. An append collection needs an explicit
migration with a duplicate policy; changing its mode in place or allowing raw
append to bypass uniqueness is outside the initial KV design.

The candidate layout is sorted runs with compaction that preserves key locality,
aimed at read-heavy, range-local workloads. Sorting a batch does not make file
ranges disjoint or provide an index. Before the main KV implementation, compare
ordered, clustered, random, long-prefix, and UTF-8 keys, including lookup misses
and deletes of missing keys. On a 16 GiB locality fixture, lookups and clustered
1,000-key updates must fit the candidate 8 GiB I/O budget and 60-second
cooperative deadline; compaction must reduce small-file count at least fourfold
without worsening range overlap. These are targets, not existing measurements.
If the locality target fails, evaluate routing or a snapshot-aware index and
record the revised design before proceeding. Random scatter throughput is not
an unconditional promise.

Bulk ingestion is one streaming user operation with bounded sort, spill, I/O,
and publication; it does not combine independent requests. Compaction has bounded
work and preserves snapshot/read semantics. Validate incremental ingestion,
queue/backlog growth, user latency, and a separate 1 TiB bulk-load scenario under
the extended profile. History exhaustion remains backpressure, not hidden
expiration. The local KV release does not wait for S3 timing qualification.

## N: Network API, Python SDK, and browser UI

Prefer a C++ HTTP adapter over the embedded engine. A Python adapter, if chosen,
must still route through the single C++ write authority. Preserve operation IDs,
snapshot preconditions, fixed pagination views, and all three outcomes across
the wire. A disconnect or timeout does not authorize a new operation ID or prove
failure. A synchronous committed response follows the core's durable commit.

Validate JSON/Arrow/SDK round trips without losing int64 or microsecond precision,
lost-response recovery, stable pagination, authentication/authorization, bounded
requests and queues, and secret redaction. SDK streaming handles explicitly close
both transport and Arrow resources on completion, exceptions, and early exit.
UI state distinguishes append mode, KV availability, qualification status, and
an unresolved write outcome.

Query-result expiration is separate from snapshot and operation retention.
If result spooling is exposed, bound disk use, publish only complete results,
and retain files while active download leases exist. Mocks and golden fixtures
can precede G3; real integration follows G3 and durable claims require G4.

## Optional asynchronous admission

The synchronous network API needs no job subsystem. Exposing an early `202`
requires durable canonical payload and a queued record before responding.
Accepted and committed remain distinct. Unknown commit outcomes stay nonterminal
and hold the writer barrier; terminal success, failure, and no-op outcomes retain
their idempotency identities permanently.

This extension needs explicit queue/control quotas, recovery and corruption
tests, and backup coverage for payload and control data. Separate SQLite
databases do not share an atomic transaction. Qualify those semantics before
enabling asynchronous endpoints rather than relabeling synchronous responses.

## S: Object storage

Begin with a small create/append/catalog-restart/historical-read experiment.
Complete immutable object uploads before persistent catalog publication; require
an atomic guard on the prepared metadata location or version token, including
metadata-only changes. A snapshot-only assertion or copy/delete rename does not
provide that boundary.

The remote gate covers upload/multipart failures, endpoint and credential
outages, lost successful catalog replies, restart, unknown outcomes, old retries,
control-volume loss, resource accounting, and backup/restore. Keep one C++ writer
and persistent control state. SQLite cannot make S3 and a remote catalog one
transaction; local qualification does not imply remote qualification.

## Evidence and status updates

Record the revision and dependency pins, environment, exact commands, exit codes,
fixtures/seeds, observations, and PASS/FAIL/NOT RUN results for each gate.
Keep raw run output in ignored `artifacts/` and preserve reviewable summaries or
stable artifact references with the corresponding change. Distinguish source
inspection, design decisions, model results, and executed environment tests.
Update this roadmap as each gate gains evidence rather than marking a whole
milestone complete from one passing test.

- [Repository structure](PROJECT_STRUCTURE.md)
- [Build and quality checks](development/BUILDING.md)
- [Dependency pins and build](../third_party/README.md)
