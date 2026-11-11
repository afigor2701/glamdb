# GlamDB agent guide

This file applies to this repository. All paths and commands below are relative
to its root. A nested `AGENTS.md` provides additional guidance for its subtree.

## Keeping this guide current

Update this file in the same change whenever implementation makes its project
status, layout, commands, conventions, or validation guidance outdated. Replace
obsolete statements, including scaffold-only and placeholder descriptions, and
add guidance for new capabilities that agents need to work with. Remove
superseded instructions instead of accumulating a history of them.

Keep the affected README and repository documentation consistent with those
updates. Derive implementation status from the code and validation status from
recorded checks; implementing a feature does not by itself qualify its durability
or complete a roadmap gate. Keep this guide concise, with detailed design and
validation material in repository-local documentation. Its links and instructions
must remain usable from a standalone checkout.

## Project context

GlamDB is an open-source C++23 columnar key-value storage engine in early
development. The checkout implements the SQLite catalog, synchronous write and
recovery coordinator, canonical Arrow input, public C++ facade, outcome JSON,
and network CLI. Independent durable I/O and Arrow/Parquet/Iceberg
storage primitives provide file production, staging, publication validation,
recovery verification, and capacity checks. `core/backend/` connects them to the
coordinator StorageBackend and `core/scan/` implements `Engine::Scan`
(fixed snapshot, SQL three-valued compound predicates `True`/`Compare`/`In`/`IsNull`/`IsNotNull`/`And`/`Or`/`Not`, projection, limit; no pruning). See
[the scanner guide](docs/development/SCANNER.md) for their contract and
validation status, [the coordinator guide](docs/development/COORDINATOR.md)
for the integration contract, and [the storage guide](docs/development/STORAGE_MODULE.md)
for the file-layer contract. Full engine durability remains unqualified. `Engine::GetHealth()` exposes
nonblocking read/write admission and recovery/shutdown state; HTTP
`GET /databases/{id}/readyz` (or legacy `/readyz`) maps write readiness to
200/503. A latched durable-I/O failure permits reads but requires reopen for
new writes; successful reconciliation does not clear that latch. `/healthz`
is process liveness only. Health never qualifies durability.

Independent verification tools, a Python network SDK and a browser workbench
are implemented. They can use the native Asio HTTP server over the public Engine facade.
`--data-dir` starts with no databases; authenticated API/UI operations create,
list, open, close and reconcile databases. A separate SQLite service registry
stores identities, persisted engine limits and open intent; collection/data writes remain in core.
Scans use bounded fixed-view pages with per-database quotas, finite replay retention,
expiry and explicit cancellation. Separate HTTP data, lifecycle and control workers
keep healthy databases and descriptors available during long operations.
Outcome lookup refuses catalog contention without blocking control workers.
Scan pages reserve replay capacity before consumption; admission refusal preserves
the existing cursor and its last replay. Other page failures invalidate it.
The network CLI also builds independently with OpenSSL; optional offline commands
are in `apps/admin` and never required for server initialization.
Collection routes are scoped by `/databases/{id}`; optional legacy
`--root/--store-id` mode retains unscoped routes. A
separate, explicitly disposable in-memory mock remains a development profile. The architecture and
invariants below describe the intended engine, not qualified guarantees.

Data studio separates compact read and schema-driven write panels; the form
generates typed JSON, while a batch JSON editor remains optional. Without HTTP
metadata APIs, Overview can count rows via a cancellable fixed-snapshot scan.
Snapshots and Storage show only confirmed receipts from this session, clearly
labelled as partial observations rather than complete history or file inventory.
See the workbench guide for configurable editor sizing and validation.

The same UI can publish as a static browser-only demo on GitHub Pages. It has
no API token or shared backend: tab-local rows/operations disappear on reload.
JSON export is available there; network SDK and Arrow IPC remain connected-server
features. Do not present BrowserDemoAPI as the public HTTP or durable core API.

The first milestone is an embedded, synchronous, durable **append store** on
local disk. A store has one owning process and one serialized writer; reads may
run concurrently. Collections are fixed-schema, unpartitioned Iceberg v2 tables.
Append preserves every input row, including duplicate keys. It provides neither
key uniqueness nor replacement semantics.

Key-value upserts and deletes are a subsequent extension using copy-on-write
and an explicit KV collection mode. Native capabilities, receipt-backed snapshot
history, selected-view statistics, snapshot-bound file/row-group details,
explain and cursor profiles use the public
facade; see [the metadata guide](docs/development/METADATA.md). Their HTTP/SDK/UI
adapters and object storage remain later extensions. Compaction, snapshot expiration,
garbage collection, asynchronous admission, replication, multiple writers, and
schema evolution are outside the initial append scope. Consult
[docs/ROADMAP.md](docs/ROADMAP.md) when assessing feature scope; its current
status table separates implemented code from executed evidence per gate and
keeps G4 power-loss qualification **NOT RUN**. Update that table when a change
adds or removes gate evidence.

## Architecture and correctness boundaries

| Component | Responsibility |
| --- | --- |
| Arrow | Typed in-memory batches and columnar buffers. |
| Parquet | Immutable user-data files. |
| Iceberg C++ | Table schemas, manifests, file membership, and snapshots. |
| SQLite | Persistent catalog pointer, operation identities, and outcomes. |
| GlamDB core | Write ordering, publication, recovery, scanning, and resource ownership. |

Read [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) before changing persistence,
recovery, scan semantics, or resource ownership.
For resource limits, read [the shared budget contract](docs/development/RESOURCE_BUDGETS.md):
it names accounting units, owners, enforcement and release points across the
engine and transport. Preserve its distinction between current local checks and
required aggregate memory/device reservations; update the matrix when either changes.

Preserve these boundaries when adding or changing engine behavior:

- **One write authority.** All writes and Iceberg commits go through the C++
  core. Transport and client layers adapt its API; they do not own independent
  writers. Keep HTTP, Python, and UI dependencies outside the public API and core.
- **Explicit store identity.** Creation and opening are distinct operations.
  `OpenExisting` validates the expected store identity and existing catalog;
  missing or corrupt state is an error, never an implicit empty-store creation.
  Store ownership remains locked while dependent operations or handles exist.
- **Durable publication.** A committed response requires durable Parquet,
  manifests, manifest lists, and metadata JSON before the final outer SQLite
  transaction commits. File finalization, buffer flush, file sync, checked close,
  and directory sync all matter. SQLite commits do not sync external files;
  successful internal Iceberg updates do not authorize a committed response.
  The coordinator reports the outer result through `PublicationFinished`:
  promote the exact candidate on success, discard it on rollback, invalidate
  the verified generation on uncertainty. Admission/preparation only use an
  explicitly confirmed base; reconciliation rebuilds invalidated generations.
  Preserve the first I/O failure through retries, repeated close, and cleanup.
  The initial catalog profile uses `journal_mode=DELETE` and `synchronous=EXTRA`,
  verified on the actual working connection.
- **Exact commit base.** A prepared write retains its metadata location, table
  UUID, and optional snapshot identity through publication. Reject a changed
  base instead of silently rebasing. A snapshot-only precondition cannot detect
  metadata-only changes.
- **Explicit outcomes and durable retries.** Distinguish `Committed`,
  `DefinitelyNotCommitted`, and `CommitUnknown`. An uncertain commit blocks
  further writes until authoritative recovery and a durability fence resolve
  it; a timeout alone does not prove failure. Bind a collection-scoped operation
  ID to its canonical content hash before file I/O. Persist the committed
  outcome atomically with the catalog pointer. The same ID and hash return the
  original terminal outcome, including failure; a different hash conflicts.
  Outcomes survive restart and do not expire. Healthy terminal receipt lookups
  use the verified generation and query one catalog record without rereading
  warehouse files or enumerating history. Lookup is not a scrub: observed
  corruption or uncertain publication requires full reconciliation and its
  durability fence before lookups resume. The initial engine has no input
  payload journal or asynchronous job queue.
- **Snapshot reads.** Resolve the selected view once and enumerate its files
  through Iceberg manifests. Read neither uncommitted nor orphan files by
  scanning warehouse directories. Retain historical snapshots; an initial empty
  view stays empty after later writes. Pruning is conservative and followed by
  exact filtering with SQL three-valued NULL logic; only TRUE matches survive.
- **Types and ownership.** Preserve stable field IDs and validate against the
  fixed schema. Timestamps use UTC microseconds in years 0001 through 9999;
  preserve integer precision across interfaces. Cursors and returned buffers
  retain their allocator, accounting, executor, and I/O resources until the last
  dependent handle is released, even after the engine or cursor is closed.
- **Bounded resources and retained data.** Account for data, metadata, and
  control memory separately, including allocations outside Arrow. Check that a
  published state can be reopened within the same limits. Capacity exhaustion
  produces backpressure before publication, not partial success or hidden
  deletion of history. Publication rechecks physical warehouse usage after
  production; recovery checks the same physical cap without reserving a future
  attempt. Logical reachable
  usage is accumulated with overflow and limit checks across collections.
  Orphan files consume capacity; automatic cleanup is
  outside the initial scope. The only exception is a metadata root superseded
  by a confirmed append (the current root retains the whole history); it is
  removed once no scan is still resolving it. Detected corruption closes readiness; leave SQLite
  journal recovery to SQLite rather than deleting or truncating its journal.

## Repository navigation

| Path | Use |
| --- | --- |
| `public/storage/` | Installed C++ types, scan API and synchronous engine facade. |
| `core/api/`, `core/catalog/`, `core/engine/` | Canonical input, catalog, coordinator, and internal storage-backend seam. |
| `core/backend/` | Production StorageBackend over `glamdb_storage`, built with `GLAMDB_BUILD_STORAGE`. |
| `core/scan/` | Predicate evaluation, snapshot planning and Parquet scan cursor. |
| `core/io/` | Independent local filesystem, attempts, receipts, durable files and copying (`glamdb_io`). |
| `core/storage/` | FileIO, Parquet writer, Iceberg staging, publication/recovery and capacity checks (`glamdb_storage`). |
| `apps/cli/`, `apps/client/` | Standalone `glamdb-cli`: network database management and HTTP/HTTPS requests, without engine dependencies. |
| `apps/admin/` | Optional offline `glamdb-admin` store creation/inspection utility. |
| `apps/server/`, `apps/transport/` | `glamdb`: Asio HTTP server, shared precise codecs and native outcome serialization. |
| `cmake/` | Build options, sanitizers, quality targets, and dependency recipes. |
| `tests/` | Native unit/recovery/SqlCatalog/VFS/consumer suites, `vertical/` public-API storage tests, `mock/` in-memory scan tests, verification and actual SDK HTTP/HTTPS tests. |
| `tests/stress/` | Explicit manual native capacity campaigns; excluded from default pytest/CTest and CI. |
| `apps/mock_server/` | Disposable Python mock, real HTTP/Arrow, no warehouse/catalog access. |
| `fixtures/`, `tools/verification/` | Literal canonical/metadata inputs, independent row oracle and evidence helpers. |
| `sdk/python/`, `ui/` | Standalone Python HTTP SDK; HTML/CSS/ES-module pink workbench and browser/unit tests. |
| `tools/workload/` | Mimesis datasets, SDK/Locust workloads, per-worker SQLite outcome journals and full verification. |
| `probes/`, `benchmarks/` | Installed-dependency/compatibility probes and workload measurements. |
| `scripts/` | Developer tooling, including quality checks. |
| `third_party/` | Pinned upstream submodules and `lock.json`. |

Read [docs/PROJECT_STRUCTURE.md](docs/PROJECT_STRUCTURE.md) when placing new
components. Read [the workbench guide](docs/development/WORKBENCH.md) for
installation, runtime theme changes and testing. The [native HTTP contract](docs/development/HTTP_SERVER.md) and executable
`fixtures/http/v0.json` inventory supported routes and explicit gaps. Keep
transport dependencies outside the public API; do not invent missing metadata.
Shared literal outcome/scalar vectors live in `fixtures/http/outcomes-v0.json`;
see [the fixture guide](fixtures/http/README.md) for their native, SDK, mock and UI
consumers. Keep complete recovery contexts in test doubles. Missing recovery
fields are protocol errors; supported extension fields remain in client responses.
For branding/static hosting read [PUBLIC_DEMO.md](docs/development/PUBLIC_DEMO.md).
Preserve the supplied PNGs unchanged. Brand paths must be local assets; sizes
belong in theme.json. Asset URLs must support both / and /glamdb/.

## Build and development commands

Client/mock development uses Python 3.13 and Node 22+:

```sh
bash scripts/bootstrap-workbench.sh
npm --prefix ui exec -- playwright install chromium
bash scripts/check-workbench.sh
```

On Linux, install browser system dependencies with Playwright's `--with-deps`.
All network tests require localhost sockets; HTTPS tests require OpenSSL. The
check script runs real HTTP/HTTPS tests and Chromium, not only transport doubles.
Node API tests also open localhost sockets to exercise Fetch gzip/Brotli decoding
and Arrow cancellation. Session-isolation browser tests delay responses across
collection, snapshot and connection changes.
The check script also packages/serves the public demo under /glamdb/ and verifies no API
requests, token prompt, cross-visitor state or credential files are introduced.
See [validation](docs/development/WORKBENCH_VALIDATION.md) for exact evidence and
limits. Keep secrets in environment or process memory; never browser storage.
Mock faults require explicit development configuration. Do not give the mock a
production store path. Every production write must eventually use the C++ core.

Requirements: CMake 3.28+, Ninja, Python 3.9+, Linux, and a C++23 compiler and
standard library with `std::expected` (GCC 14+ or Clang 18+ with a matching STL).
Quality targets require LLVM major version 18 for both `clang-format` and
`clang-tidy`.

Native CI runs normal, ASan, UBSan, and TSan on PRs via `scripts/ci-native.sh`.
Pushes to `main` and manual runs also include MSan and LSan.
MSan/TSan rebuild the dependency prefix and pinned
libc++/libc++abi with instrumentation; other profiles use ordinary dependencies.
MSan disables Arrow's custom allocators at build time and uses the system allocator.
CTest discovers GoogleTest cases, and CMake discovers test `.cpp` files within
the unit, I/O, storage, mock, vertical, and fault suites. Tooling tests use unittest discovery.
The normal profile also runs named `vertical`, `nostorage` (a separate
`GLAMDB_BUILD_STORAGE=OFF` build) and `mock` contours.
Python tests use repository-wide pytest discovery (excluding native tooling,
manual stress suites, vendor, and generated trees); Node and Playwright use their own discovery.
All four PR native profiles are required checks and run on every PR, including
documentation-only changes. The main ruleset is tracked in
`.github/rulesets/main.json`; changes also need to be applied in GitHub.
The ruleset also requires `Workflow security` and `clang-format`.
Workbench checks run only on pushes to `main` and manual dispatch; pushes still
skip documentation-only changes. See
[test CI](docs/development/BUILDING.md#test-ci) for logs and reproduction.

```sh
git submodule update --init --recursive
cmake --preset deps-local
cmake --build --preset deps-local --parallel 2
cmake --preset local
cmake --build --preset local
ctest --preset local
```

The application links the pinned SQLite, Arrow and Iceberg installation. Tests
also require GoogleTest. Applications use pinned header-only CLI11, Asio and nlohmann/json submodules;
The network client uses system OpenSSL for verified TLS; the server uses its Crypto
component for Arrow page checksums.
These dependencies are not part of the public engine API or dependency installation. Use `release`
or `asan-ubsan` in place of `local` for the corresponding configure, build, and
test presets. `GLAMDB_BUILD_STORAGE` defaults to `ON` (unless `GLAMDB_IO_ONLY=ON`)
and adds `glamdb_storage`, the production backend, the scanner, `glamdb_vertical`
(CTest labels `vertical;scan`), the in-memory planner/cursor suite `glamdb_mock`
(labels `mock;scan`) and the fresh-process `glamdb.scan_process` integration test;
with it `OFF`, public factories install no backend and collection creation,
appends and scans report `DEPENDENCY_UNAVAILABLE`.
`glamdb.installed_consumer` copies its sources into the build workspace, links
only installed public targets, and runs independent write/reopen processes.
It requires real A/B appends, latest/A/initial rows, persisted receipts and
idempotent retries when storage is enabled, and explicit dependency errors
when disabled. Run it in both builds with `ctest --test-dir <build-dir> -R
'^glamdb.installed_consumer$' --output-on-failure`; mock tests are separate.
Core protocol tests use a test-only backend through `EngineFactory::Create/Open`;
the real SqlCatalog test verifies the catalog integration separately. Neither
establishes durable user-file production or power-loss safety.

For Linux tests on macOS, `./scripts/test-in-docker.sh asan-ubsan` builds only
independent I/O with `GLAMDB_IO_ONLY=ON`, without Arrow/Iceberg/SQLite. The full
build preserves the installed `glamdb::core` engine API. To include storage
adapters, build the pinned prefix with `./scripts/build-deps-in-docker.sh`, then
run `./scripts/test-storage-in-docker.sh asan-ubsan`. See
[BUILDING](docs/development/BUILDING.md) for isolation and sanitizer compatibility.
`scripts/glamdb-local.sh up|down|status|logs|cli` builds and runs a local server
(native on Linux, Docker elsewhere) configured by `GLAMDB_PORT`, `GLAMDB_TOKEN` and
`GLAMDB_DATA`; it rebuilds the dependency prefix when its recipe fingerprint changes.

The coordinator supplies owned, immutable normalized batches and expected
operation summaries. Backend admission checks capacity and allocates the writer
batch before binding; production reuses it under the call's owned-input reservation.
The reservation includes Arrow slot growth for NULL and omitted fields.
Allocation refusal consumes no operation ID. The coordinator
confirms the outer catalog COMMIT. A publication guard's `GetCandidate()` is
tentative until that confirmation. Scan callers retain a `ReadOnlyView()` so
reads do not join the writer's attempt. Isolated storage tests use an in-memory
catalog; coordinator tests use a separate test backend. With storage enabled,
`glamdb.durable_storage_catalog` also checks the real file/catalog publication
boundary and catalog-selected recovery in a fresh process.

`GLAMDB_BUILD_IO_PROBE=ON` adds the disposable storage fault probe and requires
`GLAMDB_BUILD_STORAGE=ON`. Its Python runner checks native receipts and optional
strace evidence; passing cases without strace remain incomplete. Recovery path
deduplication uses a bounded temporary SQLite database, separate from the catalog.
See [qualification preparation](docs/development/QUALIFICATION.md) for commands,
temporary-workspace prerequisites and the remaining qualification boundaries.

After configuring `local`, check project C++ sources and headers with:

```sh
cmake --build --preset local --target format-check tidy-check quality-negative-check
```

`quality-negative-check` verifies that deliberate formatting and analyzer
violations are rejected. `cmake --build --preset local --target format` rewrites
project C++ formatting. `.github/workflows/cpp-quality.yml` runs separate LLVM 18
format and tidy jobs. Formatting runs on PRs, pushes to `main`, and manual dispatch;
tidy runs only on pushes to `main` and manual dispatch. The tidy job verifies dependency
pins and reuses an exact installation cache, rebuilding on a miss. Its key covers
the build recipe, platform, toolchain, packages and flags. Storage, the I/O probe
and tests stay enabled; formatting requires no build. Tidy uses two concurrent processes
(`GLAMDB_TIDY_JOBS` overrides this), with grouped diagnostics and per-file timings. See
[docs/development/BUILDING.md](docs/development/BUILDING.md) for installation,
tool path overrides, and build details.

For dependency work, read [third_party/README.md](third_party/README.md).
Verify checked-out revisions without building them:

```sh
cmake -P cmake/dependencies/VerifySubmodules.cmake
```

When a task needs the dependency build, initialize submodules and use the
separate preset. It downloads upstream dependencies and installs to
`.deps/install`:

```sh
git submodule update --init --recursive
cmake --preset deps-local
cmake --build --preset deps-local --parallel 2
```

## Code and dependency conventions

The network CLI's `generate` and `bench write/read/verify/resolve` use the optional
Python package through reproc; the CLI still builds without engine dependencies.
See [the workload guide](tools/workload/README.md). Install its hash-pinned
`tools/workload/requirements.txt` and local package before testing. Run
`bash scripts/check-workload.sh unit`; the default `bash scripts/check-workload.sh`
requires the built real server and CLI and runs native TLS E2E without mock fallback.
The normal native CI contour includes this E2E. Mock transports belong only in
explicit fault tests. Preserve exact saved append UUID/body/hash, never silently
retry or reconcile, and count only unique committed operations. Locust owns
scheduling, distributed statistics and percentiles; do not implement another load
engine or duplicate SDK codecs. Token values stay exclusively in the environment.

Follow [the C++ style guide](docs/development/CPP_STYLE_GUIDE.md) for naming,
declarations, layout, and error handling. Keep `.clang-format` aligned with its
mechanical rules; vendor and generated code are excluded.

- Use C++23 with the checked-in `.clang-format`, `.clang-tidy`, and
  `.editorconfig` as the formatting and diagnostic authorities. Project compiler
  warnings and configured tidy diagnostics are errors. The C++ line limit is
  120 columns.
- Keep each component's private `.h` and `.cpp` together in `core/`. Installed
  public headers belong in `public/storage/` and must be usable without private
  headers, source-tree paths, or transport dependencies.
- Make ownership and error propagation explicit. Recoverable I/O, validation,
  and capacity failures must remain reportable outcomes, not process aborts or
  fabricated successful results.
- Prefer public dependency APIs. Check the pinned source before relying on
  upstream behavior, especially I/O hooks, close semantics, and transaction
  boundaries. Record any necessary upstream patch with its dependency revision.
- Treat `third_party/lock.json` as the authority for direct dependency pins.
  Change submodule revisions and matching lock entries together; preserve
  archive verification and upstream licenses. Keep unrelated vendor edits out
  of application changes.
- Keep generated builds, dependencies, and experiment output in the ignored
  `build/`, `.deps/`, and `artifacts/` directories.

## Validation and reporting

- For behavior changes, add and run focused tests in the appropriate suite.
  Storage tests need independent expected results, including duplicate rows,
  fixed snapshots, retries, and failures at the affected persistence boundary.
  Use a fresh process for reopen/recovery tests and sanitizers for lifetime work.
- Run the relevant build, tests, and quality checks for C++ changes. For
  documentation-only changes, verify factual claims, commands, and links;
  engine or dependency builds are unnecessary unless the change concerns them.
  Read `.github/workflows/` when changing CI; the current workflow-security
  checks apply to every pull request, including documentation changes.
- Separate designed behavior, source inspection, executed PASS/FAIL, and
  checks not run. Report exact commands, results, and missing prerequisites.
  A smoke test does not establish engine correctness or durability.
- Process crashes and power loss require different evidence. `kill -9` does
  not discard the OS cache. Initial durability qualification targets Linux
  x86-64 on a dedicated local ext4 volume with verified flush behavior; macOS
  builds and simulated faults alone do not qualify that environment. Record
  environment details with measurements and durability results.
  See [qualification preparation](docs/development/QUALIFICATION.md) for the
  separate controller, evidence and remaining campaign requirements.
- Run destructive fault tests only on disposable test stores or dedicated test
  volumes. Keep credentials and private data out of fixtures, traces, and logs.

## Contributions and security

For branch, pull request, or dependency publication work, read
[CONTRIBUTING.md](CONTRIBUTING.md). Development branches belong in forks;
upstream keeps only `main`. Use the repository's pull request template to
describe behavior, validation, and any storage compatibility impact.

When changing workflows, preserve SHA-pinned Actions, read-only PR permissions,
and separation from privileged release credentials. For vulnerabilities, follow
[SECURITY.md](SECURITY.md) and report privately rather than in public issues.
Keep `.github/allowed-actions.json` and GitHub's selected Actions setting in sync;
an unlisted Action prevents the entire workflow from starting.

Native HTTP verification includes `glamdb.http_review` for framing/admission and
`glamdb.native_http_fault` for registry, publication and listener recovery faults.
Configure `Python3_EXECUTABLE` with SDK, PyArrow and
JSON Schema dependencies (`requirements-native-ci.txt` for the CI Python/platform),
then run `GLAMDB_REQUIRE_NATIVE_CLIENTS=1 GLAMDB_VALIDATE_SCHEMAS=1 ctest --preset local -R 'glamdb\.(native_http|transport\.|cli)'`.
The server requires `GLAMDB_BUILD_STORAGE=ON`; storage-disabled builds retain
the network CLI and codecs. `GLAMDB_BUILD_ADMIN=ON` additionally builds the offline
`glamdb-admin`; it is not required to initialize or operate a server. Run native browser checks with `GLAMDB_NATIVE_SERVER`
pointing to the built server; `GLAMDB_E2E_PYTHON` can select
the workbench Python environment. See the native HTTP guide for exact commands.

SIGINT/SIGTERM close socket and scan admission and mark active scans cancelled
before worker draining. Cancellation never waits on a scan's session lock; retain
its read lease until the call finishes. Accepted writes still drain, and shutdown
deadlines or lost COMMIT replies never establish a definite failed outcome.

Native CI requires SDK/Arrow/schema checks in every profile and native browser
acceptance plus a standalone client build in the normal profile. MSan uses the
ordinary standalone TLS client against the instrumented server; system OpenSSL
is not instrumented. Test-only native HTTP VFS faults model COMMIT uncertainty
and lifecycle draining; they do not establish physical power-loss durability.
