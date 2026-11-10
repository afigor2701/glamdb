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
and store-management CLI. Independent durable I/O and Arrow/Parquet/Iceberg
storage primitives provide file production, staging, publication validation,
recovery verification, and capacity checks. `core/backend/` connects them to the
coordinator StorageBackend and `core/scan/` implements a minimal `Engine::Scan`
(fixed snapshot, one-comparison predicate, projection, limit; no pruning). See
[the scanner guide](docs/development/SCANNER.md) for their contract and
validation status, [the coordinator guide](docs/development/COORDINATOR.md)
for the integration contract, and [the storage guide](docs/development/STORAGE_MODULE.md)
for the file-layer contract. Full engine durability remains unqualified.

Independent verification tools, a Python network SDK and a browser workbench
are implemented. They integrate through a separate, explicitly disposable
in-memory mock adapter, not a production storage writer. The architecture and
invariants below describe the intended engine, not qualified guarantees.

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
and an explicit KV collection mode. Production HTTP/core integration and
object storage remain later extensions. Compaction, snapshot expiration,
garbage collection, asynchronous admission, replication, multiple writers, and
schema evolution are outside the initial append scope. Consult
[docs/ROADMAP.md](docs/ROADMAP.md) when assessing feature scope.

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
  Outcomes survive restart and do not expire. The initial engine has no input
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
  deletion of history. Orphan files consume capacity; automatic cleanup is
  outside the initial scope. Detected corruption closes readiness; leave SQLite
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
| `apps/glamdb/` | Store-management CLI using CLI11 and transport-only outcome serialization. |
| `cmake/` | Build options, sanitizers, quality targets, and dependency recipes. |
| `tests/` | Native unit/recovery/SqlCatalog/VFS/consumer suites, `vertical/` public-API storage tests, `mock/` in-memory scan tests, verification and actual SDK HTTP/HTTPS tests. |
| `apps/mock_server/` | Disposable Python mock, real HTTP/Arrow, no warehouse/catalog access. |
| `fixtures/`, `tools/verification/` | Literal canonical/metadata inputs, independent row oracle and evidence helpers. |
| `sdk/python/`, `ui/` | Standalone Python HTTP SDK; HTML/CSS/ES-module pink workbench and browser/unit tests. |
| `probes/`, `benchmarks/` | Installed-dependency/compatibility probes and workload measurements. |
| `scripts/` | Developer tooling, including quality checks. |
| `third_party/` | Pinned upstream submodules and `lock.json`. |

Read [docs/PROJECT_STRUCTURE.md](docs/PROJECT_STRUCTURE.md) when placing new
components. Read [the workbench guide](docs/development/WORKBENCH.md) for
installation, runtime theme changes and testing. Its HTTP shapes are provisional
mock adapter contracts; do not change the public C++ API to match them silently.
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
The check script also packages/serves the public demo under /glamdb/ and verifies no /v0/
requests, token prompt, cross-visitor state or credential files are introduced.
See [validation](docs/development/WORKBENCH_VALIDATION.md) for exact evidence and
limits. Keep secrets in environment or process memory; never browser storage.
Mock faults require explicit development configuration. Do not give the mock a
production store path. Every production write must eventually use the C++ core.

Requirements: CMake 3.28+, Ninja, Python 3.9+, Linux, and a C++23 compiler and
standard library with `std::expected` (GCC 14+ or Clang 18+ with a matching STL).
Quality targets require LLVM major version 18 for both `clang-format` and
`clang-tidy`.

Native CI runs normal, ASan, UBSan, MSan, TSan, and LSan independently via
`scripts/ci-native.sh`. MSan/TSan rebuild the dependency prefix and pinned
libc++/libc++abi with instrumentation; other profiles use ordinary dependencies.
MSan disables Arrow's custom allocators at build time and uses the system allocator.
CTest discovers GoogleTest cases, and CMake discovers test `.cpp` files within
the unit, I/O, storage, mock, vertical, and fault suites. Tooling tests use unittest discovery.
The normal profile also runs named `vertical`, `nostorage` (a separate
`GLAMDB_BUILD_STORAGE=OFF` build) and `mock` contours.
Python tests use repository-wide pytest discovery (excluding native tooling,
vendor, and generated trees); Node and Playwright use their own discovery.
Both test workflows skip documentation-only changes. See
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
also require GoogleTest. The CLI uses the pinned, header-only CLI11 submodule;
it is not part of the public engine API or dependency installation. Use `release`
or `asan-ubsan` in place of `local` for the corresponding configure, build, and
test presets. `GLAMDB_BUILD_STORAGE` defaults to `ON` (unless `GLAMDB_IO_ONLY=ON`)
and adds `glamdb_storage`, the production backend, the scanner, `glamdb_vertical`
(CTest labels `vertical;scan`), the in-memory planner/cursor suite `glamdb_mock`
(labels `mock;scan`) and the fresh-process `glamdb.scan_process` integration test;
with it `OFF`, public factories install no backend and `Engine::Scan` reports
`DEPENDENCY_UNAVAILABLE`.
Core protocol tests use a test-only backend through `EngineFactory::Create/Open`;
the real SqlCatalog test verifies the catalog integration separately. Neither
establishes durable user-file production or power-loss safety.

For Linux tests on macOS, `./scripts/test-in-docker.sh asan-ubsan` builds only
independent I/O with `GLAMDB_IO_ONLY=ON`, without Arrow/Iceberg/SQLite. The full
build preserves the installed `glamdb::core` engine API. To include storage
adapters, build the pinned prefix with `./scripts/build-deps-in-docker.sh`, then
run `./scripts/test-storage-in-docker.sh asan-ubsan`. See
[BUILDING](docs/development/BUILDING.md) for isolation and sanitizer compatibility.

The coordinator supplies owned, immutable normalized batches and expected
operation summaries, runs capacity preflight before binding an attempt, and
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
