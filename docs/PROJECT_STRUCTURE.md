# Project Structure

```text
glamdb/
├── AGENTS.md
├── CMakeLists.txt
├── CMakePresets.json
├── .clang-format
├── .clang-tidy
├── .editorconfig
├── .gitmodules
├── apps/
│   ├── mock_server/
│   ├── cli/
│   ├── client/
│   ├── admin/
│   ├── server/
│   └── transport/
├── public/
│   └── storage/
├── core/
│   ├── CMakeLists.txt
│   ├── api/
│   ├── backend/
│   ├── catalog/
│   ├── engine/
│   ├── io/
│   ├── scan/
│   ├── storage/
│   └── telemetry/
├── deploy/logging/
├── docker/
│   ├── dev/
│   └── deps/
├── cmake/
│   ├── CompilerOptions.cmake
│   ├── Sanitizers.cmake
│   ├── Quality.cmake
│   └── dependencies/
│       └── sqlite/
├── tests/
│   ├── support/
│   ├── verification/
│   ├── unit/
│   ├── tooling/
│   ├── integration/
│   ├── fault/
│   ├── vertical/
│   └── installed_consumer/
├── fixtures/
├── deploy/alerting/
├── tools/alerting/
├── tools/verification/
├── tools/monitoring/
├── deploy/monitoring/
├── probes/
├── benchmarks/
├── sdk/
├── ui/
├── scripts/
├── docs/
│   ├── ARCHITECTURE.md
│   ├── PROJECT_STRUCTURE.md
│   ├── ROADMAP.md
│   └── development/
└── third_party/
    ├── arrow/
    ├── iceberg-cpp/
    ├── googletest/
    ├── lock.json
    └── README.md
```

| Path | Purpose |
| --- | --- |
| `AGENTS.md` | Repository guidance for coding agents, maintained alongside implementation changes. |
| `public/storage/` | Installed engine API/request/outcome/scan/metadata types, metric provenance and the independent storage schema DTO. |
| `core/api/`, `core/catalog/`, `core/engine/` | `glamdb_core` / `glamdb::core`: canonical input, SQLite catalog, coordinator, and public facade. |
| `core/backend/` | Storage backend bridging the coordinator seam to `glamdb_storage` (built with `GLAMDB_BUILD_STORAGE`). |
| `core/resources/` | Shared memory/device owner, Arrow pools and conservative working envelopes. |
| `core/scan/` | Predicate evaluation (always built), conservative zone pruning, snapshot planning, bounded metadata/footer cache and positional Parquet scan cursor (storage builds). |
| `core/telemetry/` | Internal `glamdb_logging`: bounded typed operational JSON, actual process counters and runtime-collected stderr. |
| `deploy/logging/`, `tools/logging/` | Pinned collection/storage/search stack, Python control-plane logger and ingestion/redaction probes. |
| `core/io/` | Independent `glamdb_io`: local filesystem, store directories/URIs, attempts, receipts, SHA-256, durable files and copying. |
| `core/storage/` | `glamdb_storage`: FileIO adapter, profiles, Parquet writer, Iceberg staging, publication, recovery and capacity. |
| `docker/` | Linux development and dependency toolchains; builds mount product sources read-only. |
| `apps/cli/`, `apps/client/` | Standalone HTTP/HTTPS CLI and network transport, without core dependencies. |
| `apps/admin/` | Optional offline store creation, data and inspection commands through the facade, plus HTTP mode. |
| `apps/server/`, `apps/transport/` | Native Asio server and shared JSON/HTTP codecs; no independent writer. |
| `cmake/` | Compiler options, sanitizers, quality targets, and dependency recipes. |
| `tests/` | Component/storage pipeline tests, catalog/coordinator tests, process recovery, VFS faults, installed consumers, tooling, and independent client/verification suites. |
| `fixtures/` | Shared deterministic test inputs and expected outputs. |
| `apps/mock_server/` | Disposable in-memory adapter with real HTTP/Arrow and runtime config. |
| `apps/workbench_access/` | Organization authorization catalog, local operator CLI, HTTPS authentication and database routing. |
| `tools/verification/` | Literal canonical vectors, SQL 3VL oracle and receipt/fault-evidence tools. |
| `deploy/alerting/`, `tools/alerting/` | Configurable four-state rules, native Alertmanager Telegram delivery, independent observer, captured outer-edge replay and transition tests; see [ALERTING.md](../ALERTING.md). |
| `probes/` | Standalone installed-dependency consumer and libc++ sanitizer compatibility reproducer. |
| `benchmarks/` | Performance and capacity measurements. |
| `sdk/python/` | Independently installable network SDK, unit tests and quickstart. |
| `ui/` | Source-served HTML/CSS/ES modules, live theme JSON, Node unit and Playwright browser tests. |
| `scripts/` | Developer tooling. |
| `docs/development/` | Build and tooling instructions. |
| `docs/ARCHITECTURE.md` | Intended engine semantics, persistence boundaries, and resource ownership. |
| `docs/ROADMAP.md` | Implementation stages, prerequisites, and acceptance evidence. |
| `third_party/` | Pinned upstream submodules and the direct dependency lock. |

The tree contains the catalog/coordinator, independent storage primitives, and
the production storage backend plus minimal scanner connecting them
([SCANNER.md](development/SCANNER.md)), tested against the pinned Arrow,
Parquet, Iceberg and SQLite dependencies. The native HTTP server and network CLI
([HTTP_SERVER.md](development/HTTP_SERVER.md)) serve that engine to the SDK and
workbench; the disposable mock remains a separate development profile. Read
[the module handoff](development/STORAGE_MODULE.md) for storage APIs and tests,
[ARCHITECTURE.md](ARCHITECTURE.md) for engine responsibilities,
[ROADMAP.md](ROADMAP.md#current-status) for current and remaining acceptance
evidence, and [the workbench guide](development/WORKBENCH.md) for runnable
client paths.

Generated files belong in ignored `build/`, `.deps/`, and `artifacts/` directories.

## Engine boundaries

Add components when their behavior is implemented rather than creating empty
files for every anticipated module. The intended responsibilities are:

| Area | Responsibility |
| --- | --- |
| Public API | Store/collection operations, requests/results, typed predicates, and scan cursors with schema and snapshot information. |
| Write and recovery coordination | Serialized writes, prepared bases, operation identities/outcomes, publication, and reconciliation. |
| Local I/O | Immutable file creation, durable close, path validation, file receipts, and a traceable fault-injection boundary. |
| Catalog | Persistent SQLite connection, exact-base checks, atomic pointer/outcome publication, and recovery fencing. |
| Storage integration | Arrow/Parquet writers and readers plus Iceberg metadata, snapshots, and metrics. |
| Scanning | Fixed-view planning, conservative pruning, projection, and exact filtering. |
| Runtime | Shared budgets, cancellation, resource leases, and store ownership. |

The public facade and catalog/coordinator are implemented. With
`GLAMDB_BUILD_STORAGE` (default unless IO-only), `core/backend/` installs the real
storage backend behind `core/engine/storage_backend.h` and `core/scan/` serves
`Engine::Scan` with compound SQL three-valued predicates, positional Parquet column/row-group reads, conservative manifest/row-group pruning and bounded immutable metadata/footer caching. Installed public headers work without private headers.
`GLAMDB_IO_ONLY=ON` builds independent I/O and logging without the engine or CLI; the default
full build preserves the installed `glamdb::core` package.
Transport, SDK, and UI dependencies stay outside `public/storage/` and `core/`;
all production write paths still pass through the engine.

## Validation layout

`tests/unit/io/` and `tests/unit/storage/` hold component tests registered with
CTest label `unit`. Storage tests are enabled by `GLAMDB_BUILD_STORAGE=ON`.
`tests/support/` provides the filesystem fault injector and durability trace
checker. The publication guard suite also writes real data/metadata and reopens
the retained closure in a fresh process. GoogleTest is configured only when
`BUILD_TESTING=ON`; it is neither a product dependency nor installed.

`tests/tooling/` uses Python's standard library to test the Docker runner and
CMake configuration, including error propagation, source/build isolation,
optional GoogleTest, static targets, installation contents, formatting rules,
and standalone-source classification for quality checks.
Its CTest label is `tooling`.

`tests/integration/` covers CLI behavior, separate-process recovery, and real
Iceberg catalog ordering. `tests/fault/` injects SQLite VFS/coordinator failures.
`tests/installed_consumer/` builds and runs against the installed public package.
Those coordinator tests use a storage test double; the independent real-file
pipeline lives in `tests/unit/storage/`. The installed third-party consumer
lives in `probes/build_install/`.

`tests/stress/` holds explicitly launched native load campaigns with disposable
stores, Locust measurements and reopen verification. See its
[manual runner](../tests/stress/README.md); it is excluded from default
pytest/CTest discovery and CI.

`probes/` holds bounded dependency/platform experiments, while `benchmarks/`
holds workload and capacity measurements. Neither a dependency probe nor a
benchmark replaces correctness tests. Share deterministic inputs and expected
results through `fixtures/`; put generated stores, traces, and measurements in
ignored output directories. See [BUILDING.md](development/BUILDING.md),
[COORDINATOR.md](development/COORDINATOR.md), and
[STORAGE_MODULE.md](development/STORAGE_MODULE.md) for commands and test scope.

Independent Python suites live in `tests/verification/`, `sdk/python/tests/`,
`apps/mock_server/tests/` and `tests/integration/test_network_sdk.py`; browser
tests live in `ui/tests/`. Their results do not qualify the full native storage integration.

`tools/acceptance/` contains the installed persistent cross-client recipe, native
quiesce/backup orchestrator, independent Parquet/Avro inspector and sanitized
Chromium evidence capture. `tests/system/` covers its owned-path and trace
boundaries; normal native CI invokes the documented real-service recipe.
