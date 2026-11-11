# GlamDB

An open-source C++23 columnar key-value storage engine in early development.

GlamDB combines Arrow batches, immutable Parquet files, Iceberg snapshots, and a
persistent SQLite catalog. The C++ core owns writes, commit ordering, recovery,
and snapshot reads; the native HTTP server and clients use that same core.

The first engine milestone is an embedded, synchronous **append store** on
Linux and local persistent storage, with one owning process and one writer.
Append preserves duplicate keys. Key-value upserts and deletes follow after
local durability qualification, with further network capabilities and S3
as separate extensions.
See the [architecture](docs/ARCHITECTURE.md) for semantics and the
[roadmap](docs/ROADMAP.md) for scope and acceptance criteria.

## Current status

The SQLite catalog, write/recovery coordinator, canonical Arrow input, public
C++ facade, outcome JSON and network CLI are implemented. Independent
storage primitives implement durable local I/O, Parquet writing, Iceberg append
staging, publication validation, reachable-file recovery, and capacity checks.
Tests cover real files, injected failures and fresh-process recovery.

A storage backend connecting these primitives to the coordinator and a minimal
scanner (`Engine::Scan`) are implemented and tested through `glamdb_vertical`;
see [SCANNER.md](docs/development/SCANNER.md).
The [installed public API consumer](docs/development/BUILDING.md#build-and-install)
checks real A/B appends, fixed-view scans, persisted receipts and retries after
reopening in a new process, with a separate storage-disabled contour.
Healthy terminal receipt lookups reuse the verified generation without rereading
warehouse history; observed corruption and uncertainty still require recovery.
See the [coordinator guide](docs/development/COORDINATOR.md)
and [storage guide](docs/development/STORAGE_MODULE.md) for supported boundaries.
The [shared resource budget matrix](docs/development/RESOURCE_BUDGETS.md) names
limit units, owners and enforcement points, including retained buffers and
completion/recovery reserves. It separates existing local checks from aggregate
memory and device reservations still required for a bounded process.
Power-loss, capacity and ingestion qualification (G4) are **NOT RUN**. The
[roadmap status](docs/ROADMAP.md#current-status) separates implemented code
from executed evidence for each gate.

Available now: independent golden fixtures, a typed SQL-NULL reference oracle,
receipt/fault-evidence tools, a network Python SDK, and a configurable pink
browser workbench. The [native HTTP server](docs/development/HTTP_SERVER.md)
connects them to persistent storage and serves the same UI. It supports typed
create/append/scan operations, JSON and Arrow, with explicit capabilities for
unavailable metadata/explain APIs. A separate **disposable in-memory mock**
remains available for development. Its data files,
snapshots and commits are synthetic and disappear on restart.

The embedded C++ facade also exposes [native metadata](docs/development/METADATA.md):
capabilities, receipt-backed snapshot history, selected-view statistics,
snapshot-bound file and row-group details, scan explain and execution profiles.
HTTP metadata/explain adapters remain pending.

The workbench also has a **public browser demo** requiring no token or Python
server. Each page instance owns its sample data; visitors can create/append,
filter, explore snapshots and export JSON. Reload resets only that visitor's
demo. See [public demo and publishing](docs/development/PUBLIC_DEMO.md).
The supplied wordmark/transparent mark are used in login, sidebar, avatar and
favicon; artwork paths and sizes remain runtime theme settings.

**[Open the public demo](https://nnrpi.github.io/glamdb/)** — anyone can try it.

For persistent data, [run the native server locally](#run-locally).
Launch the disposable mock workbench (Python 3.13 and Node 22+):

```sh
bash scripts/bootstrap-workbench.sh
export GLAMDB_TOKEN='choose-a-local-development-token'
.venv/bin/python -m glamdb_mock --ui-dir ui --seed-demo
```

Open **http://127.0.0.1:8765** and enter that token. Edit
[`ui/config/theme.json`](ui/config/theme.json) for live colors, fonts, layout,
branding and bows/sparkles/kisses. No compilation or restart is needed.

See the [workbench guide](docs/development/WORKBENCH.md),
[SDK](sdk/python/README.md), [independent tools](tools/verification/README.md),
[adapter contract](docs/development/HTTP_MOCK_CONTRACT.md), and
[validation report](docs/development/WORKBENCH_VALIDATION.md).

## Run locally

`scripts/glamdb-local.sh` builds what changed and runs the native server `glamdb`
in the background: natively on Linux (GCC 14+ or Clang 18+, CMake 3.28+, Ninja,
OpenSSL and zlib headers), in Docker elsewhere (Docker Desktop or
[Colima](docs/development/BUILDING.md#linux-tests-with-docker)). The first run
also initializes submodules and builds the pinned dependencies (30–60 minutes).

```sh
export GLAMDB_PORT=8080                       # optional, default 8080
export GLAMDB_TOKEN=$(openssl rand -hex 16)   # optional, else generated once
./scripts/glamdb-local.sh up                  # build, (re)start, wait until healthy
./scripts/glamdb-local.sh status              # URL, token and data location
./scripts/glamdb-local.sh logs -f
./scripts/glamdb-local.sh down                # stop; data is kept
```

Open the printed URL and enter the token. Without `GLAMDB_TOKEN`, `up` keeps a
generated owner-only token in `~/.local/share/glamdb/token`. Data lives in
`GLAMDB_DATA` (default `~/.local/share/glamdb/data` or Docker volume `glamdb-data`)
and survives restarts. Writes need max(1 GiB, 5% of the data filesystem) plus
512 MiB free, otherwise they fail with HTTP 507; `up` warns about that.
`./scripts/glamdb-local.sh --help` lists all settings.

Call the API with curl (int64 values are decimal strings) or the bundled CLI:

```sh
export GLAMDB_TOKEN=${GLAMDB_TOKEN:-$(cat ~/.local/share/glamdb/token)}
api() { curl -sS -H "Authorization: Bearer $GLAMDB_TOKEN" -H 'Content-Type: application/json' "http://127.0.0.1:${GLAMDB_PORT:-8080}$@"; echo; }
api /databases -d '{"name":"demo","database_id":"11111111-1111-4111-8111-111111111111"}'
D=/databases/11111111-1111-4111-8111-111111111111
api $D/collections -d '{"collection_id":"22222222-2222-4222-8222-222222222222","name":"events",
  "schema":{"fields":[{"name":"key","type":"string","nullable":false},{"name":"n","type":"int64","nullable":true}]}}'
C=$D/collections/22222222-2222-4222-8222-222222222222
api $C/append -d '{"operation_id":"33333333-3333-4333-8333-333333333333","rows":[{"key":"a","n":"1"},{"key":"b"}]}'
api $C/scan -d '{}'
./scripts/glamdb-local.sh cli request --path /databases
```

See the [server guide](docs/development/HTTP_SERVER.md) for manual startup and all
routes, and the [CLI guide](apps/cli/README.md) for HTTPS and Arrow output.

## Build and test

Build the pinned dependencies once, then build and test the native C++ core on
Linux:

```sh
git submodule update --init --recursive
cmake --preset deps-local
cmake --build --preset deps-local --parallel 2
cmake --preset local
cmake --build --preset local
ctest --preset local
```

On macOS, `./scripts/test-in-docker.sh asan-ubsan` tests independent I/O in Linux
without the full dependency prefix. To include the engine and storage adapters:

```sh
./scripts/build-deps-in-docker.sh
./scripts/test-storage-in-docker.sh asan-ubsan
```

The full build installs `glamdb::core` and public headers. Storage adapters
(`GLAMDB_BUILD_STORAGE`) are on by default; `GLAMDB_IO_ONLY=ON` provides a separate
lightweight build. See the build guide for Docker setup.

Store readiness is available through authenticated
`GET /databases/{id}/readyz` (legacy single-store mode: `/readyz`) and scoped
capabilities. It reports read availability separately from write readiness;
`/healthz` remains process liveness. All backends explicitly remain
`durability_qualified=false`. See the [health contract](docs/development/HTTP_SERVER.md).

- [Project structure](docs/PROJECT_STRUCTURE.md)
- [Build and quality checks](docs/development/BUILDING.md)
- [C++ style guide](docs/development/CPP_STYLE_GUIDE.md)
- [Dependencies](third_party/README.md)
- [Agent guide](AGENTS.md)
- [Contributing](CONTRIBUTING.md)
- [Security policy](SECURITY.md)
