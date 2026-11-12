<p align="center">
  <img src="ui/assets/brand/wordmark-transparent.png" alt="GlamDB" width="720">
</p>

# GlamDB

GlamDB is an open-source columnar database written in C++23. It stores typed
records in Parquet files, manages snapshots with Apache Iceberg, and uses SQLite
for its catalog and write outcomes. Use it as an embedded C++ library or through
the HTTP server, browser workbench, command-line client, and Python SDK.

<sub>GlamDB is a software project developed for a hackathon at ITMO University
and is not intended for production use.</sub>

**GlamDB is in early development.** The current engine is an append-only store
for local disk, with one owning process, one serialized writer per store, and
concurrent snapshot readers. Power-loss durability qualification is still pending.

## Features

- **Typed collections.** Fixed schemas with strings, booleans, int64, float64,
  UTC microsecond timestamps, and nullable fields. Appends preserve every row,
  including duplicate keys.
- **Snapshot reads.** Query the latest committed view or a retained historical
  snapshot. Select columns, limit results, and combine comparisons, membership,
  and NULL predicates with AND, OR, and NOT; scans prune candidate files and
  Parquet row groups conservatively and read only needed column ranges.
- **Explicit write outcomes.** Persistent operation IDs support exact retries
  and outcome lookup after restart. Writes distinguish committed, definitely
  not committed, and unknown outcomes.
- **Open columnar formats.** Arrow batches in memory, immutable Parquet data
  files, and Iceberg v2 tables with retained snapshot history.
- **Data inspection.** Browse schemas, snapshot history, statistics, files,
  Parquet row groups, scan plans, and execution profiles.
- **Connected tools.** Manage multiple databases through one authenticated
  server. Read and write through the UI, CLI, or Python SDK; retrieve scan results
  as JSON or Arrow IPC with fixed-snapshot pagination.

Queries use a typed predicate API; there is no SQL query interface. Collections
are currently unpartitioned and append-only. Upsert, delete, schema evolution,
compaction, snapshot expiration, object storage, and replication are not available.

### In development

Current work focuses on qualifying and improving the existing append engine:

- Recovery and power-loss qualification, including controlled physical failures
  ([#66](https://github.com/glamourdb/glamdb/issues/66)).
- Performance and concurrent sessions: configurable scan quotas, lower append
  overhead, and measured capacity limits
  ([#109](https://github.com/glamourdb/glamdb/issues/109)).
- Page-level and Bloom-filter pruning and broader scan memory qualification
  ([#46](https://github.com/glamourdb/glamdb/issues/46)).

### Planned

- Streaming bulk ingestion with one atomic publication
  ([#100](https://github.com/glamourdb/glamdb/issues/100)).
- An explicit key-value mode with copy-on-write upserts, deletes, and batched
  point lookups ([#101](https://github.com/glamourdb/glamdb/issues/101)).
- Configurable Parquet compression, encodings, and dictionary policies
  ([#97](https://github.com/glamourdb/glamdb/issues/97),
  [#98](https://github.com/glamourdb/glamdb/issues/98)).
- An opt-in small-write buffering and write-ahead log extension
  ([#68](https://github.com/glamourdb/glamdb/issues/68)).

These extensions are not part of the current API. Development is tracked in
[GitHub issues](https://github.com/glamourdb/glamdb/issues).

**[Open the public demo](https://nnrpi.github.io/glamdb/)** — anyone can try it.
The [organization workbench](docs/development/ORGANIZATIONS.md) uses Keycloak
sign-in, OAuth2 Proxy sessions and Cerbos policies for isolated database access.
See the [deployment and operating guide](docs/development/SSO_OPERATIONS.md)
for HTTPS hosting, native account management and database ownership workflows.
The static Pages demo contains disposable sample data and remains a separate
publication; it does not connect to protected database servers.

## Get started

Clone the repository and start a local server:

```sh
git clone --recurse-submodules https://github.com/glamourdb/glamdb.git
cd glamdb
export GLAMDB_TOKEN="$(openssl rand -hex 32)"
./scripts/glamdb-local.sh up
./scripts/glamdb-local.sh status
```

The helper builds the server and its pinned dependencies, then starts it in the
background. On Linux, it uses the native toolchain described under
[Build from source](#build-from-source); on macOS, it uses a running Docker
daemon. Allow 30–60 minutes for the first dependency build.

Open **http://127.0.0.1:8080** and enter the value of `GLAMDB_TOKEN`.
The server starts with no databases. Create a database, create a collection with
its schema, then append and query rows in the workbench. The same UI provides
snapshot selection, storage inspection, and scan explain views.

Data survives server restarts. The default location is
`~/.local/share/glamdb/data` on Linux, or the `glamdb-data` Docker volume.
Set `GLAMDB_DATA` to choose another directory or volume and `GLAMDB_PORT` to
change the port. Writes require free space of at least the larger of 1 GiB or
5% of the data filesystem, plus a 512 MiB reserve.

```sh
./scripts/glamdb-local.sh logs -f  # Follow server logs
./scripts/glamdb-local.sh down     # Stop the server and keep its data
```

Keep the token for subsequent starts, or omit `GLAMDB_TOKEN` and let the helper
generate and reuse a token file; `status` shows its location. The service token
grants access to all databases. Remote connections use HTTPS through a TLS proxy;
see the [server guide](docs/development/HTTP_SERVER.md) for deployment and API details.

## Use GlamDB

The examples below use the local server and token from the quick start. Run
commands from the repository root.

### Command line

`glamdb-cli` is a standalone HTTP/HTTPS client. The local helper runs it with the
server's address and token. Create a database, define a collection, append two
rows, and read them back:

```sh
./scripts/glamdb-local.sh cli request --path /databases --body - <<'JSON'
{"name":"cli-example","database_id":"11111111-1111-4111-8111-111111111111"}
JSON

COLLECTION=/databases/11111111-1111-4111-8111-111111111111/collections
./scripts/glamdb-local.sh cli request --path "$COLLECTION" --body - <<'JSON'
{
  "collection_id": "22222222-2222-4222-8222-222222222222",
  "name": "events",
  "schema": {"fields": [
    {"name": "key", "type": "string", "nullable": false},
    {"name": "value", "type": "int64", "nullable": true}
  ]}
}
JSON

COLLECTION=$COLLECTION/22222222-2222-4222-8222-222222222222
./scripts/glamdb-local.sh cli request --path "$COLLECTION/append" --body - <<'JSON'
{
  "operation_id": "33333333-3333-4333-8333-333333333333",
  "rows": [{"key": "a", "value": "1"}, {"key": "b", "value": "2"}]
}
JSON

printf '%s' '{}' | ./scripts/glamdb-local.sh cli request \
  --path "$COLLECTION/scan" --body -
```

Int64 values use decimal strings in JSON to preserve precision. The `request`
command exposes every HTTP route; `--body` implies POST. Scan responses include
`next_cursor` when another page is available. For scripts that consume complete
results, the Python SDK handles pagination automatically.

Named commands also cover `create-collection`, `collections`, `resolve-collection`,
`collection`, `schema`, `append`, `scan`, `operation`, `snapshots`, `files`, and
`file`. Use `--url` and `--database-id` with `glamdb-cli`. The optional
`glamdb-admin` offers the same commands offline with `--root` and `--store-id`,
or remotely with `--url`. Named scans complete HTTP pages within a 10,000-row,
16 MiB result bound. See the [CLI guide](apps/cli/README.md) for commands, input
formats and the distinct failure/unresolved exit codes.

### Python SDK

Install the SDK from the checkout with Python 3.10 or later:

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install ./sdk/python
```

```python
import os
from uuid import UUID

from glamdb_sdk import Client, Field, Schema, col

url = "http://127.0.0.1:8080"
token = os.environ["GLAMDB_TOKEN"]
schema = Schema([Field("key", "string", False), Field("value", "int64", True)])
collection_id = UUID("44444444-4444-4444-8444-444444444444")
operation_id = UUID("55555555-5555-4555-8555-555555555555")

with Client(url, token=token) as admin:
    database = admin.create_database("python-example")

with Client(url, token=token, database_id=database["database_id"]) as client:
    created = client.create_collection("events", schema, collection_id=collection_id)
    if created.outcome != "committed":
        raise RuntimeError(created.raw)

    request = client.prepare_append(
        collection_id,
        [{"key": "a", "value": 1}, {"key": "b", "value": 2}],
        operation_id=operation_id,
        schema=schema,
    )
    outcome = client.append(request)
    if outcome.outcome != "committed":
        raise RuntimeError(outcome.raw)

    result = client.scan_json(collection_id, predicate=col("value", "int64").ge(2))
    print(result.rows)  # [{"key": "b", "value": 2}]

    with client.scan_batches(collection_id) as batches:
        for batch in batches:
            print(batch.to_pylist())
```

These fixed UUIDs make the example repeatable. In an application, generate and
save a collection ID and an operation ID before submitting each new create or
append. Preserve the original IDs and payload if a response is lost; look up
the outcome before deciding to retry. The SDK never retries writes implicitly.
See the [SDK guide](sdk/python/README.md) for historical reads, metadata, streaming,
and recovery handling.

### Embedded C++

The installed `glamdb::core` CMake target provides `CreateStore`, `OpenExisting`,
`Engine::CreateCollection`, `Engine::AppendDurable`, and `Engine::Scan` through
`<storage/engine.h>`. Input and output use Arrow batches. The embedded library
and HTTP server share the same engine; an embedded owner must have exclusive
ownership of its store directory.

## Build from source

Native builds require Linux, CMake 3.28+, Ninja, Python 3.10+, OpenSSL and zlib
development headers, and a C++23 compiler with `std::expected` support: GCC 14+
or Clang 18+ with a matching standard library. Build dependencies and GlamDB
with the same compiler and C++ ABI.

```sh
git submodule update --init --recursive
cmake --preset deps-local
cmake --build --preset deps-local --parallel 2
cmake --preset local
cmake --build --preset local
ctest --preset local
```

To run the resulting server directly, with `GLAMDB_TOKEN` set:

```sh
./build/local/apps/server/glamdb --data-dir "$PWD/build/data" --ui-root "$PWD/ui"
```

The build also produces `build/local/apps/cli/glamdb-cli`. Use the `release`
or `asan-ubsan` preset instead of `local` for release or sanitizer builds.
`cmake --install build/local` installs binaries, UI assets, public headers, and
the CMake package into `build/install/local`.

For the full native test suite in Docker, including on macOS:

```sh
./scripts/build-deps-in-docker.sh
./scripts/test-storage-in-docker.sh asan-ubsan
```

See the [build guide](docs/development/BUILDING.md) for toolchain setup, standalone
client builds, and embedding the installed library.
The [native capacity campaign](docs/development/CAPACITY_CAMPAIGN.md) measures
ingestion, backpressure and fresh-process recovery while preserving history caps.
The [combined recovery harness](docs/development/COORDINATOR.md#combined-storage-faults)
exercises real file/SQLite failures and abrupt process exits; it does not qualify power-loss durability.

For four-state rules and native Telegram setup, see [the alerting guide](ALERTING.md).

For metrics, Grafana dashboards and deployment steps, see [the monitoring guide](docs/development/MONITORING.md).

For a single Linux server, the [complete observability quickstart](deploy/observability/QUICKSTART.ru.md)
provides one command for Prometheus, Grafana, Loki, Alloy, Alertmanager and host/container
collectors. Real application endpoints and a dedicated Loki filesystem are inputs;
repeated deployment preserves owned data and credentials.

### Client and UI tests

The workbench development environment requires Python 3.13, Node.js 22+, and
OpenSSL. It includes the SDK, Python tests, Node tests, and Playwright browser tests:

```sh
bash scripts/bootstrap-workbench.sh
npm --prefix ui exec -- playwright install chromium
bash scripts/check-workbench.sh
```

On Linux, add `--with-deps` to the Playwright install command to install browser
system dependencies. Tests use localhost sockets and disposable stores.

See [Metrics, Grafana dashboards and operator setup](docs/development/MONITORING.md) for monitoring.

## Contributing

Bug reports, documentation improvements, tests, and code contributions are welcome.
Use [issues](https://github.com/glamourdb/glamdb/issues) to report a reproducible
problem or discuss a proposed feature.

1. Fork `glamourdb/glamdb` and create a branch in your fork.
2. Make a focused change, add tests for behavior changes, and update affected docs.
3. Run the relevant tests. For C++ changes, also run the quality checks with
   LLVM 18 `clang-format` and `clang-tidy`:

   ```sh
   cmake --build --preset local --target format-check tidy-check quality-negative-check
   ```

4. Open a pull request against `glamourdb/glamdb:main`, describing the change and
   how you tested it. Storage changes should address recovery and compatibility.

Pull requests require passing checks and approval from another maintainer.
See [CONTRIBUTING.md](CONTRIBUTING.md) for review and merge requirements.
Report security vulnerabilities privately through the
[security policy](SECURITY.md).

## License

GlamDB is licensed under the [Apache License 2.0](LICENSE).
