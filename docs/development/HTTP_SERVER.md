# Native HTTP service

`apps/server` builds `glamdb`; `apps/cli` builds `glamdb-cli`. The server manages named databases through the public C++ facade. Start with an
empty data directory; create and administer databases through HTTP or the UI:

```sh
cmake --preset local
cmake --build --preset local
export GLAMDB_TOKEN='replace-with-a-random-secret'
./build/local/apps/server/glamdb --data-dir /tmp/my-glamdb --ui-root "$PWD/ui"
```

`scripts/glamdb-local.sh up` performs these steps (natively on Linux, in Docker
elsewhere) and runs the server in the background; see the README.
Open [the workbench](http://127.0.0.1:8080), enter the token, create a database,
then create collections, append rows and query them. The database screen also
opens, closes and reconciles databases. Restart with the same data directory. No mock
fallback exists. `--port 0` selects a port; the first stdout line reports it as
JSON. Installation places the binaries in `bin` and the UI in `share/glamdb/ui`;
the installed server discovers that UI relative to its executable on Linux.

Existing standalone stores can still be served with `--root ABSOLUTE_PATH
--store-id UUID`, mutually exclusive with `--data-dir`. This compatibility mode
uses unscoped collection routes and does not offer database management.

`glamdb-cli` is a standalone network client with DNS and verified HTTPS; it does
not link the engine or storage libraries. `create-store --name analytics` calls
the managed database API. The optional `glamdb-admin` contains offline
`create-store`/`inspect-store`, which acquire core ownership and fail with
`STATE_IN_USE` while the server owns the store. See the [client guide](../../apps/cli/README.md)
for its independent build and TLS configuration. Online:

```sh
glamdb-cli request --path /databases
# database.json: {"name":"analytics"}; response includes database_id
# Retrying the same name returns the same database.
glamdb-cli request --path /databases --body database.json
glamdb-cli request --path /databases/DATABASE_UUID/collections --body collection.json
glamdb-cli request --path /databases/DATABASE_UUID/collections/COLLECTION_UUID/append --body append.json
glamdb-cli request --path /databases/DATABASE_UUID/collections/COLLECTION_UUID/scan --body query.json
```

`request` defaults to GET, or POST with `--body FILE` (`-` reads stdin).
`--method`, `--url`, `--host` and `--port` are explicit. It writes the exact response
bytes and never retries. Exit 1 denotes HTTP or pre-send transport failure, 2 CLI
usage error, 3 an uncertain POST delivery, and 4 output failure after a complete
HTTP response. Preserve the original operation ID when delivery is uncertain.
`--accept application/vnd.apache.arrow.stream --output result.arrow --headers headers.json`
retains binary Arrow and cursor/checksum metadata. HTTP is loopback-only; remote
CLI and SDK access use verified HTTPS through a TLS reverse proxy. The server uses
system OpenSSL Crypto for page SHA-256 checksums.

## Database lifecycle

| HTTP route | Behavior |
| --- | --- |
| `GET /databases` | List registered identities, names, states and errors |
| `POST /databases` | Create/open `{name,database_id?:UUID,limits?:object}`; returns database info |
| `GET /databases/{id}` | Database info and store profile while open |
| `POST /databases/{id}/open` | Open existing data and run core recovery |
| `POST /databases/{id}/close` | Stop new calls, drain admitted calls and shut down |
| `POST /databases/{id}/reconcile` | Run core reconciliation for an open engine |
| `POST /databases/{id}/discard` | Remove an uninitialized reservation, preserving any incomplete files |

Close/reconcile/discard take `{}`; open accepts `{limits?:object}`. All routes require the bearer token. Collection
routes in the next table are relative to `/databases/{id}` in managed mode;
`GET /capabilities` at the server root advertises `features.database_management`.
A database response contains `{database_id,name,state,limits,store?,error?}`; states are
`open`, `closed`, `opening`, `closing`, `reconciling`, `error`. Lists contain `{items,next_cursor:null}`.
Names are unique, case-sensitive UTF-8 (1–128 bytes, no NUL or path separators).
At most 64 databases are registered. Paths are generated from UUIDs under
`--data-dir/databases`; clients cannot provide filesystem paths. Database and
collection identities are separate, and collection/operation IDs are database scoped.

Creation reserves the name and UUID durably before calling `CreateStore`.
Repeated creation with the same name and optional matching UUID returns or
reopens that database; a conflicting name/UUID returns 409. A lost response can
be resolved by listing databases or repeating the same request. There are no
automatic client retries. An unfinished reservation without a store can be
completed only by explicit creation. `open` never creates anything. An incomplete
store, or a missing initialized store, remains an error and is never replaced.
Discard returns `{database_id,discarded:true}` and releases the name, UUID and registry slot.
It requires an uninitialized reservation with no owned engine and no `store.ready`;
otherwise it returns 412. Any incomplete directory is preserved as an `incomplete-...`
sibling. A registry write with an uncertain outcome requires restart before reuse;
after restart, inspect the original ID (404 means the reservation was removed).
Discard never removes an initialized or ready store. There is no drop/import/rename
endpoint for initialized databases in this release.

Optional `limits` configure the public engine limits. Creation merges omitted
fields with defaults; an omitted limits object on retry preserves the existing
configuration. An explicit conflicting creation configuration returns 409.
Open merges partial settings with persisted values; changing values requires a
closed database. Configuration and open intent are durable before opening the
engine. If a lower limit prevents reopening existing data, close the errored
entry and open again with sufficient limits; data is never discarded.

| Limit | Range | Default |
| --- | --- | --- |
| `max_operations` | 1–4096 | 4096 |
| `max_collections` | 1–8 | 8 |
| `max_catalog_bytes` | 65536–268435456 | 268435456 |
| `max_pending_calls` | 1–16 | 16 |
| `max_owned_input_bytes` | 1–134217728 | 134217728 |
| `writer_wait_ms` | 0–5000 | 5000 |
| `append_deadline_ms` | 1–60000 | 60000 |

Values are JSON integers; booleans, fractions, unknown fields and values outside
these bounds are rejected before reserving a database. Every database response
includes normalized limits, including closed/error states. The service registry is created directly in version 2; core store formats are unchanged.

A separate `registry.sqlite` records identities and intended open state, using
verified SQLite DELETE/EXTRA settings. The registry does not contain collection
catalogs or data. A definitely rolled-back registry write can be retried. An uncertain registry write
blocks further lifecycle mutations until restart; outcome lookup and core reconciliation
remain available for owned engines. Service initialization builds and syncs the complete
registry before publishing it. Explicit create can retry an incomplete, uninitialized
store reservation by preserving its files under an `incomplete-...` sibling. It never
replaces an initialized store.
A missing registry in a nonempty directory fails startup. Each service directory
has one process owner; each core store retains its own identity and owner lock.
Use an absolute canonical data path with an existing parent on trusted local disk.

Open databases reopen on restart; explicitly closed databases stay closed. A
failed store open is listed as an error while other databases remain usable.
Close persists the intent to remain closed before shutdown. A shutdown deadline retains ownership in `closing`; retry close after calls drain.
A terminal readiness error releases the drained engine so close/reopen can recover. Reconciliation does not undo shutdown. A client
timeout does not roll back lifecycle changes: inspect state or retry the same
action. Per-database leases drain accepted calls and reject new calls during
close/reconcile; unrelated databases remain usable within the fixed worker pool.
Database info/list requests read immutable descriptors without waiting for lifecycle changes.
Repeated open/create of an already healthy database does not drain active calls. Failed core
reconciliation preserves outcome lookup while core readiness rejects unsafe work.
The workbench clears query results and schema caches on database switch and ignores
stale replies. It retains unresolved operation IDs and payloads per database until
resolved or disconnected, and offers reconciliation in the active workspace.

## Contract and ownership

Versioned [JSON Schema wire types](../../fixtures/http/v0.schema.json) define
requests, responses, errors and recovery envelopes. Fixed-schema row types,
UUID semantics and int64 range are additionally checked by the native core.
The executable [v0 route/shape fixtures](../../fixtures/http/v0.json) inventory
every SDK/workbench call, its core operation, request example, status and required
response-field types. `glamdb.native_http` executes them against real storage.
The fixtures are a bounded response-shape vocabulary, not a full JSON Schema
validator. Native `outcome_json_test` separately pins complete recovery/receipt
serialization. [Shared literal outcome/scalar fixtures](../../fixtures/http/README.md)
also exercise native serialization, SDK/browser parsing, incomplete recovery
rejection, and A/B/retry-A through native storage and fresh-process reopen.
Nullable recovery fields must be present; SDK/browser responses retain extension
fields. Fixture, mock and native integration evidence remain distinct.
The existing [mock contract](HTTP_MOCK_CONTRACT.md) remains a
separate, broader development profile.

| HTTP route | Public core operation | Availability |
| --- | --- | --- |
| `GET /capabilities` | Adapter inventory / `Engine::GetHealth` when scoped | Native features, predicates, transport limits; store health when selected |
| `GET /databases/{id}/readyz` (legacy `/readyz`) | `Engine::GetHealth` plus lifecycle admission | Authenticated readiness snapshot; 200 when write-ready, otherwise 503 |
| `GET /collections` | `ListCollections` | Complete bounded list; no pagination |
| `POST /collections` | `CreateCollection` | Fixed-schema append collection |
| `GET /collections:resolve?name=...` | `ResolveCollection` | Exact name |
| `GET /collections/{id}` | `GetCollection` | Identity and current schema |
| `GET /collections/{id}/schema` | `GetCollection` / `Schema` | Stable field IDs |
| `POST /collections/{id}/append` | `AppendDurable` | Synchronous native receipt |
| `GET /collections/{id}/operations/{op}` | `FindAppendOutcome` | Original result or authoritative absence |
| `POST /collections/{id}/scan/open` | `Scan` | Resolve a fixed view and return a cursor without consuming rows |
| `POST /collections/{id}/scan` | `Scan` / `ScanCursor::Next` | First page or cursor continuation, JSON or Arrow IPC |
| `POST /collections/{id}/scan/cancel` | `ScanCursor::Cancel` | Release a cursor and its retained resources |
| `GET .../stats`, `.../snapshots`, `.../files`, `.../files/{file}` | Proposed public metadata methods | `501 UNSUPPORTED_OPERATION` |
| `POST .../explain` | Proposed public explain method | `501 UNSUPPORTED_OPERATION` |

CLI mapping: `create-store` → `POST /databases`; `request` exposes every route
in the tables above. Offline `glamdb-admin` uses `CreateStore` / `OpenExisting`.

The transport owns HTTP parsing, codecs, authentication, connection admission
and static UI assets. Core owns store identity/locking, schema validation,
canonical hashes, serialized writes, durable publication, receipts, recovery and
snapshot selection. The adapter never reads core catalog tables or enumerates the
warehouse; its separate registry stores only database lifecycle metadata. Metadata/explain must be added through public core methods, not
synthetic values. The UI respects `features` and reports unavailable sections.

`GET /healthz` is public process liveness, **not write readiness**. It continues
to return 200 while a store is recovering, closed or failed. Readiness is
store-specific: authenticated `GET /databases/{id}/readyz`, or `/readyz` in
legacy single-store mode. A managed root `/readyz` requires database selection;
there is no aggregate assertion that every registered store is writable.
Unknown database IDs retain the normal 404 response.

The readiness body contains `state`, `read_available`, `write_ready`,
`storage_failure_latched`, `durability_qualified`, and the existing `backend`,
`mock`, `durability_validation` identity fields. The same object appears as
`health` in scoped `/databases/{id}/capabilities` and legacy `/capabilities`.
Managed root `/capabilities` has `health: null`; it advertises installed features,
not current store admission. Scoped capabilities return 200 even when the store
is unavailable, so clients can inspect health without attempting a write.

| State | New reads | New writes | Readiness HTTP status |
| --- | --- | --- | --- |
| `recovering` | No | No | 503 |
| `ready` | Yes with a storage reader | Yes | 200 |
| `recovery_required` | No | No | 503 |
| `storage_failed` | Yes with a storage reader | No | 503 |
| `unavailable` | Only if a reader is installed | No | 503 |
| `stopping` | No | No | 503 |
| `closed` | No | No | 503 |

Opening and reconciliation report `recovering`. An unresolved commit,
corruption, failed recovery, or registry admission fence reports
`recovery_required`. A durable file-I/O write latch reports `storage_failed`
when recovery has otherwise succeeded: confirmed reads remain available, but
new writes need shutdown and reopen. Reconciliation does **not** clear the
latch. The latch flag remains independently visible on an engine during
recovery and shutdown. Closed managed databases no longer retain an engine,
so their snapshot has no in-memory latch. Lifecycle draining reports `stopping`
before waiting for active calls. Health observation never performs catalog/file
I/O or waits for a writer or lifecycle operation.

Availability is a point-in-time observation, not a reservation. A later call
still checks admission and can fail due to resource limits, contention, or a
newly detected fault. `read_available` describes new scans, not all metadata
lookups or previously retained cursors; recovery outcome lookup keeps its
existing contract. Synchronous C++ factories return an engine only after
startup recovery succeeds; the managed HTTP lifecycle exposes runtime database
opening progress. At initial process startup, registered databases are opened
before the HTTP listener starts, so probes cannot connect during that phase.

`backend=local` and `mock=false` do not imply power-loss qualification.
`durability_validation=designed` and `durability_qualified=false` remain explicit
for both local and mock backends. Neither a successful build, backend selection,
recovery, nor a passing test changes qualification. No evidence-backed qualified
environment is currently advertised. The disposable mock exposes `/readyz` for
its in-memory operations with `backend=mock`, `mock=true`; its readiness does
not promise persistent storage. Executable examples for every state are in
`fixtures/http/health-states.json`, with the `storeHealth` schema in
`fixtures/http/v0.schema.json`.

## Requests and results

Requests use UTF-8 JSON objects with unknown/duplicate fields rejected. Create:
`{collection_id,name,schema:{fields:[{name,type,nullable}]},mode?:"append"}`.
Types: `string`, `bool`, `int64`, `float64`, `timestamp_us_utc`. Core assigns IDs
1..N; supplied field IDs must match. A field named `key`, if supplied, must be a
non-nullable string; its values are not unique.
Schema evolution, SQL, upsert and delete are unsupported.

Append: `{operation_id,rows:[{field:value,...}],expected_snapshot?}`. Omitted
precondition means any snapshot; null means no snapshot; a decimal string means
an exact snapshot. Missing nullable row values become null. Int64 and snapshot
IDs **must be decimal strings**, never JSON numbers; floats must be finite and
booleans remain booleans. Timestamps accept RFC3339 offsets and at most six
fractional digits, normalized to UTC with six digits, years 0001–9999.

Scan: `{projection?,predicate?,snapshot_id?,limit?,batch_rows?}`. Omission selects
latest once; null or `"initial"` selects the initial empty view. Historical IDs
are decimal strings. Omitted/null projection defaults to all columns; zero-column projection
is unavailable. Predicates are `null`/`{op:"true"}`, comparisons
`{op:"eq|ne|lt|le|gt|ge",field,value}` (where `value` is a correctly typed
literal or `null`), `{op:"in",field,values:[...]}` (typed literals and/or
`null`), `{op:"is_null|is_not_null",field}`, or recursive boolean trees
`{op:"and|or",args:[...]}` (non-empty `args`) and `{op:"not",arg:{...}}` within
`predicate_depth <= 64`, `predicate_nodes <= 1024`, and `predicate_literals <=
4096` (exceeding limits or malformed shapes/types returns 400; unknown operators
within the depth limit return 501). Limit is a nonnegative int64 integer;
batch_rows is 1..65536. SQL three-valued NULL logic applies across the tree and
only TRUE at the root survives. No SQL parser is implied.

JSON scan returns `{schema,snapshot_id,rows,identity,profile,next_cursor}`. The
snapshot is the resolved selected view, including null for the initial empty
view; `identity` is the core catalog identity captured when planning the scan.
Profile contains only the actual page `rows_returned` decimal string, without
pruning or physical IO claims. `Accept: application/vnd.apache.arrow.stream`
returns a schema-bearing Arrow IPC page, including empty results, with
`Content-Length`, `X-Content-SHA256`, `X-GlamDB-Snapshot-Id`, and an optional
`X-GlamDB-Next-Cursor`. Each page is fully prepared before success headers, so a
read/encoding failure cannot become a successful partial page.

For complete reads, continue with `POST .../scan {"cursor":"TOKEN"}` and the same
Accept header until JSON `next_cursor` is null or the Arrow cursor header is
absent. Each page has at most 10000 rows and targets 4 MiB; a single larger row is
allowed if its complete response fits 16 MiB. Total rows are not capped by the
page size. Writes after scan creation never enter that scan's fixed view.

For retry-safe reading from the first page, first call `POST .../scan/open` with
the ordinary scan body and chosen Accept header. Its JSON response contains
`{cursor,schema,snapshot_id,identity}` and consumes no rows. Use that cursor for
the first page. The last consumed cursor replays exactly the same response;
advancing invalidates older cursors. A lost response to ordinary `/scan` without
a cursor cannot be replayed: opening again creates a new scan. The SDK's scan entry points and the workbench use the explicit open protocol.

`POST .../scan/cancel {"cursor":"TOKEN"}` accepts either the next or most recently
consumed cursor and returns `{"cancelled":true}`. Tokens are scoped to database,
collection and representation; stale or missing tokens return 404, never restart
queries. Each database has eight sessions within a server limit of 64. Idle sessions expire
after 60 seconds (checked every second); replay does not refresh that deadline. All
sessions expire after five minutes. Retained response bodies have a shared 128 MiB
budget; completed replay caches may be evicted to admit new scans. Before advancing
a page, the server reserves a full 16 MiB response in addition to its previous
replay body. Replay-capacity refusal returns 429 before consuming rows and preserves
the cursor and its previous replay; cancel unused scans and retry the same token.
Successful encoding replaces that reservation with the actual retained body size.
Other page failures invalidate the session. These are replay reservations, not a
bound on decode buffers or outgoing copies. Close, reconciliation
and shutdown release sessions after admitted calls drain. Cancellation releases
between page calls; it does not interrupt an active filesystem read.

Create/append results use the existing native serializer. Append preserves
`committed`, `definitely_not_committed` and `commit_unknown`, phase, original ID,
full receipt/error and recovery context. No 202 or asynchronous admission exists.
A timeout/disconnect/HTTP error alone never proves rollback. Retain the original
ID and payload, query its outcome, and reconcile/reopen when core requires it.
HTTP parsing/codec rejection precedes the core call and returns an error object;
it does not register an operation. No implicit retry occurs in server or CLI.

Errors are `{error:{code,message,retry_hint}}`, with the core retry hint unchanged.
400 invalid request, 401 authentication, 404 authoritative absence, 409 conflict,
412 precondition, 413 oversized body or decoded input, 415 media type, 429 bounded resource admission,
501 unsupported, 503 recovery/dependency/unresolved outcome, 504 deadline,
507 exhausted capacity, 500 corruption/unexpected error. Core `RESOURCE_EXHAUSTED`
uses 429 for bounded admission (busy writer, pending budget) and
507 when the core marks it `OperatorAction`: free-space reserves, store limits,
`max_collections`/`max_operations`/catalog bytes, or failed allocation. `CAPACITY_EXCEEDED`
is always 507. Writer preflight also currently emits `RESOURCE_EXHAUSTED` with
`FixInput` (429) when its separate row/input estimate exceeds a bound; reduce the
input instead of retrying it unchanged. The free-space reserve is the larger of 1 GiB and 5% of the data
filesystem plus 512 MiB for an attempt and journal. Database, collection and
operation absence have distinct codes; only `OPERATION_NOT_FOUND` proves an
operation lookup absent. Transport errors carry the shared retry policy; unexpected
write failures preserve the original ID for outcome resolution.
`INPUT_TOO_LARGE` with `FixInput` denotes the deterministic 2 MiB decoded row or
16 MiB decoded batch limit; reducing the input is required. Retrying an unresolved
append still validates its canonical payload: changed content with the same ID
returns `OPERATION_CONFLICT` before reconciliation. The immutable schema used to
decode that retry does not open core readiness or admit another write.
Collections accept `page_size` 1..1000; insufficient size or any cursor returns
501 rather than silently dropping entries or inventing an unstable cursor.

## HTTP service profile

Standalone Asio provides asynchronous sockets, timers and signals. Four data workers
admit at most two concurrent calls per database. Two lifecycle workers and two
control/static workers keep database descriptors and outcome lookup independent of
long data calls. Outcome lookup uses nonblocking catalog admission: publication
contention returns 503 `OPERATION_IN_PROGRESS`, never authoritative absence or an
append disposition. Retry the original lookup after publication finishes. Health
and global capabilities run on the I/O thread. Core retains
its single serialized writer. Overload returns 503 with `Retry-After: 1` within the
bounded response capacity. There are at most 64 header readers (32 per peer), 16
admitted request bodies and 96 total connections, including overload responses;
at the final ceiling, accept pauses and checks for capacity every 100 ms. Excess
connections wait in the kernel backlog instead of being accepted and reset. The
OS backlog remains finite; connect/read timeouts still require clients to preserve
the original write identity. Header reads have at most one
second; body reads use `--request-timeout-ms` (default 10 seconds), and response
writes have five seconds. Read deadlines never cancel admitted core work.
Transient accept failures retry with backoff. Header limit: 16 KiB; request/response:
16 MiB; append: 10000 rows. JSON depth and event limits bound a single-pass parser
and accommodate the full 256-field append schema. These transport bounds are not
a qualified process-memory or storage budget.

The [shared resource budget matrix](RESOURCE_BUDGETS.md) specifies which limits
are per request, store or service, who reserves/releases them and which aggregate
budgets remain required. HTTP body bytes, parsed JSON/Arrow allocations and core
canonical bytes are different quantities; the per-store owned-input limit does
not cover adapter allocations or all databases together.

HTTP/1.1 uses Content-Length and closes each response connection. Duplicate
headers, folded headers, Transfer-Encoding, compressed requests and pipelined bytes
are rejected; keep-alive and chunked requests are not supported. `Expect: 100-continue`
is acknowledged after authentication and body admission. Oversized headers return
431, malformed line endings 400, and read timeouts 408. JSON Content-Type accepts
case-insensitive media names and UTF-8 charset parameters. Scan Accept supports
media lists, wildcards and quality weights.
Use ordinary buffered JSON requests. The server defaults to 127.0.0.1:8080.
`--listen` is explicit; terminate TLS and apply deployment limits at a trusted
reverse proxy for remote access. There is one bearer identity, no user/RBAC layer.
The token comes only from `GLAMDB_TOKEN`, never a URL, log, config response or UI
storage. API responses are uncached. CORS is disabled; the UI uses the same origin.

SIGINT/SIGTERM stop accepting, close incomplete requests, cancel scan sessions and
reject new scans before draining workers. Cancellation reaches core cursors without
waiting on their session locks; read leases remain owned until those calls finish.
Admitted writes and responses drain before Engine::Shutdown releases store
ownership. OS IO may still block; there is no forced rollback or detached writer.
Client disconnects do not cancel publication. Scan pages are synchronous bounded
responses; cancelled cursors cannot publish another page.

Static serving allows only index, theme and supported assets beneath `--ui-root`,
rejects traversal and symlinks, and never exposes the store. Administrators must
keep the UI tree trusted and separate from the store.

## Protocol choices

[ClickHouse HTTP](https://clickhouse.com/docs/interfaces/http) provides a useful
cross-language HTTP/format model and documents late streaming error caveats.
GlamDB buffers bounded pages before success headers to avoid that ambiguity.
[Trino's REST protocol](https://trino.io/docs/current/develop/client-protocol.html)
separates query submission, result pages and cancellation; GlamDB exposes these
for its fixed-view scan cursors. [PostgreSQL message flow](https://www.postgresql.org/docs/current/protocol-flow.html)
illustrates why requesting cancellation does not prove a write failed. GlamDB
keeps operation identity and durable outcome lookup separate from the connection.

## Validation

```sh
GLAMDB_REQUIRE_NATIVE_CLIENTS=1 GLAMDB_VALIDATE_SCHEMAS=1 \
ctest --preset local -R 'glamdb\.(native_http|scan_http|database_limits|transport\.|routes\.|cli)'
# With workbench Python dependencies, also exercise SDK JSON/Arrow decoding:
.venv/bin/python tests/server/native_http_test.py build/local/apps/server/glamdb build/local/apps/cli/glamdb-cli ui
GLAMDB_NATIVE_SERVER="$PWD/build/local/apps/server/glamdb" \
npm --prefix ui run test:e2e
```

The native suite starts with an empty service directory and creates databases
through HTTP without an offline CLI. It covers isolation, close/open/reconcile,
restart and process crashes, missing-store preservation, real HTTP, exact scalar checks,
repeat-ID conflicts, fixed snapshots, lost-response lookup, reopen, owner locking,
request framing/auth/limits and UI assets. Mock/client suites remain separate.
Passing these tests does not qualify physical power-loss durability.

The deterministic `glamdb.http_lifecycle` test uses a test-only backend barrier
to disconnect and deliver SIGTERM during publication, verifies that the server
waits, then checks the committed receipt after reopen. It validates lifecycle
coordination; the separate native suite supplies real storage evidence.

Set `GLAMDB_VALIDATE_SCHEMAS=1` when running the native Python suite in an
environment with `jsonschema` to validate each fixture request and actual response
against Draft 2020-12. The standard-library CTest runner always checks route/status
and required response-field types.

`glamdb.routes.Databases.*` additionally checks concurrent name reservations,
request draining across databases, failed-close ownership and retry, incomplete
creation, and registry COMMIT errors before/after rollback-journal unlink. These
are deterministic failure-model tests, not physical power-loss qualification.

Native CI installs hash-pinned SDK, Arrow and JSON Schema dependencies and requires
these checks without silent import skips. Its normal profile also builds the CLI
without engine dependencies and runs the native browser profile. Run the same gate
with `scripts/check-native-http.sh build/local build/client/glamdb-cli`, selecting
its Python environment through `GLAMDB_NATIVE_PYTHON` if needed.

`glamdb.native_http_fault` uses the production managed HTTP service and real
storage with a test-only SQLite VFS control channel. It covers faults before/after
create, append and registry COMMIT, failed recovery fencing, failure recording,
producer path corruption, descriptor exhaustion, close waiting for an admitted
native append, and SIGTERM draining publication after a shutdown deadline while
an independent embedded open still fails. Fresh-process restart resolves uncertain
COMMIT results and retains original receipts and duplicate rows.
`glamdb.http_review` checks HTTP error responses, Expect handshakes,
idle-header admission, media negotiation and canonical scoped dispatch.
`glamdb.scan_http` covers fixed-view pages beyond 100000 rows and 16 MiB, response
replay, scope, cancellation, expiry, resource capacity, small-batch IPC, abandoned
SDK iterators and real read corruption. Scan session tests also pause a real Parquet
read while another database progresses, check nonblocking shutdown cancellation
and closed scan admission, and enforce the shared replay byte budget.
These deterministic checks are separate from physical power-loss qualification.
