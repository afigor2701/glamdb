# Manual native stress tests

This suite packages the narrow/wide append and concurrent read campaigns behind
an explicit `unittest` runner. It starts the real native server on an ephemeral
loopback port, creates its own disposable database, and drives the existing
`glamdb-cli` / SDK / Locust workloads. There is no mock fallback or separate load
scheduler. Related performance work is tracked in
[#109](https://github.com/glamourdb/glamdb/issues/109).

These tests are **manual only**: they are not registered with CTest, their files
do not match normal pytest discovery, and `pytest.ini` explicitly excludes this
directory. No CI workflow invokes them. The existing short workload unit and
native integration suites keep their current CI behavior.

## Prerequisites

Use a POSIX host with localhost sockets, Python 3.13, and built native server/CLI
binaries. Install the existing pinned workload environment; no new dependencies
are needed:

```sh
python3.13 -m venv .venv
.venv/bin/python -m pip install --require-hashes --only-binary=:all: --no-deps -r tools/workload/requirements.txt
.venv/bin/python -m pip install --no-deps --no-build-isolation -e sdk/python -e tools/workload

cmake --preset release
cmake --build --preset release --target glamdb_server glamdb_cli --parallel 2
```

The native build needs the pinned dependency prefix described in
[BUILDING.md](../../docs/development/BUILDING.md). Build it first if unavailable.
Capacity runs require `CMAKE_BUILD_TYPE=Release` in the binary's build tree.
`--allow-non-release` permits diagnostic runs, whose recorded build settings
must remain separate from Release results. Smoke runs accept other builds.

## Explicit launch

From the repository root, first check the complete pipeline with small inputs:

```sh
.venv/bin/python tests/stress/run.py --profile smoke --output artifacts/stress-smoke-01
```

Run the full capacity campaign separately:

```sh
.venv/bin/python tests/stress/run.py --profile capacity --output artifacts/stress-capacity-01
```

For another build directory, supply `--server PATH` and `--cli PATH`. Paths may
contain spaces. The output directory must not exist; it holds the generated
datasets, store, journals and evidence. The runner never attaches to an existing
server or reuses an old store. Keep the output until measurements and pending
outcomes have been reviewed, then remove that disposable directory yourself.

For longer read phases or a larger session ramp:

```sh
.venv/bin/python tests/stress/run.py --profile capacity \
  --sessions 1,2,4,8,16,32,64 --read-seconds 300 --full-seconds 300 \
  --write-seconds 600 --output artifacts/stress-long-01
```

`--sessions` controls the projected-read ramp and the highest filtered-read
concurrency. Writes use one/two sessions, wide projection uses eight, and full
scans use two. `--seed` defaults to 42. `--timeout` is the SDK operation deadline,
not the duration of the complete campaign. Every subprocess has a finite outer
timeout; Ctrl-C terminates owned process groups and leaves evidence in place.

List cases or check the report reader and argument guards without starting any
server:

```sh
python3 tests/stress/run.py --list
python3 tests/stress/metrics_test.py
python3 tests/stress/runner_test.py
.venv/bin/python tests/stress/process_test.py
```

## Workloads and assertions

| Workload | Smoke | Capacity |
| --- | --- | --- |
| Two-field input, reused for separate one/two-session writes | 2,000 rows; 100/append | 1,000,000 rows; 10,000/append |
| 16-field and 64-field inputs, each written with two sessions | 200 rows each; 20/append | 100,000 rows each; 1,000/append |
| Projected JSON reads, limit 1,000 | 2 seconds per session count | 25 seconds per session count |
| Full narrow JSON/Arrow and full 16-field JSON/64-field Arrow reads | 3 seconds each | 35 seconds each |
| Wide projection and typed int64-filtered Arrow reads | 2 seconds each | 25 seconds each |
| Maximum write duration, ends early when input is exhausted | 30 seconds | 180 seconds |

The default ramp is 1/2/4/8/16/32 sessions. Four write phases confirm 4,400 rows
in smoke and 2.2 million in capacity. The default campaign assesses 17 load phases
inside seven test cases, then gracefully restarts the server. Independent
`bench verify` checks the full committed row multiset after reopen, including
duplicate multiplicity. Operation lookups must return exactly the receipt results
captured before restart. This is a graceful-reopen check, not a power-loss test.

Schemas have a required duplicate string key and nullable fields cycling through
int64, UTF-8 string, bool, float64 and UTC-microsecond timestamp. Generated dataset
manifests record exact schema/settings/seed, versions, part sizes and hashes.
No automatic append retries, reconciliation, history deletion or operation-ID
recycling are used.

Writes must consume every input part and confirm every row with no pending
outcomes. Single/two-session reads reject any failure. Higher-concurrency reads
may report only `SERVER_BUSY` / `RESOURCE_EXHAUSTED` as expected backpressure,
and must still complete successful scans. Those phases are labelled
`BACKPRESSURE`, preserving their original failing CLI status and rejection counts.
Unknown failures, mismatched counters, missing reports, unfinished workers,
Locust user exceptions, unexpected exits and zero-success runs fail the suite.
Filtered cardinality is computed independently from generated rows with a
NULL-aware Python comparison, outside the measured phase. A successful suite does
not mean that the current server scales to every tested session count.

## Evidence and interpretation

- `result.json`: suite outcome, failures/errors and backpressure phase names.
- `metrics.json`: successful requests/s, committed or fully consumed rows/s,
  refusal ratios/codes, worker CPU/RSS, and **all-attempt** p50/p95/p99/max.
- `resources.jsonl`: sampled child-process CPU, RSS, file descriptors and threads.
- `environment.json`: revision/dirty status, suite and binary hashes, build flags,
  versions and settings.
- `commands.jsonl`, command logs and `server-*`: exact commands and process exits.
- Each workload directory: original SDK journals, Locust CSV/HTML, raw summary,
  pending-operation evidence, and post-reopen verification.

Timed read shutdown uses the workload tool's bounded drain. Completed scans alone
contribute rows; cancelled scans and partial rows remain separate counters. Fast
refusals are included in Locust latency quantiles, so these are not success-only
latency percentiles. Throughput includes input loading, SDK preparation and the
client recovery journal. Arrow workloads currently materialize Python rows.

Run competing jobs separately. This is one same-host Locust worker with a warm
OS cache; its CPU may limit results. Record filesystem/device/cache conditions
alongside comparisons. There is no machine-specific minimum throughput assertion,
and these tests do not establish physical-device capacity or durability
qualification. See [the workload guide](../../tools/workload/README.md) for the
underlying commands and measurement boundaries.
