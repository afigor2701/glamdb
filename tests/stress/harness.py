"""Disposable native process and existing CLI workloads for manual stress tests."""

import hashlib
from importlib.metadata import version
import json
import os
from pathlib import Path
import platform
import select
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from uuid import uuid4

import psutil
from glamdb_sdk import Client, Schema

from metrics import assess_phase, require

REPO = Path(__file__).resolve().parents[2]


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=True, indent=2) + "\n")


def schema_spec(width):
    fields = [{"name": "key", "type": "string", "nullable": False}]
    settings = {}
    kinds = ("int64", "string", "bool", "float64", "timestamp_us_utc")
    for index in range(1, width):
        kind, name = kinds[(index - 1) % len(kinds)], f"c{index}"
        fields.append({"name": name, "type": kind, "nullable": True})
        options = {"null_rate": 0.1}
        if kind in {"int64", "float64"}:
            options.update(min=-1000, max=1000)
        elif kind == "string":
            options.update(length=16, alphabet="\u0430\u0431CD\U0001f3800123")
        settings[name] = options
    return {"schema": {"fields": fields}, "generation": {
        "key": {"profile": "duplicates", "distinct": 100, "prefix": "dup-"},
        "fields": settings,
    }}


def build_settings(binary):
    for parent in binary.parents:
        cache = parent / "CMakeCache.txt"
        if cache.is_file():
            values = {}
            for line in cache.read_text().splitlines():
                if "=" in line and ":" in line.split("=", 1)[0]:
                    key, value = line.split("=", 1)
                    if key.startswith(("CMAKE_BUILD_TYPE:", "CMAKE_CXX_COMPILER:", "GLAMDB_ENABLE_")):
                        values[key.split(":", 1)[0]] = value
            return values
    return {"CMAKE_BUILD_TYPE": "unknown"}


def terminate_group(process, timeout=10):
    """Every child is launched in its own session; never signal unrelated work."""
    try:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            pass
    finally:
        # Also runs if Ctrl-C interrupts graceful termination. A CLI wrapper
        # may have exited while its own workload child is still alive.
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=10)


class Harness:
    def __init__(self, args):
        self.args = args
        self.output = args.output
        self.env = dict(os.environ, GLAMDB_PYTHON=sys.executable,
                        GLAMDB_TOKEN="stress-test-only-" + uuid4().hex)
        sources = [REPO / "sdk/python/src", REPO / "tools/workload/src",
                   REPO / "tools/verification/src"]
        self.env["PYTHONPATH"] = os.pathsep.join(map(str, sources))
        self.server = None
        self.owns_output = False
        self.server_log = None
        self.port = 0
        self.phase = "setup"
        self.results, self.writes, self.tables = [], [], {}
        self.stop_monitor = threading.Event()
        self.monitor_error = None
        self.monitor_thread = threading.Thread(target=self.monitor, daemon=True)

    def setup(self):
        settings = build_settings(self.args.server)
        require(self.args.profile == "smoke" or self.args.allow_non_release or
                settings.get("CMAKE_BUILD_TYPE") == "Release",
                "capacity profile requires a Release build; use --allow-non-release for diagnostics")
        for binary in (self.args.server, self.args.cli):
            require(binary.is_file() and os.access(binary, os.X_OK), f"missing executable: {binary}")
        self.output.mkdir(parents=True, exist_ok=False)
        self.owns_output = True
        revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip()
        binaries = {}
        for binary in (self.args.server, self.args.cli):
            with binary.open("rb") as source:
                binaries[str(binary)] = hashlib.file_digest(source, "sha256").hexdigest()
        save(self.output / "environment.json", {
            "revision": revision, "python": sys.version, "platform": platform.platform(),
            "working_tree_dirty": bool(subprocess.check_output(
                ["git", "status", "--porcelain"], cwd=REPO, text=True).strip()),
            "suite_sha256": {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                             for path in sorted(Path(__file__).parent.glob("*.py"))},
            "cpu_count": os.cpu_count(), "memory_bytes": psutil.virtual_memory().total,
            "versions": {name: version(name) for name in ("httpx", "pyarrow", "locust", "mimesis", "psutil")},
            "build_settings": settings, "binary_sha256": binaries,
            "profile": self.args.profile, "seed": self.args.seed,
            "sessions": self.args.sessions, "read_seconds": self.args.read_seconds,
            "full_seconds": self.args.full_seconds, "write_seconds": self.args.write_seconds,
            "timeout_seconds": self.args.timeout,
            "measurement": "same-host SDK/Locust, one worker, warm OS cache; not isolated device or engine capacity",
        })
        self.monitor_thread.start()
        for width in (2, 16, 64):
            path = self.output / f"schema-{width}.json"
            save(path, schema_spec(width))
            self.command(f"generate-{width}", ["generate", "--schema", path,
                         "--rows", self.rows(width), "--seed", self.args.seed,
                         "--part-rows", self.batch_rows(width), "--output", self.dataset(width)],
                         timeout=600, expected=0)
        self.start()
        with Client(self.url, self.env["GLAMDB_TOKEN"]) as client:
            self.database = client.create_database("manual-stress", deadline=self.args.timeout)["database_id"]

    def rows(self, width):
        if self.args.profile == "smoke":
            return 2000 if width == 2 else 200
        return 1000000 if width == 2 else 100000

    def batch_rows(self, width):
        if self.args.profile == "smoke":
            return 100 if width == 2 else 20
        return 10000 if width == 2 else 1000

    def dataset(self, width):
        return self.output / f"dataset-{width}"

    def filtered_row_count(self):
        from glamdb_workload.dataset import Dataset

        self.phase = "filter-cardinality"
        matches = 0
        for rows in Dataset(self.dataset(2)).iter_parts():
            matches += sum(row["c1"] is not None and row["c1"] >= 500 for row in rows)
            if matches >= 1000:
                return 1000
        return matches

    @property
    def url(self):
        return f"http://127.0.0.1:{self.port}"

    def client(self):
        return Client(self.url, self.env["GLAMDB_TOKEN"], database_id=self.database)

    @staticmethod
    def operations(report):
        for journal in sorted(report.glob("worker-*.sqlite")):
            with sqlite3.connect(journal.as_uri() + "?mode=ro", uri=True) as database:
                for operation, state in database.execute("SELECT operation_id,state FROM operations"):
                    require(state == "committed", "reopen verification requires terminal committed writes")
                    yield operation

    def start(self):
        self.server_log = (self.output / f"server-{time.time_ns()}.stderr.log").open("w")
        self.server = subprocess.Popen(
            [str(self.args.server), "--data-dir", str(self.output / "native-data"),
             "--port", str(self.port), "--ui-root", str(REPO / "ui")],
            env=self.env, stdout=subprocess.PIPE, stderr=self.server_log,
            text=True, start_new_session=True,
        )
        require(select.select([self.server.stdout], [], [], 30)[0], "native startup timed out")
        startup = json.loads(self.server.stdout.readline())
        require(self.port == 0 or startup["port"] == self.port, "restart changed the server port")
        self.port = startup["port"]
        save(self.output / f"startup-{time.time_ns()}.json", startup)

    def stop(self):
        if self.server is None:
            return
        process = self.server
        try:
            terminate_group(process, timeout=60)
        finally:
            if process.poll() is not None:
                self.server = None
                if process.stdout:
                    process.stdout.close()
                self.server_log.close()
                save(self.output / f"server-stop-{time.time_ns()}.json", {"exit_code": process.returncode})
        require(process.returncode == 0, f"native server exited with {process.returncode}")

    def cleanup(self):
        try:
            self.stop()
        finally:
            self.stop_monitor.set()
            if self.monitor_thread.ident is not None:
                self.monitor_thread.join(timeout=5)
            require(self.monitor_error is None, f"resource monitor failed: {self.monitor_error}")

    def monitor(self):
        try:
            with (self.output / "resources.jsonl").open("w") as output:
                while not self.stop_monitor.wait(1):
                    server = self.server
                    sample = {"time": time.time(), "phase": self.phase,
                              "server_pid": server.pid if server else None, "processes": []}
                    for process in psutil.Process().children(recursive=True):
                        try:
                            cpu = process.cpu_times()
                            sample["processes"].append({
                                "pid": process.pid, "name": process.name(),
                                "rss_bytes": process.memory_info().rss,
                                "cpu_seconds": cpu.user + cpu.system,
                                "fds": process.num_fds(), "threads": process.num_threads(),
                            })
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            pass
                    output.write(json.dumps(sample) + "\n")
                    output.flush()
        except Exception as error:
            self.monitor_error = repr(error)

    def command(self, name, args, *, timeout, expected=None):
        self.phase = name
        argv = [str(self.args.cli), *map(str, args)]
        print(json.dumps({"event": "start", "phase": name}), flush=True)
        began = time.monotonic()
        with (self.output / f"{name}.log").open("w") as log:
            process = subprocess.Popen(argv, env=self.env, stdout=log,
                                       stderr=subprocess.STDOUT, start_new_session=True)
            try:
                code = process.wait(timeout=timeout)
            finally:
                terminate_group(process)
                record = {"name": name, "argv": argv, "exit_code": process.returncode,
                          "wall_seconds": time.monotonic() - began}
                with (self.output / "commands.jsonl").open("a") as output:
                    output.write(json.dumps(record) + "\n")
        print(json.dumps({"event": "finished", **record}), flush=True)
        if expected is not None:
            require(code == expected, f"{name} exited with {code}; see {name}.log")
        return code

    def write(self, width, users):
        name = f"write-width{width}-batch{self.batch_rows(width)}-u{users}"
        cid = str(uuid4())
        schema = Schema.from_dict(schema_spec(width)["schema"])
        with Client(self.url, self.env["GLAMDB_TOKEN"], database_id=self.database) as client:
            outcome = client.create_collection(name, schema, collection_id=cid, deadline=self.args.timeout)
            require(outcome.outcome == "committed", "collection was not committed")
        report = self.output / name
        code = self.command(name, ["bench", "write", "--url", self.url,
                            "--database", self.database, "--collection", cid,
                            "--dataset", self.dataset(width), "--users", users,
                            "--spawn-rate", users, "--duration", f"{self.args.write_seconds}s",
                            "--timeout", self.args.timeout, "--stop-timeout", 3,
                            "--report", report], timeout=self.args.write_seconds + self.args.timeout + 60)
        result = assess_phase(report, exit_code=code, expected_write_rows=self.rows(width))
        self.results.append(result)
        self.writes.append({"report": name, "collection": cid, "width": width})
        self.tables[width] = cid
        save(self.output / "write-targets.json", self.writes)
        save(self.output / "metrics.json", self.results)

    def read(self, name, width, users, format, query, seconds, expected_rows=None):
        require(width in self.tables, f"width-{width} write fixture did not finish")
        path, report = self.output / f"{name}.query.json", self.output / name
        save(path, query)
        code = self.command(name, ["bench", "read", "--url", self.url,
                            "--database", self.database, "--collection", self.tables[width],
                            "--query", path, "--format", format, "--users", users,
                            "--spawn-rate", users, "--duration", f"{seconds}s",
                            "--timeout", self.args.timeout, "--stop-timeout", 3,
                            "--report", report], timeout=seconds + self.args.timeout + 60)
        result = assess_phase(report, exit_code=code, allow_backpressure=users > 2, allow_cancelled=True,
                              expected_rows_per_scan=expected_rows)
        require(result["cancelled_scans"] <= users, "cancellations exceeded the final in-flight users")
        self.results.append(result)
        save(self.output / "metrics.json", self.results)
