"""Assess saved CLI/Locust reports without hiding overload or missing work."""

import csv
import json
import math
import re

BACKPRESSURE = {"SERVER_BUSY", "RESOURCE_EXHAUSTED"}


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def assess_phase(report, *, exit_code, allow_backpressure=False, allow_cancelled=False,
                 expected_write_rows=None, expected_rows_per_scan=None):
    summary = json.loads((report / "summary.json").read_bytes())
    with (report / "locust_stats.csv").open(newline="") as source:
        totals = [row for row in csv.DictReader(source) if row["Name"] == "Aggregated"]
    require(len(totals) == 1, "missing or ambiguous aggregate statistics")
    stats = totals[0]
    requests, failures = int(stats["Request Count"]), int(stats["Failure Count"])
    require(requests > 0 and 0 <= failures <= requests, "invalid request counts")
    successes = requests - failures
    elapsed = float(summary["elapsed_seconds"])
    require(math.isfinite(elapsed) and elapsed > 0, "invalid measurement duration")
    require(summary["status"] in {"PASS", "FAIL"}, "unfinished workload report")
    require(exit_code == (0 if summary["status"] == "PASS" else 1),
            "command exit does not match its final report")
    require(summary["expected_workers"] == len(summary["workers"]) == 1,
            "missing worker; this campaign uses one Locust worker")
    require(all(w["finished"] and w["fatal"] is None for w in summary["workers"]),
            "worker did not finish cleanly")
    require(summary["operations"]["pending"] == 0, "unresolved write outcomes")
    with (report / "locust_exceptions.csv").open(newline="") as source:
        exceptions = csv.DictReader(source)
        require(exceptions.fieldnames is not None and "Count" in exceptions.fieldnames,
                "missing exception report header")
        require(not list(exceptions), "Locust user exception; actual concurrency may be below the requested load")

    codes = {}
    with (report / "locust_failures.csv").open(newline="") as source:
        for row in csv.DictReader(source):
            match = re.fullmatch(r"RuntimeError\('([A-Z_]+)'\)", row["Error"])
            code = match[1] if match else row["Error"]
            count = int(row["Occurrences"])
            require(count > 0, "invalid failure count")
            codes[code] = codes.get(code, 0) + count
    require(sum(codes.values()) == failures, "failure CSV disagrees with totals")
    require(successes > 0, "no successful operations")
    reads, operations = summary["reads"], summary["operations"]
    require(allow_cancelled or reads["cancelled_scans"] == 0, "scan drain was interrupted")
    require(reads["partial_rows"] == 0 or
            (allow_cancelled and reads["cancelled_scans"] > 0) or
            (allow_backpressure and failures > 0), "unexplained partial scan rows")

    if summary["mode"] == "write":
        require(summary["status"] == "PASS" and not failures,
                "ordinary append phase failed; preserve journals, do not retry")
        require(summary["dataset_fully_consumed"] is True,
                "write duration expired before consuming all dataset parts")
        require(operations["committed"] == successes, "receipt count disagrees with statistics")
        rows = operations["confirmed_rows"]
        require(expected_write_rows is not None and rows == expected_write_rows,
                "not every input row was confirmed")
    else:
        require(summary["mode"] == "read", "unsupported workload mode")
        require(reads["completed_scans"] == successes, "scan count disagrees with statistics")
        require(reads["failed_scans"] == reads["errors"] == failures,
                "read errors disagree with statistics")
        require((summary["status"] == "PASS") == (failures == 0),
                "read status disagrees with statistics")
        require(not failures or (allow_backpressure and set(codes) <= BACKPRESSURE),
                f"unexpected read failures: {codes}")
        rows = reads["consumed_rows"]
        if expected_rows_per_scan is not None:
            require(rows == successes * expected_rows_per_scan,
                    "completed scans did not consume the selected row count")

    return {
        "name": report.name,
        "assessment": "BACKPRESSURE" if failures else "PASS",
        "raw_status": summary["status"],
        "mode": summary["mode"],
        "measured_seconds": elapsed,
        "requests": requests,
        "successful_requests": successes,
        "failed_requests": failures,
        "successful_requests_per_second": successes / elapsed,
        "rejection_ratio": failures / requests,
        "failure_codes": codes,
        "rows": rows,
        "rows_per_second": rows / elapsed,
        "cancelled_scans": reads["cancelled_scans"],
        "partial_rows": reads["partial_rows"],
        "latency_ms_all_attempts": {
            name: float(stats[column]) for name, column in
            (("p50", "50%"), ("p95", "95%"), ("p99", "99%"), ("max", "Max Response Time"))
        },
        "worker_cpu_percent_one_core": summary["workers"][0]["cpu_percent_one_core"],
        "worker_rss_peak_bytes": summary["workers"][0]["rss_peak_bytes"],
    }
