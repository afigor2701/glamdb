#!/usr/bin/env python3
"""Run the native stress campaign explicitly, outside default test discovery."""

import argparse
import json
import math
import os
from pathlib import Path
import sys
import traceback
import unittest

REPO = Path(__file__).resolve().parents[2]


def positive(value):
    result = int(value)
    if result < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return result


def timeout(value):
    result = float(value)
    if not math.isfinite(result) or result <= 0:
        raise argparse.ArgumentTypeError("must be positive and finite")
    return result


def sessions(value):
    try:
        result = [positive(part) for part in value.split(",")]
    except ValueError as error:
        raise argparse.ArgumentTypeError("use comma-separated positive session counts") from error
    if len(set(result)) != len(result):
        raise argparse.ArgumentTypeError("session counts must be distinct")
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", type=Path, default=REPO / "build/release/apps/server/glamdb")
    parser.add_argument("--cli", type=Path, default=REPO / "build/release/apps/cli/glamdb-cli")
    parser.add_argument("--output", type=Path, help="new directory for disposable data and all evidence")
    parser.add_argument("--profile", choices=("smoke", "capacity"), default="capacity")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--sessions", type=sessions, default=[1, 2, 4, 8, 16, 32])
    parser.add_argument("--read-seconds", type=positive)
    parser.add_argument("--full-seconds", type=positive)
    parser.add_argument("--write-seconds", type=positive)
    parser.add_argument("--timeout", type=timeout, default=120)
    parser.add_argument("--allow-non-release", action="store_true", help="allow diagnostic capacity runs on other builds")
    parser.add_argument("--list", action="store_true", help="list cases without loading dependencies or starting processes")
    args = parser.parse_args(argv)
    from capacity import CapacityStress

    if args.list:
        print("\n".join(unittest.defaultTestLoader.getTestCaseNames(CapacityStress)))
        return 0
    if args.output is None:
        parser.error("--output is required; use a fresh directory")
    args.output = args.output.resolve()
    if args.output.exists():
        parser.error("--output already exists; preserve prior results and choose a new directory")
    if os.name != "posix":
        parser.error("this native stress runner requires a POSIX host")
    args.server, args.cli = args.server.resolve(), args.cli.resolve()
    smoke = args.profile == "smoke"
    args.read_seconds = args.read_seconds or (2 if smoke else 25)
    args.full_seconds = args.full_seconds or (3 if smoke else 35)
    args.write_seconds = args.write_seconds or (30 if smoke else 180)
    sources = [REPO / "sdk/python/src", REPO / "tools/workload/src", REPO / "tools/verification/src"]
    sys.path[:0] = list(map(str, sources))
    from harness import Harness, save

    harness = Harness(args)
    CapacityStress.harness = harness
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(CapacityStress)
    interrupted = False
    cleanup_error = None
    try:
        result = unittest.TextTestRunner(verbosity=2).run(suite)
    except KeyboardInterrupt:
        interrupted = True
        raise
    finally:
        try:
            harness.cleanup()
        except Exception as error:
            cleanup_error = repr(error)
            traceback.print_exc()
        if harness.owns_output:
            save(args.output / "result.json", {
                "status": "INTERRUPTED" if interrupted else
                          "PASS" if "result" in locals() and result.wasSuccessful() and cleanup_error is None else "FAIL",
                "tests_run": result.testsRun if "result" in locals() else 0,
                "failures": [{"test": str(test), "detail": detail} for test, detail in result.failures]
                            if "result" in locals() else [],
                "errors": [{"test": str(test), "detail": detail} for test, detail in result.errors]
                          if "result" in locals() else [],
                "assessed_phases": len(harness.results),
                "backpressure_phases": [p["name"] for p in harness.results if p["assessment"] == "BACKPRESSURE"],
                "measurement": "accepted backpressure remains explicit; no absolute throughput threshold",
                "cleanup_error": cleanup_error,
            })
    passed = result.wasSuccessful() and cleanup_error is None
    print(json.dumps({"result": str(args.output / "result.json"), "passed": passed}))
    return 0 if passed else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
