"""Explicitly launched stress cases; no default pytest or CTest registration."""

import json
import unittest


class CapacityStress(unittest.TestCase):
    harness = None

    @classmethod
    def setUpClass(cls):
        cls.addClassCleanup(cls.harness.cleanup)
        cls.harness.setup()

    def test_01_narrow_writes_one_and_two_sessions(self):
        for sessions in (1, 2):
            with self.subTest(sessions=sessions):
                self.harness.write(2, sessions)

    def test_02_wide_writes(self):
        for width in (16, 64):
            with self.subTest(fields=width):
                self.harness.write(width, 2)

    def test_03_concurrent_projected_reads(self):
        harness = self.harness
        limit = min(1000, harness.rows(2))
        for sessions in harness.args.sessions:
            with self.subTest(sessions=sessions):
                harness.read(f"read-projection-limit{limit}-u{sessions}", 2, sessions,
                             "json", {"projection": ["key", "c1"], "limit": limit},
                             harness.args.read_seconds, expected_rows=limit)

    def test_04_full_json_and_arrow(self):
        harness = self.harness
        for format in ("json", "arrow"):
            with self.subTest(format=format):
                harness.read(f"read-full-{format}-u2", 2, 2, format, {},
                             harness.args.full_seconds, expected_rows=harness.rows(2))

    def test_05_wide_full_scans(self):
        harness = self.harness
        for width, format in ((16, "json"), (64, "arrow")):
            with self.subTest(fields=width, format=format):
                harness.read(f"read-width{width}-full-{format}-u2", width, 2, format, {},
                             harness.args.full_seconds, expected_rows=harness.rows(width))

    def test_06_projected_and_filtered_reads_under_load(self):
        from glamdb_sdk import col

        harness = self.harness
        harness.read("read-width64-projected-u8", 64, 8, "json",
                     {"projection": ["key", "c1"], "limit": 1000},
                     harness.args.read_seconds, expected_rows=min(1000, harness.rows(64)))
        # Let the typed SDK construct exact int64 wire values.
        expected_filtered = harness.filtered_row_count()
        for sessions in sorted({2, max(harness.args.sessions)}):
            with self.subTest(sessions=sessions):
                harness.read(f"read-filter-arrow-u{sessions}", 2, sessions, "arrow",
                             {"projection": ["key", "c1"], "predicate": col("c1", "int64").ge(500).to_dict(),
                              "limit": 1000}, harness.args.read_seconds, expected_rows=expected_filtered)

    def test_07_graceful_reopen_preserves_committed_rows_and_receipts(self):
        from harness import save

        harness = self.harness
        self.assertEqual(len(harness.writes), 4, "write fixtures did not all complete")
        receipts = {}
        with harness.client() as client:
            for target in harness.writes:
                for operation in harness.operations(harness.output / target["report"]):
                    outcome = client.find_append_outcome(target["collection"], operation,
                                                         deadline=harness.args.timeout)
                    self.assertIsNotNone(outcome)
                    self.assertEqual(outcome.outcome, "committed")
                    receipts[operation] = outcome.result
        save(harness.output / "receipts-before-reopen.json", receipts)
        harness.stop()
        harness.start()
        for target in harness.writes:
            with self.subTest(target=target["report"]):
                report = harness.output / target["report"]
                harness.command(target["report"] + "-verify-reopen",
                                ["bench", "verify", "--url", harness.url, "--run", report,
                                 "--format", "arrow", "--timeout", harness.args.timeout],
                                timeout=harness.args.timeout + 60, expected=0)
                verified = json.loads((report / "verify.json").read_bytes())
                self.assertEqual(verified["status"], "PASS")
                self.assertEqual(verified["actual_rows"], harness.rows(target["width"]))
                with harness.client() as client:
                    for operation in harness.operations(report):
                        outcome = client.find_append_outcome(target["collection"], operation,
                                                             deadline=harness.args.timeout)
                        self.assertIsNotNone(outcome)
                        self.assertEqual(outcome.outcome, "committed")
                        self.assertEqual(outcome.result, receipts[operation])
