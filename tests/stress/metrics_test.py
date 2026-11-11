"""Fast report-contract checks; run explicitly, without starting a database."""

import csv
import json
from pathlib import Path
import tempfile
import unittest

from metrics import assess_phase


class ReportContract(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.report = Path(self.temporary.name)
        self.summary = {
            "status": "PASS", "mode": "read", "elapsed_seconds": 10,
            "expected_workers": 1,
            "workers": [{"finished": True, "fatal": None,
                         "cpu_percent_one_core": 40, "rss_peak_bytes": 1000}],
            "operations": {"pending": 0, "committed": 0, "confirmed_rows": 0},
            "reads": {"completed_scans": 8, "consumed_rows": 800,
                      "failed_scans": 0, "errors": 0, "cancelled_scans": 0,
                      "partial_rows": 0},
            "dataset_fully_consumed": None,
        }
        self.requests, self.failures = 8, 0
        self.failure_rows = []
        self.exception_rows = []

    def save(self):
        (self.report / "summary.json").write_text(json.dumps(self.summary))
        with (self.report / "locust_stats.csv").open("w", newline="") as output:
            writer = csv.writer(output)
            writer.writerow(["Name", "Request Count", "Failure Count", "50%",
                             "95%", "99%", "Max Response Time"])
            writer.writerow(["Aggregated", self.requests, self.failures,
                             10, 20, 30, 33.25])
        with (self.report / "locust_failures.csv").open("w", newline="") as output:
            writer = csv.writer(output)
            writer.writerow(["Error", "Occurrences"])
            writer.writerows(self.failure_rows)
        with (self.report / "locust_exceptions.csv").open("w", newline="") as output:
            writer = csv.writer(output)
            writer.writerow(["Count", "Message", "Traceback", "Nodes"])
            writer.writerows(self.exception_rows)

    def assess(self, exit_code=0, **kwargs):
        self.save()
        return assess_phase(self.report, exit_code=exit_code, **kwargs)

    def backpressure(self, error="SERVER_BUSY"):
        self.requests, self.failures = 10, 2
        self.failure_rows = [(f"RuntimeError('{error}')", 2)]
        self.summary["status"] = "FAIL"
        self.summary["reads"].update(failed_scans=2, errors=2)

    def test_read_counts_only_completed_work_and_labels_attempt_percentiles(self):
        result = self.assess(expected_rows_per_scan=100)
        self.assertEqual(result["assessment"], "PASS")
        self.assertEqual(result["successful_requests_per_second"], 0.8)
        self.assertEqual(result["rows_per_second"], 80)
        self.assertEqual(result["latency_ms_all_attempts"]["p95"], 20)

    def test_backpressure_stays_visible_instead_of_inflating_goodput(self):
        self.backpressure()
        result = self.assess(exit_code=1, allow_backpressure=True)
        self.assertEqual(result["assessment"], "BACKPRESSURE")
        self.assertEqual(result["raw_status"], "FAIL")
        self.assertEqual(result["successful_requests"], 8)
        self.assertEqual(result["rejection_ratio"], 0.2)
        self.assertEqual(result["failure_codes"], {"SERVER_BUSY": 2})

    def test_strict_read_rejects_even_known_backpressure(self):
        self.backpressure()
        with self.assertRaises(AssertionError):
            self.assess(exit_code=1)

    def test_unknown_failure_is_not_accepted_as_overload(self):
        self.backpressure("CORRUPTION")
        with self.assertRaises(AssertionError):
            self.assess(exit_code=1, allow_backpressure=True)

    def test_incomplete_worker_is_not_accepted_as_overload(self):
        self.backpressure()
        self.summary["workers"][0]["finished"] = False
        with self.assertRaises(AssertionError):
            self.assess(exit_code=1, allow_backpressure=True)

    def test_missing_statistics_are_not_a_zero_error_pass(self):
        self.save()
        (self.report / "locust_stats.csv").unlink()
        with self.assertRaises((OSError, AssertionError)):
            assess_phase(self.report, exit_code=0)

    def test_locust_setup_exception_cannot_pass_at_reduced_load(self):
        self.exception_rows = [(1, "user setup failed", "on_start traceback", "local")]
        with self.assertRaises(AssertionError):
            self.assess()

    def test_missing_exception_report_cannot_pass(self):
        self.save()
        (self.report / "locust_exceptions.csv").unlink()
        with self.assertRaises((OSError, AssertionError)):
            assess_phase(self.report, exit_code=0)

    def test_mismatched_failure_counts_fail(self):
        self.backpressure()
        self.failure_rows = [("RuntimeError('SERVER_BUSY')", 1)]
        with self.assertRaises(AssertionError):
            self.assess(exit_code=1, allow_backpressure=True)

    def test_read_must_consume_the_complete_selected_view(self):
        with self.assertRaises(AssertionError):
            self.assess(expected_rows_per_scan=101)

    def test_shutdown_cancellation_does_not_count_partial_rows_as_goodput(self):
        self.summary["reads"].update(cancelled_scans=2, partial_rows=150)
        with self.assertRaises(AssertionError):
            self.assess()
        result = self.assess(allow_cancelled=True, expected_rows_per_scan=100)
        self.assertEqual(result["rows_per_second"], 80)
        self.assertEqual(result["cancelled_scans"], 2)
        self.assertEqual(result["partial_rows"], 150)

    def test_unexplained_partial_rows_fail_even_when_shutdown_is_allowed(self):
        self.summary["reads"]["partial_rows"] = 150
        with self.assertRaises(AssertionError):
            self.assess(allow_cancelled=True)

    def test_zero_successes_cannot_pass_an_overload_run(self):
        self.backpressure()
        self.requests = self.failures = 2
        self.summary["reads"].update(completed_scans=0, consumed_rows=0)
        with self.assertRaises(AssertionError):
            self.assess(exit_code=1, allow_backpressure=True)

    def test_write_requires_complete_input_and_no_pending_outcomes(self):
        self.summary.update(mode="write", dataset_fully_consumed=True)
        self.summary["reads"].update(completed_scans=0, consumed_rows=0)
        self.summary["operations"].update(committed=8, confirmed_rows=800)
        self.assertEqual(self.assess(expected_write_rows=800)["rows"], 800)
        self.summary["dataset_fully_consumed"] = False
        with self.assertRaises(AssertionError):
            self.assess(expected_write_rows=800)
        self.summary["dataset_fully_consumed"] = True
        self.summary["operations"]["pending"] = 1
        with self.assertRaises(AssertionError):
            self.assess(expected_write_rows=800)


if __name__ == "__main__":
    unittest.main()
