"""Check manual entry-point guards without loading the SDK or starting a server."""

from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

RUNNER = Path(__file__).with_name("run.py")


class EntryPoint(unittest.TestCase):
    def test_list_does_not_create_output_or_require_binaries(self):
        with tempfile.TemporaryDirectory() as root:
            output = Path(root) / "unused"
            result = subprocess.run(
                [sys.executable, str(RUNNER), "--list", "--output", str(output)],
                capture_output=True, text=True, cwd=root,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("test_03_concurrent_projected_reads", result.stdout)
            self.assertFalse(output.exists())

    def test_invalid_sessions_and_durations_fail_before_creating_output(self):
        with tempfile.TemporaryDirectory() as root:
            output = Path(root) / "unused"
            for options in (("--sessions", "1,1"), ("--sessions", "0"),
                            ("--read-seconds", "0"), ("--timeout", "nan")):
                with self.subTest(options=options):
                    result = subprocess.run(
                        [sys.executable, str(RUNNER), "--output", str(output), *options],
                        capture_output=True, text=True,
                    )
                    self.assertEqual(result.returncode, 2, result.stderr)
                    self.assertFalse(output.exists())

    def test_existing_output_is_preserved_before_loading_dependencies(self):
        with tempfile.TemporaryDirectory() as root:
            sentinel = Path(root) / "sentinel.txt"
            sentinel.write_text("keep existing evidence")
            result = subprocess.run(
                [sys.executable, str(RUNNER), "--output", root],
                capture_output=True, text=True,
            )
            self.assertEqual(result.returncode, 2, result.stderr)
            self.assertEqual(sentinel.read_text(), "keep existing evidence")
            self.assertEqual(list(Path(root).iterdir()), [sentinel])


if __name__ == "__main__":
    unittest.main()
