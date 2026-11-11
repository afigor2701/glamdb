"""Exercise owned-process cleanup with real children, without database load."""

import json
import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "sdk/python/src"))
from harness import Harness, terminate_group


class ProcessCleanup(unittest.TestCase):
    def child(self):
        program = ("import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); "
                   "print('ready', flush=True); time.sleep(60)")
        process = subprocess.Popen([sys.executable, "-c", program], stdout=subprocess.PIPE,
                                   text=True, start_new_session=True)
        self.addCleanup(self.reap, process)
        self.assertTrue(select.select([process.stdout], [], [], 5)[0])
        self.assertEqual(process.stdout.readline().strip(), "ready")
        return process

    @staticmethod
    def reap(process):
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=5)
        process.stdout.close()

    def interrupt(self):
        timer = threading.Timer(0.15, lambda: os.kill(os.getpid(), signal.SIGINT))
        timer.start()
        self.addCleanup(timer.cancel)
        return timer

    def test_timeout_kills_and_reaps_a_child_ignoring_termination(self):
        process = self.child()
        terminate_group(process, timeout=0.05)
        self.assertEqual(process.poll(), -signal.SIGKILL)

    def test_interrupt_during_wait_still_kills_and_reaps_owned_child(self):
        process = self.child()
        self.interrupt()
        with self.assertRaises(KeyboardInterrupt):
            terminate_group(process, timeout=10)
        self.assertIsNotNone(process.poll(), "interrupt left an owned child running")

    def test_interrupted_server_stop_preserves_cleanup_and_exit_evidence(self):
        process = self.child()
        with tempfile.TemporaryDirectory() as root:
            harness = Harness.__new__(Harness)
            harness.output = Path(root)
            harness.server = process
            harness.server_log = (Path(root) / "server.log").open("w")
            self.addCleanup(harness.server_log.close)
            self.interrupt()
            with self.assertRaises(KeyboardInterrupt):
                harness.stop()
            self.assertIsNotNone(process.poll(), "server ownership was discarded before termination")
            self.assertIsNone(harness.server)
            records = list(Path(root).glob("server-stop-*.json"))
            self.assertEqual(len(records), 1)
            self.assertEqual(json.loads(records[0].read_text())["exit_code"], -signal.SIGKILL)
            self.assertTrue(harness.server_log.closed)


if __name__ == "__main__":
    unittest.main()
