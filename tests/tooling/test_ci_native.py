import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]


class CiNativeRunnerTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="glamdb ci ")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        (self.root / "scripts").mkdir()
        (self.root / "bin").mkdir()
        self.runner = self.root / "scripts/ci-native.sh"
        if (ROOT / "scripts/ci-native.sh").exists():
            shutil.copyfile(ROOT / "scripts/ci-native.sh", self.runner)

    def run_script(self, *arguments):
        return subprocess.run(
            ["bash", str(self.runner), *arguments], cwd=self.root,
            env={**os.environ, "PATH": str(self.root / "bin") + os.pathsep + os.environ["PATH"]},
            text=True, capture_output=True, timeout=15,
        )

    def record_commands(self, *names):
        log = self.root / "commands.jsonl"
        for name in names:
            command = self.root / "bin" / name
            command.write_text(
                "#!/usr/bin/env python3\nimport json, sys\n"
                f"with open({str(log)!r}, 'a') as output:\n"
                f"    print(json.dumps([{name!r}, *sys.argv[1:]]), file=output)\n"
            )
            command.chmod(0o755)
        return log

    def test_root_build_includes_the_io_probe(self):
        log = self.record_commands("cmake")
        result = self.run_script("normal", "build")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        commands = [json.loads(line) for line in log.read_text().splitlines()]
        # The normal profile also configures and builds the engine without storage.
        self.assertEqual(len(commands), 4)
        self.assertIn("-DGLAMDB_BUILD_STORAGE=ON", commands[0])
        self.assertIn("-DGLAMDB_BUILD_IO_PROBE=ON", commands[0])
        self.assertEqual(commands[1][:2], ["cmake", "--build"])
        self.assertIn("-DGLAMDB_BUILD_STORAGE=OFF", commands[2])
        self.assertIn("-DGLAMDB_IO_ONLY=OFF", commands[2])
        self.assertNotIn("-DGLAMDB_BUILD_IO_PROBE=ON", commands[2])
        self.assertEqual(commands[3][:3], ["cmake", "--build", str(self.root / "build/ci-normal-nostorage")])

    def test_discovers_standalone_probes_without_reconfiguring_the_io_target(self):
        log = self.record_commands("cmake", "ctest")
        for name in ("build_install", "new_probe", "toolchain", "io"):
            source = self.root / "probes" / name
            source.mkdir(parents=True)
            (source / "CMakeLists.txt").touch()
        for profile in ("normal", "asan"):
            with self.subTest(profile=profile):
                log.write_text("")
                result = self.run_script(profile, "test")
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                commands = [json.loads(line) for line in log.read_text().splitlines()]
                configured = [command[2] for command in commands if command[:2] == ["cmake", "-S"]]
                expected = {"probes/build_install", "probes/new_probe"}
                if profile == "normal":
                    expected.add("probes/toolchain")
                self.assertEqual(set(configured), expected)
                tested = [command[2] for command in commands if command[:2] == ["ctest", "--test-dir"]]
                build = str(self.root / f"build/ci-{profile}")
                directories = {build, *(f"{build}-probe-{Path(path).name}" for path in expected)}
                if profile == "normal":
                    directories.add(f"{build}-nostorage")
                self.assertEqual(set(tested), directories)
                labels = [command[command.index("-L") + 1] for command in commands if "-L" in command]
                self.assertEqual(labels, ["vertical", "mock"] if profile == "normal" else [])

    def test_invalid_profile_or_stage_has_no_build_side_effects(self):
        for arguments in (("bogus", "test"), ("normal", "bogus"), ("asan",), ("normal", "test", "extra")):
            with self.subTest(arguments=arguments):
                result = self.run_script(*arguments)
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertFalse((self.root / "build").exists())

    def test_build_failure_keeps_exit_code_and_diagnostic_log(self):
        cmake = self.root / "bin/cmake"
        cmake.write_text('#!/bin/sh\nprintf "%s\\n" "compiler failure details" >&2\nexit 17\n')
        cmake.chmod(0o755)
        result = self.run_script("normal", "build")
        self.assertEqual(result.returncode, 17, result.stderr)
        self.assertIn("compiler failure details", result.stdout + result.stderr)
        self.assertIn("compiler failure details", (self.root / "artifacts/native-normal/build.log").read_text())

    def test_real_ctest_failure_is_reported_and_fails_the_runner(self):
        source = self.root / "fixture"
        source.mkdir()
        (source / "fail.py").write_text('print("intentional test failure details")\nraise SystemExit(42)\n')
        (source / "CMakeLists.txt").write_text(
            'cmake_minimum_required(VERSION 3.28)\nproject(Failure LANGUAGES NONE)\n'
            'enable_testing()\n'
            f'add_test(NAME intentional-failure COMMAND "{sys.executable}" "{source}/fail.py")\n'
        )
        generator = os.environ.get("GLAMDB_TEST_GENERATOR", "Ninja")
        build = self.root / "build/ci-normal"
        subprocess.run(["cmake", "-S", str(source), "-B", str(build), "-G", generator],
                       check=True, capture_output=True)
        self.assertIn(f"CMAKE_GENERATOR:INTERNAL={generator}\n",
                      (build / "CMakeCache.txt").read_text())
        result = self.run_script("normal", "test")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("intentional test failure details", result.stdout)
        self.assertIn("intentional-failure", (self.root / "artifacts/native-normal/project-junit.xml").read_text())
        self.assertIn("intentional test failure details", (self.root / "artifacts/native-normal/test.log").read_text())


if __name__ == "__main__":
    unittest.main()
