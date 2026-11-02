"""Tests for carrying one build of the workspace's unit tests to the job that runs them."""

from __future__ import annotations

import io
import json
import os
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
import re
import subprocess
import tempfile
import unittest
from unittest import mock

from scripts import scenario_shards as shards
from scripts import unit_test_archive as archive
from scripts.tests.test_scenario_shards import TARGETS, Checkout, fake_ldd, write

UNIT_EXECUTABLE = shards.COVERAGE_DEBUG / "deps" / "nervix_cli-0f92e8a94feb2377"
ROOT = shards.ROOT


def artifact(root: Path, path: Path, directory: str, *, test: bool = True, executable: bool = True) -> str:
    """One line of Cargo's messages for an artifact of the package in `directory`."""
    return json.dumps({
        "reason": "compiler-artifact",
        "manifest_path": str(root / directory / "Cargo.toml"),
        "profile": {"test": test},
        "executable": str(root / path) if executable else None,
    })


class ProgramTests(unittest.TestCase):
    def test_the_programs_are_the_test_executables_in_build_order(self) -> None:
        root = Path("/checkout")
        library = Path("target/debug/deps/nervix_models-1")
        binary = Path("target/debug/nervix-cli")
        unit = Path("target/debug/deps/nervix_cli-2")
        integration = Path("target/debug/deps/report_cli-3")
        messages = [
            artifact(root, library, "crates/models", test=False, executable=False),
            artifact(root, binary, "crates/cli", test=False),
            artifact(root, unit, "crates/cli"),
            json.dumps({"reason": "build-script-executed"}),
            artifact(root, integration, "crates/deadlock"),
            artifact(root, unit, "crates/cli"),
            json.dumps({"reason": "build-finished", "success": True}),
        ]
        self.assertEqual(archive.programs_of(root, messages), [
            archive.TestProgram(unit.as_posix(), "crates/cli"),
            archive.TestProgram(integration.as_posix(), "crates/deadlock"),
        ])

    def test_the_root_package_runs_in_the_checkout(self) -> None:
        root = Path("/checkout")
        unit = Path("target/debug/deps/nervix_server-1")
        message = artifact(root, unit, ".")
        self.assertEqual(archive.programs_of(root, [message]), [archive.TestProgram(unit.as_posix(), ".")])


class Recorded(Checkout):
    """A checkout whose build recorded one test executable beside the scenario suite."""

    def __init__(self, root: Path) -> None:
        super().__init__(root)
        resolved = root.resolve()
        write(root / archive.MESSAGES, (artifact(resolved, UNIT_EXECUTABLE, "crates/cli") + "\n").encode())
        with redirect_stdout(io.StringIO()):
            archive.plan(resolved)

    def pack_tests(self, path: Path) -> shards.Manifest:
        with mock.patch.object(shards, "workspace_targets", return_value=TARGETS), redirect_stdout(io.StringIO()):
            return archive.pack(self.root.resolve(), path)


class ArchiveTests(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)

    def test_a_build_without_messages_or_test_executables_plans_nothing(self) -> None:
        checkout = Checkout(self.directory / "checkout")
        with self.assertRaisesRegex(shards.ShardError, "just test-coverage-build"):
            archive.plan(checkout.root)
        write(checkout.root / archive.MESSAGES, b"")
        with self.assertRaisesRegex(shards.ShardError, "no test executable"):
            archive.plan(checkout.root)

    def test_the_archive_holds_the_tests_and_their_inputs_without_the_scenario_suite(self) -> None:
        checkout = Recorded(self.directory / "checkout")
        manifest = checkout.pack_tests(self.directory / "unit-tests.tar.zst")
        held = {file.path for file in manifest.files}
        self.assertIn(UNIT_EXECUTABLE.as_posix(), held)
        self.assertIn(archive.PROGRAMS.as_posix(), held)
        self.assertIn((shards.ONNX_FIXTURES / "simple_score.onnx").as_posix(), held)
        for guest in shards.WASM_GUESTS:
            self.assertIn(guest.as_posix(), held)
        self.assertNotIn((shards.COVERAGE_DEBUG / "deps" / "scenarios-4d0e03c611a711f3").as_posix(), held)

    def test_a_test_executable_the_build_did_not_leave_fails_the_pack(self) -> None:
        checkout = Recorded(self.directory / "checkout")
        (checkout.root / UNIT_EXECUTABLE).unlink()
        with self.assertRaisesRegex(shards.ShardError, "the build left no"):
            checkout.pack_tests(self.directory / "unit-tests.tar.zst")

    def test_a_restore_brings_back_what_a_run_starts(self) -> None:
        build = Recorded(self.directory / "checkout")
        packed = build.pack_tests(self.directory / "unit-tests.tar.zst")
        saved = self.directory / "saved"
        (build.root / "target").rename(saved)
        path = os.environ["PATH"]
        tools = fake_ldd(self.directory / "tools", "nothing")
        with (
            mock.patch.dict(os.environ, {"PATH": f"{tools}:{path}"}),
            mock.patch.object(archive, "library_path", return_value="/toolchain"),
            redirect_stdout(io.StringIO()),
        ):
            restored = archive.unpack(build.root.resolve(), self.directory / "unit-tests.tar.zst")
        self.assertEqual(restored.files, packed.files)
        self.assertEqual(archive.read_programs(build.root.resolve()), [
            archive.TestProgram(UNIT_EXECUTABLE.as_posix(), "crates/cli"),
        ])


    def test_a_restore_resolves_libraries_where_a_run_finds_them(self) -> None:
        build = Recorded(self.directory / "checkout")
        build.pack_tests(self.directory / "unit-tests.tar.zst")
        tools = self.directory / "tools"
        tools.mkdir()
        script = tools / "ldd"
        script.write_text(
            '#!/bin/sh\ncase "$LD_LIBRARY_PATH" in */toolchain*) ;; '
            '*) printf "\\tlibstd-1.so => not found\\n" ;; esac\n'
        )
        script.chmod(0o755)
        path = os.environ["PATH"]
        with (
            mock.patch.dict(os.environ, {"PATH": f"{tools}:{path}"}),
            mock.patch.object(archive, "library_path", return_value="/toolchain"),
            redirect_stdout(io.StringIO()),
        ):
            archive.unpack(build.root.resolve(), self.directory / "unit-tests.tar.zst")
            with self.assertRaisesRegex(shards.ShardError, "libstd-1.so"):
                shards.restore_archive(build.root.resolve(), self.directory / "unit-tests.tar.zst", archive.MANIFEST)


class LibraryPathTests(unittest.TestCase):
    def test_a_run_finds_the_build_then_the_toolchain_then_what_it_inherited(self) -> None:
        root = Path("/checkout")
        completed = subprocess.CompletedProcess([], 0, stdout="/toolchain/lib\n")
        with (
            mock.patch.object(subprocess, "run", return_value=completed),
            mock.patch.dict(os.environ, {"LD_LIBRARY_PATH": "/inherited"}),
        ):
            path = archive.library_path(root)
        debug = root / shards.COVERAGE_DEBUG
        self.assertEqual(path.split(os.pathsep), [str(debug / "deps"), str(debug), "/toolchain/lib", "/inherited"])


class RunTests(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name).resolve()
        self.log = self.root / "started"
        patcher = mock.patch.object(archive, "library_path", return_value="/toolchain")
        patcher.start()
        self.addCleanup(patcher.stop)

    def program(self, name: str, directory: str, status: int = 0) -> archive.TestProgram:
        """A stand-in test executable that records where it ran and exits with `status`."""
        executable = Path("target/debug/deps") / name
        script = (
            f'#!/bin/sh\nprintf "%s %s %s %s\\n" {name} "$PWD" "$CARGO_MANIFEST_DIR" "$LD_LIBRARY_PATH" '
            f'>> "{self.log}"\nexit {status}\n'
        )
        write(self.root / executable, script.encode(), executable=True)
        (self.root / directory).mkdir(parents=True, exist_ok=True)
        return archive.TestProgram(executable.as_posix(), directory)

    def plan(self, programs: list[archive.TestProgram]) -> None:
        write(self.root / archive.PROGRAMS, json.dumps([program.to_json() for program in programs]).encode())

    def test_each_executable_runs_from_its_package_in_order(self) -> None:
        self.plan([self.program("first", "crates/first"), self.program("second", ".")])
        with redirect_stdout(io.StringIO()):
            archive.run(self.root)
        self.assertEqual(self.log.read_text().splitlines(), [
            f"first {self.root}/crates/first {self.root}/crates/first /toolchain",
            f"second {self.root} {self.root} /toolchain",
        ])

    def test_a_run_goes_on_after_a_failure_and_names_every_one_that_failed(self) -> None:
        self.plan([
            self.program("first", "crates/first", status=101),
            self.program("second", "crates/second"),
            self.program("third", "crates/third", status=3),
        ])
        with redirect_stdout(io.StringIO()), self.assertRaises(shards.ShardError) as failure:
            archive.run(self.root)
        self.assertEqual(len(self.log.read_text().splitlines()), 3)
        message = str(failure.exception)
        self.assertIn("2 of 3 test executables failed", message)
        self.assertIn("target/debug/deps/first exited 101", message)
        self.assertIn("target/debug/deps/third exited 3", message)
        self.assertNotIn("deps/second", message)

    def test_a_run_without_a_plan_says_what_to_do(self) -> None:
        with self.assertRaisesRegex(shards.ShardError, "build or restore the unit tests first"):
            archive.run(self.root)

    def test_the_command_reports_a_failure_as_its_status(self) -> None:
        errors = io.StringIO()
        with mock.patch.object(archive, "ROOT", self.root), redirect_stderr(errors):
            status = archive.main(["run"])
        self.assertEqual(status, 1)
        self.assertIn("unit tests: ", errors.getvalue())


class WorkflowTests(unittest.TestCase):
    """One job builds the scenario suite and the unit tests, and the tests job only runs them."""

    def setUp(self) -> None:
        self.workflow = (ROOT / ".github/workflows/check.yaml").read_text()

    def job(self, name: str) -> str:
        following = self.workflow.split(f"\n  {name}:\n", 1)[1]
        return re.split(r"\n  [a-z][a-z0-9-]*:\n", following, maxsplit=1)[0]

    def test_the_build_packs_the_suite_before_the_unit_tests_join_it(self) -> None:
        build = self.job("scenarios-build")
        steps = ["just test-scenarios-coverage-build", "just test-scenarios-coverage-pack", "just test-coverage-binaries",
                 "Publish kache cache after the build", "just test-coverage-pack", "name: scenario-suite\n",
                 "name: unit-tests\n"]
        positions = [build.index(step) for step in steps]
        self.assertEqual(positions, sorted(positions))
        upload = build[build.index("name: unit-tests\n"):]
        self.assertIn("compression-level: 0", upload)
        self.assertIn("retention: transfer", upload)

    def test_the_build_publishes_the_checkout_its_archives_restore_at(self) -> None:
        self.assertIn("checkout: ${{ github.workspace }}\n", self.job("scenarios-build"))

    def test_the_jobs_on_another_provider_restore_and_run_at_the_builds_checkout(self) -> None:
        directory = "working-directory: ${{ needs.scenarios-build.outputs.checkout }}\n"
        for name, restore, run in (
            ("tests", "just test-coverage-unpack", "just test-coverage-run"),
            ("scenarios", "just test-scenarios-coverage-unpack", "just test-scenarios-coverage-shard"),
        ):
            with self.subTest(job=name):
                job = self.job(name)
                self.assertIn("needs: [scenarios-build]", job)
                self.assertLess(job.index('sudo mount --bind "${GITHUB_WORKSPACE}" "${BUILD_CHECKOUT}"'), job.index(restore))
                self.assertEqual(job.count(directory), 2)
                for command in (restore, run):
                    step = job[job.rindex("- name:", 0, job.index(command)):job.index(command)]
                    self.assertIn(directory, step)

    def test_the_tests_job_restores_the_build_and_runs_it_without_compiling(self) -> None:
        tests = self.job("tests")
        self.assertIn("needs: [scenarios-build]", tests)
        steps = ["python3 scripts/kache_remote.py lighten-volume /tmp", "name: unit-tests\n",
                 'just test-coverage-unpack "${RUNNER_TEMP}/unit-tests/unit-tests.tar.zst"',
                 "just test-coverage-run", "name: coverage-tests\n"]
        positions = [tests.index(step) for step in steps]
        self.assertEqual(positions, sorted(positions))
        for setup in ("kunobi-ninja/kache-action", "kache_remote.py start", "kache_remote.py publish",
                      "test-coverage-build", "toolchains-install"):
            self.assertNotIn(setup, tests)


class RecipeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.justfile = (ROOT / "justfile").read_text()

    def recipe(self, name: str) -> str:
        following = self.justfile.split(f"\n{name}", 1)[1]
        return re.split(r"\n\n", following, maxsplit=1)[0]

    def test_the_build_records_what_a_run_starts_and_the_run_starts_it_directly(self) -> None:
        build = self.recipe("test-coverage-binaries:")
        self.assertIn("--message-format=json-render-diagnostics", build)
        self.assertIn("python3 -m scripts.unit_test_archive plan", build)
        run = self.recipe("test-coverage-run:")
        self.assertIn("python3 -m scripts.unit_test_archive run", run)
        self.assertNotIn("cargo llvm-cov --no-report", run)

    def test_the_messages_lie_where_the_plan_reads_them(self) -> None:
        self.assertIn(archive.MESSAGES.as_posix(), self.recipe("test-coverage-binaries:"))


if __name__ == "__main__":
    unittest.main()
