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
from scripts.tests.test_scenario_shards import TARGETS, Checkout, fake_ldd, stripped, write

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

    def test_a_restored_jvm_test_reads_its_packaged_java_inputs(self) -> None:
        build = Recorded(self.directory / "checkout")
        package = build.root / "crates/client-jvm"
        package.mkdir(parents=True)
        java = Path("target/client-conformance/java")
        inputs = {
            java / "nervix-client.jar": b"packaged JNI library",
            java / "tests/io/nervix/client/Properties.class": b"property fixture",
            java / "tests/io/nervix/client/BoundaryTests.class": b"boundary fixture",
            java / "tests/io/nervix/client/Properties$Input.class": b"nested fixture",
        }
        for path, content in inputs.items():
            write(build.root / path, content)
        program = archive.TestProgram(UNIT_EXECUTABLE.as_posix(), "crates/client-jvm")
        write(build.root / archive.PROGRAMS, json.dumps([program.to_json()]).encode())
        build.pack_tests(self.directory / "unit-tests.tar.zst")
        (build.root / "target").rename(self.directory / "saved")
        tools = fake_ldd(self.directory / "tools", "nothing")
        with (
            mock.patch.dict(os.environ, {"PATH": f"{tools}:{os.environ['PATH']}"}),
            mock.patch.object(archive, "library_path", return_value="/toolchain"),
            redirect_stdout(io.StringIO()),
        ):
            archive.unpack(build.root.resolve(), self.directory / "unit-tests.tar.zst")
        for path, content in inputs.items():
            self.assertEqual((build.root / path).read_bytes(), content)

    def test_packing_jvm_tests_requires_their_jar_and_fixture_entry_classes(self) -> None:
        build = Recorded(self.directory / "checkout")
        program = archive.TestProgram(UNIT_EXECUTABLE.as_posix(), "crates/client-jvm")
        write(build.root / archive.PROGRAMS, json.dumps([program.to_json()]).encode())
        java = Path("target/client-conformance/java")
        for path in (java / "nervix-client.jar", java / "tests/io/nervix/client/Properties.class",
                     java / "tests/io/nervix/client/BoundaryTests.class"):
            with self.assertRaisesRegex(shards.ShardError, "just build-client-jvm-tests") as failure:
                build.pack_tests(self.directory / "unit-tests.tar.zst")
            self.assertIn(path.as_posix(), str(failure.exception))
            write(build.root / path, b"prepared input")

    def test_native_boundary_inputs_are_complete_and_retained(self) -> None:
        root = self.directory / "native"
        root.mkdir()
        program = archive.TestProgram("target/llvm-cov-target/debug/deps/native_suites-abcd",
                                      "crates/client-ffi")
        expected = [archive.NATIVE_ARTIFACTS / f"{host}-suite{suffix}"
                    for host in ("c", "cpp") for suffix in ("", "-sanitized")]
        expected.append(shards.COVERAGE_DEBUG / "libnervix_client_ffi.so")
        for path in expected:
            with self.assertRaisesRegex(shards.ShardError, "just test-coverage-binaries") as failure:
                archive.native_inputs(root, [program])
            self.assertIn(path.as_posix(), str(failure.exception))
            write(root / path, path.as_posix().encode())
        self.assertEqual(archive.native_inputs(root, [program]), expected)
        self.assertEqual(archive.native_inputs(root, [archive.TestProgram("ffi-unit", "crates/client-ffi")]), [])

    def test_a_restored_native_boundary_test_keeps_both_programs_and_the_library(self) -> None:
        build = Recorded(self.directory / "checkout")
        native = shards.COVERAGE_DEBUG / "deps/native_suites-abcd"
        write(build.root / native, (build.root / UNIT_EXECUTABLE).read_bytes(), executable=True)
        program = archive.TestProgram(native.as_posix(), "crates/client-ffi")
        write(build.root / archive.PROGRAMS, json.dumps([program.to_json()]).encode())
        inputs = {archive.NATIVE_ARTIFACTS / f"{host}-suite{suffix}": f"{host}{suffix}".encode()
                  for host in ("c", "cpp") for suffix in ("", "-sanitized")}
        library = shards.COVERAGE_DEBUG / "libnervix_client_ffi.so"
        inputs[library] = (build.root / shards.COVERAGE_DEBUG / "deps/libnervix_client_ffi.so").read_bytes()
        for path, content in inputs.items():
            write(build.root / path, content, executable=True)
        with mock.patch.dict(globals(), TARGETS=TARGETS | {"native_suites"}):
            packed = build.pack_tests(self.directory / "unit-tests.tar.zst")
        self.assertEqual([file.path for file in packed.files].count(library.as_posix()), 1)
        inputs[library] = stripped(build.root / library)
        (build.root / "target").rename(self.directory / "saved")
        tools = fake_ldd(self.directory / "tools", "nothing")
        with (
            mock.patch.dict(os.environ, {"PATH": f"{tools}:{os.environ['PATH']}"}),
            mock.patch.object(archive, "library_path", return_value="/toolchain"),
            redirect_stdout(io.StringIO()),
        ):
            archive.unpack(build.root.resolve(), self.directory / "unit-tests.tar.zst")
        for path, content in inputs.items():
            self.assertEqual((build.root / path).read_bytes(), content)

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

    def runner_provider(self, name: str) -> str:
        return re.search(r"\n    runs-on: ([a-z]+)-", self.job(name))[1]

    def test_the_build_publishes_the_checkout_its_archives_restore_at(self) -> None:
        self.assertIn("checkout: ${{ github.workspace }}\n", self.job("scenarios-build"))

    def test_the_tests_job_restores_and_runs_at_the_builds_checkout(self) -> None:
        directory = "working-directory: ${{ needs.scenarios-build.outputs.checkout }}\n"
        job = self.job("tests")
        restore, run = "just test-coverage-unpack", "just test-coverage-run"
        self.assertIn("needs: [scenarios-build]", job)
        self.assertLess(job.index('sudo mount --bind "${GITHUB_WORKSPACE}" "${BUILD_CHECKOUT}"'), job.index(restore))
        self.assertEqual(job.count(directory), 2)
        for command in (restore, run):
            step = job[job.rindex("- name:", 0, job.index(command)):job.index(command)]
            self.assertIn(directory, step)

    def test_the_shards_restore_and_run_at_their_own_checkout_on_the_builds_provider(self) -> None:
        self.assertEqual(self.runner_provider("scenarios"), self.runner_provider("scenarios-build"))
        shards = self.job("scenarios")
        self.assertIn("needs: [scenarios-build]", shards)
        self.assertNotIn("mount --bind", shards)
        self.assertNotIn("working-directory:", shards)

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

    def test_both_unit_test_jobs_provision_java_at_the_binding_floor(self) -> None:
        for name in ("scenarios-build", "tests"):
            job = self.job(name)
            self.assertIn("name: Setup Java for embedded JVM tests", job)
            self.assertIn("uses: actions/setup-java@v4", job)
            self.assertIn('java-version: "21"', job)


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

    def test_workspace_tests_prepare_the_embedded_jvm_inputs_before_running(self) -> None:
        for name in ("test:", "test-coverage-binaries:"):
            self.assertIn("build-client-jvm-tests", self.recipe(name).splitlines()[0])


if __name__ == "__main__":
    unittest.main()
