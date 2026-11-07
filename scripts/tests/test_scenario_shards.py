"""Tests for carrying one build of the scenario suite to its shards."""

from __future__ import annotations

import dataclasses
import io
import json
import os
import re
import shutil
import stat
import subprocess
import tempfile
import tomllib
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from scripts import scenario_shards as shards

# Workspace package and target names, as `cargo metadata --no-deps` lists some of them.
TARGETS = {"scenarios", "nervix-cli", "nervix-client-ffi", "nervix_client_ffi"}
ONNX_LIBRARY = Path(".nervix-deps/onnxruntime/onnxruntime-linux-x64-1.30.0/lib/libonnxruntime.so.1.30.0")
ONNX_LINK = ONNX_LIBRARY.parent / "libonnxruntime.so"
PYTHON_WHEEL = Path("target/paced-simulation/wheels/nervix_client-0.1.0-cp312-abi3-linux_x86_64.whl")


# What a stand-in object's debug information holds, which no stripped copy of it may.
DEBUG_MARK = b"stand-in DWARF of a coverage build"


def write(path: Path, content: bytes, executable: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    mode = 0o755 if executable else 0o644
    path.chmod(mode)


def elf(path: Path, content: bytes) -> None:
    """An ELF executable at `path`, as a coverage build leaves its objects: a real one, holding
    `content` in a section of its own and debug information marked with DEBUG_MARK."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as directory:
        held = Path(directory) / "content"
        held.write_bytes(content)
        debug = Path(directory) / "debug"
        debug.write_bytes(DEBUG_MARK * 64)
        subprocess.run(
            ["objcopy", f"--add-section=.nervix_content={held}", f"--add-section=.debug_info={debug}", "/usr/bin/true", str(path)],
            check=True,
        )
    path.chmod(0o755)


class Checkout:
    """A git checkout holding a stand-in coverage build and the inputs its scenarios read."""

    def __init__(self, root: Path) -> None:
        self.root = root
        root.mkdir(parents=True)
        debug = root / shards.COVERAGE_DEBUG
        elf(debug / "nervix-cli", b"cli")
        (debug / "deps").mkdir(parents=True)
        os.link(debug / "nervix-cli", debug / "deps" / "nervix_cli-0f92e8a94feb2377")
        driver = debug / "build" / "playwright-rs-5e6f" / "out" / "playwright-driver" / "playwright-1.62.1-linux"
        # The scenario executable holds the driver's path from compile time.
        executable = b"scenarios " + str(driver / "node").encode() + b" " + str(driver / "package/cli.js").encode()
        elf(debug / "deps" / "scenarios-4d0e03c611a711f3", executable)
        write(debug / "deps" / "scenarios-4d0e03c611a711f3.d", b"dependencies")
        elf(debug / "deps" / "libnervix_client_ffi.so", b"binding")
        write(debug / "deps" / "libnervix_client_ffi.rlib", b"rlib", executable=True)
        write(debug / "deps" / "libserde_derive-1a2b3c.so", b"\x7fELF macro", executable=True)
        write(debug / "nervix-cli.d", b"dependencies")
        write(debug / "incremental" / "nervix_cli-1" / "nervix-cli", b"\x7fELF stale", executable=True)
        write(debug / ".fingerprint" / "nervix-cli-1" / "nervix-cli", b"stamp", executable=True)
        write(debug / "build" / "nervix-cli-1" / "build-script-build", b"\x7fELF script", executable=True)
        write(driver / "node", b"\x7fELF node", executable=True)
        write(driver / "package" / "cli.js", b"cli")
        write(root / shards.ONNX_FIXTURES / "simple_score.onnx", b"model")
        write(root / ONNX_LIBRARY, b"\x7fELF onnxruntime", executable=True)
        (root / ONNX_LINK).symlink_to(ONNX_LIBRARY.name)
        write(root / PYTHON_WHEEL, b"wheel")
        for guest in shards.WASM_GUESTS:
            write(root / guest, b"\0asm")
        write(
            root / "scripts" / "download_onnxruntime.sh",
            f'printf "%s\\n" "${{PWD}}/{ONNX_LINK.as_posix()}"\n'.encode(),
        )
        self.git("init", "--quiet")
        self.git("add", "scripts")
        self.git("-c", "user.name=test", "-c", "user.email=test@example.com", "commit", "--quiet", "-m", "checkout")

    def git(self, *arguments: str) -> str:
        completed = subprocess.run(["git", *arguments], cwd=self.root, check=True, capture_output=True, text=True)
        return completed.stdout.strip()

    def pack(self, archive: Path) -> shards.Manifest:
        with mock.patch.object(shards, "workspace_targets", return_value=TARGETS), redirect_stdout(io.StringIO()):
            return shards.pack(self.root, archive)


def fake_ldd(directory: Path, missing_for: str) -> Path:
    """A `ldd` that reports a missing library for any file whose path contains `missing_for`."""
    directory.mkdir(parents=True, exist_ok=True)
    script = directory / "ldd"
    script.write_text(
        "#!/bin/sh\n"
        f'case "$1" in *{missing_for}*) printf "\\tlibmissing.so.1 => not found\\n" ;; esac\n'
        'printf "\\tlibc.so.6 => /lib/libc.so.6\\n"\n'
    )
    script.chmod(script.stat().st_mode | stat.S_IXUSR)
    return directory


def stripped(path: Path) -> bytes:
    """What `objcopy --strip-debug` leaves of the object at `path`."""
    with tempfile.TemporaryDirectory() as directory:
        copy = Path(directory) / "stripped"
        subprocess.run([*shards.STRIP, str(path), str(copy)], check=True)
        return copy.read_bytes()


class MatrixTests(unittest.TestCase):
    def test_a_matrix_holds_every_shard_counted_from_one(self) -> None:
        self.assertEqual(shards.matrix(1), [1])
        self.assertEqual(shards.matrix(3), [1, 2, 3])

    def test_a_matrix_holds_at_least_one_shard_and_at_most_the_workflow_limit(self) -> None:
        for count in (0, -1, shards.MAXIMUM_SHARDS + 1):
            with self.assertRaises(shards.ShardError):
                shards.matrix(count)

    def test_the_matrix_command_prints_json(self) -> None:
        output = io.StringIO()
        with redirect_stdout(output):
            status = shards.main(["matrix", "3"])
        self.assertEqual(status, 0)
        self.assertEqual(json.loads(output.getvalue()), [1, 2, 3])


class ObjectTests(unittest.TestCase):
    def test_the_object_pattern_matches_as_cargo_llvm_cov_does(self) -> None:
        pattern = shards.object_pattern({"nervix-cli", "nervix-client-ffi", "scenarios"})
        for stem in ("nervix-cli", "nervix_cli-0f92e8a9", "scenarios-4d0e03c6", "libnervix_client_ffi"):
            self.assertIsNotNone(pattern.match(stem), stem)
        for stem in ("serde_derive-1a2b", "nervix-cli-extra", "libscenarios-xyz", "nervix"):
            self.assertIsNone(pattern.match(stem), stem)

    def test_a_full_stem_drops_every_extension(self) -> None:
        self.assertEqual(shards.full_stem("libnervix_client_ffi.so"), "libnervix_client_ffi")
        self.assertEqual(shards.full_stem("a.tar.gz"), "a")
        self.assertEqual(shards.full_stem("nervix-cli"), "nervix-cli")

    def test_objects_are_the_workspace_executables_outside_incremental_fingerprint_and_build(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            checkout = Checkout(Path(directory) / "checkout")
            objects = shards.coverage_objects(checkout.root, shards.object_pattern(TARGETS))
        debug = shards.COVERAGE_DEBUG
        self.assertEqual(
            sorted(objects),
            sorted([
                debug / "nervix-cli",
                debug / "deps" / "libnervix_client_ffi.so",
                debug / "deps" / "nervix_cli-0f92e8a94feb2377",
                debug / "deps" / "scenarios-4d0e03c611a711f3",
            ]),
        )

    def test_the_build_must_leave_exactly_one_scenario_executable(self) -> None:
        deps = shards.COVERAGE_DEBUG / "deps"
        self.assertEqual(shards.scenario_executable([deps / "scenarios-ab12"]), deps / "scenarios-ab12")
        for objects in ([], [deps / "scenarios-ab12", deps / "scenarios-cd34"]):
            with self.assertRaises(shards.ShardError):
                shards.scenario_executable(objects)


class ArchiveTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.base = Path(self.directory.name)
        self.archive = self.base / "suite.tar.zst"

    def build(self) -> tuple[Checkout, shards.Manifest]:
        checkout = Checkout(self.base / "build")
        manifest = checkout.pack(self.archive)
        return checkout, manifest

    def shard_checkout(self, build: Checkout) -> Path:
        """A second checkout of the same revision, at the build's path once the build is gone."""
        root = build.root
        revision = build.git("rev-parse", "HEAD")
        source = self.base / "source"
        root.rename(source)
        subprocess.run(["git", "clone", "--quiet", str(source), str(root)], check=True)
        subprocess.run(["git", "checkout", "--quiet", revision], cwd=root, check=True)
        return root

    def unpack(self, root: Path) -> shards.Manifest:
        with redirect_stdout(io.StringIO()):
            return shards.unpack(root, self.archive)

    def test_a_shard_restores_every_packed_file_into_a_checkout_of_the_build_path(self) -> None:
        build, packed = self.build()
        objects = {path.as_posix() for path in shards.coverage_objects(build.root, shards.object_pattern(TARGETS))}
        expected = {}
        for file in packed.files:
            source = build.root / file.path
            if source.is_symlink():
                expected[file.path] = None
            elif file.path in objects:
                expected[file.path] = stripped(source)
            else:
                expected[file.path] = source.read_bytes()
        root = self.shard_checkout(build)
        restored = self.unpack(root)
        self.assertEqual(restored, packed)
        for path, content in expected.items():
            if content is None:
                self.assertTrue((root / path).is_symlink(), path)
            else:
                self.assertEqual((root / path).read_bytes(), content, path)
        debug = root / shards.COVERAGE_DEBUG
        self.assertEqual(
            (debug / "nervix-cli").stat().st_ino,
            (debug / "deps" / "nervix_cli-0f92e8a94feb2377").stat().st_ino,
            "a hard link stays one file",
        )
        self.assertTrue(os.access(root / shards.COVERAGE_DEBUG / "deps" / "scenarios-4d0e03c611a711f3", os.X_OK))
        packed_paths = {file.path for file in packed.files}
        driver = "target/llvm-cov-target/debug/build/playwright-rs-5e6f/out/playwright-driver/playwright-1.62.1-linux"
        self.assertIn(f"{driver}/node", packed_paths)
        self.assertIn(f"{driver}/package/cli.js", packed_paths)
        self.assertIn(ONNX_LINK.as_posix(), packed_paths)
        self.assertIn(ONNX_LIBRARY.as_posix(), packed_paths)
        self.assertNotIn("target/llvm-cov-target/debug/deps/libserde_derive-1a2b3c.so", packed_paths)
        self.assertFalse((debug / "build" / "nervix-cli-1" / "build-script-build").exists())

    def test_objects_are_packed_without_their_debug_information_and_the_build_keeps_its_own(self) -> None:
        build, packed = self.build()
        objects = shards.coverage_objects(build.root, shards.object_pattern(TARGETS))
        sizes = {file.path: file.size for file in packed.files}
        root = self.shard_checkout(build)
        self.unpack(root)
        source = self.base / "source"
        for path in objects:
            restored = (root / path).read_bytes()
            self.assertNotIn(DEBUG_MARK, restored, path)
            self.assertIn(DEBUG_MARK, (source / path).read_bytes(), "the build's object keeps its debug information")
            self.assertEqual(sizes[path.as_posix()], len(restored), "the manifest holds the stripped size")
            self.assertTrue(os.access(root / path, os.X_OK), path)
        # What the scenario executable names stays in it.
        executable = (root / shards.COVERAGE_DEBUG / "deps" / "scenarios-4d0e03c611a711f3").read_bytes()
        self.assertIn(b"/out/playwright-driver/playwright-1.62.1-linux/node", executable)
        self.assertFalse((source / shards.STRIPPED).exists(), "the stripped copies are gone once packed")

    def test_an_object_objcopy_cannot_read_fails_the_pack(self) -> None:
        checkout = Checkout(self.base / "build")
        unreadable = checkout.root / shards.COVERAGE_DEBUG / "deps" / "libnervix_client_ffi.so"
        write(unreadable, b"\x7fELF but not an object", executable=True)
        with self.assertRaisesRegex(shards.ShardError, r"stripping target/llvm-cov-target/debug/deps/libnervix_client_ffi\.so failed"):
            checkout.pack(self.archive)
        self.assertFalse((checkout.root / shards.STRIPPED).exists())

    def test_the_driver_wheel_is_carried_into_a_restored_checkout(self) -> None:
        build, manifest = self.build()
        self.assertIn(PYTHON_WHEEL.as_posix(), [file.path for file in manifest.files])
        root = self.shard_checkout(build)
        self.unpack(root)
        self.assertEqual((root / PYTHON_WHEEL).read_bytes(), b"wheel")

    def test_a_build_without_the_python_driver_package_cannot_be_packed(self) -> None:
        checkout = Checkout(self.base / "build")
        (checkout.root / PYTHON_WHEEL).unlink()
        with self.assertRaisesRegex(shards.ShardError, "Python.*wheel"):
            checkout.pack(self.archive)

    def test_a_build_with_two_python_driver_wheels_cannot_be_packed(self) -> None:
        checkout = Checkout(self.base / "build")
        write(checkout.root / PYTHON_WHEEL.with_name("nervix_client-0.2.0-cp312-abi3-linux_x86_64.whl"), b"another wheel")
        with self.assertRaisesRegex(shards.ShardError, "Python.*wheel"):
            checkout.pack(self.archive)

    def test_the_pack_holds_only_the_drivers_the_executable_names(self) -> None:
        checkout = Checkout(self.base / "build")
        debug = checkout.root / shards.COVERAGE_DEBUG
        stale = debug / "build" / "playwright-rs-0000" / "out" / "playwright-driver" / "playwright-1.61.0-linux"
        write(stale / "node", b"\x7fELF old node", executable=True)
        executable = shards.COVERAGE_DEBUG / "deps" / "scenarios-4d0e03c611a711f3"
        drivers = shards.playwright_drivers(checkout.root, executable)
        self.assertEqual(
            drivers,
            [shards.COVERAGE_DEBUG / "build" / "playwright-rs-5e6f" / "out" / "playwright-driver" / "playwright-1.62.1-linux"],
        )

    def test_an_executable_naming_a_driver_the_build_did_not_leave_fails_the_pack(self) -> None:
        checkout = Checkout(self.base / "build")
        debug = checkout.root / shards.COVERAGE_DEBUG
        shutil.rmtree(debug / "build" / "playwright-rs-5e6f")
        with self.assertRaisesRegex(shards.ShardError, "names a Playwright driver"):
            checkout.pack(self.archive)

    def test_an_executable_built_without_a_driver_packs_none(self) -> None:
        checkout = Checkout(self.base / "build")
        debug = checkout.root / shards.COVERAGE_DEBUG
        elf(debug / "deps" / "scenarios-4d0e03c611a711f3", b"scenarios")
        manifest = checkout.pack(self.archive)
        self.assertEqual([file.path for file in manifest.files if "playwright" in file.path], [])

    def test_the_executable_command_prints_the_restored_scenario_executable(self) -> None:
        build, packed = self.build()
        output = io.StringIO()
        with mock.patch.object(shards, "ROOT", build.root), redirect_stdout(output):
            status = shards.main(["executable"])
        self.assertEqual(status, 0)
        self.assertEqual(
            output.getvalue().strip(), str(build.root / shards.COVERAGE_DEBUG / "deps" / "scenarios-4d0e03c611a711f3")
        )

    def test_a_shard_refuses_an_archive_of_another_revision(self) -> None:
        build, _ = self.build()
        root = self.shard_checkout(build)
        write(root / "changed", b"change")
        subprocess.run(["git", "add", "changed"], cwd=root, check=True)
        subprocess.run(
            ["git", "-c", "user.name=test", "-c", "user.email=test@example.com", "commit", "--quiet", "-m", "next"],
            cwd=root,
            check=True,
        )
        with self.assertRaisesRegex(shards.ShardError, "was built from revision"):
            self.unpack(root)

    def test_a_shard_refuses_an_archive_built_at_another_path(self) -> None:
        build, _ = self.build()
        revision = build.git("rev-parse", "HEAD")
        elsewhere = self.base / "elsewhere"
        subprocess.run(["git", "clone", "--quiet", str(build.root), str(elsewhere)], check=True)
        subprocess.run(["git", "checkout", "--quiet", revision], cwd=elsewhere, check=True)
        with self.assertRaisesRegex(shards.ShardError, "hold absolute paths"):
            self.unpack(elsewhere)

    def test_a_shard_refuses_a_restored_file_whose_size_differs(self) -> None:
        build, packed = self.build()
        first, *rest = packed.files
        altered = dataclasses.replace(packed, files=(shards.SuiteFile(first.path, first.size + 1), *rest))
        root = self.shard_checkout(build)
        with mock.patch.object(shards.Manifest, "read", return_value=altered):
            with self.assertRaisesRegex(shards.ShardError, f"restored {first.path} with {first.size} bytes"):
                self.unpack(root)

    def test_a_shard_refuses_an_archive_missing_a_file_its_manifest_names(self) -> None:
        build, packed = self.build()
        altered = dataclasses.replace(packed, files=(*packed.files, shards.SuiteFile("target/absent", 1)))
        root = self.shard_checkout(build)
        with mock.patch.object(shards.Manifest, "read", return_value=altered):
            with self.assertRaisesRegex(shards.ShardError, "did not restore target/absent"):
                self.unpack(root)

    def test_a_shard_refuses_files_whose_shared_libraries_the_runner_lacks(self) -> None:
        build, _ = self.build()
        root = self.shard_checkout(build)
        tools = fake_ldd(self.base / "tools", "scenarios-")
        path = f"{tools}{os.pathsep}{os.environ['PATH']}"
        with mock.patch.dict(os.environ, {"PATH": path}):
            with self.assertRaisesRegex(shards.ShardError, "libmissing.so.1"):
                self.unpack(root)

    def test_a_missing_archive_is_reported(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(shards.ShardError, "is missing"):
                shards.unpack(Path(directory), Path(directory) / "absent.tar.zst")


class RecipeTests(unittest.TestCase):
    """The scenario build runs its Cargo builds beside each other, in one target directory each."""

    def setUp(self) -> None:
        self.justfile = (shards.ROOT / "justfile").read_text()

    def recipe(self, name: str) -> str:
        lines = []
        for line in self.justfile.split(f"\n{name}", 1)[1].splitlines():
            if lines and line and not line.startswith(" "):
                break
            lines.append(line)
        return "\n".join(lines)

    def test_each_helper_builds_beside_the_suite_in_a_directory_of_its_own(self) -> None:
        group = self.recipe("test-scenarios-coverage-binaries")
        self.assertIn("test-scenarios-coverage-suite", group)
        directories = re.findall(r'\(coverage_tools_dir / "([a-z-]+)"\)', group)
        self.assertEqual(sorted(directories), ["cli", "deadlock-report", "paced-simulation"])
        self.assertIn("[private, parallel]\ntest-scenarios-coverage-binaries:", self.justfile)
        self.assertIn('(run-with-jobs "test-scenarios-coverage-binaries" "6")', self.recipe("test-scenarios-coverage-build"))

    def test_every_binary_a_run_starts_ends_up_beside_the_suite(self) -> None:
        build = self.recipe("test-scenarios-coverage-build")
        shard = self.recipe("test-scenarios-coverage-shard")
        for binary in ("nervix-cli", "nervix-deadlock-report", "nervix-paced-simulation", "libnervix_client_ffi.so"):
            self.assertIn(f'/llvm-cov-target/debug/{binary}"', shard)
            self.assertIn(f'"${{suite}}/{binary}"', build)
        self.assertIn('"${suite}/nervix-plexus-format"', build)
        package = tomllib.loads((shards.ROOT / "crates/client-python/Cargo.toml").read_text())
        library = package["lib"].get("name", package["package"]["name"].replace("-", "_"))
        self.assertIn(f'"${{tools}}/paced-simulation/debug/lib{library}.so"', build)
        self.assertIn(f'"${{suite}}/lib{library}.so"', build)
        self.assertIn('NERVIX_PACED_SIMULATION_PYTHON=', shard)
        self.assertIn('install-paced-simulation-python', self.recipe("test-scenarios-coverage-unpack"))

    def test_the_prepared_runner_installs_the_instrumented_wheel(self) -> None:
        head = self.recipe("test-scenarios-coverage-run").splitlines()[0]
        self.assertEqual(head, ": install-paced-simulation-python")


class WorkflowTests(unittest.TestCase):
    """The Check workflow builds the suite once and runs it in the shards SCENARIO_SHARDS sets."""

    def setUp(self) -> None:
        self.workflow = (shards.ROOT / ".github/workflows/check.yaml").read_text()

    def job(self, name: str) -> str:
        following = self.workflow.split(f"\n  {name}:\n", 1)[1]
        return re.split(r"\n  [a-z][a-z0-9-]*:\n", following, maxsplit=1)[0]

    def test_one_build_plans_the_shards_and_packs_the_suite_they_restore(self) -> None:
        self.assertIn('  SCENARIO_SHARDS: "', self.workflow.split("\njobs:\n", 1)[0])
        build = self.job("scenarios-build")
        self.assertIn('python3 -m scripts.scenario_shards matrix "${SCENARIO_SHARDS}"', build)
        self.assertIn("shards: ${{ steps.shards.outputs.shards }}", build)
        steps = ["just test-scenarios-coverage-build",
                 'just test-scenarios-coverage-pack "${RUNNER_TEMP}/scenario-suite/scenario-suite.tar.zst"',
                 "Publish kache cache after the build", "name: scenario-suite\n", "just test-backup-cut-cleanup"]
        positions = [build.index(step) for step in steps]
        self.assertEqual(positions, sorted(positions))
        upload = build[build.index("name: scenario-suite\n"):]
        self.assertIn("compression-level: 0", upload)
        self.assertIn("retention: transfer", upload)

    def test_each_shard_restores_the_build_and_runs_its_share_without_compiling(self) -> None:
        shard = self.job("scenarios")
        self.assertIn("name: scenarios (${{ matrix.shard }} of ${{ strategy.job-total }})", shard)
        self.assertIn("needs: [scenarios-build]", shard)
        self.assertIn("shard: ${{ fromJSON(needs.scenarios-build.outputs.shards) }}", shard)
        self.assertIn("fail-fast: false", shard)
        steps = ["python3 scripts/kache_remote.py lighten-volume /tmp", "name: scenario-suite\n",
                 'just test-scenarios-coverage-unpack "${RUNNER_TEMP}/scenario-suite/scenario-suite.tar.zst"',
                 'just test-scenarios-coverage-shard "${SHARD}"', "name: coverage-scenarios-${{ matrix.shard }}\n"]
        positions = [shard.index(step) for step in steps]
        self.assertEqual(positions, sorted(positions))
        self.assertIn("SHARD: ${{ matrix.shard }}/${{ strategy.job-total }}", shard)
        # A shard compiles nothing, so it neither starts the compiler cache nor builds.
        for setup in ("kunobi-ninja/kache-action", "kache_remote.py start", "kache_remote.py publish",
                      "test-scenarios-coverage-build", "toolchains-install"):
            self.assertNotIn(setup, shard)


if __name__ == "__main__":
    unittest.main()
