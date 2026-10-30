"""Tests for carrying one build of the scenario suite to its shards."""

from __future__ import annotations

import dataclasses
import io
import json
import os
import shutil
import stat
import subprocess
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from scripts import scenario_shards as shards

# Workspace package and target names, as `cargo metadata --no-deps` lists some of them.
TARGETS = {"nervix-server", "scenarios", "nervix-cli", "nervix-client-ffi", "nervix_client_ffi"}
ONNX_LIBRARY = Path(".nervix-deps/onnxruntime/onnxruntime-linux-x64-1.30.0/lib/libonnxruntime.so.1.30.0")
ONNX_LINK = ONNX_LIBRARY.parent / "libonnxruntime.so"


def write(path: Path, content: bytes, executable: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    mode = 0o755 if executable else 0o644
    path.chmod(mode)


class Checkout:
    """A git checkout holding a stand-in coverage build and the inputs its scenarios read."""

    def __init__(self, root: Path) -> None:
        self.root = root
        root.mkdir(parents=True)
        debug = root / shards.COVERAGE_DEBUG
        write(debug / "nervix-server", b"\x7fELF server", executable=True)
        (debug / "deps").mkdir(parents=True)
        os.link(debug / "nervix-server", debug / "deps" / "nervix_server-0f92e8a94feb2377")
        driver = debug / "build" / "playwright-rs-5e6f" / "out" / "playwright-driver" / "playwright-1.62.1-linux"
        # The scenario executable holds the driver's path from compile time.
        executable = b"\x7fELF scenarios " + str(driver / "node").encode() + b" " + str(driver / "package/cli.js").encode()
        write(debug / "deps" / "scenarios-4d0e03c611a711f3", executable, executable=True)
        write(debug / "deps" / "scenarios-4d0e03c611a711f3.d", b"dependencies")
        write(debug / "deps" / "libnervix_client_ffi.so", b"\x7fELF binding", executable=True)
        write(debug / "deps" / "libnervix_client_ffi.rlib", b"rlib", executable=True)
        write(debug / "deps" / "libserde_derive-1a2b3c.so", b"\x7fELF macro", executable=True)
        write(debug / "nervix-cli", b"\x7fELF cli", executable=True)
        write(debug / "nervix-cli.d", b"dependencies")
        write(debug / "incremental" / "nervix_server-1" / "nervix-server", b"\x7fELF stale", executable=True)
        write(debug / ".fingerprint" / "nervix-server-1" / "nervix-server", b"stamp", executable=True)
        write(debug / "build" / "nervix-server-1" / "build-script-build", b"\x7fELF script", executable=True)
        write(driver / "node", b"\x7fELF node", executable=True)
        write(driver / "package" / "cli.js", b"cli")
        write(root / shards.ONNX_FIXTURES / "simple_score.onnx", b"model")
        write(root / ONNX_LIBRARY, b"\x7fELF onnxruntime", executable=True)
        (root / ONNX_LINK).symlink_to(ONNX_LIBRARY.name)
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
        pattern = shards.object_pattern({"nervix-server", "nervix-client-ffi", "scenarios"})
        for stem in ("nervix-server", "nervix_server-0f92e8a9", "scenarios-4d0e03c6", "libnervix_client_ffi"):
            self.assertIsNotNone(pattern.match(stem), stem)
        for stem in ("serde_derive-1a2b", "nervix-server-extra", "libscenarios-xyz", "nervix"):
            self.assertIsNone(pattern.match(stem), stem)

    def test_a_full_stem_drops_every_extension(self) -> None:
        self.assertEqual(shards.full_stem("libnervix_client_ffi.so"), "libnervix_client_ffi")
        self.assertEqual(shards.full_stem("a.tar.gz"), "a")
        self.assertEqual(shards.full_stem("nervix-server"), "nervix-server")

    def test_objects_are_the_workspace_executables_outside_incremental_fingerprint_and_build(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            checkout = Checkout(Path(directory) / "checkout")
            objects = shards.coverage_objects(checkout.root, shards.object_pattern(TARGETS))
        debug = shards.COVERAGE_DEBUG
        self.assertEqual(
            sorted(objects),
            sorted([
                debug / "nervix-cli",
                debug / "nervix-server",
                debug / "deps" / "libnervix_client_ffi.so",
                debug / "deps" / "nervix_server-0f92e8a94feb2377",
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
        expected = {}
        for file in packed.files:
            expected[file.path] = (build.root / file.path).read_bytes() if not (build.root / file.path).is_symlink() else None
        root = self.shard_checkout(build)
        restored = self.unpack(root)
        self.assertEqual(restored, packed)
        self.assertEqual(restored.executable, "target/llvm-cov-target/debug/deps/scenarios-4d0e03c611a711f3")
        for path, content in expected.items():
            if content is None:
                self.assertTrue((root / path).is_symlink(), path)
            else:
                self.assertEqual((root / path).read_bytes(), content, path)
        debug = root / shards.COVERAGE_DEBUG
        self.assertEqual(
            (debug / "nervix-server").stat().st_ino,
            (debug / "deps" / "nervix_server-0f92e8a94feb2377").stat().st_ino,
            "a hard link stays one file",
        )
        self.assertTrue(os.access(root / restored.executable, os.X_OK))
        packed_paths = {file.path for file in packed.files}
        driver = "target/llvm-cov-target/debug/build/playwright-rs-5e6f/out/playwright-driver/playwright-1.62.1-linux"
        self.assertIn(f"{driver}/node", packed_paths)
        self.assertIn(f"{driver}/package/cli.js", packed_paths)
        self.assertIn(ONNX_LINK.as_posix(), packed_paths)
        self.assertIn(ONNX_LIBRARY.as_posix(), packed_paths)
        self.assertNotIn("target/llvm-cov-target/debug/deps/libserde_derive-1a2b3c.so", packed_paths)
        self.assertFalse((debug / "build" / "nervix-server-1" / "build-script-build").exists())

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
        write(debug / "deps" / "scenarios-4d0e03c611a711f3", b"\x7fELF scenarios", executable=True)
        manifest = checkout.pack(self.archive)
        self.assertEqual([file.path for file in manifest.files if "playwright" in file.path], [])

    def test_the_executable_command_prints_the_restored_scenario_executable(self) -> None:
        build, packed = self.build()
        output = io.StringIO()
        with mock.patch.object(shards, "ROOT", build.root), redirect_stdout(output):
            status = shards.main(["executable"])
        self.assertEqual(status, 0)
        self.assertEqual(output.getvalue().strip(), str(build.root / packed.executable))

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


if __name__ == "__main__":
    unittest.main()
