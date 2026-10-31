"""Selection, installation and verification of the wasm-bindgen CLI the console build runs."""

from __future__ import annotations

import os
import re
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from scripts import wasm_bindgen

ROOT = Path(wasm_bindgen.__file__).resolve().parents[1]


def write_lock(root: Path, *versions: str) -> None:
    packages = "".join(
        f'[[package]]\nname = "wasm-bindgen"\nversion = "{version}"\n\n' for version in versions
    )
    (root / "Cargo.lock").write_text(
        f'version = 4\n\n[[package]]\nname = "wasm-bindgen-shared"\nversion = "9.9.9"\n\n{packages}',
        encoding="utf-8",
    )


def write_cli(path: Path, version: str) -> Path:
    """An executable that answers `--version` the way the wasm-bindgen CLI does."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "#!/usr/bin/env python3\nimport sys\n"
        f"if sys.argv[1:] == ['--version']:\n    print('wasm-bindgen {version}')\n    sys.exit(0)\n"
        "sys.exit(3)\n",
        encoding="utf-8",
    )
    path.chmod(0o755)
    return path


def copy_command(source: Path, destination: Path) -> list[str]:
    """A stand-in build that puts `source` in place of the installed binary."""

    # Plain reads and writes: GNU cp probes for holes, which can wait on a busy ZFS pool.
    program = ("import os, sys; data = open(sys.argv[1], 'rb').read(); "
               "open(sys.argv[2], 'wb').write(data); os.chmod(sys.argv[2], 0o755)")
    return [sys.executable, "-c", program, str(source), str(destination)]


class SelectionTests(unittest.TestCase):
    def test_the_version_cargo_lock_locks_selects_the_cli(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_lock(root, "0.2.129")
            self.assertEqual(wasm_bindgen.locked_version(root), "0.2.129")

    def test_a_lock_without_one_wasm_bindgen_version_is_refused(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_lock(root)
            with self.assertRaisesRegex(wasm_bindgen.ToolError, "locks no wasm-bindgen version"):
                wasm_bindgen.locked_version(root)
            write_lock(root, "0.2.128", "0.2.129")
            with self.assertRaisesRegex(wasm_bindgen.ToolError, "0.2.128, 0.2.129"):
                wasm_bindgen.locked_version(root)

    def test_the_repository_locks_the_version_trunk_requires(self) -> None:
        # trunk requires the wasm-bindgen version the console's lock file selects.
        lock = (ROOT / "Cargo.lock").read_text(encoding="utf-8")
        locked = re.findall(r'name = "wasm-bindgen"\nversion = "([^"]+)"', lock)
        self.assertEqual([wasm_bindgen.locked_version(ROOT)], locked)

    def test_the_install_builds_the_locked_version_into_a_root_of_its_own(self) -> None:
        target = Path("/build/target")
        self.assertEqual(
            wasm_bindgen.install_command(target, "0.2.129"),
            ["cargo", "install", "--locked", "--force", "--version", "0.2.129",
             "--root", "/build/target/tools/wasm-bindgen-0.2.129", "wasm-bindgen-cli"],
        )
        self.assertEqual(
            wasm_bindgen.executable(target, "0.2.129"),
            Path("/build/target/tools/wasm-bindgen-0.2.129/bin/wasm-bindgen"),
        )

    def test_the_target_directory_follows_cargo(self) -> None:
        with mock.patch.dict(os.environ, {"CARGO_TARGET_DIR": "/elsewhere/target"}):
            self.assertEqual(wasm_bindgen.default_target(), Path("/elsewhere/target"))
        environment = {name: value for name, value in os.environ.items() if name != "CARGO_TARGET_DIR"}
        with mock.patch.dict(os.environ, environment, clear=True):
            self.assertEqual(wasm_bindgen.default_target(), ROOT / "target")


class InstallTests(unittest.TestCase):
    def test_an_installed_locked_version_is_not_built_again(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_lock(root, "0.2.129")
            write_cli(wasm_bindgen.executable(root, "0.2.129"), "0.2.129")
            with mock.patch("scripts.wasm_bindgen.install_command", return_value=["false"]):
                self.assertEqual(wasm_bindgen.install(root, root), root / "tools/wasm-bindgen-0.2.129/bin")

    def test_a_missing_or_other_version_is_built_and_then_verified(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_lock(root, "0.2.129")
            path = write_cli(wasm_bindgen.executable(root, "0.2.129"), "0.2.128")
            builder = write_cli(root / "builder", "0.2.129")
            with mock.patch("scripts.wasm_bindgen.install_command", return_value=copy_command(builder, path)):
                wasm_bindgen.install(root, root)
            self.assertEqual(wasm_bindgen.installed_version(path), "0.2.129")

    def test_a_failed_build_is_reported(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_lock(root, "0.2.129")
            with mock.patch("scripts.wasm_bindgen.install_command", return_value=["false"]):
                with self.assertRaisesRegex(wasm_bindgen.ToolError, "wasm-bindgen-cli 0.2.129 failed with exit 1"):
                    wasm_bindgen.install(root, root)

    def test_a_build_that_leaves_another_version_is_refused(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_lock(root, "0.2.129")
            path = wasm_bindgen.executable(root, "0.2.129")
            builder = write_cli(root / "builder", "0.2.128")
            path.parent.mkdir(parents=True)
            with mock.patch("scripts.wasm_bindgen.install_command", return_value=copy_command(builder, path)):
                with self.assertRaisesRegex(wasm_bindgen.ToolError, "reports 0.2.128, but Cargo.lock locks 0.2.129"):
                    wasm_bindgen.install(root, root)

    def test_a_missing_cargo_is_reported(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_lock(root, "0.2.129")
            with mock.patch("scripts.wasm_bindgen.install_command", return_value=[str(root / "no-cargo")]):
                with self.assertRaisesRegex(wasm_bindgen.ToolError, "cargo could not be run"):
                    wasm_bindgen.install(root, root)

    def test_a_binary_that_reports_no_version_is_refused(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "wasm-bindgen"
            path.write_text("#!/bin/sh\necho something else\n", encoding="utf-8")
            path.chmod(0o755)
            with self.assertRaisesRegex(wasm_bindgen.ToolError, "did not report a wasm-bindgen version"):
                wasm_bindgen.installed_version(path)

    def test_a_binary_that_cannot_be_run_is_refused(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "wasm-bindgen"
            path.write_text("not a program\n", encoding="utf-8")
            path.chmod(0o644)
            with self.assertRaisesRegex(wasm_bindgen.ToolError, "could not be run"):
                wasm_bindgen.installed_version(path)


class CommandTests(unittest.TestCase):
    def run_module(self, root: Path, target: Path) -> subprocess.CompletedProcess[str]:
        environment = dict(os.environ, CARGO_TARGET_DIR=str(target))
        return subprocess.run(
            [sys.executable, str(root / "scripts/wasm_bindgen.py"), "install"],
            cwd=root, env=environment, capture_output=True, text=True,
        )

    def test_install_prints_only_the_directory_to_put_first_on_path(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "scripts").mkdir()
            (root / "scripts/wasm_bindgen.py").write_bytes(Path(wasm_bindgen.__file__).read_bytes())
            write_lock(root, "0.2.129")
            target = root / "target"
            write_cli(wasm_bindgen.executable(target, "0.2.129"), "0.2.129")
            completed = self.run_module(root, target)
        # A Dockerfile captures stdout as the PATH entry, so nothing else may reach it.
        self.assertEqual((completed.returncode, completed.stdout), (0, f"{target}/tools/wasm-bindgen-0.2.129/bin\n"))

    def test_install_failures_exit_with_their_cause(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "scripts").mkdir()
            (root / "scripts/wasm_bindgen.py").write_bytes(Path(wasm_bindgen.__file__).read_bytes())
            write_lock(root)
            completed = self.run_module(root, root / "target")
        self.assertEqual((completed.returncode, completed.stdout), (1, ""))
        self.assertIn("locks no wasm-bindgen version", completed.stderr)

    def test_cargo_output_never_reaches_standard_output(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_lock(root, "0.2.129")
            path = wasm_bindgen.executable(root, "0.2.129")
            path.parent.mkdir(parents=True)
            noisy = [sys.executable, "-c", "print('Installing wasm-bindgen-cli'); raise SystemExit(5)"]
            with mock.patch("scripts.wasm_bindgen.install_command", return_value=noisy), \
                    mock.patch("scripts.wasm_bindgen.subprocess.run", wraps=subprocess.run) as run:
                with self.assertRaises(wasm_bindgen.ToolError):
                    wasm_bindgen.install(root, root)
            self.assertIs(run.call_args.kwargs["stdout"], sys.stderr)


class MainTests(unittest.TestCase):
    """The command line in process: what it prints and what it exits with."""

    def test_install_prints_the_directory_it_installed_into(self) -> None:
        directory = Path("/build/target/tools/wasm-bindgen-0.2.129/bin")
        with mock.patch("scripts.wasm_bindgen.install", return_value=directory) as install, \
                mock.patch("sys.stdout") as stdout:
            self.assertEqual(wasm_bindgen.main(["install"]), 0)
        install.assert_called_once_with(ROOT, wasm_bindgen.default_target())
        self.assertEqual("".join(call.args[0] for call in stdout.write.call_args_list), f"{directory}\n")

    def test_a_refused_install_exits_with_its_cause_and_prints_nothing(self) -> None:
        refusal = wasm_bindgen.ToolError("installing wasm-bindgen-cli 0.2.129 failed with exit 101")
        with mock.patch("scripts.wasm_bindgen.install", side_effect=refusal), \
                mock.patch("sys.stdout") as stdout, mock.patch("sys.stderr") as stderr:
            self.assertEqual(wasm_bindgen.main(["install"]), 1)
        stdout.write.assert_not_called()
        self.assertIn("failed with exit 101", "".join(call.args[0] for call in stderr.write.call_args_list))


class ImageTests(unittest.TestCase):
    """The image builds the console with the same pinned CLI, offline."""

    def test_every_image_console_build_runs_trunk_offline_with_the_pinned_cli(self) -> None:
        dockerfile = (ROOT / "Dockerfile.debian").read_text(encoding="utf-8")
        self.assertIn("COPY scripts/wasm_bindgen.py ./scripts/wasm_bindgen.py", dockerfile)
        builds = [line for line in dockerfile.splitlines() if "trunk build" in line]
        self.assertTrue(builds)
        for line in builds:
            self.assertIn("--offline", line)
            self.assertIn('PATH="${wasm_bindgen_bin}:${PATH}"', line)
        install = dockerfile.index('wasm_bindgen_bin="$(python3 scripts/wasm_bindgen.py install)"')
        self.assertLess(install, dockerfile.index("trunk build"))


class WorkflowTests(unittest.TestCase):
    """Every pull request whose diff requires the suites runs these tests under coverage."""

    def test_the_extra_tests_job_measures_the_cli_tooling(self) -> None:
        check = (ROOT / ".github/workflows/check.yaml").read_text(encoding="utf-8")
        extras = re.split(r"\n  [a-z][a-z-]*:\n", check.split("\n  extra-tests:\n", 1)[1], maxsplit=1)[0]
        self.assertIn("needs.test-selection.outputs.tests-required == 'true'", extras)
        self.assertIn("run: just coverage-wasm-bindgen\n", extras)
        upload = extras.split("- name: Upload native extra coverage\n", 1)[1].split("- name:", 1)[0]
        self.assertIn("            target/wasm-bindgen-coverage/python.lcov\n", upload)


if __name__ == "__main__":
    unittest.main()
