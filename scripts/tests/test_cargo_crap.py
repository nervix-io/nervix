"""Installation, version refusal and invocation of the pinned complexity analyzer."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from scripts import cargo_crap

ROOT = Path(cargo_crap.__file__).resolve().parents[1]

# The ratchet's unit tests run the pinned analyzer: by module name, or as part of discovering
# every script test.
RATCHET_TESTS = ("scripts.tests.test_ratchet", "discover -s scripts/tests")


def write_analyzer(path: Path, version: str) -> Path:
    """An executable that reports `version` and echoes the arguments it was run with."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "#!/usr/bin/env python3\nimport sys\n"
        f"if sys.argv[1:] == ['--version']:\n    print('cargo-crap {version}')\n    sys.exit(0)\n"
        "print(' '.join(sys.argv[1:]))\nsys.exit(3)\n",
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


class InstallTests(unittest.TestCase):
    def test_the_install_builds_the_pinned_version_locked_into_a_root_of_its_own(self) -> None:
        target = Path("/build/target")
        self.assertEqual(
            cargo_crap.install_command(target),
            ["cargo", "install", "--locked", "--force", "--version", cargo_crap.VERSION,
             "--root", f"/build/target/tools/cargo-crap-{cargo_crap.VERSION}", "cargo-crap"],
        )
        self.assertEqual(
            cargo_crap.executable(target),
            Path(f"/build/target/tools/cargo-crap-{cargo_crap.VERSION}/bin/cargo-crap"),
        )

    def test_an_installed_pinned_version_is_not_built_again(self) -> None:
        with TemporaryDirectory() as directory:
            target = Path(directory)
            write_analyzer(cargo_crap.executable(target), cargo_crap.VERSION)
            with mock.patch("scripts.cargo_crap.install_command", return_value=["false"]):
                cargo_crap.install(target, target)

    def test_a_missing_or_other_version_is_built_and_then_verified(self) -> None:
        with TemporaryDirectory() as directory:
            target = Path(directory)
            write_analyzer(cargo_crap.executable(target), "0.5.0")
            builder = write_analyzer(target / "builder", cargo_crap.VERSION)
            # The stand-in build replaces the stale binary with the pinned one.
            command = copy_command(builder, cargo_crap.executable(target))
            with mock.patch("scripts.cargo_crap.install_command", return_value=command):
                cargo_crap.install(target, target)
            self.assertEqual(cargo_crap.installed_version(cargo_crap.executable(target)), cargo_crap.VERSION)

    def test_a_failed_build_is_reported(self) -> None:
        with TemporaryDirectory() as directory:
            target = Path(directory)
            with mock.patch("scripts.cargo_crap.install_command", return_value=["false"]):
                with self.assertRaisesRegex(cargo_crap.AnalyzerError, "failed with exit 1"):
                    cargo_crap.install(target, target)

    def test_a_build_that_leaves_another_version_is_refused(self) -> None:
        with TemporaryDirectory() as directory:
            target = Path(directory)
            builder = write_analyzer(target / "builder", "0.5.0")
            command = copy_command(builder, cargo_crap.executable(target))
            cargo_crap.executable(target).parent.mkdir(parents=True)
            with mock.patch("scripts.cargo_crap.install_command", return_value=command):
                with self.assertRaisesRegex(cargo_crap.AnalyzerError, "pins 0.6.1; run `just install-cargo-crap`"):
                    cargo_crap.install(target, target)

    def test_a_binary_that_reports_no_version_is_refused(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "cargo-crap"
            path.write_text("#!/bin/sh\necho something else\n", encoding="utf-8")
            path.chmod(0o755)
            with self.assertRaisesRegex(cargo_crap.AnalyzerError, "did not report a cargo-crap version"):
                cargo_crap.installed_version(path)

    def test_a_binary_that_cannot_be_run_is_refused(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "cargo-crap"
            path.write_text("not a program\n", encoding="utf-8")
            path.chmod(0o644)
            with self.assertRaisesRegex(cargo_crap.AnalyzerError, "could not be run"):
                cargo_crap.installed_version(path)

    def test_a_missing_cargo_is_reported(self) -> None:
        with TemporaryDirectory() as directory:
            target = Path(directory)
            with mock.patch("scripts.cargo_crap.install_command", return_value=[str(target / "no-cargo")]):
                with self.assertRaisesRegex(cargo_crap.AnalyzerError, "cargo could not be run"):
                    cargo_crap.install(target, target)


class TargetTests(unittest.TestCase):
    def test_the_target_directory_follows_cargo(self) -> None:
        with mock.patch.dict(os.environ, {"CARGO_TARGET_DIR": "/elsewhere/target"}):
            self.assertEqual(cargo_crap.default_target(), Path("/elsewhere/target"))
        environment = {name: value for name, value in os.environ.items() if name != "CARGO_TARGET_DIR"}
        with mock.patch.dict(os.environ, environment, clear=True):
            self.assertEqual(cargo_crap.default_target(), Path(cargo_crap.__file__).resolve().parents[1] / "target")


class ConfigurationTests(unittest.TestCase):
    def test_a_configuration_above_the_working_directory_is_in_reach(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "nested/deeper").mkdir(parents=True)
            self.assertIsNone(cargo_crap.configuration_in_reach(root / "nested/deeper"))
            (root / ".cargo-crap.toml").write_text("try-weight = 0.5\n", encoding="utf-8")
            self.assertEqual(cargo_crap.configuration_in_reach(root / "nested/deeper"), root / ".cargo-crap.toml")


class RunTests(unittest.TestCase):
    def command(self, target: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
        environment = dict(os.environ, CARGO_TARGET_DIR=str(target))
        return subprocess.run(
            [sys.executable, "-m", "scripts.cargo_crap", *arguments],
            cwd=Path(cargo_crap.__file__).resolve().parents[1],
            env=environment,
            capture_output=True,
            text=True,
        )

    def test_run_executes_the_pinned_analyzer_with_the_given_arguments(self) -> None:
        with TemporaryDirectory() as directory:
            target = Path(directory)
            write_analyzer(cargo_crap.executable(target), cargo_crap.VERSION)
            completed = self.command(target, "run", "--", "--lcov", "report.info", "--min", "30")
        # The analyzer's own exit status and output pass through unchanged.
        self.assertEqual((completed.returncode, completed.stdout), (3, "--lcov report.info --min 30\n"))

    def test_run_refuses_another_version(self) -> None:
        with TemporaryDirectory() as directory:
            target = Path(directory)
            write_analyzer(cargo_crap.executable(target), "0.5.0")
            completed = self.command(target, "run", "--", "--lcov", "report.info")
        self.assertEqual(completed.returncode, 1)
        self.assertIn("is cargo-crap 0.5.0, but this repository pins", completed.stderr)


class MainTests(unittest.TestCase):
    """The command line in process: which binary it replaces itself with and what it refuses."""

    def test_install_builds_into_the_target_of_this_checkout(self) -> None:
        with TemporaryDirectory() as directory:
            target = Path(directory)
            with mock.patch("scripts.cargo_crap.default_target", return_value=target), \
                    mock.patch("scripts.cargo_crap.install") as install:
                self.assertEqual(cargo_crap.main(["install"]), 0)
        install.assert_called_once_with(Path(cargo_crap.__file__).resolve().parents[1], target)

    def test_a_failed_install_exits_with_its_cause(self) -> None:
        refusal = cargo_crap.AnalyzerError("installing cargo-crap 0.6.1 failed with exit 101")
        with mock.patch("scripts.cargo_crap.install", side_effect=refusal), \
                mock.patch("sys.stderr") as stderr:
            self.assertEqual(cargo_crap.main(["install"]), 1)
        self.assertIn("failed with exit 101", "".join(call.args[0] for call in stderr.write.call_args_list))

    def test_run_replaces_itself_with_the_verified_binary(self) -> None:
        with TemporaryDirectory() as directory:
            target = Path(directory)
            path = write_analyzer(cargo_crap.executable(target), cargo_crap.VERSION)
            with mock.patch("scripts.cargo_crap.default_target", return_value=target), \
                    mock.patch("scripts.cargo_crap.os.execv") as execv:
                cargo_crap.main(["run", "--", "--lcov", "report.info"])
        execv.assert_called_once_with(path, [str(path), "--lcov", "report.info"])


def dependencies(recipes: dict[str, dict], name: str) -> set[str]:
    """Every recipe `name` depends on, directly or through another dependency."""

    found: set[str] = set()
    pending = [name]
    while pending:
        current = pending.pop()
        for dependency in recipes[current]["dependencies"]:
            recipe = dependency["recipe"]
            if recipe not in found:
                found.add(recipe)
                pending.append(recipe)
    return found


class RecipeTests(unittest.TestCase):
    """Every recipe that runs the ratchet's unit tests installs the analyzer they run first."""

    def test_every_recipe_running_the_ratchet_tests_installs_the_pinned_analyzer(self) -> None:
        dumped = subprocess.run(
            ["just", "--dump", "--dump-format", "json"], cwd=ROOT, capture_output=True, text=True, check=True,
        )
        recipes = json.loads(dumped.stdout)["recipes"]
        runners = []
        for name, recipe in sorted(recipes.items()):
            body = json.dumps(recipe["body"])
            if any(marker in body for marker in RATCHET_TESTS):
                runners.append(name)
        # Both forms are found, so an empty selection cannot pass.
        self.assertLessEqual({"test-ratchet-units", "test-docs"}, set(runners))
        uninstalled = [name for name in runners if "install-cargo-crap" not in dependencies(recipes, name)]
        self.assertEqual(uninstalled, [])


if __name__ == "__main__":
    unittest.main()
