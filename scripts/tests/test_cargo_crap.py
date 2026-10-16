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


def two_phase_analyzer(directory: Path, alone: list[dict[str, object]], against: object, status: int = 0,
                       skipped: str = "") -> Path:
    """An analyzer that answers an analysis without coverage with `alone` and `skipped` on standard
    error, and one against a report (`--lcov`) with `against` and `status`."""

    path = directory / "cargo-crap"
    alone_report = json.dumps({"version": cargo_crap.VERSION, "entries": alone})
    against_report = against if isinstance(against, str) else json.dumps({"version": cargo_crap.VERSION, "entries": against})
    path.write_text(
        "#!/usr/bin/env python3\nimport sys\n"
        f"if sys.argv[1:] == ['--version']:\n    print('cargo-crap {cargo_crap.VERSION}')\n    sys.exit(0)\n"
        f"if '--lcov' in sys.argv:\n    sys.stdout.write({against_report!r})\n    sys.stderr.write('warning: scope\\n')\n    sys.exit({status})\n"
        f"sys.stdout.write({alone_report!r})\nsys.stderr.write({skipped!r})\n",
        encoding="utf-8",
    )
    path.chmod(0o755)
    return path


def entry(file: str, coverage: float | None = None) -> dict[str, object]:
    crap = None if coverage is None else 1.0
    return {"file": file, "function": "measured", "line": 1, "cyclomatic": 1.0, "coverage": coverage, "crap": crap}


class ScoreTests(unittest.TestCase):
    """The analysis against a report: identity binding, and every way it cannot be trusted."""

    def repository(self, directory: str, sources: dict[str, str], report: str) -> Path:
        root = Path(directory)
        for path, body in sources.items():
            (root / path).parent.mkdir(parents=True, exist_ok=True)
            (root / path).write_text(body, encoding="utf-8")
        (root / "lcov.info").write_text(report, encoding="utf-8")
        return root

    def test_each_record_binds_to_its_own_source_alone(self) -> None:
        body = "fn covered() -> u32 {\n    1\n}\n"
        with TemporaryDirectory() as directory:
            root = self.repository(
                directory,
                {"src/lib.rs": body, "crates/other/src/lib.rs": body.replace("covered", "unrecorded")},
                "SF:src/lib.rs\nDA:1,1\nDA:2,1\nDA:3,1\nend_of_record\n",
            )
            scores = cargo_crap.score(root, cargo_crap.default_target(), (), root / "lcov.info")

        # By suffix, `src/lib.rs` would also have covered the other crate's `src/lib.rs`.
        self.assertEqual(scores.scored, (cargo_crap.Scored(cargo_crap.Function("src/lib.rs", "covered", 1, 1.0), 100.0, 1.0),))
        self.assertEqual(scores.unmeasured, (cargo_crap.Function("crates/other/src/lib.rs", "unrecorded", 1, 1.0),))

    def score_with(self, analyzer: Path, report: str = "SF:src/lib.rs\nDA:1,1\nend_of_record\n") -> str:
        with TemporaryDirectory() as directory:
            root = self.repository(directory, {"src/lib.rs": "fn measured() {}\n"}, report)
            with mock.patch("scripts.cargo_crap.executable", return_value=analyzer):
                with self.assertRaises(cargo_crap.AnalyzerError) as raised:
                    cargo_crap.score(root, root / "target", (), root / "lcov.info")
        return str(raised.exception)

    def test_an_absolute_report_source_is_refused(self) -> None:
        with TemporaryDirectory() as directory:
            analyzer = two_phase_analyzer(Path(directory), [entry("./src/lib.rs")], [entry("./src/lib.rs", 100.0)])
            message = self.score_with(analyzer, "SF:/elsewhere/src/lib.rs\nDA:1,1\nend_of_record\n")
        self.assertIn("names /elsewhere/src/lib.rs, not a source relative to", message)

    def test_a_coverage_analysis_that_measured_other_functions_is_refused(self) -> None:
        with TemporaryDirectory() as directory:
            analyzer = two_phase_analyzer(Path(directory), [entry("./src/lib.rs")], [entry("./src/other.rs", 100.0)])
            message = self.score_with(analyzer)
        self.assertIn("measured other functions against", message)

    def test_a_failed_or_unreadable_coverage_analysis_is_refused(self) -> None:
        with TemporaryDirectory() as directory:
            analyzer = two_phase_analyzer(Path(directory), [entry("./src/lib.rs")], [], status=2)
            message = self.score_with(analyzer)
        self.assertIn("failed with exit 2 against", message)
        with TemporaryDirectory() as directory:
            analyzer = two_phase_analyzer(Path(directory), [entry("./src/lib.rs")], "Function  CRAP\n")
            message = self.score_with(analyzer)
        self.assertIn("wrote no JSON report", message)

    def test_a_source_the_walk_skipped_is_refused_although_the_coverage_analysis_would_pass(self) -> None:
        skipped = "warning: could not analyze ./src/broken.rs: parsing ./src/broken.rs\n"
        with TemporaryDirectory() as directory:
            analyzer = two_phase_analyzer(
                Path(directory), [entry("./src/lib.rs")], [entry("./src/lib.rs", 100.0)], skipped=skipped,
            )
            message = self.score_with(analyzer)
        self.assertIn("skipped sources it could not analyze", message)
        self.assertIn("./src/broken.rs", message)


class ReachabilityTests(unittest.TestCase):
    def test_hidden_linked_and_ignored_sources_are_out_of_the_walks_reach(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            subprocess.run(["git", "init", "-q", "-b", "main"], cwd=root, check=True)
            (root / ".gitignore").write_text("/src/forced.rs\n", encoding="utf-8")
            for path in ("src/lib.rs", "src/.hidden/inner.rs", "src/forced.rs"):
                (root / path).parent.mkdir(parents=True, exist_ok=True)
                (root / path).write_text("fn f() {}\n", encoding="utf-8")
            (root / "src/linked.rs").symlink_to("lib.rs")
            subprocess.run(["git", "add", "--force", "."], cwd=root, check=True)

            unreachable = cargo_crap.unreachable_sources(
                root, ["src/lib.rs", "src/.hidden/inner.rs", "src/forced.rs", "src/linked.rs"],
            )

        self.assertEqual(unreachable, ["src/.hidden/inner.rs", "src/forced.rs", "src/linked.rs"])


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


if __name__ == "__main__":
    unittest.main()
