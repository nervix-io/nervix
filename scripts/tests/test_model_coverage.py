"""Command and artifact regressions for collecting canonical model executions."""

from __future__ import annotations

import json
import io
import copy
import subprocess
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock

from scripts import native_coverage
from scripts.model_evidence import Evidence, EvidenceError, FILENAME, VARIABLE, read_complete
from scripts.tests import test_shuttle_checks as shuttle_tests, test_loom_models as loom_tests
from scripts.tests import test_native_coverage as native_tests
from scripts.tests.test_native_coverage import FakeCommands, Producer

ROOT = Path(__file__).resolve().parents[2]


class ModelCoverageTests(unittest.TestCase):
    def test_native_primitive_collection_tracks_the_canonical_mode_recipes(self) -> None:
        dumped = subprocess.run(
            ["just", "--dump", "--dump-format", "json"], cwd=ROOT,
            capture_output=True, text=True, check=True,
        )
        recipes = json.loads(dumped.stdout)["recipes"]
        selected = native_coverage.select_producers(["test-primitives"], native_coverage.PRODUCERS)
        native = ["test-primitives-ordinary", *native_coverage.dependency_names(recipes["test-primitives-modeled"])]
        self.assertEqual([producer.instrumented for producer in selected], native)
        self.assertEqual([producer.mode for producer in selected], ["ordinary", "shuttle", "loom", "turmoil", "deloxide"])

    def test_shuttle_coverage_delegates_to_the_canonical_collector(self) -> None:
        dumped = subprocess.run(
            ["just", "--dump", "--dump-format", "json"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
        body = json.dumps(json.loads(dumped.stdout)["recipes"]["coverage-shuttle"]["body"])
        self.assertIn("scripts.native_coverage", body)
        self.assertIn("test-shuttle", body)

    def test_mode_builds_and_attempts_are_independent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ordinary = native_coverage.Workspace(root=root, target=root / "target")
            shuttle = native_coverage.Workspace(root=root, target=root / "target", mode="shuttle")
            loom = native_coverage.Workspace(root=root, target=root / "target", mode="loom")
            self.assertEqual(len({ordinary.build(), shuttle.build(), loom.build()}), 3)
            for mode, workspace in (("shuttle", shuttle), ("loom", loom)):
                producer = native_coverage.Producer("check", mode, (), "check", ())
                attempt = workspace.new_attempt(producer, "rust-1.99.0", "run")
                (attempt / "lcov.info").write_text(mode)
                with workspace.build_lock():
                    with ordinary.build_lock():
                        self.assertEqual((attempt / "lcov.info").read_text(), mode)

    def test_ci_retains_mode_artifacts_and_uses_ordinary_reports_for_the_ordinary_gate(self) -> None:
        from scripts.tests.test_native_coverage import job_section

        workflow = (ROOT / ".github/workflows/check.yaml").read_text()
        job = job_section(workflow, "shuttle")
        self.assertIn("cargo-llvm-cov", job)
        self.assertIn("just coverage-native-extras test-shuttle", job)
        self.assertIn("just test-shuttle-replay-check", job)
        self.assertIn("target/native-coverage/**/models.json", job)
        extra = job_section(workflow, "extra-tests")
        self.assertIn("just coverage-native-extras test-loom", extra)
        self.assertIn("just test-primitives-compile", extra)
        qualification = job_section(workflow, "loom-qualification")
        self.assertIn("just test-loom-qualification", qualification)
        coverage = job_section(workflow, "coverage")
        self.assertIn(
            "needs: [tests, scenarios, extra-tests, shuttle, diagnostic-evidence, bolero]",
            coverage,
        )
        self.assertIn("-path '*/ordinary/*/lcov.info'", coverage)


class RunnerEvidenceTests(unittest.TestCase):
    def test_shuttle_records_each_canonical_check_and_its_paired_runs(self) -> None:
        root = shuttle_tests.repository(self)

        def respond(arguments, environment):
            return shuttle_tests.RunTests.listings(arguments) or shuttle_tests.Outcome(0, shuttle_tests.completed_output())

        report = root / FILENAME
        commands = shuttle_tests.ScriptedCommands(root, respond)
        with redirect_stdout(io.StringIO()):
            status = shuttle_tests.run_checks(commands, shuttle_tests.inventory(), root / "target", "", report)
        self.assertEqual(status, 0)
        content = read_complete(report, "shuttle", "")
        self.assertEqual(content["discovered"], 3)
        self.assertEqual(content["selected"], 3)
        self.assertEqual(content["executed"], 3)
        self.assertEqual(content["completed"], 3)
        for check in content["checks"]:
            self.assertEqual([run["name"] for run in check["runs"]], ["exploration", "nondeterminism"])
            self.assertTrue(all(run["records"] for run in check["runs"]))

    def test_shuttle_retains_failed_nondeterminism_evidence(self) -> None:
        root = shuttle_tests.repository(self)

        def respond(arguments, environment):
            listing = shuttle_tests.RunTests.listings(arguments)
            if listing is not None:
                return listing
            if environment.get("SHUTTLE_CHECK_NONDETERMINISM") == "1":
                return shuttle_tests.Outcome(1, "uncontrolled nondeterminism")
            return shuttle_tests.Outcome(0, shuttle_tests.completed_output())

        report = root / FILENAME
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            status = shuttle_tests.run_checks(shuttle_tests.ScriptedCommands(root, respond), shuttle_tests.inventory(), root / "target", "", report)
        self.assertEqual(status, 1)
        content = json.loads(report.read_text())
        self.assertEqual(content["verdict"], "failed")
        self.assertEqual(content["completed"], 0)
        self.assertEqual(content["checks"][0]["runs"][1]["exit_status"], 1)
        with self.assertRaisesRegex(EvidenceError, "did not complete"):
            read_complete(report, "shuttle", "")

    def test_loom_retains_invariants_executions_and_bounds_with_a_cross_package_filter(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            def respond(arguments):
                if "--ignored" in arguments:
                    return loom_tests.listing()
                if "--list" in arguments:
                    return loom_tests.listing(loom_tests.PUBLICATION_TEST, loom_tests.DISARM_TEST)
                return loom_tests.Outcome(0, loom_tests.completed("execution.cancellation.disarm", 1))

            report = root / FILENAME
            with redirect_stdout(io.StringIO()):
                status = loom_tests.run_models(loom_tests.ScriptedCommands(root, respond), loom_tests.inventory(), root / "target", "disarm", report)
            self.assertEqual(status, 0)
            content = read_complete(report, "loom", "disarm")
            self.assertEqual(content["discovered"], 2)
            self.assertEqual(content["completed"], 1)
            [check] = content["checks"]
            self.assertEqual(check["invariant"], "execution.cancellation.disarm")
            self.assertEqual(check["runs"][0]["executions"], 1)
            self.assertTrue(check["runs"][0]["bounds"])

    def test_zero_executions_are_incomplete_loom_exploration(self) -> None:
        self.assertIsNone(loom_tests.completion(loom_tests.completed("execution.cancellation.disarm", 0), "execution.cancellation.disarm"))

    def test_an_empty_canonical_selection_keeps_running_evidence_and_fails(self) -> None:
        root = shuttle_tests.repository(self)
        report = root / FILENAME

        def respond(arguments, environment):
            return shuttle_tests.RunTests.listings(arguments)

        with self.assertRaisesRegex(shuttle_tests.RunnerError, "no Shuttle check"):
            shuttle_tests.run_checks(shuttle_tests.ScriptedCommands(root, respond), shuttle_tests.inventory(), root / "target", "no-match", report)
        self.assertEqual(json.loads(report.read_text())["verdict"], "running")


class EvidenceReaderTests(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / FILENAME
        evidence = Evidence(self.path, "shuttle", "", Path("inventory.toml"))
        check = {"package": "nervix-execution", "test": "owner::shuttle_check"}
        evidence.discover([{**check, "ignored": False}])
        evidence.select([check])
        selected = evidence.begin(check)
        selected["runs"] = [
            {"name": name, "exit_status": 0, "completed": True, "records": ["completion"]}
            for name in ("exploration", "nondeterminism")
        ]
        selected["completed"] = True
        evidence.finish(0)
        self.complete = json.loads(self.path.read_text())

    def test_complete_evidence_is_returned_without_losing_identity(self) -> None:
        self.assertEqual(read_complete(self.path, "shuttle", ""), self.complete)

    def test_incomplete_or_inconsistent_evidence_is_rejected(self) -> None:
        cases = [
            {"mode": "loom"}, {"filter": "other"}, {"verdict": "running"},
            {"discovery": None}, {"selection": []}, {"selection": [False]},
            {"executed": 0}, {"completed": True}, {"checks": []},
            {"selection": self.complete["selection"] * 2}, {"discovery": []},
            {"selection": [{"test": "check"}]},
        ]
        for updates in cases:
            with self.subTest(updates=updates):
                self.path.write_text(json.dumps(self.complete | updates))
                with self.assertRaises(EvidenceError):
                    read_complete(self.path, "shuttle", "")

    def test_paired_runs_require_successful_harness_evidence(self) -> None:
        for update in ({"completed": False}, {"runs": None}, {"runs": [False]}, {"runs": []}):
            with self.subTest(update=update):
                content = copy.deepcopy(self.complete)
                content["checks"][0].update(update)
                self.path.write_text(json.dumps(content))
                with self.assertRaises(EvidenceError):
                    read_complete(self.path, "shuttle", "")
        for update in ({"exit_status": 1}, {"exit_status": False}, {"completed": False}, {"records": []}, {"records": "record"}, {"records": [False]}):
            with self.subTest(update=update):
                content = copy.deepcopy(self.complete)
                content["checks"][0]["runs"][1].update(update)
                self.path.write_text(json.dumps(content))
                with self.assertRaises(EvidenceError):
                    read_complete(self.path, "shuttle", "")

    def test_discovery_keeps_ignored_and_selected_identities_distinct(self) -> None:
        for update in ({"ignored": None}, {"ignored": True}, {"package": []}, {"test": ""}, {"invariant": []}):
            content = copy.deepcopy(self.complete)
            content["discovery"][0].update(update)
            self.path.write_text(json.dumps(content))
            with self.subTest(update=update), self.assertRaises(EvidenceError):
                read_complete(self.path, "shuttle", "")

    def test_loom_requires_typed_execution_counts_and_exploration_bounds(self) -> None:
        content = copy.deepcopy(self.complete)
        content["mode"] = "loom"
        for section in ("discovery", "selection", "checks"):
            content[section][0]["invariant"] = "execution.cancellation.publication"
        content["checks"][0]["runs"] = [
            {"name": "exploration", "exit_status": 0, "completed": True, "executions": 6, "bounds": "preemption bound: none"}
        ]
        self.path.write_text(json.dumps(content))
        self.assertEqual(read_complete(self.path, "loom", ""), content)
        for update in ({"executions": 0}, {"executions": True}, {"bounds": None}, {"bounds": ""}):
            changed = copy.deepcopy(content)
            changed["checks"][0]["runs"][0].update(update)
            self.path.write_text(json.dumps(changed))
            with self.subTest(update=update), self.assertRaises(EvidenceError):
                read_complete(self.path, "loom", "")

    def test_missing_or_unreadable_evidence_is_rejected(self) -> None:
        for text in ("not json", "null"):
            self.path.write_text(text)
            with self.assertRaises(EvidenceError):
                read_complete(self.path, "shuttle", "")
        self.path.unlink()
        with self.assertRaises(EvidenceError):
            read_complete(self.path, "shuttle", "")


class CollectorEvidenceTests(unittest.TestCase):
    def test_successful_filtered_collection_retains_the_canonical_evidence(self) -> None:
        harness = native_tests.CollectTests()
        harness.setUp()
        self.addCleanup(harness.doCleanups)
        producer = Producer("test-shuttle", "shuttle", (), "checks", (), filterable=True)

        def complete():
            path = Path(commands.streamed[-1].environment[VARIABLE])
            evidence = Evidence(path, "shuttle", "cancellation", Path("inventory.toml"))
            identity = {"package": "nervix-execution", "test": "owner::shuttle_cancellation"}
            evidence.discover([{**identity, "ignored": False}])
            evidence.select([identity])
            check = evidence.begin(identity)
            check["completed"] = True
            check["runs"] = [
                {"name": name, "exit_status": 0, "completed": True, "records": ["completion"]}
                for name in ("exploration", "nondeterminism")
            ]
            evidence.finish(0)
            return 0

        commands = FakeCommands(harness.root, {"cancellation": complete})
        with mock.patch.object(native_coverage, "export", return_value=harness.exported()) as export:
            result = native_coverage.collect(commands, harness.context, producer, "cancellation")
        self.assertEqual(result.status, 0)
        content = result.record.content
        self.assertEqual(content["models"], read_complete(result.attempt / FILENAME, "shuttle", "cancellation"))
        self.assertEqual(content["rerun"], "just coverage-native-extras test-shuttle --filter cancellation")
        self.assertEqual(commands.streamed[-1].arguments, ["just", "checks", "cancellation"])
        self.assertEqual(export.call_args.args[1].mode, "shuttle")

    def test_model_collection_requires_canonical_completion_before_export(self) -> None:
        harness = native_tests.CollectTests()
        harness.setUp()
        self.addCleanup(harness.doCleanups)
        producer = Producer("test-shuttle", "shuttle", (), "checks", (), filterable=True)
        commands = FakeCommands(harness.root)
        with mock.patch.object(native_coverage, "export") as export:
            result = native_coverage.collect(commands, harness.context, producer)
        self.assertNotEqual(result.status, 0)
        self.assertEqual(result.record.content["failure"]["stage"], "run")
        export.assert_not_called()
        environment = commands.streamed[-1].environment
        self.assertEqual(Path(environment[VARIABLE]), result.attempt / FILENAME)
        self.assertEqual(Path(environment["CARGO_TARGET_DIR"]), harness.workspace.target / "native-coverage-build-shuttle")


class ModeFixtureTests(unittest.TestCase):
    """The real toolchain must export cached mode builds without changing other attempts."""

    setUpClass = classmethod(native_tests.FixtureTests.setUpClass.__func__)
    collect = native_tests.FixtureTests.collect
    line_counts = native_tests.FixtureTests.line_counts

    def test_mode_builds_export_their_own_current_source_lines(self) -> None:
        import dataclasses
        from scripts.tests.test_native_coverage import FIXTURE_PRODUCERS

        reports = []
        for mode in ("shuttle", "loom", "shuttle"):
            producers = tuple(dataclasses.replace(producer, mode=mode) for producer in FIXTURE_PRODUCERS)
            with mock.patch("scripts.tests.test_native_coverage.FIXTURE_PRODUCERS", producers):
                collected = self.collect("walk")
            self.assertEqual(collected.status, 0, collected.record.get("failure"))
            self.assertEqual(collected.record["mode"], mode)
            self.assertIn(f"native-coverage-build-{mode}", collected.record["instrumentation"]["target_directory"])
            reports.append((collected.attempt, (collected.attempt / "lcov.info").read_bytes()))
            self.assertEqual(self.line_counts(collected.attempt / "lcov.info", "src/lib.rs")[3], 1)
        self.assertEqual(len({attempt for attempt, _ in reports}), 3)
        for attempt, report in reports:
            self.assertEqual((attempt / "lcov.info").read_bytes(), report)


if __name__ == "__main__":
    unittest.main()
