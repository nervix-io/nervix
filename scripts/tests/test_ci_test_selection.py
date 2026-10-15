"""Exercise source selection, GitHub event snapshots and validation-only reruns."""

from __future__ import annotations

import contextlib
import dataclasses
import io
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest
from unittest import mock

from scripts import ci_test_selection as selection
from scripts.tests import test_bolero_coverage


ROOT = Path(__file__).resolve().parents[2]


class MergeFixture:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.git("init", "--initial-branch=main")
        self.write("README.md", "Nervix\n")
        self.write("src/runtime.rs", "fn run() {}\n")
        self.git("add", ".")
        self.git("commit", "-m", "base")
        self.base = self.git("rev-parse", "HEAD")
        self.git("switch", "-c", "change")

    def git(self, *args: str) -> str:
        return subprocess.check_output([
            "git", "-c", "user.name=CI selection test", "-c", "user.email=ci@example.com",
            *args,
        ], cwd=self.root, text=True, stderr=subprocess.PIPE).strip()

    def write(self, path: str, content: str) -> None:
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)

    def merge(self, labels: tuple[str, ...] = (), action: str = "synchronize") -> tuple[dict, str]:
        self.git("add", "-A")
        self.git("commit", "--allow-empty", "-m", "change")
        head = self.git("rev-parse", "HEAD")
        self.git("switch", "main")
        self.git("merge", "--no-ff", "change", "-m", "tested merge")
        payload = {
            "action": action,
            "pull_request": {
                "base": {"sha": self.base}, "head": {"sha": head},
                "labels": [{"name": label} for label in labels],
            },
        }
        return payload, self.git("rev-parse", "HEAD")


class SourceSelectionTests(unittest.TestCase):
    def test_verified_prose_locations_exclude_ordinary_suites(self) -> None:
        for path in ("AGENTS.md", "README.md", "docs/src/property-testing-and-fuzzing.md",
                     "docs/review notes/évidence\n.md", ".agents/skills/plexus/SKILL.md"):
            with self.subTest(path=path), tempfile.TemporaryDirectory() as directory:
                fixture = MergeFixture(Path(directory))
                fixture.write(path, "Updated prose\n")
                payload, tested = fixture.merge()
                actual = selection.TestSelection.read("pull_request", payload, tested, fixture.root)
                self.assertTrue(actual.documentation_only)
                self.assertFalse(actual.bolero_required)
                self.assertEqual(actual.paths, (path,))

    def test_code_tests_and_unknown_configuration_require_suites_with_no_docker(self) -> None:
        for path in ("src/runtime.rs", "tests/features/runtime/output.feature", "scripts/ci_test_selection.py",
                     ".github/workflows/check.yaml", "Cargo.toml", "docs/theme/script.js",
                     "docs/book.toml", "tests/corpus/case.md"):
            with self.subTest(path=path), tempfile.TemporaryDirectory() as directory:
                fixture = MergeFixture(Path(directory))
                fixture.write(path, "changed\n")
                fixture.write("README.md", "also changed\n")
                payload, tested = fixture.merge(("no-docker",))
                actual = selection.TestSelection.read("pull_request", payload, tested, fixture.root)
                self.assertFalse(actual.documentation_only)
                self.assertTrue(actual.bolero_required)
                self.assertIn(path, actual.paths)

    def test_rename_or_delete_of_product_code_cannot_be_hidden_in_documentation(self) -> None:
        for rename in (False, True):
            with self.subTest(rename=rename), tempfile.TemporaryDirectory() as directory:
                fixture = MergeFixture(Path(directory))
                if rename:
                    (fixture.root / "docs").mkdir()
                    fixture.git("mv", "src/runtime.rs", "docs/runtime.md")
                else:
                    fixture.git("rm", "src/runtime.rs")
                payload, tested = fixture.merge()
                actual = selection.TestSelection.read("pull_request", payload, tested, fixture.root)
                self.assertIn("src/runtime.rs", actual.paths)
                self.assertFalse(actual.documentation_only)
                self.assertTrue(actual.bolero_required)

    def test_empty_diff_is_not_proof_of_documentation_only_work(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = MergeFixture(Path(directory))
            payload, tested = fixture.merge()
            actual = selection.TestSelection.read("pull_request", payload, tested, fixture.root)
            self.assertEqual(actual.paths, ())
            self.assertFalse(actual.documentation_only)
            self.assertTrue(actual.bolero_required)

    def test_label_events_preserve_explicit_fuzz_requests(self) -> None:
        for action, labels, required in (
            ("opened", (), False), ("labeled", ("no-docker",), False),
            ("labeled", ("fuzz",), True), ("synchronize", ("fuzz", "no-docker"), True),
            ("unlabeled", (), False), ("reopened", (), False),
        ):
            with self.subTest(action=action, labels=labels), tempfile.TemporaryDirectory() as directory:
                fixture = MergeFixture(Path(directory))
                fixture.write("README.md", "Updated prose\n")
                payload, tested = fixture.merge(labels, action)
                actual = selection.TestSelection.read("pull_request", payload, tested, fixture.root)
                self.assertTrue(actual.documentation_only)
                self.assertEqual(actual.bolero_required, required)
                self.assertEqual(actual.labels, labels)
                self.assertEqual(actual.action, action)

    def test_missing_or_mismatched_snapshot_fails_instead_of_excluding_tests(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = MergeFixture(Path(directory))
            fixture.write("README.md", "Updated prose\n")
            payload, tested = fixture.merge()
            for which in ("base", "head"):
                changed = json.loads(json.dumps(payload))
                changed["pull_request"][which]["sha"] = "a" * 40
                with self.subTest(which=which), self.assertRaisesRegex(ValueError, "exact base and head"):
                    selection.TestSelection.read("pull_request", changed, tested, fixture.root)
            with self.assertRaises(KeyError):
                selection.TestSelection.read("pull_request", {}, tested, fixture.root)
            fixture.git("switch", "change")
            with self.assertRaisesRegex(ValueError, "checkout must be"):
                selection.TestSelection.read("pull_request", payload, tested, fixture.root)

    def test_non_pr_events_require_execution_without_a_prose_exclusion(self) -> None:
        for event in ("schedule", "workflow_dispatch", "push"):
            with self.subTest(event=event):
                actual = selection.TestSelection.read(event, {}, "a" * 40)
                self.assertFalse(actual.documentation_only)
                self.assertTrue(actual.bolero_required)
                self.assertIsNone(actual.paths)

    def test_commit_identity_is_not_a_ref_or_command_argument(self) -> None:
        for value in ("main", "", "--help", "a" * 39, "g" * 40, "a" * 40 + "\n"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                selection.commit_sha(value)

    def test_git_failure_propagates_without_a_selection(self) -> None:
        with mock.patch.object(selection.subprocess, "check_output", side_effect=subprocess.CalledProcessError(1, "git")):
            with self.assertRaises(subprocess.CalledProcessError):
                selection.TestSelection.read("pull_request", {
                    "pull_request": {"base": {"sha": "a" * 40}, "head": {"sha": "b" * 40}},
                }, "c" * 40)

    def test_selection_record_and_outputs_do_not_claim_executed_properties(self) -> None:
        for path, required in (("README.md", False), ("src/runtime.rs", True)):
            with self.subTest(path=path), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                fixture = MergeFixture(root)
                fixture.write(path, "changed\n")
                payload, tested = fixture.merge()
                event_file = root / "event.json"
                event_file.write_text(json.dumps(payload))
                record_path = root / "evidence/selection.json"
                outputs = root / "outputs"
                environment = {
                    "GITHUB_EVENT_NAME": "pull_request", "GITHUB_EVENT_PATH": str(event_file),
                    "GITHUB_SHA": tested, "GITHUB_WORKSPACE": str(root), "GITHUB_RUN_ID": "100",
                    "GITHUB_RUN_ATTEMPT": "2", "GITHUB_JOB": "test-selection", "GITHUB_OUTPUT": str(outputs),
                }
                with mock.patch.dict(os.environ, environment), \
                        mock.patch("sys.argv", ["ci_test_selection", "--record", str(record_path)]), \
                        contextlib.redirect_stdout(io.StringIO()):
                    selection.main()
                record = json.loads(record_path.read_text())
                self.assertEqual(record["verdict"], "selected")
                self.assertEqual(record["run"], "100")
                self.assertEqual(record["attempt"], "2")
                self.assertEqual(record["job"], "test-selection")
                self.assertEqual(record["tested_sha"], tested)
                self.assertEqual(record["base_sha"], fixture.base)
                self.assertEqual(record["head_sha"], payload["pull_request"]["head"]["sha"])
                self.assertEqual(record["paths"], [path])
                self.assertEqual(record["labels"], [])
                self.assertEqual(record["tests_required"], required)
                self.assertEqual(record["bolero_required"], required)
                self.assertEqual(record["documentation_only"], not required)
                reason = "suites-required" if required else "verified-documentation-only"
                self.assertEqual(record["reason"], reason)
                self.assertEqual(outputs.read_text(),
                                 f"tests-required={str(required).lower()}\nbolero-required={str(required).lower()}\n")


class CheckWorkflowSelectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.check = (ROOT / ".github/workflows/check.yaml").read_text()

    def job(self, name: str) -> str:
        return re.split(r"\n  [a-z][a-z-]*:\n", self.check.split(f"\n  {name}:\n", 1)[1], maxsplit=1)[0]

    def test_ordinary_producers_wait_for_the_same_verified_selection(self) -> None:
        for name in ("tests", "scenarios", "extra-tests", "diagnostic-evidence"):
            with self.subTest(job=name):
                job = self.job(name)
                self.assertIn("needs: [test-selection]", job)
                self.assertIn("if: needs.test-selection.outputs.tests-required == 'true'", job)
        self.assertIn("needs: [tests, scenarios, extra-tests]", self.job("coverage"))
        for name in ("checks", "conventional-pr-title"):
            with self.subTest(job=name):
                self.assertNotIn("needs:", self.job(name))

    def test_selection_retains_verified_paths_and_job_identity_before_calling_suites(self) -> None:
        job = self.job("test-selection")
        self.assertIn("fetch-depth: 2", job)
        self.assertIn("run: python3 scripts/ci_test_selection.py --record target/ci-test-selection.json", job)
        self.assertIn("tests-required: ${{ steps.selection.outputs.tests-required }}", job)
        self.assertIn("bolero-required: ${{ steps.selection.outputs.bolero-required }}", job)
        self.assertIn("uses: actions/upload-artifact@v4", job)
        self.assertIn("name: ci-test-selection", job)
        self.assertIn("if-no-files-found: error", job)

    def test_mode_labels_remain_independent_of_ordinary_prose_exclusion(self) -> None:
        for job, label in (("deloxide", "deloxide"), ("client-conformance", "client-conformance"),
                           ("shuttle", "shuttle"), ("loom", "loom"),
                           ("loom-qualification", "loom"), ("turmoil", "turmoil")):
            with self.subTest(job=job):
                self.assertIn(f"if: contains(github.event.pull_request.labels.*.name, '{label}')", self.job(job))

    def test_validation_only_rerun_keeps_source_selection_and_required_cancellation_fails(self) -> None:
        gate = test_bolero_coverage.WorkflowTests()
        gate.setUp()
        for documentation_only in (False, True):
            with self.subTest(documentation_only=documentation_only), tempfile.TemporaryDirectory() as directory:
                current = selection.TestSelection(
                    "pull_request", "opened", "a" * 40, "b" * 40, "c" * 40,
                    ("README.md",) if documentation_only else ("src/runtime.rs",), (),
                )
                # Re-evaluating unrelated always() jobs must not call a source-excluded workflow.
                self.assertIn("needs: [test-selection]", self.job("bolero"))
                self.assertIn("if: needs.test-selection.outputs.bolero-required == 'true'", self.job("bolero"))
                if current.bolero_required:
                    result = subprocess.run(["bash", "-e", "-c", gate.script], cwd=directory, env={
                        **os.environ, "RANDOM_RESULT": "cancelled", "FUZZ_RESULT": "skipped",
                        "COMBINED_RESULT": "skipped", "FUZZ_REQUIRED": "false",
                    }, capture_output=True)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertFalse((Path(directory) / "selection.json").exists())
                else:
                    rerun = dataclasses.replace(current)
                    self.assertFalse(rerun.bolero_required)


if __name__ == "__main__":
    unittest.main()
