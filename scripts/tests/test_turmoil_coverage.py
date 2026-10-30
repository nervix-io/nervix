"""Require canonical Turmoil coverage and independently attributed CI patch feedback."""

from __future__ import annotations

import json
import subprocess
import unittest

from scripts import native_coverage
from scripts.tests.test_native_coverage import job_section, step_section

ROOT = native_coverage.REPOSITORY


class TurmoilCoverageTests(unittest.TestCase):
    def test_collection_runs_the_canonical_gating_recipe(self) -> None:
        [producer] = native_coverage.select_producers(["test-turmoil"], native_coverage.PRODUCERS)
        self.assertEqual(producer.mode, "turmoil")
        self.assertEqual(producer.recipes(), ["test-turmoil"])
        dumped = subprocess.run(
            ["just", "--dump", "--dump-format", "json"], cwd=ROOT,
            capture_output=True, text=True, check=True,
        )
        recipes = json.loads(dumped.stdout)["recipes"]
        native_coverage.validate_composition(producer, recipes)
        body = json.dumps(recipes["coverage-turmoil"]["body"])
        self.assertIn("scripts.native_coverage", body)
        self.assertIn("run test-turmoil --output", body)

    def test_ci_retains_complete_coverage_and_reports_it_under_its_own_scope(self) -> None:
        workflow = (ROOT / ".github/workflows/check.yaml").read_text()
        job = job_section(workflow, "turmoil")
        self.assertRegex(job, r"tool: [^\n]*\bcargo-llvm-cov\b")
        self.assertIn("pull-requests: write", job)
        self.assertIn("fetch-depth: 0", job)
        run = step_section(job, "Turmoil simulations")
        self.assertIn("just coverage-native-extras test-turmoil --output target/turmoil.lcov", run)
        self.assertIn("just test-turmoil-replay-check", step_section(job, "Turmoil failure replay"))
        report = step_section(job, "Report Turmoil patch line coverage")
        self.assertIn("if: success()", report)
        self.assertIn("continue-on-error: true", report)
        self.assertIn('just coverage-patch "$BASE_SHA" target/turmoil.lcov', report)
        self.assertIn("--scope turmoil", report)
        self.assertIn('--pr-head "$PR_HEAD_SHA"', report)
        self.assertIn("--output target/turmoil-patch-coverage.md", report)
        upload = step_section(job, "Upload Turmoil coverage")
        self.assertIn("if: always()", upload)
        self.assertIn("name: coverage-turmoil", upload)
        for filename in ("lcov.info", "completion.json", "executions.jsonl", "export.log"):
            self.assertIn(f"target/native-coverage/test-turmoil/**/{filename}", upload)
        self.assertIn("target/turmoil.lcov", upload)
        self.assertIn("target/turmoil-patch-coverage.md", upload)
        failures = step_section(job, "Upload Turmoil failure records")
        self.assertIn("target/native-coverage-build-turmoil/turmoil-failures", failures)
        self.assertIn("target/native-coverage-build-turmoil/turmoil-suite", failures)
        ordinary = job_section(workflow, "coverage")
        self.assertIn("needs: [tests, scenarios, extra-tests]", ordinary)
        # The ordinary merge reads only the ordinary producers' artifacts, and leaves out each
        # collection of another mode that the native extras artifact holds.
        self.assertIn(
            'just coverage-merge coverage-inputs/tests "${scenarios[@]}" coverage-inputs/native-extras',
            ordinary,
        )
        for artifact in ("coverage-shuttle", "coverage-loom", "coverage-turmoil"):
            self.assertNotIn(artifact, ordinary)


if __name__ == "__main__":
    unittest.main()
