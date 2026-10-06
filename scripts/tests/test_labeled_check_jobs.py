"""Regressions for the Check workflow's jobs that run only when a pull request asks for them."""

from __future__ import annotations

import unittest
from pathlib import Path

from scripts.tests.test_native_coverage import job_section

ROOT = Path(__file__).resolve().parents[2]
# Each job runs only for a pull request that carries its label.
LABELS_BY_JOB = {
    "shuttle": "shuttle",
    "loom": "loom",
    "loom-qualification": "loom",
    "turmoil": "turmoil",
    "client-conformance": "client-conformance",
}


class LabeledJobTests(unittest.TestCase):
    def test_adding_or_removing_a_label_reevaluates_the_jobs(self) -> None:
        workflow = (ROOT / ".github/workflows/check.yaml").read_text()
        self.assertIn("types: [opened, synchronize, reopened, labeled, unlabeled]", workflow)

    def test_each_labeled_job_runs_only_for_a_pull_request_with_its_label(self) -> None:
        workflow = (ROOT / ".github/workflows/check.yaml").read_text()
        for name, label in LABELS_BY_JOB.items():
            with self.subTest(job=name):
                job = job_section(workflow, name)
                self.assertIn(f"if: contains(github.event.pull_request.labels.*.name, '{label}')", job)


if __name__ == "__main__":
    unittest.main()
