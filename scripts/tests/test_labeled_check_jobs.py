"""Regressions for the CI jobs that run only when a pull request asks for them."""

from __future__ import annotations

import re
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
    "deloxide": "deloxide",
}
# A workflow names a label it acts on in a condition, directly or through the labels Check passes to
# the Bolero workflow.
LABEL_CONDITIONS = (
    re.compile(r"labels\.\*\.name, '([a-z0-9-]+)'\)"),
    re.compile(r"labels-json \|\| '\[\]'\), '([a-z0-9-]+)'\)"),
)
# A row of the developer guide's table of labels.
DOCUMENTED_LABEL = re.compile(r"^\| `([a-z0-9-]+)` \|", re.MULTILINE)


def labels_the_workflows_read() -> set[str]:
    labels: set[str] = set()
    for workflow in (ROOT / ".github/workflows").glob("*.yaml"):
        text = workflow.read_text()
        for condition in LABEL_CONDITIONS:
            labels.update(condition.findall(text))
    return labels


def labels_the_guide_lists() -> set[str]:
    guide = (ROOT / "docs/src/developing-nervix.md").read_text()
    section = guide.split("### CI labels\n", 1)[1].split("\n### ", 1)[0]
    return set(DOCUMENTED_LABEL.findall(section))


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

    def test_the_developer_guide_lists_every_label_a_workflow_reads(self) -> None:
        self.assertEqual(labels_the_workflows_read(), labels_the_guide_lists())


if __name__ == "__main__":
    unittest.main()
