from __future__ import annotations

import unittest
from pathlib import Path


class BenchmarkWorkflowTests(unittest.TestCase):
    def test_no_docker_label_skips_pr_pipeline(self) -> None:
        workflow = Path(".github/workflows/docker-build.yaml").read_text()
        trigger, jobs = workflow.split("\njobs:", maxsplit=1)
        meta = jobs.split("\n  meta:", maxsplit=1)[1]
        meta = meta.split("\n  build-arch:", maxsplit=1)[0]
        reporter = jobs.split("\n  benchmark-comment:", maxsplit=1)[1]
        reporter = reporter.split("\n  publish-manifest:", maxsplit=1)[0]

        self.assertIn(
            "types: [opened, synchronize, reopened, labeled]",
            trigger,
        )
        self.assertIn(
            "!contains(github.event.pull_request.labels.*.name, 'no-docker')",
            meta,
        )
        self.assertIn("github.event_name != 'pull_request'", meta)
        self.assertIn(
            "!contains(github.event.pull_request.labels.*.name, 'no-docker')",
            reporter,
        )

    def test_reporter_job_is_separate_least_privilege_and_always_updates_pr(self) -> None:
        workflow = Path(".github/workflows/docker-build.yaml").read_text()
        reporter = workflow.split("\n  benchmark-comment:", maxsplit=1)[1]
        reporter = reporter.split("\n  publish-manifest:", maxsplit=1)[0]

        self.assertIn("needs: [build-arch, benchmark]", reporter)
        self.assertIn("always()", reporter)
        self.assertIn("pull-requests: write", reporter)
        self.assertNotIn("issues: write", reporter)
        self.assertNotIn("actions/checkout", reporter)
        self.assertIn("actions/download-artifact@v4", reporter)
        self.assertIn("continue-on-error: true", reporter)
        self.assertIn("<!-- nervix-benchmark-comparison -->", reporter)
        self.assertIn("needs.benchmark.result", reporter)
        self.assertIn("github.rest.issues.updateComment", reporter)
        self.assertIn("github.rest.issues.createComment", reporter)

    def test_benchmark_job_keeps_read_only_repository_permissions(self) -> None:
        workflow = Path(".github/workflows/docker-build.yaml").read_text()
        benchmark = workflow.split("\n  benchmark:", maxsplit=1)[1]
        benchmark = benchmark.split("\n  benchmark-comment:", maxsplit=1)[0]

        self.assertIn("contents: read", benchmark)
        self.assertIn("packages: read", benchmark)
        self.assertNotIn("pull-requests: write", benchmark)
        self.assertNotIn("issues: write", benchmark)


if __name__ == "__main__":
    unittest.main()
