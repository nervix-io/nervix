from __future__ import annotations

import unittest
from pathlib import Path


class BenchmarkWorkflowTests(unittest.TestCase):
    def test_reporter_job_is_separate_least_privilege_and_always_updates_pr(self) -> None:
        workflow = Path(".github/workflows/docker-build.yaml").read_text()
        _, reporter = workflow.split("\n  benchmark-comment:", maxsplit=1)
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
        _, benchmark = workflow.split("\n  benchmark:", maxsplit=1)
        benchmark = benchmark.split("\n  benchmark-comment:", maxsplit=1)[0]

        self.assertIn("contents: read", benchmark)
        self.assertIn("packages: read", benchmark)
        self.assertNotIn("pull-requests: write", benchmark)
        self.assertNotIn("issues: write", benchmark)


if __name__ == "__main__":
    unittest.main()
