from __future__ import annotations

import unittest
from pathlib import Path


class DocsWorkflowTests(unittest.TestCase):
    def test_rclone_is_installed_in_book_job_only(self) -> None:
        workflow = Path(".github/workflows/docker-build.yaml").read_text()
        build_arch, remainder = workflow.split("\n  publish-manifest:", maxsplit=1)
        _, build_book = remainder.split("\n  build-book:", maxsplit=1)

        self.assertNotIn("- name: Install rclone", build_arch)
        self.assertIn("- name: Install rclone", build_book)
        self.assertLess(
            build_book.index("- name: Install rclone"),
            build_book.index("- name: Publish Book"),
        )


if __name__ == "__main__":
    unittest.main()
