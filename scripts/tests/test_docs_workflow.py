from __future__ import annotations

import tomllib
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from scripts.publish_docs_alias import update_alias


class DocsWorkflowTests(unittest.TestCase):
    def test_book_job_installs_locked_python_dependencies_with_uv(self) -> None:
        workflow = Path(".github/workflows/docker-build.yaml").read_text()
        _, build_book = workflow.split("\n  build-book:", maxsplit=1)

        self.assertNotIn("rclone", build_book)
        self.assertIn("astral-sh/setup-uv@", build_book)
        self.assertIn("uv sync --locked", build_book)

    def test_uv_project_declares_boto3_runtime_dependency(self) -> None:
        project = tomllib.loads(Path("pyproject.toml").read_text())

        self.assertIn("boto3>=1.40,<2", project["project"]["dependencies"])
        self.assertFalse(project["tool"]["uv"]["package"])

    def test_docs_commands_run_in_the_locked_uv_environment(self) -> None:
        justfile = Path("justfile").read_text()

        self.assertIn(
            "uv run --locked python scripts/upload_book_to_r2.py",
            justfile,
        )
        self.assertIn(
            "uv run --locked python scripts/publish_docs_alias.py",
            justfile,
        )

    def test_docs_publisher_does_not_shell_out_for_storage_operations(self) -> None:
        publisher = Path("scripts/publish_docs_alias.py").read_text()

        self.assertNotIn("import subprocess", publisher)
        self.assertNotIn("wrangler", publisher)
        self.assertNotIn("NamedTemporaryFile", publisher)

    def test_alias_is_written_through_the_direct_s3_client(self) -> None:
        client = Mock()
        with patch(
            "scripts.publish_docs_alias.upload_book_to_r2.put_object"
        ) as put_object:
            update_alias(
                client=client,
                bucket="nervix-docs",
                alias="snapshot",
                target="pr-43-abc/",
            )

        put_object.assert_called_once_with(
            client=client,
            bucket="nervix-docs",
            object_key="meta/snapshot.txt",
            payload=b"pr-43-abc\n",
            content_type="text/plain; charset=utf-8",
        )


if __name__ == "__main__":
    unittest.main()
