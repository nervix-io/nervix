from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.publish_docs_alias import update_alias
from scripts.upload_book_to_r2 import R2Credentials


class DocsWorkflowTests(unittest.TestCase):
    def test_book_job_does_not_install_an_external_uploader(self) -> None:
        workflow = Path(".github/workflows/docker-build.yaml").read_text()
        _, build_book = workflow.split("\n  build-book:", maxsplit=1)

        self.assertNotIn("rclone", build_book)

    def test_docs_publisher_does_not_shell_out_for_storage_operations(self) -> None:
        publisher = Path("scripts/publish_docs_alias.py").read_text()

        self.assertNotIn("import subprocess", publisher)
        self.assertNotIn("wrangler", publisher)
        self.assertNotIn("NamedTemporaryFile", publisher)

    def test_alias_is_written_through_the_direct_s3_client(self) -> None:
        credentials = R2Credentials("token-id", "secret-key")
        with patch(
            "scripts.publish_docs_alias.upload_book_to_r2.put_object"
        ) as put_object:
            update_alias(
                account_id="account-id",
                bucket="nervix-docs",
                alias="snapshot",
                target="pr-43-abc/",
                credentials=credentials,
            )

        put_object.assert_called_once_with(
            account_id="account-id",
            bucket="nervix-docs",
            object_key="meta/snapshot.txt",
            payload=b"pr-43-abc\n",
            content_type="text/plain; charset=utf-8",
            credentials=credentials,
        )


if __name__ == "__main__":
    unittest.main()
