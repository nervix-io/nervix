from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

from scripts.mdbook_llms import render_llms
from scripts.upload_book_to_r2 import (
    MAX_UPLOAD_ATTEMPTS,
    PARALLEL_UPLOADS,
    R2Credentials,
    UploadEntry,
    content_type_for,
    create_r2_client,
    r2_credentials,
    upload_entry,
)


class RenderLlmsTests(unittest.TestCase):
    def test_renders_only_configured_public_nspl_chapters(self) -> None:
        context = {
            "book": {
                "sections": [
                    {
                        "Chapter": {
                            "name": "Manual",
                            "path": "manual.md",
                            "sub_items": [
                                {
                                    "Chapter": {
                                        "name": "How To Run",
                                        "path": "running-locally.md",
                                        "sub_items": [],
                                    }
                                },
                                {
                                    "Chapter": {
                                        "name": "NSPL Overview",
                                        "path": "nspl-overview.md",
                                        "sub_items": [],
                                    }
                                },
                            ],
                        }
                    },
                    {
                        "Chapter": {
                            "name": "Architecture",
                            "path": "architecture.md",
                            "sub_items": [
                                {
                                    "Chapter": {
                                        "name": "Streams And State",
                                        "path": "relay.md",
                                        "sub_items": [],
                                    }
                                }
                            ],
                        }
                    },
                ]
            },
            "config": {
                "output": {
                    "llms": {
                        "title": "Nervix NSPL Documentation",
                        "description": "Public NSPL configuration reference for Nervix.",
                        "version": "v1.2.3",
                        "include": ["nspl-overview.md", "relay.md"],
                    }
                }
            },
        }

        rendered = render_llms(context)

        self.assertEqual(
            rendered,
            "# Nervix NSPL Documentation (v1.2.3)\n\n"
            "> Public NSPL configuration reference for Nervix.\n\n"
            "Every link below belongs to this immutable documentation version.\n\n"
            "## Public NSPL Reference\n\n"
            "- [NSPL Overview](markdown/nspl-overview.md)\n"
            "- [Streams And State](markdown/relay.md)\n",
        )
        self.assertNotIn("How To Run", rendered)
        self.assertNotIn("Architecture", rendered)

    def test_rejects_missing_configured_chapter(self) -> None:
        context = {
            "book": {"sections": []},
            "config": {"output": {"llms": {"include": ["missing.md"]}}},
        }

        with self.assertRaisesRegex(ValueError, "missing.md"):
            render_llms(context)

    def test_accepts_mdbook_05_items_shape(self) -> None:
        context = {
            "book": {
                "items": [
                    {
                        "Chapter": {
                            "name": "NSPL Overview",
                            "path": "nspl-overview.md",
                            "sub_items": [],
                        }
                    }
                ]
            },
            "config": {
                "output": {"llms": {"include": ["nspl-overview.md"]}}
            },
        }

        self.assertIn(
            "[NSPL Overview](markdown/nspl-overview.md)", render_llms(context)
        )

    def test_book_upload_configures_boto3_for_r2(self) -> None:
        credentials = R2Credentials(
            access_key_id="token-id",
            secret_access_key="secret-key",
        )
        with patch("scripts.upload_book_to_r2.boto3.client") as client_factory:
            client = create_r2_client("account-id", credentials)

        self.assertIs(client, client_factory.return_value)
        client_factory.assert_called_once()
        options = client_factory.call_args.kwargs
        self.assertEqual(options["service_name"], "s3")
        self.assertEqual(
            options["endpoint_url"],
            "https://account-id.r2.cloudflarestorage.com",
        )
        self.assertEqual(options["aws_access_key_id"], "token-id")
        self.assertEqual(options["aws_secret_access_key"], "secret-key")
        self.assertEqual(options["region_name"], "auto")
        config = options["config"]
        self.assertEqual(config.max_pool_connections, PARALLEL_UPLOADS)
        self.assertEqual(
            config.retries,
            {
                "mode": "standard",
                "total_max_attempts": MAX_UPLOAD_ATTEMPTS,
            },
        )

    def test_s3_credentials_are_derived_from_cloudflare_token(self) -> None:
        credentials = r2_credentials(
            token_id="token-id",
            api_token="token-value",
        )

        self.assertEqual(credentials.access_key_id, "token-id")
        self.assertEqual(
            credentials.secret_access_key,
            "e6c02a5742ea9d4de588eb9b9de7bed43dc17011552186bed3e98b2c5958ff4a",
        )

    def test_markdown_upload_uses_registered_media_type(self) -> None:
        self.assertEqual(
            content_type_for(Path("markdown/nspl-overview.md")),
            "text/markdown; charset=utf-8",
        )

    def test_upload_uses_boto3_put_object_with_content_type(self) -> None:
        client = Mock()
        with TemporaryDirectory() as directory:
            path = Path(directory) / "index.html"
            path.write_text("hello", encoding="utf-8")
            upload_entry(
                client=client,
                bucket="nervix-docs",
                entry=UploadEntry(
                    path=path,
                    object_key="preview/index.html",
                    content_type="text/html; charset=utf-8",
                ),
            )

        client.put_object.assert_called_once_with(
            Bucket="nervix-docs",
            Key="preview/index.html",
            Body=b"hello",
            ContentType="text/html; charset=utf-8",
        )


if __name__ == "__main__":
    unittest.main()
