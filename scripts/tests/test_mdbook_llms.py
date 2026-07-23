from __future__ import annotations

import unittest
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.error import HTTPError

from scripts.mdbook_llms import render_llms
from scripts.upload_book_to_r2 import (
    R2Credentials,
    UploadEntry,
    build_put_request,
    content_type_for,
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

    def test_book_upload_builds_a_signed_s3_put_request(self) -> None:
        request = build_put_request(
            account_id="account-id",
            bucket="nervix-docs",
            object_key="pr-42-deadbeef/markdown/NSPL overview.md",
            payload=b"hello",
            content_type="text/markdown; charset=utf-8",
            credentials=R2Credentials(
                access_key_id="token-id",
                secret_access_key="secret-key",
            ),
            now=datetime(2026, 7, 23, 20, 30, tzinfo=UTC),
        )

        self.assertEqual(
            request.full_url,
            "https://account-id.r2.cloudflarestorage.com/nervix-docs/"
            "pr-42-deadbeef/markdown/NSPL%20overview.md",
        )
        self.assertEqual(request.method, "PUT")
        self.assertEqual(
            request.get_header("Content-type"), "text/markdown; charset=utf-8"
        )
        self.assertEqual(
            request.get_header("X-amz-content-sha256"),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
        )
        authorization = request.get_header("Authorization")
        self.assertIsNotNone(authorization)
        self.assertIn(
            "Credential=token-id/20260723/auto/s3/aws4_request", authorization
        )
        self.assertIn(
            "SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date",
            authorization,
        )
        self.assertIsNone(request.get_header("X-amz-acl"))

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

    def test_direct_upload_retries_transient_s3_errors(self) -> None:
        attempts = []
        delays = []

        class SuccessfulResponse:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

        def open_request(request, *, timeout):
            attempts.append((request, timeout))
            if len(attempts) == 1:
                raise HTTPError(request.full_url, 503, "Unavailable", None, None)
            return SuccessfulResponse()

        with TemporaryDirectory() as directory:
            path = Path(directory) / "index.html"
            path.write_text("hello", encoding="utf-8")
            upload_entry(
                account_id="account-id",
                bucket="nervix-docs",
                entry=UploadEntry(
                    path=path,
                    object_key="preview/index.html",
                    content_type="text/html; charset=utf-8",
                ),
                credentials=R2Credentials("token-id", "secret-key"),
                open_request=open_request,
                wait=delays.append,
            )

        self.assertEqual(len(attempts), 2)
        self.assertEqual(delays, [1])


if __name__ == "__main__":
    unittest.main()
