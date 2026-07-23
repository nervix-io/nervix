from __future__ import annotations

import unittest
from pathlib import Path

from scripts.mdbook_llms import render_llms
from scripts.upload_book_to_r2 import r2_environment, upload_command


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

    def test_book_upload_uses_one_parallel_rclone_copy(self) -> None:
        command = upload_command(
            "nervix-docs",
            "/pr-42-deadbeef/",
            Path("docs/book"),
        )

        self.assertEqual(command[:2], ["rclone", "copy"])
        self.assertEqual(command[2:4], ["docs/book", "r2:nervix-docs/pr-42-deadbeef"])
        self.assertIn("--no-check-dest", command)
        self.assertEqual(command[command.index("--transfers") + 1], "16")
        self.assertNotIn("wrangler", command)

    def test_rclone_uses_s3_credentials_derived_from_cloudflare_token(self) -> None:
        environment = r2_environment(
            account_id="account-id",
            token_id="token-id",
            api_token="token-value",
        )

        self.assertEqual(environment["RCLONE_CONFIG_R2_TYPE"], "s3")
        self.assertEqual(environment["RCLONE_CONFIG_R2_PROVIDER"], "Cloudflare")
        self.assertEqual(
            environment["RCLONE_CONFIG_R2_ENDPOINT"],
            "https://account-id.r2.cloudflarestorage.com",
        )
        self.assertEqual(environment["RCLONE_CONFIG_R2_ACCESS_KEY_ID"], "token-id")
        self.assertEqual(
            environment["RCLONE_CONFIG_R2_SECRET_ACCESS_KEY"],
            "e6c02a5742ea9d4de588eb9b9de7bed43dc17011552186bed3e98b2c5958ff4a",
        )
        self.assertEqual(environment["RCLONE_CONFIG_R2_NO_CHECK_BUCKET"], "true")
        self.assertNotIn("token-value", environment.values())


if __name__ == "__main__":
    unittest.main()
