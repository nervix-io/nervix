from __future__ import annotations

import unittest
from pathlib import Path

from scripts.mdbook_llms import render_llms
from scripts.upload_book_to_r2 import content_type_for


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

    def test_markdown_upload_uses_registered_media_type(self) -> None:
        self.assertEqual(
            content_type_for(Path("markdown/nspl-overview.md")),
            "text/markdown; charset=utf-8",
        )


if __name__ == "__main__":
    unittest.main()
