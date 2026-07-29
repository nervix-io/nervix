from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts.prepare_pdf_html import chapter_headings, transform


class ChapterHeadingsTests(unittest.TestCase):
    def test_reads_h1_and_hierarchy_of_every_chapter(self) -> None:
        with TemporaryDirectory() as tmp:
            src = Path(tmp)
            (src / "SUMMARY.md").write_text(
                "# Summary\n"
                "\n"
                "- [Introduction](./introduction.md)\n"
                "- [Manual](./manual.md)\n"
                "  - [Nested](./nested.md)\n",
                encoding="utf-8",
            )
            (src / "introduction.md").write_text("# Introduction\n", encoding="utf-8")
            (src / "manual.md").write_text("# Manual Title\n", encoding="utf-8")
            (src / "nested.md").write_text("# Nested\n", encoding="utf-8")
            chapters = chapter_headings(src / "SUMMARY.md", src)
        self.assertEqual(
            chapters,
            [
                ("Introduction", True),
                ("Manual Title", True),
                ("Nested", False),
            ],
        )

    def test_rejects_chapter_without_h1(self) -> None:
        with TemporaryDirectory() as tmp:
            src = Path(tmp)
            (src / "SUMMARY.md").write_text("- [Broken](./broken.md)\n", encoding="utf-8")
            (src / "broken.md").write_text("no heading\n", encoding="utf-8")
            with self.assertRaises(SystemExit):
                chapter_headings(src / "SUMMARY.md", src)


class TransformTests(unittest.TestCase):
    def test_demotes_nested_h1_when_title_matches_later_top_level_chapter(
        self,
    ) -> None:
        print_html = (
            "<main>"
            '<h1 id="quickstart">Quickstart</h1>'
            '<h1 id="installation">Installation</h1>'
            '<h1 id="running">Running Nervix</h1>'
            '<h1 id="installation-1">Installation</h1>'
            '<h1 id="cargo">Cargo Install From GitHub</h1>'
            "</main>"
        )
        result = transform(
            print_html,
            [
                ("Quickstart", True),
                ("Installation", False),
                ("Running Nervix", False),
                ("Installation", True),
                ("Cargo Install From GitHub", False),
            ],
        )
        self.assertIn('<h2 id="installation">Installation</h2>', result)
        self.assertIn('<h1 id="installation-1">Installation</h1>', result)

    def test_keeps_top_level_h1_and_demotes_everything_else(self) -> None:
        print_html = (
            "<html><body><main>"
            '<h1 id="quickstart"><a class="header" href="#quickstart">Quickstart</a></h1>'
            '<h1 id="running"><a class="header" href="#running">Running Nervix</a></h1>'
            '<h2 id="server"><a class="header" href="#server">Start The Server</a></h2>'
            '<h6 id="deep">Deep</h6>'
            "</main></body></html>"
        )
        result = transform(
            print_html,
            [("Quickstart", True), ("Running Nervix", False)],
        )
        self.assertIn('<h1 id="quickstart">Quickstart</h1>', result)
        self.assertIn('<h2 id="running">Running Nervix</h2>', result)
        self.assertIn('<h3 id="server">Start The Server</h3>', result)
        self.assertIn('<h6 id="deep">Deep</h6>', result)
        self.assertNotIn('class="header"', result)

    def test_matches_titles_through_inline_markup_and_entities(self) -> None:
        print_html = (
            "<main>"
            '<h1 id="a">Schemas <code>&amp;</code>  Codecs</h1>'
            "</main>"
        )
        result = transform(print_html, [("Schemas & Codecs", True)])
        self.assertIn('<h1 id="a">', result)

    def test_rejects_html_without_main(self) -> None:
        with self.assertRaises(SystemExit):
            transform(
                "<html><body>no main</body></html>",
                [("Quickstart", True)],
            )


if __name__ == "__main__":
    unittest.main()
