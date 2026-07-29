from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts.prepare_pdf_html import top_level_titles, transform


class TopLevelTitlesTests(unittest.TestCase):
    def test_reads_h1_of_zero_indent_chapters_only(self) -> None:
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
            titles = top_level_titles(src / "SUMMARY.md", src)
        self.assertEqual(titles, {"Introduction", "Manual Title"})

    def test_rejects_top_level_chapter_without_h1(self) -> None:
        with TemporaryDirectory() as tmp:
            src = Path(tmp)
            (src / "SUMMARY.md").write_text("- [Broken](./broken.md)\n", encoding="utf-8")
            (src / "broken.md").write_text("no heading\n", encoding="utf-8")
            with self.assertRaises(SystemExit):
                top_level_titles(src / "SUMMARY.md", src)


class TransformTests(unittest.TestCase):
    def test_keeps_top_level_h1_and_demotes_everything_else(self) -> None:
        print_html = (
            "<html><body><main>"
            '<h1 id="quickstart"><a class="header" href="#quickstart">Quickstart</a></h1>'
            '<h1 id="running"><a class="header" href="#running">Running Nervix</a></h1>'
            '<h2 id="server"><a class="header" href="#server">Start The Server</a></h2>'
            '<h6 id="deep">Deep</h6>'
            "</main></body></html>"
        )
        result = transform(print_html, {"Quickstart"})
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
        result = transform(print_html, {"Schemas & Codecs"})
        self.assertIn('<h1 id="a">', result)

    def test_rejects_html_without_main(self) -> None:
        with self.assertRaises(SystemExit):
            transform("<html><body>no main</body></html>", {"Quickstart"})


class LinkVerificationTests(unittest.TestCase):
    """The PDF must resolve its own links instead of sending readers to the site."""

    def transform_body(self, body: str) -> str:
        return transform(f"<main><h1 id=\"quickstart\">Quickstart</h1>{body}</main>", {"Quickstart"})

    def test_accepts_external_urls_and_in_document_fragments(self) -> None:
        result = self.transform_body(
            '<p><a href="https://example.test/page">external</a>'
            '<a href="mailto:docs@example.test">mail</a>'
            '<a href="#quickstart">internal</a></p>'
        )
        self.assertIn('href="#quickstart"', result)

    def test_rejects_cross_page_website_links(self) -> None:
        with self.assertRaises(SystemExit) as raised:
            self.transform_body('<p><a href="./quickstart-installation.html">Installation</a></p>')

        self.assertIn("quickstart-installation.html", str(raised.exception))

    def test_rejects_bare_relative_links(self) -> None:
        with self.assertRaises(SystemExit) as raised:
            self.transform_body('<p><a href="generate_cli">generated CLI</a></p>')

        self.assertIn("generate_cli", str(raised.exception))

    def test_rejects_fragments_with_no_target(self) -> None:
        with self.assertRaises(SystemExit) as raised:
            self.transform_body('<p><a href="#nowhere">missing</a></p>')

        self.assertIn("#nowhere", str(raised.exception))

    def test_percent_encoded_fragments_resolve(self) -> None:
        result = transform(
            '<main><h1 id="quickstart">Quickstart</h1>'
            '<h2 id="café">Café</h2>'
            '<p><a href="#caf%C3%A9">encoded</a></p></main>',
            {"Quickstart"},
        )
        self.assertIn('href="#caf%C3%A9"', result)


if __name__ == "__main__":
    unittest.main()
