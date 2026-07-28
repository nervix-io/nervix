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

    def test_rewrites_book_links_to_pdf_destinations(self) -> None:
        print_html = (
            "<main>"
            '<h1 id="manual">Manual</h1>'
            '<p><a href="udfs.html#roto-column-operations">Roto operations</a></p>'
            '<p><a href="./jaq-manual.html#jaq-corelang">JAQ core</a></p>'
            '<p><a href="https://docs.nervix.io/v1/processors.html#junction">Junction</a></p>'
            '<p><a href="https://example.com/spec.html#section">External</a></p>'
            "<pre><code>.foo ⟼ 😀🙂🧑🔬🤔☀️</code></pre>"
            "</main>"
        )

        result = transform(print_html, {"Manual"})

        self.assertIn('href="#roto-column-operations"', result)
        self.assertIn('href="#jaq-corelang"', result)
        self.assertIn('href="#junction"', result)
        self.assertIn('href="https://example.com/spec.html#section"', result)
        self.assertIn(
            ".foo =&gt; [U+1F600][U+1F642][U+1F9D1][U+1F52C]"
            "[U+1F914][U+2600]",
            result,
        )

    def test_rejects_html_without_main(self) -> None:
        with self.assertRaises(SystemExit):
            transform("<html><body>no main</body></html>", {"Quickstart"})


if __name__ == "__main__":
    unittest.main()
