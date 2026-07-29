from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts.prepare_pdf_html import chapter_hierarchy, transform


class ChapterHierarchyTests(unittest.TestCase):
    def test_reads_hierarchy_of_every_chapter(self) -> None:
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
            hierarchy = chapter_hierarchy(src / "SUMMARY.md")
        self.assertEqual(hierarchy, [True, True, False])

    def test_rejects_summary_without_top_level_chapter(self) -> None:
        with TemporaryDirectory() as tmp:
            src = Path(tmp)
            (src / "SUMMARY.md").write_text(
                "  - [Nested](./nested.md)\n",
                encoding="utf-8",
            )
            with self.assertRaises(SystemExit):
                chapter_hierarchy(src / "SUMMARY.md")


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
            [True, False, False, True, False],
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
            [True, False],
        )
        self.assertIn('<h1 id="quickstart">Quickstart</h1>', result)
        self.assertIn('<h2 id="running">Running Nervix</h2>', result)
        self.assertIn('<h3 id="server">Start The Server</h3>', result)
        self.assertIn('<h6 id="deep">Deep</h6>', result)
        self.assertNotIn('class="header"', result)

    def test_preserves_inline_markup_and_entities(self) -> None:
        print_html = (
            "<main>"
            '<h1 id="a">Schemas <code>&amp;</code>  Codecs</h1>'
            "</main>"
        )
        result = transform(print_html, [True])
        self.assertIn('<h1 id="a">', result)

    def test_rewrites_book_links_to_pdf_destinations(self) -> None:
        print_html = (
            "<main>"
            '<h1 id="manual">Manual</h1>'
            '<p><a href="udfs.html#roto-column-operations">Roto operations</a></p>'
            '<p><a href="./jaq-reference.html#jaq-reference">JAQ reference</a></p>'
            '<p><a href="https://docs.nervix.io/v1/processors.html#junction">Junction</a></p>'
            '<p><a href="https://example.com/spec.html#section">External</a></p>'
            "<pre><code>.foo ⟼ 😀🙂🧑🔬🤔☀️</code></pre>"
            # the print page carries every chapter, so the rewritten fragments
            # have targets to land on
            '<h2 id="roto-column-operations">Roto Column Operations</h2>'
            '<h2 id="jaq-reference">JAQ Reference</h2>'
            '<h2 id="junction">Junction</h2>'
            "</main>"
        )

        result = transform(print_html, [True])

        self.assertIn('href="#roto-column-operations"', result)
        self.assertIn('href="#jaq-reference"', result)
        self.assertIn('href="#junction"', result)
        self.assertIn('href="https://example.com/spec.html#section"', result)
        self.assertIn(
            ".foo =&gt; [U+1F600][U+1F642][U+1F9D1][U+1F52C]"
            "[U+1F914][U+2600]",
            result,
        )

    def test_adds_bounded_equal_widths_to_pdf_table_columns(self) -> None:
        print_html = (
            "<main>"
            '<h1 id="manual">Manual</h1>'
            "<table>"
            "<thead><tr><th>One</th><th>Two</th><th>Three</th><th>Four</th></tr></thead>"
            "<tbody><tr><td>A</td><td>B</td><td>C</td><td>D</td></tr></tbody>"
            "</table>"
            "</main>"
        )

        result = transform(print_html, [True])

        self.assertIn("<colgroup>", result)
        self.assertEqual(result.count('<col style="width: 25.000000%">'), 4)

    def test_rejects_html_without_main(self) -> None:
        with self.assertRaises(SystemExit):
            transform(
                "<html><body>no main</body></html>",
                [True],
            )


class LinkVerificationTests(unittest.TestCase):
    """The PDF must resolve its own links instead of sending readers to the site."""

    def transform_body(self, body: str) -> str:
        return transform(f'<main><h1 id="quickstart">Quickstart</h1>{body}</main>', [True])

    def test_accepts_external_urls_and_in_document_fragments(self) -> None:
        result = self.transform_body(
            '<p><a href="https://example.test/page">external</a>'
            '<a href="mailto:docs@example.test">mail</a>'
            '<a href="#quickstart">internal</a></p>'
        )
        self.assertIn('href="#quickstart"', result)

    def test_rejects_cross_page_website_links(self) -> None:
        # mdBook rewrites these on the print page; a survivor means the reader
        # would be sent to the website for a chapter the PDF already contains.
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
            [True],
        )
        self.assertIn('href="#caf%C3%A9"', result)


if __name__ == "__main__":
    unittest.main()
