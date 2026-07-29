from __future__ import annotations

import unittest

from scripts.render_pdf_title import render_title_page


class RenderPdfTitleTests(unittest.TestCase):
    def test_inserts_and_escapes_the_documentation_version(self) -> None:
        template = r"Documentation version: \texttt{@@NERVIX_DOCUMENTATION_VERSION@@}"

        rendered = render_title_page(template, "release_1&preview%")

        self.assertEqual(
            rendered,
            r"Documentation version: \texttt{release\_1\&preview\%}",
        )

    def test_requires_exactly_one_version_placeholder(self) -> None:
        with self.assertRaisesRegex(ValueError, "exactly one"):
            render_title_page("No version placeholder", "v1")

        with self.assertRaisesRegex(ValueError, "exactly one"):
            render_title_page(
                "@@NERVIX_DOCUMENTATION_VERSION@@ "
                "@@NERVIX_DOCUMENTATION_VERSION@@",
                "v1",
            )

    def test_rejects_an_empty_version(self) -> None:
        with self.assertRaisesRegex(ValueError, "non-empty"):
            render_title_page("@@NERVIX_DOCUMENTATION_VERSION@@", "")
