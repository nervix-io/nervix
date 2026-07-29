from __future__ import annotations

import subprocess
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from scripts.build_book import (
    MDBOOK_VERSION,
    bundle_roto_reference,
    render_jaq_reference,
    remove_generated_edit_links,
    resolve_package_version,
    rewrite_external_assets,
    run_mdbook,
    stage_book,
    verify_mdbook_version,
)


SAMPLE_CARGO_LOCK = """
version = 4

[[package]]
name = "regex"
version = "1.11.1"
source = "registry+https://github.com/rust-lang/crates.io-index"

[[package]]
name = "roto"
version = "0.11.3"
source = "registry+https://github.com/rust-lang/crates.io-index"

[[package]]
name = "roto-macros"
version = "0.11.0"
source = "registry+https://github.com/rust-lang/crates.io-index"

[[package]]
name = "jaq-core"
version = "3.1.0"
source = "registry+https://github.com/rust-lang/crates.io-index"
"""

SAMPLE_ROTO_UPSTREAM = """(lang)=
# Language Reference

This section describes the basic syntax of Roto scripts.

(lang_comments)=
## Comments

Comments start with `//`.

:::{note}
Floating point literals need either a `.`, `e` or `E`.
:::

{class="test-error"}
  { class = 'test-ignore' }
```roto
let x = 10;
print(f"x is {x}");
```

::::{testoutput}
x is 10
::::

Constants are [declared by the runtime](#add-constants). See also [context
variables](#lang_context) for details, and the
[Roto repository](https://github.com/NLnetLabs/roto) for sources.

The boolean type is {roto:ref}`bool`.
Optional values are described by `lang_optionals`{.interpreted-text role="ref"}.
See {doc}`std/index`.

If you're using Roto as a binary or with the [generated
CLI](generate_cli), run the tests with the `test` subcommand. Nervix documents
the catalog in [User-Defined Functions](udfs.md).
"""

SAMPLE_ROTO_LICENSE = """Copyright (c) 2022, NLnet Labs. All rights reserved.

Redistribution is permitted under the BSD-3-Clause license.
"""

class RotoReferenceBundleTests(unittest.TestCase):
    def test_resolves_exact_upstream_versions_from_cargo_lock(self) -> None:
        self.assertEqual(resolve_package_version(SAMPLE_CARGO_LOCK, "roto"), "0.11.3")
        self.assertEqual(resolve_package_version(SAMPLE_CARGO_LOCK, "jaq-core"), "3.1.0")

    def test_missing_package_is_an_error(self) -> None:
        with self.assertRaises(SystemExit):
            resolve_package_version(
                'version = 4\n[[package]]\nname = "regex"\nversion = "1.0.0"\n',
                "jaq-core",
            )

    def test_bundle_normalizes_myst_and_records_provenance(self) -> None:
        bundled = bundle_roto_reference(
            SAMPLE_ROTO_UPSTREAM,
            SAMPLE_ROTO_LICENSE,
            "0.11.3",
        )

        self.assertIn("# Roto Language Reference", bundled)
        self.assertNotIn("# Language Reference\n", bundled.replace("# Roto Language Reference\n", ""))
        # provenance names the exact upstream location and tag
        self.assertIn("NLnetLabs/roto", bundled)
        self.assertIn("docs/source/reference/language_reference.md", bundled)
        self.assertIn("v0.11.3", bundled)
        # MyST anchors become namespaced local PDF/HTML destinations.
        self.assertNotIn("(lang)=", bundled)
        self.assertNotIn("(lang_comments)=", bundled)
        self.assertIn('<a id="roto-lang"></a>', bundled)
        self.assertIn('<a id="roto-lang_comments"></a>', bundled)
        # note admonitions become blockquotes
        self.assertNotIn(":::{note}", bundled)
        self.assertIn("> **Note:**", bundled)
        self.assertIn("> Floating point literals need either a `.`, `e` or `E`.", bundled)
        # test outputs become labelled code fences
        self.assertNotIn("{testoutput}", bundled)
        self.assertIn("Output:\n\n```text\nx is 10\n```", bundled)
        self.assertNotIn(":::", bundled)
        # MyST code attributes and roles do not leak into rendered prose.
        self.assertNotIn('{class="test-error"}', bundled)
        self.assertNotIn("{ class = 'test-ignore' }", bundled)
        self.assertNotIn("{roto:ref}", bundled)
        self.assertNotIn("interpreted-text", bundled)
        self.assertNotIn("{doc}", bundled)
        self.assertIn("[`bool`](#roto-lang_booleans)", bundled)
        self.assertIn("[optional values](#roto-lang_optionals)", bundled)
        self.assertIn(
            "[Nervix Roto column operations](udfs.md#roto-column-operations)",
            bundled,
        )
        # in-page links to dropped anchors are unwrapped, including links wrapped
        # across a line break; external links survive
        self.assertIn("declared by the runtime", bundled)
        self.assertIn("context\nvariables for details", bundled)
        self.assertNotIn("(#add-constants)", bundled)
        self.assertNotIn("(#lang_context)", bundled)
        self.assertIn("[Roto repository](https://github.com/NLnetLabs/roto)", bundled)
        # cross-page MyST references name Roto pages Nervix does not bundle, so
        # they are unwrapped too; relative links to bundled chapters survive
        self.assertIn("with the generated\nCLI, run the tests", bundled)
        self.assertNotIn("(generate_cli)", bundled)
        self.assertIn("[User-Defined Functions](udfs.md)", bundled)
        # The required upstream redistribution notice ships with the generated page.
        self.assertIn(SAMPLE_ROTO_LICENSE, bundled)

    def test_bundle_rejects_unhandled_myst_artifacts(self) -> None:
        with self.assertRaisesRegex(SystemExit, "unhandled MyST artifact"):
            bundle_roto_reference(
                "# Language Reference\n\n{class=unquoted}\n",
                SAMPLE_ROTO_LICENSE,
                "0.11.3",
            )


class JaqReferenceTests(unittest.TestCase):
    def test_reference_links_the_readable_manual_and_exact_release(self) -> None:
        reference = render_jaq_reference("3.1.0")

        self.assertIn("# JAQ Reference", reference)
        self.assertIn("`jaq-core` 3.1.0", reference)
        self.assertIn("https://gedenkt.at/jaq/manual/", reference)
        self.assertIn("https://github.com/01mf02/jaq/releases/tag/v3.1.0", reference)
        self.assertNotIn("/releases/download/", reference)
        self.assertNotIn(".xhtml", reference)
        self.assertIn("[JAQ transformations](schemas-and-codecs.md#jaq-transformations)", reference)
        self.assertIn("## Common Filters", reference)
        self.assertIn(r"| `.items \| map(.value)` | Transform every array element |", reference)
        self.assertIn(r"| `.items[] \| select(.enabled)` | Select matching elements |", reference)
        self.assertLess(len(reference.split()), 350)


class ExternalAssetRewriteTests(unittest.TestCase):
    def test_rewrites_hashed_mdbook_assets_before_removing_bundled_fonts(self) -> None:
        with TemporaryDirectory() as tmp:
            book = Path(tmp)
            (book / "fonts").mkdir()
            page = book / "index.html"
            page.write_text(
                '<link rel="icon" href="favicon-de23e50b.svg">\n'
                '<link rel="shortcut icon" href="favicon-8114d1fc.png">\n'
                '<link rel="stylesheet" href="fonts/fonts-9644e21d.css">\n',
                encoding="utf-8",
            )

            rewrite_external_assets(book)

            rewritten = page.read_text(encoding="utf-8")
            self.assertIn('href="theme/nervix-mark.svg"', rewritten)
            self.assertIn("fonts.googleapis.com", rewritten)
            self.assertFalse((book / "fonts").exists())

    def test_a_link_that_stops_matching_fails_the_build(self) -> None:
        # The bundled fonts directory is deleted, so a rewrite that silently
        # matches nothing would publish a dead stylesheet reference.
        with TemporaryDirectory() as tmp:
            book = Path(tmp)
            (book / "fonts").mkdir()
            (book / "index.html").write_text(
                '<link rel="icon" href="favicon-de23e50b.svg">\n'
                '<link rel="shortcut icon" href="favicon-8114d1fc.png">\n'
                '<link rel="stylesheet" href="fonts/fonts.2024.css">\n',
                encoding="utf-8",
            )

            with self.assertRaises(SystemExit) as raised:
                rewrite_external_assets(book)

            self.assertIn("fonts/fonts", str(raised.exception))
            # the build stops before the bundled directory is removed
            self.assertTrue((book / "fonts").exists())


class BookStagingTests(unittest.TestCase):
    def test_staged_source_keeps_the_src_name_that_edit_links_render(self) -> None:
        # mdBook renders `edit-url-template`'s `{path}` as the configured source
        # directory joined with the chapter path, so staging under any other
        # name would publish "Suggest an edit" links into a build directory.
        with TemporaryDirectory() as tmp:
            staged = Path(tmp) / "book"
            with patch("scripts.build_book.generate_upstream_references") as generate:
                stage_book(staged)

            self.assertTrue((staged / "src" / "introduction.md").is_file())
            self.assertTrue((staged / "book.toml").is_file())
            self.assertTrue((staged / "theme" / "nervix.css").is_file())
            generate.assert_called_once_with(staged / "src")

    def test_generated_chapters_lose_their_edit_link(self) -> None:
        with TemporaryDirectory() as tmp:
            book = Path(tmp)
            edit_link = (
                '<a href="https://github.com/nervix-io/nervix/edit/main/docs/src/x.md"'
                ' title="Suggest an edit" rel="edit">\n  <span>icon</span>\n</a>\n'
            )
            for name in ("roto-language-reference", "jaq-reference", "udfs"):
                (book / f"{name}.html").write_text(
                    f"<nav>{edit_link}</nav><p>{name}</p>", encoding="utf-8"
                )

            remove_generated_edit_links(book)

            self.assertNotIn("Suggest an edit", (book / "roto-language-reference.html").read_text())
            self.assertNotIn("Suggest an edit", (book / "jaq-reference.html").read_text())
            # authored chapters keep theirs
            self.assertIn("Suggest an edit", (book / "udfs.html").read_text())

    def test_missing_generated_chapter_is_an_error(self) -> None:
        with TemporaryDirectory() as tmp:
            with self.assertRaises(SystemExit):
                remove_generated_edit_links(Path(tmp))


class MdbookVersionTests(unittest.TestCase):
    def completed(self, stdout: str) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(["mdbook", "--version"], 0, stdout=stdout, stderr="")

    def test_pinned_version_is_accepted(self) -> None:
        with patch(
            "scripts.build_book.subprocess.run",
            return_value=self.completed(f"mdbook v{MDBOOK_VERSION}\n"),
        ):
            verify_mdbook_version()

    def test_other_version_is_rejected_by_name(self) -> None:
        with patch(
            "scripts.build_book.subprocess.run",
            return_value=self.completed("mdbook v0.4.40\n"),
        ):
            with self.assertRaises(SystemExit) as raised:
                verify_mdbook_version()

        self.assertIn(MDBOOK_VERSION, str(raised.exception))
        self.assertIn("0.4.40", str(raised.exception))

    def test_missing_mdbook_is_reported(self) -> None:
        with patch("scripts.build_book.subprocess.run", side_effect=FileNotFoundError("mdbook")):
            with self.assertRaises(SystemExit):
                verify_mdbook_version()


class MdbookBuildTests(unittest.TestCase):
    def test_logged_renderer_errors_fail_the_build(self) -> None:
        # mdBook exits zero after rejecting `[output.html]`, which would
        # otherwise publish a book rendered with default settings.
        rejected = subprocess.CompletedProcess(
            ["mdbook", "build"],
            0,
            stdout="",
            stderr=" ERROR Failed to deserialize `output.html`\n",
        )
        with patch("scripts.build_book.subprocess.run", return_value=rejected):
            with patch("scripts.build_book.sys.stderr"):
                with self.assertRaises(SystemExit) as raised:
                    run_mdbook(Path("/nonexistent/book"), Path("/nonexistent/out"), {})

        self.assertIn("output.html", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
