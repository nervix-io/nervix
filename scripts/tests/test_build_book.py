from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts.build_book import (
    bundle_roto_reference,
    render_jaq_reference,
    resolve_package_version,
    rewrite_external_assets,
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
            (book / "FontAwesome").mkdir()
            page = book / "index.html"
            page.write_text(
                '<link rel="icon" href="favicon-de23e50b.svg">\n'
                '<link rel="shortcut icon" href="favicon-8114d1fc.png">\n'
                '<link rel="stylesheet" href="fonts/fonts-9644e21d.css">\n'
                '<link rel="stylesheet" href="FontAwesome/css/font-awesome-a1b2c3.css">\n',
                encoding="utf-8",
            )

            rewrite_external_assets(book)

            rewritten = page.read_text(encoding="utf-8")
            self.assertIn('href="theme/nervix-mark.svg"', rewritten)
            self.assertIn("fonts.googleapis.com", rewritten)
            self.assertIn("cdnjs.cloudflare.com", rewritten)
            self.assertFalse((book / "fonts").exists())
            self.assertFalse((book / "FontAwesome").exists())


if __name__ == "__main__":
    unittest.main()
