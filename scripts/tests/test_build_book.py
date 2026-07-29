from __future__ import annotations

import subprocess
import unittest
from unittest.mock import patch

from scripts.build_book import (
    MDBOOK_VERSION,
    bundle_roto_reference,
    resolve_roto_version,
    roto_bundle_is_current,
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
"""

SAMPLE_UPSTREAM = """(lang)=
# Language Reference

This section describes the basic syntax of Roto scripts.

(lang_comments)=
## Comments

Comments start with `//`.

:::{note}
Floating point literals need either a `.`, `e` or `E`.
:::

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

If you're using Roto as a binary or with the [generated
CLI](generate_cli), run the tests with the `test` subcommand. Nervix documents
the catalog in [User-Defined Functions](udfs.md).
"""


class RotoReferenceBundleTests(unittest.TestCase):
    def test_resolves_exact_roto_version_from_cargo_lock(self) -> None:
        self.assertEqual(resolve_roto_version(SAMPLE_CARGO_LOCK), "0.11.3")

    def test_missing_roto_package_is_an_error(self) -> None:
        with self.assertRaises(SystemExit):
            resolve_roto_version('version = 4\n[[package]]\nname = "regex"\nversion = "1.0.0"\n')

    def test_bundle_normalizes_myst_and_records_provenance(self) -> None:
        bundled = bundle_roto_reference(SAMPLE_UPSTREAM, "0.11.3")

        self.assertIn("# Roto Language Reference", bundled)
        self.assertNotIn("# Language Reference\n", bundled.replace("# Roto Language Reference\n", ""))
        # provenance names the exact upstream location and tag
        self.assertIn("NLnetLabs/roto", bundled)
        self.assertIn("docs/source/reference/language_reference.md", bundled)
        self.assertIn("v0.11.3", bundled)
        # anchor target lines are dropped
        self.assertNotIn("(lang)=", bundled)
        self.assertNotIn("(lang_comments)=", bundled)
        # note admonitions become blockquotes
        self.assertNotIn(":::{note}", bundled)
        self.assertIn("> **Note:**", bundled)
        self.assertIn("> Floating point literals need either a `.`, `e` or `E`.", bundled)
        # test outputs become labelled code fences
        self.assertNotIn("{testoutput}", bundled)
        self.assertIn("Output:\n\n```text\nx is 10\n```", bundled)
        self.assertNotIn(":::", bundled)
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

    def test_bundle_currency_is_detected_by_version_marker(self) -> None:
        bundled = bundle_roto_reference(SAMPLE_UPSTREAM, "0.11.3")

        self.assertTrue(roto_bundle_is_current(bundled, "0.11.3"))
        self.assertFalse(roto_bundle_is_current(bundled, "0.11.4"))
        self.assertFalse(roto_bundle_is_current("# Roto Language Reference\n", "0.11.3"))


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


if __name__ == "__main__":
    unittest.main()
