#!/usr/bin/env python3

"""Reject `Result<_, String>` in product Rust code.

A failure is a semantic `thiserror` enum returned inside `error_stack::Result` and owned by the
module that decides what the failure means. A `String` names no failure a caller can act on, so this
is a rule rather than a count: there is no baseline, and every occurrence fails the check.

The scan is the debt ratchet's. Comments and literal contents are blanked, `#[cfg(test)]` items and
the test tree are skipped, and generic arguments are resolved, so a signature split across lines and
a `collect::<Result<Vec<_>, String>>()` turbofish are found like any other occurrence.

Run it from the repository root as `python3 -m scripts.check_typed_errors`, which is what
`just validate-typed-errors` does.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Sequence

from scripts.ratchet import (
    RustFile,
    Site,
    generic_arguments,
    load_files,
    product_files,
    repository_root,
)

RULE = "typed-errors"

_RESULT = re.compile(r"\bResult\s*<")


def find_string_errors(files: Sequence[RustFile]) -> list[Site]:
    """Return every `Result` in product code whose error argument is `String`."""

    sites: list[Site] = []
    for file in product_files(files):
        for match in _RESULT.finditer(file.product):
            arguments = generic_arguments(file.product, match.end() - 1)
            if arguments is None or len(arguments) < 2:
                continue
            if arguments[-1].strip() == "String":
                sites.append(file.site(match.start(), file.source_line(match.start())))
    return sites


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Fail when product Rust code uses `Result<_, String>` in place of a typed error."
        )
    )
    parser.add_argument(
        "--root",
        type=Path,
        help="repository to check; defaults to the repository holding this script",
    )
    arguments = parser.parse_args(argv)

    root = arguments.root if arguments.root is not None else repository_root()
    sites = find_string_errors(load_files(root))
    if not sites:
        return 0

    # One stream, so a CI log cannot print the rule below the sites it explains.
    print(
        f"error[{RULE}]: `Result<_, String>` is not an error type. Return "
        "`error_stack::Result<T, E>` where `E` is a semantic `thiserror` enum owned by the module "
        'that decides what the failure means (AGENTS.md, "Typed error conversions").'
    )
    for site in sites:
        print(f"  {site.render()}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
