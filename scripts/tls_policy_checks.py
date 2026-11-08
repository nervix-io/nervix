"""Focused outward TLS publication evidence through the canonical Shuttle runner.

The complete inventory is parsed first; only this production owner's records are explored locally.
The ordinary CI Shuttle lane still discovers and qualifies the complete inventory.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from scripts import shuttle_checks

PACKAGE = "nervix-tls"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-dir", type=Path, default=Path("target/outward-tls-models"))
    arguments = parser.parse_args()
    arguments.target_dir.mkdir(parents=True, exist_ok=True)
    root = Path.cwd()
    complete = shuttle_checks.parse_inventory((root / shuttle_checks.INVENTORY).read_text())
    selected = shuttle_checks.Inventory({PACKAGE: complete.checks[PACKAGE]})
    return shuttle_checks.run_checks(
        shuttle_checks.Commands(root),
        selected,
        arguments.target_dir.resolve(),
        "shuttle_capture_racing_publication_",
        arguments.target_dir.resolve() / "models.json",
    )


if __name__ == "__main__":
    raise SystemExit(main())
