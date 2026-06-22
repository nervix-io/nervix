#!/usr/bin/env python3

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
DOCS_DIR = ROOT / "docs"
OUTPUT_DIR = DOCS_DIR / "book"
GOOGLE_FONTS_CSS = (
    "https://fonts.googleapis.com/css2"
    "?family=Open+Sans:ital,wght@0,300;0,400;0,600;0,700;0,800;1,300;1,400;1,600;1,700;1,800"
    "&family=Source+Code+Pro:wght@500&display=swap"
)
FONT_AWESOME_CSS = "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css"
NERVIX_LOGO_SVG = "theme/nervix-mark.svg"


def render_title(version: str | None) -> str:
    if version is None or version == "":
        return "The Nervix Book"
    return f"The Nervix Book ({version})"


def patch_book_toml(content: str, version: str | None) -> str:
    title = render_title(version)
    original_match = re.search(r'^title = "(.*)"$', content, flags=re.MULTILINE)
    if original_match is None:
        raise SystemExit("failed to locate title in docs/book.toml")
    if original_match.group(1) == title:
        return content
    patched = re.sub(
        r'^title = ".*"$',
        f'title = "{title}"',
        content,
        count=1,
        flags=re.MULTILINE,
    )
    return patched


def rewrite_external_assets(book_dir: Path) -> None:
    html_files = list(book_dir.rglob("*.html"))
    for html_file in html_files:
        content = html_file.read_text(encoding="utf-8")
        content = content.replace(
            '<link rel="icon" href="favicon.svg">',
            f'<link rel="icon" href="{NERVIX_LOGO_SVG}">',
        )
        content = content.replace(
            '<link rel="shortcut icon" href="favicon.png">',
            f'<link rel="shortcut icon" href="{NERVIX_LOGO_SVG}">',
        )
        content = content.replace(
            '<link rel="stylesheet" href="FontAwesome/css/font-awesome.css">',
            f'<link rel="stylesheet" href="{FONT_AWESOME_CSS}">',
        )
        content = content.replace(
            '<link rel="stylesheet" href="fonts/fonts.css">',
            f'<link rel="stylesheet" href="{GOOGLE_FONTS_CSS}">',
        )
        html_file.write_text(content, encoding="utf-8")

    for bundled_dir in (book_dir / "fonts", book_dir / "FontAwesome"):
        if bundled_dir.exists():
            shutil.rmtree(bundled_dir)


def copy_theme_assets(source_theme_dir: Path, book_dir: Path) -> None:
    if not source_theme_dir.exists():
        return
    output_theme_dir = book_dir / "theme"
    output_theme_dir.mkdir(exist_ok=True)
    for source_file in source_theme_dir.iterdir():
        if source_file.is_file():
            shutil.copy2(source_file, output_theme_dir / source_file.name)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the Nervix mdBook with an optional version label.")
    parser.add_argument("--version", required=True, help="Version label to embed into the rendered book title")
    args = parser.parse_args()
    if args.version == "":
        raise SystemExit("--version must be non-empty")

    with tempfile.TemporaryDirectory(prefix="nervix-book-") as tmp_dir:
        temp_docs_dir = Path(tmp_dir) / "docs"
        shutil.copytree(DOCS_DIR, temp_docs_dir)

        temp_book_toml = temp_docs_dir / "book.toml"
        temp_book_toml.write_text(
            patch_book_toml(temp_book_toml.read_text(), args.version),
            encoding="utf-8",
        )

        subprocess.run(
            ["mdbook", "build", str(temp_docs_dir)],
            check=True,
            cwd=ROOT,
        )

        built_output_dir = temp_docs_dir / "book"
        copy_theme_assets(temp_docs_dir / "theme", built_output_dir)
        rewrite_external_assets(built_output_dir)
        if OUTPUT_DIR.exists():
            shutil.rmtree(OUTPUT_DIR)
        shutil.copytree(built_output_dir, OUTPUT_DIR)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
