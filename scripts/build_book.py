#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
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


def render_title(version: str) -> str:
    return f"The Nervix Book ({version})"


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


def verify_publication(publication_dir: Path) -> None:
    llms_path = publication_dir / "llms.txt"
    if not llms_path.is_file():
        raise SystemExit("mdBook did not generate llms.txt")
    markdown_dir = publication_dir / "markdown"
    if not markdown_dir.is_dir():
        raise SystemExit("mdBook did not generate Markdown output")

    link_targets = re.findall(r"\]\(([^)]+)\)", llms_path.read_text(encoding="utf-8"))
    if not link_targets:
        raise SystemExit("llms.txt does not contain any documentation links")
    for link_target in link_targets:
        relative_target = Path(link_target.split("#", 1)[0])
        if relative_target.is_absolute() or ".." in relative_target.parts:
            raise SystemExit(f"llms.txt contains an unsafe link: {link_target}")
        if not (publication_dir / relative_target).is_file():
            raise SystemExit(f"llms.txt links to missing output: {link_target}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the Nervix mdBook with an optional version label.")
    parser.add_argument("--version", required=True, help="Version label to embed into the rendered book title")
    args = parser.parse_args()
    if args.version == "":
        raise SystemExit("--version must be non-empty")

    with tempfile.TemporaryDirectory(prefix="nervix-book-") as tmp_dir:
        rendered_dir = Path(tmp_dir) / "rendered"
        publication_dir = Path(tmp_dir) / "publication"
        build_env = os.environ.copy()
        build_env["MDBOOK_BOOK__TITLE"] = json.dumps(render_title(args.version))
        build_env["MDBOOK_OUTPUT__LLMS__VERSION"] = json.dumps(args.version)

        subprocess.run(
            ["mdbook", "build", str(DOCS_DIR), "--dest-dir", str(rendered_dir)],
            check=True,
            cwd=ROOT,
            env=build_env,
        )

        html_dir = rendered_dir / "html"
        markdown_dir = rendered_dir / "markdown"
        llms_path = rendered_dir / "llms" / "llms.txt"
        copy_theme_assets(DOCS_DIR / "theme", html_dir)
        rewrite_external_assets(html_dir)
        shutil.copytree(html_dir, publication_dir)
        shutil.copytree(markdown_dir, publication_dir / "markdown")
        shutil.copy2(llms_path, publication_dir / "llms.txt")
        verify_publication(publication_dir)
        if OUTPUT_DIR.exists():
            shutil.rmtree(OUTPUT_DIR)
        shutil.copytree(publication_dir, OUTPUT_DIR)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
