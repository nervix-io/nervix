#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import tempfile
import tomllib
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
DOCS_DIR = ROOT / "docs"
OUTPUT_DIR = DOCS_DIR / "book"
ROTO_UPSTREAM_PATH = "docs/source/reference/language_reference.md"
ROTO_REFERENCE_OUTPUT = DOCS_DIR / "src" / "roto-language-reference.md"
ROTO_ANCHOR_LINE = re.compile(r"^\([A-Za-z0-9_]+\)=\s*$")
ROTO_FENCE_OPEN = re.compile(r"^:{3,}\{(\w+)\}\s*$")
ROTO_FENCE_CLOSE = re.compile(r"^:{3,}\s*$")
ROTO_IN_PAGE_LINK = re.compile(r"\[([^][]+)\]\(#[A-Za-z0-9_-]+\)")
GOOGLE_FONTS_CSS = (
    "https://fonts.googleapis.com/css2"
    "?family=Open+Sans:ital,wght@0,300;0,400;0,600;0,700;0,800;1,300;1,400;1,600;1,700;1,800"
    "&family=Source+Code+Pro:wght@500&display=swap"
)
FONT_AWESOME_CSS = "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css"
NERVIX_LOGO_SVG = "theme/nervix-mark.svg"


def render_title(version: str) -> str:
    return f"The Nervix Book ({version})"


def resolve_roto_version(cargo_lock_text: str) -> str:
    for package in tomllib.loads(cargo_lock_text).get("package", []):
        if package.get("name") == "roto":
            return package["version"]
    raise SystemExit("Cargo.lock does not contain the roto package")


def roto_reference_marker(version: str) -> str:
    return (
        f"<!-- bundled from https://raw.githubusercontent.com/NLnetLabs/roto/v{version}/{ROTO_UPSTREAM_PATH}"
        " by scripts/build_book.py; do not edit -->"
    )


def roto_bundle_is_current(bundled_text: str, version: str) -> bool:
    return roto_reference_marker(version) in bundled_text


def bundle_roto_reference(upstream_text: str, version: str) -> str:
    lines: list[str] = []
    admonition: str | None = None
    for line in upstream_text.splitlines():
        if ROTO_ANCHOR_LINE.match(line):
            continue
        fence_open = ROTO_FENCE_OPEN.match(line)
        if fence_open:
            admonition = fence_open.group(1)
            if admonition == "testoutput":
                lines.extend(["Output:", "", "```text"])
            else:
                lines.append(f"> **{admonition.capitalize()}:**")
            continue
        if admonition is not None and ROTO_FENCE_CLOSE.match(line):
            if admonition == "testoutput":
                lines.append("```")
            admonition = None
            continue
        if admonition is not None and admonition != "testoutput":
            lines.append(f"> {line}".rstrip())
        else:
            lines.append(line)
    body = ROTO_IN_PAGE_LINK.sub(r"\1", "\n".join(lines))
    body = body.replace("# Language Reference", "# Roto Language Reference", 1)

    provenance = (
        "> Bundled from the upstream Roto documentation source\n"
        f"> [`{ROTO_UPSTREAM_PATH}` at tag `v{version}`]"
        f"(https://github.com/NLnetLabs/roto/blob/v{version}/{ROTO_UPSTREAM_PATH})\n"
        "> of [NLnetLabs/roto](https://github.com/NLnetLabs/roto), © NLnet Labs, BSD-3-Clause,\n"
        "> lightly normalized from MyST Markdown. Nervix `ROTO_0_11` UDFs embed this Roto release\n"
        "> line. The column operations available inside a UDF body come from the Nervix catalog in\n"
        "> [User-Defined Functions](udfs.md), not from the Roto standard library."
    )
    title = "# Roto Language Reference"
    body = body.replace(title, f"{title}\n\n{provenance}", 1)
    return f"{roto_reference_marker(version)}\n\n{body}\n"


def ensure_roto_language_reference() -> None:
    version = resolve_roto_version((ROOT / "Cargo.lock").read_text(encoding="utf-8"))
    if ROTO_REFERENCE_OUTPUT.is_file() and roto_bundle_is_current(
        ROTO_REFERENCE_OUTPUT.read_text(encoding="utf-8"), version
    ):
        return
    url = f"https://raw.githubusercontent.com/NLnetLabs/roto/v{version}/{ROTO_UPSTREAM_PATH}"
    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            upstream_text = response.read().decode("utf-8")
    except OSError as error:
        raise SystemExit(f"failed to download the Roto language reference from {url}: {error}")
    ROTO_REFERENCE_OUTPUT.write_text(bundle_roto_reference(upstream_text, version), encoding="utf-8")


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

    ensure_roto_language_reference()

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
