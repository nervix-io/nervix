"""Prepare mdbook's print.html for the pandoc PDF build.

Extracts the ``<main>`` content, removes mdbook's self-referencing heading
anchors, and demotes every heading one level except the H1s of top-level
SUMMARY chapters. With pandoc's ``--top-level-division=chapter`` and the
``report`` document class, the surviving H1s become ``\\chapter`` headings that
start on a fresh page, while nested book chapters render as sections.

Every link is also checked: a reader of the PDF must never be sent to the
website for a page the PDF itself contains, so the only accepted targets are
external URLs and fragments that resolve inside the document.
"""

from __future__ import annotations

import argparse
import html
import re
import sys
import urllib.parse
from pathlib import Path

SUMMARY_TOP_LEVEL = re.compile(r"^- \[(?P<title>[^\]]+)\]\((?P<path>[^)]+)\)\s*$")
HEADER_ANCHOR = re.compile(r'<a class="header" href="[^"]*">(.*?)</a>', re.S)
HEADING = re.compile(r"<h([1-6])([^>]*)>(.*?)</h\1>", re.S)
TAG = re.compile(r"<[^>]+>")
MAIN = re.compile(r"<main>(.*)</main>", re.S)
LINK_HREF = re.compile(r'<a\b[^>]*?\bhref="([^"]*)"', re.I | re.S)
ELEMENT_ID = re.compile(r'\bid="([^"]+)"')
EXTERNAL_URL = re.compile(r"^(?:https?|mailto):", re.I)


def top_level_titles(summary: Path, src_dir: Path) -> set[str]:
    """Return the H1 titles of the chapters listed at SUMMARY's top level."""
    titles: set[str] = set()
    for line in summary.read_text(encoding="utf-8").splitlines():
        match = SUMMARY_TOP_LEVEL.match(line)
        if not match:
            continue
        chapter = src_dir / match.group("path").removeprefix("./")
        for chapter_line in chapter.read_text(encoding="utf-8").splitlines():
            if chapter_line.startswith("# "):
                titles.add(chapter_line[2:].strip())
                break
        else:
            raise SystemExit(f"top-level chapter {chapter} has no H1 title")
    if not titles:
        raise SystemExit(f"no top-level chapters found in {summary}")
    return titles


def heading_text(inner_html: str) -> str:
    return " ".join(html.unescape(TAG.sub("", inner_html)).split())


def fragment_target(href: str) -> str:
    return urllib.parse.unquote(html.unescape(href).removeprefix("#"))


def verify_links(main: str) -> None:
    """Reject links that would leave the document or land nowhere."""
    targets = LINK_HREF.findall(main)
    offsite = sorted(
        {href for href in targets if not href.startswith("#") and not EXTERNAL_URL.match(href)}
    )
    if offsite:
        raise SystemExit(
            "the PDF must link within itself, but these targets point outside the document: "
            + ", ".join(offsite)
        )

    known_ids = {fragment_target(f"#{value}") for value in ELEMENT_ID.findall(main)}
    dangling = sorted(
        {href for href in targets if href.startswith("#") and fragment_target(href) not in known_ids}
    )
    if dangling:
        raise SystemExit(
            "the PDF contains links to missing anchors: " + ", ".join(dangling)
        )


def transform(print_html: str, titles: set[str]) -> str:
    match = MAIN.search(print_html)
    if not match:
        raise SystemExit("failed to extract <main> from print.html")
    main = HEADER_ANCHOR.sub(r"\1", match.group(1))

    def demote(heading: re.Match[str]) -> str:
        level = int(heading.group(1))
        attrs, inner = heading.group(2), heading.group(3)
        if level == 1 and heading_text(inner) in titles:
            new_level = 1
        else:
            new_level = min(level + 1, 6)
        return f"<h{new_level}{attrs}>{inner}</h{new_level}>"

    main = HEADING.sub(demote, main)
    verify_links(main)
    return (
        '<!DOCTYPE html><html><head><meta charset="UTF-8"></head>'
        f"<body><main>{main}</main></body></html>"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--print-html", type=Path, required=True)
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--src-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    titles = top_level_titles(args.summary, args.src_dir)
    result = transform(args.print_html.read_text(encoding="utf-8"), titles)
    args.output.write_text(result, encoding="utf-8")


if __name__ == "__main__":
    sys.exit(main())
