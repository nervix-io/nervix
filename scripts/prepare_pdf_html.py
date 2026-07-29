"""Prepare mdbook's print.html for the pandoc PDF build.

Extracts the ``<main>`` content, removes mdbook's self-referencing heading
anchors, and applies the chapter hierarchy from SUMMARY.md to the page H1s.
With pandoc's ``--top-level-division=chapter`` and the ``report`` document
class, top-level H1s become ``\\chapter`` headings that start on a fresh page,
while nested book chapters render as sections.
"""

from __future__ import annotations

import argparse
import html
import re
import sys
from pathlib import Path

SUMMARY_CHAPTER = re.compile(
    r"^(?P<indent>\s*)- \[(?P<title>[^\]]+)\]\((?P<path>[^)]+)\)\s*$"
)
HEADER_ANCHOR = re.compile(r'<a class="header" href="[^"]*">(.*?)</a>', re.S)
HEADING = re.compile(r"<h([1-6])([^>]*)>(.*?)</h\1>", re.S)
TAG = re.compile(r"<[^>]+>")
MAIN = re.compile(r"<main>(.*)</main>", re.S)


def chapter_headings(summary: Path, src_dir: Path) -> list[tuple[str, bool]]:
    """Return every chapter H1 and whether SUMMARY places it at the top level."""
    chapters: list[tuple[str, bool]] = []
    for line in summary.read_text(encoding="utf-8").splitlines():
        match = SUMMARY_CHAPTER.match(line)
        if not match:
            continue
        chapter = src_dir / match.group("path").removeprefix("./")
        for chapter_line in chapter.read_text(encoding="utf-8").splitlines():
            if chapter_line.startswith("# "):
                chapters.append(
                    (chapter_line[2:].strip(), not match.group("indent"))
                )
                break
        else:
            raise SystemExit(f"chapter {chapter} has no H1 title")
    if not chapters:
        raise SystemExit(f"no chapters found in {summary}")
    if not any(top_level for _, top_level in chapters):
        raise SystemExit(f"no top-level chapters found in {summary}")
    return chapters


def heading_text(inner_html: str) -> str:
    return " ".join(html.unescape(TAG.sub("", inner_html)).split())


def transform(
    print_html: str,
    chapters: list[tuple[str, bool]],
) -> str:
    match = MAIN.search(print_html)
    if not match:
        raise SystemExit("failed to extract <main> from print.html")
    main = HEADER_ANCHOR.sub(r"\1", match.group(1))
    chapter_index = 0

    def demote(heading: re.Match[str]) -> str:
        nonlocal chapter_index
        level = int(heading.group(1))
        attrs, inner = heading.group(2), heading.group(3)
        if level == 1:
            if chapter_index >= len(chapters):
                raise SystemExit("print.html contains more H1 chapters than SUMMARY.md")
            expected_title, top_level = chapters[chapter_index]
            actual_title = heading_text(inner)
            if actual_title != expected_title:
                raise SystemExit(
                    "print.html chapter "
                    f"{chapter_index + 1} is {actual_title!r}, "
                    f"expected {expected_title!r} from SUMMARY.md"
                )
            chapter_index += 1
            new_level = 1 if top_level else 2
        else:
            new_level = min(level + 1, 6)
        return f"<h{new_level}{attrs}>{inner}</h{new_level}>"

    main = HEADING.sub(demote, main)
    if chapter_index != len(chapters):
        raise SystemExit(
            f"print.html contains {chapter_index} H1 chapters, "
            f"but SUMMARY.md contains {len(chapters)}"
        )
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

    chapters = chapter_headings(args.summary, args.src_dir)
    result = transform(args.print_html.read_text(encoding="utf-8"), chapters)
    args.output.write_text(result, encoding="utf-8")


if __name__ == "__main__":
    sys.exit(main())
