"""Prepare mdbook's print.html for the pandoc PDF build.

Extracts the ``<main>`` content, removes mdbook's self-referencing heading
anchors, and demotes every heading one level except the H1s of top-level
SUMMARY chapters. With pandoc's ``--top-level-division=chapter`` and the
``report`` document class, the surviving H1s become ``\\chapter`` headings that
start on a fresh page, while nested book chapters render as sections.
"""

from __future__ import annotations

import argparse
import html
import re
import sys
from pathlib import Path
from urllib.parse import urlsplit

SUMMARY_TOP_LEVEL = re.compile(r"^- \[(?P<title>[^\]]+)\]\((?P<path>[^)]+)\)\s*$")
HEADER_ANCHOR = re.compile(r'<a class="header" href="[^"]*">(.*?)</a>', re.S)
HEADING = re.compile(r"<h([1-6])([^>]*)>(.*?)</h\1>", re.S)
TAG = re.compile(r"<[^>]+>")
MAIN = re.compile(r"<main>(.*)</main>", re.S)
TABLE = re.compile(
    r"(?P<open><table\b[^>]*>)(?P<body>.*?)(?P<close></table>)",
    re.I | re.S,
)
TABLE_ROW = re.compile(r"<tr\b[^>]*>.*?</tr>", re.I | re.S)
TABLE_CELL = re.compile(r"<t[hd]\b[^>]*>", re.I)
TABLE_COLGROUP = re.compile(r"<colgroup\b", re.I)
HREF = re.compile(
    r"(?P<prefix>\bhref=)(?P<quote>[\"'])(?P<target>[^\"']+)(?P=quote)",
    re.I,
)
PDF_GLYPH_SUBSTITUTIONS = {
    "⟼": "=&gt;",
    "☀️": "[U+2600]",
    "😀": "[U+1F600]",
    "🙂": "[U+1F642]",
    "🔬": "[U+1F52C]",
    "🤔": "[U+1F914]",
    "🧑": "[U+1F9D1]",
}


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


def rewrite_pdf_links(main_html: str) -> str:
    """Point links to chapters in the combined book at local PDF destinations."""

    def rewrite(link: re.Match[str]) -> str:
        target = html.unescape(link.group("target"))
        parsed = urlsplit(target)
        is_relative_chapter = (
            parsed.scheme == ""
            and parsed.netloc == ""
            and parsed.path.removeprefix("./").endswith(".html")
        )
        is_published_chapter = parsed.hostname == "docs.nervix.io"
        if parsed.fragment and (is_relative_chapter or is_published_chapter):
            target = f"#{parsed.fragment}"
        return (
            f'{link.group("prefix")}{link.group("quote")}'
            f'{html.escape(target, quote=True)}{link.group("quote")}'
        )

    return HREF.sub(rewrite, main_html)


def substitute_unsupported_pdf_glyphs(main_html: str) -> str:
    """Keep upstream examples legible with the fonts available to XeLaTeX."""
    for glyph, replacement in PDF_GLYPH_SUBSTITUTIONS.items():
        main_html = main_html.replace(glyph, replacement)
    return main_html


def add_pdf_table_column_widths(main_html: str) -> str:
    """Give pandoc bounded columns so longtable content wraps at print width."""

    def add_columns(table: re.Match[str]) -> str:
        body = table.group("body")
        if TABLE_COLGROUP.search(body):
            return table.group(0)
        first_row = TABLE_ROW.search(body)
        column_count = len(TABLE_CELL.findall(first_row.group(0))) if first_row else 0
        if column_count == 0:
            raise SystemExit("PDF source contains a table without any cells")
        width = 100 / column_count
        columns = "".join(
            f'<col style="width: {width:.6f}%">' for _ in range(column_count)
        )
        return (
            f'{table.group("open")}<colgroup>{columns}</colgroup>'
            f'{body}{table.group("close")}'
        )

    return TABLE.sub(add_columns, main_html)


def transform(print_html: str, titles: set[str]) -> str:
    match = MAIN.search(print_html)
    if not match:
        raise SystemExit("failed to extract <main> from print.html")
    main = HEADER_ANCHOR.sub(r"\1", match.group(1))
    main = rewrite_pdf_links(main)
    main = substitute_unsupported_pdf_glyphs(main)
    main = add_pdf_table_column_widths(main)

    def demote(heading: re.Match[str]) -> str:
        level = int(heading.group(1))
        attrs, inner = heading.group(2), heading.group(3)
        if level == 1 and heading_text(inner) in titles:
            new_level = 1
        else:
            new_level = min(level + 1, 6)
        return f"<h{new_level}{attrs}>{inner}</h{new_level}>"

    main = HEADING.sub(demote, main)
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
