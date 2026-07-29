"""Prepare mdbook's print.html for the pandoc PDF build.

Extracts the ``<main>`` content, removes mdbook's self-referencing heading
anchors, and applies the chapter hierarchy from SUMMARY.md to the page H1s.
With pandoc's ``--top-level-division=chapter`` and the ``report`` document
class, top-level H1s become ``\\chapter`` headings that start on a fresh page,
while nested book chapters render as sections.

Every link is then checked: a reader of the PDF must never be sent to the
website for a page the PDF itself contains, so the only targets left standing
are external URLs and fragments that resolve inside the document.
"""

from __future__ import annotations

import argparse
import html
import re
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit

SUMMARY_CHAPTER = re.compile(
    r"^(?P<indent>\s*)- \[(?P<title>[^\]]+)\]\((?P<path>[^)]+)\)\s*$"
)
HEADER_ANCHOR = re.compile(r'<a class="header" href="[^"]*">(.*?)</a>', re.S)
HEADING = re.compile(r"<h([1-6])([^>]*)>(.*?)</h\1>", re.S)
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
ELEMENT_ID = re.compile(r'\bid="([^"]+)"')
EXTERNAL_URL = re.compile(r"^(?:https?|mailto):", re.I)
PDF_GLYPH_SUBSTITUTIONS = {
    "⟼": "=&gt;",
    "☀️": "[U+2600]",
    "😀": "[U+1F600]",
    "🙂": "[U+1F642]",
    "🔬": "[U+1F52C]",
    "🤔": "[U+1F914]",
    "🧑": "[U+1F9D1]",
}


def chapter_hierarchy(summary: Path) -> list[bool]:
    """Return whether each SUMMARY chapter is at the top level."""
    hierarchy: list[bool] = []
    for line in summary.read_text(encoding="utf-8").splitlines():
        match = SUMMARY_CHAPTER.match(line)
        if match:
            hierarchy.append(not match.group("indent"))
    if not hierarchy:
        raise SystemExit(f"no chapters found in {summary}")
    if not any(hierarchy):
        raise SystemExit(f"no top-level chapters found in {summary}")
    return hierarchy


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


def fragment_target(href: str) -> str:
    return unquote(html.unescape(href).removeprefix("#"))


def verify_links(main_html: str) -> None:
    """Reject links that would leave the document or land nowhere."""
    targets = [link.group("target") for link in HREF.finditer(main_html)]
    offsite = sorted(
        {href for href in targets if not href.startswith("#") and not EXTERNAL_URL.match(href)}
    )
    if offsite:
        raise SystemExit(
            "the PDF must link within itself, but these targets point outside the document: "
            + ", ".join(offsite)
        )

    known_ids = {fragment_target(f"#{value}") for value in ELEMENT_ID.findall(main_html)}
    dangling = sorted(
        {href for href in targets if href.startswith("#") and fragment_target(href) not in known_ids}
    )
    if dangling:
        raise SystemExit("the PDF contains links to missing anchors: " + ", ".join(dangling))


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


def transform(
    print_html: str,
    hierarchy: list[bool],
) -> str:
    match = MAIN.search(print_html)
    if not match:
        raise SystemExit("failed to extract <main> from print.html")
    main = HEADER_ANCHOR.sub(r"\1", match.group(1))
    chapter_index = 0
    main = rewrite_pdf_links(main)
    main = substitute_unsupported_pdf_glyphs(main)
    main = add_pdf_table_column_widths(main)

    def demote(heading: re.Match[str]) -> str:
        nonlocal chapter_index
        level = int(heading.group(1))
        attrs, inner = heading.group(2), heading.group(3)
        if level == 1:
            if chapter_index >= len(hierarchy):
                raise SystemExit("print.html contains more H1 chapters than SUMMARY.md")
            top_level = hierarchy[chapter_index]
            chapter_index += 1
            new_level = 1 if top_level else 2
        else:
            new_level = min(level + 1, 6)
        return f"<h{new_level}{attrs}>{inner}</h{new_level}>"

    main = HEADING.sub(demote, main)
    if chapter_index != len(hierarchy):
        raise SystemExit(
            f"print.html contains {chapter_index} H1 chapters, "
            f"but SUMMARY.md contains {len(hierarchy)}"
        )
    verify_links(main)
    return (
        '<!DOCTYPE html><html><head><meta charset="UTF-8"></head>'
        f"<body><main>{main}</main></body></html>"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--print-html", type=Path, required=True)
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    hierarchy = chapter_hierarchy(args.summary)
    result = transform(args.print_html.read_text(encoding="utf-8"), hierarchy)
    args.output.write_text(result, encoding="utf-8")


if __name__ == "__main__":
    sys.exit(main())
