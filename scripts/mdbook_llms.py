#!/usr/bin/env python3

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import quote


def iter_chapters(items: list[dict[str, Any]]) -> Iterator[dict[str, Any]]:
    for item in items:
        chapter = item.get("Chapter")
        if chapter is None:
            continue
        yield chapter
        yield from iter_chapters(chapter.get("sub_items", []))


def escape_label(label: str) -> str:
    return label.replace("\\", "\\\\").replace("[", "\\[").replace("]", "\\]")


def render_llms(context: dict[str, Any]) -> str:
    llms_config = context.get("config", {}).get("output", {}).get("llms", {})
    included_paths = llms_config.get("include", [])
    if not isinstance(included_paths, list) or not all(
        isinstance(path, str) for path in included_paths
    ):
        raise ValueError("output.llms.include must be a list of chapter paths")

    book = context.get("book", {})
    book_items = book.get("sections", book.get("items", []))
    chapters = {
        chapter["path"]: chapter
        for chapter in iter_chapters(book_items)
        if chapter.get("path") is not None
    }
    missing_paths = [path for path in included_paths if path not in chapters]
    if missing_paths:
        missing = ", ".join(missing_paths)
        raise ValueError(f"output.llms.include references missing chapters: {missing}")

    title = llms_config.get("title", "Nervix NSPL Documentation")
    version = llms_config.get("version")
    if version:
        title = f"{title} ({version})"
    description = llms_config.get(
        "description", "Public NSPL configuration reference for Nervix."
    )

    lines = [
        f"# {title}",
        "",
        f"> {description}",
        "",
        "Every link below belongs to this immutable documentation version.",
        "",
        "## Public NSPL Reference",
        "",
    ]
    for path in included_paths:
        chapter = chapters[path]
        encoded_path = quote(path, safe="/-._~")
        lines.append(f"- [{escape_label(chapter['name'])}](markdown/{encoded_path})")

    return "\n".join(lines) + "\n"


def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] == "supports":
        return 0 if len(sys.argv) == 3 and sys.argv[2] == "llms" else 1

    context = json.load(sys.stdin)
    output = render_llms(context)
    destination = Path(context["destination"])
    destination.mkdir(parents=True, exist_ok=True)
    (destination / "llms.txt").write_text(output, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
