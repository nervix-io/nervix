from __future__ import annotations

import argparse
from pathlib import Path


VERSION_PLACEHOLDER = "@@NERVIX_DOCUMENTATION_VERSION@@"
LATEX_ESCAPES = {
    "\\": r"\textbackslash{}",
    "{": r"\{",
    "}": r"\}",
    "$": r"\$",
    "&": r"\&",
    "#": r"\#",
    "_": r"\_",
    "%": r"\%",
    "~": r"\textasciitilde{}",
    "^": r"\textasciicircum{}",
}


def escape_latex(value: str) -> str:
    return "".join(LATEX_ESCAPES.get(character, character) for character in value)


def render_title_page(template: str, version: str) -> str:
    if not version:
        raise ValueError("documentation version must be non-empty")
    if "\n" in version or "\r" in version:
        raise ValueError("documentation version must be a single line")
    if template.count(VERSION_PLACEHOLDER) != 1:
        raise ValueError("PDF title template must contain exactly one version placeholder")
    return template.replace(VERSION_PLACEHOLDER, escape_latex(version))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Render the Nervix PDF title page with its documentation version."
    )
    parser.add_argument("--template", required=True, type=Path)
    parser.add_argument("--version", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    try:
        rendered = render_title_page(
            args.template.read_text(encoding="utf-8"),
            args.version,
        )
    except ValueError as error:
        parser.error(str(error))
    args.output.write_text(rendered, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
