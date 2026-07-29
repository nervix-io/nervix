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
ROTO_REFERENCE_NAME = "roto-language-reference.md"
ROTO_LICENSE_PATH = "LICENSE"
ROTO_ANCHOR_LINE = re.compile(r"^\((?P<name>[A-Za-z0-9_-]+)\)=\s*$")
ROTO_CODE_ATTRIBUTE = re.compile(
    r"""^\s*\{\s*class\s*=\s*(?:"[^"]*"|'[^']*')(?:\s+[^}]*)?\}\s*$"""
)
ROTO_FENCE_OPEN = re.compile(r"^:{3,}\{(\w+)\}\s*$")
ROTO_FENCE_CLOSE = re.compile(r"^:{3,}\s*$")
ROTO_IN_PAGE_LINK = re.compile(r"\[([^][]+)\]\(#[A-Za-z0-9_-]+\)")
ROTO_ROLE = re.compile(r"\{roto:ref\}`([^`]+)`")
ROTO_INTERPRETED_REF = re.compile(r'`([^`]+)`\{\.interpreted-text\s+role="ref"\}')
ROTO_DOC_ROLE = re.compile(r"\{doc\}`([^`]+)`")
ROTO_UNHANDLED_MYST = re.compile(
    r"\{(?:roto:ref|doc)\}`|interpreted-text|^\s*\{\s*class\s*=",
    re.M,
)
ROTO_ROLE_TARGETS = {
    "bool": "lang_booleans",
    "u8": "lang_integers",
    "u16": "lang_integers",
    "u32": "lang_integers",
    "u64": "lang_integers",
    "i8": "lang_integers",
    "i16": "lang_integers",
    "i32": "lang_integers",
    "i64": "lang_integers",
    "f32": "lang_floats",
    "f64": "lang_floats",
    "char": "lang_char",
    "String": "lang_strings",
    "String.contains": "lang_strings",
    "List[T]": "lang_lists",
}
JAQ_REFERENCE_NAME = "jaq-reference.md"
JAQ_MANUAL_URL = "https://gedenkt.at/jaq/manual/"
UPSTREAM_USER_AGENT = "nervix-docs-builder"
GOOGLE_FONTS_CSS = (
    "https://fonts.googleapis.com/css2"
    "?family=Open+Sans:ital,wght@0,300;0,400;0,600;0,700;0,800;1,300;1,400;1,600;1,700;1,800"
    "&family=Source+Code+Pro:wght@500&display=swap"
)
FONT_AWESOME_CSS = "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css"
NERVIX_LOGO_SVG = "theme/nervix-mark.svg"
MDBOOK_FAVICON_SVG = re.compile(
    r'<link rel="icon" href="favicon(?:-[0-9a-f]+)?\.svg">'
)
MDBOOK_FAVICON_PNG = re.compile(
    r'<link rel="shortcut icon" href="favicon(?:-[0-9a-f]+)?\.png">'
)
MDBOOK_FONT_AWESOME = re.compile(
    r'<link rel="stylesheet" href="FontAwesome/css/font-awesome'
    r'(?:-[0-9a-f]+)?\.css">'
)
MDBOOK_FONTS = re.compile(
    r'<link rel="stylesheet" href="fonts/fonts(?:-[0-9a-f]+)?\.css">'
)


def render_title(version: str) -> str:
    return f"The Nervix Book ({version})"


def resolve_package_version(cargo_lock_text: str, package_name: str) -> str:
    for package in tomllib.loads(cargo_lock_text).get("package", []):
        if package.get("name") == package_name:
            return package["version"]
    raise SystemExit(f"Cargo.lock does not contain the {package_name} package")


def roto_reference_marker(version: str) -> str:
    return (
        f"<!-- generated from https://raw.githubusercontent.com/NLnetLabs/roto/v{version}/{ROTO_UPSTREAM_PATH}"
        " by scripts/build_book.py; do not edit -->"
    )


def roto_role(role: re.Match[str]) -> str:
    name = role.group(1)
    target = ROTO_ROLE_TARGETS.get(name)
    if target is None:
        return f"`{name}`"
    return f"[`{name}`](#roto-{target})"


def roto_interpreted_ref(role: re.Match[str]) -> str:
    name = role.group(1)
    if name == "lang_optionals":
        return "[optional values](#roto-lang_optionals)"
    return name.replace("_", " ")


def roto_doc_role(role: re.Match[str]) -> str:
    if role.group(1) == "std/index":
        return "[Nervix Roto column operations](udfs.md#roto-column-operations)"
    return f"`{role.group(1)}`"


def upstream_license(title: str, license_text: str) -> str:
    return f"## {title}\n\n```text\n{license_text.rstrip()}\n```"


def bundle_roto_reference(upstream_text: str, license_text: str, version: str) -> str:
    lines: list[str] = []
    admonition: str | None = None
    for line in upstream_text.splitlines():
        anchor = ROTO_ANCHOR_LINE.match(line)
        if anchor:
            lines.append(f'<a id="roto-{anchor.group("name")}"></a>')
            continue
        if ROTO_CODE_ATTRIBUTE.match(line):
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
    body = ROTO_ROLE.sub(roto_role, body)
    body = ROTO_INTERPRETED_REF.sub(roto_interpreted_ref, body)
    body = ROTO_DOC_ROLE.sub(roto_doc_role, body)
    body = body.replace("# Language Reference", "# Roto Language Reference", 1)
    artifact = ROTO_UNHANDLED_MYST.search(body)
    if artifact:
        raise SystemExit(
            f"Roto {version} reference contains an unhandled MyST artifact: "
            f"{artifact.group(0)!r}"
        )

    provenance = (
        "> Generated at book-build time from the upstream Roto documentation source\n"
        f"> [`{ROTO_UPSTREAM_PATH}` at tag `v{version}`]"
        f"(https://github.com/NLnetLabs/roto/blob/v{version}/{ROTO_UPSTREAM_PATH})\n"
        "> of [NLnetLabs/roto](https://github.com/NLnetLabs/roto), © NLnet Labs, BSD-3-Clause,\n"
        "> lightly normalized from MyST Markdown. Nervix UDFs embed this exact Roto package release.\n"
        "> The column operations available inside a UDF body come from the Nervix catalog in\n"
        "> [User-Defined Functions](udfs.md), not from the Roto standard library."
    )
    title = "# Roto Language Reference"
    body = body.replace(title, f"{title}\n\n{provenance}", 1)
    license_section = upstream_license("Upstream Roto License", license_text)
    return f"{roto_reference_marker(version)}\n\n{body}\n\n{license_section}\n"


def render_jaq_reference(version: str) -> str:
    release_url = f"https://github.com/01mf02/jaq/releases/tag/v{version}"
    return f"""<!-- generated by scripts/build_book.py; do not edit -->

# JAQ Reference

Nervix embeds `jaq-core` {version}. For the complete language and standard-library catalogue, read
the human-readable [upstream JAQ manual]({JAQ_MANUAL_URL}). The exact embedded version corresponds
to the [JAQ v{version} release]({release_url}). This page only summarizes the Nervix boundary.

## Nervix Contract

- A codec transform receives one parsed native-format or protobuf JSON value as `.`.
- An ingestion transform must yield exactly one JSON object compatible with the internal schema.
- An emitting transform must yield exactly one value accepted by the selected native format or
  protobuf message.
- Nervix compiles filters with JAQ's core, standard-library, JSON, and format functions. Standalone
  JAQ command-line options and file loading are not part of the codec contract.

See [JAQ transformations](schemas-and-codecs.md#jaq-transformations) for directionality, supported
formats, exact output rules, and NSPL syntax.

## Common Filters

| Filter | Purpose |
|---|---|
| `.` | Preserve the current value |
| `.field`, `.nested.field` | Read object fields |
| `{{field: .source}}` | Construct an object |
| `.items \\| map(.value)` | Transform every array element |
| `.items[] \\| select(.enabled)` | Select matching elements |
| `tonumber`, `tostring` | Explicitly convert JAQ values |
| `//` | Supply an alternative value |
"""


def download_text(url: str, description: str) -> str:
    request = urllib.request.Request(url, headers={"User-Agent": UPSTREAM_USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.read().decode("utf-8")
    except (OSError, UnicodeError) as error:
        raise SystemExit(f"failed to download {description} from {url}: {error}")


def generate_upstream_references(source_dir: Path) -> None:
    cargo_lock_text = (ROOT / "Cargo.lock").read_text(encoding="utf-8")

    roto_version = resolve_package_version(cargo_lock_text, "roto")
    roto_base_url = f"https://raw.githubusercontent.com/NLnetLabs/roto/v{roto_version}"
    roto_source_url = f"{roto_base_url}/{ROTO_UPSTREAM_PATH}"
    roto_license_url = f"{roto_base_url}/{ROTO_LICENSE_PATH}"
    roto_source = download_text(roto_source_url, "the Roto language reference")
    roto_license = download_text(roto_license_url, "the Roto license")
    (source_dir / ROTO_REFERENCE_NAME).write_text(
        bundle_roto_reference(roto_source, roto_license, roto_version),
        encoding="utf-8",
    )

    jaq_version = resolve_package_version(cargo_lock_text, "jaq-core")
    (source_dir / JAQ_REFERENCE_NAME).write_text(
        render_jaq_reference(jaq_version),
        encoding="utf-8",
    )


def rewrite_external_assets(book_dir: Path) -> None:
    html_files = list(book_dir.rglob("*.html"))
    for html_file in html_files:
        content = html_file.read_text(encoding="utf-8")
        content = MDBOOK_FAVICON_SVG.sub(
            f'<link rel="icon" href="{NERVIX_LOGO_SVG}">',
            content,
        )
        content = MDBOOK_FAVICON_PNG.sub(
            f'<link rel="shortcut icon" href="{NERVIX_LOGO_SVG}">',
            content,
        )
        content = MDBOOK_FONT_AWESOME.sub(
            f'<link rel="stylesheet" href="{FONT_AWESOME_CSS}">',
            content,
        )
        content = MDBOOK_FONTS.sub(
            f'<link rel="stylesheet" href="{GOOGLE_FONTS_CSS}">',
            content,
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
        source_dir = Path(tmp_dir) / "source"
        rendered_dir = Path(tmp_dir) / "rendered"
        publication_dir = Path(tmp_dir) / "publication"
        shutil.copytree(DOCS_DIR / "src", source_dir)
        generate_upstream_references(source_dir)

        build_env = os.environ.copy()
        build_env["MDBOOK_BOOK__SRC"] = json.dumps(str(source_dir))
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
