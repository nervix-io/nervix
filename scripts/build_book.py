#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import tomllib
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
DOCS_DIR = ROOT / "docs"
OUTPUT_DIR = DOCS_DIR / "book"
# The rendered theme and the PDF's in-document links both depend on this exact
# release, so local builds and CI must agree on it. CI reads this constant.
MDBOOK_VERSION = "0.5.3"
MDBOOK_LLMS_COMMAND = ROOT / "scripts" / "mdbook_llms.py"
# Chapters reference images as `images/<name>`, and mdBook copies non-Markdown
# files out of `src` verbatim, so the captured screenshots are staged under this
# name beside the chapters.
IMAGES_DIR_NAME = "images"
# `just docs-screenshots` captures the console images here. They are build output
# rather than repository content, so the published book always shows the console
# it was built from. The recipe passes the same directory to the capture tool, so
# both sides must honour CARGO_TARGET_DIR alike.
SCREENSHOTS_DIR = Path(os.environ.get("CARGO_TARGET_DIR") or ROOT / "target") / "docs-screenshots"
CLI_REFERENCE_NAME = "nervix-cli-reference.md"
CLI_PACKAGE = "nervix-cli"
# Pin the variables clap consults for width and colour. They are inert while the
# `wrap_help` and `color` features are off, and keep the rendered chapter from
# varying with the shell the book was built from once either is enabled.
CLI_HELP_COLUMNS = "96"
# clap lists subcommands two-space indented, name first, then the description
# column. Wrapped description lines are indented past that, so they do not match.
CLI_COMMANDS_HEADING = re.compile(r"^Commands:$", re.MULTILINE)
CLI_COMMAND_ENTRY = re.compile(r"^  (?P<name>[a-z][a-z0-9-]*)(?:\s{2,}\S|\s*$)")
ROTO_UPSTREAM_PATH = "docs/source/reference/language_reference.md"
ROTO_REFERENCE_NAME = "roto-language-reference.md"
ROTO_LICENSE_PATH = "LICENSE"
ROTO_ANCHOR_LINE = re.compile(r"^\((?P<name>[A-Za-z0-9_-]+)\)=\s*$")
ROTO_CODE_ATTRIBUTE = re.compile(
    r"""^\s*\{\s*class\s*=\s*(?:"[^"]*"|'[^']*')(?:\s+[^}]*)?\}\s*$"""
)
ROTO_FENCE_OPEN = re.compile(r"^:{3,}\{(\w+)\}\s*$")
ROTO_FENCE_CLOSE = re.compile(r"^:{3,}\s*$")
# Upstream MyST cross-references, either in-page (`](#anchor)`) or to a Roto
# documentation page Nervix does not bundle (`](generate_cli)`). Both resolve to
# nothing here, so the link text is kept and the target dropped. Relative links
# such as `](udfs.md)` and absolute URLs are left alone.
ROTO_UNRESOLVED_LINK = re.compile(r"\[([^][]+)\]\((?:#[A-Za-z0-9_-]+|[A-Za-z0-9_]+)\)")
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
NERVIX_LOGO_SVG = "theme/nervix-mark.svg"
MDBOOK_FAVICON_SVG = re.compile(
    r'<link rel="icon" href="favicon(?:-[0-9a-f]+)?\.svg">'
)
MDBOOK_FAVICON_PNG = re.compile(
    r'<link rel="shortcut icon" href="favicon(?:-[0-9a-f]+)?\.png">'
)
MDBOOK_FONTS = re.compile(
    r'<link rel="stylesheet" href="fonts/fonts(?:-[0-9a-f]+)?\.css">'
)
MDBOOK_EDIT_LINK = re.compile(
    r'<a href="[^"]*" title="Suggest an edit".*?</a>\s*',
    re.S,
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
    body = ROTO_UNRESOLVED_LINK.sub(r"\1", "\n".join(lines))
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


def stage_book(staging_dir: Path) -> Path:
    """Assemble a complete book root that carries the generated chapters.

    The whole root is staged rather than only redirecting `book.src`, because
    mdBook renders `edit-url-template`'s `{path}` as the configured source
    directory joined with the chapter path. Pointing `book.src` at a temporary
    directory therefore leaks that path into every "Suggest an edit" link,
    while a staged `src` keeps the links pointing at `docs/src` in the
    repository.
    """
    staging_dir.mkdir(parents=True)
    shutil.copy2(DOCS_DIR / "book.toml", staging_dir / "book.toml")
    shutil.copytree(DOCS_DIR / "theme", staging_dir / "theme")
    source_dir = staging_dir / "src"
    shutil.copytree(DOCS_DIR / "src", source_dir)
    generate_upstream_references(source_dir)
    generate_cli_reference(source_dir)
    stage_screenshots(source_dir)
    return staging_dir


def stage_screenshots(source_dir: Path) -> None:
    """Copy the captured console images in beside the chapters that use them."""
    captured = sorted(SCREENSHOTS_DIR.glob("*.png")) if SCREENSHOTS_DIR.is_dir() else []
    if not captured:
        raise SystemExit(
            f"no console screenshots in {SCREENSHOTS_DIR}; run `just docs-screenshots` "
            "to capture them, or `just book` which does it for you"
        )
    staged_images = source_dir / IMAGES_DIR_NAME
    staged_images.mkdir()
    for image in captured:
        shutil.copy2(image, staged_images / image.name)


def capture_cli_help(arguments: list[str]) -> str:
    """Run `nervix-cli [arguments] --help` and return exactly what it printed."""
    env = os.environ.copy()
    env["COLUMNS"] = CLI_HELP_COLUMNS
    env["NO_COLOR"] = "1"
    invocation = ["cargo", "run", "--quiet", "--package", CLI_PACKAGE, "--", *arguments, "--help"]
    try:
        completed = subprocess.run(
            invocation,
            check=False,
            capture_output=True,
            text=True,
            cwd=ROOT,
            env=env,
        )
    except FileNotFoundError as error:
        raise SystemExit(f"cargo is required to render the {CLI_PACKAGE} reference: {error}") from error
    if completed.returncode != 0:
        raise SystemExit(
            f"`{CLI_PACKAGE} {' '.join([*arguments, '--help'])}` failed with exit code "
            f"{completed.returncode}: {completed.stderr.strip()}"
        )
    return completed.stdout.strip("\n")


def cli_subcommands(root_help: str) -> list[str]:
    """Read the subcommand names out of the top-level help.

    Enumerating them rather than listing them here is what keeps a newly added
    subcommand from silently missing its section.
    """
    heading = CLI_COMMANDS_HEADING.search(root_help)
    if heading is None:
        raise SystemExit(f"`{CLI_PACKAGE} --help` no longer prints a Commands section")
    names = []
    for line in root_help[heading.end() :].splitlines():
        if line.strip() == "":
            continue
        if not line.startswith("  "):
            break
        entry = CLI_COMMAND_ENTRY.match(line)
        # `help` documents itself; every real subcommand gets its own section.
        if entry is not None and entry.group("name") != "help":
            names.append(entry.group("name"))
    if not names:
        raise SystemExit(f"`{CLI_PACKAGE} --help` listed no subcommands")
    return names


def render_cli_reference() -> str:
    root_help = capture_cli_help([])
    sections = [f"## nervix-cli\n\n```text\n{root_help}\n```"]
    for name in cli_subcommands(root_help):
        sections.append(f"## nervix-cli {name}\n\n```text\n{capture_cli_help([name])}\n```")
    body = "\n\n".join(sections)
    return f"""# nervix-cli Reference

Every option, subcommand, default, and environment variable `nervix-cli` accepts, printed by the
binary itself while this book was built. It therefore describes exactly the release you are reading
about. For how to use the client, see [Command Line Client](client-tools-cli.md).

{body}
"""


def generate_cli_reference(source_dir: Path) -> None:
    (source_dir / CLI_REFERENCE_NAME).write_text(render_cli_reference(), encoding="utf-8")


def verify_mdbook_version() -> None:
    try:
        reported = subprocess.run(
            ["mdbook", "--version"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    except OSError as error:
        raise SystemExit(f"mdBook {MDBOOK_VERSION} is required but could not be run: {error}")
    except subprocess.CalledProcessError as error:
        raise SystemExit(f"mdbook --version failed: {error}")

    installed = reported.strip().removeprefix("mdbook").strip().removeprefix("v")
    if installed != MDBOOK_VERSION:
        raise SystemExit(
            f"mdBook {MDBOOK_VERSION} is required, found {installed or 'nothing'}. "
            "The rendered theme and the PDF's in-document links depend on this exact release."
        )


def run_mdbook(book_dir: Path, rendered_dir: Path, build_env: dict[str, str]) -> None:
    # mdBook reports a rejected renderer configuration on stderr and still exits
    # zero, which silently renders the book with default settings. Treat any
    # logged error as fatal so a bad `book.toml` cannot ship.
    result = subprocess.run(
        ["mdbook", "build", str(book_dir), "--dest-dir", str(rendered_dir)],
        check=True,
        cwd=ROOT,
        env=build_env,
        stderr=subprocess.PIPE,
        text=True,
    )
    sys.stderr.write(result.stderr)
    errors = [line for line in result.stderr.splitlines() if line.strip().startswith("ERROR")]
    if errors:
        raise SystemExit("mdBook reported errors while building the book:\n" + "\n".join(errors))


def rewrite_external_assets(book_dir: Path) -> None:
    replacements = (
        (MDBOOK_FAVICON_SVG, f'<link rel="icon" href="{NERVIX_LOGO_SVG}">'),
        (MDBOOK_FAVICON_PNG, f'<link rel="shortcut icon" href="{NERVIX_LOGO_SVG}">'),
        (MDBOOK_FONTS, f'<link rel="stylesheet" href="{GOOGLE_FONTS_CSS}">'),
    )
    rewritten = {pattern.pattern: 0 for pattern, _ in replacements}
    for html_file in book_dir.rglob("*.html"):
        content = html_file.read_text(encoding="utf-8")
        for pattern, target in replacements:
            content, count = pattern.subn(target, content)
            rewritten[pattern.pattern] += count
        html_file.write_text(content, encoding="utf-8")

    # A pattern that stops matching, as happened when mdBook began hashing
    # asset filenames, would silently leave the bundled link in place while the
    # bundled directory below is removed, publishing a dead reference.
    unmatched = [pattern for pattern, count in rewritten.items() if count == 0]
    if unmatched:
        raise SystemExit(
            "no rendered page matched these mdBook asset links: " + ", ".join(unmatched)
        )

    bundled_fonts = book_dir / "fonts"
    if bundled_fonts.exists():
        shutil.rmtree(bundled_fonts)


def remove_generated_edit_links(book_dir: Path) -> None:
    """Drop "Suggest an edit" from chapters that have no file to edit.

    The upstream references and the nervix-cli reference are rendered at build
    time, so their source never exists in the repository and mdBook's edit link
    would resolve to nothing.
    """
    for reference_name in (ROTO_REFERENCE_NAME, JAQ_REFERENCE_NAME, CLI_REFERENCE_NAME):
        page = book_dir / f"{Path(reference_name).stem}.html"
        if not page.is_file():
            raise SystemExit(f"mdBook did not render the generated chapter {page.name}")
        content = page.read_text(encoding="utf-8")
        stripped, removed = MDBOOK_EDIT_LINK.subn("", content)
        if removed == 0:
            raise SystemExit(f"{page.name} has no edit link to remove")
        page.write_text(stripped, encoding="utf-8")


def copy_theme_assets(source_theme_dir: Path, book_dir: Path) -> None:
    if not source_theme_dir.exists():
        return
    output_theme_dir = book_dir / "theme"
    output_theme_dir.mkdir(exist_ok=True)
    for source_file in source_theme_dir.iterdir():
        if source_file.is_file():
            shutil.copy2(source_file, output_theme_dir / source_file.name)


def verify_published_images(publication_dir: Path) -> None:
    """Fail when a captured image did not reach the rendered book.

    mdBook copies non-Markdown files out of `src` verbatim. If that ever stops,
    every screenshot in the book would 404 while the build still succeeded.
    """
    for image in sorted(SCREENSHOTS_DIR.glob("*.png")):
        if not (publication_dir / IMAGES_DIR_NAME / image.name).is_file():
            raise SystemExit(
                f"the rendered book is missing the captured image {IMAGES_DIR_NAME}/{image.name}"
            )


def verify_publication(publication_dir: Path) -> None:
    verify_published_images(publication_dir)
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

    verify_mdbook_version()

    with tempfile.TemporaryDirectory(prefix="nervix-book-") as tmp_dir:
        staged_book = Path(tmp_dir) / "book"
        rendered_dir = Path(tmp_dir) / "rendered"
        publication_dir = Path(tmp_dir) / "publication"
        stage_book(staged_book)

        build_env = os.environ.copy()
        build_env["MDBOOK_BOOK__TITLE"] = json.dumps(render_title(args.version))
        # Renderer commands resolve against the book root, which is now staged.
        build_env["MDBOOK_OUTPUT__LLMS__COMMAND"] = json.dumps(str(MDBOOK_LLMS_COMMAND))
        build_env["MDBOOK_OUTPUT__LLMS__VERSION"] = json.dumps(args.version)

        run_mdbook(staged_book, rendered_dir, build_env)

        html_dir = rendered_dir / "html"
        markdown_dir = rendered_dir / "markdown"
        llms_path = rendered_dir / "llms" / "llms.txt"
        copy_theme_assets(DOCS_DIR / "theme", html_dir)
        remove_generated_edit_links(html_dir)
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
