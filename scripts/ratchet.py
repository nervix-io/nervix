#!/usr/bin/env python3

"""Count the architecture debt the layered rework retires, and hold it down.

Every count in `debt-baseline.json` is a ratchet: a change may lower it and never raise it. The
script prints each count next to its baseline and exits non-zero when any count is above it,
naming the offending counts. A count that has fallen is reported too, with the reminder that
`--update` rewrites the baseline in the same change.

Counting is textual. Comments and literal contents are blanked before matching, so a pattern
written in a doc comment or an NSPL fixture string is never counted, and `#[cfg(test)]` items are
blanked so unit tests never hold a production count up. Blanking preserves byte offsets, so spans
found in one view of a file address the same bytes in another.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from bisect import bisect_left
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Callable, Iterable, Iterator, Sequence

BASELINE_FILE = "debt-baseline.json"

LARGE_FILE_LINES = 3000

# Directories whose Rust files are tests or harnesses rather than the product.
TEST_DIRECTORIES = frozenset({"tests", "benches", "examples"})

# In-crate test files. They still count toward file size, because a test file that has grown past
# `LARGE_FILE_LINES` is split like any other, but their bodies never hold a production count up.
TEST_FILE_SUFFIXES = ("/tests.rs", "_tests.rs")

# The parser is an edge dependency: only the language crate itself, the client tools, and the
# session adapter may name it. Everything else consumes Models.
PARSER_EDGES = (
    "crates/nspl/",
    "crates/nspl-format/",
    "crates/client-core/",
    "crates/nervix-cli/",
    "crates/web-console/",
    "src/application.rs",
)

# The data plane executes plans. `planning.rs` is where a Model is still allowed to be read.
DATA_PLANE = "src/runtime/"
DATA_PLANE_PLANNER = "src/runtime/planning.rs"


@dataclass(frozen=True)
class Site:
    """One counted occurrence, addressed for a developer who has to go remove it."""

    path: str
    line: int
    detail: str

    def render(self) -> str:
        return f"{self.path}:{self.line}: {self.detail}"


class RustFile:
    """A tracked Rust file in the three views the counts need.

    `code` has comments and literal contents blanked, `literals` keeps literal contents so
    attribute values stay readable, and `product` additionally blanks `#[cfg(test)]` items. All
    three have the same length as the source, so an offset means the same byte in each.
    """

    def __init__(self, path: str, source: str) -> None:
        self.path = path
        self.source = source
        self.lines = source.count("\n")
        comments, literals = scan_spans(source)
        self.literals = blank(source, comments)
        self.code = blank(self.literals, literals)
        self.product = blank(self.code, cfg_test_spans(self.code))
        self._newlines = [index for index, char in enumerate(source) if char == "\n"]

    def line_of(self, offset: int) -> int:
        return bisect_left(self._newlines, offset) + 1

    @property
    def is_product(self) -> bool:
        return not any(self.path.endswith(suffix) for suffix in TEST_FILE_SUFFIXES)

    def site(self, offset: int, detail: str) -> Site:
        return Site(self.path, self.line_of(offset), " ".join(detail.split()))

    def source_line(self, offset: int) -> str:
        start = self.source.rfind("\n", 0, offset) + 1
        end = self.source.find("\n", offset)
        return self.source[start : end if end >= 0 else len(self.source)].strip()


def blank(text: str, spans: Iterable[tuple[int, int]]) -> str:
    """Replace each span with spaces, keeping newlines so offsets and line numbers survive."""

    characters = list(text)
    for start, end in spans:
        for index in range(start, min(end, len(characters))):
            if characters[index] != "\n":
                characters[index] = " "
    return "".join(characters)


_SCAN = re.compile(r"""//|/\*|(?<![A-Za-z0-9_])b?r(?P<hashes>\#*)"|"|'""")


def scan_spans(source: str) -> tuple[list[tuple[int, int]], list[tuple[int, int]]]:
    """Return the comment spans and the literal-content spans of a Rust source file.

    Comment spans cover the delimiters; literal spans cover only the content between them, so a
    blanked string is still recognisable as a string.
    """

    comments: list[tuple[int, int]] = []
    literals: list[tuple[int, int]] = []
    length = len(source)
    index = 0
    while index < length:
        match = _SCAN.search(source, index)
        if match is None:
            break
        start = match.start()
        token = match.group()
        if token == "//":
            end = source.find("\n", start)
            comments.append((start, length if end < 0 else end))
            index = length if end < 0 else end
        elif token == "/*":
            index = _end_of_block_comment(source, start)
            comments.append((start, index))
        elif match.group("hashes") is not None:
            terminator = '"' + match.group("hashes")
            end = source.find(terminator, match.end())
            end = length if end < 0 else end
            literals.append((match.end(), end))
            index = min(length, end + len(terminator))
        elif token == '"':
            end = _end_of_string(source, start)
            literals.append((start + 1, end - 1))
            index = end
        else:
            index = _skip_quote(source, start, literals)
    return comments, literals


def _end_of_block_comment(source: str, start: int) -> int:
    depth = 1
    index = start + 2
    length = len(source)
    while index < length and depth:
        if source.startswith("/*", index):
            depth += 1
            index += 2
        elif source.startswith("*/", index):
            depth -= 1
            index += 2
        else:
            index += 1
    return index


def _end_of_string(source: str, start: int) -> int:
    index = start + 1
    length = len(source)
    while index < length:
        char = source[index]
        if char == "\\":
            index += 2
            continue
        index += 1
        if char == '"':
            return index
    return length


def _skip_quote(source: str, start: int, literals: list[tuple[int, int]]) -> int:
    """Advance past a character literal, or past the tick of a lifetime."""

    if source[start + 1 : start + 2] == "\\":
        index = source.find("'", start + 3)
        end = len(source) if index < 0 else index + 1
        literals.append((start + 1, end - 1))
        return end
    if source[start + 2 : start + 3] == "'":
        literals.append((start + 1, start + 2))
        return start + 3
    return start + 1


_CFG_TEST = re.compile(r"#\s*\[\s*cfg\s*\(\s*(?:all\s*\(\s*)?test\s*[,)]")


def cfg_test_spans(code: str) -> list[tuple[int, int]]:
    """Return the span of every `#[cfg(test)]` item, attribute included."""

    spans: list[tuple[int, int]] = []
    for match in _CFG_TEST.finditer(code):
        spans.append((match.start(), _end_of_item(code, match.start())))
    return spans


def _end_of_item(code: str, start: int) -> int:
    """Return the end of the item an attribute at `start` applies to.

    The item is a block (`mod`, `fn`, `impl`), a statement ending in `;` (`use`, `mod tests;`), or
    a struct field or enum variant ending in `,`.
    """

    index = code.find("]", start)
    if index < 0:
        return len(code)
    depth = 0
    length = len(code)
    while index < length:
        char = code[index]
        if char in "([":
            depth += 1
        elif char in ")]":
            depth -= 1
        elif depth <= 0:
            if char == "{":
                return _end_of_block(code, index)
            if char in ";,":
                return index + 1
        index += 1
    return length


def _end_of_block(code: str, open_index: int) -> int:
    depth = 0
    index = open_index
    length = len(code)
    while index < length:
        if code[index] == "{":
            depth += 1
        elif code[index] == "}":
            depth -= 1
            if depth == 0:
                return index + 1
        index += 1
    return length


def _generic_arguments(code: str, open_index: int) -> list[str] | None:
    """Split the arguments of the `<…>` opening at `open_index`, or `None` if it never closes."""

    arguments: list[str] = []
    angle = square = round_ = 0
    start = open_index + 1
    index = open_index
    length = len(code)
    while index < length:
        char = code[index]
        if char == "<":
            angle += 1
        elif char == ">":
            if code[index - 1] in "-=":
                index += 1
                continue
            angle -= 1
            if angle == 0:
                arguments.append(code[start:index])
                if len(arguments) > 1 and not arguments[-1].strip():
                    arguments.pop()  # A trailing comma is legal and leaves a blank argument.
                return arguments
        elif char == "(":
            round_ += 1
        elif char == ")":
            round_ -= 1
        elif char == "[":
            square += 1
        elif char == "]":
            square -= 1
        elif char in ";{}":
            return None
        elif char == "," and angle == 1 and not square and not round_:
            arguments.append(code[start:index])
            start = index + 1
        index += 1
    return None


_USE_ITEM = re.compile(r"\b(?:use|extern\s+crate)\b")
_QUALIFIED_PATH = re.compile(r"<[^<>;{}]*\bas\b[^<>;{}]*>::")
_AS_CAST = re.compile(r"\bas\b")


def count_as_casts(files: Sequence[RustFile]) -> list[Site]:
    sites: list[Site] = []
    for file in product_files(files):
        code = blank(file.product, _use_item_spans(file.product))
        code = blank(code, (match.span() for match in _QUALIFIED_PATH.finditer(code)))
        for match in _AS_CAST.finditer(code):
            sites.append(file.site(match.start(), file.source_line(match.start())))
    return sites


def _use_item_spans(code: str) -> Iterator[tuple[int, int]]:
    for match in _USE_ITEM.finditer(code):
        end = code.find(";", match.end())
        yield match.start(), len(code) if end < 0 else end


_PANIC_CALL = re.compile(r"\.\s*(?:unwrap\s*\(\s*\)|expect\s*\()")


def count_bare_unwrap_and_expect(files: Sequence[RustFile]) -> list[Site]:
    sites: list[Site] = []
    for file in product_files(files):
        for match in _PANIC_CALL.finditer(file.product):
            sites.append(file.site(match.start(), file.source_line(match.start())))
    return sites


_RESULT = re.compile(r"\bResult\s*<")


def count_result_string_errors(files: Sequence[RustFile]) -> list[Site]:
    sites: list[Site] = []
    for file in product_files(files):
        for match in _RESULT.finditer(file.product):
            arguments = _generic_arguments(file.product, match.end() - 1)
            if arguments and len(arguments) >= 2 and arguments[-1].strip() == "String":
                sites.append(file.site(match.start(), file.source_line(match.start())))
    return sites


_STRUCT = re.compile(r"\bstruct\s+[A-Za-z_][A-Za-z0-9_]*")
_TESTING_FEATURE = re.compile(r"""feature\s*=\s*"testing\"""")


def count_testing_feature_struct_fields(files: Sequence[RustFile]) -> list[Site]:
    """Count `cfg(feature = "testing")` gates on the fields of product structs.

    Gates on the API that arms a seam are legitimate and stay uncounted; a product struct that has
    two shapes depending on a feature is the debt.
    """

    sites: list[Site] = []
    for file in product_files(files):
        for match in _STRUCT.finditer(file.product):
            body = _struct_body(file.product, match.end())
            if body is None:
                continue
            start, end = body
            for gate in _TESTING_FEATURE.finditer(file.literals, start, end):
                sites.append(file.site(gate.start(), file.source_line(gate.start())))
    return sites


def _struct_body(code: str, start: int) -> tuple[int, int] | None:
    """Return the brace body of a struct declared at `start`, or `None` for a tuple struct."""

    index = start
    length = len(code)
    while index < length:
        char = code[index]
        if char == "{":
            return index, _end_of_block(code, index)
        if char == ";":
            return None
        index += 1
    return None


_PARSER_CRATE = re.compile(r"\bnervix_nspl\s*::")


def count_parser_imports_outside_edges(files: Sequence[RustFile]) -> list[Site]:
    sites: list[Site] = []
    for file in product_files(files):
        if file.path.startswith(PARSER_EDGES):
            continue
        for match in _PARSER_CRATE.finditer(file.product):
            sites.append(file.site(match.start(), file.source_line(match.start())))
    return sites


_MODEL_PATH = re.compile(r"\bModel\s*::")


def count_model_matches_in_data_plane(files: Sequence[RustFile]) -> list[Site]:
    sites: list[Site] = []
    for file in product_files(files):
        if not file.path.startswith(DATA_PLANE) or file.path == DATA_PLANE_PLANNER:
            continue
        for match in _MODEL_PATH.finditer(file.product):
            sites.append(file.site(match.start(), file.source_line(match.start())))
    return sites


_NODE_ID_BINDING = re.compile(
    r"\b[A-Za-z0-9_]*[Nn]ode[A-Za-z0-9_]*\s*:\s*[^,;)\n{}]*\bString\b(?!\s*::)"
)
_NODE_ID_ALIAS = re.compile(r"\btype\s+[A-Za-z0-9_]*[Nn]ode[A-Za-z0-9_]*\s*=\s*String\s*;")


def count_string_node_ids(files: Sequence[RustFile]) -> list[Site]:
    """Count fields, parameters, and aliases that carry a node identity as a `String`."""

    sites: list[Site] = []
    for file in product_files(files):
        for pattern in (_NODE_ID_BINDING, _NODE_ID_ALIAS):
            for match in pattern.finditer(file.product):
                sites.append(file.site(match.start(), file.source_line(match.start())))
    return sites


_ERROR_DECLARATION = re.compile(r"\b(?:enum|struct)\s+([A-Za-z_][A-Za-z0-9_]*Error)\b")
_RESULT_RETURN = re.compile(r"->\s*((?:[A-Za-z_][A-Za-z0-9_]*\s*::\s*)*)Result\s*<")
_PLAIN_TYPE = re.compile(r"^(?:[A-Za-z_][A-Za-z0-9_]*\s*::\s*)*([A-Za-z_][A-Za-z0-9_]*)$")


def count_bare_error_signatures(files: Sequence[RustFile]) -> list[Site]:
    """Count signatures returning a Nervix error that carries no `error_stack` context."""

    declared = {
        match.group(1)
        for file in files
        for match in _ERROR_DECLARATION.finditer(file.code)
    }
    sites: list[Site] = []
    for file in product_files(files):
        for match in _RESULT_RETURN.finditer(file.product):
            if match.group(1).replace(" ", "") == "error_stack::":
                continue
            arguments = _generic_arguments(file.product, match.end() - 1)
            if not arguments or len(arguments) < 2:
                continue
            error = _PLAIN_TYPE.match(arguments[-1].strip())
            if error and error.group(1) in declared:
                sites.append(file.site(match.start(), file.source_line(match.start())))
    return sites


def count_files_over_3000_lines(files: Sequence[RustFile]) -> list[Site]:
    return [
        Site(file.path, 1, f"{file.lines} lines")
        for file in files
        if file.lines > LARGE_FILE_LINES
    ]


def product_files(files: Sequence[RustFile]) -> Iterator[RustFile]:
    return (file for file in files if file.is_product)


@dataclass(frozen=True)
class Count:
    name: str
    description: str
    collect: Callable[[Sequence[RustFile]], list[Site]]


COUNTS: tuple[Count, ...] = (
    Count(
        "files_over_3000_lines",
        f"Rust files longer than {LARGE_FILE_LINES} lines",
        count_files_over_3000_lines,
    ),
    Count("as_casts", "`as` casts outside imports and qualified paths", count_as_casts),
    Count(
        "bare_unwrap_and_expect",
        "bare `unwrap()` and `expect()` calls",
        count_bare_unwrap_and_expect,
    ),
    Count(
        "result_string_errors",
        "`Result<_, String>` in place of a typed error",
        count_result_string_errors,
    ),
    Count(
        "bare_error_signatures",
        "signatures returning a Nervix error without `Report`",
        count_bare_error_signatures,
    ),
    Count(
        "string_node_ids",
        "node identities carried as `String`",
        count_string_node_ids,
    ),
    Count(
        "testing_feature_struct_fields",
        'struct fields gated on `feature = "testing"`',
        count_testing_feature_struct_fields,
    ),
    Count(
        "parser_imports_outside_edges",
        "`nervix_nspl::` references outside the edges",
        count_parser_imports_outside_edges,
    ),
    Count(
        "model_matches_in_data_plane",
        f"`Model::` references under {DATA_PLANE} outside the planner",
        count_model_matches_in_data_plane,
    ),
)


def repository_root() -> Path:
    completed = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        capture_output=True,
        check=True,
        text=True,
    )
    return Path(completed.stdout.strip())


def load_files(root: Path) -> list[RustFile]:
    """Load every tracked Rust file that belongs to the workspace's own sources."""

    completed = subprocess.run(
        ["git", "-C", str(root), "ls-files", "-z", "--", "*.rs"],
        capture_output=True,
        check=True,
        text=True,
    )
    files: list[RustFile] = []
    for path in sorted(entry for entry in completed.stdout.split("\0") if entry):
        if TEST_DIRECTORIES.intersection(PurePosixPath(path).parts[:-1]):
            continue
        files.append(RustFile(path, (root / path).read_text(encoding="utf-8")))
    return files


def measure(root: Path) -> dict[str, list[Site]]:
    files = load_files(root)
    return {count.name: count.collect(files) for count in COUNTS}


def read_baseline(path: Path) -> dict[str, int]:
    if not path.exists():
        raise SystemExit(
            f"error: no baseline at {path}. Run `just ratchet --update` to write one."
        )
    baseline = json.loads(path.read_text(encoding="utf-8"))
    expected = {count.name for count in COUNTS}
    missing = sorted(expected - baseline.keys())
    unknown = sorted(baseline.keys() - expected)
    if missing or unknown:
        raise SystemExit(
            f"error: {path.name} does not match the counts this script measures"
            + (f"; missing {', '.join(missing)}" if missing else "")
            + (f"; unknown {', '.join(unknown)}" if unknown else "")
            + ". Run `just ratchet --update` to rewrite it."
        )
    return baseline


def write_baseline(path: Path, counts: dict[str, int]) -> None:
    body = json.dumps({name: counts[name] for name in sorted(counts)}, indent=2)
    path.write_text(f"{body}\n", encoding="utf-8")


def report(counts: dict[str, int], baseline: dict[str, int]) -> None:
    width = max(len(count.name) for count in COUNTS)
    print("Debt ratchet — every count may fall and none may rise.\n")
    for count in COUNTS:
        current, before = counts[count.name], baseline[count.name]
        change = current - before
        marker = "  " if change == 0 else ("^^" if change > 0 else "vv")
        print(
            f"  {marker} {count.name:<{width}}  {current:>6}"
            f"  baseline {before:>6}  {change:+6}   {count.description}"
        )
    print()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Count the architecture debt named in AGENTS.md and fail when any count is above "
            "its checked-in baseline."
        )
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="rewrite the baseline from the current counts, for the change that lowered them",
    )
    parser.add_argument(
        "--show",
        metavar="COUNT",
        help="list the sites behind one count instead of comparing against the baseline",
    )
    parser.add_argument(
        "--root",
        type=Path,
        help="repository to measure; defaults to the repository holding this script",
    )
    arguments = parser.parse_args(argv)

    root = arguments.root if arguments.root is not None else repository_root()
    baseline_path = root / BASELINE_FILE
    sites = measure(root)

    if arguments.show is not None:
        if arguments.show not in sites:
            known = ", ".join(count.name for count in COUNTS)
            print(
                f"error: unknown count {arguments.show!r}. Known counts: {known}",
                file=sys.stderr,
            )
            return 2
        try:
            for site in sites[arguments.show]:
                print(site.render())
        except BrokenPipeError:
            # A listing is read through `head` and `grep`; closing the pipe early is not an error.
            os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        return 0

    counts = {name: len(found) for name, found in sites.items()}

    if arguments.update:
        write_baseline(baseline_path, counts)
        print(f"Wrote {baseline_path.name} from the current counts.")
        return 0

    baseline = read_baseline(baseline_path)
    report(counts, baseline)

    risen = [count.name for count in COUNTS if counts[count.name] > baseline[count.name]]
    fallen = [count.name for count in COUNTS if counts[count.name] < baseline[count.name]]

    if fallen:
        print("These counts fell. Run `just ratchet --update` and commit the baseline with them:")
        for name in fallen:
            print(f"  {name}: {baseline[name]} -> {counts[name]}")
        print()

    if risen:
        sys.stdout.flush()  # Keep the report above the error when both are piped together.
        print("error: debt rose. These counts are above their baseline:", file=sys.stderr)
        for name in risen:
            print(
                f"  {name}: {baseline[name]} -> {counts[name]}"
                f" (+{counts[name] - baseline[name]});"
                f" `just ratchet --show {name}` lists the sites",
                file=sys.stderr,
            )
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
