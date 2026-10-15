"""Shared views, source spans and tracked Rust source discovery for repository validators.

Owns lexical blanking and source structure without choosing a debt rule or compiler policy.
"""

from __future__ import annotations

import re
import subprocess
from bisect import bisect_left
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Iterable, Iterator, Sequence

# Directories whose Rust files are tests or harnesses rather than the product.
TEST_DIRECTORIES = frozenset({"tests", "benches", "examples"})

# In-crate test files. They still count toward file size, because a test file that has grown past
# `LARGE_FILE_LINES` is split like any other, but their bodies never hold a production count up.
TEST_FILE_SUFFIXES = ("/tests.rs", "_tests.rs", "/test_fixtures.rs")

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
    three have the same character length as the source, so Python character offsets agree.
    """

    def __init__(self, path: str, source: str) -> None:
        self.path = path
        self.source = source
        self.lines = source.count("\n") + int(bool(source) and not source.endswith("\n"))
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

    def macro_origins(self, offset: int) -> list[dict[str, str | None]]:
        """Source provenance for captured tokens whose compiler hygiene remains at the caller.

        This labels authored macro arguments; it does not recognize acquisitions or infer policy.
        Comments and literal contents use the same source view as the mandatory boundary scanner.
        """
        origins = []
        calls = re.compile(r"(?<![\w:])(?P<name>[A-Za-z_][\w]*(?:::[A-Za-z_][\w]*)*)\s*!\s*(?P<open>[({\[])")
        closing = {"(": ")", "{": "}", "[": "]"}
        for call in calls.finditer(self.code, 0, offset):
            stack = [closing[call.group("open")]]
            index = call.end()
            while index < len(self.code) and stack:
                token = self.code[index]
                if token in closing:
                    stack.append(closing[token])
                elif token == stack[-1]:
                    stack.pop()
                index += 1
            if not stack and call.end() <= offset < index:
                origins.append({"macro_name": call.group("name"), "call_site": f"{self.path}:{self.line_of(call.start())}", "definition": None})
        return origins


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


def generic_arguments(code: str, open_index: int) -> list[str] | None:
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


def product_files(files: Sequence[RustFile]) -> Iterator[RustFile]:
    return (file for file in files if file.is_product)


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
        ["git", "-C", str(root), "ls-files", "-z", "--cached", "--others", "--exclude-standard", "--", "*.rs"],
        capture_output=True,
        check=True,
        text=True,
    )
    files: list[RustFile] = []
    for path in sorted(set(entry for entry in completed.stdout.split("\0") if entry)):
        if TEST_DIRECTORIES.intersection(PurePosixPath(path).parts[:-1]):
            continue
        files.append(RustFile(path, (root / path).read_text(encoding="utf-8")))
    return files


