#!/usr/bin/env python3

"""Count the architecture debt the layered rework retires, and hold it down.

Every count in `debt-baseline.json` is a ratchet: a change may lower it and never raise it. The
script prints each count next to its baseline and exits non-zero when any count is above it,
naming the offending counts. A count that has fallen is reported too, with the reminder that
`--update` rewrites the baseline in the same change.

Synchronization debt is compiler-resolved through the pinned tooling workspace. Disjoint counts
remain textual. Comments and literal contents are blanked before matching, so a pattern
written in a doc comment or an NSPL fixture string is never counted, and `#[cfg(test)]` items are
blanked so unit tests never hold a production count up. Blanking preserves byte offsets, so spans
found in one view of a file address the same bytes in another.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterator, Sequence

from scripts.rust_source import (
    RustFile, Site, _end_of_block, blank, load_files, product_files, repository_root,
)

BASELINE_FILE = "debt-baseline.json"

LARGE_FILE_LINES = 3000

_USE_ITEM = re.compile(r"\b(?:use|extern\s+crate)\b")
# Attribute assignments such as rkyv's `as = ArchivedRoot` select a representation, not a cast.
_AS_CAST = re.compile(r"\bas\b(?!\s*=)")


def count_as_casts(files: Sequence[RustFile]) -> list[Site]:
    sites: list[Site] = []
    for file in product_files(files):
        code = blank(file.product, _use_item_spans(file.product))
        code = blank(code, _qualified_path_spans(code))
        for match in _AS_CAST.finditer(code):
            sites.append(file.site(match.start(), file.source_line(match.start())))
    return sites


def _qualified_path_spans(code: str) -> Iterator[tuple[int, int]]:
    """Yield the spans of `<Type as Trait>::` qualified paths.

    The `as` in one of these names the trait an item resolves through, so it is not a cast. The
    brackets nest — `<&[Token] as Input<'src>>::Span` — which a regular expression cannot follow,
    so the scan tracks depth and keeps only an `as` written at the top level of the outer pair.
    """

    for start, character in enumerate(code):
        if character != "<":
            continue
        depth = 0
        qualified = False
        for index in range(start, len(code)):
            current = code[index]
            if current == "<":
                depth += 1
            elif current == ">":
                depth -= 1
                if depth == 0:
                    if qualified and code.startswith("::", index + 1):
                        yield start, index + 2
                    break
            elif current in ";{}":
                break
            elif depth == 1 and _AS_CAST.match(code, index):
                qualified = True


def _use_item_spans(code: str) -> Iterator[tuple[int, int]]:
    for match in _USE_ITEM.finditer(code):
        end = code.find(";", match.end())
        yield match.start(), len(code) if end < 0 else end


# `saturating_duration_since` is `Instant`'s query for "how long since, or zero if it has not
# happened yet". It answers a question about time rather than choosing what an overflow means, so
# it is not what this count is about.
_CLAMPED_ARITHMETIC = re.compile(
    r"\.\s*(?:saturating|wrapping)_(?!duration_since\b)[a-z_0-9]+\s*\("
)


def count_clamped_arithmetic(files: Sequence[RustFile]) -> list[Site]:
    """Count `saturating_*` and `wrapping_*` calls.

    Each remaining call has to be one whose saturation or wrapping is the meaning of the
    computation, said so at the site. Everything else is checked arithmetic with the overflow
    classified, so this count only falls.
    """

    sites: list[Site] = []
    for file in product_files(files):
        for match in _CLAMPED_ARITHMETIC.finditer(file.product):
            sites.append(file.site(match.start(), file.source_line(match.start())))
    return sites


# The adapters an `Option` or `Result` chain is built from. `and_then`, `or_else`, `ok_or*`,
# `map_or*` and `unwrap_or*` belong to those two types alone, so requiring one of them to anchor a
# chain is what keeps an iterator pipeline — which shares `map`, `filter`, and closures — out.
_ADAPTERS = frozenset(
    {
        "and_then",
        "filter",
        "map",
        "map_err",
        "map_or",
        "map_or_else",
        "ok_or",
        "ok_or_else",
        "or_else",
        "unwrap_or",
        "unwrap_or_default",
        "unwrap_or_else",
    }
)
_ANCHORS = _ADAPTERS - {"filter", "map", "map_err"}

# `map_or` and `map_or_else` take the two arms of a match as their arguments, so one call is
# already the branch, however short the arms are.
_BRANCH_ADAPTERS = frozenset({"map_or", "map_or_else"})

_ADAPTER_CALL = re.compile(r"\.\s*([a-z_][a-z_0-9]*)\s*\(")
_CHAIN_CONTINUATION = re.compile(r"\s*(\.\s*([a-z_][a-z_0-9]*)\s*\()")


@dataclass(frozen=True)
class _AdapterCall:
    """One adapter call in a chain, addressed at the parenthesis its argument opens."""

    name: str
    open_paren: int


def count_combinator_control_flow(files: Sequence[RustFile]) -> list[Site]:
    """Count control flow written as an `Option` or `Result` combinator chain.

    A chain is one site, counted at its first call, when it branches or sequences instead of
    applying a single transformation: two adapters in a row, a `map_or` or `map_or_else` holding
    the two arms of a match, or a closure whose body is a block of statements. A lone adapter
    shaping a value on its way into `?` is neither, and an iterator pipeline never anchors a chain,
    so neither is counted.
    """

    sites: list[Site] = []
    for file in product_files(files):
        code = file.product
        continuations: set[int] = set()
        for match in _ADAPTER_CALL.finditer(code):
            if match.start() in continuations or match.group(1) not in _ADAPTERS:
                continue
            chain = _adapter_chain(code, match, continuations)
            if _is_control_flow(code, chain):
                names = ".".join(call.name for call in chain)
                sites.append(
                    file.site(match.start(), f"{names}: {file.source_line(match.start())}")
                )
    return sites


def _adapter_chain(
    code: str, first: re.Match[str], continuations: set[int]
) -> list[_AdapterCall]:
    """Return the run of adapter calls starting at `first`, recording the ones it swallows.

    A recorded offset is where `_ADAPTER_CALL` matches that call, so the scan that walks the file
    skips it rather than counting the tail of this chain as a chain of its own.
    """

    chain = [_AdapterCall(first.group(1), first.end() - 1)]
    while True:
        following = _CHAIN_CONTINUATION.match(code, _end_of_call(code, chain[-1].open_paren) + 1)
        if following is None or following.group(2) not in _ADAPTERS:
            return chain
        continuations.add(following.start(1))
        chain.append(_AdapterCall(following.group(2), following.end(1) - 1))


def _is_control_flow(code: str, chain: Sequence[_AdapterCall]) -> bool:
    """Whether the chain decides between two cases or sequences steps rather than transforming."""

    if any(call.name in _BRANCH_ADAPTERS for call in chain):
        return True
    if len(chain) > 1 and any(call.name in _ANCHORS for call in chain):
        return True
    return any(
        call.name in _ANCHORS and _closure_body_has_statements(code, call.open_paren)
        for call in chain
    )


def _closure_body_has_statements(code: str, open_paren: int) -> bool:
    """Whether the call's first argument is a closure whose block body holds statements."""

    index = _skip_space(code, open_paren + 1)
    if index < len(code) and code[index] == "|":
        parameters = code.find("|", index + 1)
        if parameters < 0:
            return False
        index = _skip_space(code, parameters + 1)
    if index >= len(code) or code[index] != "{":
        return False
    body = code[index + 1 : _end_of_block(code, index) - 1]
    depth = 0
    for character in body:
        if character in "([{":
            depth += 1
        elif character in ")]}":
            depth -= 1
        elif character == ";" and depth == 0:
            return True
    return False


def _end_of_call(code: str, open_index: int) -> int:
    depth = 0
    index = open_index
    length = len(code)
    while index < length:
        if code[index] == "(":
            depth += 1
        elif code[index] == ")":
            depth -= 1
            if depth == 0:
                return index
        index += 1
    return length


def _skip_space(code: str, index: int) -> int:
    while index < len(code) and code[index].isspace():
        index += 1
    return index


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


def count_files_over_3000_lines(files: Sequence[RustFile]) -> list[Site]:
    return [
        Site(file.path, 1, f"{file.lines} lines")
        for file in files
        if file.lines > LARGE_FILE_LINES
    ]


@dataclass(frozen=True)
class Count:
    name: str
    description: str
    collect: Callable[[Sequence[RustFile]], list[Site]] | None


COUNTS: tuple[Count, ...] = (
    Count(
        "files_over_3000_lines",
        f"Rust files longer than {LARGE_FILE_LINES} lines",
        count_files_over_3000_lines,
    ),
    Count("as_casts", "`as` casts outside imports and qualified paths", count_as_casts),
    Count(
        "clamped_arithmetic",
        "`saturating_*` and `wrapping_*` calls outside the time API",
        count_clamped_arithmetic,
    ),
    Count(
        "combinator_control_flow",
        "control flow written as `Option` and `Result` combinator chains",
        count_combinator_control_flow,
    ),
    Count(
        "testing_feature_struct_fields",
        'struct fields gated on `feature = "testing"`',
        count_testing_feature_struct_fields,
    ),
)


def measure_sources(root: Path) -> dict[str, list[Site]]:
    files = load_files(root)
    return {count.name: count.collect(files) for count in COUNTS if count.collect is not None}


def check_synchronization(root: Path) -> None:
    from scripts import typed_ratchet

    target = Path(os.environ.get("CARGO_TARGET_DIR", root / "target"))
    destination = target / "typed-ratchet/gate.json"
    if typed_ratchet.main(["--root", str(root), "--target-dir", str(target), "--output", str(destination)]):
        raise SystemExit("error: compiler synchronization diagnostics failed")


def measure(root: Path) -> dict[str, list[Site]]:
    from scripts.check_layer_provenance import check

    provenance = check(root)
    if provenance:
        raise SystemExit("error: language import provenance failed\n" + "\n".join(provenance))
    check_synchronization(root)
    return measure_sources(root)


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
        # The whole run is one report on stdout, because a CI log reads stdout and stderr as
        # independent pipes and would otherwise print this above the table it refers to. The exit
        # code, not the stream, is what says the run failed.
        print("error: debt rose. These counts are above their baseline:")
        for name in risen:
            print(
                f"  {name}: {baseline[name]} -> {counts[name]}"
                f" (+{counts[name] - baseline[name]});"
                f" `just ratchet --show {name}` lists the sites"
            )
        return 1

    if arguments.update:
        write_baseline(baseline_path, counts)
        print(f"Wrote {baseline_path.name} from the current counts.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
