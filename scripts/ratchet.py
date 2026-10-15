#!/usr/bin/env python3

"""Count the architecture debt the layered rework retires, and hold it down.

Every count in `debt-baseline.json` is a ratchet: a change may lower it and never raise it. The
script prints each count next to its baseline and exits non-zero when any count is above it,
naming the offending counts. A count that has fallen is reported too, with the reminder that
`--update` rewrites the baseline in the same change.

Semantic rules are compiler-resolved through the pinned tooling workspace. The four disjoint
counts cover physical file length, Rust cast expressions, testing-gated struct fields and
functions above a cyclomatic complexity. A stable Rust syntax parser visits authored casts and
fields, including inactive configurations, while skipping test-only items. It parses the original
source so Unicode token locations remain accurate; comments and embedded fixture text never become
syntax findings. The pinned cargo-crap analyzer measures complexity over the same discovered files.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Callable, Sequence

from scripts import cargo_crap
from scripts.rust_source import (
    TEST_DIRECTORIES, RustFile, Site, load_files, product_files, repository_root,
)

BASELINE_FILE = "debt-baseline.json"

LARGE_FILE_LINES = 3000

COMPLEXITY_COUNT = "functions_over_complexity_30"
COMPLEXITY_LIMIT = 30

def count_files_over_3000_lines(files: Sequence[RustFile]) -> list[Site]:
    return [
        Site(file.path, 1, f"{file.lines} lines")
        for file in files
        if file.lines > LARGE_FILE_LINES
    ]


def unreachable_sources(root: Path, files: Sequence[RustFile]) -> list[str]:
    """Discovered files the analyzer's walk never reads: a hidden path, a symbolic link, or a
    tracked file that Git ignores."""

    completed = subprocess.run(
        ["git", "-C", str(root), "ls-files", "-z", "--cached", "--ignored", "--exclude-standard", "--", "*.rs"],
        capture_output=True,
        check=True,
        text=True,
    )
    ignored = {entry for entry in completed.stdout.split("\0") if entry}
    unreachable = []
    for file in files:
        hidden = any(part.startswith(".") for part in PurePosixPath(file.path).parts)
        if hidden or (root / file.path).is_symlink() or file.path in ignored:
            unreachable.append(file.path)
    return unreachable


def count_functions_over_complexity_30(root: Path, files: Sequence[RustFile], target: Path) -> list[Site]:
    """Functions the pinned analyzer scores above the limit, over exactly the discovered files.

    The analyzer reads the same eligible files as file size, so in-crate test files count. It
    reports no `#[test]` function and nothing inside a `#[cfg(test)]` module, and it cannot see a
    function inside a macro, a trait's provided method or a function nested in another's body.
    """

    unreachable = unreachable_sources(root, files)
    if unreachable:
        raise SystemExit(
            "error: the complexity analyzer cannot read these discovered Rust files, so the count "
            "would miss their functions:\n" + "\n".join(unreachable)
        )
    try:
        functions = cargo_crap.analyze(root, target, sorted(TEST_DIRECTORIES))
    except cargo_crap.AnalyzerError as error:
        raise SystemExit(f"error: complexity analysis failed: {error}") from error
    discovered = {file.path for file in files}
    outside = sorted({function.path for function in functions} - discovered)
    if outside:
        raise SystemExit(
            "error: the complexity analysis measured Rust files that source discovery does not "
            "govern:\n" + "\n".join(outside)
        )
    sites = []
    for function in functions:
        if function.complexity > COMPLEXITY_LIMIT:
            detail = f"{function.name} has cyclomatic complexity {function.complexity:g}"
            sites.append(Site(function.path, function.line, detail))
    return sorted(sites, key=lambda site: (site.path, site.line))


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
    Count("as_casts", "Rust cast expressions", None),
    Count(
        "testing_feature_struct_fields",
        'struct fields gated on `feature = "testing"`',
        None,
    ),
    Count(
        COMPLEXITY_COUNT,
        f"functions whose cyclomatic complexity exceeds {COMPLEXITY_LIMIT}",
        None,
    ),
)


def measure_sources(root: Path) -> dict[str, list[Site]]:
    files = load_files(root)
    sites = {count.name: count.collect(files) if count.collect is not None else [] for count in COUNTS}
    target = Path(os.environ.get("CARGO_TARGET_DIR", Path(__file__).resolve().parents[1] / "target"))
    parser = target / "typed-ratchet/source/debug/nervix-lint-report"
    if not parser.is_file():
        raise SystemExit("error: missing Rust source parser; run just typed-ratchet-source-build")
    sources = [{"path": file.path, "source": file.source} for file in product_files(files)]
    completed = subprocess.run([str(parser), "--source"], input=json.dumps(sources), text=True, capture_output=True)
    if completed.returncode:
        raise SystemExit("error: Rust source analysis failed\n" + completed.stderr)
    for site in json.loads(completed.stdout):
        sites[site["rule"]].append(Site(site["path"], site["line"], site["rule"]))
    sites[COMPLEXITY_COUNT] = count_functions_over_complexity_30(root, files, target)
    return sites


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
        # The baseline holds counts, not sites, so the gate names every site of a risen count and
        # the one a change added is among them.
        for name in risen:
            print(f"\n{name} sites:")
            for site in sites[name]:
                print(f"  {site.render()}")
        return 1

    if arguments.update:
        write_baseline(baseline_path, counts)
        print(f"Wrote {baseline_path.name} from the current counts.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
