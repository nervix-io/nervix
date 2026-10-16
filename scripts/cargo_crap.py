"""The pinned cargo-crap analyzer: its one declared version, its installation and one analysis.

Owns: which cargo-crap build this repository runs, where that build lives and how a complexity
analysis is run and read.
May depend on: Cargo, which builds the analyzer through the compiler wrapper the host configures,
and the analyzer's versioned JSON report.
Must not know: which functions a count governs, what threshold a count applies or what a
coverage score means. The complexity count of `scripts/ratchet.py` and the CRAP report of
`just check-coverage` decide those.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Iterable, Sequence

VERSION = "0.6.1"
CRATE = "cargo-crap"
CONFIGURATION = ".cargo-crap.toml"
INSTALL_COMMAND = "just install-cargo-crap"


class AnalyzerError(Exception):
    """The pinned analyzer is missing or another version, or its analysis did not complete."""


@dataclass(frozen=True)
class Function:
    """One function the analyzer measured, addressed by its repository-relative file."""

    path: str
    name: str
    line: int
    complexity: float


def default_target() -> Path:
    """The Cargo target directory of the checkout holding this script."""

    return Path(os.environ.get("CARGO_TARGET_DIR", Path(__file__).resolve().parents[1] / "target"))


def install_root(target: Path) -> Path:
    # The root names the version, so checkouts and pins never replace one another's build.
    return target / "tools" / f"{CRATE}-{VERSION}"


def executable(target: Path) -> Path:
    return install_root(target) / "bin" / CRATE


def install_command(target: Path) -> list[str]:
    # `--locked` builds the dependency versions the release recorded. Cargo applies the compiler
    # wrapper the host configures; nothing here selects or bypasses it.
    return ["cargo", "install", "--locked", "--force", "--version", VERSION,
            "--root", str(install_root(target)), CRATE]


def installed_version(path: Path) -> str | None:
    """The version the binary at `path` reports, or `None` when there is no binary to ask."""

    if not path.is_file():
        return None
    try:
        completed = subprocess.run([str(path), "--version"], capture_output=True, text=True)
    except OSError as error:
        raise AnalyzerError(f"{path} could not be run: {error}") from error
    words = completed.stdout.split()
    if completed.returncode or len(words) != 2 or words[0] != CRATE:
        raise AnalyzerError(
            f"{path} did not report a {CRATE} version (exit {completed.returncode}):\n"
            f"{completed.stdout}{completed.stderr}"
        )
    return words[1]


def verified_executable(target: Path) -> Path:
    """The pinned analyzer's binary, refusing a missing build or another version."""

    path = executable(target)
    found = installed_version(path)
    if found is None:
        raise AnalyzerError(f"{CRATE} {VERSION} is not installed at {path}; run `{INSTALL_COMMAND}`")
    if found != VERSION:
        raise AnalyzerError(
            f"{path} is {CRATE} {found}, but this repository pins {VERSION}; run `{INSTALL_COMMAND}`"
        )
    return path


def install(root: Path, target: Path) -> None:
    """Build the pinned version unless the install root already holds it."""

    if installed_version(executable(target)) == VERSION:
        return
    # From the repository, Cargo builds with the toolchain rust-toolchain.toml pins.
    try:
        completed = subprocess.run(install_command(target), cwd=root)
    except OSError as error:
        raise AnalyzerError(f"cargo could not be run: {error}") from error
    if completed.returncode:
        raise AnalyzerError(f"installing {CRATE} {VERSION} failed with exit {completed.returncode}")
    verified_executable(target)


def configuration_in_reach(directory: Path) -> Path | None:
    """The configuration file the analyzer would read when started in `directory`.

    The analyzer looks for it from its working directory upward. It can add exclusions and
    allowances, filter the report and change the weight of `?`.
    """

    for candidate in (directory, *directory.parents):
        path = candidate / CONFIGURATION
        if path.exists():
            return path
    return None


def unreachable_sources(root: Path, paths: Iterable[str]) -> list[str]:
    """The Rust sources, relative to `root`, that the analyzer's walk never reads: a hidden path, a
    symbolic link, or a tracked file that Git ignores."""

    completed = subprocess.run(
        ["git", "-C", str(root), "ls-files", "-z", "--cached", "--ignored", "--exclude-standard", "--", "*.rs"],
        capture_output=True,
        check=True,
        text=True,
    )
    ignored = {entry for entry in completed.stdout.split("\0") if entry}
    unreachable = []
    for path in paths:
        hidden = any(part.startswith(".") for part in PurePosixPath(path).parts)
        if hidden or (root / path).is_symlink() or path in ignored:
            unreachable.append(path)
    return unreachable


def analyze(root: Path, target: Path, excludes: Sequence[str]) -> list[Function]:
    """Measure every function of the Rust sources below `root` that the analyzer's walk reaches.

    The walk skips Git-ignored, hidden and symbolic-link paths and every path that one of the
    `excludes` globs matches relative to `root`. An analysis that skipped a source, read a
    configuration, used another `?` weight or measured nothing is refused rather than read as an
    empty result.
    """

    path = verified_executable(target)
    root = root.resolve()
    configuration = configuration_in_reach(root)
    if configuration is not None:
        raise AnalyzerError(
            f"{configuration} would change what the analyzer measures; "
            "the analysis reads no configuration file, so remove it"
        )
    command = [str(path), "--path", ".", "--no-default-excludes"]
    for pattern in excludes:
        command.extend(["--exclude", pattern])
    command.extend(["--format", "json", "--sort", "file"])
    completed = subprocess.run(command, cwd=root, capture_output=True, text=True)
    if completed.returncode:
        raise AnalyzerError(f"{CRATE} failed with exit {completed.returncode}:\n{completed.stderr}")
    # Without coverage input the analyzer writes to standard error only for a source it could not
    # read or parse, and it then continues without that source's functions.
    if completed.stderr:
        raise AnalyzerError(f"{CRATE} skipped sources it could not analyze:\n{completed.stderr}")
    try:
        report = json.loads(completed.stdout)
    except ValueError as error:
        raise AnalyzerError(f"{CRATE} wrote no JSON report: {error}") from error
    if report.get("version") != VERSION:
        raise AnalyzerError(f"{CRATE} wrote a version {report.get('version')!r} report, not {VERSION}")
    # The report names a `?` weight only when it is not the classical weight of 1.
    if "try_weight" in report:
        raise AnalyzerError(f"{CRATE} weighted `?` at {report['try_weight']}, not the classical 1")
    functions = [
        Function(PurePosixPath(entry["file"]).as_posix(), entry["function"], entry["line"], entry["cyclomatic"])
        for entry in report["entries"]
    ]
    if not functions:
        raise AnalyzerError(f"{CRATE} measured no function below {root}")
    return functions


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=f"Install or run the pinned {CRATE} {VERSION}.")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("install", help="build the pinned version into the target directory")
    run = commands.add_parser("run", help="run the pinned analyzer with the given arguments")
    run.add_argument("arguments", nargs=argparse.REMAINDER)
    arguments = parser.parse_args(argv)

    target = default_target()
    try:
        if arguments.command == "install":
            install(Path(__file__).resolve().parents[1], target)
            return 0
        path = verified_executable(target)
    except AnalyzerError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    forwarded = arguments.arguments
    if forwarded[:1] == ["--"]:
        forwarded = forwarded[1:]
    os.execv(path, [str(path), *forwarded])


if __name__ == "__main__":
    sys.exit(main())
