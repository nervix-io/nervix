#!/usr/bin/env python3
"""Merge the ordinary coverage reports into one repository-relative workspace report.

`just coverage-merge [input ...]` writes `lcov-workspace.info`, the report every ordinary coverage
consumer reads, and its merge record, `target/coverage-merge/record.json`. CI names the
`coverage-tests`, `coverage-scenarios` and `coverage-native-extras` artifacts it downloaded. Locally
the default input is the `lcov-workspace.info` that `just test-coverage` wrote, which the merge
rewrites in place.

An input is a producer's artifact directory or one report. Its producer wrote the absolute checkout
path it ran in to `coverage-source-root.txt`, in the directory or beside the report. A directory
holds the producer's workspace export `lcov-workspace.info`, its Python tooling reports, each named
`python.lcov`, and its native collections: attempt directories below `native-coverage` holding
`completion.json` beside `lcov.info`. The merge reads every complete ordinary collection. A
collection of another mode describes modeled or diagnostic execution, so it is never read, and the
record lists it as left out. No other file of an input is read.

Each record's source is mapped onto the repository. The producer's checkout root is stripped and `..`
segments resolve lexically; a relative source is already relative to the repository. A record is
kept only when its file is tracked at the tested revision. Dependency, toolchain, generated and any
other records are dropped, and the merge record counts them by class. The counters of every report
that measured a file are summed by `bolero_coverage.merge_lcov`. The report holds line and function
counters: llvm-cov exports no branches for the Rust producers, and coverage.py names Python branches
in words that LCOV readers such as cargo-crap refuse, so branch records are left out. A counter at or
above 2^63 is a count that concurrent threads drove below zero, which llvm-cov exports as its
unsigned wrap. It counts no executions, so it is left out of the sum and the merge record lists it
under its report; a line no other report counts is then not measured.

The merge fails, writes no report and records why, when the report would be incomplete:

- an input is missing, or a directory holds no workspace export and no complete ordinary collection;
- an input has no root metadata;
- none of a report's records lies below its producer's checkout root;
- a report contributes no record of a tracked file;
- a native report has no complete ordinary completion record beside it, or one producer has two;
- a workspace member that `cargo metadata` lists has no Rust record in the merged report.

`scripts/patch_coverage.py` maps report sources through the same `Checkout`.
"""

from __future__ import annotations

import argparse
import collections
import enum
import json
import os
import posixpath
import subprocess
import sys
import tempfile
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import TextIO

from scripts import native_coverage as native
from scripts.bolero_coverage import merge_lcov

ROOT = Path(__file__).resolve().parent.parent
SOURCE_ROOT_FILE = "coverage-source-root.txt"
WORKSPACE_REPORT = "lcov-workspace.info"
PYTHON_REPORT = "python.lcov"
RECORD = Path("target/coverage-merge/record.json")
ORDINARY = "ordinary"


class MergeError(Exception):
    """An input or a result that would leave the merged report incomplete. The message names it."""


class Dropped(enum.StrEnum):
    """Why a record's source is not a file the tested revision tracks."""

    CARGO_REGISTRY = "cargo-registry"
    CARGO_GIT = "cargo-git"
    KACHE_REGISTRY = "kache-registry"
    TOOLCHAIN = "toolchain"
    GENERATED = "generated"
    UNTRACKED = "untracked"
    OUTSIDE = "outside"

    @classmethod
    def outside_the_checkout(cls, source: str) -> Dropped:
        """The class of an absolute source that lies below none of its producer's checkout roots."""
        # CI's compilers name the standard library by its commit; one with the rust-src component
        # names the copy in its rustup toolchain.
        if source.startswith("/rustc/") or "/.rustup/toolchains/" in source:
            return cls.TOOLCHAIN
        if source.startswith("/kache/.cargo/registry/"):
            return cls.KACHE_REGISTRY
        if "/.cargo/registry/" in source:
            return cls.CARGO_REGISTRY
        if "/.cargo/git/" in source:
            return cls.CARGO_GIT
        return cls.OUTSIDE

    def inside_the_checkout(self) -> bool:
        """Whether a source of this class lies below its producer's checkout root."""
        return self in (Dropped.GENERATED, Dropped.UNTRACKED)


DROPPED_POLICY = {
    Dropped.CARGO_REGISTRY: "dependency sources below ~/.cargo/registry",
    Dropped.CARGO_GIT: "dependency sources below ~/.cargo/git",
    Dropped.KACHE_REGISTRY: "dependency sources below /kache/.cargo/registry",
    Dropped.TOOLCHAIN: "standard library sources below /rustc/<commit> or a rustup toolchain",
    Dropped.GENERATED: "files generated below the checkout's Cargo target directory",
    Dropped.UNTRACKED: "files inside the checkout that the tested revision does not track",
    Dropped.OUTSIDE: "any other file outside the checkout",
}
KEPT_POLICY = "files the tested revision tracks, test sources included, by their repository paths"
BRANCH_POLICY = "left out: the Rust producers export none, and Python branches are named in words"
# The smallest counter that is negative as a signed 64-bit count. llvm-cov derives some line and
# function counts as differences of counters, and an instrumented executable updates its counters
# without atomics, so threads that run the same code concurrently can lose increments and leave a
# difference below zero. The export writes it as its unsigned wrap, which counts no executions.
WRAPPED_COUNT = 2**63
WRAPPED_POLICY = (
    "left out of the sum and listed by report: a counter at or above 2^63 is a count difference "
    "that concurrent threads drove below zero and llvm-cov exported as its unsigned wrap"
)
# The branch records of an LCOV record, which the merged report leaves out.
BRANCH_LINES = ("BRDA:", "BRF:", "BRH:")


def display(path: Path, root: Path) -> str:
    """A path as the merge names it: relative to the repository when it lies inside it."""
    if path.is_relative_to(root):
        return path.relative_to(root).as_posix()
    return str(path)


def git(root: Path, *arguments: str) -> str:
    result = subprocess.run(
        ["git", *arguments], cwd=root, capture_output=True, text=True, encoding="utf-8", timeout=60,
    )
    if result.returncode:
        raise MergeError(f"git {arguments[0]} failed: {result.stderr.strip()}")
    return result.stdout


@dataclass(frozen=True)
class Tracked:
    """A record's source mapped onto a file the tested revision tracks, relative to the repository."""

    path: str


@dataclass(frozen=True)
class Checkout:
    """The files the tested revision tracks, onto which every report's sources are mapped."""

    tracked: frozenset[str]

    @classmethod
    def load(cls, root: Path) -> Checkout:
        listed = git(root, "ls-files", "-z")
        return cls(tracked=frozenset(path for path in listed.split("\0") if path))

    def locate(self, source: str, origins: Sequence[PurePosixPath]) -> Tracked | Dropped:
        """Map one LCOV source onto a tracked file, or name the class it is dropped as.

        `origins` are the absolute checkout roots the source's producer may have run in, most
        specific first; the first one that holds the source is stripped. A relative source is
        already relative to the repository. `..` segments resolve lexically, without reading the
        file system, so a `#[path]` module's source is the file it names.
        """
        path = PurePosixPath(posixpath.normpath(source))
        if not path.is_absolute():
            return self.inside(path)
        for origin in origins:
            if path.is_relative_to(origin):
                return self.inside(path.relative_to(origin))
        return Dropped.outside_the_checkout(path.as_posix())

    def inside(self, relative: PurePosixPath) -> Tracked | Dropped:
        parts = relative.parts
        if parts and parts[0] == "..":
            return Dropped.OUTSIDE
        source = relative.as_posix()
        if source in self.tracked:
            return Tracked(source)
        if parts and parts[0] == "target":
            return Dropped.GENERATED
        return Dropped.UNTRACKED


@dataclass(frozen=True)
class Member:
    name: str
    # Relative to the repository; the root package's directory has no parts.
    directory: PurePosixPath


@dataclass(frozen=True)
class Members:
    """The workspace members, each owning the sources below its manifest, most specific first."""

    members: tuple[Member, ...]

    @classmethod
    def load(cls, root: Path) -> Members:
        """The members `cargo metadata` lists for the workspace with its ordinary features."""
        result = subprocess.run(
            ["cargo", "metadata", "--no-deps", "--format-version", "1"],
            cwd=root, capture_output=True, text=True, timeout=300,
        )
        if result.returncode:
            raise MergeError(f"cargo metadata failed: {result.stderr.strip()}")
        return cls.from_metadata(json.loads(result.stdout))

    @classmethod
    def from_metadata(cls, metadata: Mapping[str, object]) -> Members:
        workspace = PurePosixPath(str(metadata["workspace_root"]))
        identities = set(metadata["workspace_members"])
        members = []
        for package in metadata["packages"]:
            if package["id"] not in identities:
                continue
            manifest = PurePosixPath(package["manifest_path"])
            members.append(Member(name=package["name"], directory=manifest.parent.relative_to(workspace)))
        if not members:
            raise MergeError("cargo metadata listed no workspace members")
        members.sort(key=lambda member: len(member.directory.parts), reverse=True)
        return cls(members=tuple(members))

    def names(self) -> list[str]:
        return sorted(member.name for member in self.members)

    def owner(self, path: str) -> str | None:
        parts = PurePosixPath(path).parts
        for member in self.members:
            prefix = member.directory.parts
            if parts[: len(prefix)] == prefix:
                return member.name
        return None

    def measured(self, sources: Iterable[str]) -> set[str]:
        """The members that own at least one of these sources' Rust files."""
        owners = set()
        for source in sources:
            if not source.endswith(".rs"):
                continue
            owner = self.owner(source)
            if owner is not None:
                owners.add(owner)
        return owners


@dataclass(frozen=True)
class Collection:
    """A native collection: the completion record of one attempt, beside its report."""

    completion: Path
    content: Mapping[str, object]

    @property
    def producer(self) -> str:
        return str(self.content["producer"])

    @property
    def mode(self) -> str:
        return str(self.content["mode"])

    @property
    def verdict(self) -> str:
        return str(self.content["verdict"])

    @property
    def report(self) -> Path:
        return self.completion.with_name(native.REPORT)

    def describe(self) -> dict[str, object]:
        return {
            "producer": self.producer, "mode": self.mode, "attempt": self.content.get("attempt"),
            "revision": self.content.get("revision"), "run": self.content.get("run"),
        }


@dataclass(frozen=True)
class Report:
    """One report the merge reads, its producer's checkout root, and its native collection."""

    path: Path
    origin: PurePosixPath
    collection: Collection | None = None


@dataclass
class Input:
    """One input as the merge record describes it."""

    path: str
    origin: PurePosixPath
    reports: list[dict[str, object]] = field(default_factory=list)
    left_out: list[dict[str, object]] = field(default_factory=list)

    def describe(self) -> dict[str, object]:
        return {"input": self.path, "root": str(self.origin), "reports": self.reports, "left_out": self.left_out}


@dataclass(frozen=True)
class WrappedCounter:
    """A line or function counter of a kept record that wrapped below zero, so it counts nothing."""

    source: str
    # The line a DA counter counts, or the function an FNDA counter counts.
    line: int | None
    function: str | None
    count: int

    @classmethod
    def of(cls, line: str, source: str) -> WrappedCounter | None:
        """The wrapped counter an LCOV line of the record holds, or None when it holds a count."""
        kind, _, value = line.rstrip("\n").partition(":")
        if kind == "DA":
            number, count = value.split(",")[:2]
            wrapped = cls(source, int(number), None, int(count))
        elif kind == "FNDA":
            count, _, name = value.partition(",")
            if not count.isdigit():
                raise native.RunnerError(f"malformed LCOV line in the record of {source}: {line.rstrip()}")
            wrapped = cls(source, None, name, int(count))
        else:
            return None
        if wrapped.count < WRAPPED_COUNT:
            return None
        return wrapped

    def describe(self) -> dict[str, object]:
        if self.line is not None:
            return {"source": self.source, "line": self.line, "count": self.count}
        return {"source": self.source, "function": self.function, "count": self.count}


@dataclass
class Counts:
    """What one report held: its records, the ones kept, the dropped ones by class, and the
    counters of its kept records that wrapped below zero."""

    records: int = 0
    kept: int = 0
    # Records whose sources lie below the producer's checkout root, or are relative to the repository.
    inside: int = 0
    dropped: collections.Counter[Dropped] = field(default_factory=collections.Counter)
    first: str | None = None
    wrapped: list[WrappedCounter] = field(default_factory=list)

    def describe_dropped(self) -> dict[str, int]:
        return {str(dropped): count for dropped, count in sorted(self.dropped.items())}


class Merge:
    """One merge: the inputs it read, each record mapped onto the tested revision, and what it kept."""

    def __init__(self, root: Path, checkout: Checkout, members: Members, scratch: Path) -> None:
        self.root = root
        self.checkout = checkout
        self.members = members
        self.scratch = scratch
        self.inputs: list[Input] = []
        self.named: set[str] = set()
        # Each ordinary native producer's completion record, so that no producer is counted twice.
        self.producers: dict[str, Path] = {}
        self.normalized: list[Path] = []
        self.records = 0
        self.kept_records = 0
        self.kept: set[str] = set()
        self.dropped: dict[Dropped, set[str]] = {}
        self.wrapped = 0

    def display(self, path: Path) -> str:
        return display(path, self.root)

    def read(self, item: Path) -> None:
        """Read one input: a producer's artifact directory, or one report."""
        if not item.exists():
            raise MergeError(f"{self.display(item)} does not exist: name a producer's artifact directory or report")
        identity = os.path.normpath(item.absolute())
        if identity in self.named:
            raise MergeError(f"{self.display(item)} is named twice")
        self.named.add(identity)
        if item.is_dir():
            metadata = item / SOURCE_ROOT_FILE
        else:
            metadata = item.with_name(SOURCE_ROOT_FILE)
        described = Input(path=self.display(item), origin=self.origin(item, metadata))
        self.inputs.append(described)
        if item.is_dir():
            reports = self.discover(item, described)
        else:
            reports = [Report(item, described.origin)]
        for report in reports:
            described.reports.append(self.normalize(report))

    def origin(self, item: Path, metadata: Path) -> PurePosixPath:
        """The absolute checkout path the input's producer ran in, from its root metadata."""
        if not metadata.is_file():
            raise MergeError(
                f"{self.display(item)} has no root metadata: {self.display(metadata)} is missing; "
                "its producer writes there the absolute checkout path it ran in"
            )
        value = metadata.read_text(encoding="utf-8").strip()
        if not value.startswith("/"):
            raise MergeError(f"{self.display(metadata)} must name the absolute checkout path its producer ran in, not {value!r}")
        return PurePosixPath(posixpath.normpath(value))

    def discover(self, directory: Path, described: Input) -> list[Report]:
        """The reports a producer's artifact directory holds, and its collections of other modes."""
        workspace = []
        export = directory / WORKSPACE_REPORT
        if export.is_file():
            workspace.append(Report(export, described.origin))
        recorded = set()
        ordinary = []
        for completion in self.collection_files(directory, native.RECORD):
            recorded.add(completion.parent)
            collection = self.collection(completion)
            if collection.mode != ORDINARY:
                described.left_out.append({
                    "collection": self.display(completion), "producer": collection.producer,
                    "mode": collection.mode, "verdict": collection.verdict,
                })
                continue
            ordinary.append(Report(collection.report, described.origin, collection))
        for report in self.collection_files(directory, native.REPORT):
            if report.parent not in recorded:
                raise MergeError(f"{self.display(report)} is a native report without a completion record beside it")
        if not workspace and not ordinary:
            raise MergeError(
                f"{self.display(directory)} holds no workspace export ({WORKSPACE_REPORT}) and no complete "
                "ordinary native collection: its producer published no ordinary Rust coverage"
            )
        python = [Report(path, described.origin) for path in sorted(directory.rglob(PYTHON_REPORT))]
        return [*workspace, *python, *ordinary]

    @staticmethod
    def collection_files(directory: Path, name: str) -> list[Path]:
        """The files of this name that the native collections below the directory hold."""
        found = []
        for path in sorted(directory.rglob(name)):
            if native.COLLECTION_DIRECTORY in path.relative_to(directory).parts:
                found.append(path)
        return found

    def collection(self, completion: Path) -> Collection:
        """A native collection's completion record, which an ordinary one must state complete."""
        content = json.loads(completion.read_text(encoding="utf-8"))
        named = isinstance(content, dict) and all(isinstance(content.get(key), str) for key in ("producer", "mode", "verdict"))
        if not named:
            raise MergeError(f"{self.display(completion)} is not a native coverage completion record: it names no producer, mode and verdict")
        collection = Collection(completion, content)
        if collection.mode != ORDINARY:
            return collection
        if collection.verdict != native.Verdict.COMPLETE:
            rerun = content.get("rerun") or f"just coverage-native-extras {collection.producer}"
            raise MergeError(
                f"{self.display(completion)}: the ordinary collection of {collection.producer} is "
                f"{collection.verdict}, not complete; rerun {rerun}"
            )
        if not collection.report.is_file():
            raise MergeError(
                f"{self.display(completion)}: the complete ordinary collection of {collection.producer} "
                f"holds no {native.REPORT}"
            )
        earlier = self.producers.get(collection.producer)
        if earlier is not None:
            raise MergeError(
                f"{collection.producer} has more than one ordinary collection, {self.display(earlier)} "
                f"and {self.display(completion)}; merge the attempt of the tested run alone"
            )
        self.producers[collection.producer] = completion
        return collection

    def normalize(self, report: Report) -> dict[str, object]:
        """Copy a report's kept records, with repository paths, for the merge, and count the rest."""
        counts = Counts()
        destination = self.scratch / f"{len(self.normalized)}.info"
        try:
            with report.path.open(encoding="utf-8") as content, destination.open("w", encoding="utf-8") as normalized:
                for record in native.lcov_records(content):
                    self.place(record, report.origin, counts, normalized)
        except native.RunnerError as error:
            raise MergeError(f"{self.display(report.path)}: {error}") from error
        if counts.records and not counts.inside:
            raise MergeError(
                f"{self.display(report.path)}: none of its {counts.records} records lies below "
                f"{report.origin}, the checkout path its producer recorded; the first is {counts.first}"
            )
        if not counts.kept:
            held = ", ".join(f"{count} {dropped}" for dropped, count in counts.describe_dropped().items())
            raise MergeError(
                f"{self.display(report.path)} contributes no record of a file the tested revision tracks; "
                f"it holds {held or 'no records'}"
            )
        self.normalized.append(destination)
        self.records += counts.records
        self.kept_records += counts.kept
        self.wrapped += len(counts.wrapped)
        described: dict[str, object] = {
            "report": self.display(report.path), "records": counts.records, "kept": counts.kept,
            "dropped": counts.describe_dropped(),
            "wrapped": [wrapped.describe() for wrapped in counts.wrapped],
        }
        if report.collection is not None:
            described.update(report.collection.describe())
        return described

    def place(self, record: native.LcovRecord, origin: PurePosixPath, counts: Counts, normalized: TextIO) -> None:
        """Keep one record under its repository path, or count it under the class it is dropped as.
        A kept record's counters that wrapped below zero are listed instead of summed."""
        counts.records += 1
        if counts.first is None:
            counts.first = record.source
        located = self.checkout.locate(record.source, (origin,))
        if isinstance(located, Tracked):
            counts.kept += 1
            counts.inside += 1
            self.kept.add(located.path)
            normalized.write(f"SF:{located.path}\n")
            for line in record.lines[1:]:
                if line.startswith(BRANCH_LINES):
                    continue
                wrapped = WrappedCounter.of(line, located.path)
                if wrapped is not None:
                    counts.wrapped.append(wrapped)
                    continue
                if not line.endswith("\n"):
                    line += "\n"
                normalized.write(line)
            return
        counts.dropped[located] += 1
        if located.inside_the_checkout():
            counts.inside += 1
        self.dropped.setdefault(located, set()).add(posixpath.normpath(record.source))

    def write(self, output: Path) -> None:
        """Sum every kept record into the report, once every workspace member has a Rust record."""
        measured = self.members.measured(self.kept)
        unmeasured = [name for name in self.members.names() if name not in measured]
        if unmeasured:
            raise MergeError(
                f"the merged report has no record of the workspace members {', '.join(unmeasured)}: "
                "no input measured any of their Rust sources"
            )
        temporary = output.with_name(f".{output.name}.tmp")
        merge_lcov(self.normalized, temporary)
        os.replace(temporary, output)

    def describe(self) -> dict[str, object]:
        measured = self.members.measured(self.kept)
        return {
            "inputs": [item.describe() for item in self.inputs],
            "records": {"read": self.records, "kept": self.kept_records, "files": len(self.kept)},
            "dropped_files": {str(dropped): len(sources) for dropped, sources in sorted(self.dropped.items())},
            "wrapped_counters": self.wrapped,
            "members": {
                "measured": sorted(measured),
                "unmeasured": [name for name in self.members.names() if name not in measured],
            },
            "policy": {
                "kept": KEPT_POLICY, "branches": BRANCH_POLICY, "wrapped": WRAPPED_POLICY,
                "dropped": {str(dropped): meaning for dropped, meaning in DROPPED_POLICY.items()},
            },
        }

    def summary(self, output: str, record: str) -> list[str]:
        dropped = ", ".join(f"{count} {name}" for name, count in self.describe()["dropped_files"].items())
        left_out = [f"{item['producer']} ({item['mode']})" for described in self.inputs for item in described.left_out]
        reports = sum(len(described.reports) for described in self.inputs)
        return [
            f"Merged {self.kept_records} of {self.records} records from {reports} reports of "
            f"{len(self.inputs)} inputs into {output}: {len(self.kept)} tracked files",
            f"Dropped files by class: {dropped or 'none'}",
            f"Left out collections of other modes: {', '.join(left_out) or 'none'}",
            f"Counters that wrapped below zero, left out of the sum: {self.wrapped or 'none'}",
            f"Workspace members measured: {len(self.members.measured(self.kept))} of {len(self.members.members)}",
            f"Merge record: {record}",
        ]


def main(arguments: Sequence[str] | None = None, *, root: Path = ROOT) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "inputs", nargs="*", type=Path,
        help=f"producer artifact directories or reports; {WORKSPACE_REPORT} when none is named",
    )
    parser.add_argument("--output", type=Path, default=Path(WORKSPACE_REPORT))
    parser.add_argument("--record", type=Path, default=RECORD)
    options = parser.parse_args(arguments)
    inputs = options.inputs or [Path(WORKSPACE_REPORT)]
    output = root / options.output
    record_path = root / options.record
    record: dict[str, object] = {
        "verdict": str(native.Verdict.RUNNING), "report": display(output, root),
        "started_at": native.timestamp(native.utc_now()),
    }
    merge: Merge | None = None
    try:
        record["revision"] = git(root, "rev-parse", "HEAD").strip()
        checkout = Checkout.load(root)
        members = Members.load(root)
        with tempfile.TemporaryDirectory(prefix="coverage-merge-") as scratch:
            merge = Merge(root, checkout, members, Path(scratch))
            for item in inputs:
                merge.read(root / item)
            merge.write(output)
        record["verdict"] = str(native.Verdict.COMPLETE)
        status = 0
    except (MergeError, native.RunnerError, OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
        record["verdict"] = str(native.Verdict.FAILED)
        record["failure"] = str(error)
        print(f"coverage merge: {error}", file=sys.stderr)
        status = 1
    record["finished_at"] = native.timestamp(native.utc_now())
    if merge is not None:
        record.update(merge.describe())
    record_path.parent.mkdir(parents=True, exist_ok=True)
    record_path.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    if merge is not None and status == 0:
        for line in merge.summary(display(output, root), display(record_path, root)):
            print(line)
    return status


if __name__ == "__main__":
    sys.exit(main())
