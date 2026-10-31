#!/usr/bin/env python3
"""Combine every coverage producer one CI run selects into one workspace line-coverage union.

`just coverage-full-select` records the selection the union is derived from: the run's event and
pull request label snapshot, whether the tested diff required the suites and the Bolero call, the
run and its attempt, the tested revision, and the result of every producer job. `just coverage-full
[input ...]` then reads the reports and completion records the producers exported, validates them
against that selection, and writes the union to `target/coverage-full/`. It runs nothing and needs
no binary or compiler, so repeating it over retained artifacts gives the same lines. An input is a
downloaded artifact directory, or one report or record.

The selection decides which producers the union requires. Every run whose tested diff requires the
suites needs the ordinary workspace report the coverage job merged and the collections of the
diagnostic-evidence job. `extra-primitives` adds the extra-tests job's Shuttle, Loom, Turmoil and
Deloxide primitive conformance collections, the `shuttle`, `loom`, `turmoil` and `deloxide` labels
add their jobs' canonical collections, and a pull request carrying `fuzz` adds the combined live Bolero fuzz
campaign. The Bolero call's selection record must show that its randomized properties passed exactly
when `bolero-random` requested them and that its sanitizer campaigns ran exactly when `fuzz`
requested them: a deliberate skip is valid and supplies no report. Every required producer job must
have succeeded, and every other one must have been skipped, except extra-tests, whose other labels
run steps of their own.

Each input is validated before any of its lines joins the union. A native collection must be
complete; at the tested revision with an unmodified working tree; from this run at an attempt no
later than the current one; in its producer's declared mode, attempt directory and source policy;
and its model or lane evidence must be complete and canonical. The ordinary report's merge record
must be complete at the tested revision, and so must the ordinary collections it merged. The fuzz
campaign must have run every target of the tested inventory once. A report's sources are mapped onto
the files the tested revision tracks exactly as the ordinary merge maps them, and a line beyond the
end of its file shows that the report describes another revision. A missing, stale, foreign,
corrupt, failed or incomplete input, two different inputs of one producer, and an input the
selection does not enable each fail the union with a message naming what to fix; it then writes no
report. An identical input named twice counts once.

The union is line coverage. Its executable lines are the union of every contribution's lines, a line
is covered when any contribution executed it, and its counter is the sum. A counter at or above 2^63
is a count difference that concurrent threads drove below zero, which llvm-cov exports as its
unsigned wrap. As in the ordinary merge, it counts no executions: it is left out of the sum and listed
under its contribution, and a line only such counters count is not measured. Function and branch
identities depend on the toolchain and configuration each producer built with, so the union carries
none; each producer's own report keeps them. The ordinary report is not changed, and the CRAP report
and the ordinary patch coverage keep reading it alone.

`target/coverage-full/` then holds `lcov-full.info`, the union; `beyond-ordinary.info`, one test
section per contribution with the lines it covered that the ordinary report did not cover;
`record.json`, the verdict, the selection, every required producer and where it was found, each
contribution's identity, attribution and counters that wrapped below zero, the totals per mode, and
the source policy; and
`summary.md`, the contributor breakdown CI publishes as the job summary. A failed union writes its
record and summary, naming every problem, and no report.
"""

from __future__ import annotations

import argparse
import collections
import enum
import hashlib
import json
import os
import subprocess
import sys
import tomllib
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath

from scripts import native_coverage as native
from scripts.coverage_merge import (
    DROPPED_POLICY,
    KEPT_POLICY,
    WORKSPACE_REPORT,
    WRAPPED_POLICY,
    Checkout,
    Dropped,
    MergeError,
    Tracked,
    WrappedCounter,
    display,
    git,
)
from scripts.deloxide_lane import RECORD as LANE_RECORD
from scripts.deloxide_lane import RecordError
from scripts.deloxide_lane import read_complete as read_complete_lane
from scripts.model_evidence import FILENAME as MODEL_EVIDENCE
from scripts.model_evidence import EvidenceError
from scripts.model_evidence import read_complete as read_complete_models

ROOT = Path(__file__).resolve().parent.parent
DIRECTORY = Path("target/coverage-full")
SELECTION = "selection.json"
REPORT = "lcov-full.info"
BEYOND_ORDINARY = "beyond-ordinary.info"
RECORD = "record.json"
SUMMARY = "summary.md"
SELECTION_PRODUCER = "coverage-full-selection"
ORDINARY = "ordinary"
# The ordinary workspace report the coverage job merged. The coverage-merged artifact holds its
# merge record at this path relative to the report's directory, as a local checkout does.
ORDINARY_REPORT = "ordinary"
MERGE_RECORD = Path("target/coverage-merge/record.json")
BOLERO_SELECTION = "bolero-selection"
FUZZ_PRODUCER = "bolero-fuzz"
FUZZ_MODE = "fuzz"
FUZZ_LABEL = "fuzz"
RANDOMIZED_LABEL = "bolero-random"
COMBINED_SCOPE = "combined shards"
INVENTORY = Path("tests/bolero-targets.toml")
PULL_REQUEST = "pull_request"
LOCAL_EVENT = "local"
SUCCESS = "success"
SKIPPED = "skipped"
# The label that makes the extra-tests job collect the modeled primitive conformance.
PRIMITIVES_LABEL = "extra-primitives"
# A job another label can run for steps of its own, so it may succeed without any producer here.
SHARED_JOBS = frozenset({"extra-tests"})
NATIVE_PRODUCERS = {producer.name: producer for producer in native.PRODUCERS}
DA_PREFIX = "DA:"

LINE_POLICY = (
    "line coverage: the executable lines are the union of every contribution's lines, a line is "
    "covered when any contribution executed it, and its counter is the sum"
)
FUNCTION_POLICY = (
    "left out: function and branch identities depend on each producer's toolchain and "
    "configuration, so only each producer's own report keeps them"
)
HARNESS_POLICY = (
    "native collections leave out harness code below tests, examples and benches directories, so "
    "only the ordinary report measures those files"
)
ORDINARY_POLICY = (
    "the ordinary report is read unchanged, and the CRAP report and the ordinary patch coverage keep "
    "reading it alone"
)
UNAVAILABLE = (
    "the browser console's WebAssembly build, which no collector instruments",
    "WASM guest code, which runs inside a user's guest, outside every collector",
    "the client conformance probes in other languages",
    "product processes the chaos suites kill, which write no counters",
    "Miri, mutation testing, compile-only checks, compile_fail doctests and the Loom weakening "
    "qualification, whose verdicts stay separate from executable coverage",
    "the randomized Bolero properties and the Bolero runner's Python report, which measure no Rust "
    "source lines",
)


class FullError(Exception):
    """A selection or an input the union cannot be derived from. The message names it."""


class Provider(enum.StrEnum):
    """Where a run ran: GitHub Actions in CI, or this machine."""

    GITHUB = "github"
    LOCAL = "local"


@dataclass(frozen=True)
class Requirement:
    """A producer the union requires when its run selects it, and where CI publishes it."""

    producer: str
    job: str
    artifact: str
    # The pull request label that enables the producer's job; `None` when every run whose tested
    # diff requires the suites runs it.
    label: str | None = None

    @property
    def mode(self) -> str | None:
        """The execution mode the producer's lines describe; `None` for the Bolero call's selection
        record, which measures no lines."""
        if self.producer == ORDINARY_REPORT:
            return ORDINARY
        if self.producer == FUZZ_PRODUCER:
            return FUZZ_MODE
        if self.producer == BOLERO_SELECTION:
            return None
        return NATIVE_PRODUCERS[self.producer].mode

    def name(self) -> str:
        """The producer and its mode, as messages name them."""
        if self.mode is None:
            return self.producer
        return f"{self.producer} ({self.mode})"

    def enabled(self, selection: Selection) -> bool:
        if self.label is None:
            return True
        if self.label == FUZZ_LABEL:
            return selection.fuzz_requested()
        return self.label in selection.labels

    def source(self) -> str:
        """Where CI publishes the producer, as messages name it."""
        return f"the {self.job} job's {self.artifact} artifact"

    def describe(self) -> dict[str, object]:
        return {
            "producer": self.producer, "mode": self.mode, "job": self.job,
            "artifact": self.artifact, "label": self.label,
        }


# Every producer the union reads, in the order the record lists them. The workflow contract tests
# hold each label-gated job, the producer it collects and the artifact it uploads to this table.
REQUIREMENTS: tuple[Requirement, ...] = (
    Requirement(ORDINARY_REPORT, job="coverage", artifact="coverage-merged"),
    Requirement(
        "test-primitives-shuttle", job="extra-tests", artifact="coverage-native-extras",
        label=PRIMITIVES_LABEL,
    ),
    Requirement(
        "test-primitives-loom", job="extra-tests", artifact="coverage-native-extras",
        label=PRIMITIVES_LABEL,
    ),
    Requirement(
        "test-primitives-turmoil", job="extra-tests", artifact="coverage-native-extras",
        label=PRIMITIVES_LABEL,
    ),
    Requirement(
        "test-primitives-deloxide", job="extra-tests", artifact="coverage-native-extras",
        label=PRIMITIVES_LABEL,
    ),
    Requirement("test-deadlock-evidence-order", job="diagnostic-evidence", artifact="diagnostic-evidence"),
    Requirement("test-deadlock-report", job="diagnostic-evidence", artifact="diagnostic-evidence"),
    Requirement("test-shuttle", job="shuttle", artifact="coverage-shuttle", label="shuttle"),
    Requirement("test-loom", job="loom", artifact="coverage-loom", label="loom"),
    Requirement("test-turmoil", job="turmoil", artifact="coverage-turmoil", label="turmoil"),
    Requirement("test-deloxide", job="deloxide", artifact="deloxide-deloxide", label="deloxide"),
    Requirement("test-deloxide-order", job="deloxide", artifact="deloxide-deloxide-order", label="deloxide"),
    Requirement("test-deloxide-stress", job="deloxide", artifact="deloxide-deloxide-stress", label="deloxide"),
    Requirement(
        "test-deloxide-stress-restore", job="deloxide", artifact="deloxide-deloxide-stress-restore",
        label="deloxide",
    ),
    Requirement(FUZZ_PRODUCER, job="bolero", artifact="coverage-bolero-fuzz", label=FUZZ_LABEL),
)
# The Bolero call's selection record, which every run that calls Bolero publishes.
SELECTION_RECORD = Requirement(BOLERO_SELECTION, job="bolero", artifact="bolero-selection")
# The jobs whose results the union checks: each required one succeeded and every other was skipped.
JOBS = tuple(sorted({requirement.job for requirement in (*REQUIREMENTS, SELECTION_RECORD)}))


@dataclass(frozen=True)
class Selection:
    """What one run selected, from which the union derives the producers it requires."""

    event: str
    labels: tuple[str, ...]
    revision: str
    provider: Provider
    run: str | None
    attempt: int | None
    tests_required: bool
    bolero_required: bool
    # Each producer job's result; empty for a local selection, which has no jobs.
    jobs: Mapping[str, str]

    def fuzz_requested(self) -> bool:
        return self.event == PULL_REQUEST and FUZZ_LABEL in self.labels

    def randomized_requested(self) -> bool:
        """A pull request runs the randomized properties only when `bolero-random` requests them, and
        a Bolero run of any other event always runs them, as the Bolero call's gate requires."""
        return self.event != PULL_REQUEST or RANDOMIZED_LABEL in self.labels

    def required(self) -> list[Requirement]:
        if not self.tests_required:
            return []
        required = []
        for requirement in REQUIREMENTS:
            if requirement.enabled(self):
                required.append(requirement)
        return required

    def required_jobs(self) -> set[str]:
        jobs = {requirement.job for requirement in self.required()}
        if self.bolero_required:
            jobs.add(SELECTION_RECORD.job)
        return jobs

    def admits(self, recorded: object) -> str | None:
        """Why a producer's recorded run is not an attempt of this run, or `None` when it is.

        A job that a later attempt did not run keeps its earlier attempt's verdict and artifacts, so
        any attempt up to the current one is this run's.
        """
        if not isinstance(recorded, Mapping):
            return "records no run"
        provider = recorded.get("provider")
        if self.provider is Provider.LOCAL:
            if provider != Provider.LOCAL:
                return f"comes from a {provider} run, while the selection is a local one"
            return None
        if provider != Provider.GITHUB:
            return f"comes from a {provider} run, not from GitHub Actions run {self.run}"
        if str(recorded.get("run")) != self.run:
            return f"comes from run {recorded.get('run')}, not from this run {self.run}"
        attempt = str(recorded.get("attempt"))
        if not attempt.isdigit() or int(attempt) < 1:
            return f"records no attempt of run {self.run}"
        if int(attempt) > self.attempt:
            return f"comes from attempt {attempt}, after this attempt {self.attempt} of run {self.run}"
        return None

    def describe(self) -> dict[str, object]:
        return {
            "producer": SELECTION_PRODUCER,
            "event": self.event,
            "labels": list(self.labels),
            "revision": self.revision,
            "run": {"provider": str(self.provider), "run": self.run, "attempt": self.attempt},
            "tests_required": self.tests_required,
            "bolero_required": self.bolero_required,
            "fuzz_requested": self.fuzz_requested(),
            "randomized_requested": self.randomized_requested(),
            "jobs": dict(sorted(self.jobs.items())),
        }

    @classmethod
    def load(cls, path: Path) -> Selection:
        try:
            content = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as error:
            raise FullError(f"cannot read the selection {path}: {error}; record it with just coverage-full-select") from error
        if not isinstance(content, dict) or content.get("producer") != SELECTION_PRODUCER:
            raise FullError(f"{path} is not a selection that just coverage-full-select recorded")
        run = content.get("run")
        labels = content.get("labels")
        jobs = content.get("jobs")
        valid = (
            isinstance(run, dict) and isinstance(labels, list) and isinstance(jobs, dict)
            and isinstance(content.get("event"), str) and isinstance(content.get("revision"), str)
            and type(content.get("tests_required")) is bool and type(content.get("bolero_required")) is bool
        )
        if not valid:
            raise FullError(f"the selection {path} lacks its event, labels, revision, run, jobs or requirements")
        try:
            provider = Provider(run.get("provider"))
        except ValueError:
            raise FullError(f"the selection {path} names no known run provider") from None
        attempt = run.get("attempt")
        if provider is Provider.GITHUB and (not isinstance(run.get("run"), str) or type(attempt) is not int or attempt < 1):
            raise FullError(f"the selection {path} names no GitHub Actions run and attempt")
        return cls(
            event=content["event"], labels=tuple(labels), revision=content["revision"],
            provider=provider, run=run.get("run"), attempt=attempt,
            tests_required=content["tests_required"], bolero_required=content["bolero_required"],
            jobs={str(name): str(result) for name, result in jobs.items()},
        )


def parse_labels(text: str) -> tuple[str, ...]:
    """The label snapshot, given as the JSON list GitHub's `toJSON` writes."""
    try:
        labels = json.loads(text)
    except ValueError as error:
        raise FullError(f"the labels must be a JSON list of names, not {text!r}") from error
    if not isinstance(labels, list) or not all(isinstance(label, str) and label for label in labels):
        raise FullError(f"the labels must be a JSON list of names, not {text!r}")
    if len(set(labels)) != len(labels):
        raise FullError(f"the label snapshot names a label twice: {text}")
    return tuple(sorted(labels))


def parse_jobs(text: str) -> dict[str, str]:
    """Each job's result, from the JSON object GitHub's `toJSON(needs)` writes."""
    try:
        needs = json.loads(text)
    except ValueError as error:
        raise FullError(f"the jobs must be the JSON object of the needs context, not {text!r}") from error
    if not isinstance(needs, dict):
        raise FullError(f"the jobs must be the JSON object of the needs context, not {text!r}")
    results = {}
    for name, job in sorted(needs.items()):
        if not isinstance(job, dict) or not isinstance(job.get("result"), str):
            raise FullError(f"the needs context records no result of the {name} job")
        results[name] = job["result"]
    return results


def parse_flag(value: str | None, name: str, default: bool) -> bool:
    if value is None:
        return default
    if value not in ("true", "false"):
        raise FullError(f"{name} must be true or false, not {value!r}")
    return value == "true"


def record_selection(
    root: Path, environment: Mapping[str, str], event: str | None, labels: str, tests_required: str | None,
    bolero_required: str | None, jobs: str | None,
) -> Selection:
    """The selection of the run this process belongs to: a GitHub Actions run, or a local one."""
    revision = git(root, "rev-parse", "HEAD").strip()
    if environment.get("GITHUB_ACTIONS") != "true":
        return Selection(
            event=event or LOCAL_EVENT, labels=parse_labels(labels), revision=revision,
            provider=Provider.LOCAL, run=None, attempt=None,
            tests_required=parse_flag(tests_required, "--tests-required", True),
            bolero_required=parse_flag(bolero_required, "--bolero-required", False),
            jobs=parse_jobs(jobs) if jobs else {},
        )
    tested = environment.get("GITHUB_SHA")
    if tested and tested != revision:
        raise FullError(f"the checkout is at {revision}, not at the tested revision {tested}")
    run = environment.get("GITHUB_RUN_ID", "")
    attempt = environment.get("GITHUB_RUN_ATTEMPT", "")
    if not run or not attempt.isdigit():
        raise FullError("GitHub Actions did not name the run and its attempt")
    if event is None or tests_required is None or bolero_required is None or jobs is None:
        raise FullError("a GitHub Actions selection needs --event, --tests-required, --bolero-required and --jobs")
    results = parse_jobs(jobs)
    unrecorded = [job for job in JOBS if job not in results]
    if unrecorded:
        raise FullError(f"the needs context records no result of {', '.join(unrecorded)}; coverage-full must need every producer job")
    return Selection(
        event=event, labels=parse_labels(labels), revision=revision, provider=Provider.GITHUB,
        run=run, attempt=int(attempt),
        tests_required=parse_flag(tests_required, "--tests-required", True),
        bolero_required=parse_flag(bolero_required, "--bolero-required", True),
        jobs=results,
    )


def digest(*paths: Path) -> str:
    """The content of these files, so that an input named twice is told from two inputs."""
    content = hashlib.sha256()
    for path in paths:
        content.update(path.read_bytes())
        content.update(b"\0")
    return content.hexdigest()


@dataclass(frozen=True)
class Record:
    """A JSON record an input holds, the file it came from, and its content's identity."""

    path: Path
    content: Mapping[str, object]
    identity: str
    # The report the record describes, when it does not lie beside the record.
    report: Path | None = None

    @property
    def producer(self) -> str:
        return str(self.content.get("producer"))


@dataclass
class Found:
    """Everything the inputs hold that the union reads, by producer."""

    ordinary: list[Record] = field(default_factory=list)
    collections: dict[str, list[Record]] = field(default_factory=dict)
    selections: list[Record] = field(default_factory=list)
    # Every file already read, so that inputs naming the same file count it once.
    read: set[Path] = field(default_factory=set)

    def add_collection(self, record: Record) -> None:
        self.collections.setdefault(record.producer, []).append(record)


@dataclass
class Lines:
    """Line counts: files, executable lines and covered lines, and those the attribution derives."""

    files: int = 0
    executable: int = 0
    covered: int = 0
    exclusive: int = 0
    beyond_ordinary: int = 0

    def describe(self) -> dict[str, int]:
        return {
            "files": self.files, "executable": self.executable, "covered": self.covered,
            "exclusive": self.exclusive, "beyond_ordinary": self.beyond_ordinary,
        }


@dataclass
class Contribution:
    """One producer's report as the union reads it: its identity, its line counters by file, and
    the counters that wrapped below zero, which it leaves out as the ordinary merge does."""

    requirement: Requirement
    input: str
    report: str
    identity: dict[str, object]
    lines: dict[str, dict[int, int]] = field(default_factory=dict)
    records: int = 0
    kept: int = 0
    dropped: collections.Counter[Dropped] = field(default_factory=collections.Counter)
    wrapped: list[WrappedCounter] = field(default_factory=list)
    attribution: Lines = field(default_factory=Lines)

    def covered(self, path: str, number: int) -> bool:
        return self.lines.get(path, {}).get(number, 0) > 0

    def describe(self) -> dict[str, object]:
        dropped = {str(item): count for item, count in sorted(self.dropped.items())}
        return {
            **self.requirement.describe(), "input": self.input, "report": self.report,
            **self.identity, "records": {"read": self.records, "kept": self.kept, "dropped": dropped},
            "wrapped": [wrapped.describe() for wrapped in self.wrapped],
            "lines": self.attribution.describe(),
        }


class FullCoverage:
    """One union: the selection, the inputs it read, every problem it found and its contributions."""

    def __init__(self, root: Path, checkout: Checkout, selection: Selection) -> None:
        self.root = root
        self.checkout = checkout
        self.selection = selection
        self.problems: list[str] = []
        self.contributions: list[Contribution] = []
        # Every required producer as the record describes it, in requirement order.
        self.required: dict[str, dict[str, object]] = {}
        # The ordinary collections the ordinary merge read, by producer and attempt; `None` until a
        # valid ordinary merge record named them.
        self.merged: dict[str, str] | None = None
        self.ordinary_record: Mapping[str, object] | None = None
        self.fuzz: dict[str, object] = {}
        self.line_counts: dict[str, int] = {}
        self.full: dict[str, dict[int, int]] = {}
        self.totals: dict[str, object] = {}
        self.modes: dict[str, dict[str, int]] = {}

    def display(self, path: Path) -> str:
        return display(path, self.root)

    def problem(self, text: str) -> None:
        self.problems.append(text)

    # Reading the inputs.

    def read(self, inputs: Sequence[Path]) -> Found:
        found = Found()
        for item in inputs:
            if not item.exists():
                self.problem(f"{self.display(item)} does not exist: name a downloaded artifact directory, report or record")
                continue
            if item.is_dir() and (item.resolve() == self.root.resolve() or (item / ".git").exists()):
                self.problem(
                    f"{self.display(item)} is a checkout, which holds every attempt ever collected in it: name the "
                    f"artifact directories, the attempt directories and the {WORKSPACE_REPORT} to combine"
                )
                continue
            if item.is_dir():
                self.read_directory(item, found)
            else:
                self.read_file(item, found)
        return found

    def read_directory(self, directory: Path, found: Found) -> None:
        held = len(found.read)
        export = directory / WORKSPACE_REPORT
        if export.is_file():
            self.read_file(export, found)
        for completion in sorted(directory.rglob(native.RECORD)):
            self.read_file(completion, found)
        for record in sorted(directory.rglob(SELECTION)):
            self.read_file(record, found)
        for report in sorted(directory.rglob(native.REPORT)):
            if not (report.parent / native.RECORD).is_file():
                self.problem(f"{self.display(report)} is a native report without a completion record beside it")
        if len(found.read) == held:
            self.problem(
                f"{self.display(directory)} holds no ordinary report ({WORKSPACE_REPORT}), completion record "
                f"or Bolero selection record: it is not an artifact the union reads"
            )

    def read_file(self, path: Path, found: Found) -> None:
        resolved = path.resolve()
        if resolved in found.read:
            return
        found.read.add(resolved)
        if path.name == WORKSPACE_REPORT:
            self.read_ordinary(path, found)
            return
        if path.name not in (native.RECORD, SELECTION):
            self.problem(f"{self.display(path)} is not a report or record the union reads")
            return
        content = self.json(path)
        if content is None:
            return
        producer = content.get("producer")
        if not isinstance(producer, str):
            self.problem(f"{self.display(path)} is not a coverage record: it names no producer")
            return
        if path.name == SELECTION and producer != BOLERO_SELECTION:
            self.problem(f"{self.display(path)} is the record of {producer}, not of the Bolero call's selection")
            return
        if not isinstance(content.get("verdict"), str):
            self.problem(f"{self.display(path)} is the record of {producer} without a verdict")
            return
        if path.name == SELECTION:
            found.selections.append(Record(path, content, digest(path)))
            return
        if producer != FUZZ_PRODUCER and producer not in NATIVE_PRODUCERS:
            self.problem(f"{self.display(path)} is the record of {producer}, which is no coverage producer the union reads")
            return
        report = path.with_name(native.REPORT)
        identity = digest(path, report) if report.is_file() else digest(path)
        found.add_collection(Record(path, content, identity))

    def read_ordinary(self, report: Path, found: Found) -> None:
        record = report.parent / MERGE_RECORD
        if not record.is_file():
            self.problem(f"{self.display(report)} has no merge record beside it at {MERGE_RECORD}")
            return
        content = self.json(record)
        if content is None:
            return
        found.ordinary.append(Record(record, content, digest(record, report), report=report))

    def json(self, path: Path) -> dict[str, object] | None:
        try:
            content = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as error:
            self.problem(f"{self.display(path)} cannot be read: {error}")
            return None
        if not isinstance(content, dict):
            self.problem(f"{self.display(path)} is not a JSON object")
            return None
        return content

    # Resolving the selection's requirements.

    def resolve(self, found: Found) -> None:
        if not self.selection.tests_required:
            self.problem("the selection required no suites, so no producer ran and there is no coverage to combine")
            return
        self.check_jobs()
        required = self.selection.required()
        enabled = {requirement.producer for requirement in required}
        for requirement in required:
            if requirement.producer == ORDINARY_REPORT:
                records = found.ordinary
            else:
                records = found.collections.get(requirement.producer, [])
            record = self.one(requirement, records)
            if record is None:
                continue
            if requirement.producer == ORDINARY_REPORT:
                self.ordinary(requirement, record)
            elif requirement.producer == FUZZ_PRODUCER:
                self.campaign(requirement, record)
            else:
                self.collection(requirement, record)
        self.left_over(found, enabled)
        self.bolero_selection(found)

    def check_jobs(self) -> None:
        """Every required producer job succeeded, and every other one was skipped."""
        if self.selection.provider is Provider.LOCAL:
            return
        required = self.selection.required_jobs()
        for job in JOBS:
            result = self.selection.jobs.get(job)
            if job in required and result != SUCCESS:
                self.problem(
                    f"the {job} job ended {result or 'without a recorded result'}, so its coverage is not "
                    f"complete evidence of this run; fix that job first"
                )
            allowed = {SKIPPED, SUCCESS} if job in SHARED_JOBS else {SKIPPED}
            if job not in required and result not in allowed:
                self.problem(f"the {job} job ended {result}, although this run's selection does not require it")

    def one(self, requirement: Requirement, records: list[Record]) -> Record | None:
        """The one record of a required producer; an identical record found twice counts once."""
        distinct: dict[str, Record] = {}
        for record in records:
            distinct.setdefault(record.identity, record)
        described = requirement.describe()
        self.required[requirement.producer] = described
        if not distinct:
            reason = "is not among the inputs"
            result = self.selection.jobs.get(requirement.job)
            if result is not None:
                reason += f"; the {requirement.job} job ended {result}"
            self.problem(f"{requirement.name()} is required, but {requirement.source()} {reason}")
            described["found"] = None
            return None
        if len(distinct) > 1:
            paths = ", ".join(self.display(record.path) for record in distinct.values())
            self.problem(f"{requirement.producer} has more than one different record: {paths}; combine the inputs of one run alone")
            described["found"] = None
            return None
        record = next(iter(distinct.values()))
        described["found"] = self.display(record.path)
        return record

    def left_over(self, found: Found, enabled: set[str]) -> None:
        """No input holds a producer the selection does not enable, or an ordinary collection
        that the ordinary report did not merge."""
        for producer, records in sorted(found.collections.items()):
            if producer in enabled:
                continue
            native_producer = NATIVE_PRODUCERS.get(producer)
            is_ordinary = native_producer is not None and native_producer.mode == ORDINARY
            for record in records:
                attempt = record.content.get("attempt")
                if is_ordinary and self.merged is None:
                    # The ordinary report's own problem is already named; which collections it
                    # merged is unknown.
                    continue
                if is_ordinary and self.merged.get(producer) == attempt:
                    continue
                if is_ordinary:
                    self.problem(
                        f"{self.display(record.path)} is the ordinary collection {producer} at attempt {attempt}, "
                        "which the ordinary report did not merge"
                    )
                    continue
                self.problem(
                    f"{self.display(record.path)} is the collection of {producer}, which this run's selection "
                    "does not enable: a stale or fabricated input"
                )

    # Validating each kind of input.

    def ordinary(self, requirement: Requirement, record: Record) -> None:
        content = record.content
        name = self.display(record.path)
        report = record.report
        self.ordinary_record = content
        if content.get("verdict") != native.Verdict.COMPLETE:
            self.problem(f"{name}: the ordinary merge is {content.get('verdict')}, not complete: {content.get('failure')}")
            return
        if content.get("revision") != self.selection.revision:
            self.problem(f"{name}: the ordinary report describes {content.get('revision')}, not the tested revision {self.selection.revision}")
            return
        if content.get("report") != WORKSPACE_REPORT:
            self.problem(f"{name} is the record of {content.get('report')}, not of the ordinary {WORKSPACE_REPORT}")
            return
        merged = self.merged_collections(name, content)
        if merged is None:
            return
        self.merged = merged
        contribution = Contribution(
            requirement=requirement, input=self.display(report.parent), report=self.display(report),
            identity={"revision": content.get("revision"), "merged": dict(sorted(merged.items()))},
        )
        self.load(contribution, report, (), strict=True)

    def merged_collections(self, name: str, content: Mapping[str, object]) -> dict[str, str] | None:
        """The ordinary collections the ordinary merge read, each of this run at the tested revision."""
        merged = {}
        inputs = content.get("inputs")
        if not isinstance(inputs, list):
            self.problem(f"{name} lists no inputs")
            return None
        for described in inputs:
            reports = described.get("reports", []) if isinstance(described, dict) else None
            if not isinstance(reports, list):
                self.problem(f"{name} lists an input without its reports")
                return None
            for report in reports:
                if not isinstance(report, dict) or "producer" not in report:
                    continue
                producer = str(report["producer"])
                problem = self.revision_problem(report.get("revision")) or self.selection.admits(report.get("run"))
                if problem is not None:
                    self.problem(f"{name}: the ordinary collection {producer} it merged {problem}")
                    return None
                merged[producer] = str(report.get("attempt"))
        return merged

    def revision_problem(self, revision: object) -> str | None:
        """Why a recorded revision is not the tested one in an unmodified tree, or `None`."""
        if not isinstance(revision, dict):
            return "records no revision"
        if revision.get("commit") != self.selection.revision:
            return f"describes {revision.get('commit')}, not the tested revision {self.selection.revision}"
        if revision.get("modified") is not False:
            return "was collected from a modified working tree, so it does not describe the tested source"
        return None

    def collection(self, requirement: Requirement, record: Record) -> None:
        content = record.content
        name = self.display(record.path)
        producer = NATIVE_PRODUCERS[requirement.producer]
        if content.get("verdict") != native.Verdict.COMPLETE:
            failure = content.get("failure")
            self.problem(
                f"{name}: the collection of {requirement.producer} is {content.get('verdict')}, not complete"
                f"{f' ({failure})' if failure else ''}; rerun {content.get('rerun') or producer.rerun()}"
            )
            return
        report = record.path.with_name(native.REPORT)
        if not report.is_file():
            self.problem(f"{name}: the complete collection of {requirement.producer} holds no {native.REPORT}")
            return
        problem = self.collection_problem(record, producer)
        if problem is not None:
            self.problem(f"{name}: the collection of {requirement.producer} {problem}")
            return
        if self.merged is not None and self.merged.get(requirement.producer) == content.get("attempt"):
            # The ordinary report already holds this ordinary collection's counters.
            self.required[requirement.producer]["merged_into"] = ORDINARY_REPORT
            return
        origin = PurePosixPath(content["sources"]["root"])
        identity = {"attempt": content.get("attempt"), "run": content.get("run"), "toolchain": content.get("toolchain")}
        contribution = Contribution(
            requirement=requirement, input=self.display(record.path.parent), report=self.display(report), identity=identity,
        )
        self.load(contribution, report, (origin,), strict=False)

    def collection_problem(self, record: Record, producer: native.Producer) -> str | None:
        """The first reason a complete collection is not this run's current-source evidence."""
        content = record.content
        problem = self.layout_problem(record, producer)
        if problem is not None:
            return problem
        problem = self.revision_problem(content.get("revision"))
        if problem is not None:
            return problem
        problem = self.selection.admits(content.get("run"))
        if problem is not None:
            return problem
        problem = self.policy_problem(content)
        if problem is not None:
            return problem
        return self.evidence_problem(record, producer)

    def layout_problem(self, record: Record, producer: native.Producer) -> str | None:
        """Why a collection's attempt directory does not name its record's producer, mode and
        attempt, or `None`: `<producer>/<mode>/<toolchain>/<attempt>/completion.json`."""
        content = record.content
        parts = record.path.parts
        if len(parts) < 5:
            return "lies outside a <producer>/<mode>/<toolchain>/<attempt> directory"
        named = {"producer": parts[-5], "mode": parts[-4], "attempt": parts[-2]}
        recorded = {"producer": content.get("producer"), "mode": content.get("mode"), "attempt": content.get("attempt")}
        if named != recorded:
            return f"lies in a directory that names {named}, while its record names {recorded}"
        if content.get("mode") != producer.mode:
            return f"ran in {content.get('mode')}, while {producer.name} is collected in {producer.mode}"
        run = content.get("run")
        if isinstance(run, dict) and run.get("provider") == Provider.GITHUB:
            expected = f"github-{run.get('run')}-{run.get('attempt')}"
            if content.get("attempt") != expected:
                return f"names the attempt {content.get('attempt')}, not {expected} of the run it records"
        return None

    def policy_problem(self, content: Mapping[str, object]) -> str | None:
        sources = content.get("sources")
        if not isinstance(sources, dict) or not isinstance(sources.get("root"), str) or not sources["root"].startswith("/"):
            return "records no absolute checkout root of its sources"
        expected = {str(key): meaning for key, meaning in native.SOURCE_POLICY.items()}
        if sources.get("policy") != expected:
            return f"kept its sources by another policy than the union reads: {sources.get('policy')}"
        return None

    def evidence_problem(self, record: Record, producer: native.Producer) -> str | None:
        """Why a model or lane collection's evidence is not complete and canonical, or `None`."""
        if producer.filterable:
            if record.content.get("filter") != "":
                return f"selected only the checks matching {record.content.get('filter')!r}, not the canonical inventory"
            try:
                read_complete_models(record.path.with_name(MODEL_EVIDENCE), producer.mode, "")
            except EvidenceError as error:
                return f"has no complete canonical model evidence: {error}"
        if producer.lane is not None:
            try:
                read_complete_lane(record.path.with_name(LANE_RECORD), producer.lane)
            except RecordError as error:
                return f"has no complete lane record: {error}"
        return None

    def campaign(self, requirement: Requirement, record: Record) -> None:
        content = record.content
        name = self.display(record.path)
        if content.get("verdict") != native.Verdict.COMPLETE:
            self.problem(f"{name}: the fuzz campaign is {content.get('verdict')}, not complete: {content.get('failure')}")
            return
        report = record.path.with_name(native.REPORT)
        if not report.is_file():
            self.problem(f"{name}: the complete fuzz campaign holds no {native.REPORT}")
            return
        problem = self.campaign_problem(content)
        if problem is not None:
            self.problem(f"{name}: the fuzz campaign {problem}")
            return
        self.fuzz = {
            "campaign": name, "targets": content["counts"]["completed"], "inventory": content["inventory"]["sha256"],
            "toolchain": content.get("toolchain"),
        }
        identity = {"run": content.get("run"), "toolchain": content.get("toolchain"), "targets": content["counts"]["completed"]}
        contribution = Contribution(
            requirement=requirement, input=self.display(record.path.parent), report=self.display(report), identity=identity,
        )
        self.load(contribution, report, (), strict=False)

    def campaign_problem(self, content: Mapping[str, object]) -> str | None:
        """The first reason a complete campaign is not this run's whole requested campaign."""
        problem = self.scope_problem(content)
        if problem is not None:
            return problem
        problem = self.revision_problem(content.get("revision"))
        if problem is not None:
            return problem
        problem = self.selection.admits(content.get("run"))
        if problem is not None:
            return problem
        problem = self.snapshot_problem(content)
        if problem is not None:
            return problem
        return self.inventory_problem(content)

    def scope_problem(self, content: Mapping[str, object]) -> str | None:
        """Why a fuzz campaign record is not one whole campaign, or `None`: CI's combination of its
        shards, or one local campaign over the whole inventory."""
        if content.get("mode") != FUZZ_MODE:
            return f"ran in {content.get('mode')}, not in {FUZZ_MODE}"
        if content.get("scope") == COMBINED_SCOPE:
            return None
        if "target" in content:
            return "is one target's record; name the combined campaign instead"
        if content.get("shard") is not None:
            return "is one shard's campaign; just combine-bolero-fuzz combines the shards of a run"
        if self.selection.provider is Provider.GITHUB:
            return "is no combination of this run's shards"
        return None

    def snapshot_problem(self, content: Mapping[str, object]) -> str | None:
        """Why a campaign does not belong to this run's event and label snapshot, or `None`."""
        if self.selection.provider is Provider.LOCAL:
            return None
        if content.get("tested_sha") != self.selection.revision:
            return f"tested {content.get('tested_sha')}, not the tested revision {self.selection.revision}"
        labels = content.get("labels")
        if content.get("event") != self.selection.event or not isinstance(labels, list) or tuple(sorted(labels)) != self.selection.labels:
            return f"ran for {content.get('event')} with labels {labels}, not for this run's selection"
        return None

    def inventory_problem(self, content: Mapping[str, object]) -> str | None:
        """Why a campaign did not run every target of the tested inventory once, or `None`."""
        path = self.root / INVENTORY
        try:
            targets = [str(target["id"]) for target in tomllib.loads(path.read_text(encoding="utf-8"))["target"]]
        except (OSError, KeyError, TypeError, tomllib.TOMLDecodeError) as error:
            return f"cannot be checked against the tested inventory {INVENTORY}: {error}"
        inventory = content.get("inventory")
        if not isinstance(inventory, dict) or inventory.get("sha256") != hashlib.sha256(path.read_bytes()).hexdigest():
            return f"ran another inventory than the tested revision's {INVENTORY}"
        if content.get("selection") != targets:
            return f"selected {content.get('selection')}, not the {len(targets)} targets of the tested inventory"
        counts = content.get("counts")
        expected = {key: len(targets) for key in ("discovered", "selected", "executed", "completed")}
        if counts != expected:
            return f"counts {counts}, not every one of the {len(targets)} registered targets executed and completed once"
        return None

    def bolero_selection(self, found: Found) -> None:
        """The Bolero call ran the randomized properties exactly when `bolero-random` requested them,
        and the sanitizer campaigns exactly when `fuzz` requested them."""
        if not self.selection.bolero_required:
            for record in found.selections:
                self.problem(f"{self.display(record.path)} is a Bolero selection, although this run did not call Bolero")
            return
        record = self.one(SELECTION_RECORD, found.selections)
        if record is None:
            return
        problem = self.bolero_selection_problem(record.content)
        if problem is not None:
            self.problem(f"{self.display(record.path)}: the Bolero selection {problem}")
            return
        if not self.selection.fuzz_requested():
            self.fuzz = {"skipped": "the pull request carries no fuzz label, so the sanitizer campaigns were deliberately skipped"}

    def bolero_selection_problem(self, content: Mapping[str, object]) -> str | None:
        """The first reason the Bolero call's record is not this run's, or does not show the
        randomized properties and the sanitizer campaigns run exactly when requested."""
        run = {"provider": Provider.GITHUB, "run": content.get("run"), "attempt": content.get("attempt")}
        problem = self.selection.admits(run)
        if problem is not None:
            return problem
        if content.get("verdict") != native.Verdict.COMPLETE:
            return f"is {content.get('verdict')}, not complete"
        problem = self.snapshot_problem(content)
        if problem is not None:
            return problem
        randomized = self.selection.randomized_requested()
        if content.get("randomized_required") is not randomized:
            return f"records the randomized properties as required: {content.get('randomized_required')}"
        if randomized and content.get("randomized") != SUCCESS:
            return f"records the requested randomized properties as {content.get('randomized')}"
        if not randomized and content.get("randomized") != SKIPPED:
            return f"records unrequested randomized properties as {content.get('randomized')}"
        requested = self.selection.fuzz_requested()
        if content.get("sanitizer_required") is not requested:
            return f"records the sanitizer campaigns as required: {content.get('sanitizer_required')}"
        outcome = (content.get("sanitizer"), content.get("sanitizer_combined"))
        if requested and outcome != (SUCCESS, SUCCESS):
            return f"records the requested sanitizer campaigns as {outcome}"
        if not requested and outcome != (SKIPPED, SKIPPED):
            return f"records unrequested sanitizer campaigns as {outcome}"
        return None

    # Mapping each report onto the tested revision.

    def load(self, contribution: Contribution, report: Path, origins: Sequence[PurePosixPath], *, strict: bool) -> None:
        """Read one report's line counters under their repository paths.

        `strict` marks a report that holds only tracked files, the ordinary merge's output: any
        other source shows that it describes another revision.
        """
        name = contribution.report
        inside = 0
        first = None
        try:
            with report.open(encoding="utf-8") as content:
                for record in native.lcov_records(content):
                    contribution.records += 1
                    if first is None:
                        first = record.source
                    located = self.checkout.locate(record.source, origins)
                    if isinstance(located, Dropped):
                        contribution.dropped[located] += 1
                        if located.inside_the_checkout() or not PurePosixPath(record.source).is_absolute():
                            inside += 1
                        if strict:
                            self.problem(f"{name} names {record.source}, a file the tested revision does not track; it describes another revision")
                            return
                        continue
                    inside += 1
                    contribution.kept += 1
                    self.counters(contribution, located, record.lines)
        except (native.RunnerError, ValueError) as error:
            self.problem(f"{name} is not a valid LCOV report: {error}")
            return
        if contribution.records and not inside:
            self.problem(f"{name}: none of its {contribution.records} records lies below its checkout root; the first is {first}")
            return
        if not contribution.kept:
            self.problem(f"{name} contributes no record of a file the tested revision tracks")
            return
        beyond = self.beyond_the_end(contribution)
        if beyond is not None:
            self.problem(f"{name} counts {beyond}; it describes another revision of that file")
            return
        self.contributions.append(contribution)

    @staticmethod
    def counters(contribution: Contribution, located: Tracked, lines: Iterable[str]) -> None:
        """Read a record's line counters. A counter that wrapped below zero counts no executions, so
        it is listed instead of summed, and a line only such counters count is not measured."""
        counters = contribution.lines.setdefault(located.path, {})
        for line in lines:
            if not line.startswith(DA_PREFIX):
                continue
            wrapped = WrappedCounter.of(line, located.path)
            if wrapped is not None:
                contribution.wrapped.append(wrapped)
                continue
            fields = line[len(DA_PREFIX):].rstrip("\n").split(",")
            number = int(fields[0])
            counters[number] = counters.get(number, 0) + int(fields[1])

    def beyond_the_end(self, contribution: Contribution) -> str | None:
        """The first counted line that lies beyond the end of its file, or `None`."""
        for path in sorted(contribution.lines):
            last = max(contribution.lines[path], default=0)
            length = self.length(path)
            if last > length:
                return f"line {last} of {path}, which has {length} lines at the tested revision"
        return None

    def length(self, path: str) -> int:
        known = self.line_counts.get(path)
        if known is not None:
            return known
        content = (self.root / path).read_bytes()
        count = content.count(b"\n")
        if content and not content.endswith(b"\n"):
            count += 1
        self.line_counts[path] = count
        return count

    # The union and its attribution.

    def combine(self) -> None:
        """Sum every contribution's counters, and attribute each covered line."""
        coverers: dict[str, dict[int, int]] = {}
        for contribution in self.contributions:
            for path, lines in contribution.lines.items():
                merged = self.full.setdefault(path, {})
                covering = coverers.setdefault(path, {})
                for number, hits in lines.items():
                    merged[number] = merged.get(number, 0) + hits
                    if hits > 0:
                        covering[number] = covering.get(number, 0) + 1
        ordinary = self.ordinary_contribution()
        modes: dict[str, set[tuple[str, int]]] = {}
        modes_beyond: dict[str, set[tuple[str, int]]] = {}
        for contribution in self.contributions:
            covered = modes.setdefault(contribution.requirement.mode, set())
            beyond = modes_beyond.setdefault(contribution.requirement.mode, set())
            attribution = contribution.attribution
            for path, lines in contribution.lines.items():
                attribution.files += 1
                for number, hits in lines.items():
                    attribution.executable += 1
                    if hits == 0:
                        continue
                    attribution.covered += 1
                    covered.add((path, number))
                    if coverers[path][number] == 1:
                        attribution.exclusive += 1
                    if contribution is not ordinary and not ordinary.covered(path, number):
                        attribution.beyond_ordinary += 1
                        beyond.add((path, number))
        for mode in sorted(modes):
            self.modes[mode] = {"covered": len(modes[mode]), "beyond_ordinary": len(modes_beyond[mode])}
        self.totals = self.count_totals(ordinary)

    def ordinary_contribution(self) -> Contribution:
        for contribution in self.contributions:
            if contribution.requirement.producer == ORDINARY_REPORT:
                return contribution
        raise FullError("the union has no ordinary contribution")

    def count_totals(self, ordinary: Contribution) -> dict[str, object]:
        full = Lines()
        beyond = Lines()
        beyond_files = set()
        added = 0
        for path, lines in self.full.items():
            full.files += 1
            for number, hits in lines.items():
                full.executable += 1
                if number not in ordinary.lines.get(path, {}):
                    added += 1
                if hits == 0:
                    continue
                full.covered += 1
                if not ordinary.covered(path, number):
                    beyond.covered += 1
                    beyond_files.add(path)
        beyond.files = len(beyond_files)
        return {
            "ordinary": ordinary.attribution.describe(),
            "full": {"files": full.files, "executable": full.executable, "covered": full.covered},
            "beyond_ordinary": {"files": beyond.files, "covered": beyond.covered},
            "executable_beyond_ordinary": added,
            "wrapped_counters": sum(len(contribution.wrapped) for contribution in self.contributions),
        }

    # Publication.

    def write(self, directory: Path) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        replace(directory / REPORT, lcov_text(self.full))
        replace(directory / BEYOND_ORDINARY, self.beyond_ordinary_text())

    def beyond_ordinary_text(self) -> str:
        """One test section per contribution: the lines it covered that the ordinary report did not."""
        ordinary = self.ordinary_contribution()
        text = []
        for contribution in sorted(self.contributions, key=lambda item: item.requirement.producer):
            if contribution is ordinary:
                continue
            beyond: dict[str, dict[int, int]] = {}
            for path, lines in contribution.lines.items():
                for number, hits in lines.items():
                    if hits > 0 and not ordinary.covered(path, number):
                        beyond.setdefault(path, {})[number] = hits
            if beyond:
                text.append(f"TN:{contribution.requirement.producer}\n")
                text.append(lcov_text(beyond))
        return "".join(text)

    def describe(self) -> dict[str, object]:
        members = None
        if isinstance(self.ordinary_record, Mapping):
            members = self.ordinary_record.get("members")
        return {
            "selection": self.selection.describe(),
            "required": list(self.required.values()),
            "contributions": [contribution.describe() for contribution in self.contributions],
            "fuzz": self.fuzz,
            "totals": self.totals,
            "modes": self.modes,
            "members": members,
            "problems": self.problems,
            "policy": {
                "lines": LINE_POLICY, "functions_and_branches": FUNCTION_POLICY, "harness": HARNESS_POLICY,
                "ordinary": ORDINARY_POLICY, "kept": KEPT_POLICY, "wrapped": WRAPPED_POLICY,
                "dropped": {str(dropped): meaning for dropped, meaning in DROPPED_POLICY.items()},
                "unavailable": list(UNAVAILABLE),
            },
        }

    def summary(self) -> str:
        selection = self.selection
        labels = ", ".join(selection.labels) or "none"
        if selection.provider is Provider.GITHUB:
            run = f"{selection.event} run {selection.run} attempt {selection.attempt}"
        else:
            run = f"{selection.event} run"
        heading = f"{run} at {selection.revision[:12]}, labels: {labels}"
        if self.problems:
            lines = ["## Full workspace coverage failed", "", heading, "", "The union wrote no report:", ""]
            lines.extend(f"- {problem}" for problem in self.problems)
            return "\n".join(lines) + "\n"
        lines = [
            "## Full workspace coverage", "", heading, "",
            "| Contribution | Mode | Job | Files | Lines | Covered | Only here | Beyond ordinary |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
        ]
        for contribution in self.contributions:
            attribution = contribution.attribution
            lines.append(
                f"| {contribution.requirement.producer} | {contribution.requirement.mode} | {contribution.requirement.job} | "
                f"{attribution.files:,} | {attribution.executable:,} | {attribution.covered:,} | "
                f"{attribution.exclusive:,} | {attribution.beyond_ordinary:,} |"
            )
        ordinary = self.totals["ordinary"]
        full = self.totals["full"]
        beyond = self.totals["beyond_ordinary"]
        lines.extend([
            "", "| Report | Files | Lines | Covered | Line coverage |", "| --- | ---: | ---: | ---: | ---: |",
            f"| ordinary | {ordinary['files']:,} | {ordinary['executable']:,} | {ordinary['covered']:,} | {percent(ordinary['covered'], ordinary['executable'])} |",
            f"| full union | {full['files']:,} | {full['executable']:,} | {full['covered']:,} | {percent(full['covered'], full['executable'])} |",
            "",
        ])
        by_mode = ", ".join(
            f"{mode} {counts['beyond_ordinary']:,}" for mode, counts in self.modes.items() if counts["beyond_ordinary"]
        )
        lines.append(
            f"Lines covered beyond the ordinary report: {beyond['covered']:,} in {beyond['files']:,} files"
            f"{f' ({by_mode})' if by_mode else ''}; {self.totals['executable_beyond_ordinary']:,} executable lines "
            "only other configurations compile."
        )
        wrapped = [
            f"{contribution.requirement.producer} {len(contribution.wrapped):,}"
            for contribution in self.contributions if contribution.wrapped
        ]
        if wrapped:
            lines.append(
                f"Counters that wrapped below zero, left out of the sum: {self.totals['wrapped_counters']:,} "
                f"({', '.join(wrapped)})."
            )
        if "skipped" in self.fuzz:
            lines.append(f"Fuzzing: {self.fuzz['skipped']}.")
        elif self.fuzz:
            lines.append(f"Fuzzing: the combined live campaign of {self.fuzz['targets']} targets.")
        lines.extend(["", "Not measured: " + "; ".join(UNAVAILABLE) + "."])
        return "\n".join(lines) + "\n"


def percent(covered: int, executable: int) -> str:
    if not executable:
        return "n/a"
    return f"{100 * covered / executable:.2f}%"


def lcov_text(files: Mapping[str, Mapping[int, int]]) -> str:
    """Line records, in source and line order, so the same counters always read the same."""
    text = []
    for path in sorted(files):
        lines = files[path]
        text.append(f"SF:{path}\n")
        for number in sorted(lines):
            text.append(f"DA:{number},{lines[number]}\n")
        covered = sum(1 for hits in lines.values() if hits > 0)
        text.append(f"LF:{len(lines)}\nLH:{covered}\nend_of_record\n")
    return "".join(text)


def replace(path: Path, text: str) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, path)


def union(root: Path, selection_path: Path, inputs: Sequence[Path], directory: Path, environment: Mapping[str, str]) -> int:
    record: dict[str, object] = {
        "verdict": str(native.Verdict.RUNNING), "report": display(directory / REPORT, root),
        "started_at": native.timestamp(native.utc_now()),
    }
    full: FullCoverage | None = None
    directory.mkdir(parents=True, exist_ok=True)
    for stale in (REPORT, BEYOND_ORDINARY):
        (directory / stale).unlink(missing_ok=True)
    try:
        selection = Selection.load(selection_path)
        record["revision"] = selection.revision
        head = git(root, "rev-parse", "HEAD").strip()
        if head != selection.revision:
            raise FullError(f"the checkout is at {head}, not at the revision the selection tested, {selection.revision}")
        full = FullCoverage(root, Checkout.load(root), selection)
        found = full.read(inputs)
        full.resolve(found)
        if not full.problems:
            full.combine()
            full.write(directory)
            record["verdict"] = str(native.Verdict.COMPLETE)
        else:
            record["verdict"] = str(native.Verdict.FAILED)
            record["failure"] = f"{len(full.problems)} problems; the union wrote no report"
    except (FullError, MergeError, OSError, subprocess.SubprocessError) as error:
        record["verdict"] = str(native.Verdict.FAILED)
        record["failure"] = str(error)
    record["finished_at"] = native.timestamp(native.utc_now())
    if full is not None:
        record.update(full.describe())
        text = full.summary()
    else:
        text = f"## Full workspace coverage failed\n\n{record['failure']}\n"
    (directory / RECORD).write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    (directory / SUMMARY).write_text(text, encoding="utf-8")
    summary_path = environment.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as step_summary:
            step_summary.write(text)
    if record["verdict"] != native.Verdict.COMPLETE:
        print(text, file=sys.stderr, end="")
        print(f"coverage full: {record['failure']}; record: {display(directory / RECORD, root)}", file=sys.stderr)
        return 1
    print(text, end="")
    print(f"Union: {display(directory / REPORT, root)}; record: {display(directory / RECORD, root)}")
    return 0


def main(arguments: Sequence[str] | None = None, *, root: Path = ROOT, environment: Mapping[str, str] | None = None) -> int:
    if environment is None:
        environment = os.environ
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)
    select = commands.add_parser("select", help="record the selection of the run this process belongs to")
    select.add_argument("--output", type=Path, default=DIRECTORY / SELECTION)
    select.add_argument("--event", help="the run's event; a local selection's event is local")
    select.add_argument("--labels", default="[]", help="the pull request's labels as a JSON list")
    select.add_argument("--tests-required", help="true when the tested diff required the suites")
    select.add_argument("--bolero-required", help="true when the run called Bolero")
    select.add_argument("--jobs", help="the needs context as JSON: each producer job's result")
    combine = commands.add_parser("union", help="combine the inputs the selection requires")
    combine.add_argument("inputs", nargs="+", type=Path, help="downloaded artifact directories, reports or records")
    combine.add_argument("--selection", type=Path, default=DIRECTORY / SELECTION)
    combine.add_argument("--directory", type=Path, default=DIRECTORY)
    options = parser.parse_args(arguments)
    if options.command == "select":
        try:
            selection = record_selection(
                root, environment, options.event, options.labels, options.tests_required,
                options.bolero_required, options.jobs,
            )
        except (FullError, MergeError) as error:
            print(f"coverage full: {error}", file=sys.stderr)
            return 1
        output = root / options.output
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(selection.describe(), indent=2) + "\n", encoding="utf-8")
        required = ", ".join(requirement.producer for requirement in selection.required()) or "nothing"
        print(f"Selection: {display(output, root)}; required: {required}")
        return 0
    inputs = [root / item for item in options.inputs]
    return union(root, root / options.selection, inputs, root / options.directory, environment)


if __name__ == "__main__":
    sys.exit(main())
