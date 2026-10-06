#!/usr/bin/env python3

"""Run the Deloxide diagnostic lane, qualify its supervision, replay its invocations, and hold
the owners of tracked locks to their applicability records.

`just test-deloxide` and `just test-deloxide-order` run `run deloxide` and `run deloxide-order` after
their prerequisites. The inventory in `tests/deloxide-inventory.toml` registers every workload a
selection runs: the disposable-process probes of `nervix-deadlock`, the diagnostic owner tests of the
server library, and the tagged scenarios the scenario binary runs on diagnostic nodes. For each
invocation the lane builds its executable for the selection, discovers what the build and the
feature files hold, and refuses a registered workload that is missing or ignored, a discovered test
or tagged scenario that is not registered, and a selection with nothing in it. It then runs the
invocation supervised and accounts for what ran: libtest's outcome of every registered test, or the
scenario count Cucumber reports against the examples the tags select.

Every process the lane starts runs in a session of its own within its bound and the lane's budget.
A process that outlives its bound gets SIGTERM, then SIGKILL after the stop grace, and its whole
group with it; a process that the lane finds still running after its invocation ended is killed and
fails the lane. The lane names how each invocation ended: completed, an active deadlock (status 3),
a diagnostic failure (status 4), killed by a signal, timed out, failed, or incomplete. After every
invocation that records evidence, the lane qualifies each process's evidence file with
`nervix-deadlock-report qualify` and keeps its summary: a missing file, a partly written one, or one
that does not qualify fails the lane, and repeated deliveries of one potential cycle are reported
apart from findings lost to overload. The first failure ends the lane.

Each run keeps a fresh attempt directory below `<build target>/test-deloxide/<selection>/`: every
log, the probes' artifacts, the evidence of every process, the description of every finding with
its source sites, and `lane.json`, which records the revision, the toolchain and Deloxide's locked
version, the selection, its features and bounds, and for every invocation its exact command,
environment additions, discovery, accounting, ending, duration and evidence. A failed attempt is
never removed or reused. `replay` runs one recorded invocation again with exactly its command,
environment and bound in a fresh attempt; OS schedules and timing are not recorded, so a replay can
take another interleaving. `qualify` proves the supervision end to end: it runs deliberately failing
workloads of the probe binary through the lane's own supervision and requires each failure class,
its evidence, the cleanup of every process, and the replay of a recorded active deadlock.

Under the native coverage collector the lane builds into the collector's instrumented target
directory, takes the prepared binaries from the target the prerequisites built, starts every test
executable through Cargo's configured runner, and writes its record where the collector reads it.

`applicability --catalog <gate.json>` reads the compiler's acquisition catalog and requires every
source file with a tracked blocking acquisition in a diagnostic configuration to have exactly one
`[[owner]]` record, and every record to name such a file.

Run it as `python3 -m scripts.deloxide_lane --target-dir <dir> <command> ...`.
"""

from __future__ import annotations

import argparse
import datetime
import enum
import hashlib
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import tomllib
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO

from scripts import libtest_accounting

INVENTORY = Path("tests/deloxide-inventory.toml")
# The invocation of disposable-process probes, which the supervision qualification starts its cases
# from.
PROBES = "probes"
RECORD = "lane.json"
ATTEMPTS = "test-deloxide"
# Set by the native coverage collector: where it wants the lane's record, which target directory
# its prerequisites built, and that the build is instrumented.
REPORT_VARIABLE = "NERVIX_DELOXIDE_LANE_REPORT"
PREPARED_TARGET_VARIABLE = "NERVIX_PREPARED_TARGET_DIR"
COVERAGE_ATTEMPT_VARIABLE = "NERVIX_NATIVE_COVERAGE_ATTEMPT"
# The variables the scenario binary, the probes and the diagnostic processes read.
EVIDENCE_VARIABLE = "NERVIX_DEADLOCK_EVIDENCE"
PROBE_ARTIFACTS_VARIABLE = "NERVIX_DEADLOCK_PROBE_ARTIFACTS"
PROBE_WORKLOAD_VARIABLE = "NERVIX_DEADLOCK_PROBE_WORKLOAD"
PROBE_EVIDENCE_VARIABLE = "NERVIX_DEADLOCK_PROBE_EVIDENCE"
REPORT_TOOL_VARIABLE = "NERVIX_DEADLOCK_REPORT_TOOL"
SUITE_BUDGET_VARIABLE = "NERVIX_TEST_SUITE_BUDGET"
ACTIVE_DEADLOCK_STATUS = 3
DIAGNOSTIC_FAILURE_STATUS = 4
NONQUALIFYING_STATUS = 5
BUDGET_STATUS = 124
FAILURE_STATUS = 1
EVIDENCE_PATTERN = "deadlock-*.rkyv"
PARTIAL_PATTERN = "deadlock-*.partial"
SUMMARY_PREFIX = "evidence summary: "
SUMMARY_KEYS = (
    "scope",
    "findings",
    "active",
    "potential",
    "unreviewed",
    "nonqualifying",
    "repeated-deliveries",
    "lost-handoff",
    "lost-order-history",
    "lost-retention",
)
# The compiler-resolved receivers of a tracked blocking acquisition in a diagnostic build.
TRACKED_RECEIVERS = frozenset({
    "nervix_primitives::sync::blocking::tracked::Mutex",
    "nervix_primitives::sync::blocking::tracked::RwLock",
})
DIAGNOSTIC_FEATURES = frozenset({"deloxide", "deloxide-order"})
_WORKLOAD_ID = re.compile(r"^[a-z]+(?:\.[a-z0-9]+(?:-[a-z0-9]+)*)+$")
_LISTED_TEST = re.compile(r"^(?P<name>\S+): test$")
_SCENARIO_SUMMARY = re.compile(r"^(?P<total>\d+) scenarios? \((?P<parts>[^)]*)\)$")
_SUMMARY_PART = re.compile(r"^(?P<count>\d+) (?P<kind>[a-z]+)$")
_STEP = re.compile(r"^(?:Given|When|Then|And|But|\*)\b")


class LaneError(Exception):
    """A configuration, inventory or discovery problem that stops the lane before or instead of a
    workload. The message names what was wrong and where."""


class Interrupted(Exception):
    """A termination signal asked the lane to stop before it finished."""

    def __init__(self, number: int) -> None:
        super().__init__(signal.Signals(number).name)
        self.number = number


@contextmanager
def interruptible() -> Iterator[None]:
    """Turn SIGTERM and SIGINT into an exception, so an interrupted lane still ends every process it
    started and records its verdict."""

    def interrupt(number: int, frame: object) -> None:
        raise Interrupted(number)

    previous = {number: signal.signal(number, interrupt) for number in (signal.SIGTERM, signal.SIGINT)}
    try:
        yield
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)


# The inventory.


class Kind(enum.StrEnum):
    """How an invocation runs: one libtest process for every selected test, one process per
    selected test, or the scenario binary over its tagged scenarios."""

    LIBTEST = "libtest"
    LIBTEST_EACH = "libtest-each"
    CUCUMBER = "cucumber"

    def is_libtest(self) -> bool:
        return self is not Kind.CUCUMBER


@dataclass(frozen=True)
class Bounds:
    budget_seconds: int
    stop_grace_seconds: int
    suite_teardown_reserve_seconds: int


@dataclass(frozen=True)
class Selection:
    name: str
    # The diagnostic selection a process of this build records in its evidence.
    recorded: str


@dataclass(frozen=True)
class Invocation:
    id: str
    kind: Kind
    package: str
    # `lib`, or `test:<name>` for an integration test target.
    target: str
    features: tuple[str, ...]
    bound_seconds: int
    evidence: bool
    ignored: frozenset[str]
    marker: str | None
    inputs: tuple[str, ...]
    tags: tuple[str, ...]
    driver: str | None
    # Whether its tests retain the output and evidence of the disposable children they start.
    artifacts: bool
    # How many scenarios the scenario binary runs at once, whatever the machine's CPU count.
    concurrency: int | None

    def cargo_target(self) -> list[str]:
        if self.target == "lib":
            return ["--lib"]
        return ["--test", self.target.removeprefix("test:")]

    def registers(self, test: str) -> bool:
        return self.marker is None or self.marker in test

    def builds(self, target: Mapping[str, object]) -> bool:
        """Whether a target of Cargo's build messages is this invocation's test target."""

        kinds = target.get("kind")
        if self.target == "lib":
            return isinstance(kinds, list) and "lib" in kinds
        return kinds == ["test"] and target.get("name") == self.target.removeprefix("test:")


@dataclass(frozen=True)
class Workload:
    id: str
    invocation: str
    selections: frozenset[str]
    invariant: str
    coverage: str
    test: str | None
    feature: str | None
    scenario: str | None
    examples: int | None


@dataclass(frozen=True)
class Owner:
    path: str
    workloads: tuple[str, ...]
    gap: str | None


@dataclass(frozen=True)
class Inventory:
    bounds: Bounds
    selections: Mapping[str, Selection]
    invocations: Mapping[str, Invocation]
    workloads: Mapping[str, Workload]
    owners: Mapping[str, Owner]

    def selection(self, name: str) -> Selection:
        selection = self.selections.get(name)
        if selection is None:
            known = ", ".join(self.selections)
            raise LaneError(f"no selection `{name}`; the inventory selects {known}")
        return selection

    def workloads_of(self, invocation: str, selection: str) -> list[Workload]:
        found: list[Workload] = []
        for workload in self.workloads.values():
            if workload.invocation == invocation and selection in workload.selections:
                found.append(workload)
        return found


def _string(table: Mapping[str, object], key: str, where: str) -> str:
    value = table.get(key)
    if not isinstance(value, str) or not value.strip():
        raise LaneError(f"{where} needs a non-empty string `{key}`")
    return value


def _positive(table: Mapping[str, object], key: str, where: str) -> int:
    value = table.get(key)
    if type(value) is not int or value <= 0:
        raise LaneError(f"{where} needs a positive integer `{key}`")
    return value


def _strings(table: Mapping[str, object], key: str, where: str) -> tuple[str, ...]:
    value = table.get(key, [])
    if not isinstance(value, list):
        raise LaneError(f"{where}: `{key}` is not a list")
    for item in value:
        if not isinstance(item, str) or not item:
            raise LaneError(f"{where}: `{key}` holds an entry that is not a non-empty string")
    if len(set(value)) != len(value):
        raise LaneError(f"{where}: `{key}` names an entry twice")
    return tuple(value)


def _known_keys(table: Mapping[str, object], allowed: set[str], where: str) -> None:
    unknown = sorted(set(table) - allowed)
    if unknown:
        raise LaneError(f"{where} has unknown keys: {', '.join(unknown)}")


def parse_inventory(text: str) -> Inventory:
    """Parse and validate the inventory. Every problem names the entry it was found in."""

    document = tomllib.loads(text)
    _known_keys(document, {"lane", "selection", "invocation", "workload", "owner"}, "the inventory")

    lane = document.get("lane")
    if not isinstance(lane, dict):
        raise LaneError("the inventory needs a [lane] table")
    _known_keys(lane, {"budget_seconds", "stop_grace_seconds", "suite_teardown_reserve_seconds"}, "[lane]")
    bounds = Bounds(
        budget_seconds=_positive(lane, "budget_seconds", "[lane]"),
        stop_grace_seconds=_positive(lane, "stop_grace_seconds", "[lane]"),
        suite_teardown_reserve_seconds=_positive(lane, "suite_teardown_reserve_seconds", "[lane]"),
    )

    selections: dict[str, Selection] = {}
    for index, table in enumerate(document.get("selection", [])):
        where = f"selection #{index + 1}"
        _known_keys(table, {"name", "recorded"}, where)
        name = _string(table, "name", where)
        if name not in DIAGNOSTIC_FEATURES:
            raise LaneError(f"{where}: `{name}` is not a diagnostic build feature")
        if name in selections:
            raise LaneError(f"selection {name} is registered twice")
        selections[name] = Selection(name=name, recorded=_string(table, "recorded", where))
    if not selections:
        raise LaneError("the inventory registers no selection")

    invocations: dict[str, Invocation] = {}
    for index, table in enumerate(document.get("invocation", [])):
        where = f"invocation #{index + 1}"
        invocation_id = _string(table, "id", where)
        where = f"invocation {invocation_id}"
        if invocation_id in invocations:
            raise LaneError(f"{where} is registered twice")
        _known_keys(
            table,
            {"id", "kind", "package", "target", "features", "bound_seconds", "evidence", "ignored",
             "marker", "inputs", "tags", "driver", "artifacts", "concurrency"},
            where,
        )
        try:
            kind = Kind(_string(table, "kind", where))
        except ValueError:
            raise LaneError(f"{where}: `kind` is not one of {', '.join(Kind)}") from None
        target = _string(table, "target", where)
        if target != "lib" and not re.fullmatch(r"test:[A-Za-z_][A-Za-z_0-9-]*", target):
            raise LaneError(f"{where}: `target` is neither `lib` nor `test:<name>`")
        features = _strings(table, "features", where)
        for feature in features:
            if feature in DIAGNOSTIC_FEATURES or feature in {"loom", "shuttle", "turmoil"}:
                raise LaneError(f"{where}: `features` names the execution mode `{feature}`")
        evidence = table.get("evidence", False)
        if type(evidence) is not bool:
            raise LaneError(f"{where}: `evidence` is not a boolean")
        artifacts = table.get("artifacts", False)
        if type(artifacts) is not bool:
            raise LaneError(f"{where}: `artifacts` is not a boolean")
        marker = table.get("marker")
        if marker is not None and (not isinstance(marker, str) or not marker):
            raise LaneError(f"{where}: `marker` is not a non-empty string")
        ignored = _strings(table, "ignored", where)
        inputs = _strings(table, "inputs", where)
        tags = _strings(table, "tags", where)
        driver = table.get("driver")
        if driver is not None and (not isinstance(driver, str) or not driver):
            raise LaneError(f"{where}: `driver` is not a non-empty string")
        concurrency: int | None = None
        if kind is Kind.CUCUMBER:
            concurrency = _positive(table, "concurrency", where)
            if not inputs or not tags:
                raise LaneError(f"{where}: a scenario invocation needs `inputs` and `tags`")
            if any(not tag.startswith("@") for tag in tags):
                raise LaneError(f"{where}: a tag does not start with `@`")
            if ignored or marker is not None:
                raise LaneError(f"{where}: a scenario invocation has no `ignored` or `marker`")
        else:
            if inputs or tags or driver is not None or "concurrency" in table:
                raise LaneError(
                    f"{where}: a libtest invocation has no `inputs`, `tags`, `driver` or "
                    "`concurrency`"
                )
            if kind is Kind.LIBTEST_EACH and marker is None:
                raise LaneError(f"{where}: one process per test needs the `marker` its tests carry")
            if marker is not None and any(marker not in test for test in ignored):
                raise LaneError(f"{where}: an ignored test does not carry its marker")
        invocations[invocation_id] = Invocation(
            id=invocation_id,
            kind=kind,
            package=_string(table, "package", where),
            target=target,
            features=features,
            bound_seconds=_positive(table, "bound_seconds", where),
            evidence=evidence,
            ignored=frozenset(ignored),
            marker=marker,
            inputs=inputs,
            tags=tags,
            driver=driver,
            artifacts=artifacts,
            concurrency=concurrency,
        )
        if kind is Kind.CUCUMBER and invocations[invocation_id].bound_seconds <= bounds.suite_teardown_reserve_seconds:
            raise LaneError(f"{where}: its bound leaves the suite no budget after its teardown reserve")
    if not invocations:
        raise LaneError("the inventory registers no invocation")
    probes = invocations.get(PROBES)
    if probes is None or probes.kind is not Kind.LIBTEST or not probes.artifacts:
        raise LaneError(
            f"the inventory needs the `{PROBES}` libtest invocation that retains its children's "
            "artifacts; the supervision qualification starts its cases from it"
        )

    workloads: dict[str, Workload] = {}
    tests_seen: set[tuple[str, str]] = set()
    scenarios_seen: set[tuple[str, str, str]] = set()
    for index, table in enumerate(document.get("workload", [])):
        where = f"workload #{index + 1}"
        workload_id = _string(table, "id", where)
        where = f"workload {workload_id}"
        if not _WORKLOAD_ID.fullmatch(workload_id):
            raise LaneError(f"{where}: an identity is a kind and dotted lowercase words")
        if workload_id in workloads:
            raise LaneError(f"{where} is registered twice")
        _known_keys(
            table,
            {"id", "invocation", "selections", "invariant", "coverage", "test", "feature",
             "scenario", "examples"},
            where,
        )
        invocation = invocations.get(_string(table, "invocation", where))
        if invocation is None:
            raise LaneError(f"{where} names an invocation the inventory does not register")
        names = _strings(table, "selections", where)
        if not names:
            raise LaneError(f"{where} names no selection")
        for name in names:
            if name not in selections:
                raise LaneError(f"{where} names the unknown selection `{name}`")
        test = table.get("test")
        feature = table.get("feature")
        scenario = table.get("scenario")
        examples = table.get("examples")
        if invocation.kind.is_libtest():
            test = _string(table, "test", where)
            if feature is not None or scenario is not None or examples is not None:
                raise LaneError(f"{where}: a test workload has no `feature`, `scenario` or `examples`")
            if not invocation.registers(test):
                raise LaneError(f"{where}: {test} does not carry its invocation's marker")
            if test in invocation.ignored or (invocation.id, test) in tests_seen:
                raise LaneError(f"{where}: {test} is registered twice")
            tests_seen.add((invocation.id, test))
        else:
            feature = _string(table, "feature", where)
            scenario = _string(table, "scenario", where)
            examples = _positive(table, "examples", where)
            if test is not None:
                raise LaneError(f"{where}: a scenario workload has no `test`")
            # Tags select the same scenarios in every build, so every selection runs them.
            if set(names) != set(selections):
                raise LaneError(f"{where}: a scenario runs in every selection its tags reach")
            if (invocation.id, feature, scenario) in scenarios_seen:
                raise LaneError(f"{where}: the scenario is registered twice")
            scenarios_seen.add((invocation.id, feature, scenario))
        workloads[workload_id] = Workload(
            id=workload_id,
            invocation=invocation.id,
            selections=frozenset(names),
            invariant=_string(table, "invariant", where),
            coverage=_string(table, "coverage", where),
            test=test,
            feature=feature,
            scenario=scenario,
            examples=examples,
        )
    for invocation in invocations.values():
        for name in selections:
            if not any(
                workload.invocation == invocation.id and name in workload.selections
                for workload in workloads.values()
            ):
                raise LaneError(f"invocation {invocation.id} registers no workload for {name}")

    owners: dict[str, Owner] = {}
    for index, table in enumerate(document.get("owner", [])):
        where = f"owner #{index + 1}"
        path = _string(table, "path", where)
        where = f"owner {path}"
        _known_keys(table, {"path", "workloads", "gap"}, where)
        if path in owners:
            raise LaneError(f"{where} is registered twice")
        named = _strings(table, "workloads", where)
        for workload_id in named:
            if workload_id not in workloads:
                raise LaneError(f"{where} names the unknown workload `{workload_id}`")
        gap = table.get("gap")
        if gap is not None and (not isinstance(gap, str) or not gap.strip()):
            raise LaneError(f"{where}: `gap` is not a non-empty string")
        if not named and gap is None:
            raise LaneError(f"{where} names neither the workloads that reach it nor its gap")
        owners[path] = Owner(path=path, workloads=named, gap=gap)

    return Inventory(
        bounds=bounds,
        selections=selections,
        invocations=invocations,
        workloads=workloads,
        owners=owners,
    )


def load_inventory(root: Path) -> Inventory:
    path = root / INVENTORY
    try:
        return parse_inventory(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise LaneError(f"{INVENTORY}: {error}") from error


# Discovery.


def listed_tests(output: str) -> list[str]:
    """The test names of libtest's `--list --format terse` output."""

    names: list[str] = []
    for line in output.splitlines():
        match = _LISTED_TEST.match(line.strip())
        if match is not None:
            names.append(match.group("name"))
    return names


@dataclass(frozen=True)
class Scenario:
    """A scenario of a feature file, and how many runs of it a tag selection makes: the example rows
    of its selected `Examples` blocks, or one for a plain scenario."""

    feature: str
    name: str
    line: int
    runs: int


def parse_scenarios(feature: str, text: str, tags: frozenset[str]) -> list[Scenario]:
    """The scenarios of one feature file that `tags` select, with the runs they select.

    This reads the Gherkin the suite writes: tags on their own lines before a feature, rule,
    scenario or examples block, and doc strings whose content is never Gherkin. A scenario's tags
    are its feature's, its rule's and its own; an example row adds its block's.
    """

    pending: set[str] = set()
    feature_tags: set[str] = set()
    rule_tags: set[str] = set()
    found: list[Scenario] = []
    name = ""
    line_number = 0
    outline = False
    scenario_tags: set[str] = set()
    examples_tags: set[str] = set()
    runs = 0
    in_examples = False
    header_pending = False
    docstring: str | None = None

    def finish() -> None:
        if name and runs > 0:
            found.append(Scenario(feature=feature, name=name, line=line_number, runs=runs))

    for number, raw in enumerate(text.splitlines(), start=1):
        stripped = raw.strip()
        if docstring is not None:
            if stripped.startswith(docstring):
                docstring = None
            continue
        if stripped.startswith('"""') or stripped.startswith("```"):
            docstring = stripped[:3]
            continue
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("@"):
            for token in stripped.split():
                if token.startswith("#"):
                    break
                pending.add(token)
            continue
        if stripped.startswith("|"):
            if in_examples:
                if header_pending:
                    header_pending = False
                elif (feature_tags | rule_tags | scenario_tags | examples_tags) & tags:
                    runs += 1
            continue
        in_examples = False
        keyword, _, rest = stripped.partition(":")
        if keyword == "Feature":
            feature_tags = pending
            pending = set()
        elif keyword == "Rule":
            finish()
            name = ""
            rule_tags = pending
            pending = set()
        elif keyword == "Background":
            pending = set()
        elif keyword in {"Scenario", "Example", "Scenario Outline", "Scenario Template"}:
            finish()
            name = rest.strip()
            line_number = number
            outline = keyword in {"Scenario Outline", "Scenario Template"}
            scenario_tags = pending
            pending = set()
            runs = 0
            if not outline and (feature_tags | rule_tags | scenario_tags) & tags:
                runs = 1
        elif keyword in {"Examples", "Scenarios"}:
            examples_tags = pending
            pending = set()
            in_examples = outline
            header_pending = outline
        elif not _STEP.match(stripped):
            # Free description text under a feature, rule or scenario.
            continue
    finish()
    return found


def discover_scenarios(root: Path, inputs: Sequence[str], tags: Sequence[str]) -> list[Scenario]:
    paths: set[Path] = set()
    for pattern in inputs:
        for path in root.glob(pattern):
            if path.is_file():
                paths.add(path)
    selected = frozenset(tags)
    found: list[Scenario] = []
    for path in sorted(paths):
        relative = path.relative_to(root).as_posix()
        found.extend(parse_scenarios(relative, path.read_text(encoding="utf-8"), selected))
    return found


def scenario_problems(
    invocation: Invocation, registered: Sequence[Workload], discovered: Sequence[Scenario]
) -> list[str]:
    """Every registered scenario the feature files no longer hold as registered, and every tagged
    scenario that is not registered."""

    problems: list[str] = []
    by_identity = {(scenario.feature, scenario.name): scenario for scenario in discovered}
    registered_identities: set[tuple[str, str]] = set()
    for workload in registered:
        identity = (workload.feature or "", workload.scenario or "")
        registered_identities.add(identity)
        scenario = by_identity.get(identity)
        if scenario is None:
            problems.append(
                f"{invocation.id}: workload {workload.id} names a scenario its tags no longer "
                f"select in {workload.feature}: {workload.scenario}"
            )
        elif scenario.runs != workload.examples:
            problems.append(
                f"{invocation.id}: workload {workload.id} registers {workload.examples} runs, and "
                f"the tags select {scenario.runs} in {workload.feature}:{scenario.line}"
            )
    for scenario in discovered:
        if (scenario.feature, scenario.name) not in registered_identities:
            problems.append(
                f"{invocation.id}: {scenario.feature}:{scenario.line} `{scenario.name}` is tagged "
                f"for the lane but not registered"
            )
    return problems


def test_problems(
    invocation: Invocation,
    registered: Sequence[Workload],
    listed: Sequence[str],
    listed_ignored: Sequence[str],
) -> list[str]:
    """Every registered test the build does not hold, holds ignored, or that a discovered lane test
    is not registered as."""

    problems: list[str] = []
    expected = {workload.test or "" for workload in registered}
    ignored = {test for test in listed_ignored if invocation.registers(test)}
    runnable = {test for test in listed if invocation.registers(test)} - ignored
    for workload in registered:
        if workload.test in ignored:
            problems.append(f"{invocation.id}: workload {workload.id} is ignored: {workload.test}")
        elif workload.test not in runnable:
            problems.append(
                f"{invocation.id}: workload {workload.id} names a test the build does not hold: "
                f"{workload.test}"
            )
    for test in sorted(runnable - expected):
        problems.append(f"{invocation.id}: {test} is in the build but not registered")
    for test in sorted(invocation.ignored - ignored):
        problems.append(f"{invocation.id}: {test} is registered as ignored but the build does not ignore it")
    for test in sorted(ignored - invocation.ignored - expected):
        problems.append(f"{invocation.id}: {test} is ignored in the build but not registered")
    return problems


# Supervision.


class Ending(enum.StrEnum):
    EXITED = "exited"
    SIGNALED = "signaled"
    TIMED_OUT = "timed-out"


@dataclass(frozen=True)
class Launch:
    """One process the lane starts: what it runs, where, with which additions to the environment,
    within which bound, and where its output goes."""

    name: str
    argv: tuple[str, ...]
    cwd: Path
    environment: Mapping[str, str]
    bound_seconds: float
    log: Path
    # A separate file for standard output, when it carries a machine-readable stream.
    stdout: Path | None = None

    def describe(self, root: Path) -> dict[str, object]:
        return {
            "name": self.name,
            "argv": list(self.argv),
            "cwd": display(self.cwd, root),
            "environment": dict(sorted(self.environment.items())),
            "bound_seconds": self.bound_seconds,
            "log": display(self.log, root),
        }


def signal_name(number: int) -> str:
    try:
        return signal.Signals(number).name
    except ValueError:
        return f"signal {number}"


@dataclass(frozen=True)
class Ended:
    """How a launched process ended: its process and group identifier, its ending with the status
    or signal, how long it ran, and what it left running, which the lane then killed."""

    pid: int
    ending: Ending
    status: int | None
    signal: int | None
    seconds: float
    leftovers: tuple[int, ...] = ()

    def describe(self) -> dict[str, object]:
        described: dict[str, object] = {"ending": str(self.ending), "seconds": round(self.seconds, 3)}
        if self.status is not None:
            described["status"] = self.status
        if self.signal is not None:
            described["signal"] = signal_name(self.signal)
        if self.leftovers:
            described["leftovers"] = list(self.leftovers)
        return described


@dataclass(frozen=True)
class ProcessState:
    parent: int
    group: int
    # The kernel's one-letter state; `Z` is a zombie, which has ended and awaits its parent.
    state: str

    def is_running(self) -> bool:
        return self.state != "Z"


def process_states() -> dict[int, ProcessState]:
    """Every process's parent, process group and state, read from /proc."""

    states: dict[int, ProcessState] = {}
    proc = Path("/proc")
    if not proc.is_dir():
        return states
    for entry in proc.iterdir():
        if not entry.name.isdigit():
            continue
        try:
            stat = (entry / "stat").read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        # The command name is parenthesized and may hold spaces; the fields after it are fixed:
        # state, parent, process group.
        fields = stat[stat.rfind(")") + 2 :].split()
        if len(fields) < 3:
            continue
        states[int(entry.name)] = ProcessState(parent=int(fields[1]), group=int(fields[2]), state=fields[0])
    return states


def running_members(group: int) -> list[int]:
    """The processes of a process group that have not ended."""

    members: list[int] = []
    for pid, state in process_states().items():
        if state.group == group and state.is_running():
            members.append(pid)
    return sorted(members)


def become_subreaper() -> None:
    """Adopt the orphans of every process the lane starts, so none escapes the cleanup check."""

    if sys.platform != "linux":
        return
    import ctypes

    pr_set_child_subreaper = 36
    library = ctypes.CDLL(None, use_errno=True)
    library.prctl(pr_set_child_subreaper, 1, 0, 0, 0)


# How long the lane waits for killed leftovers to be reaped, in polls of `REAP_POLL_SECONDS`.
REAP_POLLS = 100
REAP_POLL_SECONDS = 0.05


class Processes:
    """How the lane starts and ends processes. Tests substitute a double."""

    def __init__(self, base_environment: Mapping[str, str], stop_grace_seconds: float) -> None:
        self.base_environment = dict(base_environment)
        self.stop_grace_seconds = stop_grace_seconds

    def run(self, launch: Launch) -> Ended:
        """Run `launch` in a session of its own within its bound, then end and reap everything it
        left running."""

        environment = {**self.base_environment, **launch.environment}
        launch.log.parent.mkdir(parents=True, exist_ok=True)
        started = time.monotonic()
        with launch.log.open("wb") as log:
            stdout: IO[bytes] = log
            if launch.stdout is not None:
                stdout = launch.stdout.open("wb")
            try:
                try:
                    process = subprocess.Popen(
                        list(launch.argv),
                        cwd=launch.cwd,
                        env=environment,
                        stdin=subprocess.DEVNULL,
                        stdout=stdout,
                        stderr=log,
                        start_new_session=True,
                    )
                except OSError as error:
                    raise LaneError(f"{launch.name}: cannot start {launch.argv[0]}: {error}") from error
                timed_out = False
                try:
                    process.wait(timeout=max(launch.bound_seconds, 0.0))
                except subprocess.TimeoutExpired:
                    self.stop(process, self.stop_grace_seconds)
                    timed_out = True
                except Interrupted:
                    # The native coverage collector kills the whole recipe one grace period after it
                    # forwards a signal, so an interrupted lane ends its process sooner.
                    self.stop(process, min(self.stop_grace_seconds, 10.0))
                    self.reap_leftovers(process.pid)
                    raise
            finally:
                if stdout is not log:
                    stdout.close()
        seconds = time.monotonic() - started
        leftovers = self.reap_leftovers(process.pid)
        if timed_out:
            return Ended(process.pid, Ending.TIMED_OUT, None, None, seconds, leftovers)
        returncode = process.returncode
        if returncode < 0:
            return Ended(process.pid, Ending.SIGNALED, None, -returncode, seconds, leftovers)
        return Ended(process.pid, Ending.EXITED, returncode, None, seconds, leftovers)

    def stop(self, process: subprocess.Popen[bytes], grace: float) -> None:
        """End a process and its whole group: SIGTERM, then SIGKILL after the grace period."""

        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=grace)
        except subprocess.TimeoutExpired:
            pass
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()

    def reap_leftovers(self, group: int) -> tuple[int, ...]:
        """Kill and reap what a launch left running: members of its process group, and orphans that
        left the group and were adopted by the lane. Ended processes are reaped without counting."""

        own = os.getpid()
        leftovers: set[int] = set()
        for _ in range(REAP_POLLS):
            pending = False
            for pid, state in process_states().items():
                if pid == own or (state.group != group and state.parent != own):
                    continue
                if state.is_running():
                    leftovers.add(pid)
                    pending = True
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        continue
                elif state.parent == own:
                    try:
                        os.waitpid(pid, os.WNOHANG)
                    except ChildProcessError:
                        pass
                else:
                    pending = True
            if not pending:
                break
            time.sleep(REAP_POLL_SECONDS)
        return tuple(sorted(leftovers))

    def capture(
        self, argv: Sequence[str], cwd: Path, environment: Mapping[str, str] | None = None
    ) -> tuple[int, str, str]:
        merged = {**self.base_environment, **(environment or {})}
        completed = subprocess.run(
            list(argv), cwd=cwd, env=merged, capture_output=True, text=True, check=False
        )
        return completed.returncode, completed.stdout, completed.stderr


# Evidence.


@dataclass(frozen=True)
class EvidenceCounts:
    """The counts `nervix-deadlock-report qualify` prints for one evidence file."""

    scope: str
    findings: int
    active: int
    potential: int
    unreviewed: int
    nonqualifying: int
    repeated_deliveries: int
    lost_handoff: int
    lost_order_history: int
    lost_retention: int

    def lost(self) -> int:
        return self.lost_handoff + self.lost_order_history + self.lost_retention

    def describe(self) -> dict[str, object]:
        return {
            "scope": self.scope,
            "findings": self.findings,
            "active": self.active,
            "potential": self.potential,
            "unreviewed": self.unreviewed,
            "nonqualifying": self.nonqualifying,
            "repeated_deliveries": self.repeated_deliveries,
            "lost_handoff": self.lost_handoff,
            "lost_order_history": self.lost_order_history,
            "lost_retention": self.lost_retention,
        }


def parse_summary(output: str) -> EvidenceCounts | None:
    """The counts the report tool printed, or `None` when it printed no complete summary line: every
    key once, in its fixed order, each count a decimal number."""

    for line in output.splitlines():
        if not line.startswith(SUMMARY_PREFIX):
            continue
        keys: list[str] = []
        values: list[str] = []
        for pair in line.removeprefix(SUMMARY_PREFIX).split(" "):
            key, separator, value = pair.partition("=")
            if not separator or not value:
                return None
            keys.append(key)
            values.append(value)
        if tuple(keys) != SUMMARY_KEYS:
            return None
        counts = values[1:]
        if not all(count.isdigit() for count in counts):
            return None
        numbers = [int(count) for count in counts]
        return EvidenceCounts(values[0], *numbers)
    return None


@dataclass(frozen=True)
class Qualified:
    """One process's evidence and what qualifying it found."""

    file: Path
    status: int
    counts: EvidenceCounts | None
    description: Path | None

    def qualifies(self) -> bool:
        return self.status == 0

    def active(self) -> bool:
        return self.counts is not None and self.counts.active > 0

    def describe(self, root: Path) -> dict[str, object]:
        described: dict[str, object] = {
            "file": display(self.file, root),
            "qualifies": self.qualifies(),
            "status": self.status,
            "counts": self.counts.describe() if self.counts is not None else None,
        }
        if self.description is not None:
            described["description"] = display(self.description, root)
        return described


@dataclass
class Totals:
    """Every process observation of a run, and the findings they hold."""

    observations: int = 0
    qualifying: int = 0
    active: int = 0
    potential: int = 0
    unreviewed: int = 0
    repeated_deliveries: int = 0
    lost_handoff: int = 0
    lost_order_history: int = 0
    lost_retention: int = 0

    def add(self, qualified: Qualified) -> None:
        self.observations += 1
        if qualified.qualifies():
            self.qualifying += 1
        counts = qualified.counts
        if counts is None:
            return
        self.active += counts.active
        self.potential += counts.potential
        self.unreviewed += counts.unreviewed
        self.repeated_deliveries += counts.repeated_deliveries
        self.lost_handoff += counts.lost_handoff
        self.lost_order_history += counts.lost_order_history
        self.lost_retention += counts.lost_retention

    def lost(self) -> int:
        return self.lost_handoff + self.lost_order_history + self.lost_retention

    def describe(self) -> dict[str, int]:
        return {
            "observations": self.observations,
            "qualifying": self.qualifying,
            "active": self.active,
            "potential": self.potential,
            "unreviewed": self.unreviewed,
            "repeated_deliveries": self.repeated_deliveries,
            "lost_handoff": self.lost_handoff,
            "lost_order_history": self.lost_order_history,
            "lost_retention": self.lost_retention,
        }

    def line(self) -> str:
        return (
            f"{self.observations} process observations, {self.qualifying} qualify; findings: "
            f"active {self.active}, potential {self.potential} ({self.unreviewed} unreviewed), "
            f"repeated deliveries {self.repeated_deliveries}, lost {self.lost()} (handoff "
            f"{self.lost_handoff}, order history {self.lost_order_history}, retention "
            f"{self.lost_retention})"
        )


def evidence_files(directory: Path) -> tuple[list[Path], list[Path]]:
    """Every complete and every partly written evidence file below `directory`."""

    if not directory.is_dir():
        return [], []
    complete = sorted(path for path in directory.rglob(EVIDENCE_PATTERN) if path.is_file())
    partial = sorted(path for path in directory.rglob(PARTIAL_PATTERN) if path.is_file())
    return complete, partial


# The run.


class Failure(enum.StrEnum):
    """Why a run failed, in the words its record and its status use."""

    PREREQUISITE = "prerequisite-missing"
    INVENTORY = "inventory"
    BUILD = "build-failed"
    ACTIVE_DEADLOCK = "active-deadlock"
    DIAGNOSTIC_FAILURE = "diagnostic-failure"
    SIGNALED = "signaled"
    TIMED_OUT = "timed-out"
    FAILED = "failed"
    INCOMPLETE = "incomplete"
    LEFTOVER = "leftover-processes"
    EVIDENCE_MISSING = "evidence-missing"
    EVIDENCE_PARTIAL = "evidence-partial"
    EVIDENCE_UNQUALIFIED = "evidence-unqualified"
    BUDGET = "budget-expired"

    def status(self) -> int:
        if self is Failure.ACTIVE_DEADLOCK:
            return ACTIVE_DEADLOCK_STATUS
        if self is Failure.DIAGNOSTIC_FAILURE:
            return DIAGNOSTIC_FAILURE_STATUS
        if self in {Failure.TIMED_OUT, Failure.BUDGET}:
            return BUDGET_STATUS
        return FAILURE_STATUS


def ending_failure(ended: Ended) -> Failure | None:
    """The failure a process's ending is, before its tests are accounted for: `None` for a clean
    exit that left nothing running."""

    if ended.ending is Ending.TIMED_OUT:
        return Failure.TIMED_OUT
    if ended.ending is Ending.SIGNALED:
        return Failure.SIGNALED
    if ended.status == ACTIVE_DEADLOCK_STATUS:
        return Failure.ACTIVE_DEADLOCK
    if ended.status == DIAGNOSTIC_FAILURE_STATUS:
        return Failure.DIAGNOSTIC_FAILURE
    if ended.status == BUDGET_STATUS:
        return Failure.TIMED_OUT
    if ended.status != 0:
        return Failure.FAILED
    if ended.leftovers:
        return Failure.LEFTOVER
    return None


def describe_ending(ended: Ended) -> str:
    if ended.ending is Ending.TIMED_OUT:
        return f"outlived its bound after {ended.seconds:.0f}s"
    if ended.ending is Ending.SIGNALED and ended.signal is not None:
        return f"was killed by {signal_name(ended.signal)}"
    described = f"exited with status {ended.status}"
    if ended.leftovers:
        described += f" and left processes running: {', '.join(map(str, ended.leftovers))}"
    return described


class LaneFailed(Exception):
    """A workload or its evidence failed; the run stops and records why."""

    def __init__(self, failure: Failure, detail: str) -> None:
        super().__init__(detail)
        self.failure = failure
        self.detail = detail


@dataclass(frozen=True)
class Workspace:
    """Where the lane builds, where its prerequisites were built, and where attempts go."""

    root: Path
    build: Path
    prepared: Path
    instrumented: bool
    # Cargo's configured runner for this host, which every test executable starts through.
    runner: tuple[str, ...]

    def attempts(self, selection: str) -> Path:
        return self.build / ATTEMPTS / selection

    def report_tool(self) -> Path:
        return self.prepared / "debug" / "nervix-deadlock-report"


def host_triple(processes: Processes, root: Path) -> str:
    status, output, error = processes.capture(["rustc", "-vV"], root)
    if status != 0:
        raise LaneError(f"rustc -vV failed: {error.strip()}")
    for line in output.splitlines():
        if line.startswith("host: "):
            return line.removeprefix("host: ").strip()
    raise LaneError("rustc -vV names no host")


def runner_variable(host: str) -> str:
    return "CARGO_TARGET_" + host.upper().replace("-", "_").replace(".", "_") + "_RUNNER"


def resolve_workspace(
    root: Path, target: Path, environment: Mapping[str, str], host: str
) -> Workspace:
    instrumented = COVERAGE_ATTEMPT_VARIABLE in environment
    # A plain run keeps its diagnostic builds apart from the ordinary ones, so the diagnostic
    # server binary never replaces the ordinary one; the collector already chose its own directory.
    if instrumented:
        build = target
    else:
        build = target / "deloxide"
    prepared = Path(environment.get(PREPARED_TARGET_VARIABLE, str(target)))
    runner = tuple(environment.get(runner_variable(host), "").split())
    return Workspace(
        root=root, build=build, prepared=prepared, instrumented=instrumented, runner=runner
    )


def display(path: Path, root: Path) -> str:
    if path.is_relative_to(root):
        return str(path.relative_to(root))
    return str(path)


def utc_now() -> str:
    return datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds")


def locked_version(root: Path, package: str) -> dict[str, str] | None:
    lock = tomllib.loads((root / "Cargo.lock").read_text(encoding="utf-8"))
    for entry in lock.get("package", []):
        if entry.get("name") == package:
            return {"version": entry.get("version", ""), "source": entry.get("source", "")}
    return None


def file_digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


class Record:
    """`lane.json` of an attempt, rewritten atomically after every stage, and copied where the
    native coverage collector reads it."""

    def __init__(self, path: Path, report: Path | None, content: dict[str, object]) -> None:
        self.path = path
        self.report = report
        self.content = content

    def write(self) -> None:
        text = json.dumps(self.content, indent=2) + "\n"
        for destination in (self.path, self.report):
            if destination is None:
                continue
            temporary = destination.with_name(f".{destination.name}.tmp")
            temporary.write_text(text, encoding="utf-8")
            os.replace(temporary, destination)


@dataclass
class Counts:
    discovered: int = 0
    selected: int = 0
    executed: int = 0
    completed: int = 0

    def describe(self) -> dict[str, int]:
        return {
            "discovered": self.discovered,
            "selected": self.selected,
            "executed": self.executed,
            "completed": self.completed,
        }


def qualify_file(processes: Processes, workspace: Workspace, attempt: Path, file: Path) -> Qualified:
    """Qualify one evidence file, and keep the description of what it holds with its source sites
    when it holds a finding or does not qualify."""

    tool = workspace.report_tool()
    status, output, error = processes.capture([str(tool), "qualify", str(file)], workspace.root)
    counts = parse_summary(output)
    description: Path | None = None
    if status != 0 or counts is None or counts.findings > 0:
        _, inspected, inspect_error = processes.capture([str(tool), "inspect", str(file)], workspace.root)
        name = file.relative_to(attempt).as_posix().replace("/", "__")
        description = attempt / "findings" / f"{name}.txt"
        description.parent.mkdir(parents=True, exist_ok=True)
        description.write_text(output + error + inspected + inspect_error, encoding="utf-8")
    # A qualification that printed no summary is a tool failure, never a clean observation.
    if status == 0 and counts is None:
        status = DIAGNOSTIC_FAILURE_STATUS
    return Qualified(file=file, status=status, counts=counts, description=description)


def publish_step_summary(environment: Mapping[str, str], lines: Sequence[str]) -> None:
    """Add the lane's verdict to the CI job's summary, when the job keeps one."""

    path = environment.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as summary:
        summary.write("```\n" + "\n".join(lines) + "\n```\n")


def scenario_summary(output: str) -> dict[str, int] | None:
    """Cucumber's last scenario summary: the total and the count of each outcome."""

    summary: dict[str, int] | None = None
    for line in output.splitlines():
        match = _SCENARIO_SUMMARY.match(line.strip())
        if match is None:
            continue
        found = {"total": int(match.group("total"))}
        for part in match.group("parts").split(","):
            outcome = _SUMMARY_PART.match(part.strip())
            if outcome is None:
                found = {}
                break
            found[outcome.group("kind")] = int(outcome.group("count"))
        if found:
            summary = found
        else:
            summary = None
    return summary


def tail(log: Path, lines: int = 60) -> None:
    try:
        text = log.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return
    for line in text[-lines:]:
        print(line, flush=True)


class Lane:
    """One selection's run of every registered workload, in a fresh attempt directory."""

    def __init__(
        self,
        inventory: Inventory,
        selection: Selection,
        workspace: Workspace,
        processes: Processes,
        report: Path | None = None,
        clock: Callable[[], float] = time.monotonic,
        prefix: str = "run.",
    ) -> None:
        self.inventory = inventory
        self.selection = selection
        self.workspace = workspace
        self.processes = processes
        self.clock = clock
        self.deadline = clock() + inventory.bounds.budget_seconds
        attempts = workspace.attempts(selection.name)
        attempts.mkdir(parents=True, exist_ok=True)
        self.attempt = Path(tempfile.mkdtemp(prefix=prefix, dir=attempts))
        self.record = Record(self.attempt / RECORD, report, {})
        self.counts = Counts()
        self.totals = Totals()
        self.launches: list[dict[str, object]] = []
        self.workloads: list[dict[str, object]] = []
        self.evidence: list[dict[str, object]] = []

    @property
    def root(self) -> Path:
        return self.workspace.root

    def label(self) -> str:
        return f"deloxide lane ({self.selection.name})"

    def remaining(self) -> float:
        return self.deadline - self.clock()

    def bound(self, seconds: float) -> float:
        remaining = self.remaining()
        if remaining <= 0:
            raise LaneFailed(Failure.BUDGET, "the lane's budget expired before its next stage")
        return min(seconds, remaining)

    def begin(self) -> None:
        root = self.root
        status, revision, _ = self.processes.capture(["git", "rev-parse", "HEAD"], root)
        _, changes, _ = self.processes.capture(["git", "status", "--porcelain"], root)
        _, rustc, _ = self.processes.capture(["rustc", "-vV"], root)
        _, cargo, _ = self.processes.capture(["cargo", "-V"], root)
        commit: str | None = None
        if status == 0:
            commit = revision.strip()
        bounds = self.inventory.bounds
        self.record.content = {
            "lane": "deloxide",
            "selection": self.selection.name,
            "recorded_selection": self.selection.recorded,
            "verdict": "running",
            "started_at": utc_now(),
            "revision": {"commit": commit, "working_tree_modified": bool(changes.strip())},
            "toolchain": rustc.strip(),
            "cargo": cargo.strip(),
            "dependencies": {"deloxide": locked_version(root, "deloxide")},
            "inventory": {"path": str(INVENTORY), "sha256": file_digest(root / INVENTORY)},
            "bounds": {
                "budget_seconds": bounds.budget_seconds,
                "stop_grace_seconds": bounds.stop_grace_seconds,
                "suite_teardown_reserve_seconds": bounds.suite_teardown_reserve_seconds,
            },
            "workspace": {
                "attempt": display(self.attempt, root),
                "build_target": display(self.workspace.build, root),
                "prepared_target": display(self.workspace.prepared, root),
                "instrumented": self.workspace.instrumented,
                "runner": list(self.workspace.runner),
            },
            "launches": self.launches,
            "workloads": self.workloads,
            "evidence": self.evidence,
        }
        self.record.write()

    def base_environment(self) -> dict[str, str]:
        prepared = self.workspace.prepared / "debug"
        return {
            "CARGO_TARGET_DIR": str(self.workspace.build),
            REPORT_TOOL_VARIABLE: str(self.workspace.report_tool()),
            "NERVIX_TEST_CLI_PATH": str(prepared / "nervix-cli"),
        }

    def onnx_runtime(self) -> str:
        status, output, error = self.processes.capture(
            ["bash", "scripts/download_onnxruntime.sh", "--print-path"], self.root
        )
        if status != 0 or not output.strip():
            raise LaneFailed(Failure.PREREQUISITE, f"the ONNX runtime path is unknown: {error.strip()}")
        return output.strip()

    def execute(self) -> int:
        """Run every invocation and qualify its evidence; return the lane's status."""

        self.begin()
        try:
            if not self.workspace.report_tool().is_file():
                raise LaneFailed(
                    Failure.PREREQUISITE,
                    f"{self.workspace.report_tool()} is missing; `just tests-deps` builds it",
                )
            onnx = self.onnx_runtime()
            for invocation in self.inventory.invocations.values():
                self.run_invocation(invocation, onnx)
            if self.totals.observations == 0:
                raise LaneFailed(Failure.EVIDENCE_MISSING, "no process recorded evidence")
        except LaneFailed as failed:
            return self.conclude(failed.failure, failed.detail)
        except LaneError as error:
            # A launch whose executable could not start.
            return self.conclude(Failure.PREREQUISITE, str(error))
        except Interrupted as interruption:
            self.record.content["verdict"] = "interrupted"
            self.record.content["failure"] = {
                "class": "interrupted",
                "detail": f"interrupted by {interruption}",
            }
            self.finish()
            attempt = display(self.attempt, self.root)
            print(f"{self.label()}: interrupted by {interruption}; attempt {attempt}", file=sys.stderr)
            return 128 + interruption.number
        return self.conclude(None, "")

    def finish(self) -> None:
        self.record.content["counts"] = self.counts.describe()
        self.record.content["findings"] = self.totals.describe()
        self.record.content["finished_at"] = utc_now()
        self.record.write()

    def conclude(self, failure: Failure | None, detail: str) -> int:
        if failure is None:
            self.record.content["verdict"] = "complete"
        else:
            self.record.content["verdict"] = "failed"
            self.record.content["failure"] = {"class": str(failure), "detail": detail}
        self.finish()
        counts = self.counts
        accounted = (
            f"{self.label()}: discovered {counts.discovered}, selected {counts.selected}, executed "
            f"{counts.executed}, completed {counts.completed}"
        )
        observed = f"{self.label()}: evidence: {self.totals.line()}"
        print(accounted, flush=True)
        print(observed, flush=True)
        attempt = display(self.attempt, self.root)
        if failure is None:
            verdict = [f"{self.label()}: complete; attempt {attempt}"]
            print(verdict[0], flush=True)
        else:
            verdict = [
                f"{self.label()}: {failure}: {detail}",
                f"{self.label()}: attempt retained at {attempt}",
            ]
            for line in verdict:
                print(line, file=sys.stderr)
        publish_step_summary(self.processes.base_environment, [accounted, observed, *verdict])
        if failure is None:
            return 0
        return failure.status()

    # Building.

    def cargo_build(self, name: str, arguments: Sequence[str]) -> list[dict[str, object]]:
        """Build with Cargo within the budget, and return its JSON messages."""

        launch = Launch(
            name=name,
            argv=("cargo", *arguments, "--message-format=json-render-diagnostics"),
            cwd=self.root,
            environment=self.base_environment(),
            bound_seconds=self.bound(self.remaining()),
            log=self.attempt / f"{name}.log",
            stdout=self.attempt / f"{name}.jsonl",
        )
        print(f"{self.label()}: {name}", flush=True)
        ended = self.processes.run(launch)
        self.launches.append({"stage": "build", **launch.describe(self.root), "ended": ended.describe()})
        self.record.write()
        if ended.ending is Ending.TIMED_OUT:
            raise LaneFailed(Failure.BUDGET, f"{name} outlived the lane's budget; see {launch.log}")
        if ended.ending is not Ending.EXITED or ended.status != 0:
            tail(launch.log)
            raise LaneFailed(Failure.BUILD, f"{name} {describe_ending(ended)}; see {launch.log}")
        messages: list[dict[str, object]] = []
        stdout = launch.stdout or launch.log
        for line in stdout.read_text(encoding="utf-8", errors="replace").splitlines():
            if line.startswith("{"):
                messages.append(json.loads(line))
        return messages

    def build_tests(self, invocation: Invocation) -> tuple[Path, Path]:
        """The test executable of `invocation` in this selection, and the package directory Cargo
        runs it in."""

        features = " ".join([*invocation.features, self.selection.name])
        messages = self.cargo_build(
            f"{invocation.id}-build",
            ["test", "--no-run", "--package", invocation.package, "--features", features,
             *invocation.cargo_target()],
        )
        for message in messages:
            if message.get("reason") != "compiler-artifact":
                continue
            target = message.get("target")
            profile = message.get("profile")
            executable = message.get("executable")
            if not isinstance(target, dict) or not isinstance(profile, dict) or not executable:
                continue
            if profile.get("test") is not True or not invocation.builds(target):
                continue
            manifest = Path(str(message.get("manifest_path")))
            return Path(str(executable)), manifest.parent
        raise LaneFailed(Failure.BUILD, f"{invocation.id}: the build produced no test executable")

    def build_driver(self, package: str) -> Path:
        messages = self.cargo_build(
            f"{package}-build", ["build", "--package", package, "--features", self.selection.name]
        )
        for message in messages:
            if message.get("reason") != "compiler-artifact":
                continue
            target = message.get("target")
            executable = message.get("executable")
            if not isinstance(target, dict) or not executable:
                continue
            if target.get("kind") == ["bin"] and target.get("name") == package:
                return Path(str(executable))
        raise LaneFailed(Failure.BUILD, f"{package}: the build produced no executable")

    # Running.

    def command(self, executable: Path, arguments: Sequence[str]) -> tuple[str, ...]:
        return (*self.workspace.runner, str(executable), *arguments)

    def run_invocation(self, invocation: Invocation, onnx: str) -> None:
        registered = self.inventory.workloads_of(invocation.id, self.selection.name)
        executable, cwd = self.build_tests(invocation)
        environment = {
            **self.base_environment(),
            "ORT_DYLIB_PATH": onnx,
            "CARGO_MANIFEST_DIR": str(cwd),
        }
        evidence = self.attempt / "evidence" / invocation.id
        if invocation.evidence:
            evidence.mkdir(parents=True, exist_ok=True)
            environment[EVIDENCE_VARIABLE] = str(evidence)
        if invocation.artifacts:
            environment[PROBE_ARTIFACTS_VARIABLE] = str(self.attempt / f"{invocation.id}-artifacts")
        if invocation.kind is Kind.CUCUMBER:
            self.run_scenarios(invocation, registered, executable, cwd, environment)
        else:
            self.run_tests(invocation, registered, executable, cwd, environment)
        if invocation.evidence:
            self.qualify_evidence(invocation, evidence)

    def discover_tests(self, invocation: Invocation, executable: Path, cwd: Path) -> tuple[list[str], list[str]]:
        listings: list[list[str]] = []
        for extra in ([], ["--ignored"]):
            status, output, error = self.processes.capture(
                [str(executable), "--list", "--format", "terse", *extra], cwd, self.base_environment()
            )
            if status != 0:
                raise LaneFailed(Failure.BUILD, f"{invocation.id}: listing its tests failed: {error.strip()}")
            listings.append(listed_tests(output))
        return listings[0], listings[1]

    def run_tests(
        self,
        invocation: Invocation,
        registered: Sequence[Workload],
        executable: Path,
        cwd: Path,
        environment: Mapping[str, str],
    ) -> None:
        listed, listed_ignored = self.discover_tests(invocation, executable, cwd)
        ignored = set(listed_ignored)
        discovered = [test for test in listed if invocation.registers(test) and test not in ignored]
        self.counts.discovered += len(discovered)
        problems = test_problems(invocation, registered, listed, listed_ignored)
        if problems:
            raise LaneFailed(Failure.INVENTORY, "\n".join(problems))
        self.counts.selected += len(registered)
        if invocation.kind is Kind.LIBTEST:
            launch = Launch(
                name=invocation.id,
                argv=self.command(executable, []),
                cwd=cwd,
                environment=environment,
                bound_seconds=self.bound(invocation.bound_seconds),
                log=self.attempt / f"{invocation.id}.log",
            )
            self.supervise_tests(invocation, registered, launch)
            return
        for workload in registered:
            test_environment = dict(environment)
            if invocation.evidence:
                directory = Path(environment[EVIDENCE_VARIABLE]) / workload.id
                directory.mkdir(parents=True, exist_ok=True)
                test_environment[EVIDENCE_VARIABLE] = str(directory)
            launch = Launch(
                name=f"{invocation.id}-{workload.id}",
                argv=self.command(executable, [workload.test or "", "--exact", "--test-threads=1"]),
                cwd=cwd,
                environment=test_environment,
                bound_seconds=self.bound(invocation.bound_seconds),
                log=self.attempt / f"{invocation.id}-{workload.id}.log",
            )
            self.supervise_tests(invocation, [workload], launch)

    def supervise_tests(self, invocation: Invocation, registered: Sequence[Workload], launch: Launch) -> None:
        print(f"{self.label()}: {launch.name}", flush=True)
        ended = self.processes.run(launch)
        report = libtest_accounting.report(launch.log.read_text(encoding="utf-8", errors="replace"))
        expected = {workload.test or "" for workload in registered}
        problems: list[str] = []
        for workload in registered:
            outcome = report.outcomes.get(workload.test or "")
            self.workloads.append({
                "id": workload.id,
                "invocation": invocation.id,
                "test": workload.test,
                "outcome": outcome,
                "completed": outcome == "ok",
            })
            if outcome is None:
                problems.append(f"{launch.name}: {workload.id} did not run: {workload.test}")
            elif outcome == "ignored":
                problems.append(f"{launch.name}: {workload.id} ran ignored: {workload.test}")
            elif outcome != "ok":
                problems.append(f"{launch.name}: {workload.id} failed: {workload.test}")
        for test, outcome in sorted(report.outcomes.items()):
            if test in expected:
                continue
            if test in invocation.ignored and outcome == "ignored":
                continue
            if invocation.registers(test):
                problems.append(f"{launch.name}: {test} ran but is not registered")
        counts = report.counts
        if counts is None:
            problems.append(f"{launch.name}: libtest printed no result; the process ended before its tests did")
        else:
            self.counts.executed += counts.executed
            self.counts.completed += counts.completed
            if counts.executed == 0:
                problems.append(f"{launch.name}: executed no test")
        self.record_launch(invocation, launch, ended, {"outcomes": dict(report.outcomes)})
        self.check(launch, ended, problems)

    def run_scenarios(
        self,
        invocation: Invocation,
        registered: Sequence[Workload],
        executable: Path,
        cwd: Path,
        environment: Mapping[str, str],
    ) -> None:
        discovered = discover_scenarios(self.root, invocation.inputs, invocation.tags)
        self.counts.discovered += sum(scenario.runs for scenario in discovered)
        problems = scenario_problems(invocation, registered, discovered)
        if problems:
            raise LaneFailed(Failure.INVENTORY, "\n".join(problems))
        expected = sum(workload.examples or 0 for workload in registered)
        self.counts.selected += expected
        scenario_environment = dict(environment)
        if invocation.driver is not None:
            driver = self.build_driver(invocation.driver)
            scenario_environment["NERVIX_PACED_SIMULATION_PATH"] = str(driver)
            library = self.workspace.prepared / "debug" / "libnervix_client_ffi.so"
            scenario_environment["NERVIX_CLIENT_LIBRARY"] = str(library)
        bound = self.bound(invocation.bound_seconds)
        suite_budget = int(bound) - self.inventory.bounds.suite_teardown_reserve_seconds
        if suite_budget <= 0:
            raise LaneFailed(
                Failure.BUDGET,
                f"{invocation.id}: the budget left cannot hold the suite and its teardown reserve",
            )
        scenario_environment[SUITE_BUDGET_VARIABLE] = f"{suite_budget}s"
        arguments: list[str] = []
        for pattern in invocation.inputs:
            arguments.extend(["--input", pattern])
        arguments.extend(["--tags", " or ".join(invocation.tags), "--retry", "0"])
        if invocation.concurrency is not None:
            arguments.extend(["--concurrency", str(invocation.concurrency)])
        launch = Launch(
            name=invocation.id,
            argv=self.command(executable, arguments),
            cwd=cwd,
            environment=scenario_environment,
            bound_seconds=bound,
            log=self.attempt / f"{invocation.id}.log",
        )
        print(f"{self.label()}: {launch.name}: {expected} scenario runs", flush=True)
        ended = self.processes.run(launch)
        self.retain_suite_logs(invocation)
        summary = scenario_summary(launch.log.read_text(encoding="utf-8", errors="replace"))
        total = 0
        passed = 0
        if summary is not None:
            total = summary.get("total", 0)
            passed = summary.get("passed", 0)
        self.counts.executed += total
        self.counts.completed += passed
        complete = total == expected and passed == expected
        for workload in registered:
            self.workloads.append({
                "id": workload.id,
                "invocation": invocation.id,
                "scenario": workload.scenario,
                "runs": workload.examples,
                "completed": complete,
            })
        self.record_launch(invocation, launch, ended, {"scenarios": summary, "expected": expected})
        problems = []
        if not complete:
            problems.append(
                f"{invocation.id}: the tags select {expected} scenario runs, and Cucumber reported "
                f"{summary if summary is not None else 'no summary'}"
            )
        self.check(launch, ended, problems)

    def retain_suite_logs(self, invocation: Invocation) -> None:
        logs = self.root / "tests" / "logs"
        if logs.is_dir():
            shutil.copytree(logs, self.attempt / f"{invocation.id}-logs", dirs_exist_ok=True)

    def record_launch(
        self, invocation: Invocation, launch: Launch, ended: Ended, accounting: Mapping[str, object]
    ) -> None:
        self.launches.append({
            "stage": "run",
            "invocation": invocation.id,
            "kind": str(invocation.kind),
            **launch.describe(self.root),
            "ended": ended.describe(),
            "accounting": dict(accounting),
        })
        self.record.write()

    def check(self, launch: Launch, ended: Ended, problems: Sequence[str]) -> None:
        """Fail the lane when the process ended badly or its accounting found a problem.

        A node that deadlocks ends its own process with status 3 and fails the step that waited on
        it, so a failed invocation first looks for an active cycle in the evidence it recorded.
        """

        failure = ending_failure(ended)
        if failure is None and not problems:
            return
        tail(launch.log)
        lines = list(problems)
        if failure is None:
            failure = Failure.INCOMPLETE
        else:
            lines.insert(0, f"{launch.name} {describe_ending(ended)}; see {display(launch.log, self.root)}")
        directory = launch.environment.get(EVIDENCE_VARIABLE)
        if directory is not None:
            for file in evidence_files(Path(directory))[0]:
                qualified = qualify_file(self.processes, self.workspace, self.attempt, file)
                self.evidence.append(qualified.describe(self.root))
                self.totals.add(qualified)
                if qualified.active():
                    failure = Failure.ACTIVE_DEADLOCK
                    where = display(qualified.description or file, self.root)
                    lines.insert(0, f"{display(file, self.root)} records an active deadlock; see {where}")
        raise LaneFailed(failure, "\n".join(lines))

    def qualify_evidence(self, invocation: Invocation, directory: Path) -> None:
        complete, partial = evidence_files(directory)
        if partial:
            names = ", ".join(display(path, self.root) for path in partial)
            raise LaneFailed(Failure.EVIDENCE_PARTIAL, f"{invocation.id}: partly written evidence: {names}")
        if not complete:
            raise LaneFailed(Failure.EVIDENCE_MISSING, f"{invocation.id}: no process recorded evidence in {directory}")
        if invocation.kind is Kind.LIBTEST_EACH:
            for workload in self.inventory.workloads_of(invocation.id, self.selection.name):
                files = evidence_files(directory / workload.id)[0]
                if len(files) != 1:
                    raise LaneFailed(
                        Failure.EVIDENCE_MISSING,
                        f"{invocation.id}: {workload.id} recorded {len(files)} evidence files, not one",
                    )
        unqualified: list[str] = []
        active: list[str] = []
        for file in complete:
            qualified = qualify_file(self.processes, self.workspace, self.attempt, file)
            self.evidence.append(qualified.describe(self.root))
            self.totals.add(qualified)
            if qualified.qualifies():
                continue
            where = display(qualified.description or file, self.root)
            if qualified.active():
                active.append(where)
            unqualified.append(f"{display(file, self.root)} (status {qualified.status}; {where})")
        self.record.write()
        print(f"{self.label()}: {invocation.id}: {len(complete)} process observations", flush=True)
        if active:
            raise LaneFailed(Failure.ACTIVE_DEADLOCK, f"{invocation.id}: active deadlock recorded in {', '.join(active)}")
        if unqualified:
            raise LaneFailed(
                Failure.EVIDENCE_UNQUALIFIED,
                f"{invocation.id}: evidence does not qualify: {'; '.join(unqualified)}",
            )


# Completion evidence for the native coverage collector.


class RecordError(Exception):
    """A lane record that does not prove a complete run."""


def read_complete(path: Path, selection: str) -> dict[str, object]:
    """The record of a complete run of `selection`, which the collector requires before export:
    every selected workload completed and every process observation qualified."""

    try:
        content = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise RecordError(f"cannot read the Deloxide lane record at {path}: {error}") from error
    if not isinstance(content, dict):
        raise RecordError("the Deloxide lane record is not an object")
    if content.get("selection") != selection:
        raise RecordError(f"the Deloxide lane record is not of the {selection} selection")
    if content.get("verdict") != "complete":
        raise RecordError("the Deloxide lane did not complete")
    counts = content.get("counts")
    if not isinstance(counts, dict):
        raise RecordError("the Deloxide lane record has no counts")
    numbers: dict[str, int] = {}
    for key in ("discovered", "selected", "executed", "completed"):
        value = counts.get(key)
        if type(value) is not int:
            raise RecordError(f"the Deloxide lane record has no {key} count")
        numbers[key] = value
    if numbers["selected"] <= 0:
        raise RecordError("the Deloxide lane selected nothing")
    if not numbers["selected"] == numbers["executed"] == numbers["completed"] <= numbers["discovered"]:
        raise RecordError("the Deloxide lane record's counts do not show every selected workload complete")
    workloads = content.get("workloads")
    if not isinstance(workloads, list) or not workloads:
        raise RecordError("the Deloxide lane record lists no workload")
    for workload in workloads:
        if not isinstance(workload, dict) or workload.get("completed") is not True:
            raise RecordError("the Deloxide lane record has a workload that did not complete")
    findings = content.get("findings")
    if not isinstance(findings, dict):
        raise RecordError("the Deloxide lane record has no findings")
    observations = findings.get("observations")
    if type(observations) is not int or observations <= 0:
        raise RecordError("the Deloxide lane record holds no process evidence")
    if findings.get("qualifying") != observations:
        raise RecordError("the Deloxide lane record holds evidence that does not qualify")
    return content


# Replay.


def replay(
    inventory: Inventory, processes: Processes, workspace: Workspace, record_path: Path, name: str
) -> int:
    """Run one recorded launch again with exactly its command, environment and bound, in a fresh
    attempt of its selection. Paths into the recorded attempt move to the fresh one."""

    try:
        content = json.loads(record_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise LaneError(f"cannot read {record_path}: {error}") from error
    selection = inventory.selection(str(content.get("selection")))
    launches = [item for item in content.get("launches", []) if item.get("name") == name]
    if len(launches) != 1:
        names = ", ".join(str(item.get("name")) for item in content.get("launches", []))
        raise LaneError(f"{record_path} records no single launch named {name}; it records {names}")
    recorded = launches[0]
    original = str(record_path.parent)
    attempts = workspace.attempts(selection.name)
    attempts.mkdir(parents=True, exist_ok=True)
    attempt = Path(tempfile.mkdtemp(prefix="replay.", dir=attempts))
    environment: dict[str, str] = {}
    for key, value in dict(recorded.get("environment", {})).items():
        text = str(value)
        if text.startswith(original):
            text = str(attempt) + text.removeprefix(original)
            if key in {EVIDENCE_VARIABLE, PROBE_EVIDENCE_VARIABLE}:
                Path(text).mkdir(parents=True, exist_ok=True)
        environment[key] = text
    commit = dict(content.get("revision", {})).get("commit")
    _, current, _ = processes.capture(["git", "rev-parse", "HEAD"], workspace.root)
    if commit != current.strip():
        print(f"deloxide replay: recorded at {commit}, replaying at {current.strip()}", flush=True)
    cwd = Path(str(recorded.get("cwd")))
    if not cwd.is_absolute():
        cwd = workspace.root / cwd
    launch = Launch(
        name=name,
        argv=tuple(str(part) for part in recorded.get("argv", [])),
        cwd=cwd,
        environment=environment,
        bound_seconds=float(recorded.get("bound_seconds", 0)),
        log=attempt / f"{name}.log",
    )
    ended = processes.run(launch)
    failure = ending_failure(ended)
    outcome = str(failure) if failure is not None else "clean"
    print(
        f"deloxide replay: {name} was recorded {recorded.get('ended')} and replayed "
        f"{ended.describe()}: {outcome}; attempt {display(attempt, workspace.root)}",
        flush=True,
    )
    replayed = {
        "replay_of": str(record_path),
        "launch": launch.describe(workspace.root),
        "ended": ended.describe(),
        "failure": str(failure) if failure is not None else None,
    }
    (attempt / RECORD).write_text(json.dumps(replayed, indent=2) + "\n", encoding="utf-8")
    if failure is None:
        return 0
    return failure.status()


# Qualification of the supervision.


@dataclass(frozen=True)
class Expected:
    """What a qualification case's evidence must show, or that it leaves none readable."""

    readable: bool
    active: int = 0
    unreviewed: bool = False
    lost: bool = False

    def matches(self, counts: EvidenceCounts) -> bool:
        return (
            counts.active == self.active
            and (counts.unreviewed > 0) == self.unreviewed
            and (counts.lost() > 0) == self.lost
        )


@dataclass(frozen=True)
class Case:
    """A probe workload, deliberately failing or clean, and how the lane must classify it."""

    workload: str
    selections: frozenset[str]
    failure: Failure | None
    bound_seconds: int
    evidence: Expected
    signal: int | None = None


BOTH_SELECTIONS = frozenset(DIAGNOSTIC_FEATURES)
CASES = (
    Case("two_mutexes_in_one_order", BOTH_SELECTIONS, None, 120, Expected(readable=True)),
    Case("two_mutexes_in_opposite_orders", BOTH_SELECTIONS, Failure.ACTIVE_DEADLOCK, 120,
         Expected(readable=True, active=1)),
    Case("evidence_that_cannot_be_recorded", BOTH_SELECTIONS, Failure.DIAGNOSTIC_FAILURE, 120,
         Expected(readable=False)),
    Case("an_untracked_wait_that_never_ends", BOTH_SELECTIONS, Failure.TIMED_OUT, 15,
         Expected(readable=True)),
    Case("an_aborted_process", BOTH_SELECTIONS, Failure.SIGNALED, 120, Expected(readable=True),
         signal=signal.SIGABRT),
    Case("serial_inversion", frozenset({"deloxide-order"}), Failure.EVIDENCE_UNQUALIFIED, 120,
         Expected(readable=True, unreviewed=True)),
    Case("retention_overload", frozenset({"deloxide-order"}), Failure.DIAGNOSTIC_FAILURE, 120,
         Expected(readable=True, unreviewed=True, lost=True)),
)


def classify(ended: Ended, qualified: Sequence[Qualified], partial: Sequence[Path]) -> Failure | None:
    """How the lane classifies one supervised process and its own evidence: an active cycle in the
    evidence first, then the process's ending, then the evidence itself."""

    if any(item.active() for item in qualified):
        return Failure.ACTIVE_DEADLOCK
    failure = ending_failure(ended)
    if failure is not None:
        return failure
    if partial:
        return Failure.EVIDENCE_PARTIAL
    if not qualified:
        return Failure.EVIDENCE_MISSING
    if any(not item.qualifies() for item in qualified):
        return Failure.EVIDENCE_UNQUALIFIED
    return None


def qualify(inventory: Inventory, processes: Processes, workspace: Workspace, selection: Selection) -> int:
    """Run every qualification case of `selection` through the lane's own supervision, then replay
    the recorded active deadlock."""

    probes = inventory.invocations[PROBES]
    lane = Lane(inventory, selection, workspace, processes, prefix="qualification.")
    lane.begin()
    cases: list[dict[str, object]] = []
    lane.record.content["qualification"] = cases
    try:
        executable, cwd = lane.build_tests(probes)
    except LaneFailed as failed:
        print(f"deloxide qualification: {failed.failure}: {failed.detail}", file=sys.stderr)
        return failed.failure.status()
    problems: list[str] = []
    replayable: Launch | None = None
    for case in CASES:
        if selection.name not in case.selections:
            continue
        directory = lane.attempt / "cases" / case.workload
        evidence = directory / "evidence"
        evidence.mkdir(parents=True)
        launch = Launch(
            name=f"case-{case.workload}",
            argv=lane.command(executable, ["--exact", "workload", "--ignored", "--nocapture", "--test-threads=1"]),
            cwd=cwd,
            environment={
                PROBE_WORKLOAD_VARIABLE: case.workload,
                PROBE_EVIDENCE_VARIABLE: str(evidence),
                "CARGO_MANIFEST_DIR": str(cwd),
            },
            bound_seconds=case.bound_seconds,
            log=directory / "output.log",
        )
        ended = processes.run(launch)
        complete, partial = evidence_files(evidence)
        qualified = [qualify_file(processes, workspace, lane.attempt, file) for file in complete]
        observed = classify(ended, qualified, partial)
        survivors = running_members(ended.pid)
        case_problems: list[str] = []
        if observed is not case.failure:
            case_problems.append(f"classified {observed or 'clean'}, not {case.failure or 'clean'}")
        if case.signal is not None and ended.signal != case.signal:
            case_problems.append(f"{describe_ending(ended)}, not killed by {signal_name(case.signal)}")
        if case.evidence.readable:
            if len(qualified) != 1 or qualified[0].counts is None:
                case_problems.append(f"left {len(qualified)} readable evidence files, not one")
            elif not case.evidence.matches(qualified[0].counts):
                case_problems.append(f"left evidence {qualified[0].counts.describe()}, not {case.evidence}")
        elif complete:
            case_problems.append("left readable evidence where none can be written")
        if not launch.log.is_file():
            case_problems.append("retained no output")
        if ended.leftovers or survivors:
            case_problems.append(f"left processes running: {sorted({*ended.leftovers, *survivors})}")
        status = 0
        if observed is not None:
            status = observed.status()
        if case.failure is not None and status == 0:
            case_problems.append("would not fail the lane")
        cases.append({
            **launch.describe(workspace.root),
            "ended": ended.describe(),
            "expected": str(case.failure) if case.failure is not None else None,
            "observed": str(observed) if observed is not None else None,
            "lane_status": status,
            "evidence": [item.describe(workspace.root) for item in qualified],
            "problems": case_problems,
        })
        lane.launches.append({"stage": "qualification", **launch.describe(workspace.root), "ended": ended.describe()})
        lane.record.write()
        if case_problems:
            verdict = "; ".join(case_problems)
        else:
            verdict = "as required"
        print(
            f"deloxide qualification ({selection.name}): {case.workload}: {observed or 'clean'}, "
            f"lane status {status}: {verdict}",
            flush=True,
        )
        problems.extend(f"{case.workload}: {problem}" for problem in case_problems)
        if case.failure is Failure.ACTIVE_DEADLOCK:
            replayable = launch
    if replayable is None:
        problems.append("no case recorded an active deadlock to replay")
    else:
        replayed = replay(inventory, processes, workspace, lane.record.path, replayable.name)
        if replayed != ACTIVE_DEADLOCK_STATUS:
            problems.append(
                f"the replay of {replayable.name} ended with lane status {replayed}, not "
                f"{ACTIVE_DEADLOCK_STATUS}"
            )
    lane.record.content["verdict"] = "complete" if not problems else "failed"
    lane.record.content["problems"] = problems
    lane.record.write()
    for problem in problems:
        print(f"deloxide qualification ({selection.name}): {problem}", file=sys.stderr)
    attempt = display(lane.attempt, workspace.root)
    print(f"deloxide qualification ({selection.name}): attempt {attempt}", flush=True)
    if problems:
        return FAILURE_STATUS
    return 0


# Applicability.


def catalog_owners(catalog: Mapping[str, object]) -> set[str]:
    """Every source file with a tracked blocking acquisition in a diagnostic configuration of the
    compiler's acquisition catalog."""

    configurations = catalog.get("configurations")
    findings = catalog.get("findings")
    if not isinstance(configurations, list) or not isinstance(findings, list):
        raise LaneError("the catalog holds no configurations or findings")
    diagnostic: set[str] = set()
    for configuration in configurations:
        if not isinstance(configuration, dict):
            raise LaneError("the catalog holds a malformed configuration")
        features = configuration.get("features", [])
        if isinstance(features, list) and DIAGNOSTIC_FEATURES & set(features):
            diagnostic.add(str(configuration.get("name")))
    if not diagnostic:
        raise LaneError("the catalog holds no diagnostic configuration; run the full compiler matrix")
    owners: set[str] = set()
    for finding in findings:
        if not isinstance(finding, dict):
            raise LaneError("the catalog holds a malformed finding")
        for key, items in dict(finding.get("configurations", {})).items():
            name, _, _ = str(key).partition(":")
            if name not in diagnostic:
                continue
            for item in items:
                if item.get("receiver") not in TRACKED_RECEIVERS:
                    continue
                site = dict(dict(item.get("span", {})).get("site", {}))
                path = site.get("path")
                if not isinstance(path, str) or not path:
                    raise LaneError("a tracked acquisition in the catalog names no source file")
                owners.add(path)
    return owners


def applicability(inventory: Inventory, catalog: Mapping[str, object]) -> list[str]:
    owners = catalog_owners(catalog)
    problems: list[str] = []
    for path in sorted(owners - set(inventory.owners)):
        problems.append(
            f"{path} acquires tracked blocking locks in a diagnostic build and has no [[owner]] "
            f"record in {INVENTORY}: name the workloads that reach its locks, or the path the lane "
            "does not reach and why"
        )
    for path in sorted(set(inventory.owners) - owners):
        problems.append(f"{INVENTORY} records the owner {path}, which acquires no tracked lock")
    return problems


# The command line.


def repository_root() -> Path:
    completed = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"], capture_output=True, check=True, text=True
    )
    return Path(completed.stdout.strip())


def main(
    argv: Sequence[str] | None = None,
    processes_for: Callable[[Mapping[str, str], float], Processes] = Processes,
    environment: Mapping[str, str] | None = None,
    adopt_orphans: Callable[[], None] = become_subreaper,
) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", type=Path, default=None)
    parser.add_argument("--target-dir", type=Path, default=None)
    commands = parser.add_subparsers(dest="command", required=True)
    run_parser = commands.add_parser("run", help="run every registered workload of a selection")
    run_parser.add_argument("selection")
    qualify_parser = commands.add_parser("qualify", help="prove the lane's supervision on failing workloads")
    qualify_parser.add_argument("selection")
    replay_parser = commands.add_parser("replay", help="run one recorded launch again")
    replay_parser.add_argument("record", type=Path)
    replay_parser.add_argument("launch")
    applicability_parser = commands.add_parser("applicability", help="hold owner records to the compiler catalog")
    applicability_parser.add_argument("--catalog", type=Path, required=True)
    commands.add_parser("validate", help="check the inventory alone")
    arguments = parser.parse_args(argv)

    current = dict(os.environ if environment is None else environment)
    root = (arguments.root or repository_root()).resolve()
    try:
        inventory = load_inventory(root)
        if arguments.command == "validate":
            print(
                f"deloxide inventory: {len(inventory.workloads)} workloads in "
                f"{len(inventory.invocations)} invocations, {len(inventory.owners)} owners",
                flush=True,
            )
            return 0
        if arguments.command == "applicability":
            try:
                catalog = json.loads(arguments.catalog.read_text(encoding="utf-8"))
            except (OSError, ValueError) as error:
                raise LaneError(f"cannot read the compiler catalog {arguments.catalog}: {error}") from error
            problems = applicability(inventory, catalog)
            for problem in problems:
                print(f"deloxide applicability: {problem}", file=sys.stderr)
            if problems:
                return FAILURE_STATUS
            gaps = sum(owner.gap is not None for owner in inventory.owners.values())
            reached = sum(bool(owner.workloads) for owner in inventory.owners.values())
            print(
                f"deloxide applicability: {len(inventory.owners)} owners of tracked locks, "
                f"{reached} reached by registered workloads, {gaps} with a recorded gap",
                flush=True,
            )
            return 0
        if arguments.target_dir is None:
            raise LaneError(f"`{arguments.command}` needs --target-dir")
        processes = processes_for(current, inventory.bounds.stop_grace_seconds)
        host = host_triple(processes, root)
        workspace = resolve_workspace(root, arguments.target_dir.resolve(), current, host)
        adopt_orphans()
        with interruptible():
            if arguments.command == "replay":
                return replay(inventory, processes, workspace, arguments.record.resolve(), arguments.launch)
            selection = inventory.selection(arguments.selection)
            if arguments.command == "qualify":
                return qualify(inventory, processes, workspace, selection)
            report = current.get(REPORT_VARIABLE)
            report_path: Path | None = None
            if report:
                report_path = Path(report)
            lane = Lane(inventory, selection, workspace, processes, report_path)
            return lane.execute()
    except LaneError as error:
        print(f"deloxide lane: {error}", file=sys.stderr)
        return FAILURE_STATUS


if __name__ == "__main__":
    sys.exit(main())
