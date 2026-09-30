#!/usr/bin/env python3

"""Account for the tests a suite of libtest invocations discovered, selected, executed and completed.

A suite such as `just test-turmoil` runs several `cargo test` invocations, each with its own filter.
libtest reports success for an invocation whose filter matched nothing, so a renamed test or module
would silently drop out of the suite. Each invocation's output is saved to its own log, and this
reads what libtest printed in it: each test binary's outcome line for every test, and its
`test result` line, which counts its passed, failed, ignored and measured tests and the ones its
filter left out. A test that starts its own binary again in a fresh process lets that child write
its libtest output into the parent's, between the parent test's name and its outcome; the child's
lines belong to the test that started it and are not counted as the suite's.

For every log, and for the suite as a whole: discovered counts every test of the binaries that ran,
selected those the filter kept, executed those that ran rather than being ignored, and completed
those that passed. A log without a result line, as when an invocation ended before its tests did,
and a log whose invocation executed no test, fail the suite. Ignored tests are counted as selected
and not executed: a suite may start them itself in fresh processes.

With `--inventory`, each log is an invocation the inventory registers by the log's name, with the
tests it must run and the tests it must leave ignored. A registered test that did not run, or ran
ignored, fails the suite, and so does a test that ran without being registered: an invariant can
neither disappear nor join the suite unnoticed. An invocation that also runs tests which are not
the suite's invariants, such as a library's ordinary unit tests built for the suite's mode, names
the marker its invariants carry, and only a test whose name contains the marker must be
registered.

Run it as `python3 -m scripts.libtest_accounting <suite> [--inventory <path>] <log>...`.
"""

from __future__ import annotations

import argparse
import re
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

_RUNNING = re.compile(r"^running \d+ tests?$")
_TEST = re.compile(r"^test (?P<name>\S+) \.\.\.(?: (?P<rest>.*))?$")
_OUTCOME = re.compile(r"^(?P<outcome>ok|ignored|FAILED)\b")
_RESULT = re.compile(
    r"^test result: (?:ok|FAILED)\. (?P<passed>\d+) passed; (?P<failed>\d+) failed; "
    r"(?P<ignored>\d+) ignored; (?P<measured>\d+) measured; (?P<filtered>\d+) filtered out"
)


@dataclass(frozen=True)
class Counts:
    """What one invocation, or a whole suite, discovered, selected, executed and completed."""

    discovered: int = 0
    selected: int = 0
    executed: int = 0
    completed: int = 0

    def __add__(self, other: Counts) -> Counts:
        return Counts(
            discovered=self.discovered + other.discovered,
            selected=self.selected + other.selected,
            executed=self.executed + other.executed,
            completed=self.completed + other.completed,
        )

    def describe(self) -> str:
        return (
            f"discovered {self.discovered}, selected {self.selected}, executed {self.executed}, "
            f"completed {self.completed}"
        )


@dataclass(frozen=True)
class Report:
    """What the binaries of one invocation reported: every test with its outcome, and the counts of
    their result lines, or `None` when no binary finished."""

    outcomes: Mapping[str, str]
    counts: Counts | None


def binary_counts(match: re.Match[str]) -> Counts:
    passed = int(match.group("passed"))
    failed = int(match.group("failed"))
    ignored = int(match.group("ignored"))
    measured = int(match.group("measured"))
    filtered = int(match.group("filtered"))
    return Counts(
        discovered=passed + failed + ignored + measured + filtered,
        selected=passed + failed + ignored + measured,
        executed=passed + failed + measured,
        completed=passed + measured,
    )


def report(output: str) -> Report:
    """Read an invocation's libtest output, crediting only its own binaries' tests and results.

    `depth` counts the libtest runs in progress: an invocation's binary runs at depth one, and a
    fresh process a test starts runs nested inside it until its own result line. A test whose name
    was printed without an outcome waits for the outcome on a later line of its own run.
    """

    outcomes: dict[str, str] = {}
    total: Counts | None = None
    depth = 0
    waiting: str | None = None
    for raw in output.splitlines():
        line = raw.strip()
        if _RUNNING.match(line):
            depth += 1
            continue
        result = _RESULT.match(line)
        if result is not None:
            if depth == 1:
                binary = binary_counts(result)
                if total is None:
                    total = binary
                else:
                    total = total + binary
            if depth > 0:
                depth -= 1
            continue
        if depth != 1:
            continue
        test = _TEST.match(line)
        if test is not None:
            rest = test.group("rest") or ""
            outcome = _OUTCOME.match(rest)
            if outcome is None:
                waiting = test.group("name")
            else:
                outcomes[test.group("name")] = outcome.group("outcome")
                waiting = None
            continue
        if waiting is not None:
            outcome = _OUTCOME.match(line)
            if outcome is not None:
                outcomes[waiting] = outcome.group("outcome")
                waiting = None
    return Report(outcomes=outcomes, counts=total)


def counts(output: str) -> Counts | None:
    """The counts of the invocation's own binaries, or `None` when none of them finished."""

    return report(output).counts


def outcomes(output: str) -> dict[str, str]:
    """Each test the invocation's own binaries report, with its outcome: `ok`, `ignored` or
    `FAILED`."""

    return dict(report(output).outcomes)


@dataclass(frozen=True)
class Invocation:
    """The tests one invocation of a suite must run, the ones it must leave ignored, and the marker
    its invariants carry when it runs other tests too."""

    tests: frozenset[str]
    ignored: frozenset[str]
    marker: str | None

    def must_register(self, test: str) -> bool:
        return self.marker is None or self.marker in test


class InventoryError(Exception):
    """An inventory that cannot be read, which stops the accounting instead of passing it."""


def parse_inventory(text: str) -> dict[str, Invocation]:
    document = tomllib.loads(text)
    unknown = sorted(set(document) - {"invocation"})
    if unknown:
        raise InventoryError(f"unknown inventory tables: {', '.join(unknown)}")
    invocations: dict[str, Invocation] = {}
    for index, table in enumerate(document.get("invocation", [])):
        name = table.get("name")
        if not isinstance(name, str) or not name:
            raise InventoryError(f"invocation #{index + 1} needs a non-empty string `name`")
        if name in invocations:
            raise InventoryError(f"invocation {name} is registered twice")
        tests = table.get("tests")
        ignored = table.get("ignored", [])
        if not isinstance(tests, list) or not tests:
            raise InventoryError(f"invocation {name} registers no test")
        if not isinstance(ignored, list):
            raise InventoryError(f"invocation {name}: `ignored` is not a list")
        for test in [*tests, *ignored]:
            if not isinstance(test, str) or not test:
                raise InventoryError(f"invocation {name} lists a test that is not a name")
        both = sorted(set(tests) & set(ignored))
        if both:
            raise InventoryError(f"invocation {name} registers {both[0]} as run and as ignored")
        if len(set(tests)) != len(tests) or len(set(ignored)) != len(ignored):
            raise InventoryError(f"invocation {name} registers a test twice")
        marker = table.get("marker")
        if marker is not None and (not isinstance(marker, str) or not marker):
            raise InventoryError(f"invocation {name}: `marker` is not a non-empty string")
        if marker is not None:
            for test in [*tests, *ignored]:
                if marker not in test:
                    raise InventoryError(
                        f"invocation {name} registers {test}, which does not carry its marker "
                        f"`{marker}`"
                    )
        unknown_keys = sorted(set(table) - {"name", "tests", "ignored", "marker"})
        if unknown_keys:
            raise InventoryError(f"invocation {name} has unknown keys: {', '.join(unknown_keys)}")
        invocations[name] = Invocation(
            tests=frozenset(tests), ignored=frozenset(ignored), marker=marker
        )
    if not invocations:
        raise InventoryError("the inventory registers no invocation")
    return invocations


def inventory_problems(
    suite: str, name: str, reported: Mapping[str, str], invocation: Invocation
) -> list[str]:
    """Every registered test the invocation did not run as registered, and every test it ran that is
    not registered."""

    problems: list[str] = []
    for test in sorted(invocation.tests):
        outcome = reported.get(test)
        if outcome is None:
            problems.append(f"{suite}: {name}: the registered test {test} did not run")
        elif outcome == "ignored":
            problems.append(f"{suite}: {name}: the registered test {test} is ignored")
    for test in sorted(invocation.ignored):
        if reported.get(test) != "ignored":
            problems.append(
                f"{suite}: {name}: {test} is registered as ignored but was not reported ignored"
            )
    for test in sorted(reported):
        registered = test in invocation.tests or test in invocation.ignored
        if not registered and invocation.must_register(test):
            problems.append(f"{suite}: {name}: {test} ran but is not registered")
    return problems


def account(
    suite: str, logs: Sequence[Path], inventory: Mapping[str, Invocation] | None = None
) -> tuple[Counts, list[str]]:
    """The suite's counts, and every invocation that did not finish, executed no test, or ran other
    tests than the inventory registers for it."""

    problems: list[str] = []
    suite_counts = Counts()
    if not logs:
        problems.append(f"{suite}: no invocation of the suite left a log")
    if inventory is not None:
        for name in sorted(set(inventory) - {log.stem for log in logs}):
            problems.append(f"{suite}: the registered invocation {name} left no log")
    for log in logs:
        output = log.read_text(encoding="utf-8", errors="replace")
        if inventory is not None:
            registered = inventory.get(log.stem)
            if registered is None:
                problems.append(f"{suite}: {log.stem} is not an invocation the inventory registers")
            else:
                problems.extend(inventory_problems(suite, log.stem, outcomes(output), registered))
        invocation = counts(output)
        if invocation is None:
            problems.append(
                f"{suite}: {log.name} holds no test result; the invocation ended before its tests "
                "did"
            )
            continue
        print(f"{suite}: {log.stem}: {invocation.describe()}", flush=True)
        if invocation.executed == 0:
            problems.append(
                f"{suite}: {log.stem} executed no test; its selection matched nothing"
            )
        suite_counts = suite_counts + invocation
    return suite_counts, problems


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("suite")
    parser.add_argument("--inventory", type=Path, default=None)
    parser.add_argument("logs", nargs="*", type=Path)
    arguments = parser.parse_args(argv)
    inventory = None
    if arguments.inventory is not None:
        try:
            inventory = parse_inventory(arguments.inventory.read_text(encoding="utf-8"))
        except (OSError, InventoryError, tomllib.TOMLDecodeError) as error:
            print(f"{arguments.suite}: {arguments.inventory}: {error}", file=sys.stderr)
            return 1
    suite_counts, problems = account(arguments.suite, arguments.logs, inventory)
    print(f"{arguments.suite}: {suite_counts.describe()}", flush=True)
    for problem in problems:
        print(problem, file=sys.stderr)
    if problems:
        return 1
    if suite_counts.completed != suite_counts.executed:
        print(f"{arguments.suite}: a test failed", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
