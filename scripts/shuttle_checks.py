#!/usr/bin/env python3

"""Run the Shuttle checks of production owners, replay a failed one, and prove replay works.

`just test-shuttle [filter]` runs `run`, `just test-shuttle-replay <schedule>` runs `replay`, and
`just test-shuttle-replay-check` runs `replay-check`. The inventory in
`crates/model-harness/shuttle-inventory.toml` registers every check by package and test.

`run` lists the library tests whose full names contain `shuttle_` in every inventory package, built
with its `shuttle` feature, and checks them against the inventory: a run over the whole inventory
fails when a registered check is missing or ignored, or when a discovered check is not registered.
It then runs each selected check twice, each time in its own process: under the exploration the
check declares, and under the uncontrolled-nondeterminism detector. A run counts only when its test
passed and the harness printed the record of a completed exploration; a passing test without that
record explored nothing. The command reports how many checks it discovered, selected, executed and
saw complete, and a filter that selects nothing across every package fails, although a package
with no match inside a nonempty selection is fine.

A failed check leaves `<target>/shuttle-failures/<package>/<test>/` behind: the schedule Shuttle
persisted for the failing execution, the run's output, and `metadata.json` naming the check, the
run, the revision, the toolchain and Shuttle's version. `replay` runs exactly that check in a fresh
process with the persisted schedule. `replay-check` proves the path end to end: it fails one check
deliberately after its invariant held, requires exactly one persisted schedule, and requires that
schedule to reproduce the failure in a fresh process. A persisted schedule ends where its execution
failed, so it replays that failure and nothing else. The artifacts hold check output only; a check
has no payloads or secrets to leak.
"""

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
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Mapping, Sequence

from scripts.model_evidence import Evidence

INVENTORY = Path("crates/model-harness/shuttle-inventory.toml")
FAILURES = "shuttle-failures"
CHECK_MARKER = "shuttle_"
SHUTTLE_FEATURE = "shuttle"
COMPLETED = "nervix-model-harness: shuttle check explored to completion:"
REPLAYED = "nervix-model-harness: shuttle check replays the schedule in"
FORCED_FAILURE = "forced Shuttle schedule replay verification"
# The check `replay-check` fails deliberately: a small one of a package that builds without the
# server's test dependencies.
REPLAY_CHECK_PACKAGE = "nervix-execution"
REPLAY_CHECK_TEST = "tests::shuttle_checks::shuttle_full_wait_queue_is_exact_typed_backpressure"

_LISTED_TEST = re.compile(r"^(?P<name>\S+): test$")


class RunnerError(Exception):
    """A configuration or inventory problem that stops a run before or instead of a check."""


@dataclass(frozen=True)
class Inventory:
    """The registered checks of every package, in the order the inventory lists them."""

    checks: Mapping[str, tuple[str, ...]]

    def packages(self) -> list[str]:
        return list(self.checks)

    def is_registered(self, package: str, test: str) -> bool:
        return test in self.checks.get(package, ())


def parse_inventory(text: str) -> Inventory:
    """Parse and validate the inventory. Every problem names the entry it was found in."""

    document = tomllib.loads(text)
    unknown = sorted(set(document) - {"package"})
    if unknown:
        raise RunnerError(f"unknown inventory tables: {', '.join(unknown)}")
    checks: dict[str, tuple[str, ...]] = {}
    for index, table in enumerate(document.get("package", [])):
        name = table.get("name")
        if not isinstance(name, str) or not name:
            raise RunnerError(f"package #{index + 1} needs a non-empty string `name`")
        if name in checks:
            raise RunnerError(f"package {name} is registered twice")
        tests = table.get("checks")
        if not isinstance(tests, list) or not tests:
            raise RunnerError(f"package {name} registers no check")
        seen: set[str] = set()
        for test in tests:
            if not isinstance(test, str) or CHECK_MARKER not in test:
                raise RunnerError(
                    f"package {name}: `{test}` is not a test whose full name contains "
                    f"`{CHECK_MARKER}`"
                )
            if test in seen:
                raise RunnerError(f"package {name} registers {test} twice")
            seen.add(test)
        unknown_keys = sorted(set(table) - {"name", "checks"})
        if unknown_keys:
            raise RunnerError(f"package {name} has unknown keys: {', '.join(unknown_keys)}")
        checks[name] = tuple(tests)
    if not checks:
        raise RunnerError(f"{INVENTORY} registers no package")
    return Inventory(checks=checks)


def listed_tests(output: str) -> list[str]:
    """The test names of `cargo test -- --list --format terse` output."""

    names: list[str] = []
    for line in output.splitlines():
        match = _LISTED_TEST.match(line.strip())
        if match is not None:
            names.append(match.group("name"))
    return names


def is_check(test: str) -> bool:
    return CHECK_MARKER in test


@dataclass(frozen=True)
class Discovery:
    """The checks one package's Shuttle build contains."""

    package: str
    checks: tuple[str, ...]
    ignored: tuple[str, ...]


@dataclass(frozen=True)
class Check:
    package: str
    test: str


def select(inventory: Inventory, discoveries: Sequence[Discovery], filter_text: str) -> list[Check]:
    """Choose the checks to run, checking the inventory against what the builds contain.

    An empty filter is the whole gate, so every registered check must be discovered and not ignored,
    and every discovered check must be registered. A filter selects the registered checks whose full
    name contains it, across every package, and refuses to run an unregistered one. A package with
    no match is fine; a selection with none at all fails.
    """

    discovered = {discovery.package: discovery for discovery in discoveries}
    problems: list[str] = []
    selected: list[Check] = []
    for discovery in discoveries:
        for test in discovery.checks:
            if not inventory.is_registered(discovery.package, test):
                if not filter_text or filter_text in test:
                    problems.append(
                        f"{discovery.package} check {test} is not registered in {INVENTORY}"
                    )
                continue
            if filter_text and filter_text not in test:
                continue
            if test in discovery.ignored:
                problems.append(f"{discovery.package} check {test} is ignored")
                continue
            selected.append(Check(package=discovery.package, test=test))
    if not filter_text:
        for package, tests in inventory.checks.items():
            discovery = discovered.get(package)
            for test in tests:
                if discovery is None or test not in discovery.checks:
                    problems.append(f"{package} check {test} is not discovered")
    if problems:
        raise RunnerError("\n".join(problems))
    if not selected:
        scope = f"matching `{filter_text}`" if filter_text else "in the inventory"
        raise RunnerError(f"no Shuttle check {scope} was selected in any package")
    return selected


@dataclass(frozen=True)
class Outcome:
    status: int
    output: str


class Commands:
    """How the runner reaches Cargo, Git and the toolchain. Tests substitute a recording double."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def run(
        self,
        arguments: Sequence[str],
        *,
        environment: Mapping[str, str] | None = None,
        echo: bool = True,
    ) -> Outcome:
        merged = dict(os.environ)
        for variable in ("SHUTTLE_CHECK_NONDETERMINISM", "SHUTTLE_TRACE_FILE", "SHUTTLE_TRACE_DIR"):
            merged.pop(variable, None)
        if environment:
            merged.update(environment)
        process = subprocess.Popen(
            list(arguments),
            cwd=self.root,
            env=merged,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        lines: list[str] = []
        if process.stdout is None:
            raise RunnerError(f"no output stream for {' '.join(arguments)}")
        for line in process.stdout:
            lines.append(line)
            if echo:
                sys.stdout.write(line)
                sys.stdout.flush()
        return Outcome(status=process.wait(), output="".join(lines))


def cargo_test(package: str, *arguments: str) -> list[str]:
    return [
        "cargo",
        "test",
        "--package",
        package,
        "--features",
        SHUTTLE_FEATURE,
        "--lib",
        *arguments,
    ]


def check_command(check: Check) -> list[str]:
    return cargo_test(
        check.package, check.test, "--", "--exact", "--nocapture", "--test-threads=1"
    )


def discover(commands: Commands, package: str) -> Discovery:
    listing = commands.run(cargo_test(package, "--", "--list", "--format", "terse"), echo=False)
    if listing.status != 0:
        sys.stdout.write(listing.output)
        raise RunnerError(f"cannot list the Shuttle build of {package}")
    ignored_listing = commands.run(
        cargo_test(package, "--", "--list", "--format", "terse", "--ignored"), echo=False
    )
    if ignored_listing.status != 0:
        sys.stdout.write(ignored_listing.output)
        raise RunnerError(f"cannot list the ignored tests of {package}")
    checks = tuple(test for test in listed_tests(listing.output) if is_check(test))
    ignored = tuple(test for test in listed_tests(ignored_listing.output) if is_check(test))
    return Discovery(package=package, checks=checks, ignored=ignored)


@dataclass(frozen=True)
class Run:
    """One of the two runs of every check: its name, and the environment that selects it."""

    name: str
    environment: Mapping[str, str]


EXPLORATION = Run(name="exploration", environment={})
NONDETERMINISM = Run(name="nondeterminism", environment={"SHUTTLE_CHECK_NONDETERMINISM": "1"})
RUNS = (EXPLORATION, NONDETERMINISM)


def completed(outcome: Outcome) -> bool:
    return outcome.status == 0 and COMPLETED in outcome.output


def shuttle_version(root: Path) -> str | None:
    lock = tomllib.loads((root / "Cargo.lock").read_text(encoding="utf-8"))
    for package in lock.get("package", []):
        if package.get("name") == "shuttle":
            return package.get("version")
    return None


def schedules(directory: Path) -> list[Path]:
    """The schedules Shuttle persisted in a check's failure directory."""

    found: list[Path] = []
    if directory.is_dir():
        for path in sorted(directory.iterdir()):
            if path.is_file() and path.suffix not in (".log", ".json"):
                found.append(path)
    return found


def failure_metadata(
    commands: Commands,
    check: Check,
    run: Run,
    command: Sequence[str],
    outcome: Outcome,
    directory: Path,
) -> dict[str, object]:
    revision = commands.run(["git", "rev-parse", "HEAD"], echo=False).output.strip()
    status = commands.run(["git", "status", "--porcelain"], echo=False).output.strip()
    toolchain = commands.run(["rustc", "-vV"], echo=False).output.strip()
    persisted = schedules(directory)
    replays = [f"just test-shuttle-replay {schedule}" for schedule in persisted]
    return {
        "package": check.package,
        "test": check.test,
        "run": run.name,
        "revision": revision,
        "working_tree_modified": bool(status),
        "toolchain": toolchain,
        "shuttle": shuttle_version(commands.root),
        "command": list(command),
        "environment": dict(run.environment),
        "exit_status": outcome.status,
        "schedules": [schedule.name for schedule in persisted],
        "replay": replays,
    }


def run_checks(
    commands: Commands, inventory: Inventory, target: Path, filter_text: str, report: Path | None = None
) -> int:
    evidence = Evidence(report, "shuttle", filter_text, INVENTORY)
    discoveries = [discover(commands, package) for package in inventory.packages()]
    evidence.discover([
        {"package": discovery.package, "test": test, "ignored": test in discovery.ignored}
        for discovery in discoveries for test in discovery.checks
    ])
    discovered = sum(len(discovery.checks) for discovery in discoveries)
    selected = select(inventory, discoveries, filter_text)
    evidence.select([{"package": check.package, "test": check.test} for check in selected])

    executed = 0
    complete = 0
    failures: list[str] = []
    for check in selected:
        check_evidence = evidence.begin({"package": check.package, "test": check.test})
        runs: list[dict[str, object]] = []
        check_evidence["runs"] = runs
        directory = target / FAILURES / check.package / check.test
        shutil.rmtree(directory, ignore_errors=True)
        directory.mkdir(parents=True)
        command = check_command(check)
        executed += 1
        check_failures: list[str] = []
        for run in RUNS:
            print(f"shuttle: {check.package} {check.test} ({run.name})", flush=True)
            outcome = commands.run(
                command,
                environment={**run.environment, "SHUTTLE_TRACE_DIR": str(directory)},
            )
            runs.append({
                "name": run.name,
                "exit_status": outcome.status,
                "completed": completed(outcome),
                "records": [line.strip() for line in outcome.output.splitlines() if COMPLETED in line],
            })
            evidence.write()
            if completed(outcome):
                continue
            if outcome.status == 0:
                reason = "passed without the record of a completed exploration"
            else:
                reason = f"failed with exit status {outcome.status}"
            (directory / f"{run.name}.log").write_text(outcome.output, encoding="utf-8")
            metadata = failure_metadata(commands, check, run, command, outcome, directory)
            (directory / f"{run.name}.json").write_text(
                json.dumps(metadata, indent=2) + "\n", encoding="utf-8"
            )
            check_failures.append(
                f"{check.package} check {check.test} {reason} in its {run.name} run; evidence "
                f"in {directory}"
            )
        if check_failures:
            failures.extend(check_failures)
            continue
        complete += 1
        check_evidence["completed"] = True
        evidence.write()
        shutil.rmtree(directory)

    print(
        f"shuttle: discovered {discovered}, selected {len(selected)}, executed {executed}, "
        f"completed {complete}",
        flush=True,
    )
    for failure in failures:
        print(f"shuttle: {failure}", file=sys.stderr)
    if failures:
        evidence.finish(1)
        return 1
    evidence.finish(0)
    return 0


def check_of_schedule(inventory: Inventory, schedule: Path) -> Check:
    """The check a persisted schedule belongs to, named by its failure directory."""

    test = schedule.parent.name
    package = schedule.parent.parent.name
    if not inventory.is_registered(package, test):
        raise RunnerError(
            f"{schedule} is not in `<target>/{FAILURES}/<package>/<test>/` of a registered check"
        )
    return Check(package=package, test=test)


def replay(commands: Commands, inventory: Inventory, schedule: Path) -> int:
    if not schedule.is_file():
        raise RunnerError(f"Shuttle schedule does not exist: {schedule}")
    check = check_of_schedule(inventory, schedule)
    outcome = commands.run(
        check_command(check), environment={"SHUTTLE_TRACE_FILE": str(schedule)}
    )
    if REPLAYED not in outcome.output:
        print(
            f"shuttle: {check.package} check {check.test} did not replay {schedule}; it may come "
            "from another revision",
            file=sys.stderr,
        )
        return 1
    if outcome.status == 0:
        print(f"shuttle: the schedule of {check.test} replayed without failing", file=sys.stderr)
    else:
        print(f"shuttle: the recorded failure of {check.test} reproduced", file=sys.stderr)
    return outcome.status


def replay_check(commands: Commands, inventory: Inventory, target: Path) -> int:
    """Prove persistence and replay end to end on one real check."""

    check = Check(package=REPLAY_CHECK_PACKAGE, test=REPLAY_CHECK_TEST)
    if not inventory.is_registered(check.package, check.test):
        raise RunnerError(f"the replay check {check.test} is not registered in {INVENTORY}")
    forced = {"SHUTTLE_FORCE_FAILURE": "1"}
    with tempfile.TemporaryDirectory(dir=target) as scratch:
        directory = Path(scratch) / FAILURES / check.package / check.test
        directory.mkdir(parents=True)
        failed = commands.run(
            check_command(check),
            environment={**forced, "SHUTTLE_TRACE_DIR": str(directory)},
            echo=False,
        )
        if failed.status == 0 or FORCED_FAILURE not in failed.output:
            sys.stdout.write(failed.output)
            raise RunnerError("the forced failure did not fail the check")
        persisted = schedules(directory)
        if len(persisted) != 1:
            sys.stdout.write(failed.output)
            raise RunnerError(f"expected one persisted schedule, found {len(persisted)}")
        schedule = persisted[0]
        reproduced = commands.run(
            check_command(check),
            environment={**forced, "SHUTTLE_TRACE_FILE": str(schedule)},
            echo=False,
        )
        if (
            reproduced.status == 0
            or FORCED_FAILURE not in reproduced.output
            or REPLAYED not in reproduced.output
        ):
            sys.stdout.write(reproduced.output)
            raise RunnerError("the persisted schedule did not reproduce the forced failure")
    print(
        f"shuttle: {check.test} persisted one schedule for its forced failure, and the schedule "
        "reproduced the failure in a fresh process",
        flush=True,
    )
    return 0


def list_checks(commands: Commands, inventory: Inventory) -> int:
    for package in inventory.packages():
        discovery = discover(commands, package)
        for test in discovery.checks:
            marker = "" if inventory.is_registered(package, test) else "  (not registered)"
            print(f"{package} {test}{marker}")
    return 0


def repository_root() -> Path:
    completed_process = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"], capture_output=True, check=True, text=True
    )
    return Path(completed_process.stdout.strip())


def main(
    argv: Sequence[str] | None = None,
    commands_for: Callable[[Path], Commands] = Commands,
) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", type=Path, default=None)
    parser.add_argument("--target-dir", type=Path, default=None)
    subcommands = parser.add_subparsers(dest="command", required=True)
    run_parser = subcommands.add_parser("run")
    run_parser.add_argument("filter", nargs="?", default="")
    run_parser.add_argument("--report", type=Path)
    replay_parser = subcommands.add_parser("replay")
    replay_parser.add_argument("schedule", type=Path)
    subcommands.add_parser("replay-check")
    subcommands.add_parser("list")
    subcommands.add_parser("packages")
    arguments = parser.parse_args(argv)

    root = arguments.root or repository_root()
    commands = commands_for(root)
    try:
        inventory = parse_inventory((root / INVENTORY).read_text(encoding="utf-8"))
        if arguments.command == "packages":
            print("\n".join(inventory.packages()))
            return 0
        if arguments.command == "list":
            return list_checks(commands, inventory)
        if arguments.command == "replay":
            return replay(commands, inventory, arguments.schedule.resolve())
        if arguments.target_dir is None:
            raise RunnerError(f"`{arguments.command}` needs --target-dir")
        target = arguments.target_dir.resolve()
        target.mkdir(parents=True, exist_ok=True)
        if arguments.command == "run":
            return run_checks(commands, inventory, target, arguments.filter, arguments.report)
        return replay_check(commands, inventory, target)
    except RunnerError as error:
        print(f"shuttle: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
