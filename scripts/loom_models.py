#!/usr/bin/env python3

"""Run the Loom models of production owners, replay a failed one, and qualify them.

`just test-loom [filter]` runs `run`, `just test-loom-replay <failure>` runs `replay`, and
`just test-loom-qualification` runs `qualify`. The inventory in
`crates/model-harness/loom-inventory.toml` registers every model by the invariant it checks.

`run` lists the library tests named `loom_*` of every inventory package, built with its `loom`
feature, and checks them against the inventory: a run over the whole inventory fails when a
registered test is missing or ignored, or when a discovered model is not registered. It then runs
each selected model in its own process and accepts it only when the model's harness printed the
record of an exhaustive exploration for the model's own invariant; a passing test without that
record is an incomplete run. It reports how many models it discovered, selected, executed and saw
complete, and a filter that selects nothing fails.

A failed model leaves `<target>/loom-failures/<package>/<invariant>/` behind: Loom's checkpoint of the
failed execution, the run's output, and `metadata.json` with the invariant, revision, toolchain,
Loom version and exploration bounds. `replay` resumes Loom from that checkpoint with location
tracking and tracing enabled, so the failed execution runs first. The artifacts hold model output
only; a model has no payloads or secrets to leak.

`qualify` applies each registered weakening to a copy of the working tree, requires the named
model to fail with the registered message, and requires the checkpoint of that failure to replay
it. The copy shares the target directory, so only the mutated packages are rebuilt.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Mapping, Sequence

INVENTORY = Path("crates/model-harness/loom-inventory.toml")
FAILURES = "loom-failures"
QUALIFICATIONS = "loom-qualification"
MODEL_PREFIX = "loom_"
LOOM_FEATURE = "loom"

_INVARIANT_ID = re.compile(r"^[a-z0-9-]+(?:\.[a-z0-9-]+)+$")
# The test harness prints `test <name> ... ` before a test's own output, so a record can share its
# line with that prefix and is matched anywhere in a line.
_EXPLORING = re.compile(
    r"nervix-model-harness: exploring loom invariant (?P<id>\S+) to exhaustion "
    r"\((?P<bounds>[^)\n]*)\)"
)
_COMPLETED = re.compile(
    r"nervix-model-harness: loom invariant (?P<id>\S+) explored to exhaustion in "
    r"(?P<executions>\d+) executions \((?P<bounds>[^)\n]*)\)"
)
_LISTED_TEST = re.compile(r"^(?P<name>\S+): test$")


class RunnerError(Exception):
    """A configuration or inventory problem that stops a run before or instead of a model."""


@dataclass(frozen=True)
class Invariant:
    id: str
    package: str
    test: str
    claim: str


@dataclass(frozen=True)
class Qualification:
    id: str
    invariant: str
    path: str
    original: str
    weakened: str
    failure: str


@dataclass(frozen=True)
class Inventory:
    invariants: tuple[Invariant, ...]
    qualifications: tuple[Qualification, ...]

    def packages(self) -> list[str]:
        return sorted({invariant.package for invariant in self.invariants})

    def invariant(self, invariant_id: str) -> Invariant:
        for invariant in self.invariants:
            if invariant.id == invariant_id:
                return invariant
        raise RunnerError(f"no invariant {invariant_id} in {INVENTORY}")

    def registered(self, package: str, test: str) -> Invariant | None:
        for invariant in self.invariants:
            if invariant.package == package and invariant.test == test:
                return invariant
        return None


def _required_string(table: Mapping[str, object], key: str, context: str) -> str:
    value = table.get(key)
    if not isinstance(value, str) or not value:
        raise RunnerError(f"{context} needs a non-empty string `{key}`")
    return value


def parse_inventory(text: str) -> Inventory:
    """Parse and validate the inventory. Every problem names the entry it was found in."""

    document = tomllib.loads(text)
    unknown = sorted(set(document) - {"invariant", "qualification"})
    if unknown:
        raise RunnerError(f"unknown inventory tables: {', '.join(unknown)}")

    invariants: list[Invariant] = []
    for index, table in enumerate(document.get("invariant", [])):
        context = f"invariant #{index + 1}"
        invariant_id = _required_string(table, "id", context)
        if not _INVARIANT_ID.match(invariant_id):
            raise RunnerError(
                f"invariant {invariant_id} is not two or more dot-separated words of lowercase "
                "ASCII letters, digits and hyphens"
            )
        context = f"invariant {invariant_id}"
        invariant = Invariant(
            id=invariant_id,
            package=_required_string(table, "package", context),
            test=_required_string(table, "test", context),
            claim=_required_string(table, "claim", context),
        )
        if not invariant.test.rsplit("::", 1)[-1].startswith(MODEL_PREFIX):
            raise RunnerError(f"{context}: test {invariant.test} is not named `{MODEL_PREFIX}*`")
        invariants.append(invariant)
    if not invariants:
        raise RunnerError(f"{INVENTORY} registers no invariant")

    seen_ids: set[str] = set()
    seen_tests: set[tuple[str, str]] = set()
    for invariant in invariants:
        if invariant.id in seen_ids:
            raise RunnerError(f"invariant {invariant.id} is registered twice")
        seen_ids.add(invariant.id)
        test_key = (invariant.package, invariant.test)
        if test_key in seen_tests:
            raise RunnerError(f"{invariant.package} test {invariant.test} is registered twice")
        seen_tests.add(test_key)

    qualifications: list[Qualification] = []
    seen_qualifications: set[str] = set()
    for index, table in enumerate(document.get("qualification", [])):
        context = f"qualification #{index + 1}"
        qualification_id = _required_string(table, "id", context)
        context = f"qualification {qualification_id}"
        qualification = Qualification(
            id=qualification_id,
            invariant=_required_string(table, "invariant", context),
            path=_required_string(table, "path", context),
            original=_required_string(table, "original", context),
            weakened=_required_string(table, "weakened", context),
            failure=_required_string(table, "failure", context),
        )
        if qualification.id in seen_qualifications:
            raise RunnerError(f"qualification {qualification.id} is registered twice")
        seen_qualifications.add(qualification.id)
        if qualification.invariant not in seen_ids:
            raise RunnerError(f"{context} names unknown invariant {qualification.invariant}")
        if qualification.original == qualification.weakened:
            raise RunnerError(f"{context} does not change its original text")
        qualifications.append(qualification)

    return Inventory(invariants=tuple(invariants), qualifications=tuple(qualifications))


def listed_tests(output: str) -> list[str]:
    """The test names of `cargo test -- --list --format terse` output."""

    names: list[str] = []
    for line in output.splitlines():
        match = _LISTED_TEST.match(line.strip())
        if match is not None:
            names.append(match.group("name"))
    return names


def is_model(test: str) -> bool:
    return test.rsplit("::", 1)[-1].startswith(MODEL_PREFIX)


@dataclass(frozen=True)
class Discovery:
    """The models one package's Loom build contains."""

    package: str
    models: tuple[str, ...]
    ignored: tuple[str, ...]


@dataclass(frozen=True)
class Model:
    package: str
    test: str
    invariant: Invariant


def select(inventory: Inventory, discoveries: Sequence[Discovery], filter_text: str) -> list[Model]:
    """Choose the models to run, checking the inventory against what the build contains.

    An empty filter is the whole gate, so every registered invariant must be discovered and not
    ignored, and every discovered model must be registered. A filter selects registered models
    whose test name or invariant contains it and refuses to run an unregistered one.
    """

    discovered: dict[str, Discovery] = {discovery.package: discovery for discovery in discoveries}
    problems: list[str] = []
    selected: list[Model] = []

    for discovery in discoveries:
        for test in discovery.models:
            invariant = inventory.registered(discovery.package, test)
            if invariant is None:
                if not filter_text or filter_text in test:
                    problems.append(
                        f"{discovery.package} model {test} is not registered in {INVENTORY}"
                    )
                continue
            if filter_text and filter_text not in test and filter_text not in invariant.id:
                continue
            if test in discovery.ignored:
                problems.append(f"invariant {invariant.id} is ignored: {test}")
                continue
            selected.append(Model(package=discovery.package, test=test, invariant=invariant))

    if not filter_text:
        for invariant in inventory.invariants:
            discovery = discovered.get(invariant.package)
            if discovery is None or invariant.test not in discovery.models:
                problems.append(
                    f"invariant {invariant.id} is not discovered: no test {invariant.test} in "
                    f"{invariant.package}"
                )

    if problems:
        raise RunnerError("\n".join(problems))
    if not selected:
        scope = f"matching `{filter_text}`" if filter_text else "in the inventory"
        raise RunnerError(f"no Loom model {scope} was selected")
    return selected


@dataclass(frozen=True)
class Completion:
    executions: int
    bounds: str


def completion(output: str, invariant_id: str) -> Completion | None:
    """The exhaustive-exploration record the harness printed for `invariant_id`, if any."""

    for match in _COMPLETED.finditer(output):
        if match.group("id") == invariant_id:
            return Completion(
                executions=int(match.group("executions")), bounds=match.group("bounds")
            )
    return None


def exploration_bounds(output: str, invariant_id: str) -> str | None:
    for match in _EXPLORING.finditer(output):
        if match.group("id") == invariant_id:
            return match.group("bounds")
    return None


def weaken(source: str, qualification: Qualification) -> str:
    """Apply one registered weakening, which must match its original text exactly once."""

    occurrences = source.count(qualification.original)
    if occurrences != 1:
        raise RunnerError(
            f"qualification {qualification.id}: `{qualification.original}` occurs "
            f"{occurrences} times in {qualification.path}, not once"
        )
    return source.replace(qualification.original, qualification.weakened)


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
        cwd: Path | None = None,
        echo: bool = True,
    ) -> Outcome:
        merged = dict(os.environ)
        if environment:
            merged.update(environment)
        process = subprocess.Popen(
            list(arguments),
            cwd=cwd or self.root,
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


def cargo_test(package: str, *arguments: str, manifest: Path | None = None) -> list[str]:
    command = ["cargo", "test"]
    if manifest is not None:
        command += ["--manifest-path", str(manifest)]
    return [*command, "--package", package, "--features", LOOM_FEATURE, "--lib", *arguments]


def model_command(model: Model, manifest: Path | None = None) -> list[str]:
    return cargo_test(
        model.package,
        model.test,
        "--",
        "--exact",
        "--nocapture",
        "--test-threads=1",
        manifest=manifest,
    )


def discover(commands: Commands, package: str) -> Discovery:
    listing = commands.run(
        cargo_test(package, "--", "--list", "--format", "terse"), echo=False
    )
    if listing.status != 0:
        sys.stdout.write(listing.output)
        raise RunnerError(f"cannot list the Loom build of {package}")
    ignored_listing = commands.run(
        cargo_test(package, "--", "--list", "--format", "terse", "--ignored"), echo=False
    )
    if ignored_listing.status != 0:
        sys.stdout.write(ignored_listing.output)
        raise RunnerError(f"cannot list the ignored tests of {package}")
    models = tuple(test for test in listed_tests(listing.output) if is_model(test))
    ignored = tuple(test for test in listed_tests(ignored_listing.output) if is_model(test))
    return Discovery(package=package, models=models, ignored=ignored)


def loom_version(root: Path) -> str | None:
    lock = tomllib.loads((root / "Cargo.lock").read_text(encoding="utf-8"))
    for package in lock.get("package", []):
        if package.get("name") == "loom":
            return package.get("version")
    return None


def failure_metadata(
    commands: Commands, model: Model, command: Sequence[str], outcome: Outcome, directory: Path
) -> dict[str, object]:
    revision = commands.run(["git", "rev-parse", "HEAD"], echo=False).output.strip()
    status = commands.run(["git", "status", "--porcelain"], echo=False).output.strip()
    toolchain = commands.run(["rustc", "-vV"], echo=False).output.strip()
    checkpoint = directory / "checkpoint.json"
    return {
        "invariant": model.invariant.id,
        "claim": model.invariant.claim,
        "package": model.package,
        "test": model.test,
        "revision": revision,
        "working_tree_modified": bool(status),
        "toolchain": toolchain,
        "loom": loom_version(commands.root),
        "exploration": exploration_bounds(outcome.output, model.invariant.id),
        "command": list(command),
        "exit_status": outcome.status,
        "checkpoint": checkpoint.name if checkpoint.exists() else None,
        "replay": f"just test-loom-replay {directory}",
    }


def checkpoint_environment(checkpoint: Path) -> dict[str, str]:
    return {
        "LOOM_CHECKPOINT_FILE": str(checkpoint),
        "LOOM_CHECKPOINT_INTERVAL": "1",
    }


def run_models(commands: Commands, inventory: Inventory, target: Path, filter_text: str) -> int:
    discoveries = [discover(commands, package) for package in inventory.packages()]
    discovered = sum(len(discovery.models) for discovery in discoveries)
    selected = select(inventory, discoveries, filter_text)

    executed = 0
    completed = 0
    failures: list[str] = []
    for model in selected:
        directory = target / FAILURES / model.package / model.invariant.id
        shutil.rmtree(directory, ignore_errors=True)
        directory.mkdir(parents=True)
        checkpoint = directory / "checkpoint.json"
        command = model_command(model)
        print(f"loom: {model.invariant.id} ({model.package} {model.test})", flush=True)
        outcome = commands.run(command, environment=checkpoint_environment(checkpoint))
        executed += 1
        record = completion(outcome.output, model.invariant.id)
        if outcome.status == 0 and record is not None:
            completed += 1
            shutil.rmtree(directory)
            continue
        if outcome.status == 0:
            reason = "passed without the record of an exhaustive exploration"
        else:
            reason = f"failed with exit status {outcome.status}"
        (directory / "output.log").write_text(outcome.output, encoding="utf-8")
        metadata = failure_metadata(commands, model, command, outcome, directory)
        (directory / "metadata.json").write_text(
            json.dumps(metadata, indent=2) + "\n", encoding="utf-8"
        )
        failures.append(f"invariant {model.invariant.id} {reason}; evidence in {directory}")

    print(
        f"loom: discovered {discovered}, selected {len(selected)}, executed {executed}, "
        f"completed {completed}",
        flush=True,
    )
    for failure in failures:
        print(f"loom: {failure}", file=sys.stderr)
    if failures:
        return 1
    return 0


def replay(commands: Commands, inventory: Inventory, directory: Path) -> int:
    metadata_path = directory / "metadata.json"
    if not metadata_path.is_file():
        raise RunnerError(f"{directory} holds no metadata.json from `just test-loom`")
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    checkpoint = directory / "checkpoint.json"
    if not checkpoint.is_file():
        raise RunnerError(f"{directory} holds no Loom checkpoint to replay")
    invariant = inventory.invariant(metadata["invariant"])
    model = Model(package=invariant.package, test=invariant.test, invariant=invariant)
    replayed = directory / "replay-checkpoint.json"
    shutil.copyfile(checkpoint, replayed)
    environment = checkpoint_environment(replayed)
    environment.setdefault("LOOM_LOCATION", "1")
    if "LOOM_LOG" not in os.environ:
        environment["LOOM_LOG"] = "trace"
    outcome = commands.run(model_command(model), environment=environment)
    if outcome.status == 0:
        print(
            f"loom: the recorded failure of {invariant.id} did not reproduce from its checkpoint",
            file=sys.stderr,
        )
    else:
        print(f"loom: the recorded failure of {invariant.id} reproduced", file=sys.stderr)
    return outcome.status


def copy_working_tree(commands: Commands, destination: Path) -> None:
    listing = commands.run(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"], echo=False
    )
    if listing.status != 0:
        raise RunnerError("cannot list the working tree")
    shutil.rmtree(destination, ignore_errors=True)
    for relative in sorted(entry for entry in listing.output.split("\0") if entry):
        source = commands.root / relative
        if not source.is_file():
            continue
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
    # Server models embed the generated web console at compile time. It is ignored by Git, so
    # tracked-source copies need this build input alongside the source under qualification.
    console_dist = Path("crates/web-console/dist")
    if (commands.root / console_dist).is_dir():
        shutil.copytree(commands.root / console_dist, destination / console_dist)


def qualification_failure(outcome: Outcome, qualification: Qualification) -> str | None:
    """Why a weakened model's run does not qualify it, or `None` when it failed as it must."""

    if outcome.status == 0:
        return "the model passed with the weakened ordering"
    if "Running unittests" not in outcome.output:
        return "the weakened copy did not build, so the model never ran"
    if qualification.failure not in outcome.output:
        return f"the model failed without reporting `{qualification.failure}`"
    return None


def qualify(commands: Commands, inventory: Inventory, target: Path) -> int:
    if not inventory.qualifications:
        raise RunnerError(f"{INVENTORY} registers no qualification")
    problems: list[str] = []
    for qualification in inventory.qualifications:
        invariant = inventory.invariant(qualification.invariant)
        model = Model(package=invariant.package, test=invariant.test, invariant=invariant)
        directory = target / QUALIFICATIONS / qualification.id
        tree = directory / "tree"
        copy_working_tree(commands, tree)
        mutated = tree / qualification.path
        mutated.write_text(
            weaken(mutated.read_text(encoding="utf-8"), qualification), encoding="utf-8"
        )
        manifest = tree / "Cargo.toml"
        environment = {"CARGO_TARGET_DIR": str(target)}
        clean_command = [
            "cargo", "clean", "--manifest-path", str(manifest), "--package", model.package,
        ]
        cleaned = commands.run(clean_command, environment=environment, cwd=tree, echo=False)
        if cleaned.status != 0:
            problems.append(
                f"qualification {qualification.id}: could not clear a previous package build; "
                f"{cleaned.output}"
            )
            continue
        checkpoint = directory / "checkpoint.json"
        checkpoint.unlink(missing_ok=True)
        print(f"loom: qualifying {invariant.id} against {qualification.id}", flush=True)
        weakened = commands.run(
            model_command(model, manifest),
            environment={**environment, **checkpoint_environment(checkpoint)},
            cwd=tree,
        )
        (directory / "output.log").write_text(weakened.output, encoding="utf-8")
        problem = qualification_failure(weakened, qualification)
        if problem is None and not checkpoint.is_file():
            problem = "the failed model left no checkpoint to replay"
        if problem is None:
            replayed_checkpoint = directory / "replay-checkpoint.json"
            shutil.copyfile(checkpoint, replayed_checkpoint)
            replayed = commands.run(
                model_command(model, manifest),
                environment={**environment, **checkpoint_environment(replayed_checkpoint)},
                cwd=tree,
            )
            (directory / "replay.log").write_text(replayed.output, encoding="utf-8")
            replay_problem = qualification_failure(replayed, qualification)
            if replay_problem is not None:
                problem = f"its checkpoint does not replay the failure: {replay_problem}"
        cleaned = commands.run(clean_command, environment=environment, cwd=tree, echo=False)
        if cleaned.status != 0:
            problem = f"could not clear its weakened package build: {cleaned.output}"
        if problem is not None:
            problems.append(f"qualification {qualification.id}: {problem}; see {directory}")
            continue
        shutil.rmtree(directory)
        print(
            f"loom: {qualification.id} makes {invariant.id} fail with "
            f"`{qualification.failure}`, and its checkpoint replays the failure",
            flush=True,
        )
    for problem in problems:
        print(f"loom: {problem}", file=sys.stderr)
    if problems:
        return 1
    return 0


def repository_root() -> Path:
    completed = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"], capture_output=True, check=True, text=True
    )
    return Path(completed.stdout.strip())


def main(
    argv: Sequence[str] | None = None,
    commands_for: Callable[[Path], Commands] = Commands,
) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", type=Path, default=None)
    parser.add_argument("--target-dir", type=Path, required=True)
    subcommands = parser.add_subparsers(dest="command", required=True)
    run_parser = subcommands.add_parser("run")
    run_parser.add_argument("filter", nargs="?", default="")
    replay_parser = subcommands.add_parser("replay")
    replay_parser.add_argument("failure", type=Path)
    subcommands.add_parser("qualify")
    arguments = parser.parse_args(argv)

    root = arguments.root or repository_root()
    target = arguments.target_dir.resolve()
    commands = commands_for(root)
    try:
        inventory = parse_inventory((root / INVENTORY).read_text(encoding="utf-8"))
        if arguments.command == "run":
            return run_models(commands, inventory, target, arguments.filter)
        if arguments.command == "replay":
            return replay(commands, inventory, arguments.failure.resolve())
        return qualify(commands, inventory, target)
    except RunnerError as error:
        print(f"loom: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
