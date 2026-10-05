#!/usr/bin/env python3

"""Collect LLVM source coverage from the extra checks during the runs CI already makes of them.

`just coverage-native-extras [producer ...]` runs `run`. A producer is an extra check whose recipe
executes Nervix code natively in its declared mode: `test-typed-ratchet` qualifies compiler-resolved
source diagnostics, generated reports, semantic fixtures and paired API doctests with the pinned
compiler's matching LLVM tools; `bench-smoke` exercises every Criterion body once,
`test-primitives` runs the primitive boundary's conformance checks, and `nspl-completion-walk`
walks the NSPL completion graph. `test-shuttle` and `test-loom` collect the canonical inventories,
`test-deadlock-evidence-order` executes diagnostic owners and disposable-process probes,
`test-deadlock-report` exercises the ordinary local report command in its own build, and
`test-deloxide` and `test-deloxide-order` run the whole Deloxide diagnostic lane of each selection.
`test-primitives` selects the native conformance producers of every mode. Without names every producer runs. A producer runs
its check exactly as `just <producer>` does and fails when the check fails, which is why CI's
extra-tests job runs those checks through this command instead of beside it.

A check's recipes fall into three parts, and the justfile must compose the check from exactly
those parts. The prepare recipes, such as the web console the server benches link, run first in the
ordinary environment. The instrumented recipe then runs in the environment that
`cargo llvm-cov show-env --sh --no-rustc-wrapper` describes: every crate is built with source
coverage instrumentation, the configured compiler wrapper stays in place so kache still serves the
build, and ordinary instrumentation goes to `<target>/native-coverage-build`; other modes use
`<target>/native-coverage-build-<mode>`, so ordinary builds are not
invalidated. The Deloxide lanes instrument only workspace crates instead: the collector moves the
instrumentation flags into a workspace compiler wrapper, `coverage_workspace_wrapper.py`, which
Cargo runs beneath the configured kache for workspace crates alone, as `cargo llvm-cov` itself does
for the ordinary coverage build. Their dependencies, wasmtime's compiler among them, then run at
full speed under nodes whose product deadlines the lane must meet, and their reports are the same,
because export keeps only repository sources. Kache runs a workspace wrapper chain directly, so
those workspace crates are compiled rather than served from the cache. The finish recipes complete
the check outside instrumentation: compile-only checks,
browser builds. Primitive compile checks and Loom weakening qualification run independently.
When a producer selects a compiler, its prepare recipes install that toolchain before the
collector resolves the compiler and validates its LLVM tools. Its attempt directory uses the
requested toolchain's name, and the record gains the installed compiler identity after preparation.

Each producer collects into a fresh directory,
`<target>/native-coverage/<producer>/<mode>/<toolchain>/<attempt>/`, that no other run reads,
writes or cleans. The attempt is the GitHub Actions run and attempt in CI, and a timestamp and
process id locally, and a directory that already exists is never reused, so no run can consume
another's counters. Cargo runs every instrumented executable through this script's `exec` command,
its runner, which records the executable, its arguments and its build ID in `executions.jsonl` and
points the profiles of the executable and of its child processes at `profiles/`, one file per
process and module. Build scripts and procedural macros execute while Cargo builds, and only when it
builds rather than when kache serves the crate; their counters go to /dev/null, so a fresh build and
a cached one collect the same executions.

Export fails, and with it the run, when the instrumented recipe ran no executable, when an
executable it ran is gone or was rebuilt since, when one wrote no profile, or when a profile is
unreadable or comes from an executable that is not retained. It merges the profiles with the
toolchain's own `llvm-profdata`, exports LCOV with its own `llvm-cov` for exactly the executables
that ran and the children they started, and keeps the repository's sources: a file outside the
repository is a dependency, one below a `tests`, `examples` or `benches` directory is harness code,
and one below a Cargo target directory is generated. Because every profile is matched to its
executable by build ID first, a warning from `llvm-cov` is never a stale profile: a function that
one executable never ran can carry a different hash there than in the executable that ran it, and
`llvm-cov` reads it from the one that ran it. The record keeps those warnings.

`completion.json` beside `lcov.info` records the verdict, the revision, the run and attempt, the
producer and mode, the toolchain and its LLVM version, the instrumentation flags, the recipes, the
executions and profiles selected, the export warnings, and the source policy with the lines each
package contributed.
It reads `running` from the moment the directory exists, so an interrupted collection is never
`complete`. It becomes `complete` only once the whole check passed and the report was written, and
`failed` or `interrupted` otherwise, naming the stage. An attempt keeps its raw profiles, whatever
its verdict, as the evidence the report was made from; `export.log` holds the LLVM tools' own
diagnostics, each bounded.

The model runners write `models.json` with the canonical discovery, selection, executions and
completions. Shuttle retains both exploration and nondeterminism records, and Loom retains each
InvariantId, execution count and bounds. The Deloxide lane writes `lane.json` with its discovery,
selection, executions, completions and the qualification of every process's evidence. The
collector requires complete matching evidence before export; qualification never supplies
current-source counters. The instrumented recipe also learns the target directory its prepare
recipes built into, so a check runs prepared binaries from there while it builds its own
instrumented.

The record times every stage it reaches, so a check's budget can be set from what preparation,
the instrumented run, export and finishing each took.
"""

from __future__ import annotations

import argparse
import datetime
import enum
import fcntl
import json
import os
import shlex
import signal
import shutil
import subprocess
import sys
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TextIO

SCRIPT = Path(__file__).resolve()
REPOSITORY = SCRIPT.parent.parent
BUILD_DIRECTORY = "native-coverage-build"
COLLECTION_DIRECTORY = "native-coverage"
ATTEMPT_VARIABLE = "NERVIX_NATIVE_COVERAGE_ATTEMPT"
# The target directory the prepare recipes built into, which the instrumented recipe reads prepared
# binaries from.
PREPARED_TARGET_VARIABLE = "NERVIX_PREPARED_TARGET_DIR"
# The workspace compiler wrapper of a producer that instruments only workspace crates, and the
# instrumentation flags it adds, separated as in CARGO_ENCODED_RUSTFLAGS.
WORKSPACE_WRAPPER = SCRIPT.with_name("coverage_workspace_wrapper.py")
WORKSPACE_FLAGS_VARIABLE = "NERVIX_NATIVE_COVERAGE_WORKSPACE_RUSTFLAGS"
ENCODED_FLAG_SEPARATOR = "\x1f"
EXECUTIONS = "executions.jsonl"
PROFILES = "profiles"
MERGED_PROFILE = "merged.profdata"
UNFILTERED_REPORT = "unfiltered.lcov"
RECORD = "completion.json"
REPORT = "lcov.info"
EXPORT_LOG = "export.log"
LOCK = ".native-coverage.lock"
# One file per process and module: the process id keeps concurrent processes apart, and the module
# signature merges, rather than overwrites, the counters of a later process that reuses an id.
PROFILE_PATTERN = "%p-%m.profraw"
BUILD_TIME_PROFILE = "/dev/null"
HARNESS_DIRECTORIES = frozenset({"tests", "examples", "benches"})
# How much of one tool's diagnostics `export.log` keeps, and how many export warnings the record.
LOG_LIMIT = 64 * 1024
WARNING_LIMIT = 20
# How long an interrupted recipe gets to stop on the forwarded signal before it is killed.
STOP_GRACE_SECONDS = 30
ELF_MAGIC = b"\x7fELF"
ELF_CLASS_64 = 2
ELF_LITTLE_ENDIAN = 1
PT_NOTE = 4
NT_GNU_BUILD_ID = 3
GNU_NOTE_NAME = b"GNU\x00"


class RunnerError(Exception):
    """A problem that stops a collection. The message names what was wrong and where."""


class Interrupted(Exception):
    """A termination signal asked the collection to stop before it finished."""

    def __init__(self, number: int) -> None:
        super().__init__(signal.Signals(number).name)
        self.number = number


class Verdict(enum.StrEnum):
    RUNNING = "running"
    COMPLETE = "complete"
    FAILED = "failed"
    INTERRUPTED = "interrupted"


class Stage(enum.StrEnum):
    PREPARE = "prepare"
    INSTRUMENT = "instrument"
    RUN = "run"
    EXPORT = "export"
    FINISH = "finish"


class Classification(enum.StrEnum):
    INCLUDED = "included"
    DEPENDENCY = "dependency"
    HARNESS = "harness"
    GENERATED = "generated"


SOURCE_POLICY = {
    Classification.INCLUDED: "files inside the repository",
    Classification.DEPENDENCY: "files outside the repository",
    Classification.HARNESS: "files below a tests, examples or benches directory",
    Classification.GENERATED: "files below a Cargo target directory",
}


class InstrumentedCrates(enum.StrEnum):
    """Which crates of a producer's build carry source coverage instrumentation."""

    # Every crate, dependencies included, through the build's own compiler flags.
    EVERY = "every"
    # Only workspace crates, through a workspace compiler wrapper beneath the configured kache.
    WORKSPACE = "workspace"


@dataclass(frozen=True)
class Producer:
    """An extra check whose executions are collected, and the recipes the check consists of."""

    name: str
    mode: str
    prepare: tuple[str, ...]
    instrumented: str
    finish: tuple[str, ...]
    toolchain: str | None = None
    filterable: bool = False
    # Its instrumented recipe is the Deloxide lane, whose complete record export requires.
    diagnostic_lane: bool = False
    instrumented_crates: InstrumentedCrates = InstrumentedCrates.EVERY

    def rerun(self, filter_text: str = "") -> str:
        command = f"just coverage-native-extras {self.name}"
        if filter_text:
            command += f" --filter {shlex.quote(filter_text)}"
        return command

    def recipes(self) -> list[str]:
        return [*self.prepare, self.instrumented, *self.finish]


PRODUCERS: tuple[Producer, ...] = (
    Producer(
        name="test-typed-ratchet",
        mode="ordinary",
        prepare=(),
        instrumented="test-typed-ratchet-ordinary",
        finish=("test-typed-ratchet-product-docs", "test-typed-ratchet-modeled"),
        toolchain="nightly-2026-09-17",
    ),
    Producer(
        name="bench-smoke",
        mode="ordinary",
        prepare=("build-web-console", "wasm-processor-guests", "download-onnxruntime"),
        instrumented="bench-smoke-bodies",
        finish=(),
    ),
    Producer(
        name="test-primitives-ordinary",
        mode="ordinary",
        prepare=(),
        instrumented="test-primitives-ordinary",
        finish=(),
    ),
    *(
        Producer(
            name=f"test-primitives-{mode}", mode=mode, prepare=(),
            instrumented=f"test-primitives-{mode}", finish=(),
        )
        for mode in ("shuttle", "loom", "turmoil", "deloxide")
    ),
    Producer(
        name="nspl-completion-walk",
        mode="ordinary",
        prepare=(),
        instrumented="nspl-completion-walk",
        finish=(),
    ),
    Producer(
        name="test-shuttle", mode="shuttle",
        prepare=("build-web-console", "wasm-processor-guests", "download-onnxruntime"),
        instrumented="test-shuttle-checks", finish=(), filterable=True,
    ),
    Producer(
        name="test-loom", mode="loom", prepare=("build-web-console",),
        instrumented="test-loom-models", finish=(), filterable=True,
    ),
    Producer(
        name="test-deadlock-evidence-order", mode="deloxide-order", prepare=(),
        instrumented="test-deadlock-evidence-order", finish=(),
    ),
    Producer(
        name="test-deadlock-report", mode="ordinary", prepare=(),
        instrumented="test-deadlock-report", finish=(),
    ),
    # The lanes' nodes must meet product deadlines while they prepare WASM processors and Arrow
    # state, which instrumented dependencies made several times slower.
    *(
        Producer(
            name=f"test-{mode}", mode=mode, prepare=("tests-deps",),
            instrumented=f"test-{mode}-workloads", finish=(), diagnostic_lane=True,
            instrumented_crates=InstrumentedCrates.WORKSPACE,
        )
        for mode in ("deloxide", "deloxide-order")
    ),
)


def dependency_names(recipe: Mapping[str, object]) -> list[str]:
    names: list[str] = []
    dependencies = recipe.get("dependencies")
    if not isinstance(dependencies, list):
        return names
    for dependency in dependencies:
        if not isinstance(dependency, Mapping):
            raise RunnerError(f"unexpected dependency in the justfile dump: {dependency!r}")
        if dependency.get("arguments"):
            names.append(f"{dependency.get('recipe')} with arguments")
        else:
            names.append(str(dependency.get("recipe")))
    return names


def validate_composition(producer: Producer, recipes: Mapping[str, object]) -> None:
    """Require the justfile to compose the producer's check from exactly its recipes, in order.

    The instrumented recipe must have no dependencies of its own, because a dependency would run
    inside the instrumentation: a browser build compiled for coverage, or a prerequisite counted as
    an execution of the check.
    """

    declared: dict[str, Mapping[str, object]] = {}
    for recipe in (producer.name, *producer.recipes()):
        value = recipes.get(recipe)
        if not isinstance(value, Mapping):
            raise RunnerError(f"the justfile has no `{recipe}` recipe, which `{producer.name}` runs")
        declared[recipe] = value
    instrumented_dependencies = dependency_names(declared[producer.instrumented])
    if instrumented_dependencies:
        raise RunnerError(
            f"`{producer.instrumented}` depends on {', '.join(instrumented_dependencies)}, which "
            f"would run inside the instrumentation; make them prepare recipes of `{producer.name}`"
        )
    if producer.instrumented == producer.name:
        if producer.prepare or producer.finish:
            raise RunnerError(
                f"`{producer.name}` is its own instrumented recipe, so it has no other recipes"
            )
        return
    check = declared[producer.name]
    composition = dependency_names(check)
    expected = producer.recipes()
    if producer.filterable:
        expected[expected.index(producer.instrumented)] += " with arguments"
        dependency = check["dependencies"][-1]
        if dependency.get("arguments") != [["variable", "filter"]]:
            raise RunnerError(f"`{producer.name}` must forward its filter to `{producer.instrumented}`")
    if composition != expected:
        raise RunnerError(
            f"`just {producer.name}` must consist of exactly {', '.join(expected)} for its "
            f"coverage to be the check's own run; the justfile has "
            f"{', '.join(composition) or 'no dependencies'}"
        )
    if check.get("body"):
        raise RunnerError(
            f"`just {producer.name}` has a body of its own, which would run outside the collection"
        )


def select_producers(names: Sequence[str], producers: Sequence[Producer]) -> list[Producer]:
    if not names:
        return list(producers)
    known = {producer.name: producer for producer in producers}
    selected: list[Producer] = []
    for name in names:
        matching = [producer for producer in producers if name == "test-primitives" and producer.name.startswith("test-primitives-")]
        if not matching and name in known:
            matching = [known[name]]
        if not matching:
            raise RunnerError(f"no producer `{name}`; the producers are {', '.join(known)}")
        for producer in matching:
            if producer in selected:
                raise RunnerError(f"`{producer.name}` is named twice")
            selected.append(producer)
    return selected


@dataclass(frozen=True)
class Captured:
    status: int
    stdout: str
    stderr: str


class Commands:
    """How the collector reaches just, Cargo, Git and the LLVM tools.

    Recipes write to this process's output unless `output` names a file for them. Tests substitute a
    double, or keep a real run's output out of their own.
    """

    def __init__(self, root: Path, output: TextIO | None = None) -> None:
        self.root = root
        self.output = output

    def capture(
        self, arguments: Sequence[str], *, environment: Mapping[str, str] | None = None
    ) -> Captured:
        completed = subprocess.run(
            list(arguments),
            cwd=self.root,
            env=dict(environment) if environment is not None else None,
            capture_output=True,
            text=True,
            check=False,
        )
        return Captured(completed.returncode, completed.stdout, completed.stderr)

    def export(self, arguments: Sequence[str], output: Path) -> Captured:
        """Run a tool whose standard output is a report too large to hold in memory."""

        with output.open("w", encoding="utf-8") as destination:
            completed = subprocess.run(
                list(arguments),
                cwd=self.root,
                stdout=destination,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
            )
        return Captured(completed.returncode, "", completed.stderr)

    def stream(self, arguments: Sequence[str], *, environment: Mapping[str, str]) -> int:
        """Run a recipe with its output on this terminal, in a process group of its own.

        An interrupt that reaches the collector is forwarded to the whole group, so a recipe's Cargo
        and test processes stop with it however the signal arrived.
        """

        process = subprocess.Popen(
            list(arguments),
            cwd=self.root,
            env=dict(environment),
            process_group=0,
            stdout=self.output,
            stderr=self.output,
        )
        try:
            return process.wait()
        except KeyboardInterrupt:
            stop(process, signal.SIGINT)
            raise
        except Interrupted as interruption:
            stop(process, interruption.number)
            raise


def stop(process: subprocess.Popen[bytes], number: int) -> None:
    try:
        os.killpg(process.pid, number)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=STOP_GRACE_SECONDS)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait()


@dataclass(frozen=True)
class Toolchain:
    """The compiler that builds the instrumented executables and the LLVM tools that read them."""

    version: str
    release: str
    commit: str
    host: str
    llvm: str
    tools: Path

    @classmethod
    def parse(cls, verbose_version: str, sysroot: str) -> Toolchain:
        lines = verbose_version.strip().splitlines()
        if not lines:
            raise RunnerError("`rustc -vV` printed nothing")
        fields: dict[str, str] = {}
        for line in lines[1:]:
            key, separator, value = line.partition(": ")
            if separator:
                fields[key] = value.strip()
        missing = [
            key for key in ("commit-hash", "host", "release", "LLVM version") if not fields.get(key)
        ]
        if missing:
            raise RunnerError(f"`rustc -vV` did not report {', '.join(missing)}")
        host = fields["host"]
        return cls(
            version=lines[0],
            release=fields["release"],
            commit=fields["commit-hash"],
            host=host,
            llvm=fields["LLVM version"],
            tools=Path(sysroot.strip()) / "lib" / "rustlib" / host / "bin",
        )

    def label(self) -> str:
        return f"rust-{self.release}"

    def tool(self, name: str) -> Path:
        path = self.tools / name
        if not path.is_file():
            raise RunnerError(
                f"{path} is missing; install the toolchain's LLVM tools with "
                "`rustup component add llvm-tools`"
            )
        return path

    def describe(self) -> dict[str, str]:
        return {
            "rustc": self.version,
            "commit": self.commit,
            "host": self.host,
            "llvm": self.llvm,
        }


def load_toolchain(commands: Commands, environment: Mapping[str, str] | None = None) -> Toolchain:
    verbose = commands.capture(["rustc", "-vV"], environment=environment)
    sysroot = commands.capture(["rustc", "--print", "sysroot"], environment=environment)
    if verbose.status != 0 or sysroot.status != 0:
        raise RunnerError(f"rustc could not describe itself: {verbose.stderr or sysroot.stderr}")
    toolchain = Toolchain.parse(verbose.stdout, sysroot.stdout)
    for name in ("llvm-profdata", "llvm-cov"):
        reported = commands.capture([str(toolchain.tool(name)), "--version"])
        if f"LLVM version {toolchain.llvm}" not in reported.stdout:
            first_line = reported.stdout.strip().splitlines()[:1] or ["nothing"]
            raise RunnerError(
                f"{name} reports {first_line[0]}, but rustc was built with LLVM {toolchain.llvm}"
            )
    return toolchain


@dataclass(frozen=True)
class Revision:
    commit: str
    modified: bool

    def describe(self) -> dict[str, object]:
        return {"commit": self.commit, "modified": self.modified}


def load_revision(commands: Commands) -> Revision:
    head = commands.capture(["git", "rev-parse", "HEAD"])
    status = commands.capture(["git", "status", "--porcelain", "--untracked-files=no"])
    if head.status != 0 or status.status != 0:
        raise RunnerError(f"git could not name the tested revision: {head.stderr or status.stderr}")
    return Revision(commit=head.stdout.strip(), modified=bool(status.stdout.strip()))


@dataclass(frozen=True)
class GitHubRun:
    run: str
    attempt: str
    job: str

    def attempt_name(self) -> str:
        return f"github-{self.run}-{self.attempt}"

    def describe(self) -> dict[str, str]:
        return {"provider": "github", "run": self.run, "attempt": self.attempt, "job": self.job}


@dataclass(frozen=True)
class LocalRun:
    started: datetime.datetime
    process: int

    def attempt_name(self) -> str:
        return f"local-{self.started:%Y%m%dT%H%M%S.%fZ}-{self.process}"

    def describe(self) -> dict[str, str]:
        return {"provider": "local"}


def run_identity(
    environment: Mapping[str, str], started: datetime.datetime, process: int
) -> GitHubRun | LocalRun:
    if environment.get("GITHUB_ACTIONS") != "true":
        return LocalRun(started=started, process=process)
    run = environment.get("GITHUB_RUN_ID", "")
    attempt = environment.get("GITHUB_RUN_ATTEMPT", "")
    job = environment.get("GITHUB_JOB", "")
    if not run or not attempt or not job:
        raise RunnerError("GitHub Actions did not name the run, its attempt and the job")
    return GitHubRun(run=run, attempt=attempt, job=job)


@dataclass(frozen=True)
class Workspace:
    """The repository whose sources are measured and the Cargo target directory it builds into."""

    root: Path
    target: Path
    mode: str = "ordinary"

    def build(self) -> Path:
        name = BUILD_DIRECTORY if self.mode == "ordinary" else f"{BUILD_DIRECTORY}-{self.mode}"
        return self.target / name

    def new_attempt(self, producer: Producer, toolchain: str, name: str) -> Path:
        directory = (
            self.target
            / COLLECTION_DIRECTORY
            / producer.name
            / producer.mode
            / toolchain
            / name
        )
        directory.parent.mkdir(parents=True, exist_ok=True)
        try:
            directory.mkdir()
        except FileExistsError:
            raise RunnerError(
                f"{directory} already exists; a collection never reuses an attempt's directory"
            ) from None
        (directory / PROFILES).mkdir()
        return directory

    @contextmanager
    def build_lock(self) -> Iterator[None]:
        """Hold the instrumented build directory from a recipe's build until its export.

        Another collection would otherwise rebuild an executable between its run and its export.
        """

        build = self.build()
        build.mkdir(parents=True, exist_ok=True)
        with (build / LOCK).open("w") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise RunnerError(f"another native coverage run is using {build}") from None
            yield

    def display(self, path: Path) -> str:
        if path.is_relative_to(self.root):
            return str(path.relative_to(self.root))
        return str(path)


def parse_exports(text: str) -> dict[str, str]:
    """Read the output of `cargo llvm-cov show-env --sh`: one `export NAME=value` per line."""

    exported: dict[str, str] = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        words = shlex.split(line)
        if len(words) != 2 or words[0] != "export" or "=" not in words[1]:
            raise RunnerError(f"unexpected line from cargo llvm-cov show-env: {line}")
        name, _, value = words[1].partition("=")
        exported[name] = value
    return exported


def compiler_flags(exported: Mapping[str, str]) -> list[str]:
    encoded = exported.get("CARGO_ENCODED_RUSTFLAGS")
    if encoded is not None:
        return [flag for flag in encoded.split(ENCODED_FLAG_SEPARATOR) if flag]
    return exported.get("RUSTFLAGS", "").split()


def instruments_coverage(flags: Sequence[str]) -> bool:
    for index, flag in enumerate(flags):
        if flag == "-Cinstrument-coverage":
            return True
        if flag == "-C" and index + 1 < len(flags) and flags[index + 1] == "instrument-coverage":
            return True
    return False


# The cfg values `cargo llvm-cov` sets beside the instrumentation, which belong to the crates it
# instruments.
COVERAGE_CFGS = frozenset({"coverage", "coverage_nightly"})


@dataclass(frozen=True)
class SeparatedFlags:
    """A build's compiler flags apart from the instrumentation a workspace wrapper adds instead."""

    build: tuple[str, ...]
    coverage: tuple[str, ...]


def separate_coverage_flags(flags: Sequence[str]) -> SeparatedFlags:
    build: list[str] = []
    coverage: list[str] = []
    index = 0
    while index < len(flags):
        flag = flags[index]
        following = flags[index + 1] if index + 1 < len(flags) else None
        if flag == "-C" and following == "instrument-coverage":
            coverage.extend([flag, following])
            index += 2
            continue
        if flag == "--cfg" and following in COVERAGE_CFGS:
            coverage.extend([flag, following])
            index += 2
            continue
        if flag == "-Cinstrument-coverage" or flag.removeprefix("--cfg=") in COVERAGE_CFGS:
            coverage.append(flag)
        else:
            build.append(flag)
        index += 1
    return SeparatedFlags(build=tuple(build), coverage=tuple(coverage))


def runner_variable(host: str) -> str:
    return "CARGO_TARGET_" + host.upper().replace("-", "_").replace(".", "_") + "_RUNNER"


@dataclass(frozen=True)
class Instrumentation:
    environment: dict[str, str]
    # The flags every crate of the build is compiled with.
    flags: tuple[str, ...]
    crates: InstrumentedCrates = InstrumentedCrates.EVERY
    # The flags the workspace wrapper adds to workspace crates alone.
    workspace_flags: tuple[str, ...] = ()

    def describe(self, workspace: Workspace) -> dict[str, object]:
        description: dict[str, object] = {
            "target_directory": workspace.display(workspace.build()),
            "instrumented_crates": str(self.crates),
            "rustflags": list(self.flags),
            "runtime_profiles": f"{PROFILES}/{PROFILE_PATTERN}",
            "build_time_profiles": BUILD_TIME_PROFILE,
        }
        if self.crates is InstrumentedCrates.WORKSPACE:
            description["workspace_rustflags"] = list(self.workspace_flags)
        return description


def instrumentation(
    commands: Commands,
    workspace: Workspace,
    toolchain: Toolchain,
    attempt: Path,
    base: Mapping[str, str],
    crates: InstrumentedCrates = InstrumentedCrates.EVERY,
) -> Instrumentation:
    runner = runner_variable(toolchain.host)
    if runner in base:
        raise RunnerError(
            f"{runner} is already set; native coverage runs Cargo's executables through its own runner"
        )
    command = [sys.executable, str(SCRIPT), "exec"]
    for part in command:
        if any(character.isspace() for character in part):
            raise RunnerError(f"{part} contains whitespace, which Cargo's runner setting cannot carry")
    environment = dict(base)
    environment["CARGO_TARGET_DIR"] = str(workspace.build())
    shown = commands.capture(
        ["cargo", "llvm-cov", "show-env", "--sh", "--no-rustc-wrapper"], environment=environment
    )
    if shown.status != 0:
        raise RunnerError(
            f"cargo llvm-cov show-env exited with status {shown.status}: {shown.stderr.strip()}"
        )
    exported = parse_exports(shown.stdout)
    for wrapper in ("RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER"):
        if wrapper in exported:
            raise RunnerError(
                f"cargo llvm-cov show-env sets {wrapper}; the configured compiler wrapper must stay"
            )
    flags = compiler_flags(exported)
    if not instruments_coverage(flags):
        raise RunnerError("cargo llvm-cov show-env did not enable -C instrument-coverage")
    exported.pop("LLVM_PROFILE_FILE", None)
    environment.update(exported)
    environment["LLVM_PROFILE_FILE"] = BUILD_TIME_PROFILE
    environment[runner] = " ".join(command)
    environment[ATTEMPT_VARIABLE] = str(attempt)
    environment[PREPARED_TARGET_VARIABLE] = str(workspace.target)
    if crates is InstrumentedCrates.EVERY:
        return Instrumentation(environment=environment, flags=tuple(flags))

    if "RUSTC_WORKSPACE_WRAPPER" in base:
        raise RunnerError(
            "RUSTC_WORKSPACE_WRAPPER is already set; instrumenting only workspace crates composes "
            "a workspace compiler wrapper of its own"
        )
    separated = separate_coverage_flags(flags)
    if "CARGO_ENCODED_RUSTFLAGS" in exported:
        environment["CARGO_ENCODED_RUSTFLAGS"] = ENCODED_FLAG_SEPARATOR.join(separated.build)
    else:
        environment["RUSTFLAGS"] = " ".join(separated.build)
    environment["RUSTC_WORKSPACE_WRAPPER"] = str(WORKSPACE_WRAPPER)
    environment[WORKSPACE_FLAGS_VARIABLE] = ENCODED_FLAG_SEPARATOR.join(separated.coverage)
    return Instrumentation(
        environment=environment,
        flags=separated.build,
        crates=crates,
        workspace_flags=separated.coverage,
    )


def aligned(size: int, alignment: int) -> int:
    return (size + alignment - 1) // alignment * alignment


def gnu_build_id(notes: bytes, alignment: int) -> str | None:
    """Find the GNU build ID among the notes of one note segment.

    Each note's name and descriptor start at offsets aligned to the segment's alignment.
    """

    position = 0
    while position + 12 <= len(notes):
        name_size = int.from_bytes(notes[position : position + 4], "little")
        description_size = int.from_bytes(notes[position + 4 : position + 8], "little")
        note_type = int.from_bytes(notes[position + 8 : position + 12], "little")
        name_start = position + 12
        name = notes[name_start : name_start + name_size]
        description_start = aligned(name_start + name_size, alignment)
        description_end = description_start + description_size
        if note_type == NT_GNU_BUILD_ID and name == GNU_NOTE_NAME:
            return notes[description_start:description_end].hex()
        position = aligned(description_end, alignment)
    return None


def build_id(path: Path) -> str:
    """The GNU build ID of a 64-bit little-endian ELF executable or shared object, in hex.

    LLVM writes the same ID into every raw profile, which is how a profile names its executable.
    """

    with path.open("rb") as file:
        header = file.read(64)
        if header[:4] != ELF_MAGIC:
            raise RunnerError(f"{path} is not an ELF file")
        if len(header) < 64:
            raise RunnerError(f"{path} is a truncated ELF file")
        if header[4] != ELF_CLASS_64 or header[5] != ELF_LITTLE_ENDIAN:
            raise RunnerError(f"{path} is not a 64-bit little-endian ELF file")
        table = int.from_bytes(header[32:40], "little")
        entry_size = int.from_bytes(header[54:56], "little")
        entries = int.from_bytes(header[56:58], "little")
        for index in range(entries):
            file.seek(table + index * entry_size)
            entry = file.read(entry_size)
            if len(entry) < 56:
                raise RunnerError(f"{path} is a truncated ELF file")
            if int.from_bytes(entry[0:4], "little") != PT_NOTE:
                continue
            offset = int.from_bytes(entry[8:16], "little")
            size = int.from_bytes(entry[32:40], "little")
            alignment = max(int.from_bytes(entry[48:56], "little"), 4)
            file.seek(offset)
            notes = file.read(size)
            # A linker may place 4-byte aligned notes in a segment aligned for 8-byte ones.
            for note_alignment in dict.fromkeys((alignment, 4)):
                identifier = gnu_build_id(notes, note_alignment)
                if identifier is not None:
                    return identifier
    raise RunnerError(f"{path} carries no GNU build ID, so its profiles cannot name it")


def is_elf(path: Path) -> bool:
    try:
        with path.open("rb") as file:
            return file.read(4) == ELF_MAGIC
    except OSError:
        return False


def executable_candidates(build: Path) -> Iterator[Path]:
    """The executables and shared objects of every profile in a Cargo target directory.

    Build scripts are left out: they run while Cargo builds, never as a child of a check.
    """

    if not build.is_dir():
        return
    # Tooling uses nested Cargo target directories, and Cargo can place procedural macro shared
    # objects under build/<package>/<hash>/out. Those are loaded by the compiler being checked.
    for directory, subdirectories, names in os.walk(build):
        subdirectories[:] = sorted(name for name in subdirectories if not name.startswith((".", "incremental")))
        for name in sorted(names):
            if name.startswith(("build-script-build", "build_script_build")):
                continue
            entry = Path(directory) / name
            if entry.is_file() and not entry.is_symlink() and is_elf(entry):
                yield entry


def locate(build: Path, identifiers: set[str]) -> dict[str, Path]:
    found: dict[str, Path] = {}
    for candidate in executable_candidates(build):
        try:
            identifier = build_id(candidate)
        except RunnerError:
            continue
        if identifier in identifiers and identifier not in found:
            found[identifier] = candidate
    return found


def execute(arguments: Sequence[str], environment: Mapping[str, str]) -> int:
    """Run as Cargo's runner: record the executable, then become it with its profiles collected."""

    attempt = environment.get(ATTEMPT_VARIABLE)
    if not arguments or not attempt:
        print("native coverage: `exec` runs only as Cargo's runner inside `run`", file=sys.stderr)
        return 2
    executable = Path(arguments[0])
    entry = {
        "executable": str(executable.resolve()),
        "arguments": list(arguments[1:]),
        "build_id": build_id(executable),
    }
    with (Path(attempt) / EXECUTIONS).open("a", encoding="utf-8") as log:
        log.write(json.dumps(entry) + "\n")
    child = dict(environment)
    child["LLVM_PROFILE_FILE"] = str(Path(attempt) / PROFILES / PROFILE_PATTERN)
    os.execve(executable, list(arguments), child)


@dataclass(frozen=True)
class Execution:
    executable: Path
    arguments: tuple[str, ...]
    build_id: str


def read_executions(path: Path) -> list[Execution]:
    if not path.is_file():
        return []
    executions: list[Execution] = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        try:
            entry = json.loads(line)
            execution = Execution(
                executable=Path(entry["executable"]),
                arguments=tuple(entry["arguments"]),
                build_id=entry["build_id"],
            )
        except (json.JSONDecodeError, KeyError, TypeError) as error:
            raise RunnerError(f"{path.name} line {number} is not an execution: {error}") from None
        executions.append(execution)
    return executions


def profile_binary_ids(commands: Commands, toolchain: Toolchain, profile: Path) -> tuple[str, ...]:
    shown = commands.capture(
        [str(toolchain.tool("llvm-profdata")), "show", "--binary-ids", str(profile)]
    )
    if shown.status != 0:
        detail = shown.stderr.strip().splitlines()[:1] or [f"status {shown.status}"]
        raise RunnerError(f"{profile.name} is not a readable raw profile: {detail[0]}")
    identifiers: list[str] = []
    listing = False
    for line in shown.stdout.splitlines():
        if line.startswith("Binary IDs:"):
            listing = True
            continue
        if not listing:
            continue
        candidate = line.strip()
        if not candidate:
            continue
        if any(character not in "0123456789abcdef" for character in candidate):
            break
        identifiers.append(candidate)
    if not identifiers:
        raise RunnerError(f"{profile.name} names no binary ID, so its executable cannot be found")
    return tuple(identifiers)


@dataclass(frozen=True)
class Selected:
    """An executable whose counters the report reads."""

    executable: Path
    build_id: str


@dataclass(frozen=True)
class Selection:
    """What a run executed, and the profiles that hold its counters.

    The executions and their children are the same for a fresh and a cached build of the same
    revision; the profile files are named by process and differ from run to run.
    """

    executions: tuple[Execution, ...]
    executed: tuple[Selected, ...]
    children: tuple[Selected, ...]
    profiles: Mapping[Path, tuple[str, ...]]

    def objects(self) -> list[Path]:
        return [selected.executable for selected in (*self.executed, *self.children)]

    def describe(self, workspace: Workspace) -> dict[str, object]:
        executions: list[dict[str, object]] = []
        for execution in self.executions:
            executions.append(
                {
                    "executable": workspace.display(execution.executable),
                    "arguments": list(execution.arguments),
                    "build_id": execution.build_id,
                }
            )
        children: list[dict[str, object]] = []
        for child in self.children:
            children.append(
                {"executable": workspace.display(child.executable), "build_id": child.build_id}
            )
        return {"executions": executions, "children": children}

    def describe_profiles(self) -> list[dict[str, object]]:
        profiles: list[dict[str, object]] = []
        for profile in sorted(self.profiles):
            profiles.append({"file": profile.name, "binary_ids": list(self.profiles[profile])})
        return profiles


def select(
    executions: Sequence[Execution],
    profiles: Mapping[Path, tuple[str, ...]],
    current_build_id: Callable[[Path], str],
    locate_children: Callable[[set[str]], dict[str, Path]],
) -> Selection:
    """Match the profiles to the executables Cargo ran and to the children those started.

    Every executable that ran must still be the one that ran and must have written a profile, and
    every profile must name an executable that is still there to export.
    """

    if not executions:
        raise RunnerError("the instrumented recipe ran no executable through Cargo")
    if not profiles:
        raise RunnerError("no executable wrote a profile")
    written: set[str] = set()
    for identifiers in profiles.values():
        written.update(identifiers)

    executed: dict[Path, Selected] = {}
    for execution in executions:
        if execution.executable in executed:
            if executed[execution.executable].build_id != execution.build_id:
                raise RunnerError(f"{execution.executable} was rebuilt between two of its runs")
            continue
        if not execution.executable.is_file():
            raise RunnerError(f"{execution.executable} ran but is no longer there to export")
        if current_build_id(execution.executable) != execution.build_id:
            raise RunnerError(f"{execution.executable} was rebuilt after it ran")
        if execution.build_id not in written:
            raise RunnerError(f"{execution.executable} ran but wrote no profile")
        executed[execution.executable] = Selected(
            executable=execution.executable, build_id=execution.build_id
        )

    executed_ids = {selected.build_id for selected in executed.values()}
    remaining = written - executed_ids
    located = locate_children(remaining) if remaining else {}
    unretained = sorted(remaining - set(located))
    if unretained:
        raise RunnerError(
            "profiles name executables that are not retained for export: " + ", ".join(unretained)
        )
    children: list[Selected] = []
    for identifier in sorted(located):
        children.append(Selected(executable=located[identifier], build_id=identifier))
    return Selection(
        executions=tuple(executions),
        executed=tuple(executed.values()),
        children=tuple(children),
        profiles=dict(profiles),
    )


@dataclass(frozen=True)
class SourcePolicy:
    """Which files of an LLVM report are repository sources, and why the others are not."""

    root: Path
    generated: tuple[Path, ...]

    def classify(self, source: str) -> Classification:
        path = Path(source)
        if not path.is_absolute():
            path = self.root / path
        path = path.resolve()
        if not path.is_relative_to(self.root):
            return Classification.DEPENDENCY
        for directory in self.generated:
            if path.is_relative_to(directory):
                return Classification.GENERATED
        relative = path.relative_to(self.root)
        if HARNESS_DIRECTORIES.intersection(relative.parts[:-1]):
            return Classification.HARNESS
        return Classification.INCLUDED


@dataclass(frozen=True)
class LcovRecord:
    source: str
    lines: tuple[str, ...]
    executable: int
    covered: int


def lcov_records(lines: Iterable[str]) -> Iterator[LcovRecord]:
    source: str | None = None
    collected: list[str] = []
    executable = 0
    covered = 0
    for line in lines:
        if line.startswith("SF:"):
            if source is not None:
                raise RunnerError(f"the LCOV record of {source} has no end_of_record")
            source = line[3:].rstrip("\n")
            collected = [line]
            executable = 0
            covered = 0
            continue
        if source is None:
            if line.strip() and not line.startswith("TN:"):
                raise RunnerError(f"LCOV line outside a record: {line.rstrip()}")
            continue
        collected.append(line)
        if line.startswith("DA:"):
            fields = line[3:].rstrip("\n").split(",")
            if len(fields) < 2 or not fields[1].isdigit():
                raise RunnerError(f"malformed LCOV line in the record of {source}: {line.rstrip()}")
            executable += 1
            if int(fields[1]) > 0:
                covered += 1
        elif line.rstrip("\n") == "end_of_record":
            yield LcovRecord(source, tuple(collected), executable, covered)
            source = None
    if source is not None:
        raise RunnerError(f"the LCOV record of {source} is incomplete")


@dataclass(frozen=True)
class PackageDirectory:
    directory: Path
    name: str


@dataclass(frozen=True)
class Packages:
    """The workspace packages by directory, most specific first, to attribute each source file."""

    directories: tuple[PackageDirectory, ...]

    @classmethod
    def from_metadata(cls, metadata: Mapping[str, object]) -> Packages:
        packages = metadata.get("packages")
        if not isinstance(packages, list):
            raise RunnerError("cargo metadata listed no packages")
        directories: list[PackageDirectory] = []
        for package in packages:
            manifest = Path(package["manifest_path"]).resolve()
            directories.append(PackageDirectory(directory=manifest.parent, name=package["name"]))
        directories.sort(key=lambda package: len(package.directory.parts), reverse=True)
        return cls(directories=tuple(directories))

    def owner(self, source: str) -> str:
        path = Path(source).resolve()
        for package in self.directories:
            if path.is_relative_to(package.directory):
                return package.name
        raise RunnerError(f"{source} belongs to no workspace package")


def load_packages(commands: Commands) -> Packages:
    metadata = commands.capture(["cargo", "metadata", "--no-deps", "--format-version", "1"])
    if metadata.status != 0:
        raise RunnerError(f"cargo metadata failed: {metadata.stderr.strip()}")
    combined = json.loads(metadata.stdout)
    workspace_metadata = combined.get("metadata")
    if workspace_metadata is None:
        workspace_metadata = {}
    for tooling in workspace_metadata.get("tooling", {}).get("workspaces", []):
        extra = commands.capture(["cargo", "metadata", "--manifest-path", str(commands.root / tooling / "Cargo.toml"), "--no-deps", "--format-version", "1"])
        if extra.status != 0:
            raise RunnerError(f"tooling metadata failed: {extra.stderr.strip()}")
        isolated = json.loads(extra.stdout)
        combined["packages"].extend(isolated["packages"])
        combined["workspace_members"].extend(isolated["workspace_members"])
    return Packages.from_metadata(combined)


@dataclass
class Lines:
    files: int = 0
    executable: int = 0
    covered: int = 0

    def add(self, record: LcovRecord) -> None:
        self.files += 1
        self.executable += record.executable
        self.covered += record.covered

    def describe(self) -> dict[str, int]:
        return {"files": self.files, "executable": self.executable, "covered": self.covered}


@dataclass
class Sources:
    total: Lines = field(default_factory=Lines)
    packages: dict[str, Lines] = field(default_factory=dict)
    excluded: dict[Classification, int] = field(default_factory=dict)

    def describe(self, policy: SourcePolicy) -> dict[str, object]:
        excluded = {str(key): count for key, count in sorted(self.excluded.items())}
        return {
            "root": str(policy.root),
            "policy": {str(key): meaning for key, meaning in SOURCE_POLICY.items()},
            **self.total.describe(),
            "excluded_files": excluded,
            "packages": {
                name: lines.describe() for name, lines in sorted(self.packages.items())
            },
        }


def filter_report(raw: Path, report: Path, policy: SourcePolicy, packages: Packages) -> Sources:
    """Keep the report's repository sources and count the lines each package contributed."""

    sources = Sources()
    temporary = report.with_name(f".{report.name}.tmp")
    with raw.open(encoding="utf-8") as source, temporary.open("w", encoding="utf-8") as destination:
        for record in lcov_records(source):
            classification = policy.classify(record.source)
            if classification is not Classification.INCLUDED:
                sources.excluded[classification] = sources.excluded.get(classification, 0) + 1
                continue
            destination.writelines(record.lines)
            sources.total.add(record)
            package = packages.owner(record.source)
            sources.packages.setdefault(package, Lines()).add(record)
    os.replace(temporary, report)
    return sources


class ExportLog:
    """The LLVM tools' own diagnostics, each bounded, written as they arrive."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def add(self, arguments: Sequence[str], captured: Captured) -> None:
        diagnostics = captured.stderr
        if len(diagnostics) > LOG_LIMIT:
            omitted = len(diagnostics) - LOG_LIMIT
            diagnostics = diagnostics[:LOG_LIMIT] + f"\n[{omitted} more characters omitted]\n"
        with self.path.open("a", encoding="utf-8") as log:
            log.write(f"$ {shlex.join(arguments)}\nexit status {captured.status}\n{diagnostics}\n")


@dataclass(frozen=True)
class Exported:
    selection: Selection
    sources: Sources
    policy: SourcePolicy
    warnings: tuple[str, ...]


def export(
    commands: Commands,
    workspace: Workspace,
    toolchain: Toolchain,
    packages: Packages,
    attempt: Path,
) -> Exported:
    executions = read_executions(attempt / EXECUTIONS)
    profiles: dict[Path, tuple[str, ...]] = {}
    for profile in sorted((attempt / PROFILES).glob("*.profraw")):
        profiles[profile] = profile_binary_ids(commands, toolchain, profile)
    selection = select(
        executions,
        profiles,
        build_id,
        lambda identifiers: locate(workspace.build(), identifiers),
    )

    log = ExportLog(attempt / EXPORT_LOG)
    merged = attempt / MERGED_PROFILE
    merge_arguments = [
        str(toolchain.tool("llvm-profdata")),
        "merge",
        "-sparse",
        "--failure-mode=any",
        "-o",
        str(merged),
        *(str(profile) for profile in sorted(selection.profiles)),
    ]
    merged_outcome = commands.capture(merge_arguments)
    log.add(merge_arguments, merged_outcome)
    if merged_outcome.status != 0:
        raise RunnerError(f"llvm-profdata could not merge the profiles; see {EXPORT_LOG}")

    objects = selection.objects()
    export_arguments = [
        str(toolchain.tool("llvm-cov")),
        "export",
        "-format=lcov",
        f"-instr-profile={merged}",
        str(objects[0]),
    ]
    for other in objects[1:]:
        export_arguments += ["-object", str(other)]
    unfiltered = attempt / UNFILTERED_REPORT
    exported = commands.export(export_arguments, unfiltered)
    log.add(export_arguments, exported)
    if exported.status != 0:
        raise RunnerError(f"llvm-cov could not export the report; see {EXPORT_LOG}")
    # Every profile already names the executable it came from, so a warning here is never a stale
    # profile. A function one executable never ran can carry a different hash there than in the
    # executable that ran it, and llvm-cov reads it from the one that ran it and warns about the other.
    warnings: list[str] = []
    for line in exported.stderr.splitlines():
        if line.strip():
            warnings.append(line.strip())

    policy = SourcePolicy(
        root=workspace.root.resolve(),
        generated=(workspace.target.resolve(), workspace.build().resolve()),
    )
    sources = filter_report(unfiltered, attempt / REPORT, policy, packages)
    unfiltered.unlink()
    if sources.total.files == 0:
        raise RunnerError("the report holds no repository source")
    return Exported(
        selection=selection,
        sources=sources,
        policy=policy,
        warnings=tuple(warnings[:WARNING_LIMIT]),
    )


class Record:
    """The completion record beside a report, replaced whole at every verdict."""

    def __init__(self, path: Path, content: dict[str, object]) -> None:
        self.path = path
        self.content = content

    def write(self) -> None:
        temporary = self.path.with_name(f".{self.path.name}.tmp")
        temporary.write_text(json.dumps(self.content, indent=2) + "\n", encoding="utf-8")
        os.replace(temporary, self.path)

    def conclude(self, verdict: Verdict, finished: datetime.datetime) -> None:
        self.content["verdict"] = str(verdict)
        self.content["finished_at"] = timestamp(finished)
        self.write()

    def fail(self, verdict: Verdict, stage: Stage, detail: str, finished: datetime.datetime) -> None:
        self.content["failure"] = {"stage": str(stage), "detail": detail}
        self.conclude(verdict, finished)


def timestamp(moment: datetime.datetime) -> str:
    return moment.astimezone(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass(frozen=True)
class Context:
    """What every producer of one invocation shares."""

    workspace: Workspace
    toolchain: Toolchain
    revision: Revision
    run: GitHubRun | LocalRun
    packages: Packages
    clock: Callable[[], datetime.datetime]
    environment: Mapping[str, str]


@dataclass(frozen=True)
class Collected:
    status: int
    attempt: Path
    record: Record


def collect(
    commands: Commands, context: Context, producer: Producer, filter_text: str = ""
) -> Collected:
    """Run one producer's check with its native executions collected, and write its record."""

    # Cargo's standalone `exec` runner also runs from fixture workspaces with no Python package.
    # Only collection needs the canonical model report reader.
    from scripts.model_evidence import (
        EvidenceError,
        FILENAME as MODEL_REPORT,
        VARIABLE as MODEL_VARIABLE,
        read_complete,
    )
    from scripts.deloxide_lane import (
        RECORD as LANE_REPORT,
        REPORT_VARIABLE as LANE_VARIABLE,
        RecordError,
        read_complete as read_complete_lane,
    )

    workspace = Workspace(context.workspace.root, context.workspace.target, mode=producer.mode)
    toolchain = context.toolchain
    environment = dict(context.environment)
    label = f"rust-{producer.toolchain}" if producer.toolchain else toolchain.label()
    attempt = workspace.new_attempt(producer, label, context.run.attempt_name())
    record = Record(
        attempt / RECORD,
        {
            "producer": producer.name,
            "mode": producer.mode,
            "verdict": str(Verdict.RUNNING),
            "rerun": producer.rerun(filter_text),
            "revision": context.revision.describe(),
            "run": context.run.describe(),
            "attempt": attempt.name,
            "started_at": timestamp(context.clock()),
            "toolchain": (
                {"requested": producer.toolchain} if producer.toolchain else toolchain.describe()
            ),
            "recipes": {
                "prepare": list(producer.prepare),
                "instrumented": producer.instrumented,
                "finish": list(producer.finish),
            },
        },
    )
    stages: dict[str, str] = {}
    record.content["stages"] = stages

    def enter(next_stage: Stage) -> Stage:
        stages[str(next_stage)] = timestamp(context.clock())
        record.write()
        return next_stage

    stage = enter(Stage.PREPARE)
    try:
        for recipe in producer.prepare:
            status = commands.stream(["just", recipe], environment=context.environment)
            if status != 0:
                detail = f"`just {recipe}` exited with status {status}"
                record.fail(Verdict.FAILED, stage, detail, context.clock())
                return Collected(status, attempt, record)

        if producer.toolchain:
            environment["RUSTUP_TOOLCHAIN"] = producer.toolchain
            toolchain = load_toolchain(commands, environment)
            record.content["toolchain"] = toolchain.describe()
            record.write()

        stage = enter(Stage.INSTRUMENT)
        instrumented = instrumentation(
            commands, workspace, toolchain, attempt, environment, producer.instrumented_crates
        )
        if producer.filterable:
            instrumented.environment[MODEL_VARIABLE] = str(attempt / MODEL_REPORT)
            record.content["filter"] = filter_text
        if producer.diagnostic_lane:
            instrumented.environment[LANE_VARIABLE] = str(attempt / LANE_REPORT)
        record.content["instrumentation"] = instrumented.describe(workspace)
        record.write()

        stage = enter(Stage.RUN)
        with workspace.build_lock():
            arguments = ["just", producer.instrumented]
            if producer.filterable:
                arguments.append(filter_text)
            status = commands.stream(
                arguments, environment=instrumented.environment
            )
            if status != 0:
                detail = f"`just {producer.instrumented}` exited with status {status}"
                record.fail(Verdict.FAILED, stage, detail, context.clock())
                return Collected(status, attempt, record)
            if producer.filterable:
                record.content["models"] = read_complete(attempt / MODEL_REPORT, producer.mode, filter_text)
                record.write()
            if producer.diagnostic_lane:
                lane = read_complete_lane(attempt / LANE_REPORT, producer.mode)
                record.content["lane"] = {
                    "record": LANE_REPORT,
                    "attempt": dict(lane["workspace"])["attempt"],
                    "counts": lane["counts"],
                    "findings": lane["findings"],
                }
                record.write()
            stage = enter(Stage.EXPORT)
            exported = export(commands, workspace, toolchain, context.packages, attempt)
        record.content["selection"] = exported.selection.describe(workspace)
        record.content["profiles"] = exported.selection.describe_profiles()
        record.content["export_warnings"] = list(exported.warnings)
        record.content["sources"] = exported.sources.describe(exported.policy)
        record.content["report"] = REPORT
        record.write()

        stage = enter(Stage.FINISH)
        for recipe in producer.finish:
            status = commands.stream(["just", recipe], environment=environment)
            if status != 0:
                detail = f"`just {recipe}` exited with status {status}"
                record.fail(Verdict.FAILED, stage, detail, context.clock())
                return Collected(status, attempt, record)
    except KeyboardInterrupt:
        record.fail(Verdict.INTERRUPTED, stage, "interrupted by SIGINT", context.clock())
        return Collected(128 + signal.SIGINT, attempt, record)
    except Interrupted as interruption:
        detail = f"interrupted by {interruption}"
        record.fail(Verdict.INTERRUPTED, stage, detail, context.clock())
        return Collected(128 + interruption.number, attempt, record)
    except (RunnerError, EvidenceError, RecordError) as error:
        record.fail(Verdict.FAILED, stage, str(error), context.clock())
        return Collected(1, attempt, record)
    record.conclude(Verdict.COMPLETE, context.clock())
    return Collected(0, attempt, record)


def summary(workspace: Workspace, producer: Producer, collected: Collected) -> list[str]:
    content = collected.record.content
    heading = f"native coverage: {producer.name} ({producer.mode}) {content['verdict']}"
    failure = content.get("failure")
    if isinstance(failure, Mapping):
        heading += f" at {failure['stage']}: {failure['detail']}"
    lines = [heading]
    sources = content.get("sources")
    if isinstance(sources, Mapping) and not isinstance(failure, Mapping):
        lines.append(
            f"  {sources['covered']:,} of {sources['executable']:,} lines covered in "
            f"{sources['files']:,} repository files"
        )
    if (collected.attempt / REPORT).is_file():
        lines.append(f"  report: {workspace.display(collected.attempt / REPORT)}")
    lines.append(f"  record: {workspace.display(collected.record.path)}")
    lines.append(f"  rerun:  {content['rerun']}")
    return lines


def publish_step_summary(environment: Mapping[str, str], lines: Sequence[str]) -> None:
    path = environment.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as step_summary:
        step_summary.write("```\n" + "\n".join(lines) + "\n```\n")


@contextmanager
def interruptible() -> Iterator[None]:
    """Turn SIGTERM into an exception, so a cancelled collection still records its verdict."""

    def interrupt(number: int, frame: object) -> None:
        raise Interrupted(number)

    previous = signal.signal(signal.SIGTERM, interrupt)
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, previous)


def utc_now() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC)


def run(
    commands: Commands,
    workspace: Workspace,
    names: Sequence[str],
    producers: Sequence[Producer],
    environment: Mapping[str, str],
    filter_text: str = "",
    output: Path | None = None,
) -> int:
    selected = select_producers(names, producers)
    if filter_text and any(not producer.filterable for producer in selected):
        raise RunnerError("a filter is supported only by the canonical Shuttle and Loom producers")
    if output is not None and len(selected) != 1:
        raise RunnerError("an output path requires exactly one producer; each mode keeps its own report")
    dumped = commands.capture(["just", "--dump", "--dump-format", "json"])
    if dumped.status != 0:
        raise RunnerError(f"just could not describe the justfile: {dumped.stderr.strip()}")
    recipes = json.loads(dumped.stdout).get("recipes", {})
    for producer in selected:
        validate_composition(producer, recipes)
    context = Context(
        workspace=workspace,
        toolchain=load_toolchain(commands),
        revision=load_revision(commands),
        run=run_identity(environment, utc_now(), os.getpid()),
        packages=load_packages(commands),
        clock=utc_now,
        environment=environment,
    )
    for producer in selected:
        collected = collect(commands, context, producer, filter_text)
        lines = summary(workspace, producer, collected)
        print("\n".join(lines), flush=True)
        publish_step_summary(environment, lines)
        if collected.status != 0:
            return collected.status
        if output is not None:
            output.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(collected.attempt / REPORT, output)
    return 0


def main(
    argv: Sequence[str] | None = None,
    *,
    producers: Sequence[Producer] = PRODUCERS,
    commands_for: Callable[[Path], Commands] = Commands,
    environment: Mapping[str, str] | None = None,
) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    current = os.environ if environment is None else environment
    if arguments[:1] == ["exec"]:
        try:
            return execute(arguments[1:], current)
        except (RunnerError, OSError) as error:
            print(f"native coverage: {error}", file=sys.stderr)
            return 1

    names = ", ".join(producer.name for producer in producers)
    parser = argparse.ArgumentParser(
        description=__doc__.splitlines()[0],
        epilog=f"Run it as `just coverage-native-extras [producer ...]`, producers: {names}.",
    )
    parser.add_argument("--root", type=Path, default=REPOSITORY)
    parser.add_argument("--target-dir", type=Path, required=True)
    subcommands = parser.add_subparsers(dest="command", required=True)
    run_parser = subcommands.add_parser("run", help="collect the named producers, or all of them")
    run_parser.add_argument("producers", nargs="*", metavar="producer", help=f"one of {names}")
    run_parser.add_argument("--filter", default="", help="canonical Shuttle test or Loom invariant filter")
    run_parser.add_argument("--output", type=Path, help="copy one complete report to this path")
    subcommands.add_parser("exec", help="Cargo's runner inside `run`; not for direct use")
    parsed = parser.parse_args(arguments)

    root = parsed.root.resolve()
    workspace = Workspace(root=root, target=parsed.target_dir.resolve())
    commands = commands_for(root)
    try:
        with interruptible():
            return run(commands, workspace, parsed.producers, producers, current, parsed.filter, parsed.output)
    except RunnerError as error:
        print(f"native coverage: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
