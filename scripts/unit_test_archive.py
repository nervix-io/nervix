#!/usr/bin/env python3

"""Carry one build of the workspace's unit tests to the job that runs them.

One CI job builds the scenario suite and the unit tests in the same target directory, so their
dependencies compile once and the jobs that run them need no compiler or cache. `just
test-coverage-binaries` keeps Cargo's messages in `target/unit-tests/cargo-messages.jsonl`, and
`plan` reads the test executables from them into `target/unit-tests/programs.json`. `pack` writes
those executables, every other object `cargo llvm-cov report` reads and the generated inputs a test
reads into one archive, and `unpack` restores it into a checkout of the same revision and path,
with the checks `scripts/scenario_shards.py` makes for the scenario suite.

The scenario suite's own executable stays out: the shards run it, and it is the largest object in
the directory. The pack therefore takes every other object the target directory holds, which is the
set the report would read after a build of the unit tests alone, and the tools the suite installed
beside them.

`run` compiles nothing. Cargo would compare the restored files with sources whose modification times
the checkout set and rebuild them, so `run` starts each test executable itself, one at a time as
`cargo test` does: from its package's directory, with that directory as `CARGO_MANIFEST_DIR`, which
is what the workspace's tests and their libraries read of Cargo's environment at run time, and with
the loader's path Cargo gives them, because a procedural macro crate's tests link the toolchain's
`libstd`. Unlike `cargo test` it goes on after a failure, so one run names every test executable
that failed.
"""

from __future__ import annotations

import argparse
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
import json
import os
from pathlib import Path
import subprocess
import sys

from scripts import scenario_shards as shards

ROOT = shards.ROOT
MESSAGES = Path("target/unit-tests/cargo-messages.jsonl")
PROGRAMS = Path("target/unit-tests/programs.json")
MANIFEST = Path("target/unit-tests/manifest.json")


@dataclass(frozen=True)
class TestProgram:
    """One test executable and the package directory it runs in, both below the checkout."""

    executable: str
    directory: str

    def to_json(self) -> dict[str, str]:
        return {"executable": self.executable, "directory": self.directory}


def programs_of(root: Path, messages: Iterable[str]) -> list[TestProgram]:
    """The test executables Cargo's messages name, once each, in the order Cargo built them."""
    programs: dict[str, TestProgram] = {}
    for line in messages:
        message = json.loads(line)
        if message.get("reason") != "compiler-artifact":
            continue
        executable = message.get("executable")
        if executable is None or not message["profile"]["test"]:
            continue
        path = Path(executable).resolve().relative_to(root).as_posix()
        directory = Path(message["manifest_path"]).resolve().parent.relative_to(root).as_posix()
        programs[path] = TestProgram(path, directory)
    return list(programs.values())


def plan(root: Path) -> list[TestProgram]:
    """Write the list of test executables a run starts, from the messages of the build."""
    try:
        lines = (root / MESSAGES).read_text().splitlines()
    except FileNotFoundError as error:
        raise shards.ShardError(f"{MESSAGES} is missing; run `just test-coverage-build` first") from error
    programs = programs_of(root, lines)
    if not programs:
        raise shards.ShardError(f"{MESSAGES} names no test executable")
    document = [program.to_json() for program in programs]
    (root / PROGRAMS).write_text(json.dumps(document, indent=1) + "\n")
    print(f"planned {len(programs)} test executables")
    return programs


def read_programs(root: Path) -> list[TestProgram]:
    try:
        document = json.loads((root / PROGRAMS).read_text())
    except FileNotFoundError as error:
        raise shards.ShardError(f"{PROGRAMS} is missing; build or restore the unit tests first") from error
    return [TestProgram(entry["executable"], entry["directory"]) for entry in document]


def pack(root: Path, archive: Path) -> shards.Manifest:
    """Write the archive of the unit tests' build and the generated inputs they read."""
    objects = shards.coverage_objects(root, shards.object_pattern(shards.workspace_targets(root)))
    suite = shards.scenario_executable(objects)
    kept = [path for path in objects if path != suite]
    held = {path.as_posix() for path in kept}
    for program in read_programs(root):
        if program.executable not in held:
            raise shards.ShardError(f"the build left no {program.executable}")
    return shards.write_archive(root, archive, MANIFEST, [*kept, *shards.generated_inputs(root), PROGRAMS])


def library_path(root: Path) -> str:
    """The loader's path `cargo test` gives a test executable: the build's directories, then the
    toolchain's, then what the environment held."""
    completed = subprocess.run(
        ["rustc", "--print", "target-libdir"], cwd=root, check=True, capture_output=True, text=True
    )
    debug = root / shards.COVERAGE_DEBUG
    directories = [str(debug / "deps"), str(debug), completed.stdout.strip()]
    inherited = os.environ.get("LD_LIBRARY_PATH")
    if inherited:
        directories.append(inherited)
    return os.pathsep.join(directories)


def unpack(root: Path, archive: Path) -> shards.Manifest:
    """Restore the unit tests' archive into the checkout at `root`."""
    environment = {**os.environ, "LD_LIBRARY_PATH": library_path(root)}
    return shards.restore_archive(root, archive, MANIFEST, environment)


def run(root: Path) -> None:
    """Run every test executable, and fail naming each one that failed."""
    programs = read_programs(root)
    loader_path = library_path(root)
    failed = []
    for index, program in enumerate(programs, start=1):
        print(f"[{index}/{len(programs)}] {program.executable} in {program.directory}", flush=True)
        directory = root / program.directory
        environment = {**os.environ, "CARGO_MANIFEST_DIR": str(directory), "LD_LIBRARY_PATH": loader_path}
        completed = subprocess.run([str(root / program.executable)], cwd=directory, env=environment)
        if completed.returncode != 0:
            failed.append(f"{program.executable} exited {completed.returncode}")
    if failed:
        lines = "\n".join(f"  {entry}" for entry in failed)
        raise shards.ShardError(f"{len(failed)} of {len(programs)} test executables failed:\n{lines}")
    print(f"{len(programs)} test executables passed")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("plan", help="list the test executables the build left")
    pack_command = commands.add_parser("pack", help="write the archive the tests job restores")
    pack_command.add_argument("archive", type=Path)
    unpack_command = commands.add_parser("unpack", help="restore the archive into this checkout")
    unpack_command.add_argument("archive", type=Path)
    commands.add_parser("run", help="run every test executable")
    arguments = parser.parse_args(argv)
    try:
        if arguments.command == "plan":
            plan(ROOT)
        elif arguments.command == "pack":
            pack(ROOT, arguments.archive)
        elif arguments.command == "unpack":
            unpack(ROOT, arguments.archive)
        else:
            run(ROOT)
    except shards.ShardError as error:
        print(f"unit tests: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
