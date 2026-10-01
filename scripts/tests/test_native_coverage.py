"""Tests for the native coverage collector: its inventory, source policy, selection and verdicts.

The toolchain tests build a fixture crate with the real toolchain and collect it end to end. They
are skipped where just, cargo-llvm-cov or the toolchain's LLVM tools are missing, unless
NERVIX_NATIVE_COVERAGE_TOOLCHAIN_TESTS=required, which `just test-native-coverage` sets so that CI
cannot skip them.
"""

from __future__ import annotations

import datetime
import io
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import textwrap
import tomllib
import unittest
from collections.abc import Callable, Mapping
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass
from pathlib import Path
from unittest import mock

from scripts import native_coverage
from scripts.native_coverage import (
    Captured,
    Classification,
    Commands,
    Context,
    Execution,
    Exported,
    GitHubRun,
    Interrupted,
    LocalRun,
    Packages,
    Producer,
    Revision,
    RunnerError,
    Selected,
    Selection,
    SourcePolicy,
    Sources,
    Toolchain,
    Workspace,
)

REPOSITORY = Path(__file__).resolve().parent.parent.parent
HOST = "x86_64-unknown-linux-gnu"
RUNNER = "CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER"
SHOW_ENV = """\
export LLVM_PROFILE_FILE='/repo/target/nervix-%p-%24m.profraw'
export RUSTFLAGS='-Clink-arg=--ld-path=wild -C instrument-coverage --cfg=coverage'
export CARGO_LLVM_COV=1
export CARGO_LLVM_COV_SHOW_ENV=1
export CARGO_LLVM_COV_TARGET_DIR=/repo/target/native-coverage-build
"""
VERBOSE_VERSION = f"""\
rustc 1.98.1 (48a229cea 2026-09-01)
binary: rustc
commit-hash: 48a229ceaefd4985c50990b14116b6d856af0985
commit-date: 2026-09-01
host: {HOST}
release: 1.98.1
LLVM version: 22.1.8
"""


def toolchain(tools: Path = Path("/toolchain/bin")) -> Toolchain:
    return Toolchain(
        version="rustc 1.98.1 (48a229cea 2026-09-01)",
        release="1.98.1",
        commit="48a229ceaefd4985c50990b14116b6d856af0985",
        host=HOST,
        llvm="22.1.8",
        tools=tools,
    )


def recipe(*dependencies: str, body: bool = False) -> dict[str, object]:
    return {
        "dependencies": [
            {"arguments": [], "recipe": dependency, "star": None} for dependency in dependencies
        ],
        "body": [["cargo test"]] if body else [],
    }


class InventoryTests(unittest.TestCase):
    def test_every_producer_composes_its_check_in_the_justfile(self) -> None:
        dumped = subprocess.run(
            ["just", "--dump", "--dump-format", "json"],
            cwd=REPOSITORY,
            capture_output=True,
            text=True,
            check=True,
        )
        recipes = json.loads(dumped.stdout)["recipes"]
        for producer in native_coverage.PRODUCERS:
            with self.subTest(producer=producer.name):
                native_coverage.validate_composition(producer, recipes)
        # Coverage profiles must be read with the selected compiler's own LLVM tools.
        toolchain = tomllib.loads((REPOSITORY / "rust-toolchain.toml").read_text())["toolchain"]
        self.assertIn("llvm-tools", toolchain["components"])

    def test_the_producers_are_the_native_extra_checks_in_ci_order(self) -> None:
        self.assertEqual(
            [producer.name for producer in native_coverage.PRODUCERS],
            ["test-typed-ratchet", "bench-smoke", "test-primitives", "nspl-completion-walk"],
        )
        for producer in native_coverage.PRODUCERS:
            self.assertEqual(producer.mode, "ordinary")
            self.assertEqual(producer.rerun(), f"just coverage-native-extras {producer.name}")

    def test_a_check_composed_otherwise_than_its_producer_is_refused(self) -> None:
        producer = Producer("check", "ordinary", ("prepare",), "body", ("finish",))
        complete = {
            "check": recipe("prepare", "body", "finish"),
            "prepare": recipe(),
            "body": recipe(body=True),
            "finish": recipe(),
        }
        native_coverage.validate_composition(producer, complete)

        cases: dict[str, tuple[dict[str, object], str]] = {
            "a missing part": (
                complete | {"check": recipe("prepare", "body")},
                "must consist of exactly prepare, body, finish",
            ),
            "a reordered part": (
                complete | {"check": recipe("body", "prepare", "finish")},
                "must consist of exactly",
            ),
            "a body of its own": (
                complete | {"check": recipe("prepare", "body", "finish", body=True)},
                "has a body of its own",
            ),
            "a dependency of the instrumented recipe": (
                complete | {"body": recipe("build-web-console", body=True)},
                "would run inside the instrumentation",
            ),
            "a missing recipe": (
                {name: value for name, value in complete.items() if name != "finish"},
                "no `finish` recipe",
            ),
        }
        for case, (recipes, message) in cases.items():
            with self.subTest(case=case), self.assertRaisesRegex(RunnerError, message):
                native_coverage.validate_composition(producer, recipes)

    def test_a_check_that_is_its_own_instrumented_recipe_has_no_other_part(self) -> None:
        walk = Producer("walk", "ordinary", (), "walk", ())
        native_coverage.validate_composition(walk, {"walk": recipe(body=True)})
        with self.assertRaisesRegex(RunnerError, "would run inside the instrumentation"):
            native_coverage.validate_composition(walk, {"walk": recipe("prepare"), "prepare": {}})
        prepared = Producer("walk", "ordinary", ("prepare",), "walk", ())
        with self.assertRaisesRegex(RunnerError, "has no other recipes"):
            native_coverage.validate_composition(
                prepared, {"walk": recipe(body=True), "prepare": recipe()}
            )

    def test_producers_are_selected_by_name_once_each(self) -> None:
        producers = native_coverage.PRODUCERS
        self.assertEqual(native_coverage.select_producers([], producers), list(producers))
        self.assertEqual(
            native_coverage.select_producers(["nspl-completion-walk", "bench-smoke"], producers),
            [next(producer for producer in producers if producer.name == "nspl-completion-walk"), next(producer for producer in producers if producer.name == "bench-smoke")],
        )
        with self.assertRaisesRegex(RunnerError, "no producer `bench`; the producers are"):
            native_coverage.select_producers(["bench"], producers)
        with self.assertRaisesRegex(RunnerError, "named twice"):
            native_coverage.select_producers(["bench-smoke", "bench-smoke"], producers)

    def test_extra_tests_run_every_producer_through_the_collector_and_publish_its_evidence(
        self,
    ) -> None:
        workflow = (REPOSITORY / ".github/workflows/check.yaml").read_text(encoding="utf-8")
        job = job_section(workflow, "extra-tests")
        self.assertRegex(job, r"tool: [^\n]*\bcargo-llvm-cov\b")
        self.assertIn("run: just test-native-coverage\n", job)
        for producer in native_coverage.PRODUCERS:
            with self.subTest(producer=producer.name):
                self.assertIn(f"run: just coverage-native-extras {producer.name}\n", job)
                self.assertNotIn(f"run: just {producer.name}\n", job)
        upload = step_section(job, "Upload native extra coverage")
        self.assertIn("if: always()", upload)
        self.assertIn("name: coverage-native-extras", upload)
        for artifact in ("completion.json", "lcov.info", "executions.jsonl", "export.log"):
            self.assertIn(f"target/native-coverage/**/{artifact}", upload)
        self.assertNotIn("profraw", upload)


def job_section(workflow: str, name: str) -> str:
    starts = list(re.finditer(r"^  ([a-z][a-z-]*):\n", workflow, re.MULTILINE))
    for index, start in enumerate(starts):
        if start.group(1) == name:
            end = starts[index + 1].start() if index + 1 < len(starts) else len(workflow)
            return workflow[start.start() : end]
    raise AssertionError(f"no job {name}")


def step_section(job: str, name: str) -> str:
    start = job.index(f"- name: {name}\n")
    following = job.find("\n      - name:", start + 1)
    return job[start : following if following != -1 else len(job)]


class InstrumentationTests(unittest.TestCase):
    def test_show_env_output_is_read_in_shell_quoting(self) -> None:
        exported = native_coverage.parse_exports(SHOW_ENV)
        self.assertEqual(exported["CARGO_LLVM_COV"], "1")
        self.assertEqual(
            exported["RUSTFLAGS"],
            "-Clink-arg=--ld-path=wild -C instrument-coverage --cfg=coverage",
        )
        with self.assertRaisesRegex(RunnerError, "unexpected line"):
            native_coverage.parse_exports("RUSTFLAGS=-Cinstrument-coverage\n")

    def test_instrumentation_is_recognized_in_every_flag_spelling(self) -> None:
        self.assertTrue(native_coverage.instruments_coverage(["-C", "instrument-coverage"]))
        self.assertTrue(native_coverage.instruments_coverage(["-Cinstrument-coverage"]))
        self.assertFalse(native_coverage.instruments_coverage(["-C", "target-cpu=native"]))
        self.assertFalse(native_coverage.instruments_coverage(["-C"]))
        encoded = {"CARGO_ENCODED_RUSTFLAGS": "-C\x1finstrument-coverage\x1f", "RUSTFLAGS": ""}
        self.assertEqual(
            native_coverage.compiler_flags(encoded), ["-C", "instrument-coverage"]
        )
        self.assertEqual(native_coverage.runner_variable(HOST), RUNNER)

    def test_the_environment_instruments_the_build_and_keeps_the_compiler_wrapper(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workspace = Workspace(root=Path(directory), target=Path(directory) / "target")
            attempt = Path(directory) / "attempt"
            commands = FakeCommands(workspace.root)
            base = {"RUSTC_WRAPPER": "kache", "PATH": "/usr/bin"}
            instrumented = native_coverage.instrumentation(
                commands, workspace, toolchain(), attempt, base
            )
        environment = instrumented.environment
        self.assertEqual(environment["RUSTC_WRAPPER"], "kache")
        self.assertEqual(environment["CARGO_TARGET_DIR"], str(workspace.build()))
        self.assertEqual(environment["LLVM_PROFILE_FILE"], "/dev/null")
        self.assertEqual(environment[native_coverage.ATTEMPT_VARIABLE], str(attempt))
        self.assertEqual(
            environment[RUNNER], f"{sys.executable} {native_coverage.SCRIPT} exec"
        )
        self.assertIn("instrument-coverage", environment["RUSTFLAGS"])
        self.assertEqual(commands.captured[0].environment["CARGO_TARGET_DIR"], str(workspace.build()))
        self.assertEqual(
            instrumented.describe(workspace)["target_directory"], "target/native-coverage-build"
        )
        self.assertNotIn("CARGO_TARGET_DIR", base)

    def test_an_environment_that_would_lose_the_wrapper_or_the_counters_is_refused(self) -> None:
        cases = {
            "a replaced compiler wrapper": (
                SHOW_ENV + "export RUSTC_WRAPPER=/usr/bin/cargo-llvm-cov\n",
                {},
                "configured compiler wrapper must stay",
            ),
            "no instrumentation": (
                "export RUSTFLAGS='-C target-cpu=native'\n",
                {},
                "did not enable -C instrument-coverage",
            ),
            "a runner of its own": (SHOW_ENV, {RUNNER: "valgrind"}, "is already set"),
        }
        for case, (shown, base, message) in cases.items():
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                workspace = Workspace(root=Path(directory), target=Path(directory) / "target")
                commands = FakeCommands(workspace.root, show_env=shown)
                with self.assertRaisesRegex(RunnerError, message):
                    native_coverage.instrumentation(
                        commands, workspace, toolchain(), Path(directory), base
                    )

    def test_a_failed_show_env_stops_the_collection(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workspace = Workspace(root=Path(directory), target=Path(directory) / "target")
            commands = FakeCommands(workspace.root, show_env_status=1)
            with self.assertRaisesRegex(RunnerError, "show-env exited with status 1"):
                native_coverage.instrumentation(
                    commands, workspace, toolchain(), Path(directory), {}
                )


class ToolchainTests(unittest.TestCase):
    def test_the_verbose_version_names_the_compiler_and_its_llvm_tools(self) -> None:
        parsed = Toolchain.parse(VERBOSE_VERSION, "/home/user/.rustup/toolchains/1.98\n")
        self.assertEqual(parsed, toolchain(Path(f"/home/user/.rustup/toolchains/1.98/lib/rustlib/{HOST}/bin")))
        self.assertEqual(parsed.label(), "rust-1.98.1")
        self.assertEqual(parsed.describe()["llvm"], "22.1.8")
        with self.assertRaisesRegex(RunnerError, "did not report LLVM version"):
            Toolchain.parse(VERBOSE_VERSION.replace("LLVM version: 22.1.8\n", ""), "/sysroot")
        with self.assertRaisesRegex(RunnerError, "printed nothing"):
            Toolchain.parse("", "/sysroot")

    def test_llvm_tools_must_exist_and_match_the_compiler(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            sysroot = Path(directory)
            tools = sysroot / "lib" / "rustlib" / HOST / "bin"
            with self.assertRaisesRegex(RunnerError, "rustup component add llvm-tools"):
                toolchain(tools).tool("llvm-cov")
            tools.mkdir(parents=True)
            for name in ("llvm-profdata", "llvm-cov"):
                (tools / name).write_text("")
            matching = VersionCommands(sysroot, "LLVM version 22.1.8-rust-1.98.1-stable")
            self.assertEqual(native_coverage.load_toolchain(matching).tools, tools)
            other = VersionCommands(sysroot, "LLVM version 21.1.0")
            with self.assertRaisesRegex(RunnerError, "rustc was built with LLVM 22.1.8"):
                native_coverage.load_toolchain(other)


class VersionCommands(Commands):
    def __init__(self, sysroot: Path, version: str) -> None:
        super().__init__(sysroot)
        self.sysroot = sysroot
        self.version = version

    def capture(self, arguments, *, environment=None):  # type: ignore[no-untyped-def]
        if arguments == ["rustc", "-vV"]:
            return Captured(0, VERBOSE_VERSION, "")
        if arguments == ["rustc", "--print", "sysroot"]:
            return Captured(0, f"{self.sysroot}\n", "")
        return Captured(0, f"LLVM (http://llvm.org/):\n  {self.version}\n", "")


class RunIdentityTests(unittest.TestCase):
    def test_ci_attempts_are_named_by_run_and_attempt_and_local_ones_by_time_and_process(
        self,
    ) -> None:
        started = datetime.datetime(2026, 9, 29, 17, 5, 6, 789, tzinfo=datetime.UTC)
        github = native_coverage.run_identity(
            {
                "GITHUB_ACTIONS": "true",
                "GITHUB_RUN_ID": "36628895480",
                "GITHUB_RUN_ATTEMPT": "2",
                "GITHUB_JOB": "extra-tests",
            },
            started,
            42,
        )
        self.assertEqual(github, GitHubRun(run="36628895480", attempt="2", job="extra-tests"))
        self.assertEqual(github.attempt_name(), "github-36628895480-2")
        self.assertEqual(github.describe()["provider"], "github")
        local = native_coverage.run_identity({}, started, 42)
        self.assertEqual(local.attempt_name(), "local-20260929T170506.000789Z-42")
        self.assertEqual(local.describe(), {"provider": "local"})
        with self.assertRaisesRegex(RunnerError, "did not name the run"):
            native_coverage.run_identity({"GITHUB_ACTIONS": "true"}, started, 42)


def note(name: bytes, note_type: int, description: bytes, alignment: int, start: int) -> bytes:
    """One ELF note at `start` within its segment, padded as a segment of `alignment` pads it."""

    header = (
        len(name).to_bytes(4, "little")
        + len(description).to_bytes(4, "little")
        + note_type.to_bytes(4, "little")
    )
    body = bytearray(header + name)
    body += bytes(native_coverage.aligned(start + len(body), alignment) - start - len(body))
    body += description
    body += bytes(native_coverage.aligned(start + len(body), alignment) - start - len(body))
    return bytes(body)


def elf(segments: list[tuple[bytes, int]], *, elf_class: int = 2) -> bytes:
    """A minimal ELF file whose program headers are the given note segments."""

    entry_size = 56
    table = 64
    header = bytearray(64)
    header[0:4] = b"\x7fELF"
    header[4] = elf_class
    header[5] = 1
    header[6] = 1
    header[32:40] = table.to_bytes(8, "little")
    header[54:56] = entry_size.to_bytes(2, "little")
    header[56:58] = len(segments).to_bytes(2, "little")
    entries = bytearray()
    payload = bytearray()
    offset = table + entry_size * len(segments)
    for notes, alignment in segments:
        entry = bytearray(entry_size)
        entry[0:4] = native_coverage.PT_NOTE.to_bytes(4, "little")
        entry[8:16] = offset.to_bytes(8, "little")
        entry[32:40] = len(notes).to_bytes(8, "little")
        entry[48:56] = alignment.to_bytes(8, "little")
        entries += entry
        payload += notes
        offset += len(notes)
    return bytes(header + entries + payload)


BUILD_ID = bytes(range(32))
GNU = b"GNU\x00"


class BuildIdTests(unittest.TestCase):
    def assert_build_id(self, contents: bytes, expected: str) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "executable"
            path.write_bytes(contents)
            self.assertEqual(native_coverage.build_id(path), expected)

    def assert_no_build_id(self, contents: bytes, message: str) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "executable"
            path.write_bytes(contents)
            with self.assertRaisesRegex(RunnerError, message):
                native_coverage.build_id(path)

    def test_the_build_id_is_read_from_a_note_segment(self) -> None:
        segment = note(GNU, native_coverage.NT_GNU_BUILD_ID, BUILD_ID, 4, 0)
        self.assert_build_id(elf([(segment, 4)]), BUILD_ID.hex())

    def test_a_build_id_behind_an_eight_byte_aligned_property_note_is_found(self) -> None:
        # The layout wild links: the property note and the build ID share one 8-aligned segment.
        property_note = note(GNU, 5, bytes(16), 8, 0)
        build_note = note(GNU, native_coverage.NT_GNU_BUILD_ID, BUILD_ID, 8, len(property_note))
        self.assert_build_id(elf([(property_note + build_note, 8)]), BUILD_ID.hex())

    def test_four_byte_aligned_notes_in_an_eight_byte_aligned_segment_are_found(self) -> None:
        abi_note = note(GNU, 1, bytes(20), 4, 0)
        build_note = note(GNU, native_coverage.NT_GNU_BUILD_ID, BUILD_ID, 4, len(abi_note))
        self.assert_build_id(elf([(abi_note + build_note, 8)]), BUILD_ID.hex())

    def test_files_without_a_build_id_are_refused(self) -> None:
        self.assert_no_build_id(b"#!/bin/sh\n", "is not an ELF file")
        self.assert_no_build_id(b"\x7fELF", "is a truncated ELF file")
        segment = note(GNU, native_coverage.NT_GNU_BUILD_ID, BUILD_ID, 4, 0)
        self.assert_no_build_id(elf([(segment, 4)])[:100], "is a truncated ELF file")
        self.assert_no_build_id(elf([(segment, 4)], elf_class=1), "64-bit little-endian")
        abi_only = note(GNU, 1, bytes(16), 4, 0)
        self.assert_no_build_id(elf([(abi_only, 4)]), "carries no GNU build ID")
        other_owner = note(b"LLVM\x00", native_coverage.NT_GNU_BUILD_ID, BUILD_ID, 4, 0)
        self.assert_no_build_id(elf([(other_owner, 4)]), "carries no GNU build ID")

    def test_children_are_located_by_build_id_among_the_build_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            build = Path(directory)
            (build / "debug" / "deps").mkdir(parents=True)
            (build / "debug" / "build" / "crate-1").mkdir(parents=True)
            child = elf([(note(GNU, 3, BUILD_ID, 4, 0), 4)])
            script = elf([(note(GNU, 3, bytes(32), 4, 0), 4)])
            (build / "debug" / "deps" / "child-123").write_bytes(child)
            (build / "debug" / "deps" / "libcrate.rlib").write_bytes(b"!<arch>\n")
            (build / "debug" / "deps" / "broken").write_bytes(b"\x7fELF")
            (build / "debug" / "build" / "crate-1" / "build-script-build").write_bytes(script)
            located = native_coverage.locate(build, {BUILD_ID.hex(), bytes(32).hex()})
        self.assertEqual(located, {BUILD_ID.hex(): build / "debug" / "deps" / "child-123"})
        self.assertEqual(native_coverage.locate(Path(directory) / "missing", {"x"}), {})

    def test_compiler_loaded_macros_in_nested_cargo_build_outputs_are_retained(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            build = Path(directory)
            output = build / "typed-ratchet/driver/debug/build/macro/hash/out/libmacro.so"
            output.parent.mkdir(parents=True)
            output.write_bytes(elf([(note(GNU, 3, BUILD_ID, 4, 0), 4)]))
            self.assertEqual(native_coverage.locate(build, {BUILD_ID.hex()}), {BUILD_ID.hex(): output})


def execution(path: Path, identifier: str, *arguments: str) -> Execution:
    return Execution(executable=path, arguments=tuple(arguments), build_id=identifier)


class SelectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.walk = Path(self.directory.name) / "walk-1"
        self.walk.write_bytes(b"")
        self.child = Path(self.directory.name) / "child-2"

    def select(
        self,
        executions: list[Execution],
        profiles: Mapping[str, tuple[str, ...]],
        current: str = "walk-id",
        located: Mapping[str, Path] | None = None,
    ) -> Selection:
        return native_coverage.select(
            executions,
            {Path(name): identifiers for name, identifiers in profiles.items()},
            lambda path: current,
            lambda identifiers: {
                identifier: path
                for identifier, path in (located or {}).items()
                if identifier in identifiers
            },
        )

    def test_profiles_are_matched_to_executions_and_to_the_children_they_started(self) -> None:
        selection = self.select(
            [execution(self.walk, "walk-id", "--list"), execution(self.walk, "walk-id")],
            {
                "11-walk_0.profraw": ("walk-id",),
                "12-walk_0.profraw": ("walk-id",),
                "13-child_0.profraw": ("child-id",),
            },
            located={"child-id": self.child},
        )
        self.assertEqual(selection.executed, (Selected(self.walk, "walk-id"),))
        self.assertEqual(selection.children, (Selected(self.child, "child-id"),))
        self.assertEqual(selection.objects(), [self.walk, self.child])
        workspace = Workspace(root=Path(self.directory.name), target=Path(self.directory.name))
        self.assertEqual(
            selection.describe(workspace),
            {
                "executions": [
                    {"executable": "walk-1", "arguments": ["--list"], "build_id": "walk-id"},
                    {"executable": "walk-1", "arguments": [], "build_id": "walk-id"},
                ],
                "children": [{"executable": "child-2", "build_id": "child-id"}],
            },
        )
        self.assertEqual(
            selection.describe_profiles(),
            [
                {"file": "11-walk_0.profraw", "binary_ids": ["walk-id"]},
                {"file": "12-walk_0.profraw", "binary_ids": ["walk-id"]},
                {"file": "13-child_0.profraw", "binary_ids": ["child-id"]},
            ],
        )

    def test_every_gap_between_executions_and_profiles_fails(self) -> None:
        cases: dict[str, tuple[Callable[[], Selection], str]] = {
            "no execution": (
                lambda: self.select([], {"1.profraw": ("walk-id",)}),
                "ran no executable through Cargo",
            ),
            "no profile": (
                lambda: self.select([execution(self.walk, "walk-id")], {}),
                "no executable wrote a profile",
            ),
            "a missing executable": (
                lambda: self.select(
                    [execution(self.child, "walk-id")], {"1.profraw": ("walk-id",)}
                ),
                "ran but is no longer there to export",
            ),
            "a rebuilt executable": (
                lambda: self.select(
                    [execution(self.walk, "walk-id")], {"1.profraw": ("walk-id",)}, "other-id"
                ),
                "was rebuilt after it ran",
            ),
            "an executable rebuilt between runs": (
                lambda: self.select(
                    [execution(self.walk, "walk-id"), execution(self.walk, "other-id")],
                    {"1.profraw": ("walk-id",)},
                ),
                "rebuilt between two of its runs",
            ),
            "an execution without a profile": (
                lambda: self.select(
                    [execution(self.walk, "walk-id")], {"1.profraw": ("child-id",)}
                ),
                "ran but wrote no profile",
            ),
            "a profile of an executable that is gone": (
                lambda: self.select(
                    [execution(self.walk, "walk-id")],
                    {"1.profraw": ("walk-id",), "2.profraw": ("gone-id",)},
                ),
                "not retained for export: gone-id",
            ),
        }
        for case, (selection, message) in cases.items():
            with self.subTest(case=case), self.assertRaisesRegex(RunnerError, message):
                selection()

    def test_executions_are_read_back_as_the_runner_wrote_them(self) -> None:
        log = Path(self.directory.name) / native_coverage.EXECUTIONS
        self.assertEqual(native_coverage.read_executions(log), [])
        log.write_text(
            json.dumps({"executable": str(self.walk), "arguments": ["--test"], "build_id": "a"})
            + "\n"
        )
        self.assertEqual(
            native_coverage.read_executions(log), [execution(self.walk, "a", "--test")]
        )
        log.write_text('{"executable": "/walk"}\n')
        with self.assertRaisesRegex(RunnerError, "line 1 is not an execution"):
            native_coverage.read_executions(log)

    def test_binary_ids_are_read_from_the_profile_summary(self) -> None:
        summary = (
            "Instrumentation level: Front-end\nTotal functions: 3\nBinary IDs: \n"
            "d8b4dadfe3c9e772b97b1c31473f26fb\n"
        )
        tools = Path(self.directory.name) / "tools"
        tools.mkdir()
        (tools / "llvm-profdata").write_text("")
        profile = Path(self.directory.name) / "1.profraw"
        commands = ProfileCommands(Captured(0, summary, ""))
        self.assertEqual(
            native_coverage.profile_binary_ids(commands, toolchain(tools), profile),
            ("d8b4dadfe3c9e772b97b1c31473f26fb",),
        )
        unnamed = ProfileCommands(Captured(0, "Total functions: 3\n", ""))
        with self.assertRaisesRegex(RunnerError, "names no binary ID"):
            native_coverage.profile_binary_ids(unnamed, toolchain(tools), profile)
        broken = ProfileCommands(Captured(1, "", "error: 1.profraw: malformed profile data\n"))
        with self.assertRaisesRegex(RunnerError, "not a readable raw profile: error"):
            native_coverage.profile_binary_ids(broken, toolchain(tools), profile)


class ProfileCommands(Commands):
    def __init__(self, answer: Captured) -> None:
        super().__init__(Path("/"))
        self.answer = answer

    def capture(self, arguments, *, environment=None):  # type: ignore[no-untyped-def]
        return self.answer


RAW_REPORT = """\
SF:{root}/crates/alpha/src/lib.rs
FN:1,alpha
FNDA:1,alpha
DA:1,1
DA:2,0
DA:3,4
LF:3
LH:2
end_of_record
SF:{root}/crates/alpha/tests/walk.rs
DA:1,1
end_of_record
SF:{root}/benches/relay.rs
DA:1,1
end_of_record
SF:/home/user/.cargo/registry/src/index/serde-1.0.0/src/lib.rs
DA:1,9
end_of_record
SF:/rustc/48a229ceaefd4985c50990b14116b6d856af0985/library/core/src/option.rs
DA:1,9
end_of_record
SF:{root}/target/native-coverage-build/debug/build/alpha-1/out/generated.rs
DA:1,1
end_of_record
SF:{root}/src/main.rs
DA:7,0
DA:8,2
end_of_record
"""


class ReportTests(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name).resolve()
        self.policy = SourcePolicy(root=self.root, generated=(self.root / "target",))
        self.packages = Packages.from_metadata(
            {
                "packages": [
                    {"name": "server", "manifest_path": str(self.root / "Cargo.toml")},
                    {
                        "name": "alpha",
                        "manifest_path": str(self.root / "crates" / "alpha" / "Cargo.toml"),
                    },
                ]
            }
        )

    def test_sources_are_classified_by_where_they_live(self) -> None:
        cases = {
            "crates/alpha/src/lib.rs": Classification.INCLUDED,
            "src/tests.rs": Classification.INCLUDED,
            "crates/alpha/tests/walk.rs": Classification.HARNESS,
            "crates/alpha/src/tests/unit.rs": Classification.HARNESS,
            "examples/demo.rs": Classification.HARNESS,
            "benches/relay.rs": Classification.HARNESS,
            "target/debug/build/alpha-1/out/generated.rs": Classification.GENERATED,
        }
        for relative, expected in cases.items():
            with self.subTest(path=relative):
                self.assertEqual(self.policy.classify(str(self.root / relative)), expected)
                self.assertEqual(self.policy.classify(relative), expected)
        self.assertEqual(
            self.policy.classify("/rustc/48a229ce/library/core/src/option.rs"),
            Classification.DEPENDENCY,
        )
        self.assertEqual(
            self.policy.classify(str(self.root.parent / "sibling" / "src" / "lib.rs")),
            Classification.DEPENDENCY,
        )

    def test_the_report_keeps_repository_sources_and_counts_their_lines_by_package(self) -> None:
        raw = self.root / "unfiltered.lcov"
        raw.write_text(RAW_REPORT.format(root=self.root), encoding="utf-8")
        report = self.root / "lcov.info"
        sources = native_coverage.filter_report(raw, report, self.policy, self.packages)
        kept = [line[3:].strip() for line in report.read_text().splitlines() if line.startswith("SF:")]
        self.assertEqual(
            kept, [str(self.root / "crates/alpha/src/lib.rs"), str(self.root / "src/main.rs")]
        )
        self.assertIn("FNDA:1,alpha\n", report.read_text())
        described = sources.describe(self.policy)
        self.assertEqual(
            (described["files"], described["executable"], described["covered"]), (2, 5, 3)
        )
        self.assertEqual(
            described["excluded_files"], {"dependency": 2, "generated": 1, "harness": 2}
        )
        self.assertEqual(
            described["packages"],
            {
                "alpha": {"files": 1, "executable": 3, "covered": 2},
                "server": {"files": 1, "executable": 2, "covered": 1},
            },
        )
        self.assertEqual(described["root"], str(self.root))
        self.assertEqual(set(described["policy"]), {str(value) for value in Classification})
        self.assertFalse((self.root / ".lcov.info.tmp").exists())

    def test_malformed_reports_are_refused(self) -> None:
        cases = {
            "an unterminated record": "SF:/a.rs\nDA:1,1\n",
            "a record inside a record": "SF:/a.rs\nSF:/b.rs\nend_of_record\n",
            "a line outside a record": "DA:1,1\n",
            "a malformed line count": "SF:/a.rs\nDA:1,many\nend_of_record\n",
        }
        for case, text in cases.items():
            with self.subTest(case=case), self.assertRaises(RunnerError):
                list(native_coverage.lcov_records(text.splitlines(keepends=True)))
        records = list(native_coverage.lcov_records(["TN:\n", "SF:/a.rs\n", "end_of_record\n"]))
        self.assertEqual(records[0].source, "/a.rs")

    def test_every_source_is_attributed_to_its_most_specific_package(self) -> None:
        self.assertEqual(self.packages.owner(str(self.root / "crates/alpha/src/lib.rs")), "alpha")
        self.assertEqual(self.packages.owner(str(self.root / "crates/beta/src/lib.rs")), "server")
        with self.assertRaisesRegex(RunnerError, "belongs to no workspace package"):
            self.packages.owner("/elsewhere/lib.rs")
        with self.assertRaisesRegex(RunnerError, "listed no packages"):
            Packages.from_metadata({})

    def test_tool_diagnostics_are_bounded_in_the_export_log(self) -> None:
        log = native_coverage.ExportLog(self.root / "export.log")
        log.add(["llvm-cov", "export"], Captured(0, "", "x" * (native_coverage.LOG_LIMIT + 10)))
        text = log.path.read_text()
        self.assertTrue(text.startswith("$ llvm-cov export\nexit status 0\n"))
        self.assertIn("[10 more characters omitted]", text)
        self.assertLess(len(text), native_coverage.LOG_LIMIT + 200)


class ExportCommands(Commands):
    """Plays llvm-profdata and llvm-cov: every profile names `identifier`, and the export writes
    `report` and prints `warnings`."""

    def __init__(
        self,
        root: Path,
        identifier: str,
        report: str,
        *,
        warnings: str = "",
        merge_status: int = 0,
        export_status: int = 0,
    ) -> None:
        super().__init__(root)
        self.identifier = identifier
        self.report = report
        self.warnings = warnings
        self.merge_status = merge_status
        self.export_status = export_status
        self.exported: list[str] = []

    def capture(self, arguments, *, environment=None):  # type: ignore[no-untyped-def]
        if arguments[1:3] == ["show", "--binary-ids"]:
            return Captured(0, f"Total functions: 2\nBinary IDs: \n{self.identifier}\n", "")
        if arguments[1] == "merge":
            return Captured(self.merge_status, "", "error: malformed" if self.merge_status else "")
        raise AssertionError(f"unexpected command {arguments}")

    def export(self, arguments, output):  # type: ignore[no-untyped-def]
        self.exported = list(arguments)
        output.write_text(self.report, encoding="utf-8")
        return Captured(self.export_status, "", self.warnings)


class ExportTests(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name).resolve()
        self.workspace = Workspace(root=self.root, target=self.root / "target")
        tools = self.root / "toolchain"
        tools.mkdir()
        for name in ("llvm-profdata", "llvm-cov"):
            (tools / name).write_text("")
        self.toolchain = toolchain(tools)
        self.packages = Packages.from_metadata(
            {"packages": [{"name": "server", "manifest_path": str(self.root / "Cargo.toml")}]}
        )
        deps = self.workspace.build() / "debug" / "deps"
        deps.mkdir(parents=True)
        self.walk = deps / "walk-1"
        self.walk.write_bytes(elf([(note(GNU, 3, BUILD_ID, 4, 0), 4)]))
        self.attempt = self.workspace.target / "native-coverage" / "attempt"
        (self.attempt / "profiles").mkdir(parents=True)
        (self.attempt / "profiles" / "7-9_0.profraw").write_bytes(b"counters")
        entry = {"executable": str(self.walk), "arguments": ["--test"], "build_id": BUILD_ID.hex()}
        (self.attempt / native_coverage.EXECUTIONS).write_text(json.dumps(entry) + "\n")
        self.report = RAW_REPORT.format(root=self.root)

    def export(self, commands: ExportCommands) -> Exported:
        return native_coverage.export(
            commands, self.workspace, self.toolchain, self.packages, self.attempt
        )

    def test_the_export_reads_exactly_the_executables_that_ran_and_keeps_their_warnings(
        self,
    ) -> None:
        commands = ExportCommands(
            self.root,
            BUILD_ID.hex(),
            self.report,
            warnings="warning: 1 functions have mismatched data\n",
        )
        exported = self.export(commands)
        self.assertEqual(exported.warnings, ("warning: 1 functions have mismatched data",))
        self.assertEqual(exported.selection.objects(), [self.walk])
        self.assertEqual(commands.exported[-1], str(self.walk))
        self.assertIn(f"-instr-profile={self.attempt / 'merged.profdata'}", commands.exported)
        self.assertEqual(exported.sources.total.files, 2)
        self.assertTrue((self.attempt / "lcov.info").is_file())
        self.assertFalse((self.attempt / native_coverage.UNFILTERED_REPORT).exists())
        log = (self.attempt / "export.log").read_text()
        self.assertIn(" merge -sparse --failure-mode=any ", log)
        self.assertIn("warning: 1 functions have mismatched data", log)

    def test_a_failed_merge_or_export_or_an_empty_report_fails_the_export(self) -> None:
        cases = {
            "a failed merge": (
                ExportCommands(self.root, BUILD_ID.hex(), self.report, merge_status=1),
                "llvm-profdata could not merge the profiles",
            ),
            "a failed export": (
                ExportCommands(self.root, BUILD_ID.hex(), self.report, export_status=1),
                "llvm-cov could not export the report",
            ),
            "a report without repository sources": (
                ExportCommands(self.root, BUILD_ID.hex(), "SF:/elsewhere/lib.rs\nDA:1,1\nend_of_record\n"),
                "holds no repository source",
            ),
            "only a profile of another executable": (
                ExportCommands(self.root, bytes(32).hex(), self.report),
                "ran but wrote no profile",
            ),
        }
        for case, (commands, message) in cases.items():
            with self.subTest(case=case), self.assertRaisesRegex(RunnerError, message):
                self.export(commands)


@dataclass(frozen=True)
class Streamed:
    arguments: list[str]
    environment: dict[str, str]


class FakeCommands(Commands):
    """Answers show-env and plays each recipe's outcome, recording what was run and how."""

    def __init__(
        self,
        root: Path,
        outcomes: Mapping[str, int | BaseException | Callable[[], int]] | None = None,
        *,
        show_env: str = SHOW_ENV,
        show_env_status: int = 0,
    ) -> None:
        super().__init__(root)
        self.outcomes = dict(outcomes or {})
        self.show_env = show_env
        self.show_env_status = show_env_status
        self.captured: list[Streamed] = []
        self.streamed: list[Streamed] = []

    def capture(self, arguments, *, environment=None):  # type: ignore[no-untyped-def]
        self.captured.append(Streamed(list(arguments), dict(environment or {})))
        if list(arguments[:3]) == ["cargo", "llvm-cov", "show-env"]:
            return Captured(self.show_env_status, self.show_env, "cargo-llvm-cov failed")
        raise AssertionError(f"unexpected command {arguments}")

    def stream(self, arguments, *, environment):  # type: ignore[no-untyped-def]
        self.streamed.append(Streamed(list(arguments), dict(environment)))
        outcome = self.outcomes.get(arguments[-1], 0)
        if isinstance(outcome, BaseException):
            raise outcome
        if callable(outcome):
            return outcome()
        return outcome


PRODUCER = Producer("check", "ordinary", ("prepare",), "instrumented", ("finish",))


class CollectTests(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name).resolve()
        self.workspace = Workspace(root=self.root, target=self.root / "target")
        self.moments = iter(
            datetime.datetime(2026, 9, 29, 12, 0, second, tzinfo=datetime.UTC)
            for second in range(60)
        )
        self.context = Context(
            workspace=self.workspace,
            toolchain=toolchain(),
            revision=Revision(commit="c32a5d2d", modified=False),
            run=GitHubRun(run="100", attempt="1", job="extra-tests"),
            packages=Packages(directories=()),
            clock=lambda: next(self.moments),
            environment={"RUSTC_WRAPPER": "kache"},
        )

    def exported(self) -> Exported:
        walk = self.workspace.build() / "debug" / "deps" / "walk-1"
        sources = Sources()
        sources.total.files = 1
        sources.total.executable = 10
        sources.total.covered = 7
        return Exported(
            selection=Selection(
                executions=(execution(walk, "walk-id"),),
                executed=(Selected(walk, "walk-id"),),
                children=(),
                profiles={Path("1-2_0.profraw"): ("walk-id",)},
            ),
            sources=sources,
            policy=SourcePolicy(root=self.root, generated=()),
            warnings=("warning: 1 functions have mismatched data",),
        )

    def collect(
        self,
        commands: FakeCommands,
        exported: Exported | BaseException | None = None,
        producer: Producer = PRODUCER,
    ) -> native_coverage.Collected:
        with mock.patch.object(
            native_coverage,
            "export",
            side_effect=[exported if exported is not None else self.exported()],
        ):
            return native_coverage.collect(commands, self.context, producer)

    def record(self, collected: native_coverage.Collected) -> dict[str, object]:
        return json.loads(collected.record.path.read_text())

    def test_a_passing_check_is_collected_complete(self) -> None:
        def running_record() -> int:
            record = json.loads(next(self.root.rglob("completion.json")).read_text())
            self.assertEqual(record["verdict"], "running")
            self.assertEqual(record["instrumentation"]["build_time_profiles"], "/dev/null")
            return 0

        commands = FakeCommands(self.root, {"instrumented": running_record})
        collected = self.collect(commands)
        self.assertEqual(collected.status, 0)
        self.assertEqual(
            collected.attempt,
            self.root / "target/native-coverage/check/ordinary/rust-1.98.1/github-100-1",
        )
        record = self.record(collected)
        self.assertEqual(record["verdict"], "complete")
        self.assertNotIn("failure", record)
        self.assertEqual(record["producer"], "check")
        self.assertEqual(record["mode"], "ordinary")
        self.assertEqual(record["rerun"], "just coverage-native-extras check")
        self.assertEqual(record["revision"], {"commit": "c32a5d2d", "modified": False})
        self.assertEqual(
            record["run"],
            {"provider": "github", "run": "100", "attempt": "1", "job": "extra-tests"},
        )
        self.assertEqual(record["attempt"], "github-100-1")
        self.assertEqual(record["toolchain"]["rustc"], "rustc 1.98.1 (48a229cea 2026-09-01)")
        self.assertEqual(
            record["recipes"],
            {"prepare": ["prepare"], "instrumented": "instrumented", "finish": ["finish"]},
        )
        self.assertEqual(
            record["selection"]["executions"][0]["executable"],
            "target/native-coverage-build/debug/deps/walk-1",
        )
        self.assertEqual(record["profiles"], [{"file": "1-2_0.profraw", "binary_ids": ["walk-id"]}])
        self.assertEqual(
            record["export_warnings"], ["warning: 1 functions have mismatched data"]
        )
        self.assertEqual(record["sources"]["covered"], 7)
        self.assertEqual(record["report"], "lcov.info")
        self.assertEqual(record["started_at"], "2026-09-29T12:00:00Z")
        self.assertEqual(record["finished_at"], "2026-09-29T12:00:01Z")
        self.assertTrue((collected.attempt / "profiles").is_dir())

        recipes = [streamed.arguments for streamed in commands.streamed]
        self.assertEqual(
            recipes, [["just", "prepare"], ["just", "instrumented"], ["just", "finish"]]
        )
        prepare, instrumented, finish = (streamed.environment for streamed in commands.streamed)
        for ordinary in (prepare, finish):
            self.assertEqual(ordinary, {"RUSTC_WRAPPER": "kache"})
        self.assertEqual(instrumented["RUSTC_WRAPPER"], "kache")
        self.assertEqual(instrumented["LLVM_PROFILE_FILE"], "/dev/null")
        self.assertEqual(instrumented[native_coverage.ATTEMPT_VARIABLE], str(collected.attempt))
        self.assertIn(RUNNER, instrumented)

    def test_a_failing_part_fails_the_collection_at_its_stage(self) -> None:
        cases = {
            "prepare": ({"prepare": 2}, 2, "prepare", "`just prepare` exited with status 2"),
            "run": ({"instrumented": 101}, 101, "run", "`just instrumented` exited with status 101"),
            "finish": ({"finish": 3}, 3, "finish", "`just finish` exited with status 3"),
        }
        for case, (outcomes, status, stage, detail) in cases.items():
            with self.subTest(case=case):
                self.setUp()
                collected = self.collect(FakeCommands(self.root, outcomes))
                record = self.record(collected)
                self.assertEqual(collected.status, status)
                self.assertEqual(record["verdict"], "failed")
                self.assertEqual(record["failure"], {"stage": stage, "detail": detail})
                self.assertIn("finished_at", record)
        self.assertIn("sources", record)

    def test_preparation_installs_the_selected_compiler_and_llvm_tools_before_resolution(self) -> None:
        producer = Producer(
            "check", "ordinary", ("prepare",), "instrumented", ("finish",), "nightly-2026-09-17"
        )
        sysroot = self.root / "installed-toolchain"
        tools = sysroot / "lib" / "rustlib" / HOST / "bin"

        def install() -> int:
            record = json.loads(next(self.root.rglob("completion.json")).read_text())
            self.assertEqual(record["verdict"], "running")
            self.assertEqual(record["toolchain"], {"requested": producer.toolchain})
            tools.mkdir(parents=True)
            for name in ("llvm-profdata", "llvm-cov"):
                (tools / name).write_text("")
            return 0

        commands = FakeCommands(self.root, {"prepare": install})
        capture = commands.capture
        versions = VersionCommands(sysroot, "LLVM version 22.1.8")

        def installed_capture(arguments, *, environment=None):
            if arguments[0] == "rustc" or str(arguments[0]).startswith(str(tools)):
                self.assertTrue(tools.is_dir(), "the selected compiler is installed by preparation")
                if arguments[0] == "rustc":
                    self.assertEqual(environment["RUSTUP_TOOLCHAIN"], producer.toolchain)
                return versions.capture(arguments, environment=environment)
            return capture(arguments, environment=environment)

        with mock.patch.object(commands, "capture", side_effect=installed_capture):
            collected = self.collect(commands, producer=producer)
        self.assertEqual(collected.status, 0, self.record(collected).get("failure"))
        self.assertEqual(self.record(collected)["toolchain"], toolchain(tools).describe())
        self.assertEqual(collected.attempt.parent.name, "rust-nightly-2026-09-17")
        prepare, instrumented, finish = (streamed.environment for streamed in commands.streamed)
        self.assertEqual(prepare, {"RUSTC_WRAPPER": "kache"})
        self.assertEqual(instrumented["RUSTUP_TOOLCHAIN"], producer.toolchain)
        self.assertEqual(finish, {"RUSTC_WRAPPER": "kache", "RUSTUP_TOOLCHAIN": producer.toolchain})

    def test_failed_or_interrupted_toolchain_preparation_keeps_its_completion_record(self) -> None:
        producer = Producer(
            "check", "ordinary", ("prepare",), "instrumented", (), "nightly-2026-09-17"
        )
        cases = ((2, 2, "failed"), (Interrupted(signal.SIGTERM), 143, "interrupted"))
        for outcome, status, verdict in cases:
            with self.subTest(verdict=verdict):
                self.setUp()
                with mock.patch.object(native_coverage, "load_toolchain") as load:
                    collected = self.collect(
                        FakeCommands(self.root, {"prepare": outcome}), producer=producer
                    )
                load.assert_not_called()
                record = self.record(collected)
                self.assertEqual(collected.status, status)
                self.assertEqual(record["verdict"], verdict)
                self.assertEqual(record["failure"]["stage"], "prepare")
                self.assertEqual(record["toolchain"], {"requested": producer.toolchain})

    def test_unavailable_selected_compiler_fails_preparation_without_instrumentation(self) -> None:
        producer = Producer(
            "check", "ordinary", (), "instrumented", (), "nightly-2026-09-17"
        )
        commands = FakeCommands(self.root)
        with mock.patch.object(
            native_coverage, "load_toolchain", side_effect=RunnerError("compiler unavailable")
        ):
            collected = self.collect(commands, producer=producer)
        record = self.record(collected)
        self.assertEqual(collected.status, 1)
        self.assertEqual(record["failure"], {"stage": "prepare", "detail": "compiler unavailable"})
        self.assertEqual(commands.streamed, [])

    def test_a_failed_export_fails_the_collection(self) -> None:
        collected = self.collect(
            FakeCommands(self.root), RunnerError("the recipe ran no executable through Cargo")
        )
        record = self.record(collected)
        self.assertEqual(collected.status, 1)
        self.assertEqual(
            record["failure"],
            {"stage": "export", "detail": "the recipe ran no executable through Cargo"},
        )
        self.assertNotIn("sources", record)

    def test_a_failed_instrumentation_fails_the_collection(self) -> None:
        collected = self.collect(FakeCommands(self.root, show_env_status=2))
        record = self.record(collected)
        self.assertEqual(collected.status, 1)
        self.assertEqual(record["failure"]["stage"], "instrument")

    def test_an_interrupted_collection_is_never_complete(self) -> None:
        cases = {
            "SIGINT": (KeyboardInterrupt(), 130, "interrupted by SIGINT"),
            "SIGTERM": (Interrupted(signal.SIGTERM), 143, "interrupted by SIGTERM"),
        }
        for case, (interruption, status, detail) in cases.items():
            with self.subTest(case=case):
                self.setUp()
                collected = self.collect(FakeCommands(self.root, {"instrumented": interruption}))
                record = self.record(collected)
                self.assertEqual(collected.status, status)
                self.assertEqual(record["verdict"], "interrupted")
                self.assertEqual(record["failure"], {"stage": "run", "detail": detail})

    def test_an_attempt_directory_is_never_reused(self) -> None:
        self.collect(FakeCommands(self.root))
        with self.assertRaisesRegex(RunnerError, "never reuses an attempt's directory"):
            self.collect(FakeCommands(self.root))

    def test_one_collection_at_a_time_holds_the_instrumented_build(self) -> None:
        with self.workspace.build_lock():
            collected = self.collect(FakeCommands(self.root))
        self.assertEqual(collected.status, 1)
        self.assertIn("another native coverage run is using", self.record(collected)["failure"]["detail"])

    def test_the_summary_names_the_verdict_report_record_and_rerun(self) -> None:
        collected = self.collect(FakeCommands(self.root))
        (collected.attempt / "lcov.info").write_text("")
        lines = native_coverage.summary(self.workspace, PRODUCER, collected)
        self.assertEqual(lines[0], "native coverage: check (ordinary) complete")
        self.assertEqual(lines[1], "  7 of 10 lines covered in 1 repository files")
        self.assertIn("lcov.info", lines[2])
        self.assertIn("completion.json", lines[3])
        self.assertEqual(lines[4], "  rerun:  just coverage-native-extras check")
        with tempfile.NamedTemporaryFile("r", suffix=".md") as step_summary:
            native_coverage.publish_step_summary({"GITHUB_STEP_SUMMARY": step_summary.name}, lines)
            self.assertIn("native coverage: check (ordinary) complete", step_summary.read())
        native_coverage.publish_step_summary({}, lines)


class CommandLineTests(unittest.TestCase):
    def test_an_unknown_producer_stops_before_anything_runs(self) -> None:
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            status = native_coverage.main(
                ["--target-dir", "/nonexistent/target", "run", "bench"],
                commands_for=lambda root: FakeCommands(root),
            )
        self.assertEqual(status, 1)
        self.assertIn("native coverage: no producer `bench`", stderr.getvalue())

    def test_the_runner_refuses_to_run_outside_a_collection(self) -> None:
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            status = native_coverage.main(["exec", "/usr/bin/env"], environment={})
        self.assertEqual(status, 2)
        self.assertIn("runs only as Cargo's runner", stderr.getvalue())

    def test_the_runner_records_the_executable_and_collects_its_profiles(self) -> None:
        executable = Path("/usr/bin/env").resolve()
        try:
            identifier = native_coverage.build_id(executable)
        except (RunnerError, OSError) as error:
            self.skipTest(f"{executable} has no build ID to record: {error}")
        with tempfile.TemporaryDirectory() as directory:
            attempt = Path(directory)
            environment = {"PATH": os.environ.get("PATH", ""), native_coverage.ATTEMPT_VARIABLE: directory}
            completed = subprocess.run(
                [sys.executable, str(native_coverage.SCRIPT), "exec", str(executable), "-0"],
                env=environment,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            variables = dict(
                entry.split("=", 1) for entry in completed.stdout.split("\0") if "=" in entry
            )
            self.assertEqual(variables["LLVM_PROFILE_FILE"], f"{directory}/profiles/%p-%m.profraw")
            executions = native_coverage.read_executions(attempt / native_coverage.EXECUTIONS)
        self.assertEqual(executions, [execution(executable, identifier, "-0")])

    def test_the_runner_refuses_an_executable_it_cannot_identify(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            script = Path(directory) / "script"
            script.write_text("#!/bin/sh\nexit 0\n")
            stderr = io.StringIO()
            with redirect_stderr(stderr):
                status = native_coverage.main(
                    ["exec", str(script)],
                    environment={native_coverage.ATTEMPT_VARIABLE: directory},
                )
        self.assertEqual(status, 1)
        self.assertIn("is not an ELF file", stderr.getvalue())


TOOLCHAIN_TESTS = "NERVIX_NATIVE_COVERAGE_TOOLCHAIN_TESTS"

FIXTURE = {
    "Cargo.toml": """
        [package]
        name = "coverage-fixture"
        version = "0.0.0"
        edition = "2021"
        publish = false

        [[bin]]
        name = "coverage-fixture-child"
        path = "src/bin/child.rs"

        [[test]]
        name = "walk"
        path = "tests/walk.rs"
        harness = false

        [workspace]
    """,
    "build.rs": """
        fn main() {
            println!("cargo:rerun-if-changed=build.rs");
        }
    """,
    "src/lib.rs": """
        pub fn walked(value: u32) -> u32 {
            if value > 1 {
                value * 2
            } else {
                value + 1
            }
        }

        pub fn spawned() -> &'static str {
            "spawned"
        }

        pub fn never_called() -> u32 {
            7
        }

        #[inline]
        pub fn inlined(value: u32) -> u32 {
            value + 3
        }
    """,
    "src/bin/child.rs": """
        fn main() {
            println!("{} {}", coverage_fixture::spawned(), coverage_fixture::inlined(1));
        }
    """,
    "tests/walk.rs": """
        extern "C" {
            fn _exit(status: i32) -> !;
        }

        fn main() {
            assert_eq!(coverage_fixture::walked(3), 6);
            if std::env::var_os("COVERAGE_FIXTURE_FAIL").is_some() {
                std::process::exit(3);
            }
            let child = env!("CARGO_BIN_EXE_coverage-fixture-child");
            let status = std::process::Command::new(child).status().expect("the child runs");
            assert!(status.success());
            if std::env::var_os("COVERAGE_FIXTURE_SKIP_PROFILE").is_some() {
                unsafe { _exit(0) }
            }
        }
    """,
    "justfile": """
        walk:
            cargo test --test walk

        checked-walk: prepare walk finish

        prepare:
            env > prepare.env

        finish:
            env > finish.env

        failing-walk:
            COVERAGE_FIXTURE_FAIL=1 cargo test --test walk

        walk-without-profile:
            COVERAGE_FIXTURE_SKIP_PROFILE=1 cargo test --test walk

        walk-then-remove:
            cargo test --test walk
            rm "$CARGO_TARGET_DIR"/debug/deps/walk-*

        nothing:
            true

        interrupted:
            kill -INT "$COVERAGE_FIXTURE_COLLECTOR"
            sleep 60

        terminated:
            kill -TERM "$COVERAGE_FIXTURE_COLLECTOR"
            sleep 60
    """,
}


def fixture_producer(name: str, instrumented: str | None = None) -> Producer:
    return Producer(name, "ordinary", (), instrumented or name, ())


FIXTURE_PRODUCERS = (
    fixture_producer("walk"),
    Producer("checked-walk", "ordinary", ("prepare",), "walk", ("finish",)),
    fixture_producer("failing-walk"),
    fixture_producer("walk-without-profile"),
    fixture_producer("walk-then-remove"),
    fixture_producer("nothing"),
    fixture_producer("interrupted"),
    fixture_producer("terminated"),
)


def toolchain_gap() -> str | None:
    """Why the real toolchain cannot collect the fixture here, or None when it can."""

    for tool in ("just", "cargo", "rustc", "git"):
        if shutil.which(tool) is None:
            return f"{tool} is not installed"
    version = subprocess.run(
        ["cargo", "llvm-cov", "--version"], cwd=REPOSITORY, capture_output=True, text=True
    )
    if version.returncode != 0:
        return "cargo-llvm-cov is not installed"
    commands = Commands(REPOSITORY)
    try:
        native_coverage.load_toolchain(commands)
    except RunnerError as error:
        return str(error)
    return None


@dataclass(frozen=True)
class Collection:
    status: int
    attempt: Path
    record: dict[str, object]


def files(directory: Path) -> dict[str, int]:
    """Every file below a directory with its size, to tell whether anything removed or changed one."""

    found: dict[str, int] = {}
    for path in sorted(directory.rglob("*")):
        if path.is_file():
            found[str(path.relative_to(directory))] = path.stat().st_size
    return found


class FixtureTests(unittest.TestCase):
    """Collect a fixture crate end to end with the real toolchain, its runner and LLVM tools."""

    fixture: Path
    target: Path

    @classmethod
    def setUpClass(cls) -> None:
        gap = toolchain_gap()
        if gap is not None:
            if os.environ.get(TOOLCHAIN_TESTS) == "required":
                raise AssertionError(f"{TOOLCHAIN_TESTS}=required, but {gap}")
            raise unittest.SkipTest(gap)
        directory = tempfile.TemporaryDirectory()
        cls.addClassCleanup(directory.cleanup)
        cls.fixture = Path(directory.name).resolve()
        cls.target = cls.fixture / "target"
        for relative, text in FIXTURE.items():
            path = cls.fixture / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(textwrap.dedent(text).lstrip(), encoding="utf-8")
        shutil.copy(REPOSITORY / "rust-toolchain.toml", cls.fixture / "rust-toolchain.toml")
        for command in (
            ["git", "init", "--quiet"],
            ["git", "add", "--all"],
            [
                "git",
                "-c",
                "user.name=fixture",
                "-c",
                "user.email=fixture@example.invalid",
                "commit",
                "--quiet",
                "--message",
                "fixture",
            ],
        ):
            subprocess.run(command, cwd=cls.fixture, check=True, capture_output=True)

    def collect(self, producer: str, **variables: str) -> Collection:
        environment = {
            name: value
            for name, value in os.environ.items()
            if not name.startswith("GITHUB_")
            and name not in ("CARGO_TARGET_DIR", native_coverage.ATTEMPT_VARIABLE)
        }
        environment.update(variables)
        printed = io.StringIO()
        with (self.fixture / "recipes.log").open("a", encoding="utf-8") as log, redirect_stdout(
            printed
        ):
            status = native_coverage.main(
                ["--root", str(self.fixture), "--target-dir", str(self.target), "run", producer],
                producers=FIXTURE_PRODUCERS,
                commands_for=lambda root: Commands(root, output=log),
                environment=environment,
            )
        match = re.search(r"^  record: (\S+)$", printed.getvalue(), re.MULTILINE)
        self.assertIsNotNone(match, printed.getvalue())
        assert match is not None
        record_path = self.fixture / match.group(1)
        return Collection(
            status=status,
            attempt=record_path.parent,
            record=json.loads(record_path.read_text(encoding="utf-8")),
        )

    def assert_failed(self, collection: Collection, stage: str, detail: str) -> None:
        self.assertNotEqual(collection.status, 0)
        self.assertEqual(collection.record["verdict"], "failed")
        failure = collection.record["failure"]
        assert isinstance(failure, dict)
        self.assertEqual(failure["stage"], stage)
        self.assertIn(detail, failure["detail"])
        self.assertFalse((collection.attempt / "lcov.info").exists())

    def line_counts(self, report: Path, source: str) -> dict[int, int]:
        counts: dict[int, int] = {}
        current = None
        for line in report.read_text(encoding="utf-8").splitlines():
            if line.startswith("SF:"):
                current = line[3:]
            elif line.startswith("DA:") and current == str(self.fixture / source):
                number, count = line[3:].split(",")[:2]
                counts[int(number)] = int(count)
        return counts

    def test_repeated_runs_select_the_same_executions_and_count_only_their_own(self) -> None:
        first = self.collect("walk")
        first_report = (first.attempt / "lcov.info").read_bytes()
        first_record = (first.attempt / "completion.json").read_bytes()
        second = self.collect("walk")

        for collection in (first, second):
            self.assertEqual(collection.status, 0, collection.record.get("failure"))
            self.assertEqual(collection.record["verdict"], "complete")
        self.assertNotEqual(first.attempt, second.attempt)
        self.assertEqual(first.record["selection"], second.record["selection"])
        self.assertEqual((second.attempt / "lcov.info").read_bytes(), first_report)
        self.assertEqual((first.attempt / "completion.json").read_bytes(), first_record)

        selection = first.record["selection"]
        assert isinstance(selection, dict)
        [walked] = selection["executions"]
        self.assertRegex(walked["executable"], r"^target/native-coverage-build/debug/deps/walk-[0-9a-f]+$")
        self.assertEqual(walked["arguments"], [])
        [child] = selection["children"]
        self.assertIn("coverage-fixture-child", child["executable"])

        report = first.attempt / "lcov.info"
        sources = [line[3:] for line in report.read_text().splitlines() if line.startswith("SF:")]
        self.assertEqual(
            sorted(sources),
            [str(self.fixture / "src/bin/child.rs"), str(self.fixture / "src/lib.rs")],
        )
        library = self.line_counts(report, "src/lib.rs")
        self.assertEqual(library[3], 1)
        self.assertEqual(library[5], 0)
        self.assertEqual(library[10], 1)
        self.assertEqual(library[14], 0)
        # Only the child runs the inlined function; the walk's copy of it never ran.
        self.assertEqual(library[19], 1)
        self.assertIsInstance(first.record["export_warnings"], list)
        described = first.record["sources"]
        assert isinstance(described, dict)
        self.assertEqual(described["excluded_files"], {"harness": 1})
        self.assertEqual(set(described["packages"]), {"coverage-fixture"})

    def test_the_ordinary_coverage_cleanup_leaves_every_collection_alone(self) -> None:
        collection = self.collect("walk")
        self.assertEqual(collection.status, 0, collection.record.get("failure"))
        collections = files(self.target / native_coverage.COLLECTION_DIRECTORY)
        build = files(self.target / native_coverage.BUILD_DIRECTORY)
        subprocess.run(
            ["cargo", "llvm-cov", "clean", "--workspace"],
            cwd=self.fixture,
            check=True,
            capture_output=True,
        )
        self.assertEqual(files(self.target / native_coverage.COLLECTION_DIRECTORY), collections)
        self.assertEqual(files(self.target / native_coverage.BUILD_DIRECTORY), build)

    def test_prepare_and_finish_recipes_run_outside_the_instrumentation(self) -> None:
        collection = self.collect("checked-walk")
        self.assertEqual(collection.status, 0, collection.record.get("failure"))
        for recipe in ("prepare", "finish"):
            with self.subTest(recipe=recipe):
                variables = (self.fixture / f"{recipe}.env").read_text()
                self.assertNotIn(native_coverage.ATTEMPT_VARIABLE, variables)
                self.assertNotIn("instrument-coverage", variables)
                self.assertNotIn("LLVM_PROFILE_FILE=/dev/null", variables)

    def test_a_failing_check_fails_and_keeps_the_profiles_it_wrote(self) -> None:
        collection = self.collect("failing-walk")
        self.assert_failed(collection, "run", "`just failing-walk` exited with status")
        self.assertTrue(any((collection.attempt / "profiles").glob("*.profraw")))

    def test_an_execution_without_a_profile_fails_the_export(self) -> None:
        collection = self.collect("walk-without-profile")
        self.assert_failed(collection, "export", "ran but wrote no profile")

    def test_an_executable_gone_before_the_export_fails_it(self) -> None:
        collection = self.collect("walk-then-remove")
        self.assert_failed(collection, "export", "ran but is no longer there to export")

    def test_a_recipe_that_runs_nothing_fails(self) -> None:
        collection = self.collect("nothing")
        self.assert_failed(collection, "export", "ran no executable through Cargo")

    def test_an_interrupted_collection_is_never_complete(self) -> None:
        cases = {"interrupted": (130, "SIGINT"), "terminated": (143, "SIGTERM")}
        for producer, (status, signal_name) in cases.items():
            with self.subTest(producer=producer):
                collection = self.collect(producer, COVERAGE_FIXTURE_COLLECTOR=str(os.getpid()))
                self.assertEqual(collection.status, status)
                self.assertEqual(collection.record["verdict"], "interrupted")
                self.assertEqual(
                    collection.record["failure"],
                    {"stage": "run", "detail": f"interrupted by {signal_name}"},
                )


if __name__ == "__main__":
    unittest.main()
