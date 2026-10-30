"""Exercise the ordinary coverage merge: its source mapping, its integrity failures and its CI contract.

Inputs are built the way the producer jobs publish them: a directory holding the absolute checkout
path its producer ran in, the workspace export, Python tooling reports and native collections.
"""

from __future__ import annotations

import io
import json
import re
import subprocess
import tempfile
import unittest
from collections.abc import Mapping
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path, PurePosixPath

from scripts import coverage_merge
from scripts.coverage_merge import Checkout, Dropped, Members, Merge, MergeError, Tracked
from scripts.tests.test_native_coverage import job_section, step_section

ROOT = coverage_merge.ROOT
TESTS_ROOT = "/ci/tests/nervix"
SCENARIOS_ROOT = "/ci/scenarios/nervix"
EXTRAS_ROOT = "/ci/extras/nervix"
TRACKED = (
    "src/lib.rs",
    "src/runtime/mod.rs",
    "crates/alpha/src/lib.rs",
    "tests/runtime/restore.rs",
    "scripts/tool.py",
)


def lcov(records: Mapping[str, Mapping[int, int]]) -> str:
    """An LCOV report holding one record of line counters for each source."""
    text = ""
    for source, lines in records.items():
        entries = "".join(f"DA:{line},{hits}\n" for line, hits in lines.items())
        text += f"TN:\nSF:{source}\n{entries}end_of_record\n"
    return text


class Workspace:
    """A checkout of a two-package workspace and the producer artifacts a merge reads."""

    def __init__(self, case: unittest.TestCase) -> None:
        directory = tempfile.TemporaryDirectory()
        case.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        scratch = tempfile.TemporaryDirectory()
        case.addCleanup(scratch.cleanup)
        self.scratch = Path(scratch.name)
        self.git("init", "--quiet")
        self.git("config", "user.name", "Coverage Test")
        self.git("config", "user.email", "coverage@example.com")
        self.write("Cargo.toml", '[workspace]\nmembers = ["crates/alpha"]\n\n[package]\nname = "server"\nversion = "0.1.0"\nedition = "2021"\n')
        self.write("crates/alpha/Cargo.toml", '[package]\nname = "alpha"\nversion = "0.1.0"\nedition = "2021"\n')
        for path in TRACKED:
            self.write(path, "fn main() {}\n")
        self.git("add", ".")
        self.git("commit", "--quiet", "-m", "test: record sources")
        self.members = Members.from_metadata({
            "workspace_root": str(self.root),
            "workspace_members": ["server", "alpha"],
            "packages": [
                {"id": "server", "name": "server", "manifest_path": str(self.root / "Cargo.toml")},
                {"id": "alpha", "name": "alpha", "manifest_path": str(self.root / "crates/alpha/Cargo.toml")},
            ],
        })

    def git(self, *arguments: str) -> str:
        return subprocess.run(
            ["git", *arguments], cwd=self.root, check=True, capture_output=True, text=True
        ).stdout

    def write(self, path: str, text: str) -> Path:
        destination = self.root / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(text)
        return destination

    def producer(self, name: str, origin: str | None) -> Path:
        directory = self.root / "coverage-inputs" / name
        directory.mkdir(parents=True)
        if origin is not None:
            (directory / coverage_merge.SOURCE_ROOT_FILE).write_text(f"{origin}\n")
        return directory

    def export(self, name: str, origin: str, records: Mapping[str, Mapping[int, int]]) -> Path:
        directory = self.producer(name, origin)
        (directory / coverage_merge.WORKSPACE_REPORT).write_text(lcov(records))
        return directory

    def collection(
        self, directory: Path, producer: str, mode: str, records: Mapping[str, Mapping[int, int]] | None,
        verdict: str = "complete", attempt: str = "github-1-1",
    ) -> Path:
        attempt_directory = directory / "native-coverage" / producer / mode / "rust-1.99.0" / attempt
        attempt_directory.mkdir(parents=True)
        (attempt_directory / "completion.json").write_text(json.dumps({
            "producer": producer, "mode": mode, "verdict": verdict, "attempt": attempt,
            "revision": {"commit": "c0ffee", "modified": False},
            "run": {"provider": "github", "run": "1", "attempt": "1", "job": "extra-tests"},
            "rerun": f"just coverage-native-extras {producer}",
        }))
        if records is not None:
            (attempt_directory / "lcov.info").write_text(lcov(records))
        return attempt_directory

    def extras(self, records: Mapping[str, Mapping[int, int]] | None = None) -> Path:
        """The native extras artifact: Python tooling reports and the native collections of every mode."""
        directory = self.producer("native-extras", EXTRAS_ROOT)
        (directory / "patch-coverage").mkdir()
        (directory / "patch-coverage" / "python.lcov").write_text(lcov({"scripts/tool.py": {1: 1}}))
        self.collection(directory, "bench-smoke", "ordinary", records or {f"{EXTRAS_ROOT}/crates/alpha/src/lib.rs": {1: 4}})
        for mode in ("shuttle", "loom", "turmoil", "deloxide"):
            self.collection(directory, f"test-primitives-{mode}", mode, {f"{EXTRAS_ROOT}/src/runtime/mod.rs": {1: 9}})
        return directory

    def merge(self, *inputs: Path) -> tuple[Merge, str]:
        merge = Merge(self.root, Checkout.load(self.root), self.members, Path(tempfile.mkdtemp(dir=self.scratch)))
        for item in inputs:
            merge.read(item)
        output = self.root / "lcov-workspace.info"
        merge.write(output)
        return merge, output.read_text()


def sources(report: str) -> list[str]:
    return [line[3:] for line in report.splitlines() if line.startswith("SF:")]


def counters(report: str, source: str) -> dict[int, int]:
    lines: dict[int, int] = {}
    current = None
    for line in report.splitlines():
        if line.startswith("SF:"):
            current = line[3:]
        elif line.startswith("DA:") and current == source:
            number, hits = line[3:].split(",")[:2]
            lines[int(number)] = int(hits)
    return lines


class MappingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.checkout = Checkout(frozenset(TRACKED))
        self.origin = (PurePosixPath(TESTS_ROOT),)

    def test_the_producer_root_is_stripped_and_dot_dot_segments_resolve_lexically(self) -> None:
        located = self.checkout.locate(f"{TESTS_ROOT}/src/runtime/../../tests/runtime/restore.rs", self.origin)
        self.assertEqual(located, Tracked("tests/runtime/restore.rs"))
        self.assertEqual(self.checkout.locate(f"{TESTS_ROOT}/src/lib.rs", self.origin), Tracked("src/lib.rs"))

    def test_relative_sources_are_repository_paths(self) -> None:
        self.assertEqual(self.checkout.locate("scripts/tool.py", self.origin), Tracked("scripts/tool.py"))
        self.assertEqual(self.checkout.locate("scripts/../scripts/tool.py", ()), Tracked("scripts/tool.py"))

    def test_the_most_specific_declared_root_strips_a_source(self) -> None:
        origins = (PurePosixPath("/ci/nested/nervix"), PurePosixPath("/ci"))
        self.assertEqual(self.checkout.locate("/ci/nested/nervix/src/lib.rs", origins), Tracked("src/lib.rs"))

    def test_each_dependency_toolchain_and_generated_location_is_dropped_by_its_class(self) -> None:
        for source, dropped in (
            ("/home/runner/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/anstream-1.0.0/src/lib.rs", Dropped.CARGO_REGISTRY),
            ("/home/runner/.cargo/git/checkouts/rkyv-85ab059846312217/868fa9c/rkyv/src/lib.rs", Dropped.CARGO_GIT),
            ("/kache/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/chumsky-0.12.0/src/lib.rs", Dropped.KACHE_REGISTRY),
            ("/rustc/b940084d7eb6a299eb4bfeb8e34901bc051e7ac4/library/std/src/sys/thread_local/native/mod.rs", Dropped.TOOLCHAIN),
            ("/home/dev/.rustup/toolchains/1.99-x86_64-unknown-linux-gnu/lib/rustlib/src/rust/library/std/src/lib.rs", Dropped.TOOLCHAIN),
            (f"{TESTS_ROOT}/target/llvm-cov-target/debug/build/nervix-client-wire-4a7cdd872e94304c/out/flatbuffers/session_module.rs", Dropped.GENERATED),
        ):
            with self.subTest(dropped=dropped):
                self.assertEqual(self.checkout.locate(source, self.origin), dropped)

    def test_sources_the_tested_revision_does_not_track_are_dropped(self) -> None:
        for source, dropped in (
            (f"{TESTS_ROOT}/src/removed.rs", Dropped.UNTRACKED),
            ("scripts/removed.py", Dropped.UNTRACKED),
            ("target/paced-simulation/generated.py", Dropped.GENERATED),
            (f"{TESTS_ROOT}/../elsewhere/src/lib.rs", Dropped.OUTSIDE),
            ("../elsewhere/src/lib.rs", Dropped.OUTSIDE),
            ("/opt/other/src/lib.rs", Dropped.OUTSIDE),
        ):
            with self.subTest(source=source):
                self.assertEqual(self.checkout.locate(source, self.origin), dropped)

    def test_every_dropped_class_states_its_policy(self) -> None:
        self.assertEqual(set(coverage_merge.DROPPED_POLICY), set(Dropped))


class MergeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workspace = Workspace(self)

    def test_ordinary_producers_merge_into_one_repository_relative_report(self) -> None:
        tests = self.workspace.export("tests", TESTS_ROOT, {
            f"{TESTS_ROOT}/src/lib.rs": {1: 2, 2: 0},
            f"{TESTS_ROOT}/src/runtime/../../tests/runtime/restore.rs": {1: 1},
            "/rustc/b940084d7eb6a299eb4bfeb8e34901bc051e7ac4/library/std/src/lib.rs": {1: 1},
        })
        scenarios = self.workspace.export("scenarios", SCENARIOS_ROOT, {
            f"{SCENARIOS_ROOT}/src/lib.rs": {1: 3, 2: 5},
            f"{SCENARIOS_ROOT}/tests/runtime/restore.rs": {1: 4, 3: 0},
            "/home/runner/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/anstream-1.0.0/src/lib.rs": {1: 1},
        })
        (scenarios / "target" / "paced-simulation").mkdir(parents=True)
        (scenarios / "target" / "paced-simulation" / "python.lcov").write_text(lcov({"scripts/tool.py": {2: 1}}))
        extras = self.workspace.extras()
        merge, report = self.workspace.merge(tests, scenarios, extras)
        self.assertEqual(
            sources(report),
            ["crates/alpha/src/lib.rs", "scripts/tool.py", "src/lib.rs", "tests/runtime/restore.rs"],
        )
        self.assertEqual(counters(report, "src/lib.rs"), {1: 5, 2: 5})
        self.assertEqual(counters(report, "tests/runtime/restore.rs"), {1: 5, 3: 0})
        self.assertEqual(counters(report, "scripts/tool.py"), {1: 1, 2: 1})
        self.assertEqual(counters(report, "crates/alpha/src/lib.rs"), {1: 4})
        described = merge.describe()
        self.assertEqual(described["records"], {"read": 9, "kept": 7, "files": 4})
        self.assertEqual(described["dropped_files"], {"cargo-registry": 1, "toolchain": 1})

    def test_collections_of_other_modes_are_never_read_and_are_recorded_as_left_out(self) -> None:
        tests = self.workspace.export("tests", TESTS_ROOT, {f"{TESTS_ROOT}/src/lib.rs": {1: 1}})
        merge, report = self.workspace.merge(tests, self.workspace.extras())
        self.assertNotIn("src/runtime/mod.rs", sources(report))
        [extras] = [item for item in merge.describe()["inputs"] if item["input"].endswith("native-extras")]
        self.assertEqual(
            [(item["producer"], item["mode"]) for item in extras["left_out"]],
            [("test-primitives-deloxide", "deloxide"), ("test-primitives-loom", "loom"),
             ("test-primitives-shuttle", "shuttle"), ("test-primitives-turmoil", "turmoil")],
        )
        self.assertEqual(
            [(item["producer"], item["mode"]) for item in extras["reports"] if "producer" in item],
            [("bench-smoke", "ordinary")],
        )

    def test_an_incomplete_collection_of_another_mode_is_left_out_without_failing(self) -> None:
        tests = self.workspace.export("tests", TESTS_ROOT, {f"{TESTS_ROOT}/src/lib.rs": {1: 1}})
        extras = self.workspace.extras()
        self.workspace.collection(extras, "test-shuttle", "shuttle", None, verdict="failed")
        merge, _ = self.workspace.merge(tests, extras)
        [extras_input] = [item for item in merge.describe()["inputs"] if item["input"].endswith("native-extras")]
        self.assertIn(("test-shuttle", "shuttle", "failed"), [
            (item["producer"], item["mode"], item["verdict"]) for item in extras_input["left_out"]
        ])

    def test_the_merge_record_lists_inputs_counts_and_dropped_records_by_class(self) -> None:
        tests = self.workspace.export("tests", TESTS_ROOT, {
            f"{TESTS_ROOT}/src/lib.rs": {1: 1},
            f"{TESTS_ROOT}/crates/alpha/src/lib.rs": {1: 1},
            "/kache/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/chumsky-0.12.0/src/lib.rs": {1: 1},
            "/home/runner/.cargo/git/checkouts/rkyv-85ab059846312217/868fa9c/rkyv/src/lib.rs": {1: 1},
            f"{TESTS_ROOT}/target/llvm-cov-target/debug/build/wire-4a7c/out/flatbuffers/session_module.rs": {1: 1},
            f"{TESTS_ROOT}/src/removed.rs": {1: 1},
        })
        merge, _ = self.workspace.merge(tests)
        described = merge.describe()
        [item] = described["inputs"]
        self.assertEqual(item["root"], TESTS_ROOT)
        [report] = item["reports"]
        self.assertEqual(report["report"], "coverage-inputs/tests/lcov-workspace.info")
        self.assertEqual((report["records"], report["kept"]), (6, 2))
        self.assertEqual(report["dropped"], {"cargo-git": 1, "generated": 1, "kache-registry": 1, "untracked": 1})
        self.assertEqual(described["dropped_files"], report["dropped"])
        self.assertEqual(described["members"], {"measured": ["alpha", "server"], "unmeasured": []})
        self.assertEqual(set(described["policy"]["dropped"]), {str(dropped) for dropped in Dropped})

    def test_the_report_keeps_line_and_function_counters_and_leaves_out_branches(self) -> None:
        tests = self.workspace.export("tests", TESTS_ROOT, {f"{TESTS_ROOT}/src/lib.rs": {1: 1}, f"{TESTS_ROOT}/crates/alpha/src/lib.rs": {1: 1}})
        (tests / "python.lcov").write_text(
            "SF:scripts/tool.py\nFN:1,2,Tool.run\nFNDA:1,Tool.run\nFNF:1\nFNH:1\n"
            "BRDA:1,0,jump to line 2,1\nBRDA:1,0,return from function 'run',0\nBRF:2\nBRH:1\n"
            "DA:1,1\nDA:2,1\nLF:2\nLH:2\nend_of_record\n"
        )
        _, report = self.workspace.merge(tests)
        self.assertIn("SF:scripts/tool.py\nFN:1,2,Tool.run\nFNDA:1,Tool.run\nFNF:1\nFNH:1\nDA:1,1\nDA:2,1\n", report)
        self.assertNotIn("BR", report)

    def test_a_line_counter_that_wrapped_below_zero_is_left_out_of_the_sum_and_recorded(self) -> None:
        wrapped = 2**64 - 1
        tests = self.workspace.export("tests", TESTS_ROOT, {
            f"{TESTS_ROOT}/src/lib.rs": {1: 3, 2: 1, 4: 1, 5: 1},
            f"{TESTS_ROOT}/crates/alpha/src/lib.rs": {1: 1},
        })
        scenarios = self.workspace.export("scenarios", SCENARIOS_ROOT, {
            f"{SCENARIOS_ROOT}/src/lib.rs": {1: wrapped, 2: 5, 3: 2**64 - 2, 4: 2**63 - 1, 5: 2**63},
        })
        merge, report = self.workspace.merge(tests, scenarios)
        # Line 3 has no counter but a wrapped one, so the merged report does not measure it.
        self.assertEqual(counters(report, "src/lib.rs"), {1: 3, 2: 6, 4: 2**63, 5: 1})
        described = merge.describe()
        [tests_input, scenarios_input] = described["inputs"]
        self.assertEqual(tests_input["reports"][0]["wrapped"], [])
        self.assertEqual(scenarios_input["reports"][0]["wrapped"], [
            {"source": "src/lib.rs", "line": 1, "count": wrapped},
            {"source": "src/lib.rs", "line": 3, "count": 2**64 - 2},
            {"source": "src/lib.rs", "line": 5, "count": 2**63},
        ])
        self.assertEqual(described["wrapped_counters"], 3)
        self.assertIn("wrapped", described["policy"])
        self.assertIn("Counters that wrapped below zero, left out of the sum: 3", merge.summary("report", "record"))

    def test_a_function_counter_that_wrapped_below_zero_is_left_out_of_the_sum_and_recorded(self) -> None:
        tests = self.workspace.export("tests", TESTS_ROOT, {f"{TESTS_ROOT}/crates/alpha/src/lib.rs": {1: 1}})
        (tests / coverage_merge.WORKSPACE_REPORT).write_text(
            (tests / coverage_merge.WORKSPACE_REPORT).read_text()
            + f"SF:{TESTS_ROOT}/src/lib.rs\nFN:1,2,main\nFNDA:{2**64 - 1},main\nDA:1,1\nend_of_record\n"
        )
        scenarios = self.workspace.export("scenarios", SCENARIOS_ROOT, {})
        (scenarios / coverage_merge.WORKSPACE_REPORT).write_text(
            f"SF:{SCENARIOS_ROOT}/src/lib.rs\nFN:1,2,main\nFNDA:4,main\nDA:1,2\nend_of_record\n"
        )
        merge, report = self.workspace.merge(tests, scenarios)
        self.assertIn("SF:src/lib.rs\nFN:1,2,main\nFNDA:4,main\nFNF:1\nFNH:1\nDA:1,3\n", report)
        [tests_input, _] = merge.describe()["inputs"]
        self.assertEqual(
            tests_input["reports"][0]["wrapped"], [{"source": "src/lib.rs", "function": "main", "count": 2**64 - 1}]
        )

    def test_files_outside_the_reports_of_an_input_are_never_read(self) -> None:
        tests = self.workspace.export("tests", TESTS_ROOT, {f"{TESTS_ROOT}/src/lib.rs": {1: 1}})
        (tests / "lcov.info").write_text(lcov({f"{TESTS_ROOT}/src/runtime/mod.rs": {1: 1}}))
        (tests / "qualification").mkdir()
        (tests / "qualification" / "completion.json").write_text("{}")
        merge, report = self.workspace.merge(tests, self.workspace.extras())
        self.assertEqual(sources(report), ["crates/alpha/src/lib.rs", "scripts/tool.py", "src/lib.rs"])
        [item] = [item for item in merge.describe()["inputs"] if item["input"] == "coverage-inputs/tests"]
        self.assertEqual([report["report"] for report in item["reports"]], ["coverage-inputs/tests/lcov-workspace.info"])

    def test_a_last_record_without_a_final_newline_keeps_its_end(self) -> None:
        tests = self.workspace.export("tests", TESTS_ROOT, {})
        (tests / coverage_merge.WORKSPACE_REPORT).write_text(
            f"SF:{TESTS_ROOT}/crates/alpha/src/lib.rs\nDA:1,1\nend_of_record\nSF:{TESTS_ROOT}/src/lib.rs\nDA:1,2\nend_of_record"
        )
        _, report = self.workspace.merge(tests)
        self.assertEqual(sources(report), ["crates/alpha/src/lib.rs", "src/lib.rs"])
        self.assertEqual(counters(report, "src/lib.rs"), {1: 2})

    def test_an_input_outside_the_repository_is_named_by_its_absolute_path(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            outside = Path(directory) / "coverage-tests"
            with self.assertRaisesRegex(MergeError, f"^{outside} does not exist"):
                self.workspace.merge(outside)

    def test_one_input_named_twice_is_refused(self) -> None:
        tests = self.workspace.export("tests", TESTS_ROOT, {f"{TESTS_ROOT}/src/lib.rs": {1: 1}})
        with self.assertRaisesRegex(MergeError, "named twice"):
            self.workspace.merge(tests, tests)


class IntegrityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workspace = Workspace(self)
        self.records = {f"{TESTS_ROOT}/src/lib.rs": {1: 1}, f"{TESTS_ROOT}/crates/alpha/src/lib.rs": {1: 1}}

    def test_a_missing_input_fails(self) -> None:
        with self.assertRaisesRegex(MergeError, "coverage-inputs/tests does not exist"):
            self.workspace.merge(self.workspace.root / "coverage-inputs" / "tests")

    def test_an_input_without_an_ordinary_rust_report_fails(self) -> None:
        scenarios = self.workspace.producer("scenarios", SCENARIOS_ROOT)
        (scenarios / "python.lcov").write_text(lcov({"scripts/tool.py": {1: 1}}))
        with self.assertRaisesRegex(MergeError, "holds no workspace export .* and no complete ordinary native collection"):
            self.workspace.merge(scenarios)

    def test_an_input_without_root_metadata_fails(self) -> None:
        tests = self.workspace.producer("tests", None)
        (tests / coverage_merge.WORKSPACE_REPORT).write_text(lcov(self.records))
        with self.assertRaisesRegex(MergeError, "has no root metadata"):
            self.workspace.merge(tests)
        for value in ("", "relative/nervix"):
            with self.subTest(value=value):
                (tests / coverage_merge.SOURCE_ROOT_FILE).write_text(value)
                with self.assertRaisesRegex(MergeError, "must name the absolute checkout path"):
                    self.workspace.merge(tests)

    def test_records_below_none_of_the_declared_roots_fail(self) -> None:
        tests = self.workspace.export("tests", "/home/runner/work/nervix/nervix", {
            "/home/runner/_work/nervix/nervix/src/lib.rs": {1: 1},
        })
        with self.assertRaisesRegex(MergeError, "none of its 1 records lies below /home/runner/work/nervix/nervix"):
            self.workspace.merge(tests)

    def test_a_producer_without_a_repository_record_fails(self) -> None:
        for records in ({f"{TESTS_ROOT}/src/removed.rs": {1: 1}}, {}):
            with self.subTest(records=records):
                workspace = Workspace(self)
                tests = workspace.export("tests", TESTS_ROOT, records)
                with self.assertRaisesRegex(MergeError, "contributes no record of a file the tested revision tracks"):
                    workspace.merge(tests)
        workspace = Workspace(self)
        scenarios = workspace.export("scenarios", SCENARIOS_ROOT, {f"{SCENARIOS_ROOT}/src/lib.rs": {1: 1}})
        (scenarios / "python.lcov").write_text(lcov({"scripts/removed.py": {1: 1}}))
        with self.assertRaisesRegex(MergeError, "python.lcov contributes no record"):
            workspace.merge(scenarios)

    def test_a_native_report_without_a_complete_ordinary_completion_record_fails(self) -> None:
        def failed(directory: Path) -> None:
            self.workspace.collection(directory, "plexus-completion-walk", "ordinary", self.records, verdict="failed")

        def unrecorded(directory: Path) -> None:
            attempt = self.workspace.collection(directory, "plexus-completion-walk", "ordinary", self.records)
            (attempt / "completion.json").unlink()

        def reportless(directory: Path) -> None:
            self.workspace.collection(directory, "plexus-completion-walk", "ordinary", None)

        def malformed(directory: Path) -> None:
            attempt = self.workspace.collection(directory, "plexus-completion-walk", "ordinary", self.records)
            (attempt / "completion.json").write_text(json.dumps({"verdict": "complete"}))

        def repeated(directory: Path) -> None:
            self.workspace.collection(directory, "bench-smoke", "ordinary", self.records, attempt="github-1-2")

        for corrupt, reason in (
            (failed, r"ordinary collection of plexus-completion-walk is failed, not complete; rerun just coverage-native-extras plexus-completion-walk"),
            (unrecorded, r"lcov.info is a native report without a completion record beside it"),
            (reportless, r"complete ordinary collection of plexus-completion-walk holds no lcov.info"),
            (malformed, r"completion.json is not a native coverage completion record"),
            (repeated, r"bench-smoke has more than one ordinary collection"),
        ):
            with self.subTest(reason=reason):
                self.workspace = Workspace(self)
                extras = self.workspace.extras()
                corrupt(extras)
                with self.assertRaisesRegex(MergeError, reason):
                    self.workspace.merge(extras)

    def test_a_workspace_member_without_a_record_fails(self) -> None:
        tests = self.workspace.export("tests", TESTS_ROOT, {f"{TESTS_ROOT}/src/lib.rs": {1: 1}, "scripts/tool.py": {1: 1}})
        with self.assertRaisesRegex(MergeError, r"no record of the workspace members alpha"):
            self.workspace.merge(tests)

    def test_a_python_record_inside_a_member_does_not_measure_it(self) -> None:
        self.workspace.write("crates/alpha/tool.py", "answer = 1\n")
        self.workspace.git("add", ".")
        self.workspace.git("commit", "--quiet", "-m", "test: add a script inside a package")
        tests = self.workspace.export("tests", TESTS_ROOT, {f"{TESTS_ROOT}/src/lib.rs": {1: 1}, "crates/alpha/tool.py": {1: 1}})
        with self.assertRaisesRegex(MergeError, r"no record of the workspace members alpha"):
            self.workspace.merge(tests)


class CommandTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workspace = Workspace(self)
        self.output = self.workspace.root / coverage_merge.WORKSPACE_REPORT
        self.record = self.workspace.root / "target" / "coverage-merge" / "record.json"

    def main(self, *arguments: str) -> tuple[int, str]:
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()) as errors:
            status = coverage_merge.main(
                ["--output", str(self.output), "--record", str(self.record), *arguments],
                root=self.workspace.root,
            )
        return status, errors.getvalue()

    def test_a_local_producer_report_merges_in_place_into_the_shape_ci_publishes(self) -> None:
        root = self.workspace.root
        self.output.write_text(lcov({
            f"{root}/src/lib.rs": {1: 1},
            f"{root}/crates/alpha/src/lib.rs": {1: 0},
            "/rustc/b940084d7eb6a299eb4bfeb8e34901bc051e7ac4/library/std/src/lib.rs": {1: 1},
        }))
        (root / coverage_merge.SOURCE_ROOT_FILE).write_text(f"{root}\n")
        status, errors = self.main()
        self.assertEqual((status, errors), (0, ""))
        merged = self.output.read_text()
        self.assertEqual(sources(merged), ["crates/alpha/src/lib.rs", "src/lib.rs"])
        record = json.loads(self.record.read_text())
        self.assertEqual(record["verdict"], "complete")
        self.assertEqual(record["revision"], self.workspace.git("rev-parse", "HEAD").strip())
        self.assertEqual(record["report"], coverage_merge.WORKSPACE_REPORT)
        status, errors = self.main()
        self.assertEqual((status, errors), (0, ""))
        self.assertEqual(self.output.read_text(), merged)

    def test_a_failed_merge_publishes_no_report_and_records_its_failure(self) -> None:
        tests = self.workspace.export("tests", TESTS_ROOT, {f"{TESTS_ROOT}/src/lib.rs": {1: 1}})
        status, errors = self.main(str(tests.relative_to(self.workspace.root)))
        self.assertEqual(status, 1)
        self.assertIn("coverage merge: the merged report has no record of the workspace members alpha", errors)
        self.assertFalse(self.output.exists())
        record = json.loads(self.record.read_text())
        self.assertEqual(record["verdict"], "failed")
        self.assertIn("workspace members alpha", record["failure"])
        self.assertEqual(record["inputs"][0]["input"], "coverage-inputs/tests")

    def test_a_malformed_report_fails_with_its_location(self) -> None:
        tests = self.workspace.producer("tests", TESTS_ROOT)
        (tests / coverage_merge.WORKSPACE_REPORT).write_text("SF:/ci/tests/nervix/src/lib.rs\nDA:1,many\nend_of_record\n")
        status, errors = self.main(str(tests))
        self.assertEqual(status, 1)
        self.assertIn("coverage-inputs/tests/lcov-workspace.info: malformed LCOV line", errors)
        self.assertFalse(self.output.exists())

    def test_a_malformed_function_counter_fails_with_its_location(self) -> None:
        tests = self.workspace.producer("tests", TESTS_ROOT)
        (tests / coverage_merge.WORKSPACE_REPORT).write_text(
            "SF:/ci/tests/nervix/src/lib.rs\nFN:1,2,main\nFNDA:many,main\nDA:1,1\nend_of_record\n"
        )
        status, errors = self.main(str(tests))
        self.assertEqual(status, 1)
        self.assertIn(
            "coverage-inputs/tests/lcov-workspace.info: malformed LCOV line in the record of src/lib.rs: FNDA:many,main",
            errors,
        )
        self.assertFalse(self.output.exists())


class MembersTests(unittest.TestCase):
    def test_only_workspace_members_own_sources_and_a_virtual_workspace_root_owns_none(self) -> None:
        members = Members.from_metadata({
            "workspace_root": "/ci/nervix",
            "workspace_members": ["alpha"],
            "packages": [
                {"id": "alpha", "name": "alpha", "manifest_path": "/ci/nervix/crates/alpha/Cargo.toml"},
                {"id": "vendored", "name": "vendored", "manifest_path": "/ci/nervix/vendor/vendored/Cargo.toml"},
            ],
        })
        self.assertEqual(members.names(), ["alpha"])
        self.assertEqual(members.owner("crates/alpha/src/lib.rs"), "alpha")
        self.assertIsNone(members.owner("vendor/vendored/src/lib.rs"))
        self.assertEqual(members.measured(["vendor/vendored/src/lib.rs", "crates/alpha/src/lib.rs"]), {"alpha"})

    def test_a_workspace_without_members_or_without_metadata_fails(self) -> None:
        with self.assertRaisesRegex(MergeError, "listed no workspace members"):
            Members.from_metadata({"workspace_root": "/ci/nervix", "workspace_members": [], "packages": []})
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(MergeError, "cargo metadata failed"):
                Members.load(Path(directory))
            with self.assertRaisesRegex(MergeError, "git ls-files failed"):
                Checkout.load(Path(directory))

    def test_workspace_members_come_from_cargo_metadata_by_most_specific_directory(self) -> None:
        members = Members.load(ROOT)
        self.assertEqual(members.owner("src/runtime/mod.rs"), "nervix-server")
        self.assertEqual(members.owner("crates/primitives/macros/src/lib.rs"), "nervix-primitives-macros")
        self.assertEqual(members.owner("crates/primitives/src/lib.rs"), "nervix-primitives")
        self.assertIn("nervix-paced-simulation", members.names())
        self.assertNotIn("nervix-lint", members.names())


class WorkflowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workflow = (ROOT / ".github/workflows/check.yaml").read_text()
        self.justfile = (ROOT / "justfile").read_text()

    def test_the_coverage_job_merges_the_ordinary_artifacts_through_the_recipe(self) -> None:
        job = job_section(self.workflow, "coverage")
        self.assertIn("needs: [tests, scenarios, extra-tests]", job)
        downloads = [step for step in job.split("\n      - name: ") if "uses: ./.github/actions/ci-artifact-download" in step]
        self.assertEqual(
            [re.search(r"(?:name|pattern): (\S+)", step.split("with:")[1])[1] for step in downloads],
            ["coverage-tests", "coverage-scenarios-*", "coverage-native-extras"],
        )
        # Every scenario shard's report lands in a directory named after its artifact, and the merge
        # names each shard the workflow runs, so a missing report fails it.
        self.assertIn("          path: coverage-inputs\n", step_section(job, "Download scenarios coverage"))
        merge = step_section(job, "Merge coverage")
        self.assertIn('for shard in $(seq 1 "${SCENARIO_SHARDS}"); do\n', merge)
        self.assertIn('scenarios+=("coverage-inputs/coverage-scenarios-${shard}")\n', merge)
        self.assertIn(
            'just coverage-merge coverage-inputs/tests "${scenarios[@]}" coverage-inputs/native-extras\n',
            merge,
        )
        self.assertIn('  SCENARIO_SHARDS: "', self.workflow.split("\njobs:\n", 1)[0])
        self.assertNotIn("lcov --add-tracefile", job)
        self.assertNotIn("apt-get install -y lcov", job)
        report = step_section(job, "Report patch line coverage")
        self.assertIn('just coverage-patch "$BASE_SHA" lcov-workspace.info', report)
        self.assertNotIn("--source-root-file", report)
        upload = step_section(job, "Upload merged coverage")
        for path in ("lcov-workspace.info", "target/coverage-merge/record.json", "target/patch-coverage.md"):
            self.assertIn(f"            {path}\n", upload)
        self.assertNotIn("lcov.info", upload)

    def test_producers_publish_the_workspace_export_with_its_root_and_no_focused_report(self) -> None:
        for name, artifact in (("tests", "coverage-tests"), ("scenarios", "coverage-scenarios-${{ matrix.shard }}")):
            with self.subTest(job=name):
                job = job_section(self.workflow, name)
                upload = step_section(job, f"Upload {name} coverage")
                self.assertIn(f"name: {artifact}\n", upload)
                self.assertIn("            lcov-workspace.info\n", upload)
                self.assertIn("            coverage-source-root.txt\n", upload)
                self.assertNotIn("            lcov.info\n", upload)
                self.assertNotIn("pwd > coverage-source-root.txt", job)
        for recipe in (
            "test-coverage-phase",
            "test-scenarios-coverage-run",
            "test-scenarios-coverage-shard",
            "coverage-report-workspace",
        ):
            with self.subTest(recipe=recipe):
                body = recipe_body(self.justfile, recipe)
                self.assertIn("--no-default-ignore-filename-regex --lcov --output-path lcov-workspace.info", body)
                self.assertIn("> coverage-source-root.txt", body)
                self.assertNotIn("--output-path lcov.info", body)

    def test_the_merge_runs_through_one_recipe_and_ci_measures_its_own_tooling(self) -> None:
        self.assertIn("python3 -m scripts.coverage_merge", recipe_body(self.justfile, "coverage-merge"))
        runner = recipe_body(self.justfile, "coverage-merge-runner")
        self.assertIn("--source=scripts.coverage_merge,scripts.tests.test_coverage_merge", runner)
        self.assertIn("coverage-merge/python.lcov", runner)
        extras = job_section(self.workflow, "extra-tests")
        self.assertIn("run: just coverage-merge-runner\n", extras)
        self.assertIn("            target/coverage-merge/python.lcov\n", step_section(extras, "Upload native extra coverage"))


def recipe_body(justfile: str, name: str) -> str:
    """A recipe's header and its indented body."""
    header = re.search(rf"^{re.escape(name)}[ :].*$", justfile, re.MULTILINE)
    if header is None:
        raise AssertionError(f"no recipe {name}")
    lines = [header[0]]
    for line in justfile[header.end() + 1 :].splitlines():
        if line and not line.startswith(" "):
            break
        lines.append(line)
    return "\n".join(lines)


if __name__ == "__main__":
    unittest.main()
