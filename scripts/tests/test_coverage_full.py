"""Exercise the full coverage union: its selection, its validation of every producer, its
attribution and its CI contract.

Inputs are built the way the producer jobs publish them: the coverage job's merged ordinary report
and merge record, the native collections of the extra-tests, diagnostic-evidence and label-enabled
jobs with their model and lane evidence, the combined fuzz campaign and the Bolero call's selection
record.
"""

from __future__ import annotations

import hashlib
import io
import json
import re
import shlex
import shutil
import subprocess
import tempfile
import unittest
from collections.abc import Mapping, Sequence
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

from scripts import coverage_full, native_coverage
from scripts.coverage_full import JOBS, REQUIREMENTS, Provider, Selection
from scripts.deloxide_lane import BuildPass, Stage
from scripts.tests.test_coverage_merge import counters, lcov, recipe_body, sources
from scripts.tests.test_native_coverage import job_section, step_section

ROOT = coverage_full.ROOT
RUN = "77"
CI_ROOT = "/ci/runner/nervix"
TOOLCHAIN = {"rustc": "rustc 1.99.1 (58cf8f9c3 2026-09-01)", "commit": "58cf8f9c3", "host": "x86_64-unknown-linux-gnu", "llvm": "23.1.0"}
INVENTORY = '[tool]\nnightly = "nightly-2026-09-17"\n\n[[target]]\nid = "alpha-values"\n\n[[target]]\nid = "beta-values"\n'
TARGETS = ["alpha-values", "beta-values"]
FILES = {
    "src/lib.rs": 6,
    "src/runtime/mod.rs": 4,
    "crates/alpha/src/lib.rs": 4,
    "tests/runtime/restore.rs": 3,
}
# The ordinary report: test sources included, line 2 of src/lib.rs executable but never run.
ORDINARY_LINES = {
    "src/lib.rs": {1: 3, 2: 0, 3: 1},
    "crates/alpha/src/lib.rs": {1: 2, 2: 0},
    "tests/runtime/restore.rs": {1: 1},
}
LANE_MODES = {"test-deloxide": "deloxide", "test-deloxide-order": "deloxide-order", "test-deloxide-stress": "deloxide-stress", "test-deloxide-stress-restore": "deloxide-stress"}


def models(mode: str) -> dict[str, object]:
    """Canonical complete model evidence of one check, as the Shuttle and Loom runners write it."""
    check: dict[str, object] = {"package": "nervix-primitives", "test": f"{mode}::check"}
    if mode == "loom":
        check["invariant"] = "sync.publication"
        runs = [{"name": "exploration", "completed": True, "exit_status": 0, "executions": 12, "bounds": "preemption 2"}]
    else:
        runs = [
            {"name": "exploration", "completed": True, "exit_status": 0, "records": ["exploration.json"]},
            {"name": "nondeterminism", "completed": True, "exit_status": 0, "records": ["nondeterminism.json"]},
        ]
    return {
        "mode": mode, "filter": "", "inventory": "crates/model-harness/inventory.toml", "verdict": "complete",
        "discovery": [{**check, "ignored": False}], "selection": [check], "checks": [{**check, "runs": runs, "completed": True}],
        "discovered": 1, "selected": 1, "executed": 1, "completed": 1,
    }


def lane(selection: str) -> dict[str, object]:
    """A complete Deloxide lane record of one selection: its build and workload stages completed
    within their budgets, the instrumented workspace's initial and warm builds measured inside the
    build stage, and every selected workload completed with qualifying evidence."""
    stages = [
        {"name": Stage.BUILD, "verdict": "complete", "budget_seconds": 600, "duration_seconds": 2.0},
        {"name": Stage.WORKLOADS, "verdict": "complete", "budget_seconds": 600, "duration_seconds": 2.0},
    ]
    build_measurements = [
        {"kind": BuildPass.INITIAL, "duration_seconds": 0.5},
        {"kind": BuildPass.WARM, "duration_seconds": 0.5},
    ]
    return {
        "selection": selection, "verdict": "complete", "stages": stages,
        "workspace": {"instrumented": True}, "build_measurements": build_measurements,
        "counts": {"discovered": 2, "selected": 1, "executed": 1, "completed": 1},
        "workloads": [{"completed": True}], "findings": {"observations": 2, "qualifying": 2},
    }


def collecting_primitives(labels: Sequence[str]) -> list[str]:
    """The labels of a run that collects the primitive conformance, as most runs here do."""
    return sorted({*labels, coverage_full.PRIMITIVES_LABEL})


class Run:
    """A checkout at a tested revision and the artifacts one CI run of it downloaded."""

    def __init__(self, case: unittest.TestCase) -> None:
        directory = tempfile.TemporaryDirectory()
        case.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.git("init", "--quiet")
        self.git("config", "user.name", "Coverage Test")
        self.git("config", "user.email", "coverage@example.com")
        for path, length in FILES.items():
            text = "".join(f"line {number}\n" for number in range(1, length + 1))
            if path.startswith("tests/"):
                # A last line without its newline is still a line.
                text = text.rstrip("\n")
            self.write(path, text)
        self.write("tests/bolero-targets.toml", INVENTORY)
        self.git("add", ".")
        self.git("commit", "--quiet", "-m", "test: record sources")
        self.revision = self.git("rev-parse", "HEAD").strip()
        self.inputs = self.root / "coverage-full-inputs"
        self.directory = self.root / "target/coverage-full"

    def git(self, *arguments: str) -> str:
        return subprocess.run(["git", *arguments], cwd=self.root, check=True, capture_output=True, text=True).stdout

    def write(self, path: str, text: str) -> Path:
        destination = self.root / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(text)
        return destination

    def selection(
        self, labels: Sequence[str] = (), *, event: str = "pull_request", attempt: int = 1,
        jobs: Mapping[str, str] | None = None, tests_required: bool = True, bolero_required: bool = True,
    ) -> Selection:
        """Record the selection of a GitHub run with these labels, each required job successful."""
        selection = Selection(
            event=event, labels=tuple(collecting_primitives(labels)), revision=self.revision,
            provider=Provider.GITHUB,
            run=RUN, attempt=attempt, tests_required=tests_required, bolero_required=bolero_required, jobs={},
        )
        results = {}
        for job in JOBS:
            results[job] = "success" if job in selection.required_jobs() else "skipped"
        results.update(jobs or {})
        selection = Selection(**{**selection.__dict__, "jobs": results})
        self.directory.mkdir(parents=True, exist_ok=True)
        (self.directory / coverage_full.SELECTION).write_text(json.dumps(selection.describe(), indent=2))
        return selection

    def ordinary(
        self, records: Mapping[str, Mapping[int, int]] = ORDINARY_LINES, *, verdict: str = "complete",
        revision: str | None = None, merged: Mapping[str, str] | None = None, run: str = RUN,
    ) -> Path:
        """The coverage job's coverage-merged artifact: the merged report and its merge record, beside
        the patch coverage and CRAP reports the artifact also carries and the union does not read."""
        directory = self.inputs / "coverage-merged"
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "lcov-workspace.info").write_text(lcov(records))
        if merged is None:
            merged = {"bench-smoke": f"github-{RUN}-1"}
        reports = [{"report": "coverage-inputs/tests/lcov-workspace.info", "records": 3, "kept": 3, "dropped": {}}]
        for producer, attempt in merged.items():
            reports.append({
                "report": f"coverage-inputs/native-extras/native-coverage/{producer}/ordinary/rust-1.99.1/{attempt}/lcov.info",
                "producer": producer, "mode": "ordinary", "attempt": attempt,
                "revision": {"commit": self.revision, "modified": False},
                "run": {"provider": "github", "run": run, "attempt": "1", "job": "extra-tests"},
            })
        record = {
            "verdict": verdict, "report": "lcov-workspace.info", "revision": revision or self.revision,
            "inputs": [{"input": "coverage-inputs/native-extras", "root": "/ci/extras/nervix", "reports": reports, "left_out": []}],
            "members": {"measured": ["alpha", "server"], "unmeasured": []},
        }
        if verdict != "complete":
            record["failure"] = "the merged report has no record of the workspace members alpha"
        record_path = directory / "target/coverage-merge/record.json"
        record_path.parent.mkdir(parents=True, exist_ok=True)
        record_path.write_text(json.dumps(record))
        (directory / "target/patch-coverage.md").write_text("## Patch line coverage\n")
        crap = directory / "target/crap-report"
        crap.mkdir(parents=True, exist_ok=True)
        (crap / "crap.json").write_text(json.dumps({"verdict": "complete", "report": "lcov-workspace.info"}))
        (crap / "crap.md").write_text("## CRAP report\n")
        return directory

    def collection(
        self, artifact: str, producer: str, records: Mapping[str, Mapping[int, int]] | None = None, *,
        verdict: str = "complete", attempt: int = 1, run: str = RUN, revision: str | None = None,
        modified: bool = False, prefix: str = "native-coverage", filter_text: str = "", evidence: bool = True,
        absolute: bool = True,
    ) -> Path:
        """One native collection in an artifact: its completion record, report and evidence."""
        mode = native_coverage_mode(producer)
        name = f"github-{run}-{attempt}"
        directory = self.inputs / artifact / prefix / producer / mode / "rust-1.99.1" / name
        directory.mkdir(parents=True)
        content: dict[str, object] = {
            "producer": producer, "mode": mode, "verdict": verdict, "rerun": f"just coverage-native-extras {producer}",
            "revision": {"commit": revision or self.revision, "modified": modified},
            "run": {"provider": "github", "run": run, "attempt": str(attempt), "job": artifact},
            "attempt": name, "toolchain": TOOLCHAIN,
            "sources": {"root": CI_ROOT, "policy": {str(key): meaning for key, meaning in native_coverage.SOURCE_POLICY.items()}},
        }
        producer_definition = coverage_full.NATIVE_PRODUCERS[producer]
        if producer_definition.filterable:
            content["filter"] = filter_text
        if verdict != "complete":
            content["failure"] = {"stage": "run", "detail": "`just test-shuttle-checks` exited with status 101"}
        (directory / "completion.json").write_text(json.dumps(content))
        if records is None:
            records = {"crates/alpha/src/lib.rs": {1: 1}}
        if absolute:
            records = {f"{CI_ROOT}/{source}": lines for source, lines in records.items()}
        (directory / "lcov.info").write_text(lcov(records))
        if evidence and producer_definition.filterable:
            (directory / "models.json").write_text(json.dumps(models(mode)))
        if evidence and producer_definition.lane is not None:
            (directory / "lane.json").write_text(json.dumps(lane(producer_definition.lane)))
        return directory

    def campaign(
        self, records: Mapping[str, Mapping[int, int]] | None = None, *, labels: Sequence[str] = ("fuzz",),
        selection: Sequence[str] = TARGETS, scope: str | None = "combined shards", **changes: object,
    ) -> Path:
        """The combined live fuzz campaign: its record and its repository-relative report."""
        directory = self.inputs / "coverage-bolero-fuzz"
        directory.mkdir(parents=True)
        content: dict[str, object] = {
            "producer": "bolero-fuzz", "mode": "fuzz", "verdict": "complete", "tested_sha": self.revision,
            "inventory": {"file": "tests/bolero-targets.toml", "sha256": hashlib.sha256(INVENTORY.encode()).hexdigest()},
            "run": {"provider": "github", "run": RUN, "attempt": "1"},
            "revision": {"commit": self.revision, "modified": False}, "toolchain": TOOLCHAIN,
            "event": "pull_request", "labels": collecting_primitives(labels), "selection": list(selection),
            "counts": {key: len(selection) for key in ("discovered", "selected", "executed", "completed")},
        }
        if scope is not None:
            content["scope"] = scope
        content.update(changes)
        (directory / "completion.json").write_text(json.dumps(content))
        (directory / "lcov.info").write_text(lcov(records or {"src/runtime/mod.rs": {3: 7}}))
        return directory

    def bolero_selection(self, labels: Sequence[str] = (), /, **changes: object) -> Path:
        """The Bolero call's record of its randomized and sanitizer outcomes."""
        randomized = "bolero-random" in labels
        requested = "fuzz" in labels
        outcome = "success" if requested else "skipped"
        content: dict[str, object] = {
            "producer": "bolero-selection", "verdict": "complete", "tested_sha": self.revision,
            "run": RUN, "attempt": "1", "event": "pull_request", "labels": collecting_primitives(labels),
            "randomized": "success" if randomized else "skipped", "randomized_required": randomized,
            "sanitizer": outcome, "sanitizer_combined": outcome, "sanitizer_required": requested,
        }
        content.update(changes)
        directory = self.inputs / "bolero-selection"
        directory.mkdir(parents=True)
        (directory / "selection.json").write_text(json.dumps(content))
        return directory

    def complete(self, labels: Sequence[str] = ()) -> None:
        """Every input a run with these labels publishes, and its selection."""
        self.selection(labels)
        self.ordinary()
        extras = "coverage-native-extras"
        self.collection(extras, "bench-smoke", {"crates/alpha/src/lib.rs": {1: 9}})
        self.collection(extras, "test-primitives-shuttle", {"src/lib.rs": {2: 5, 3: 1}})
        self.collection(extras, "test-primitives-loom", {"src/runtime/mod.rs": {1: 1, 2: 0}})
        self.collection(extras, "test-primitives-turmoil", {"crates/alpha/src/lib.rs": {1: 1}})
        self.collection(extras, "test-primitives-deloxide", {"crates/alpha/src/lib.rs": {1: 1}})
        self.collection("diagnostic-evidence", "test-deadlock-evidence-order", {"src/lib.rs": {1: 1}})
        self.collection("diagnostic-evidence", "test-deadlock-report", {"src/lib.rs": {4: 1}})
        if "shuttle" in labels:
            self.collection("coverage-shuttle", "test-shuttle", {"src/lib.rs": {2: 1}}, prefix="")
        if "loom" in labels:
            self.collection("coverage-loom", "test-loom", {"src/runtime/mod.rs": {2: 3}}, prefix="")
        if "turmoil" in labels:
            self.collection("coverage-turmoil", "test-turmoil", {"crates/alpha/src/lib.rs": {2: 1}})
        if "deloxide" in labels:
            for producer in LANE_MODES:
                self.collection(f"deloxide-{producer.removeprefix('test-')}", producer, {"src/lib.rs": {1: 1}})
        if "fuzz" in labels:
            self.campaign(labels=labels)
        self.bolero_selection(labels)

    def union(self, *inputs: Path) -> tuple[int, dict[str, object], str, str]:
        """Run `just coverage-full` over these inputs, or over every downloaded artifact."""
        named = inputs or tuple(sorted(self.inputs.iterdir()))
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            status = coverage_full.main(
                ["union", "--selection", str(self.directory / coverage_full.SELECTION), "--directory", str(self.directory), *map(str, named)],
                root=self.root, environment={},
            )
        record = json.loads((self.directory / coverage_full.RECORD).read_text())
        return status, record, stdout.getvalue(), stderr.getvalue()

    def report(self) -> str:
        return (self.directory / coverage_full.REPORT).read_text()


def native_coverage_mode(producer: str) -> str:
    return coverage_full.NATIVE_PRODUCERS[producer].mode


def problems(record: Mapping[str, object]) -> str:
    return "\n".join(record["problems"])


class UnionTests(unittest.TestCase):
    def test_a_run_without_mode_labels_combines_the_ordinary_report_with_modeled_and_diagnostic_collections(
        self,
    ) -> None:
        run = Run(self)
        run.complete()
        ordinary_before = (run.inputs / "coverage-merged/lcov-workspace.info").read_text()
        status, record, stdout, _ = run.union()
        self.assertEqual(status, 0, problems(record))
        self.assertEqual(record["verdict"], "complete")
        report = run.report()
        self.assertEqual(sources(report), ["crates/alpha/src/lib.rs", "src/lib.rs", "src/runtime/mod.rs", "tests/runtime/restore.rs"])
        # Line 2 of src/lib.rs runs only under Shuttle; line 4 only in the deadlock report's own build.
        self.assertEqual(counters(report, "src/lib.rs"), {1: 4, 2: 5, 3: 2, 4: 1})
        self.assertEqual(counters(report, "src/runtime/mod.rs"), {1: 1, 2: 0})
        self.assertEqual(counters(report, "crates/alpha/src/lib.rs"), {1: 4, 2: 0})
        self.assertNotIn("FN", report)
        # The ordinary report stays as the coverage job published it.
        self.assertEqual((run.inputs / "coverage-merged/lcov-workspace.info").read_text(), ordinary_before)
        contributions = [contribution["producer"] for contribution in record["contributions"]]
        self.assertEqual(contributions, [
            "ordinary", "test-primitives-shuttle", "test-primitives-loom", "test-primitives-turmoil",
            "test-primitives-deloxide", "test-deadlock-evidence-order", "test-deadlock-report",
        ])
        self.assertEqual(record["fuzz"], {"skipped": "the pull request carries no fuzz label, so the sanitizer campaigns were deliberately skipped"})
        self.assertIn("## Full workspace coverage", stdout)
        self.assertEqual((run.directory / coverage_full.SUMMARY).read_text() + "Union: target/coverage-full/lcov-full.info; record: target/coverage-full/record.json\n", stdout)

    def test_mode_only_lines_are_attributed_to_the_contributions_that_covered_them(self) -> None:
        run = Run(self)
        run.complete()
        status, record, _, _ = run.union()
        self.assertEqual(status, 0, problems(record))
        by_producer = {contribution["producer"]: contribution["lines"] for contribution in record["contributions"]}
        self.assertEqual(by_producer["ordinary"], {"files": 3, "executable": 6, "covered": 4, "exclusive": 1, "beyond_ordinary": 0})
        self.assertEqual(by_producer["test-primitives-shuttle"], {"files": 1, "executable": 2, "covered": 2, "exclusive": 1, "beyond_ordinary": 1})
        self.assertEqual(record["totals"]["full"], {"files": 4, "executable": 9, "covered": 7})
        self.assertEqual(record["totals"]["beyond_ordinary"], {"files": 2, "covered": 3})
        self.assertEqual(record["totals"]["executable_beyond_ordinary"], 3)
        self.assertEqual(record["modes"]["shuttle"], {"covered": 2, "beyond_ordinary": 1})
        beyond = (run.directory / coverage_full.BEYOND_ORDINARY).read_text()
        sections = re.findall(r"^TN:(\S+)$", beyond, re.MULTILINE)
        self.assertEqual(sections, ["test-deadlock-report", "test-primitives-loom", "test-primitives-shuttle"])
        self.assertIn("TN:test-primitives-shuttle\nSF:src/lib.rs\nDA:2,5\nLF:1\nLH:1\nend_of_record\n", beyond)
        self.assertEqual(record["members"], {"measured": ["alpha", "server"], "unmeasured": []})

    def test_a_counter_that_wrapped_below_zero_counts_nothing_and_is_listed(self) -> None:
        # llvm-cov exports a count difference that concurrent threads drove below zero as its unsigned
        # wrap, here -1. Line 2 of the alpha crate is executable but never run in the ordinary report,
        # and line 4 of the runtime module is counted by the wrapped counter alone.
        wrapped = 2**64 - 1
        run = Run(self)
        run.complete()
        turmoil = next((run.inputs / "coverage-native-extras").rglob("test-primitives-turmoil/**/lcov.info"))
        turmoil.write_text(lcov({
            f"{CI_ROOT}/crates/alpha/src/lib.rs": {1: 1, 2: wrapped},
            f"{CI_ROOT}/src/runtime/mod.rs": {4: wrapped},
        }))
        status, record, stdout, _ = run.union()
        self.assertEqual(status, 0, problems(record))
        report = run.report()
        self.assertEqual(counters(report, "crates/alpha/src/lib.rs"), {1: 4, 2: 0})
        self.assertEqual(counters(report, "src/runtime/mod.rs"), {1: 1, 2: 0})
        by_producer = {contribution["producer"]: contribution for contribution in record["contributions"]}
        self.assertEqual(by_producer["test-primitives-turmoil"]["wrapped"], [
            {"source": "crates/alpha/src/lib.rs", "line": 2, "count": wrapped},
            {"source": "src/runtime/mod.rs", "line": 4, "count": wrapped},
        ])
        self.assertEqual(by_producer["test-primitives-turmoil"]["lines"]["executable"], 1)
        self.assertEqual(by_producer["ordinary"]["wrapped"], [])
        self.assertEqual(record["totals"]["wrapped_counters"], 2)
        self.assertEqual(record["totals"]["full"], {"files": 4, "executable": 9, "covered": 7})
        self.assertEqual(record["policy"]["wrapped"], coverage_full.WRAPPED_POLICY)
        self.assertIn("Counters that wrapped below zero, left out of the sum: 2 (test-primitives-turmoil 2).", stdout)

    def test_input_order_and_repeated_identical_artifacts_change_nothing(self) -> None:
        run = Run(self)
        run.complete()
        status, _, _, _ = run.union()
        self.assertEqual(status, 0)
        first = run.report()
        repeated = run.inputs / "coverage-native-extras-again"
        shutil.copytree(run.inputs / "coverage-native-extras", repeated)
        status, record, _, _ = run.union(*reversed(sorted(run.inputs.iterdir())))
        self.assertEqual(status, 0, problems(record))
        self.assertEqual(run.report(), first)
        self.assertEqual(record["totals"]["full"], {"files": 4, "executable": 9, "covered": 7})

    def test_every_label_enabled_producer_joins_the_union(self) -> None:
        run = Run(self)
        labels = ("shuttle", "loom", "turmoil", "deloxide", "fuzz")
        run.complete(labels)
        status, record, _, _ = run.union()
        self.assertEqual(status, 0, problems(record))
        contributions = [contribution["producer"] for contribution in record["contributions"]]
        for producer in ("test-shuttle", "test-loom", "test-turmoil", *LANE_MODES, "bolero-fuzz"):
            self.assertIn(producer, contributions)
        self.assertEqual(counters(run.report(), "src/runtime/mod.rs"), {1: 1, 2: 3, 3: 7})
        self.assertEqual(record["fuzz"]["targets"], 2)
        self.assertTrue(record["selection"]["fuzz_requested"])
        required = {item["producer"]: item["found"] is not None for item in record["required"]}
        self.assertTrue(all(required.values()))
        self.assertIn("bolero-selection", required)

    def test_a_required_ordinary_producer_the_ordinary_merge_read_counts_once(self) -> None:
        run = Run(self)
        run.complete()
        run.ordinary(merged={"bench-smoke": f"github-{RUN}-1", "test-deadlock-report": f"github-{RUN}-1"})
        status, record, _, _ = run.union()
        self.assertEqual(status, 0, problems(record))
        self.assertNotIn("test-deadlock-report", [contribution["producer"] for contribution in record["contributions"]])
        required = {item["producer"]: item for item in record["required"]}
        self.assertEqual(required["test-deadlock-report"]["merged_into"], "ordinary")


class SelectionTests(unittest.TestCase):
    def test_non_pr_events_keep_the_deliberate_fuzz_skip_even_with_a_fuzz_label(self) -> None:
        for event in ("push", "workflow_dispatch"):
            with self.subTest(event=event):
                run = Run(self)
                run.complete()
                run.selection(("fuzz",), event=event)
                path = run.inputs / "bolero-selection/selection.json"
                content = json.loads(path.read_text())
                # A Bolero run of any other event runs the randomized properties whatever its labels.
                content.update(
                    event=event, labels=collecting_primitives(["fuzz"]), randomized="success",
                    randomized_required=True,
                )
                path.write_text(json.dumps(content))
                status, record, _, _ = run.union()
                self.assertEqual(status, 0, problems(record))
                self.assertFalse(record["selection"]["fuzz_requested"])
                self.assertTrue(record["selection"]["randomized_requested"])
                self.assertNotIn("bolero-fuzz", [item["producer"] for item in record["contributions"]])

    def test_missing_label_enabled_producers_fail_naming_their_job_and_artifact(self) -> None:
        run = Run(self)
        run.complete(("shuttle",))
        shutil.rmtree(run.inputs / "coverage-shuttle")
        status, record, _, stderr = run.union()
        self.assertEqual(status, 1)
        self.assertIn(
            "test-shuttle (shuttle) is required, but the shuttle job's coverage-shuttle artifact is not among the inputs; the shuttle job ended success",
            problems(record),
        )
        self.assertFalse((run.directory / coverage_full.REPORT).exists())
        self.assertIn("## Full workspace coverage failed", stderr)

    def test_an_input_the_selection_does_not_enable_is_stale(self) -> None:
        run = Run(self)
        run.complete()
        run.collection("coverage-loom", "test-loom", prefix="")
        status, record, _, _ = run.union()
        self.assertEqual(status, 1)
        self.assertIn("is the collection of test-loom, which this run's selection does not enable: a stale or fabricated input", problems(record))

    def test_a_fuzz_report_without_the_fuzz_label_is_refused(self) -> None:
        run = Run(self)
        run.complete()
        run.campaign(labels=())
        status, record, _, _ = run.union()
        self.assertEqual(status, 1)
        self.assertIn("is the collection of bolero-fuzz, which this run's selection does not enable", problems(record))

    def test_requested_fuzzing_requires_successful_sanitizer_campaigns(self) -> None:
        run = Run(self)
        run.complete(("fuzz",))
        shutil.rmtree(run.inputs / "bolero-selection")
        run.bolero_selection(("fuzz",), sanitizer="skipped", sanitizer_combined="skipped")
        status, record, _, _ = run.union()
        self.assertEqual(status, 1)
        self.assertIn("records the requested sanitizer campaigns as ('skipped', 'skipped')", problems(record))

    def test_an_unrequested_sanitizer_campaign_must_be_a_deliberate_skip(self) -> None:
        run = Run(self)
        run.complete()
        shutil.rmtree(run.inputs / "bolero-selection")
        run.bolero_selection((), sanitizer="success", sanitizer_combined="success")
        status, record, _, _ = run.union()
        self.assertEqual(status, 1)
        self.assertIn("records unrequested sanitizer campaigns as ('success', 'success')", problems(record))

    def test_requested_randomized_properties_must_pass(self) -> None:
        for result in ("failure", "cancelled", "skipped"):
            with self.subTest(result=result):
                run = Run(self)
                run.complete(("bolero-random",))
                shutil.rmtree(run.inputs / "bolero-selection")
                run.bolero_selection(("bolero-random",), randomized=result)
                status, record, _, _ = run.union()
                self.assertEqual(status, 1)
                self.assertIn(f"records the requested randomized properties as {result}", problems(record))

    def test_requested_randomized_properties_join_the_bolero_selection(self) -> None:
        run = Run(self)
        run.complete(("bolero-random",))
        status, record, _, _ = run.union()
        self.assertEqual(status, 0, problems(record))
        self.assertTrue(record["selection"]["randomized_requested"])
        self.assertFalse(record["selection"]["fuzz_requested"])

    def test_the_bolero_selection_must_record_a_deliberate_randomized_skip_of_this_snapshot(self) -> None:
        cases = {
            "verdict": ({"verdict": "failed"}, "the Bolero selection is failed, not complete"),
            "randomized": ({"randomized": "success"}, "records unrequested randomized properties as success"),
            "cancelled": ({"randomized": "cancelled"}, "records unrequested randomized properties as cancelled"),
            "randomized-required": ({"randomized_required": True}, "records the randomized properties as required: True"),
            "labels": ({"labels": ["shuttle"]}, "with labels ['shuttle'], not for this run's selection"),
            "run": ({"run": "76"}, "comes from run 76, not from this run 77"),
            "required": ({"sanitizer_required": True}, "records the sanitizer campaigns as required: True"),
        }
        for name, (changes, expected) in cases.items():
            with self.subTest(case=name):
                run = Run(self)
                run.complete()
                shutil.rmtree(run.inputs / "bolero-selection")
                run.bolero_selection((), **changes)
                status, record, _, _ = run.union()
                self.assertEqual(status, 1)
                self.assertIn(expected, problems(record))

    def test_a_required_job_that_did_not_succeed_fails_the_union(self) -> None:
        for result in ("failure", "cancelled", "skipped"):
            with self.subTest(result=result):
                run = Run(self)
                run.complete(("turmoil",))
                run.selection(("turmoil",), jobs={"turmoil": result})
                status, record, _, _ = run.union()
                self.assertEqual(status, 1)
                self.assertIn(f"the turmoil job ended {result}, so its coverage is not complete evidence of this run", problems(record))

    def test_the_primitive_collections_need_their_label(self) -> None:
        selection = Selection(
            event="pull_request", labels=(), revision="a" * 40, provider=Provider.GITHUB, run=RUN,
            attempt=1, tests_required=True, bolero_required=False, jobs={},
        )
        producers = {requirement.producer for requirement in selection.required()}
        self.assertFalse({name for name in producers if name.startswith("test-primitives")})
        self.assertNotIn("extra-tests", selection.required_jobs())

    def test_the_shared_job_may_succeed_for_a_label_of_another_step(self) -> None:
        run = Run(self)
        run.complete()
        run.selection((), jobs={"extra-tests": "success"})
        status, record, _, _ = run.union()
        self.assertEqual(status, 0, problems(record))

    def test_a_job_the_selection_does_not_require_must_have_been_skipped(self) -> None:
        run = Run(self)
        run.complete()
        run.selection((), jobs={"loom": "success"})
        status, record, _, _ = run.union()
        self.assertEqual(status, 1)
        self.assertIn("the loom job ended success, although this run's selection does not require it", problems(record))

    def test_a_run_without_suites_has_nothing_to_combine(self) -> None:
        run = Run(self)
        run.complete()
        run.selection(tests_required=False, bolero_required=False)
        status, record, _, _ = run.union()
        self.assertEqual(status, 1)
        self.assertIn("the selection required no suites", problems(record))

    def test_a_selection_of_another_revision_fails_before_reading_inputs(self) -> None:
        run = Run(self)
        run.complete()
        run.write("src/extra.rs", "fn extra() {}\n")
        run.git("add", ".")
        run.git("commit", "--quiet", "-m", "test: move on")
        status, record, _, _ = run.union()
        self.assertEqual(status, 1)
        self.assertIn("not at the revision the selection tested", record["failure"])
        self.assertNotIn("contributions", record)

    def test_a_missing_or_malformed_selection_fails(self) -> None:
        run = Run(self)
        run.complete()
        (run.directory / coverage_full.SELECTION).write_text(json.dumps({"producer": "coverage-full-selection"}))
        status, record, _, _ = run.union()
        self.assertEqual(status, 1)
        self.assertIn("lacks its event, labels, revision, run, jobs or requirements", record["failure"])
        (run.directory / coverage_full.SELECTION).unlink()
        status, record, _, _ = run.union()
        self.assertEqual(status, 1)
        self.assertIn("record it with just coverage-full-select", record["failure"])


class ValidationTests(unittest.TestCase):
    def failing(self, mutate, expected: str, labels: Sequence[str] = ()) -> None:
        run = Run(self)
        run.complete(labels)
        mutate(run)
        status, record, _, _ = run.union()
        self.assertEqual(status, 1)
        self.assertIn(expected, problems(record))
        self.assertFalse((run.directory / coverage_full.REPORT).exists())

    def replace_collection(self, run: Run, artifact: str, producer: str, /, **changes: object) -> None:
        for directory in (run.inputs / artifact).rglob(producer):
            shutil.rmtree(directory)
        run.collection(artifact, producer, **changes)

    def test_failed_and_incomplete_collections_name_their_stage_and_rerun(self) -> None:
        for verdict in ("failed", "interrupted", "running"):
            with self.subTest(verdict=verdict):
                self.failing(
                    lambda run, verdict=verdict: self.replace_collection(run, "coverage-shuttle", "test-shuttle", verdict=verdict, prefix=""),
                    f"the collection of test-shuttle is {verdict}, not complete",
                    ("shuttle",),
                )

    def test_a_collection_of_another_run_attempt_or_revision_is_refused(self) -> None:
        cases = {
            "run": ({"run": "76"}, "comes from run 76, not from this run 77"),
            "attempt": ({"attempt": 2}, "comes from attempt 2, after this attempt 1 of run 77"),
            "revision": ({"revision": "0" * 40}, f"describes {'0' * 40}, not the tested revision"),
            "modified": ({"modified": True}, "was collected from a modified working tree"),
        }
        for name, (changes, expected) in cases.items():
            with self.subTest(case=name):
                self.failing(
                    lambda run, changes=changes: self.replace_collection(run, "coverage-native-extras", "test-primitives-loom", **changes),
                    expected,
                )

    def test_a_collection_of_an_earlier_attempt_is_this_runs(self) -> None:
        run = Run(self)
        run.complete()
        # A rerun of failed jobs keeps every other job's verdict and artifacts of attempt 1.
        run.selection(attempt=2)
        status, record, _, _ = run.union()
        self.assertEqual(status, 0, problems(record))
        self.assertEqual(record["problems"], [])

    def test_two_different_collections_of_one_producer_conflict(self) -> None:
        def add_second(run: Run) -> None:
            run.collection("coverage-native-extras-again", "test-primitives-turmoil", {"src/lib.rs": {1: 1}})

        self.failing(add_second, "test-primitives-turmoil has more than one different record")

    def test_a_directory_that_names_another_producer_than_its_record_is_refused(self) -> None:
        def misplace(run: Run) -> None:
            source = next((run.inputs / "coverage-native-extras").rglob("test-primitives-loom"))
            source.rename(source.with_name("test-primitives-elsewhere"))

        self.failing(misplace, "lies in a directory that names")

    def test_a_filtered_or_evidence_less_model_collection_is_refused(self) -> None:
        self.failing(
            lambda run: self.replace_collection(run, "coverage-shuttle", "test-shuttle", filter_text="cancellation", prefix=""),
            "selected only the checks matching 'cancellation', not the canonical inventory",
            ("shuttle",),
        )
        self.failing(
            lambda run: self.replace_collection(run, "coverage-loom", "test-loom", evidence=False, prefix=""),
            "has no complete canonical model evidence",
            ("loom",),
        )

    def test_a_lane_collection_without_its_complete_lane_record_is_refused(self) -> None:
        self.failing(
            lambda run: self.replace_collection(run, "deloxide-deloxide-order", "test-deloxide-order", evidence=False),
            "has no complete lane record",
            ("deloxide",),
        )

    def test_a_collection_kept_by_another_source_policy_is_refused(self) -> None:
        def change_policy(run: Run) -> None:
            completion = next((run.inputs / "diagnostic-evidence").rglob("test-deadlock-report")) / "ordinary/rust-1.99.1" / f"github-{RUN}-1" / "completion.json"
            content = json.loads(completion.read_text())
            content["sources"]["policy"] = {"included": "every file"}
            completion.write_text(json.dumps(content))

        self.failing(change_policy, "kept its sources by another policy than the union reads")

    def test_an_incomplete_ordinary_merge_or_one_of_another_revision_is_refused(self) -> None:
        self.failing(lambda run: run.ordinary(verdict="failed"), "the ordinary merge is failed, not complete")
        self.failing(lambda run: run.ordinary(revision="1" * 40), f"the ordinary report describes {'1' * 40}, not the tested revision")
        self.failing(lambda run: run.ordinary(run="76"), "the ordinary collection bench-smoke it merged comes from run 76")

    def test_an_ordinary_collection_the_ordinary_report_did_not_merge_is_refused(self) -> None:
        self.failing(lambda run: run.ordinary(merged={}), "is the ordinary collection bench-smoke at attempt github-77-1, which the ordinary report did not merge")

    def test_a_report_that_describes_another_revision_is_refused(self) -> None:
        self.failing(
            lambda run: self.replace_collection(run, "coverage-native-extras", "test-primitives-turmoil", records={"src/runtime/mod.rs": {9: 1}}),
            "counts line 9 of src/runtime/mod.rs, which has 4 lines at the tested revision",
        )
        self.failing(
            lambda run: run.ordinary({"src/lib.rs": {1: 1}, "src/removed.rs": {1: 1}}),
            "names src/removed.rs, a file the tested revision does not track",
        )

    def test_records_below_none_of_the_recorded_roots_are_refused(self) -> None:
        def move_root(run: Run) -> None:
            completion = next((run.inputs / "coverage-native-extras").rglob("test-primitives-shuttle")) / "shuttle/rust-1.99.1" / f"github-{RUN}-1" / "completion.json"
            content = json.loads(completion.read_text())
            content["sources"]["root"] = "/elsewhere/nervix"
            completion.write_text(json.dumps(content))

        self.failing(move_root, "none of its 1 records lies below its checkout root")

    def test_corrupt_reports_and_records_are_refused(self) -> None:
        def corrupt_report(run: Run) -> None:
            report = next((run.inputs / "diagnostic-evidence").rglob("lcov.info"))
            report.write_text("SF:/ci/runner/nervix/src/lib.rs\nDA:one,1\nend_of_record\n")

        self.failing(corrupt_report, "is not a valid LCOV report")

        def corrupt_record(run: Run) -> None:
            completion = next((run.inputs / "coverage-native-extras").rglob("completion.json"))
            completion.write_text("{")

        self.failing(corrupt_record, "cannot be read")

        def foreign_record(run: Run) -> None:
            completion = next((run.inputs / "coverage-native-extras").rglob("completion.json"))
            completion.write_text(json.dumps({"complete": True}))

        self.failing(foreign_record, "is not a coverage record: it names no producer")

        def no_verdict(run: Run) -> None:
            completion = next((run.inputs / "coverage-native-extras").rglob("completion.json"))
            content = json.loads(completion.read_text())
            del content["verdict"]
            completion.write_text(json.dumps(content))

        self.failing(no_verdict, "without a verdict")

    def test_a_native_report_without_its_completion_record_is_refused(self) -> None:
        def orphan(run: Run) -> None:
            stray = run.inputs / "coverage-native-extras/native-coverage/stray"
            stray.mkdir(parents=True)
            (stray / "lcov.info").write_text(lcov({"src/lib.rs": {1: 1}}))

        self.failing(orphan, "is a native report without a completion record beside it")

    def test_an_unknown_producer_is_refused(self) -> None:
        def unknown(run: Run) -> None:
            directory = run.inputs / "coverage-loom-qualification/test-loom-qualification/loom/rust-1.99.1/github-77-1"
            directory.mkdir(parents=True)
            (directory / "completion.json").write_text(json.dumps({"producer": "test-loom-qualification", "mode": "loom", "verdict": "complete"}))

        self.failing(unknown, "is the record of test-loom-qualification, which is no coverage producer the union reads")


class CampaignTests(unittest.TestCase):
    def test_a_newly_registered_target_joins_the_required_campaign_automatically(self) -> None:
        run = Run(self)
        inventory = INVENTORY + '\n[[target]]\nid = "gamma-values"\n'
        run.write("tests/bolero-targets.toml", inventory)
        run.git("add", "tests/bolero-targets.toml")
        run.git("commit", "--quiet", "-m", "test: register another property")
        run.revision = run.git("rev-parse", "HEAD").strip()
        run.complete(("fuzz",))
        path = run.inputs / "coverage-bolero-fuzz/completion.json"
        content = json.loads(path.read_text())
        content["inventory"]["sha256"] = hashlib.sha256(inventory.encode()).hexdigest()
        path.write_text(json.dumps(content))
        status, record, _, _ = run.union()
        self.assertEqual(status, 1)
        self.assertIn("not the 3 targets of the tested inventory", problems(record))
        self.assertFalse((run.directory / coverage_full.REPORT).exists())

        content["selection"] = [*TARGETS, "gamma-values"]
        content["counts"] = {key: 3 for key in ("discovered", "selected", "executed", "completed")}
        path.write_text(json.dumps(content))
        status, record, _, _ = run.union()
        self.assertEqual(status, 0, problems(record))
        self.assertEqual(record["fuzz"]["targets"], 3)

    def failing(self, expected: str, **changes: object) -> None:
        run = Run(self)
        run.complete(("fuzz",))
        shutil.rmtree(run.inputs / "coverage-bolero-fuzz")
        run.campaign(**changes)
        status, record, _, _ = run.union()
        self.assertEqual(status, 1)
        self.assertIn(expected, problems(record))

    def test_the_campaign_must_run_every_registered_target_once(self) -> None:
        self.failing("selected ['alpha-values'], not the 2 targets of the tested inventory", selection=["alpha-values"])
        self.failing(
            "not every one of the 2 registered targets executed and completed once",
            counts={"discovered": 2, "selected": 2, "executed": 2, "completed": 1},
        )
        self.failing("ran another inventory than the tested revision's", inventory={"file": "tests/bolero-targets.toml", "sha256": "0" * 64})

    def test_the_campaign_must_be_one_whole_combined_campaign_of_this_run(self) -> None:
        self.failing("is one shard's campaign; just combine-bolero-fuzz combines the shards", scope=None, shard={"number": 1, "count": 2})
        self.failing("is one target's record", scope=None, target={"id": "alpha-values"})
        self.failing("is no combination of this run's shards", scope=None)
        self.failing("tested", tested_sha="2" * 40)
        self.failing(f"describes {'4' * 40}, not the tested revision", revision={"commit": "4" * 40, "modified": False})
        self.failing("comes from run 76", run={"provider": "github", "run": "76", "attempt": "1"})
        self.failing("the fuzz campaign is failed, not complete", verdict="failed")


class InputTests(unittest.TestCase):
    def test_missing_inputs_and_unreadable_artifacts_are_named(self) -> None:
        run = Run(self)
        run.complete()
        status, record, _, _ = run.union(*sorted(run.inputs.iterdir()), run.inputs / "absent")
        self.assertEqual(status, 1)
        self.assertIn("coverage-full-inputs/absent does not exist", problems(record))
        empty = run.inputs / "empty"
        empty.mkdir()
        (empty / "notes.txt").write_text("nothing\n")
        status, record, _, _ = run.union()
        self.assertEqual(status, 1)
        self.assertIn("coverage-full-inputs/empty holds no ordinary report", problems(record))

    def test_the_checkout_itself_is_no_input(self) -> None:
        run = Run(self)
        run.complete()
        status, record, _, _ = run.union(run.root)
        self.assertEqual(status, 1)
        self.assertIn("is a checkout, which holds every attempt ever collected in it", problems(record))

    def test_reports_and_records_can_be_named_one_by_one(self) -> None:
        run = Run(self)
        run.complete()
        named = [run.inputs / "coverage-merged/lcov-workspace.info", run.inputs / "bolero-selection/selection.json"]
        named.extend(sorted(run.inputs.rglob("completion.json")))
        status, record, _, _ = run.union(*named)
        self.assertEqual(status, 0, problems(record))
        stray = run.write("coverage-full-inputs/notes.txt", "nothing\n")
        status, record, _, _ = run.union(*named, stray)
        self.assertEqual(status, 1)
        self.assertIn("notes.txt is not a report or record the union reads", problems(record))

    def test_an_ordinary_report_without_its_merge_record_is_refused(self) -> None:
        run = Run(self)
        run.complete()
        (run.inputs / "coverage-merged/target/coverage-merge/record.json").unlink()
        status, record, _, _ = run.union()
        self.assertEqual(status, 1)
        self.assertIn("has no merge record beside it at target/coverage-merge/record.json", problems(record))

    def test_a_selection_record_of_another_producer_is_refused(self) -> None:
        run = Run(self)
        run.complete()
        shutil.copy(run.directory / coverage_full.SELECTION, run.inputs / "bolero-selection/selection.json")
        status, record, _, _ = run.union()
        self.assertEqual(status, 1)
        self.assertIn("is the record of coverage-full-selection, not of the Bolero call's selection", problems(record))

    def test_the_step_summary_receives_the_breakdown(self) -> None:
        run = Run(self)
        run.complete()
        summary = run.root / "step-summary.md"
        with redirect_stdout(io.StringIO()):
            status = coverage_full.main(
                ["union", "--selection", str(run.directory / coverage_full.SELECTION), "--directory", str(run.directory), *map(str, sorted(run.inputs.iterdir()))],
                root=run.root, environment={"GITHUB_STEP_SUMMARY": str(summary)},
            )
        self.assertEqual(status, 0)
        self.assertEqual(summary.read_text(), (run.directory / coverage_full.SUMMARY).read_text())
        self.assertIn("| test-primitives-shuttle | shuttle | extra-tests | 1 | 2 | 2 | 1 | 1 |", summary.read_text())
        self.assertIn("Lines covered beyond the ordinary report: 3 in 2 files", summary.read_text())


class SelectCommandTests(unittest.TestCase):
    def select(self, run: Run, arguments: Sequence[str], environment: Mapping[str, str]) -> tuple[int, str]:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            status = coverage_full.main(["select", *arguments], root=run.root, environment=environment)
        return status, stdout.getvalue() + stderr.getvalue()

    def test_a_github_selection_records_the_snapshot_and_every_producer_result(self) -> None:
        run = Run(self)
        needs = {job: {"result": "success", "outputs": {}} for job in JOBS}
        needs["test-selection"] = {"result": "success", "outputs": {"tests-required": "true"}}
        environment = {"GITHUB_ACTIONS": "true", "GITHUB_SHA": run.revision, "GITHUB_RUN_ID": RUN, "GITHUB_RUN_ATTEMPT": "2"}
        status, output = self.select(run, [
            "--event", "pull_request", "--labels", '["shuttle", "fuzz", "extra-primitives"]',
            "--tests-required", "true", "--bolero-required", "true", "--jobs", json.dumps(needs),
        ], environment)
        self.assertEqual(status, 0, output)
        recorded = json.loads((run.directory / coverage_full.SELECTION).read_text())
        self.assertEqual(recorded["labels"], ["extra-primitives", "fuzz", "shuttle"])
        self.assertEqual(recorded["run"], {"provider": "github", "run": RUN, "attempt": 2})
        self.assertEqual(recorded["revision"], run.revision)
        self.assertTrue(recorded["fuzz_requested"])
        self.assertEqual(recorded["jobs"]["shuttle"], "success")
        self.assertIn("required: ordinary, test-primitives-shuttle", output)
        self.assertIn("test-shuttle, bolero-fuzz", output)
        loaded = Selection.load(run.directory / coverage_full.SELECTION)
        self.assertEqual(loaded.attempt, 2)
        self.assertEqual([requirement.producer for requirement in loaded.required()][-2:], ["test-shuttle", "bolero-fuzz"])

    def test_a_github_selection_needs_every_producer_job_and_the_tested_checkout(self) -> None:
        run = Run(self)
        environment = {"GITHUB_ACTIONS": "true", "GITHUB_SHA": run.revision, "GITHUB_RUN_ID": RUN, "GITHUB_RUN_ATTEMPT": "1"}
        arguments = ["--event", "pull_request", "--tests-required", "true", "--bolero-required", "true"]
        status, output = self.select(run, [*arguments, "--jobs", json.dumps({"coverage": {"result": "success"}})], environment)
        self.assertEqual(status, 1)
        self.assertIn("coverage-full must need every producer job", output)
        status, output = self.select(run, arguments, environment)
        self.assertEqual(status, 1)
        self.assertIn("needs --event, --tests-required, --bolero-required and --jobs", output)
        status, output = self.select(run, [*arguments, "--jobs", "{}"], {**environment, "GITHUB_SHA": "3" * 40})
        self.assertEqual(status, 1)
        self.assertIn("not at the tested revision", output)
        status, output = self.select(run, [*arguments, "--jobs", "{}"], {**environment, "GITHUB_RUN_ATTEMPT": ""})
        self.assertEqual(status, 1)
        self.assertIn("did not name the run and its attempt", output)

    def test_malformed_labels_jobs_and_flags_are_refused(self) -> None:
        run = Run(self)
        cases = {
            "labels": (["--labels", "shuttle"], "the labels must be a JSON list of names"),
            "labels list": (["--labels", '["shuttle", "shuttle"]'], "names a label twice"),
            "labels type": (["--labels", '[1]'], "the labels must be a JSON list of names"),
            "jobs": (["--jobs", "[]"], "the jobs must be the JSON object of the needs context"),
            "jobs json": (["--jobs", "{"], "the jobs must be the JSON object of the needs context"),
            "job result": (["--jobs", '{"coverage": {}}'], "records no result of the coverage job"),
            "flag": (["--tests-required", "yes"], "--tests-required must be true or false"),
        }
        for name, (arguments, expected) in cases.items():
            with self.subTest(case=name):
                status, output = self.select(run, arguments, {})
                self.assertEqual(status, 1)
                self.assertIn(expected, output)

    def test_a_local_selection_combines_local_collections(self) -> None:
        run = Run(self)
        status, output = self.select(run, ["--labels", '["shuttle", "extra-primitives"]'], {})
        self.assertEqual(status, 0, output)
        recorded = json.loads((run.directory / coverage_full.SELECTION).read_text())
        self.assertEqual(recorded["run"], {"provider": "local", "run": None, "attempt": None})
        self.assertEqual((recorded["event"], recorded["tests_required"], recorded["bolero_required"]), ("local", True, False))
        run.ordinary(merged={})
        for producer, artifact in (
            ("test-primitives-shuttle", "extras"), ("test-primitives-loom", "extras"), ("test-primitives-turmoil", "extras"),
            ("test-primitives-deloxide", "extras"), ("test-deadlock-evidence-order", "diagnostic"),
            ("test-deadlock-report", "diagnostic"), ("test-shuttle", "shuttle"),
        ):
            directory = run.collection(artifact, producer)
            completion = directory / "completion.json"
            content = json.loads(completion.read_text())
            content["run"] = {"provider": "local"}
            content["attempt"] = "local-20261016T000000.000000Z-1"
            target = directory.with_name(content["attempt"])
            completion.write_text(json.dumps(content))
            directory.rename(target)
        status, record, _, _ = run.union()
        self.assertEqual(status, 0, problems(record))
        self.assertEqual(record["selection"]["run"]["provider"], "local")
        self.assertIn("local run at", (run.directory / coverage_full.SUMMARY).read_text())

    def test_a_local_selection_refuses_ci_collections(self) -> None:
        run = Run(self)
        run.complete()
        status, _ = self.select(run, [], {})
        self.assertEqual(status, 0)
        status, record, _, _ = run.union()
        self.assertEqual(status, 1)
        self.assertIn("comes from a github run, while the selection is a local one", problems(record))
        self.assertIn("is a Bolero selection, although this run did not call Bolero", problems(record))


class EdgeTests(unittest.TestCase):
    """The rarer ways a record can be malformed, each named rather than read."""

    def union_with(self, mutate, labels: Sequence[str] = ()) -> tuple[int, dict[str, object]]:
        run = Run(self)
        run.complete(labels)
        mutate(run)
        status, record, _, _ = run.union()
        return status, record

    def edit(self, path: Path, change) -> None:
        content = json.loads(path.read_text())
        change(content)
        path.write_text(json.dumps(content))

    def completion(self, run: Run, producer: str) -> Path:
        for path in run.inputs.rglob("completion.json"):
            if json.loads(path.read_text()).get("producer") == producer:
                return path
        raise AssertionError(f"no collection of {producer}")

    def test_malformed_run_identities_are_named(self) -> None:
        cases = {
            "no run": (lambda content: content.pop("run"), "records no run"),
            "provider": (lambda content: content["run"].update(provider="buildkite"), "comes from a buildkite run, not from GitHub Actions run 77"),
            "attempt": (lambda content: content["run"].update(attempt="first"), "names the attempt github-77-1, not github-77-first"),
            "revision": (lambda content: content.update(revision="c0ffee"), "records no revision"),
            "root": (lambda content: content["sources"].pop("root"), "records no absolute checkout root of its sources"),
            "mode": (lambda content: content.update(mode="shuttle"), "lies in a directory that names"),
        }
        for name, (change, expected) in cases.items():
            with self.subTest(case=name):
                status, record = self.union_with(lambda run, change=change: self.edit(self.completion(run, "test-primitives-loom"), change))
                self.assertEqual(status, 1)
                self.assertIn(expected, problems(record))

    def test_a_bolero_selection_without_an_attempt_is_refused(self) -> None:
        status, record = self.union_with(lambda run: self.edit(run.inputs / "bolero-selection/selection.json", lambda content: content.update(attempt="first")))
        self.assertEqual(status, 1)
        self.assertIn("the Bolero selection records no attempt of run 77", problems(record))

    def test_a_collection_in_its_directory_but_another_mode_or_attempt_is_refused(self) -> None:
        def mode(run: Run) -> None:
            completion = self.completion(run, "test-primitives-loom")
            self.edit(completion, lambda content: content.update(mode="shuttle"))
            moved = completion.parents[3] / "shuttle"
            completion.parents[2].rename(moved)

        status, record = self.union_with(mode)
        self.assertEqual(status, 1)
        self.assertIn("ran in shuttle, while test-primitives-loom is collected in loom", problems(record))

        def attempt(run: Run) -> None:
            completion = self.completion(run, "test-primitives-loom")
            self.edit(completion, lambda content: content.update(attempt="github-77-9"))
            completion.parent.rename(completion.parent.with_name("github-77-9"))

        status, record = self.union_with(attempt)
        self.assertEqual(status, 1)
        self.assertIn("names the attempt github-77-9, not github-77-1 of the run it records", problems(record))

    def test_a_record_too_shallow_for_its_attempt_directory_is_refused(self) -> None:
        run = Run(self)
        run.complete()
        full = coverage_full.FullCoverage(run.root, coverage_full.Checkout.load(run.root), Selection.load(run.directory / coverage_full.SELECTION))
        record = coverage_full.Record(Path("loom/completion.json"), {"producer": "test-primitives-loom", "mode": "loom"}, "identity")
        problem = full.layout_problem(record, coverage_full.NATIVE_PRODUCERS["test-primitives-loom"])
        self.assertEqual(problem, "lies outside a <producer>/<mode>/<toolchain>/<attempt> directory")

    def test_complete_records_without_their_reports_are_refused(self) -> None:
        def remove(run: Run) -> None:
            (self.completion(run, "test-primitives-turmoil").parent / "lcov.info").unlink()

        status, record = self.union_with(remove)
        self.assertEqual(status, 1)
        self.assertIn("the complete collection of test-primitives-turmoil holds no lcov.info", problems(record))

        def remove_campaign(run: Run) -> None:
            (run.inputs / "coverage-bolero-fuzz/lcov.info").unlink()

        status, record = self.union_with(remove_campaign, ("fuzz",))
        self.assertEqual(status, 1)
        self.assertIn("the complete fuzz campaign holds no lcov.info", problems(record))

    def test_a_campaign_of_another_mode_or_an_unreadable_inventory_is_refused(self) -> None:
        status, record = self.union_with(lambda run: self.edit(run.inputs / "coverage-bolero-fuzz/completion.json", lambda content: content.update(mode="ordinary")), ("fuzz",))
        self.assertEqual(status, 1)
        self.assertIn("the fuzz campaign ran in ordinary, not in fuzz", problems(record))
        status, record = self.union_with(lambda run: (run.root / "tests/bolero-targets.toml").write_text("[[target]]\nname = 1\n"), ("fuzz",))
        self.assertEqual(status, 1)
        self.assertIn("cannot be checked against the tested inventory tests/bolero-targets.toml", problems(record))

    def test_malformed_ordinary_merge_records_are_named(self) -> None:
        record_path = Path("coverage-merged/target/coverage-merge/record.json")
        cases = {
            "report": (lambda content: content.update(report="lcov.info"), "is the record of lcov.info, not of the ordinary lcov-workspace.info"),
            "inputs": (lambda content: content.pop("inputs"), "lists no inputs"),
            "reports": (lambda content: content["inputs"][0].update(reports="none"), "lists an input without its reports"),
        }
        for name, (change, expected) in cases.items():
            with self.subTest(case=name):
                status, record = self.union_with(lambda run, change=change: self.edit(run.inputs / record_path, change))
                self.assertEqual(status, 1)
                self.assertIn(expected, problems(record))
        status, record = self.union_with(lambda run: (run.inputs / record_path).write_text("[]"))
        self.assertEqual(status, 1)
        self.assertIn("record.json is not a JSON object", problems(record))
        status, record = self.union_with(lambda run: (run.inputs / record_path).write_text("{"))
        self.assertEqual(status, 1)
        self.assertIn("record.json cannot be read", problems(record))

    def test_a_report_of_untracked_files_only_contributes_nothing(self) -> None:
        def untracked(run: Run) -> None:
            report = self.completion(run, "test-primitives-deloxide").parent / "lcov.info"
            report.write_text(lcov({f"{CI_ROOT}/src/generated.rs": {1: 1}}))

        status, record = self.union_with(untracked)
        self.assertEqual(status, 1)
        self.assertIn("contributes no record of a file the tested revision tracks", problems(record))

    def test_a_file_named_twice_is_read_once(self) -> None:
        run = Run(self)
        run.complete()
        named = sorted(run.inputs.iterdir())
        status, record, _, _ = run.union(*named, run.inputs / "bolero-selection/selection.json")
        self.assertEqual(status, 0, problems(record))

    def test_a_selection_that_names_no_known_run_is_refused(self) -> None:
        cases = {
            "producer": (lambda content: content.update(producer="ci-test-selection"), "is not a selection that just coverage-full-select recorded"),
            "provider": (lambda content: content["run"].update(provider="buildkite"), "names no known run provider"),
            "attempt": (lambda content: content["run"].update(attempt=0), "names no GitHub Actions run and attempt"),
        }
        for name, (change, expected) in cases.items():
            with self.subTest(case=name):
                run = Run(self)
                run.complete()
                self.edit(run.directory / coverage_full.SELECTION, change)
                status, record, _, _ = run.union()
                self.assertEqual(status, 1)
                self.assertIn(expected, record["failure"])

    def test_a_local_campaign_over_the_whole_inventory_joins_a_local_union(self) -> None:
        run = Run(self)
        local = Selection(
            event="local", labels=("fuzz",), revision=run.revision, provider=Provider.LOCAL, run=None, attempt=None,
            tests_required=True, bolero_required=False, jobs={},
        )
        self.assertFalse(local.fuzz_requested())
        pull = Selection(**{**local.__dict__, "event": "pull_request"})
        self.assertTrue(pull.fuzz_requested())
        full = coverage_full.FullCoverage(run.root, coverage_full.Checkout.load(run.root), pull)
        run.campaign(scope=None, shard=None, run={"provider": "local"}, tested_sha=None)
        record = coverage_full.Record(run.inputs / "coverage-bolero-fuzz/completion.json", json.loads((run.inputs / "coverage-bolero-fuzz/completion.json").read_text()), "identity")
        full.campaign(REQUIREMENTS[-1], record)
        self.assertEqual(full.problems, [])
        self.assertEqual(full.fuzz["targets"], 2)

    def test_a_missing_producer_of_a_local_selection_names_no_job_result(self) -> None:
        run = Run(self)
        local = Selection(
            event="local", labels=(), revision=run.revision, provider=Provider.LOCAL, run=None, attempt=None,
            tests_required=True, bolero_required=False, jobs={},
        )
        full = coverage_full.FullCoverage(run.root, coverage_full.Checkout.load(run.root), local)
        self.assertIsNone(full.one(REQUIREMENTS[1], []))
        self.assertEqual(full.problems, ["test-primitives-shuttle (shuttle) is required, but the extra-tests job's coverage-native-extras artifact is not among the inputs"])

    def test_a_percentage_of_nothing_is_not_a_number(self) -> None:
        self.assertEqual(coverage_full.percent(0, 0), "n/a")
        self.assertEqual(coverage_full.percent(1, 3), "33.33%")


class WorkflowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workflow = (ROOT / ".github/workflows/check.yaml").read_text()
        self.justfile = (ROOT / "justfile").read_text()

    def test_each_union_download_keeps_the_artifact_in_its_own_directory(self) -> None:
        job = job_section(self.workflow, "coverage-full")
        for step in job.split("\n      - name: "):
            if "uses: actions/download-artifact" not in step:
                continue
            inputs = step.split("with:", 1)[1]
            artifact = re.search(r"pattern: (\S+)", inputs)[1]
            self.assertNotRegex(artifact, r"[?*\[\]{}]", "each download selects one exact artifact")
            self.assertIn(f"path: coverage-full-inputs/{artifact}\n", inputs)

    def test_the_union_job_waits_for_every_producer_job_and_runs_unless_cancelled(self) -> None:
        job = job_section(self.workflow, "coverage-full")
        needs = re.search(r"needs: \[(.*)\]", job)[1].split(", ")
        self.assertEqual(sorted(needs), sorted(["test-selection", *JOBS]))
        self.assertIn(
            "if: ${{ !cancelled() && (needs.test-selection.result != 'success' || "
            "needs.test-selection.outputs.tests-required == 'true') }}", job,
        )
        self.assertNotIn("cargo", job)
        self.assertNotIn("pull-requests: write", job)

    def test_missing_artifacts_reach_the_union_with_the_run_selection_preserved(self) -> None:
        job = job_section(self.workflow, "coverage-full")
        selection = job.index("      - name: Record the event, labels and producer results")
        download = job.index("uses: actions/download-artifact")
        self.assertLess(selection, download)
        for step in job.split("\n      - name: "):
            if "uses: actions/download-artifact" in step:
                self.assertRegex(step, r"\n          pattern: \S+")
        extras = step_section(job, "Download native extra coverage")
        self.assertIn("if: needs.extra-tests.outputs.coverage-required == 'true'", extras)
        combine = step_section(job, "Combine coverage")
        self.assertIn("if: ${{ !cancelled() }}", combine)

    def test_the_union_job_downloads_every_artifact_the_requirements_name(self) -> None:
        job = job_section(self.workflow, "coverage-full")
        downloaded = set()
        for step in job.split("\n      - name: "):
            if "uses: actions/download-artifact" not in step:
                continue
            match = re.search(r"(?:name|pattern): (\S+)", step.split("with:")[1])
            downloaded.add(match[1])
        expected = {requirement.artifact for requirement in (*REQUIREMENTS, coverage_full.SELECTION_RECORD)}
        lane_job = job_section(self.workflow, "deloxide")
        selections = re.search(r"selection: \[(.*)\]", lane_job)[1].split(", ")
        self.assertEqual({f"deloxide-{selection}" for selection in selections}, {name for name in expected if name.startswith("deloxide-")})
        self.assertEqual(downloaded, expected)
        for step in job.split("\n      - name: "):
            if "pattern:" in step:
                artifact = re.search(r"pattern: (\S+)", step)[1]
                self.assertIn(f"path: coverage-full-inputs/{artifact}\n", step)

    def test_the_union_job_records_the_snapshot_then_combines_through_the_recipes(self) -> None:
        job = job_section(self.workflow, "coverage-full")
        select = step_section(job, "Record the event, labels and producer results")
        self.assertIn("LABELS: ${{ toJSON(github.event.pull_request.labels.*.name) }}", select)
        self.assertIn("JOBS: ${{ toJSON(needs) }}", select)
        self.assertIn("just coverage-full-select --event", select)
        self.assertIn("run: just coverage-full coverage-full-inputs/*\n", step_section(job, "Combine coverage"))
        upload = step_section(job, "Upload full coverage")
        self.assertIn("if: always()", upload)
        for name in (coverage_full.REPORT, coverage_full.BEYOND_ORDINARY, coverage_full.RECORD, coverage_full.SELECTION, coverage_full.SUMMARY):
            self.assertIn(f"            target/coverage-full/{name}\n", upload)
        self.assertIn("python3 -m scripts.coverage_full select", recipe_body(self.justfile, "coverage-full-select"))
        self.assertIn("python3 -m scripts.coverage_full union", recipe_body(self.justfile, "coverage-full"))

    def test_the_recipes_pass_every_argument_through_intact(self) -> None:
        # GitHub's toJSON writes the label snapshot and the needs context across several lines.
        labels = json.dumps(["no-docker", "fuzz"], indent=2)
        jobs = json.dumps({job: {"result": "success", "outputs": {}} for job in JOBS}, indent=2)
        cases = {
            "coverage-full-select": ["--event", "pull_request", "--labels", labels, "--jobs", jobs],
            "coverage-full": ["coverage-full-inputs/with space", "--selection", "retained/selection.json"],
        }
        for recipe, arguments in cases.items():
            with self.subTest(recipe=recipe):
                dry_run = subprocess.run(
                    ["just", "--dry-run", recipe, *arguments], cwd=ROOT, capture_output=True, text=True, timeout=120,
                )
                self.assertEqual(dry_run.returncode, 0, dry_run.stderr)
                command = shlex.split(dry_run.stderr)
                self.assertEqual(command[:3], ["python3", "-m", "scripts.coverage_full"])
                self.assertEqual(command[-len(arguments):], arguments)

    def test_each_label_enabled_job_collects_its_producers_into_the_artifact_the_union_reads(self) -> None:
        for requirement in REQUIREMENTS:
            if requirement.label is None or requirement.producer == coverage_full.FUZZ_PRODUCER:
                continue
            with self.subTest(producer=requirement.producer):
                job = job_section(self.workflow, requirement.job)
                self.assertIn(f"contains(github.event.pull_request.labels.*.name, '{requirement.label}')", job)
                if requirement.job == "extra-tests":
                    self.assertIn("run: just coverage-native-extras test-primitives\n", job)
                    self.assertIn(f"name: {requirement.artifact}\n", job)
                elif requirement.job == "deloxide":
                    selection = requirement.producer.removeprefix("test-")
                    self.assertRegex(job, rf"selection: \[[^\]]*\b{re.escape(selection)}\b")
                    self.assertIn('just coverage-native-extras "test-${SELECTION}"', job)
                    self.assertIn("name: deloxide-${{ matrix.selection }}", job)
                else:
                    self.assertIn(f"just coverage-native-extras {requirement.producer}", job)
                    self.assertIn(f"name: {requirement.artifact}\n", job)

    def test_the_always_required_producers_come_from_their_jobs(self) -> None:
        extras = job_section(self.workflow, "extra-tests")
        self.assertIn("run: just coverage-native-extras test-primitives\n", extras)
        self.assertIn("name: coverage-native-extras", extras)
        diagnostic = job_section(self.workflow, "diagnostic-evidence")
        self.assertIn("run: just coverage-deadlock", diagnostic)
        self.assertIn("name: diagnostic-evidence", diagnostic)
        self.assertIn("name: coverage-merged", job_section(self.workflow, "coverage"))
        bolero = (ROOT / ".github/workflows/bolero.yaml").read_text()
        self.assertIn("name: coverage-bolero-fuzz\n", bolero)
        self.assertIn("name: bolero-selection\n", bolero)
        self.assertIn("contains(fromJSON(inputs.labels-json || '[]'), 'fuzz')", bolero)

    def test_the_coverage_job_retains_the_ordinary_report_and_merge_record_the_union_reads(self) -> None:
        upload = step_section(job_section(self.workflow, "coverage"), "Upload merged coverage")
        self.assertIn("name: coverage-merged\n", upload)
        for path in (coverage_full.WORKSPACE_REPORT, coverage_full.MERGE_RECORD):
            self.assertIn(f"            {path}\n", upload)

    def test_ci_measures_the_unions_own_tooling(self) -> None:
        runner = recipe_body(self.justfile, "coverage-full-runner")
        self.assertIn("--source=scripts.coverage_full,scripts.tests.test_coverage_full", runner)
        self.assertIn("coverage-full/python.lcov", runner)
        extras = job_section(self.workflow, "extra-tests")
        self.assertIn("run: just coverage-full-runner\n", extras)
        self.assertIn("            target/coverage-full/python.lcov\n", step_section(extras, "Upload native extra coverage"))

    def test_every_producer_job_retains_the_load_of_its_runner(self) -> None:
        # The ordinary report's producer jobs, every job of this workflow a requirement names, and
        # the qualification jobs whose budgets the same measurements size.
        coverage = job_section(self.workflow, "coverage")
        jobs = set(re.search(r"needs: \[(.*)\]", coverage)[1].split(", "))
        for requirement in REQUIREMENTS:
            if requirement.job not in ("coverage", "bolero"):
                jobs.add(requirement.job)
        jobs.update(("loom-qualification", "client-conformance"))
        artifacts = []
        for name in sorted(jobs):
            with self.subTest(job=name):
                job = job_section(self.workflow, name)
                if "python3 scripts/record_runner_load.py" in job:
                    # A matrix job's entries each retain their own load under the entry's value.
                    if "\n      matrix:\n" in job:
                        matrix_artifact = re.search(r"name: (runner-load-\$\{\{ github\.job \}\}-\$\{\{ matrix\.[a-z]+ \}\})\n", job)
                        self.assertIsNotNone(matrix_artifact, "a matrix job names its load after its entry")
                        artifacts.append(matrix_artifact[1].replace("${{ github.job }}", name))
                        continue
                    self.assertIn("name: runner-load-${{ github.job }}\n", job)
                    artifacts.append(f"runner-load-{name}")
                    continue
                start = step_section(job, "Start recording runner load")
                self.assertIn("uses: ./.github/actions/runner-load\n", start)
                self.assertIn("phase: start\n", start)
                finish = step_section(job, "Publish runner load")
                self.assertIn("if: always()\n", finish)
                self.assertIn("phase: finish\n", finish)
                self.assertLess(job.index(start), job.index(finish))
                artifact = re.search(r"artifact-name: (.+)\n", finish)[1]
                self.assertTrue(artifact.startswith("runner-load-"), artifact)
                if "\n      matrix:\n" in job:
                    self.assertIn("${{ matrix.", artifact)
                artifacts.append(artifact)
        self.assertEqual(len(artifacts), len(set(artifacts)), artifacts)

    def test_every_required_native_producer_is_collected_in_its_declared_mode(self) -> None:
        for requirement in REQUIREMENTS:
            if requirement.producer in (coverage_full.ORDINARY_REPORT, coverage_full.FUZZ_PRODUCER):
                continue
            with self.subTest(producer=requirement.producer):
                self.assertIn(requirement.producer, coverage_full.NATIVE_PRODUCERS)
                self.assertEqual(requirement.mode, coverage_full.NATIVE_PRODUCERS[requirement.producer].mode)
        lanes = {requirement.producer for requirement in REQUIREMENTS if requirement.job == "deloxide"}
        self.assertEqual(lanes, {producer.name for producer in native_coverage.PRODUCERS if producer.lane is not None})


if __name__ == "__main__":
    unittest.main()
