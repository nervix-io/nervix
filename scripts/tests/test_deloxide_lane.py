"""Tests of the Deloxide diagnostic lane: its inventory, discovery, supervision, accounting, record,
replay, qualification classes, applicability check and the CI and recipe contracts it runs under.

The lane's own commands run against a scripted double of its processes; supervision runs real
processes, because what it must prove is how real processes end and what they leave behind.
"""

from __future__ import annotations

import json
import os
import re
import signal
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest
from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from pathlib import Path

from scripts import deloxide_lane
from scripts.deloxide_lane import (
    Ended,
    Ending,
    EvidenceCounts,
    Failure,
    Inventory,
    Kind,
    Lane,
    LaneError,
    Launch,
    Processes,
    Qualified,
    Workspace,
)

REPOSITORY = Path(__file__).resolve().parents[2]

INVENTORY = textwrap.dedent(
    """
    [lane]
    budget_seconds = 600
    stop_grace_seconds = 1
    suite_teardown_reserve_seconds = 10

    [[selection]]
    name = "deloxide"
    recorded = "ActiveOnly"

    [[selection]]
    name = "deloxide-order"
    recorded = "OrderAnalysis"

    [[invocation]]
    artifacts = true
    bound_seconds = 60
    id = "probes"
    ignored = ["workload"]
    kind = "libtest"
    package = "nervix-deadlock"
    target = "test:active_cycles"

    [[invocation]]
    bound_seconds = 60
    evidence = true
    features = ["testing"]
    id = "owner-tests"
    kind = "libtest-each"
    marker = "deloxide_"
    package = "nervix-server"
    target = "lib"

    [[invocation]]
    bound_seconds = 120
    concurrency = 4
    evidence = true
    features = ["testing"]
    id = "scenarios"
    inputs = ["features/**/*.feature"]
    kind = "cucumber"
    package = "nervix-server"
    tags = ["@lane"]
    target = "test:scenarios"

    [[workload]]
    coverage = "Two tracked mutexes."
    id = "probe.cycle"
    invariant = "A cycle is reported."
    invocation = "probes"
    selections = ["deloxide", "deloxide-order"]
    test = "cycle_reports"

    [[workload]]
    coverage = "Historical order."
    id = "probe.order"
    invariant = "Order is retained."
    invocation = "probes"
    selections = ["deloxide-order"]
    test = "order_reports"

    [[workload]]
    coverage = "The store's locks."
    id = "owner.store"
    invariant = "The store records no finding."
    invocation = "owner-tests"
    selections = ["deloxide", "deloxide-order"]
    test = "store::tests::deloxide_store"

    [[workload]]
    coverage = "Every node lock."
    examples = 2
    feature = "features/lane.feature"
    id = "scenario.nodes"
    invariant = "Nodes record no finding."
    invocation = "scenarios"
    scenario = "Nodes on <nodes> nodes"
    selections = ["deloxide", "deloxide-order"]

    [[owner]]
    path = "src/store.rs"
    workloads = ["owner.store", "scenario.nodes"]

    [[owner]]
    gap = "Client locks run outside the selected workloads."
    path = "crates/client/src/lib.rs"
    """
)

# Checks that run one per process and record no evidence, as the tracked locks' conformance checks
# of the primitive crate do.
CONFORMANCE = textwrap.dedent(
    """
    [[invocation]]
    bound_seconds = 30
    features = ["native"]
    id = "conformance"
    kind = "libtest-each"
    marker = "tests::tracked_locks::"
    package = "nervix-primitives"
    target = "lib"

    [[workload]]
    coverage = "Debug formatting of a held lock."
    id = "primitive.debug"
    invariant = "Formatting never waits."
    invocation = "conformance"
    selections = ["deloxide", "deloxide-order"]
    test = "tests::tracked_locks::debug_never_waits"
    """
)

FEATURE = textwrap.dedent(
    """
    @lane
    Feature: Lane
      Scenario Outline: Nodes on <nodes> nodes
        Given a step
        Examples:
          | nodes |
          | 1     |
          | 3     |
    """
)

CLEAN_SUMMARY = (
    "evidence summary: scope=whole-process findings=0 active=0 potential=0 unreviewed=0 "
    "nonqualifying=0 repeated-deliveries=0 lost-handoff=0 lost-order-history=0 lost-retention=0\n"
)


def summary_line(**counts: int) -> str:
    values = {
        "findings": 0, "active": 0, "potential": 0, "unreviewed": 0, "nonqualifying": 0,
        "repeated-deliveries": 0, "lost-handoff": 0, "lost-order-history": 0, "lost-retention": 0,
    }
    for key, value in counts.items():
        values[key.replace("_", "-")] = value
    pairs = " ".join(f"{key}={value}" for key, value in values.items())
    return f"evidence summary: scope=whole-process {pairs}\n"


def libtest(outcomes: Mapping[str, str], filtered: int = 0) -> str:
    lines = [f"running {len(outcomes)} tests"]
    for name, outcome in outcomes.items():
        lines.append(f"test {name} ... {outcome}")
    passed = sum(outcome == "ok" for outcome in outcomes.values())
    failed = sum(outcome == "FAILED" for outcome in outcomes.values())
    ignored = sum(outcome == "ignored" for outcome in outcomes.values())
    verdict = "ok" if failed == 0 else "FAILED"
    lines.append(
        f"test result: {verdict}. {passed} passed; {failed} failed; {ignored} ignored; 0 measured; "
        f"{filtered} filtered out; finished in 0.01s"
    )
    return "\n".join(lines) + "\n"


def artifact(kind: str, name: str, executable: Path, manifest: Path, test: bool = True) -> str:
    return json.dumps({
        "reason": "compiler-artifact",
        "target": {"kind": [kind], "name": name},
        "profile": {"test": test},
        "executable": str(executable),
        "manifest_path": str(manifest),
    })


class ScriptedProcesses(Processes):
    """The lane's processes, scripted: builds print their artifacts, test runs print libtest's
    report and record evidence, and the report tool answers from the evidence file's name."""

    def __init__(self, root: Path, selection: str = "deloxide") -> None:
        super().__init__({}, 1.0)
        self.root = root
        self.selection = selection
        self.launches: list[Launch] = []
        self.captures: list[list[str]] = []
        self.listed: dict[str, list[str]] = {
            "probes": ["cycle_reports", "workload"],
            "server": ["store::tests::deloxide_store", "store::tests::ordinary", "workload"],
        }
        if selection == "deloxide-order":
            self.listed["probes"].append("order_reports")
        self.listed["primitives"] = ["tests::tracked_locks::debug_never_waits", "tests::atomics::ordinary"]
        self.listed_ignored: dict[str, list[str]] = {"probes": ["workload"], "server": [], "primitives": []}
        self.endings: dict[str, Ended] = {}
        self.outputs: dict[str, str] = {}
        self.evidence: dict[str, list[str]] = {
            "owner-tests-owner.store": ["deadlock-1-1.rkyv"],
            "scenarios": ["deadlock-2-1.rkyv", "process-clusters/cluster-a/deadlock-3-1.rkyv"],
        }
        self.scenario_summary = "2 scenarios (2 passed)"
        self.scenario_summaries: dict[str, str] = {}

    def executable(self, name: str) -> Path:
        return self.root / "bin" / name

    def build_messages(self, name: str) -> str:
        if name == "probes-build":
            return artifact("test", "active_cycles", self.executable("probes"), self.root / "crates/deadlock/Cargo.toml")
        if name == "owner-tests-build":
            return artifact("lib", "nervix_server", self.executable("server"), self.root / "Cargo.toml")
        if name == "conformance-build":
            return artifact("lib", "nervix_primitives", self.executable("primitives"), self.root / "crates/primitives/Cargo.toml")
        if name == "scenarios-build":
            lines = [
                artifact("bin", "nervix-server", self.executable("nervix-server"), self.root / "Cargo.toml", test=False),
                artifact("test", "scenarios", self.executable("scenarios"), self.root / "Cargo.toml"),
            ]
            return "\n".join(lines)
        raise AssertionError(f"unexpected build {name}")

    def run(self, launch: Launch) -> Ended:
        self.launches.append(launch)
        launch.log.parent.mkdir(parents=True, exist_ok=True)
        if launch.stdout is not None:
            launch.stdout.write_text(self.build_messages(launch.name) + "\n")
            launch.log.write_text("Compiling\n")
        elif launch.name in self.outputs:
            launch.log.write_text(self.outputs[launch.name])
        elif launch.name == "probes":
            outcomes = {"cycle_reports": "ok", "workload": "ignored"}
            if self.selection == "deloxide-order":
                outcomes["order_reports"] = "ok"
            launch.log.write_text(libtest(outcomes))
        elif launch.name.startswith("owner-tests-"):
            launch.log.write_text(libtest({"store::tests::deloxide_store": "ok"}, filtered=2))
        elif launch.name.startswith("conformance-"):
            launch.log.write_text(libtest({"tests::tracked_locks::debug_never_waits": "ok"}, filtered=1))
        elif launch.name == "scenarios" or launch.name.startswith("scenarios-"):
            summary = self.scenario_summaries.get(launch.name, self.scenario_summary)
            launch.log.write_text(f"[Summary]\n1 feature\n{summary}\n4 steps (4 passed)\n")
        directory = launch.environment.get(deloxide_lane.EVIDENCE_VARIABLE)
        if directory is not None:
            evidence = self.evidence.get(launch.name, [])
            if launch.name.startswith("scenarios-"):
                evidence = self.evidence.get("scenarios", [])
            for name in evidence:
                path = Path(directory) / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b"evidence")
        return self.endings.get(launch.name, Ended(4242, Ending.EXITED, 0, None, 0.5))

    def capture(self, argv: Sequence[str], cwd: Path, environment: Mapping[str, str] | None = None) -> tuple[int, str, str]:
        arguments = [str(part) for part in argv]
        self.captures.append(arguments)
        if arguments[:2] == ["git", "rev-parse"]:
            return 0, "0123abcd\n", ""
        if arguments[:2] == ["git", "status"]:
            return 0, "", ""
        if arguments == ["rustc", "-vV"]:
            return 0, "rustc 1.99.0\nhost: x86_64-unknown-linux-gnu\n", ""
        if arguments == ["cargo", "-V"]:
            return 0, "cargo 1.99.0\n", ""
        if arguments[:2] == ["bash", "scripts/download_onnxruntime.sh"]:
            return 0, "/opt/onnxruntime/libonnxruntime.so\n", ""
        if arguments[1:2] == ["--list"]:
            executable = Path(arguments[0]).name
            if "--ignored" in arguments:
                names = self.listed_ignored[executable]
            else:
                names = self.listed[executable]
            return 0, "".join(f"{name}: test\n" for name in names), ""
        if arguments[1:2] == ["qualify"]:
            name = Path(arguments[2]).name
            if "active" in name:
                return 5, summary_line(findings=1, active=1, nonqualifying=1) + "cannot qualify\n", ""
            if "unreviewed" in name:
                return 5, summary_line(findings=1, potential=1, unreviewed=1, nonqualifying=1, repeated_deliveries=3) + "cannot qualify\n", ""
            if "lost" in name:
                return 5, summary_line(findings=1, nonqualifying=1, lost_retention=2) + "cannot qualify\n", ""
            return 0, CLEAN_SUMMARY + "diagnostic evidence qualifies\n", ""
        if arguments[1:2] == ["inspect"]:
            return 0, "finding 0: nervix deadlock detector: active deadlock among 2 threads\n", ""
        raise AssertionError(f"unexpected capture {arguments}")


class Fixture:
    """A repository with an inventory, a feature file, Cargo.lock and the prepared report tool."""

    def __init__(self, test: unittest.TestCase, inventory: str = INVENTORY) -> None:
        directory = tempfile.TemporaryDirectory()
        test.addCleanup(directory.cleanup)
        self.root = Path(directory.name).resolve()
        (self.root / "tests").mkdir()
        (self.root / deloxide_lane.INVENTORY).write_text(inventory)
        (self.root / "features").mkdir()
        (self.root / "features/lane.feature").write_text(FEATURE)
        (self.root / "Cargo.lock").write_text(
            'version = 4\n\n[[package]]\nname = "deloxide"\nversion = "1.1.0"\n'
            'source = "registry+https://github.com/rust-lang/crates.io-index"\n'
        )
        self.target = self.root / "target"
        tool = self.target / "debug" / "nervix-deadlock-report"
        tool.parent.mkdir(parents=True)
        tool.write_text("")
        self.inventory = deloxide_lane.load_inventory(self.root)

    def workspace(self, environment: Mapping[str, str] | None = None) -> Workspace:
        return deloxide_lane.resolve_workspace(
            self.root, self.target, environment or {}, "x86_64-unknown-linux-gnu"
        )

    def lane(
        self,
        processes: ScriptedProcesses,
        selection: str = "deloxide",
        report: Path | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> Lane:
        return Lane(
            self.inventory,
            self.inventory.selection(selection),
            self.workspace(),
            processes,
            report,
            clock,
        )


def record(lane: Lane) -> dict[str, object]:
    return json.loads(lane.record.path.read_text())


def quietly(function: Callable[[], int]) -> int:
    with open(os.devnull, "w") as sink:
        stdout, stderr = sys.stdout, sys.stderr
        sys.stdout, sys.stderr = sink, sink
        try:
            return function()
        finally:
            sys.stdout, sys.stderr = stdout, stderr


class InventoryTests(unittest.TestCase):
    def test_the_repository_inventory_registers_every_selection_and_workload(self) -> None:
        inventory = deloxide_lane.load_inventory(REPOSITORY)
        self.assertEqual(list(inventory.selections), ["deloxide", "deloxide-order"])
        self.assertEqual(
            list(inventory.invocations),
            ["probes", "primitive-conformance", "owner-tests", "scenarios", "paced-simulation"],
        )
        self.assertEqual(inventory.selections["deloxide"].recorded, "ActiveOnly")
        self.assertEqual(inventory.selections["deloxide-order"].recorded, "OrderAnalysis")
        for workload in inventory.workloads.values():
            with self.subTest(workload=workload.id):
                self.assertTrue(workload.invariant.endswith("."), workload.invariant)
                self.assertTrue(workload.coverage.endswith("."), workload.coverage)
        order_only = {
            workload.id
            for workload in inventory.workloads.values()
            if workload.selections == {"deloxide-order"}
        }
        self.assertEqual(len(inventory.workloads_of("probes", "deloxide")), 14)
        self.assertEqual(len(inventory.workloads_of("probes", "deloxide-order")), 22)
        self.assertEqual(len(order_only), 8)
        for path, owner in inventory.owners.items():
            with self.subTest(owner=path):
                self.assertTrue((REPOSITORY / path).is_file(), path)
                self.assertTrue(owner.workloads or owner.gap)

    def test_the_inventory_registers_exactly_the_lane_tagged_scenarios(self) -> None:
        inventory = deloxide_lane.load_inventory(REPOSITORY)
        for invocation in inventory.invocations.values():
            if invocation.kind is not Kind.CUCUMBER:
                continue
            with self.subTest(invocation=invocation.id):
                discovered = deloxide_lane.discover_scenarios(REPOSITORY, invocation.inputs, invocation.tags)
                registered = inventory.workloads_of(invocation.id, "deloxide")
                self.assertEqual(deloxide_lane.scenario_problems(invocation, registered, discovered), [])
                self.assertGreater(sum(scenario.runs for scenario in discovered), 0)

    def test_order_scenarios_have_fresh_feature_and_large_example_processes(self) -> None:
        inventory = deloxide_lane.load_inventory(REPOSITORY)
        invocation = inventory.invocations["scenarios"]
        registered = inventory.workloads_of("scenarios", "deloxide-order")
        active = deloxide_lane.scenario_chunks(REPOSITORY, invocation, registered, "deloxide")
        self.assertEqual(len(active), 1)
        self.assertEqual(active[0].expected, sum(workload.examples or 0 for workload in registered))
        order = deloxide_lane.scenario_chunks(REPOSITORY, invocation, registered, "deloxide-order")
        self.assertEqual(sum(chunk.expected for chunk in order), active[0].expected)
        self.assertEqual(len({chunk.name for chunk in order}), len(order))
        self.assertEqual(
            {chunk.example_tag for chunk in order if chunk.example_tag is not None},
            {
                "@order_generation_two_saves_single", "@order_generation_two_saves_cluster",
                "@order_generation_many_saves_single", "@order_generation_many_saves_cluster",
                "@order_resume_fanout_single", "@order_resume_fanout_cluster",
                "@order_resume_tenants_single", "@order_resume_tenants_cluster",
                "@order_materialized_fanout", "@order_materialized_tenants",
            },
        )
        self.assertTrue(all(chunk.expected == 1 for chunk in order if chunk.example_tag))
        generation = [
            chunk for chunk in order
            if chunk.inputs == ("tests/features/cluster/backup_generation.feature",)
        ]
        self.assertEqual(len(generation), 4)
        self.assertTrue(all(chunk.expected == 1 and chunk.example_tag for chunk in generation))
        named = next(chunk for chunk in order if chunk.name_filter is not None)
        named_arguments = deloxide_lane.scenario_arguments(named, invocation.concurrency)
        self.assertEqual(named_arguments.count("--name"), 1)
        self.assertNotIn("--tags", named_arguments)
        self.assertIn("--retry", named_arguments)
        tagged = next(chunk for chunk in order if chunk.example_tag is not None)
        tagged_arguments = deloxide_lane.scenario_arguments(tagged, invocation.concurrency)
        self.assertEqual(tagged_arguments.count("--tags"), 1)
        self.assertNotIn("--name", tagged_arguments)
        remote = [chunk for chunk in order if chunk.inputs == ("tests/features/runtime/remote_ack_owners.feature",)]
        self.assertEqual(len(remote), 1)
        self.assertEqual(remote[0].expected, 2)
        bad = [
            replace(workload, order_tags=("@missing_example", *workload.order_tags[1:]))
            if workload.id == "scenario.materialized-bulk-generations" else workload
            for workload in registered
        ]
        with self.assertRaisesRegex(LaneError, "order tag @missing_example must select exactly one example"):
            deloxide_lane.scenario_chunks(REPOSITORY, invocation, bad, "deloxide-order")

    def test_the_fixture_inventory_parses(self) -> None:
        inventory = deloxide_lane.parse_inventory(INVENTORY)
        probes = inventory.invocations["probes"]
        self.assertIs(probes.kind, Kind.LIBTEST)
        self.assertTrue(probes.artifacts)
        self.assertEqual(probes.cargo_target(), ["--test", "active_cycles"])
        self.assertEqual(inventory.invocations["owner-tests"].cargo_target(), ["--lib"])
        self.assertEqual(
            [workload.id for workload in inventory.workloads_of("probes", "deloxide-order")],
            ["probe.cycle", "probe.order"],
        )
        self.assertEqual([workload.id for workload in inventory.workloads_of("probes", "deloxide")], ["probe.cycle"])
        with self.assertRaisesRegex(LaneError, "no selection `loom`; the inventory selects deloxide, deloxide-order"):
            inventory.selection("loom")
        bad_tags = INVENTORY.replace(
            'id = "scenario.nodes"', 'id = "scenario.nodes"\n    order_tags = ["@single"]', 1
        )
        with self.assertRaisesRegex(LaneError, "order_tags.*one per run"):
            deloxide_lane.parse_inventory(bad_tags)

    def test_every_malformed_entry_is_refused_naming_it(self) -> None:
        def edited(old: str, new: str) -> str:
            self.assertIn(old, INVENTORY)
            return INVENTORY.replace(old, new, 1)

        cases = {
            "an unknown table": (INVENTORY + "\n[extra]\nkey = 1\n", "the inventory has unknown keys: extra"),
            "no lane": (INVENTORY.replace("[lane]", "[lanes]", 1), "unknown keys: lanes"),
            "a zero budget": (edited("budget_seconds = 600", "budget_seconds = 0"), r"\[lane\] needs a positive integer `budget_seconds`"),
            "a mode as a selection": (edited('name = "deloxide"\nrecorded', 'name = "loom"\nrecorded'), "`loom` is not a diagnostic build feature"),
            "a selection twice": (edited('name = "deloxide-order"', 'name = "deloxide"'), "selection deloxide is registered twice"),
            "an unknown kind": (edited('kind = "libtest"', 'kind = "doctest"'), "invocation probes: `kind` is not one of"),
            "a malformed target": (edited('target = "test:active_cycles"', 'target = "bench:x"'), "neither `lib` nor `test:<name>`"),
            "a mode feature": (edited('features = ["testing"]\nid = "owner-tests"', 'features = ["shuttle"]\nid = "owner-tests"'), "names the execution mode `shuttle`"),
            "one process per test without its marker": (edited('marker = "deloxide_"\n', ""), "needs the `marker` its tests carry"),
            "scenarios without tags": (edited('tags = ["@lane"]\n', ""), "needs `inputs` and `tags`"),
            "scenarios without a concurrency": (edited("concurrency = 4\n", ""), "needs a positive integer `concurrency`"),
            "a libtest concurrency": (edited('id = "probes"', 'id = "probes"\nconcurrency = 2'), "has no `inputs`, `tags`, `driver` or `concurrency`"),
            "a tag without its sign": (edited('tags = ["@lane"]', 'tags = ["lane"]'), "does not start with `@`"),
            "probes that keep nothing": (edited("artifacts = true\n", ""), "needs the `probes` libtest invocation"),
            "a suite bound inside its reserve": (edited("bound_seconds = 120", "bound_seconds = 10"), "leaves the suite no budget"),
            "a malformed identity": (edited('id = "probe.cycle"', 'id = "Probe Cycle"'), "an identity is a kind and dotted lowercase words"),
            "an unknown invocation": (edited('invocation = "probes"\nselections = ["deloxide", "deloxide-order"]\ntest = "cycle_reports"', 'invocation = "missing"\nselections = ["deloxide", "deloxide-order"]\ntest = "cycle_reports"'), "names an invocation the inventory does not register"),
            "an unknown selection": (edited('selections = ["deloxide-order"]\ntest = "order_reports"', 'selections = ["shuttle"]\ntest = "order_reports"'), "names the unknown selection `shuttle`"),
            "a test without its marker": (edited('test = "store::tests::deloxide_store"', 'test = "store::tests::store"'), "does not carry its invocation's marker"),
            "a test workload with examples": (edited('test = "cycle_reports"', 'test = "cycle_reports"\nexamples = 2'), "a test workload has no `feature`"),
            "a scenario in one selection": (edited('scenario = "Nodes on <nodes> nodes"\nselections = ["deloxide", "deloxide-order"]', 'scenario = "Nodes on <nodes> nodes"\nselections = ["deloxide"]'), "a scenario runs in every selection its tags reach"),
            "a test registered twice": (edited('test = "order_reports"', 'test = "cycle_reports"'), "cycle_reports is registered twice"),
            "a registered ignored test": (edited('test = "order_reports"', 'test = "workload"'), "workload is registered twice"),
            "an owner of an unknown workload": (edited('workloads = ["owner.store", "scenario.nodes"]', 'workloads = ["owner.gone"]'), "names the unknown workload `owner.gone`"),
            "an owner without a disposition": (edited('gap = "Client locks run outside the selected workloads."\n', ""), "names neither the workloads that reach it nor its gap"),
            "an empty gap": (edited('gap = "Client locks run outside the selected workloads."', 'gap = " "'), "`gap` is not a non-empty string"),
            "an owner twice": (edited('path = "crates/client/src/lib.rs"', 'path = "src/store.rs"'), "owner src/store.rs is registered twice"),
            "an invocation without a selection's workloads": (edited('selections = ["deloxide", "deloxide-order"]\ntest = "cycle_reports"', 'selections = ["deloxide-order"]\ntest = "cycle_reports"'), "invocation probes registers no workload for deloxide"),
            "no selection": (INVENTORY.replace("[[selection]]", "[[selections]]"), "unknown keys: selections"),
            "a selection without its recording": (edited('recorded = "ActiveOnly"', 'recorded = ""'), "needs a non-empty string `recorded`"),
            "an unknown selection key": (edited('recorded = "ActiveOnly"', 'recorded = "ActiveOnly"\nmode = "x"'), "selection #1 has unknown keys: mode"),
            "an invocation without an identity": (edited('id = "probes"', 'id = ""'), "invocation #1 needs a non-empty string `id`"),
            "an invocation twice": (edited('id = "owner-tests"', 'id = "probes"'), "invocation probes is registered twice"),
            "an unknown invocation key": (edited('id = "probes"', 'id = "probes"\nretries = 2'), "invocation probes has unknown keys: retries"),
            "features that are not a list": (edited('features = ["testing"]\nid = "owner-tests"', 'features = "testing"\nid = "owner-tests"'), "`features` is not a list"),
            "a feature twice": (edited('features = ["testing"]\nid = "owner-tests"', 'features = ["testing", "testing"]\nid = "owner-tests"'), "`features` names an entry twice"),
            "an empty feature": (edited('features = ["testing"]\nid = "owner-tests"', 'features = [""]\nid = "owner-tests"'), "`features` holds an entry that is not a non-empty string"),
            "evidence that is not a flag": (edited("evidence = true\nfeatures = [\"testing\"]\nid = \"owner-tests\"", "evidence = 1\nfeatures = [\"testing\"]\nid = \"owner-tests\""), "`evidence` is not a boolean"),
            "artifacts that are not a flag": (edited("artifacts = true", "artifacts = \"yes\""), "`artifacts` is not a boolean"),
            "an empty marker": (edited('marker = "deloxide_"', 'marker = ""'), "`marker` is not a non-empty string"),
            "an ignored test without its marker": (edited('marker = "deloxide_"', 'marker = "deloxide_"\nignored = ["workload"]'), "an ignored test does not carry its marker"),
            "a scenario invocation with a marker": (edited('tags = ["@lane"]', 'tags = ["@lane"]\nmarker = "x"'), "a scenario invocation has no `ignored` or `marker`"),
            "a libtest driver": (edited('id = "probes"', 'id = "probes"\ndriver = "nervix-paced-simulation"'), "a libtest invocation has no `inputs`, `tags`, `driver` or `concurrency`"),
            "an empty driver": (edited('tags = ["@lane"]', 'tags = ["@lane"]\ndriver = ""'), "`driver` is not a non-empty string"),
            "a workload without an identity": (edited('id = "probe.cycle"', 'id = ""'), "workload #1 needs a non-empty string `id`"),
            "a workload twice": (edited('id = "probe.order"', 'id = "probe.cycle"'), "workload probe.cycle is registered twice"),
            "an unknown workload key": (edited('id = "probe.cycle"', 'id = "probe.cycle"\nretries = 1'), "workload probe.cycle has unknown keys: retries"),
            "a workload without a selection": (edited('selections = ["deloxide-order"]\ntest = "order_reports"', 'selections = []\ntest = "order_reports"'), "workload probe.order names no selection"),
            "a scenario workload with a test": (edited('scenario = "Nodes on <nodes> nodes"', 'scenario = "Nodes on <nodes> nodes"\ntest = "x"'), "a scenario workload has no `test`"),
            "a scenario workload without examples": (edited("examples = 2\n", ""), "needs a positive integer `examples`"),
            "a scenario twice": (INVENTORY + textwrap.dedent('''
                [[workload]]
                coverage = "Again."
                examples = 2
                feature = "features/lane.feature"
                id = "scenario.again"
                invariant = "Again."
                invocation = "scenarios"
                scenario = "Nodes on <nodes> nodes"
                selections = ["deloxide", "deloxide-order"]
                '''), "the scenario is registered twice"),
            "an owner without a path": (edited('path = "src/store.rs"', 'path = ""'), "owner #1 needs a non-empty string `path`"),
            "an unknown owner key": (edited('path = "src/store.rs"', 'path = "src/store.rs"\nreviewer = "x"'), "owner src/store.rs has unknown keys: reviewer"),
            "a malformed document": ("[lane", "the inventory"),
        }
        for case, (text, message) in cases.items():
            if case == "a malformed document":
                continue
            with self.subTest(case=case), self.assertRaisesRegex(LaneError, message):
                deloxide_lane.parse_inventory(text)

    def test_an_unreadable_inventory_is_a_lane_error(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        with self.assertRaisesRegex(LaneError, "tests/deloxide-inventory.toml"):
            deloxide_lane.load_inventory(root)
        (root / "tests").mkdir()
        (root / deloxide_lane.INVENTORY).write_text("[lane")
        with self.assertRaisesRegex(LaneError, "tests/deloxide-inventory.toml"):
            deloxide_lane.load_inventory(root)


class DiscoveryTests(unittest.TestCase):
    def test_listed_tests_are_read_from_libtests_terse_list(self) -> None:
        output = "cycle_reports: test\nmodule::bench_x: benchmark\n\n2 tests, 0 benchmarks\nworkload: test\n"
        self.assertEqual(deloxide_lane.listed_tests(output), ["cycle_reports", "workload"])

    def test_scenarios_count_the_runs_their_tags_select(self) -> None:
        text = textwrap.dedent(
            '''
            @lane
            Feature: Every tagged scenario
              A description that mentions Scenario: in prose.

              Background:
                Given a cluster

              Scenario: Plain
                Given a step
                  """
                  @other
                  Scenario: inside a doc string
                  | not | a row |
                  """

              Scenario Outline: Outline on <nodes>
                Given a step with a table
                  | key   |
                  | value |
                Examples:
                  | nodes |
                  | 1     |
                  | 3     |
            '''
        )
        found = deloxide_lane.parse_scenarios("features/a.feature", text, frozenset({"@lane"}))
        self.assertEqual(
            [(scenario.name, scenario.runs) for scenario in found],
            [("Plain", 1), ("Outline on <nodes>", 2)],
        )
        self.assertEqual([scenario.line for scenario in found], [9, 17])

    def test_scenario_and_examples_tags_select_their_own_runs(self) -> None:
        text = textwrap.dedent(
            """
            Feature: Mixed
              @lane
              Scenario: Tagged
                Given a step

              Scenario: Untagged
                Given a step

              Scenario Outline: Some examples
                Given <x>
                @lane
                Examples:
                  | x |
                  | 1 |
                  | 2 |
                Examples:
                  | x |
                  | 3 |

              Rule: A tagged rule
              @ignored
              Scenario: Outside the selection
                Given a step

            @lane
            Rule: Selected rule
              Scenario: Inside the rule
                Given a step
            """
        )
        found = deloxide_lane.parse_scenarios("f.feature", text, frozenset({"@lane"}))
        self.assertEqual(
            [(scenario.name, scenario.runs) for scenario in found],
            [("Tagged", 1), ("Some examples", 2), ("Inside the rule", 1)],
        )

    def test_scenario_problems_name_missing_mismatched_and_unregistered_scenarios(self) -> None:
        inventory = deloxide_lane.parse_inventory(INVENTORY)
        invocation = inventory.invocations["scenarios"]
        registered = inventory.workloads_of("scenarios", "deloxide")
        Scenario = deloxide_lane.Scenario
        self.assertEqual(
            deloxide_lane.scenario_problems(invocation, registered, [Scenario("features/lane.feature", "Nodes on <nodes> nodes", 3, 2)]),
            [],
        )
        problems = deloxide_lane.scenario_problems(
            invocation,
            registered,
            [
                Scenario("features/lane.feature", "Nodes on <nodes> nodes", 3, 3),
                Scenario("features/lane.feature", "A new scenario", 9, 1),
            ],
        )
        self.assertEqual(len(problems), 2)
        self.assertIn("registers 2 runs, and the tags select 3 in features/lane.feature:3", problems[0])
        self.assertIn("features/lane.feature:9 `A new scenario` is tagged for the lane but not registered", problems[1])
        missing = deloxide_lane.scenario_problems(invocation, registered, [])
        self.assertEqual(len(missing), 1)
        self.assertIn("names a scenario its tags no longer select", missing[0])

    def test_test_problems_name_missing_ignored_and_unregistered_tests(self) -> None:
        inventory = deloxide_lane.parse_inventory(INVENTORY)
        probes = inventory.invocations["probes"]
        registered = inventory.workloads_of("probes", "deloxide")
        self.assertEqual(
            deloxide_lane.test_problems(probes, registered, ["cycle_reports", "workload"], ["workload"]), []
        )
        problems = deloxide_lane.test_problems(
            probes, registered, ["cycle_reports", "new_probe", "workload"], ["cycle_reports"]
        )
        self.assertIn("probes: workload probe.cycle is ignored: cycle_reports", problems)
        self.assertIn("probes: new_probe is in the build but not registered", problems)
        self.assertIn("probes: workload is registered as ignored but the build does not ignore it", problems)
        missing = deloxide_lane.test_problems(probes, registered, ["workload"], ["workload"])
        self.assertEqual(missing, ["probes: workload probe.cycle names a test the build does not hold: cycle_reports"])
        owner = inventory.invocations["owner-tests"]
        owners = inventory.workloads_of("owner-tests", "deloxide")
        self.assertEqual(
            deloxide_lane.test_problems(owner, owners, ["store::tests::deloxide_store", "store::tests::ordinary"], []),
            [],
        )
        extra = deloxide_lane.test_problems(owner, owners, ["store::tests::deloxide_store", "other::deloxide_more"], ["ignored::deloxide_x"])
        self.assertIn("owner-tests: other::deloxide_more is in the build but not registered", extra)
        self.assertIn("owner-tests: ignored::deloxide_x is ignored in the build but not registered", extra)


class SummaryTests(unittest.TestCase):
    def test_the_summary_line_is_read_whole_or_not_at_all(self) -> None:
        counts = deloxide_lane.parse_summary("noise\n" + summary_line(findings=3, active=1, potential=2, unreviewed=1, nonqualifying=2, repeated_deliveries=5, lost_retention=7))
        self.assertEqual(
            counts,
            EvidenceCounts("whole-process", 3, 1, 2, 1, 2, 5, 0, 0, 7),
        )
        assert counts is not None
        self.assertEqual(counts.lost(), 7)
        self.assertIsNone(deloxide_lane.parse_summary("diagnostic evidence qualifies\n"))
        line = CLEAN_SUMMARY.strip()
        for broken in (
            line.replace(" findings=0", ""),
            line.replace("active=0 potential=0", "potential=0 active=0"),
            line.replace("active=0", "active=x"),
            line.replace("active=0", "active="),
            line + " extra=1",
        ):
            with self.subTest(line=broken):
                self.assertIsNone(deloxide_lane.parse_summary(broken))

    def test_cucumbers_last_scenario_summary_counts_each_outcome(self) -> None:
        output = "2 scenarios (1 passed, 1 failed)\n[Summary]\n36 scenarios (35 passed, 1 skipped)\n90 steps (90 passed)\n"
        self.assertEqual(deloxide_lane.scenario_summary(output), {"total": 36, "passed": 35, "skipped": 1})
        self.assertEqual(deloxide_lane.scenario_summary("1 scenario (1 passed)\n"), {"total": 1, "passed": 1})
        self.assertIsNone(deloxide_lane.scenario_summary("no summary\n"))
        self.assertIsNone(deloxide_lane.scenario_summary("3 scenarios (all passed)\n"))

    def test_totals_keep_repeated_deliveries_apart_from_lost_findings(self) -> None:
        totals = deloxide_lane.Totals()
        clean = Qualified(Path("a"), 0, EvidenceCounts("whole-process", 0, 0, 0, 0, 0, 0, 0, 0, 0), None)
        repeated = Qualified(Path("b"), 5, EvidenceCounts("whole-process", 1, 0, 1, 1, 1, 4, 0, 0, 0), None)
        lost = Qualified(Path("c"), 5, EvidenceCounts("whole-process", 1, 0, 0, 0, 1, 0, 1, 2, 3), None)
        unreadable = Qualified(Path("d"), 4, None, None)
        for qualified in (clean, repeated, lost, unreadable):
            totals.add(qualified)
        self.assertEqual(totals.observations, 4)
        self.assertEqual(totals.qualifying, 1)
        self.assertEqual(totals.repeated_deliveries, 4)
        self.assertEqual(totals.lost(), 6)
        self.assertIn("repeated deliveries 4, lost 6 (handoff 1, order history 2, retention 3)", totals.line())


class ClassificationTests(unittest.TestCase):
    def test_every_ending_has_its_class_and_status(self) -> None:
        def ended(ending: Ending, status: int | None = None, number: int | None = None, leftovers: tuple[int, ...] = ()) -> Ended:
            return Ended(1, ending, status, number, 1.0, leftovers)

        cases = [
            (ended(Ending.EXITED, 0), None, 0),
            (ended(Ending.EXITED, 3), Failure.ACTIVE_DEADLOCK, 3),
            (ended(Ending.EXITED, 4), Failure.DIAGNOSTIC_FAILURE, 4),
            (ended(Ending.EXITED, 124), Failure.TIMED_OUT, 124),
            (ended(Ending.EXITED, 101), Failure.FAILED, 1),
            (ended(Ending.SIGNALED, number=signal.SIGABRT), Failure.SIGNALED, 1),
            (ended(Ending.TIMED_OUT), Failure.TIMED_OUT, 124),
            (ended(Ending.EXITED, 0, leftovers=(7,)), Failure.LEFTOVER, 1),
        ]
        for value, failure, status in cases:
            with self.subTest(ended=value):
                self.assertIs(deloxide_lane.ending_failure(value), failure)
                if failure is not None:
                    self.assertEqual(failure.status(), status)
        self.assertEqual(Failure.BUDGET.status(), 124)
        self.assertIn("SIGABRT", deloxide_lane.describe_ending(ended(Ending.SIGNALED, number=signal.SIGABRT)))
        self.assertIn("left processes running: 7", deloxide_lane.describe_ending(ended(Ending.EXITED, 0, leftovers=(7,))))
        self.assertEqual(deloxide_lane.signal_name(250), "signal 250")

    def test_a_supervised_process_is_classified_by_its_evidence_and_ending(self) -> None:
        clean_counts = EvidenceCounts("whole-process", 0, 0, 0, 0, 0, 0, 0, 0, 0)
        active_counts = EvidenceCounts("whole-process", 1, 1, 0, 0, 1, 0, 0, 0, 0)
        clean = Qualified(Path("a"), 0, clean_counts, None)
        active = Qualified(Path("b"), 5, active_counts, None)
        unqualified = Qualified(Path("c"), 5, EvidenceCounts("whole-process", 1, 0, 1, 1, 1, 0, 0, 0, 0), None)
        exited = Ended(1, Ending.EXITED, 0, None, 1.0)
        failed = Ended(1, Ending.EXITED, 101, None, 1.0)
        classify = deloxide_lane.classify
        self.assertIsNone(classify(exited, [clean], []))
        self.assertIs(classify(failed, [active], []), Failure.ACTIVE_DEADLOCK)
        self.assertIs(classify(failed, [clean], []), Failure.FAILED)
        self.assertIs(classify(exited, [clean], [Path("p")]), Failure.EVIDENCE_PARTIAL)
        self.assertIs(classify(exited, [], []), Failure.EVIDENCE_MISSING)
        self.assertIs(classify(exited, [unqualified], []), Failure.EVIDENCE_UNQUALIFIED)

    def test_each_qualification_case_expects_one_failure_class_and_one_control_passes(self) -> None:
        expected = {case.workload: case.failure for case in deloxide_lane.CASES}
        self.assertIsNone(expected.pop("two_mutexes_in_one_order"))
        self.assertEqual(
            set(expected.values()),
            {Failure.ACTIVE_DEADLOCK, Failure.DIAGNOSTIC_FAILURE, Failure.TIMED_OUT, Failure.SIGNALED, Failure.EVIDENCE_UNQUALIFIED},
        )
        probes = (REPOSITORY / "crates/deadlock/tests/active_cycles.rs").read_text()
        for case in deloxide_lane.CASES:
            with self.subTest(case=case.workload):
                self.assertIn(f'"{case.workload}" =>', probes)
        matches = deloxide_lane.Expected(readable=True, unreviewed=True, lost=True).matches
        self.assertTrue(matches(EvidenceCounts("whole-process", 16, 0, 15, 15, 16, 0, 0, 0, 1)))
        self.assertFalse(matches(EvidenceCounts("whole-process", 15, 0, 15, 15, 15, 0, 0, 0, 0)))


class SupervisionTests(unittest.TestCase):
    """Real processes: the supervisor must see exactly how they end and what they leave behind."""

    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.processes = Processes(dict(os.environ), 0.5)

    def launch(self, script: str, bound: float = 30.0, name: str = "case") -> Launch:
        return Launch(
            name=name,
            argv=("sh", "-c", script),
            cwd=self.root,
            environment={"LANE_PROBE": "value"},
            bound_seconds=bound,
            log=self.root / f"{name}.log",
        )

    def test_an_exit_status_and_the_output_are_kept(self) -> None:
        ended = self.processes.run(self.launch('echo "$LANE_PROBE" >&2; exit 3'))
        self.assertEqual((ended.ending, ended.status, ended.signal, ended.leftovers), (Ending.EXITED, 3, None, ()))
        self.assertEqual((self.root / "case.log").read_text(), "value\n")

    def test_a_signal_is_named_rather_than_folded_into_a_status(self) -> None:
        ended = self.processes.run(self.launch("kill -ABRT $$"))
        self.assertEqual(ended.ending, Ending.SIGNALED)
        self.assertEqual(ended.signal, signal.SIGABRT)
        self.assertEqual(ended.describe()["signal"], "SIGABRT")

    def test_a_process_outliving_its_bound_is_ended_with_its_whole_group(self) -> None:
        marker = self.root / "child.pid"
        ended = self.processes.run(self.launch(f"sleep 60 & echo $! > {marker}; trap '' TERM; wait", bound=0.5))
        self.assertEqual(ended.ending, Ending.TIMED_OUT)
        self.assertIsNone(ended.status)
        child = int(marker.read_text())
        self.assertFalse(Path(f"/proc/{child}").exists() and deloxide_lane.process_states().get(child, deloxide_lane.ProcessState(0, 0, "Z")).is_running())
        self.assertEqual(deloxide_lane.running_members(ended.pid), [])

    def test_a_process_left_running_is_killed_and_named(self) -> None:
        ended = self.processes.run(self.launch("sleep 60 & exit 0"))
        self.assertEqual(ended.ending, Ending.EXITED)
        self.assertEqual(len(ended.leftovers), 1)
        self.assertIs(deloxide_lane.ending_failure(ended), Failure.LEFTOVER)
        self.assertEqual(deloxide_lane.running_members(ended.pid), [])

    def test_a_separate_standard_output_holds_only_what_the_process_printed_there(self) -> None:
        launch = Launch("build", ("sh", "-c", "echo json; echo diagnostics >&2"), self.root, {}, 30.0, self.root / "build.log", self.root / "build.jsonl")
        self.processes.run(launch)
        self.assertEqual((self.root / "build.jsonl").read_text(), "json\n")
        self.assertEqual((self.root / "build.log").read_text(), "diagnostics\n")

    def test_a_missing_executable_is_a_lane_error(self) -> None:
        launch = Launch("missing", (str(self.root / "absent"),), self.root, {}, 1.0, self.root / "m.log")
        with self.assertRaisesRegex(LaneError, "missing: cannot start"):
            self.processes.run(launch)

    def test_an_interruption_ends_the_process_and_propagates(self) -> None:
        marker = self.root / "started"
        launch = self.launch(f"touch {marker}; sleep 60")
        previous = signal.signal(signal.SIGALRM, lambda number, frame: (_ for _ in ()).throw(deloxide_lane.Interrupted(signal.SIGTERM)))
        self.addCleanup(signal.signal, signal.SIGALRM, previous)
        signal.setitimer(signal.ITIMER_REAL, 0.5)
        with self.assertRaises(deloxide_lane.Interrupted):
            self.processes.run(launch)
        signal.setitimer(signal.ITIMER_REAL, 0)
        self.assertTrue(marker.exists())


class LaneTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = Fixture(self)

    def test_order_example_processes_account_for_every_run_and_keep_separate_evidence(self) -> None:
        extra = textwrap.dedent("""
            [[workload]]
            coverage = "Large restore examples."
            examples = 2
            feature = "features/lane.feature"
            id = "scenario.large"
            invariant = "Each restore example ends cleanly."
            invocation = "scenarios"
            order_tags = ["@large_first", "@large_second"]
            scenario = "Large restore on <kind>"
            selections = ["deloxide", "deloxide-order"]

        """)
        fixture = Fixture(self, INVENTORY.replace("[[owner]]\n", extra + "[[owner]]\n", 1))
        (fixture.root / "features/lane.feature").write_text(FEATURE + textwrap.dedent("""
              Scenario Outline: Large restore on <kind>
                Given a step
                @large_first
                Examples:
                  | kind |
                  | a    |
                @large_second
                Examples:
                  | kind |
                  | b    |
        """))
        processes = ScriptedProcesses(fixture.root, "deloxide-order")
        processes.scenario_summaries = {
            "scenarios-1": "1 scenario (1 passed)",
            "scenarios-2": "1 scenario (1 passed)",
        }
        lane = fixture.lane(processes, "deloxide-order")
        self.assertEqual(quietly(lane.execute), 0)
        content = record(lane)
        self.assertEqual([workload["id"] for workload in content["workloads"][-2:]], ["scenario.nodes", "scenario.large"])
        self.assertEqual(content["counts"], {"discovered": 7, "selected": 7, "executed": 7, "completed": 7})
        chunks = [launch for launch in processes.launches if re.fullmatch(r"scenarios-[0-9]+", launch.name)]
        self.assertEqual([launch.name for launch in chunks], ["scenarios-0", "scenarios-1", "scenarios-2"])
        self.assertIn("--name", chunks[0].argv)
        self.assertIn("@large_first", chunks[1].argv)
        self.assertIn("@large_second", chunks[2].argv)
        self.assertEqual(len({launch.environment[deloxide_lane.EVIDENCE_VARIABLE] for launch in chunks}), 3)

    def test_a_complete_run_accounts_for_every_workload_and_qualifies_every_observation(self) -> None:
        processes = ScriptedProcesses(self.fixture.root, "deloxide-order")
        step_summary = self.fixture.root / "step-summary.md"
        processes.base_environment["GITHUB_STEP_SUMMARY"] = str(step_summary)
        report = self.fixture.root / "collector" / "lane.json"
        report.parent.mkdir()
        lane = self.fixture.lane(processes, "deloxide-order", report=report)
        self.assertEqual(quietly(lane.execute), 0)
        self.assertIn(
            "deloxide lane (deloxide-order): discovered 5, selected 5, executed 5, completed 5\n",
            step_summary.read_text(),
        )
        self.assertIn("complete; attempt", step_summary.read_text())
        content = record(lane)
        self.assertEqual(content, json.loads(report.read_text()))
        self.assertEqual(content["verdict"], "complete")
        self.assertEqual(content["selection"], "deloxide-order")
        self.assertEqual(content["recorded_selection"], "OrderAnalysis")
        self.assertEqual(content["revision"], {"commit": "0123abcd", "working_tree_modified": False})
        self.assertEqual(content["dependencies"]["deloxide"]["version"], "1.1.0")
        self.assertEqual(content["counts"], {"discovered": 5, "selected": 5, "executed": 5, "completed": 5})
        self.assertEqual(content["findings"]["observations"], 3)
        self.assertEqual(content["findings"]["qualifying"], 3)
        self.assertEqual([workload["id"] for workload in content["workloads"]], ["probe.cycle", "probe.order", "owner.store", "scenario.nodes"])
        self.assertTrue(all(workload["completed"] for workload in content["workloads"]))
        self.assertEqual(deloxide_lane.read_complete(lane.record.path, "deloxide-order"), content)

        launches = {launch.name: launch for launch in processes.launches}
        self.assertEqual(list(launches), ["probes-build", "probes", "owner-tests-build", "owner-tests-owner.store", "scenarios-build", "scenarios-0"])
        build = launches["probes-build"]
        self.assertEqual(build.argv[:5], ("cargo", "test", "--no-run", "--package", "nervix-deadlock"))
        self.assertIn("deloxide-order", build.argv)
        self.assertEqual(build.environment["CARGO_TARGET_DIR"], str(self.fixture.target / "deloxide"))
        self.assertEqual(launches["owner-tests-build"].argv[5:8], ("--features", "testing deloxide-order", "--lib"))
        probes = launches["probes"]
        self.assertEqual(probes.argv, (str(processes.executable("probes")),))
        self.assertEqual(probes.cwd, self.fixture.root / "crates/deadlock")
        self.assertEqual(probes.environment[deloxide_lane.PROBE_ARTIFACTS_VARIABLE], str(lane.attempt / "probes-artifacts"))
        self.assertEqual(probes.environment["ORT_DYLIB_PATH"], "/opt/onnxruntime/libonnxruntime.so")
        owner = launches["owner-tests-owner.store"]
        self.assertEqual(owner.argv[1:], ("store::tests::deloxide_store", "--exact", "--test-threads=1"))
        self.assertEqual(owner.environment[deloxide_lane.EVIDENCE_VARIABLE], str(lane.attempt / "evidence/owner-tests/owner.store"))
        scenarios = launches["scenarios-0"]
        self.assertEqual(scenarios.argv[1:], ("--input", "features/lane.feature", "--tags", "@lane", "--retry", "0", "--concurrency", "4"))
        self.assertEqual(scenarios.environment[deloxide_lane.SUITE_BUDGET_VARIABLE], "110s")
        self.assertEqual(scenarios.environment[deloxide_lane.EVIDENCE_VARIABLE], str(lane.attempt / "evidence/scenarios/scenarios-0"))
        self.assertEqual(scenarios.environment[deloxide_lane.REPORT_TOOL_VARIABLE], str(self.fixture.target / "debug/nervix-deadlock-report"))
        recorded = [launch for launch in content["launches"] if launch["stage"] == "run"]
        self.assertEqual([launch["name"] for launch in recorded], ["probes", "owner-tests-owner.store", "scenarios-0"])
        self.assertEqual(recorded[0]["accounting"]["outcomes"]["workload"], "ignored")
        self.assertEqual(recorded[2]["accounting"]["scenarios"], {"total": 2, "passed": 2})

    def test_an_instrumented_run_builds_where_the_collector_chose_and_starts_through_its_runner(self) -> None:
        environment = {
            deloxide_lane.COVERAGE_ATTEMPT_VARIABLE: "/collector/attempt",
            deloxide_lane.PREPARED_TARGET_VARIABLE: str(self.fixture.target),
            "CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER": "python3 scripts/native_coverage.py exec",
        }
        build = self.fixture.target / "native-coverage-build-deloxide"
        workspace = deloxide_lane.resolve_workspace(self.fixture.root, build, environment, "x86_64-unknown-linux-gnu")
        self.assertTrue(workspace.instrumented)
        self.assertEqual(workspace.build, build)
        self.assertEqual(workspace.report_tool(), self.fixture.target / "debug/nervix-deadlock-report")
        processes = ScriptedProcesses(self.fixture.root)
        lane = Lane(self.fixture.inventory, self.fixture.inventory.selection("deloxide"), workspace, processes)
        self.assertEqual(quietly(lane.execute), 0)
        self.assertTrue(lane.attempt.is_relative_to(build / "test-deloxide" / "deloxide"))
        probes = next(launch for launch in processes.launches if launch.name == "probes")
        self.assertEqual(probes.argv, ("python3", "scripts/native_coverage.py", "exec", str(processes.executable("probes"))))
        self.assertEqual(record(lane)["workspace"]["instrumented"], True)

    def test_a_check_that_records_no_evidence_completes_on_its_own_exit_and_report(self) -> None:
        self.fixture = Fixture(self, INVENTORY + CONFORMANCE)
        processes = ScriptedProcesses(self.fixture.root)
        lane = self.fixture.lane(processes)
        self.assertEqual(quietly(lane.execute), 0)
        content = record(lane)
        self.assertEqual(content["counts"], {"discovered": 5, "selected": 5, "executed": 5, "completed": 5})
        self.assertEqual(content["workloads"][-1], {
            "id": "primitive.debug",
            "invocation": "conformance",
            "test": "tests::tracked_locks::debug_never_waits",
            "outcome": "ok",
            "completed": True,
        })
        # Only the invocations that record evidence are observed and qualified.
        self.assertEqual(content["findings"]["observations"], 3)
        self.assertEqual(deloxide_lane.read_complete(lane.record.path, "deloxide"), content)
        launches = {launch.name: launch for launch in processes.launches}
        self.assertEqual(
            launches["conformance-build"].argv[3:8],
            ("--package", "nervix-primitives", "--features", "native deloxide", "--lib"),
        )
        check = launches["conformance-primitive.debug"]
        self.assertEqual(check.argv[1:], ("tests::tracked_locks::debug_never_waits", "--exact", "--test-threads=1"))
        self.assertEqual(check.cwd, self.fixture.root / "crates/primitives")
        self.assertEqual(check.bound_seconds, 30)
        self.assertNotIn(deloxide_lane.EVIDENCE_VARIABLE, check.environment)
        self.assertFalse((lane.attempt / "evidence" / "conformance").exists())

    def test_a_check_whose_detector_aborted_it_fails_the_lane_as_signaled(self) -> None:
        self.fixture = Fixture(self, INVENTORY + CONFORMANCE)
        processes = ScriptedProcesses(self.fixture.root)
        processes.outputs["conformance-primitive.debug"] = (
            "running 1 test\na conformance check of the tracked locks deadlocked\n"
        )
        processes.endings["conformance-primitive.debug"] = Ended(9, Ending.SIGNALED, None, signal.SIGABRT, 0.2)
        status, content = self.run_failing(processes)
        self.assertEqual(status, 1)
        self.assertEqual(content["failure"]["class"], "signaled")
        self.assertIn("conformance-primitive.debug was killed by SIGABRT", content["failure"]["detail"])
        self.assertIn("primitive.debug did not run", content["failure"]["detail"])

        processes = ScriptedProcesses(self.fixture.root)
        processes.listed["primitives"].append("tests::tracked_locks::a_new_check")
        status, content = self.run_failing(processes)
        self.assertEqual(content["failure"]["class"], "inventory")
        self.assertIn(
            "conformance: tests::tracked_locks::a_new_check is in the build but not registered",
            content["failure"]["detail"],
        )

    def run_failing(self, processes: ScriptedProcesses, selection: str = "deloxide", clock: Callable[[], float] = time.monotonic) -> tuple[int, dict[str, object]]:
        lane = self.fixture.lane(processes, selection, clock=clock)
        status = quietly(lane.execute)
        content = record(lane)
        self.assertEqual(content["verdict"], "failed")
        with self.assertRaises(deloxide_lane.RecordError):
            deloxide_lane.read_complete(lane.record.path, selection)
        return status, content

    def test_an_unregistered_or_ignored_test_stops_the_lane_before_it_runs(self) -> None:
        processes = ScriptedProcesses(self.fixture.root)
        processes.listed["probes"].append("a_new_probe")
        status, content = self.run_failing(processes)
        self.assertEqual(status, 1)
        self.assertEqual(content["failure"]["class"], "inventory")
        self.assertIn("a_new_probe is in the build but not registered", content["failure"]["detail"])
        self.assertNotIn("probes", [launch.name for launch in processes.launches])

        processes = ScriptedProcesses(self.fixture.root)
        processes.listed_ignored["probes"].append("cycle_reports")
        status, content = self.run_failing(processes)
        self.assertIn("workload probe.cycle is ignored", content["failure"]["detail"])

    def test_a_test_that_did_not_run_or_failed_is_incomplete_or_failed(self) -> None:
        processes = ScriptedProcesses(self.fixture.root)
        processes.outputs["probes"] = libtest({"workload": "ignored"})
        status, content = self.run_failing(processes)
        self.assertEqual(content["failure"]["class"], "incomplete")
        self.assertIn("probe.cycle did not run: cycle_reports", content["failure"]["detail"])
        self.assertIn("executed no test", content["failure"]["detail"])

        processes = ScriptedProcesses(self.fixture.root)
        processes.outputs["probes"] = libtest({"cycle_reports": "FAILED", "workload": "ignored"})
        processes.endings["probes"] = Ended(9, Ending.EXITED, 101, None, 1.0)
        status, content = self.run_failing(processes)
        self.assertEqual(content["failure"]["class"], "failed")
        self.assertIn("probes exited with status 101", content["failure"]["detail"])
        self.assertIn("probe.cycle failed: cycle_reports", content["failure"]["detail"])

        processes = ScriptedProcesses(self.fixture.root)
        processes.outputs["probes"] = "running 2 tests\n"
        status, content = self.run_failing(processes)
        self.assertIn("libtest printed no result", content["failure"]["detail"])

    def test_a_failed_lane_publishes_its_failure_to_the_step_summary(self) -> None:
        processes = ScriptedProcesses(self.fixture.root)
        step_summary = self.fixture.root / "step-summary.md"
        processes.base_environment["GITHUB_STEP_SUMMARY"] = str(step_summary)
        processes.endings["probes"] = Ended(9, Ending.EXITED, 3, None, 1.0)
        self.run_failing(processes)
        text = step_summary.read_text()
        self.assertIn("deloxide lane (deloxide): active-deadlock: probes exited with status 3", text)
        self.assertIn("attempt retained at", text)

    def test_each_ending_of_a_workload_fails_the_lane_with_its_own_status(self) -> None:
        cases = {
            "an active deadlock": (Ended(9, Ending.EXITED, 3, None, 1.0), "active-deadlock", 3),
            "a diagnostic failure": (Ended(9, Ending.EXITED, 4, None, 1.0), "diagnostic-failure", 4),
            "a signal": (Ended(9, Ending.SIGNALED, None, signal.SIGKILL, 1.0), "signaled", 1),
            "a timeout": (Ended(9, Ending.TIMED_OUT, None, None, 60.0), "timed-out", 124),
            "a leftover": (Ended(9, Ending.EXITED, 0, None, 1.0, (77,)), "leftover-processes", 1),
        }
        for case, (ended, failure, status) in cases.items():
            with self.subTest(case=case):
                processes = ScriptedProcesses(self.fixture.root)
                processes.endings["probes"] = ended
                observed, content = self.run_failing(processes)
                self.assertEqual(observed, status)
                self.assertEqual(content["failure"]["class"], failure)

    def test_a_failed_scenario_whose_evidence_records_a_deadlock_is_an_active_deadlock(self) -> None:
        processes = ScriptedProcesses(self.fixture.root)
        processes.evidence["scenarios"] = ["process-clusters/cluster-a/deadlock-active-9.rkyv"]
        processes.scenario_summary = "2 scenarios (1 passed, 1 failed)"
        processes.endings["scenarios"] = Ended(9, Ending.EXITED, 101, None, 1.0)
        status, content = self.run_failing(processes)
        self.assertEqual(status, 3)
        self.assertEqual(content["failure"]["class"], "active-deadlock")
        self.assertIn("records an active deadlock", content["failure"]["detail"])
        described = [item for item in content["evidence"] if item.get("description")]
        self.assertEqual(len(described), 1)
        self.assertIn("active deadlock", (self.fixture.root / described[0]["description"]).read_text())

    def test_a_scenario_count_other_than_the_tags_select_is_incomplete(self) -> None:
        processes = ScriptedProcesses(self.fixture.root)
        processes.scenario_summary = "1 scenario (1 passed)"
        status, content = self.run_failing(processes)
        self.assertEqual(content["failure"]["class"], "incomplete")
        self.assertIn("selected 2 scenario runs, and Cucumber reported {'total': 1, 'passed': 1}", content["failure"]["detail"])

    def test_missing_partial_and_nonqualifying_evidence_fail_the_lane(self) -> None:
        cases: dict[str, tuple[Callable[[ScriptedProcesses], None], str, int]] = {
            "no evidence": (lambda processes: processes.evidence.__setitem__("scenarios", []), "evidence-missing", 1),
            "an owner test with two files": (lambda processes: processes.evidence.__setitem__("owner-tests-owner.store", ["deadlock-1-1.rkyv", "deadlock-1-2.rkyv"]), "evidence-missing", 1),
            "a partly written file": (lambda processes: processes.evidence["scenarios"].append("deadlock-4-1.partial"), "evidence-partial", 1),
            "an unreviewed potential cycle": (lambda processes: processes.evidence["scenarios"].append("deadlock-unreviewed-5.rkyv"), "evidence-unqualified", 1),
            "lost findings": (lambda processes: processes.evidence["scenarios"].append("deadlock-lost-6.rkyv"), "evidence-unqualified", 1),
            "an active cycle": (lambda processes: processes.evidence["scenarios"].append("deadlock-active-7.rkyv"), "active-deadlock", 3),
        }
        for case, (change, failure, status) in cases.items():
            with self.subTest(case=case):
                processes = ScriptedProcesses(self.fixture.root)
                change(processes)
                observed, content = self.run_failing(processes)
                self.assertEqual(content["failure"]["class"], failure)
                self.assertEqual(observed, status)

    def test_repeated_deliveries_and_lost_findings_are_reported_apart(self) -> None:
        processes = ScriptedProcesses(self.fixture.root)
        processes.evidence["scenarios"] += ["deadlock-unreviewed-5.rkyv", "deadlock-lost-6.rkyv"]
        _, content = self.run_failing(processes)
        findings = content["findings"]
        self.assertEqual(findings["repeated_deliveries"], 3)
        self.assertEqual(findings["lost_retention"], 2)
        self.assertEqual(findings["unreviewed"], 1)

    def test_a_missing_report_tool_or_runtime_is_a_missing_prerequisite(self) -> None:
        (self.fixture.target / "debug" / "nervix-deadlock-report").unlink()
        status, content = self.run_failing(ScriptedProcesses(self.fixture.root))
        self.assertEqual(content["failure"]["class"], "prerequisite-missing")
        self.assertIn("just tests-deps", content["failure"]["detail"])
        self.assertEqual(status, 1)

    def test_a_launch_that_cannot_start_is_recorded_as_a_missing_prerequisite(self) -> None:
        processes = ScriptedProcesses(self.fixture.root)
        original = processes.run

        def run(launch: Launch) -> Ended:
            if launch.name == "probes":
                raise LaneError("probes: cannot start /missing/probes: No such file or directory")
            return original(launch)

        processes.run = run  # type: ignore[method-assign]
        status, content = self.run_failing(processes)
        self.assertEqual(status, 1)
        self.assertEqual(content["failure"]["class"], "prerequisite-missing")
        self.assertIn("cannot start /missing/probes", content["failure"]["detail"])

    def test_a_build_failure_or_an_expired_budget_stops_the_lane(self) -> None:
        processes = ScriptedProcesses(self.fixture.root)
        processes.endings["probes-build"] = Ended(9, Ending.EXITED, 101, None, 1.0)
        _, content = self.run_failing(processes)
        self.assertEqual(content["failure"]["class"], "build-failed")

        moments = iter([0.0, 0.0, 0.0, 700.0, 700.0, 700.0, 700.0, 700.0, 700.0])
        processes = ScriptedProcesses(self.fixture.root)
        status, content = self.run_failing(processes, clock=lambda: next(moments))
        self.assertEqual(status, 124)
        self.assertEqual(content["failure"]["class"], "budget-expired")

    def test_an_interrupted_lane_records_it_and_ends_with_the_signal(self) -> None:
        processes = ScriptedProcesses(self.fixture.root)
        original = processes.run

        def interrupted(launch: Launch) -> Ended:
            if launch.name == "scenarios":
                raise deloxide_lane.Interrupted(signal.SIGTERM)
            return original(launch)

        processes.run = interrupted  # type: ignore[method-assign]
        lane = self.fixture.lane(processes)
        self.assertEqual(quietly(lane.execute), 128 + signal.SIGTERM)
        content = record(lane)
        self.assertEqual(content["verdict"], "interrupted")
        self.assertEqual(content["failure"]["detail"], "interrupted by SIGTERM")

    def test_a_recorded_launch_replays_exactly_in_a_fresh_attempt(self) -> None:
        processes = ScriptedProcesses(self.fixture.root)
        lane = self.fixture.lane(processes)
        self.assertEqual(quietly(lane.execute), 0)
        replaying = ScriptedProcesses(self.fixture.root)
        replaying.endings["owner-tests-owner.store"] = Ended(9, Ending.EXITED, 3, None, 1.0)
        status = quietly(lambda: deloxide_lane.replay(self.fixture.inventory, replaying, self.fixture.workspace(), lane.record.path, "owner-tests-owner.store"))
        self.assertEqual(status, 3)
        replayed = replaying.launches[-1]
        original = next(launch for launch in processes.launches if launch.name == "owner-tests-owner.store")
        self.assertEqual(replayed.argv, original.argv)
        self.assertEqual(replayed.cwd, original.cwd)
        self.assertEqual(replayed.bound_seconds, original.bound_seconds)
        evidence = Path(replayed.environment[deloxide_lane.EVIDENCE_VARIABLE])
        self.assertTrue(evidence.is_dir())
        self.assertNotEqual(evidence, Path(original.environment[deloxide_lane.EVIDENCE_VARIABLE]))
        self.assertTrue(evidence.is_relative_to(replayed.log.parent))
        replay_record = json.loads((replayed.log.parent / deloxide_lane.RECORD).read_text())
        self.assertEqual(replay_record["failure"], "active-deadlock")
        with self.assertRaisesRegex(LaneError, "records no single launch named absent"):
            deloxide_lane.replay(self.fixture.inventory, replaying, self.fixture.workspace(), lane.record.path, "absent")

    def test_a_record_proves_completion_only_when_every_count_and_observation_does(self) -> None:
        processes = ScriptedProcesses(self.fixture.root)
        lane = self.fixture.lane(processes)
        self.assertEqual(quietly(lane.execute), 0)
        complete = record(lane)
        path = lane.record.path
        self.assertEqual(deloxide_lane.read_complete(path, "deloxide")["verdict"], "complete")
        with self.assertRaisesRegex(deloxide_lane.RecordError, "not of the deloxide-order selection"):
            deloxide_lane.read_complete(path, "deloxide-order")
        broken: dict[str, Callable[[dict[str, object]], None]] = {
            "selected nothing": lambda content: content["counts"].update({"selected": 0, "executed": 0, "completed": 0}),  # type: ignore[union-attr]
            "do not show every selected workload complete": lambda content: content["counts"].update({"completed": 1}),  # type: ignore[union-attr]
            "has no executed count": lambda content: content["counts"].pop("executed"),  # type: ignore[union-attr]
            "a workload that did not complete": lambda content: content["workloads"][0].update({"completed": False}),  # type: ignore[index]
            "holds no process evidence": lambda content: content["findings"].update({"observations": 0}),  # type: ignore[union-attr]
            "evidence that does not qualify": lambda content: content["findings"].update({"qualifying": 0}),  # type: ignore[union-attr]
            "lists no workload": lambda content: content.update({"workloads": []}),
        }
        for message, change in broken.items():
            with self.subTest(case=message):
                content = json.loads(json.dumps(complete))
                change(content)
                path.write_text(json.dumps(content))
                with self.assertRaisesRegex(deloxide_lane.RecordError, message):
                    deloxide_lane.read_complete(path, "deloxide")
        path.write_text("[]")
        with self.assertRaisesRegex(deloxide_lane.RecordError, "not an object"):
            deloxide_lane.read_complete(path, "deloxide")
        path.write_text("{")
        with self.assertRaisesRegex(deloxide_lane.RecordError, "cannot read"):
            deloxide_lane.read_complete(path, "deloxide")


class QualificationTests(unittest.TestCase):
    """The qualification's own bookkeeping, against scripted cases; `just test-deloxide-qualification`
    runs the real probe binary."""

    def test_every_case_is_classified_and_the_deadlock_replays(self) -> None:
        fixture = Fixture(self)
        processes = ScriptedProcesses(fixture.root, "deloxide-order")
        endings = {
            "case-two_mutexes_in_opposite_orders": Ended(9, Ending.EXITED, 3, None, 1.0),
            "case-evidence_that_cannot_be_recorded": Ended(9, Ending.EXITED, 4, None, 1.0),
            "case-an_untracked_wait_that_never_ends": Ended(9, Ending.TIMED_OUT, None, None, 15.0),
            "case-an_aborted_process": Ended(9, Ending.SIGNALED, None, signal.SIGABRT, 1.0),
            "case-retention_overload": Ended(9, Ending.EXITED, 4, None, 1.0),
        }
        evidence = {
            "case-two_mutexes_in_one_order": "deadlock-1-1.rkyv",
            "case-two_mutexes_in_opposite_orders": "deadlock-active-1.rkyv",
            "case-an_untracked_wait_that_never_ends": "deadlock-2-1.rkyv",
            "case-an_aborted_process": "deadlock-3-1.rkyv",
            "case-serial_inversion": "deadlock-unreviewed-1.rkyv",
            "case-retention_overload": "deadlock-unreviewed-lost-1.rkyv",
        }
        original = processes.run

        def run(launch: Launch) -> Ended:
            if launch.stdout is not None:
                return original(launch)
            processes.launches.append(launch)
            launch.log.write_text("output\n")
            name = launch.name if launch.name in evidence or launch.name in endings else launch.name
            file = evidence.get(name)
            if file is not None:
                directory = Path(launch.environment[deloxide_lane.PROBE_EVIDENCE_VARIABLE])
                directory.mkdir(parents=True, exist_ok=True)
                (directory / file).write_bytes(b"evidence")
            return endings.get(name, Ended(9, Ending.EXITED, 0, None, 1.0))

        processes.run = run  # type: ignore[method-assign]
        original_capture = processes.capture

        def capture(argv: Sequence[str], cwd: Path, environment: Mapping[str, str] | None = None) -> tuple[int, str, str]:
            arguments = [str(part) for part in argv]
            if arguments[1:2] == ["qualify"] and "unreviewed-lost" in arguments[2]:
                return 5, summary_line(findings=16, potential=15, unreviewed=15, nonqualifying=16, lost_retention=1), ""
            return original_capture(argv, cwd, environment)

        processes.capture = capture  # type: ignore[method-assign]
        status = quietly(lambda: deloxide_lane.qualify(fixture.inventory, processes, fixture.workspace(), fixture.inventory.selection("deloxide-order")))
        cases_dir = next((fixture.target / "deloxide/test-deloxide/deloxide-order").glob("qualification.*"))
        content = json.loads((cases_dir / deloxide_lane.RECORD).read_text())
        self.assertEqual(content["problems"], [])
        self.assertEqual(status, 0)
        observed = {case["name"]: (case["observed"], case["lane_status"]) for case in content["qualification"]}
        self.assertEqual(observed["case-two_mutexes_in_one_order"], (None, 0))
        self.assertEqual(observed["case-two_mutexes_in_opposite_orders"], ("active-deadlock", 3))
        self.assertEqual(observed["case-evidence_that_cannot_be_recorded"], ("diagnostic-failure", 4))
        self.assertEqual(observed["case-an_untracked_wait_that_never_ends"], ("timed-out", 124))
        self.assertEqual(observed["case-an_aborted_process"], ("signaled", 1))
        self.assertEqual(observed["case-serial_inversion"], ("evidence-unqualified", 1))
        self.assertEqual(observed["case-retention_overload"], ("diagnostic-failure", 4))
        replays = list((fixture.target / "deloxide/test-deloxide/deloxide-order").glob("replay.*"))
        self.assertEqual(len(replays), 1)

    def test_a_case_classified_otherwise_fails_the_qualification(self) -> None:
        fixture = Fixture(self)
        processes = ScriptedProcesses(fixture.root, "deloxide")
        original = processes.run

        def run(launch: Launch) -> Ended:
            if launch.stdout is not None:
                return original(launch)
            launch.log.write_text("output\n")
            directory = Path(launch.environment[deloxide_lane.PROBE_EVIDENCE_VARIABLE])
            (directory / "deadlock-1-1.rkyv").write_bytes(b"evidence")
            # Every case exits cleanly: a supervisor that saw no failure must not pass.
            return Ended(9, Ending.EXITED, 0, None, 1.0)

        processes.run = run  # type: ignore[method-assign]
        status = quietly(lambda: deloxide_lane.qualify(fixture.inventory, processes, fixture.workspace(), fixture.inventory.selection("deloxide")))
        self.assertEqual(status, 1)
        content = json.loads(next((fixture.target / "deloxide/test-deloxide/deloxide").glob("qualification.*/lane.json")).read_text())
        self.assertTrue(any("classified clean, not active-deadlock" in problem for problem in content["problems"]))
        self.assertTrue(any("would not fail the lane" in problem for problem in content["problems"]))


class ApplicabilityTests(unittest.TestCase):
    def catalog(self, *sites: tuple[str, str, str]) -> dict[str, object]:
        findings = []
        for configuration, receiver, path in sites:
            findings.append({
                "configurations": {
                    f"{configuration}:crate": [
                        {"receiver": receiver, "span": {"site": {"path": path, "start": 1}}}
                    ]
                },
                "site": {"path": path, "start": 1},
            })
        return {
            "configurations": [
                {"name": "ordinary", "features": ["nervix-deadlock/report-tool"]},
                {"name": "deloxide", "features": ["native", "deloxide"]},
                {"name": "deloxide-order-binaries", "features": ["native", "deloxide-order"]},
            ],
            "findings": findings,
        }

    def test_every_tracked_owner_needs_one_record_and_every_record_an_owner(self) -> None:
        inventory = deloxide_lane.parse_inventory(INVENTORY)
        tracked = "nervix_primitives::sync::blocking::tracked::Mutex"
        catalog = self.catalog(
            ("deloxide", tracked, "src/store.rs"),
            ("deloxide-order-binaries", "nervix_primitives::sync::blocking::tracked::RwLock", "crates/client/src/lib.rs"),
            ("ordinary", "lock_api::mutex::Mutex", "src/ordinary.rs"),
            ("deloxide", "dashmap::DashMap", "src/map.rs"),
        )
        self.assertEqual(deloxide_lane.catalog_owners(catalog), {"src/store.rs", "crates/client/src/lib.rs"})
        self.assertEqual(deloxide_lane.applicability(inventory, catalog), [])
        changed = self.catalog(
            ("deloxide", tracked, "src/store.rs"),
            ("deloxide", tracked, "src/new_owner.rs"),
        )
        problems = deloxide_lane.applicability(inventory, changed)
        self.assertEqual(len(problems), 2)
        self.assertIn("src/new_owner.rs acquires tracked blocking locks in a diagnostic build and has no [[owner]] record", problems[0])
        self.assertIn("records the owner crates/client/src/lib.rs, which acquires no tracked lock", problems[1])

    def test_a_catalog_without_a_diagnostic_configuration_is_refused(self) -> None:
        catalog = self.catalog()
        catalog["configurations"] = [{"name": "ordinary", "features": []}]
        with self.assertRaisesRegex(LaneError, "holds no diagnostic configuration"):
            deloxide_lane.catalog_owners(catalog)
        with self.assertRaisesRegex(LaneError, "holds no configurations or findings"):
            deloxide_lane.catalog_owners({})
        broken = self.catalog(("deloxide", "nervix_primitives::sync::blocking::tracked::Mutex", ""))
        with self.assertRaisesRegex(LaneError, "names no source file"):
            deloxide_lane.catalog_owners(broken)

    def test_the_command_reports_owners_and_fails_on_a_gap_in_the_records(self) -> None:
        fixture = Fixture(self)
        catalog_path = fixture.root / "gate.json"
        tracked = "nervix_primitives::sync::blocking::tracked::Mutex"
        catalog_path.write_text(json.dumps(self.catalog(("deloxide", tracked, "src/store.rs"), ("deloxide", tracked, "crates/client/src/lib.rs"))))
        arguments = ["--root", str(fixture.root), "applicability", "--catalog", str(catalog_path)]
        self.assertEqual(quietly(lambda: deloxide_lane.main(arguments)), 0)
        catalog_path.write_text(json.dumps(self.catalog(("deloxide", tracked, "src/store.rs"))))
        self.assertEqual(quietly(lambda: deloxide_lane.main(arguments)), 1)
        catalog_path.write_text("{")
        self.assertEqual(quietly(lambda: deloxide_lane.main(arguments)), 1)
        self.assertEqual(quietly(lambda: deloxide_lane.main(["--root", str(fixture.root), "validate"])), 0)
        self.assertEqual(quietly(lambda: deloxide_lane.main(["--root", str(fixture.root), "run", "deloxide"])), 1)


class CommandLineTests(unittest.TestCase):
    """The command line drives the lane, its qualification and its replay through the processes it
    is given, inside the interruption handling every command runs under."""

    def setUp(self) -> None:
        self.fixture = Fixture(self)
        self.processes: list[ScriptedProcesses] = []

    def processes_for(self, selection: str) -> Callable[[Mapping[str, str], float], Processes]:
        def build(environment: Mapping[str, str], grace: float) -> Processes:
            processes = ScriptedProcesses(self.fixture.root, selection)
            self.processes.append(processes)
            return processes
        return build

    def main(self, *arguments: str, selection: str = "deloxide", environment: Mapping[str, str] | None = None) -> int:
        adopted: list[bool] = []
        status = quietly(lambda: deloxide_lane.main(
            ["--root", str(self.fixture.root), "--target-dir", str(self.fixture.target), *arguments],
            processes_for=self.processes_for(selection),
            environment=environment or {},
            adopt_orphans=lambda: adopted.append(True),
        ))
        self.assertEqual(adopted, [True])
        return status

    def test_run_writes_the_record_where_the_collector_asks(self) -> None:
        report = self.fixture.root / "collector-lane.json"
        status = self.main("run", "deloxide", environment={deloxide_lane.REPORT_VARIABLE: str(report)})
        self.assertEqual(status, 0)
        self.assertEqual(json.loads(report.read_text())["verdict"], "complete")
        self.assertEqual(self.main("run", "loom"), 1)

    def test_order_run_records_fresh_scenario_process_and_its_evidence(self) -> None:
        self.assertEqual(self.main("run", "deloxide-order", selection="deloxide-order"), 0)
        launches = self.processes[-1].launches
        scenario = next(launch for launch in launches if launch.name == "scenarios-0")
        self.assertIn("features/lane.feature", scenario.argv)
        self.assertIn("@lane", scenario.argv)
        self.assertTrue(scenario.environment[deloxide_lane.EVIDENCE_VARIABLE].endswith("/scenarios-0"))

    def test_replay_runs_a_launch_of_a_record(self) -> None:
        self.assertEqual(self.main("run", "deloxide"), 0)
        record_path = next((self.fixture.target / "deloxide/test-deloxide/deloxide").glob("run.*/lane.json"))
        self.assertEqual(self.main("replay", str(record_path), "probes"), 0)
        self.assertEqual(self.processes[-1].launches[-1].name, "probes")

    def test_qualify_fails_when_the_cases_end_otherwise_than_required(self) -> None:
        self.assertEqual(self.main("qualify", "deloxide"), 1)

    def test_an_interruption_reaches_the_lane_as_its_signal(self) -> None:
        handler: list[object] = []

        def build(environment: Mapping[str, str], grace: float) -> Processes:
            processes = ScriptedProcesses(self.fixture.root)

            def run(launch: Launch) -> Ended:
                handler.append(signal.getsignal(signal.SIGTERM))
                os.kill(os.getpid(), signal.SIGTERM)
                raise AssertionError("the interruption is raised before this")

            processes.run = run  # type: ignore[method-assign]
            return processes

        status = quietly(lambda: deloxide_lane.main(
            ["--root", str(self.fixture.root), "--target-dir", str(self.fixture.target), "run", "deloxide"],
            processes_for=build,
            environment={},
            adopt_orphans=lambda: None,
        ))
        self.assertEqual(status, 128 + signal.SIGTERM)
        self.assertNotEqual(handler[0], signal.SIG_DFL)
        self.assertEqual(signal.getsignal(signal.SIGTERM), signal.SIG_DFL)


class ContractTests(unittest.TestCase):
    """The recipes and the CI job the lane runs under."""

    def setUp(self) -> None:
        self.workflow = (REPOSITORY / ".github/workflows/check.yaml").read_text()
        self.justfile = (REPOSITORY / "justfile").read_text()
        dumped = subprocess.run(
            ["just", "--dump", "--dump-format", "json"],
            cwd=REPOSITORY, capture_output=True, text=True, check=True,
        )
        self.recipes = json.loads(dumped.stdout)["recipes"]

    def dependencies(self, recipe: str) -> list[str]:
        return [str(dependency["recipe"]) for dependency in self.recipes[recipe]["dependencies"]]

    def test_each_selection_is_its_prerequisites_then_the_lane_alone(self) -> None:
        for selection in ("deloxide", "deloxide-order"):
            with self.subTest(selection=selection):
                self.assertEqual(self.dependencies(f"test-{selection}"), ["tests-deps", f"test-{selection}-workloads"])
                self.assertEqual(self.dependencies(f"test-{selection}-workloads"), [])
                body = json.dumps(self.recipes[f"test-{selection}-workloads"]["body"])
                self.assertIn("scripts.deloxide_lane", body)
                self.assertIn(f"run {selection}", body)
        self.assertEqual(self.dependencies("test-deloxide-qualification"), ["build-deadlock-report"])
        self.assertEqual(self.dependencies("validate-deloxide-applicability"), ["ratchet"])
        for target in ("validate-targets", "validate-ci-targets"):
            self.assertIn("validate-deloxide-applicability", self.dependencies(target))

    def test_the_ci_lane_runs_both_selections_bounded_with_their_evidence_retained(self) -> None:
        match = re.search(r"^  deloxide:\n(?P<body>(?:    .*\n|\n)+)", self.workflow, re.MULTILINE)
        assert match is not None
        job = match.group("body")
        self.assertIn("selection: [deloxide, deloxide-order]", job)
        self.assertIn("fail-fast: false", job)
        job_limit = int(re.search(r"timeout-minutes: (\d+)", job).group(1))  # type: ignore[union-attr]
        lane_limit = int(re.search(r'--kill-after=60 (\d+)m just coverage-native-extras "test-\$\{SELECTION\}"', job).group(1))  # type: ignore[union-attr]
        qualification_limit = int(re.search(r'--kill-after=60 (\d+)m just test-deloxide-qualification "\$\{SELECTION\}"', job).group(1))  # type: ignore[union-attr]
        inventory = deloxide_lane.load_inventory(REPOSITORY)
        # The inventory's budget is the diagnostic part of the lane's bound; preparation and
        # export share the rest, and setup and the uploads the job's remaining minutes.
        self.assertLess(inventory.bounds.budget_seconds / 60, lane_limit)
        self.assertLessEqual(lane_limit + 1 + qualification_limit + 1, job_limit - 10)
        self.assertIn("if: always()", job[job.index("Upload diagnostic lane evidence"):])
        self.assertIn("target/native-coverage-build-${{ matrix.selection }}/test-deloxide/", job)
        self.assertIn("target/deloxide/test-deloxide/", job)
        needs = re.search(r"^  coverage:\n(?:    .*\n|\n)+", self.workflow, re.MULTILINE)
        assert needs is not None
        self.assertNotIn("deloxide", needs.group(0))


if __name__ == "__main__":
    unittest.main()
