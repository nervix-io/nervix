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
    build_budget_seconds = 1200
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
    id = "scenarios"
    inputs = ["features/**/*.feature"]
    kind = "cucumber"
    package = "nervix-scenarios"
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


def replaced(text: str, *replacements: tuple[str, str]) -> str:
    """`text` with each `(old, new)` pair replaced once; an `old` the text does not hold fails."""

    for old, new in replacements:
        if old not in text:
            raise AssertionError(f"the fixture holds no {old!r}")
        text = text.replace(old, new, 1)
    return text


# Two more selections, each compiling another selection's build under its own name, as a selection
# does whose workloads of that build run in a lane, budget and CI runner of their own. The probes
# and the scenarios run in all four; the owner tests stay with the first two.
SHARDS = replaced(
    INVENTORY,
    (
        '[[selection]]\nname = "deloxide-order"\nrecorded = "OrderAnalysis"\n',
        '[[selection]]\nname = "deloxide-order"\nrecorded = "OrderAnalysis"\n\n'
        '[[selection]]\nfeature = "deloxide"\nname = "deloxide-restore"\nrecorded = "ActiveOnly"\n\n'
        '[[selection]]\nfeature = "deloxide-order"\nname = "deloxide-order-restore"\n'
        'recorded = "OrderAnalysis"\n',
    ),
    ('id = "owner-tests"\n', 'id = "owner-tests"\nselections = ["deloxide", "deloxide-order"]\n'),
    (
        'invocation = "probes"\nselections = ["deloxide", "deloxide-order"]',
        'invocation = "probes"\nselections = ["deloxide", "deloxide-order", "deloxide-restore", '
        '"deloxide-order-restore"]',
    ),
    (
        'invocation = "probes"\nselections = ["deloxide-order"]',
        'invocation = "probes"\nselections = ["deloxide-order", "deloxide-order-restore"]',
    ),
    (
        'scenario = "Nodes on <nodes> nodes"\nselections = ["deloxide", "deloxide-order"]',
        'scenario = "Nodes on <nodes> nodes"\nselections = ["deloxide", "deloxide-order", '
        '"deloxide-restore", "deloxide-order-restore"]',
    ),
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
        name = name.removesuffix("-warm")
        if name == "probes-build":
            return artifact("test", "active_cycles", self.executable("probes"), self.root / "crates/deadlock/Cargo.toml")
        if name == "owner-tests-build":
            return artifact("lib", "nervix_server", self.executable("server"), self.root / "Cargo.toml")
        if name == "conformance-build":
            return artifact("lib", "nervix_primitives", self.executable("primitives"), self.root / "crates/primitives/Cargo.toml")
        if name == "scenarios-build":
            return artifact("test", "scenarios", self.executable("scenarios"), self.root / "Cargo.toml")
        if name == "nervix-paced-simulation-build":
            return artifact("bin", "nervix-paced-simulation", self.executable("driver"), self.root / "examples/paced-simulation/Cargo.toml", test=False)
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
    def test_stressed_lifecycle_phases_are_complete_disjoint_and_bounded(self) -> None:
        inventory = deloxide_lane.load_inventory(REPOSITORY)
        endpoint = inventory.invocations.get("stress-endpoint-acknowledgement")
        self.assertIsNotNone(endpoint, "the ten ACK examples need their own bounded phase")
        assert endpoint is not None
        retention = inventory.invocations.get("stress-transaction-retention")
        self.assertIsNotNone(retention, "the six retention examples need their own bounded phase")
        assert retention is not None
        baseline = inventory.invocations["stress-scenarios"]
        self.assertEqual(inventory.bounds.budget_seconds, 14400)
        self.assertEqual(baseline.bound_seconds, 3960)
        self.assertEqual(endpoint.bound_seconds, 900)
        self.assertEqual(retention.bound_seconds, 900)
        self.assertEqual(retention.concurrency, 1)
        phases = (baseline, endpoint, retention)
        groups = [
            deloxide_lane.discover_scenarios(REPOSITORY, invocation.inputs, invocation.tags)
            for invocation in phases
        ]
        identities = [{(scenario.feature, scenario.name) for scenario in group} for group in groups]
        for index, group in enumerate(identities):
            for other in identities[index + 1:]:
                self.assertTrue(group.isdisjoint(other))
        self.assertEqual([sum(scenario.runs for scenario in group) for group in groups], [216, 10, 6])
