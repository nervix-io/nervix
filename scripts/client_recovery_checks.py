"""Focused Nervix client evidence through the canonical model and diagnostic runners.

Complete inventories are validated before selecting the client package's Shuttle checks or all
registered workloads in the feature files reached by its changed lifecycle owners. These local
records never claim whole-workspace qualification. Normal CI uses the full model inventories.
"""

from __future__ import annotations

import argparse
from dataclasses import replace
import os
from pathlib import Path
import sys

from scripts import deloxide_lane, shuttle_checks

PACKAGE = "nervix-client-core"
OWNERS = (
    "crates/client-core/src/client.rs",
    "crates/client-core/src/exchange.rs",
)


def shuttle_inventory(inventory: shuttle_checks.Inventory) -> shuttle_checks.Inventory:
    if PACKAGE not in inventory.checks:
        raise shuttle_checks.RunnerError(f"no registered checks for {PACKAGE}")
    return shuttle_checks.Inventory({PACKAGE: inventory.checks[PACKAGE]})


def diagnostic_inventory(inventory: deloxide_lane.Inventory) -> deloxide_lane.Inventory:
    missing = set(OWNERS) - inventory.owners.keys()
    if missing:
        raise deloxide_lane.LaneError(f"client lifecycle owners are not registered: {sorted(missing)}")
    identities = {identity for path in OWNERS for identity in inventory.owners[path].workloads}
    features = {inventory.workloads[identity].feature for identity in identities}
    invocations = {inventory.workloads[identity].invocation for identity in identities}
    workloads = {
        name: item for name, item in inventory.workloads.items()
        if item.invocation in invocations and (item.feature in features or item.feature is None)
    }
    if not workloads or not identities <= workloads.keys():
        raise deloxide_lane.LaneError("client scope lost a registered lifecycle workload")
    selected = {}
    for name, item in inventory.invocations.items():
        if name not in invocations:
            continue
        if item.kind is deloxide_lane.Kind.CUCUMBER:
            inputs = tuple(sorted({workload.feature for workload in workloads.values()
                                   if workload.invocation == name and workload.feature is not None}))
            item = replace(item, inputs=inputs)
        selected[name] = item
    owners = {
        name: replace(item, workloads=tuple(identity for identity in item.workloads if identity in workloads))
        for name, item in inventory.owners.items() if set(item.workloads) & workloads.keys()
    }
    return replace(inventory, invocations=selected, workloads=workloads, owners=owners)


class ClientLane(deloxide_lane.Lane):
    def begin(self) -> None:
        super().begin()
        self.record.content["scope"] = {
            "owners": list(OWNERS),
            "features": sorted({item.feature for item in self.inventory.workloads.values()
                                if item.feature is not None}),
            "whole_workspace": False,
        }
        self.record.write()


def run_diagnostic(root: Path, target: Path, selection: str, environment: dict[str, str]) -> int:
    if deloxide_lane.COVERAGE_ATTEMPT_VARIABLE in environment:
        raise deloxide_lane.LaneError("a client-only diagnostic run cannot publish a whole-lane coverage record")
    inventory = diagnostic_inventory(deloxide_lane.load_inventory(root))
    processes = deloxide_lane.Processes(environment, inventory.bounds.stop_grace_seconds)
    host = deloxide_lane.host_triple(processes, root)
    workspace = deloxide_lane.resolve_workspace(root, target, environment, host)
    deloxide_lane.become_subreaper()
    with deloxide_lane.interruptible():
        return ClientLane(inventory, inventory.selection(selection), workspace, processes).execute()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("shuttle", "deloxide", "deloxide-order", "deloxide-stress", "deloxide-stress-restore"))
    parser.add_argument("--target-dir", type=Path, required=True)
    parser.add_argument("--filter", default="exchange::shuttle_tests::shuttle_a_foreign_reply_racing_registration_interrupts_every_waiter")
    arguments = parser.parse_args(argv)
    root = Path(__file__).resolve().parents[1]
    target = arguments.target_dir.resolve()
    target.mkdir(parents=True, exist_ok=True)
    print(f"Focused {arguments.mode} scope: {PACKAGE} lifecycle; not whole-workspace qualification", flush=True)
    if arguments.mode == "shuttle":
        full = shuttle_checks.parse_inventory((root / shuttle_checks.INVENTORY).read_text())
        return shuttle_checks.run_checks(shuttle_checks.Commands(root), shuttle_inventory(full), target,
                                         arguments.filter, target / "models.json")
    return run_diagnostic(root, target, arguments.mode, dict(os.environ))


if __name__ == "__main__":
    sys.exit(main())
