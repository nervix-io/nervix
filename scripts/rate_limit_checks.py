"""Focused rate-limit evidence through the existing canonical model and diagnostic runners.

The complete inventories are validated first. Only this package's records are then selected;
these local results never claim whole-workspace qualification. Normal CI uses the full runners.
"""

from __future__ import annotations

import argparse
from dataclasses import replace
import os
from pathlib import Path
import sys

from scripts import bolero, deloxide_lane, loom_models, shuttle_checks

PACKAGE = "nervix-rate-limit"


def property_inventory(inventory: bolero.Inventory) -> bolero.Inventory:
    targets = tuple(item for item in inventory.targets if item.package == PACKAGE)
    if not targets:
        raise bolero.BoleroError(f"no registered properties for {PACKAGE}")
    return replace(inventory, prepared_targets=(), targets=targets)


def run_properties(root: Path) -> int:
    inventory = property_inventory(bolero.load_inventory())
    compiled = bolero.discover_package(PACKAGE, root / "crates/rate-limit/Cargo.toml", inventory.targets)
    registered = {(item.test_target, item.test): item for item in inventory.targets}
    identities = [(kind, item["test_name"]) for kind, item in compiled]
    if len(set(identities)) != len(identities) or set(identities) != set(registered):
        raise bolero.BoleroError("compiled engine properties differ from their registered targets")
    for kind, item in compiled:
        if Path(item["work_dir"]).resolve() != registered[(kind, item["test_name"])].work_dir.resolve():
            raise bolero.BoleroError("compiled engine property work directory differs from its register")
    bolero.test_targets(inventory, inventory.targets)
    return 0


def loom_inventory(inventory: loom_models.Inventory) -> loom_models.Inventory:
    invariants = tuple(item for item in inventory.invariants if item.package == PACKAGE)
    if not invariants:
        raise loom_models.RunnerError(f"no registered model for {PACKAGE}")
    identities = {item.id for item in invariants}
    qualifications = tuple(item for item in inventory.qualifications if item.invariant in identities)
    return loom_models.Inventory(invariants, qualifications)


def shuttle_inventory(inventory: shuttle_checks.Inventory) -> shuttle_checks.Inventory:
    if PACKAGE not in inventory.checks:
        raise shuttle_checks.RunnerError(f"no registered checks for {PACKAGE}")
    return shuttle_checks.Inventory({PACKAGE: inventory.checks[PACKAGE]})


def diagnostic_inventory(inventory: deloxide_lane.Inventory) -> deloxide_lane.Inventory:
    invocations = {name: item for name, item in inventory.invocations.items() if item.package == PACKAGE}
    if not invocations:
        raise deloxide_lane.LaneError(f"no registered invocation for {PACKAGE}")
    workloads = {name: item for name, item in inventory.workloads.items() if item.invocation in invocations}
    owners = {
        name: replace(item, workloads=tuple(workload for workload in item.workloads if workload in workloads))
        for name, item in inventory.owners.items()
        if set(item.workloads) & workloads.keys()
    }
    return replace(inventory, invocations=invocations, workloads=workloads, owners=owners)


class PackageLane(deloxide_lane.Lane):
    def begin(self) -> None:
        super().begin()
        self.record.content["scope"] = {"package": PACKAGE, "whole_workspace": False}
        self.record.write()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("properties", "loom", "loom-qualification", "shuttle", "deloxide", "deloxide-order", "deloxide-stress", "deloxide-stress-restore"))
    parser.add_argument("--target-dir", type=Path, required=True)
    arguments = parser.parse_args(argv)
    root = Path(__file__).resolve().parents[1]
    target = arguments.target_dir.resolve()
    target.mkdir(parents=True, exist_ok=True)
    print(f"Focused {arguments.mode} scope: {PACKAGE}; not whole-workspace qualification", flush=True)
    if arguments.mode == "properties":
        return run_properties(root)
    if arguments.mode.startswith("loom"):
        full = loom_models.parse_inventory((root / loom_models.INVENTORY).read_text())
        inventory = loom_inventory(full)
        commands = loom_models.Commands(root)
        if arguments.mode == "loom-qualification":
            return loom_models.qualify(commands, inventory, target)
        return loom_models.run_models(commands, inventory, target, "rate-limit.", target / "models.json")
    if arguments.mode == "shuttle":
        full = shuttle_checks.parse_inventory((root / shuttle_checks.INVENTORY).read_text())
        inventory = shuttle_inventory(full)
        return shuttle_checks.run_checks(shuttle_checks.Commands(root), inventory, target, "shuttle_key_leases_", target / "models.json")
    environment = dict(os.environ)
    if deloxide_lane.COVERAGE_ATTEMPT_VARIABLE in environment:
        raise deloxide_lane.LaneError("a package-only diagnostic run cannot publish a whole-lane coverage record")
    inventory = diagnostic_inventory(deloxide_lane.load_inventory(root))
    processes = deloxide_lane.Processes(environment, inventory.bounds.stop_grace_seconds)
    host = deloxide_lane.host_triple(processes, root)
    workspace = deloxide_lane.resolve_workspace(root, target, environment, host)
    deloxide_lane.become_subreaper()
    with deloxide_lane.interruptible():
        return PackageLane(inventory, inventory.selection(arguments.mode), workspace, processes).execute()


if __name__ == "__main__":
    sys.exit(main())
