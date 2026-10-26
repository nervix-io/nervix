"""Completion evidence owned by the canonical Shuttle and Loom command runners.

This bookkeeping runs outside the modeled process. It observes harness records and supplies no
ordering, wakeups or decisions to a production invariant.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Mapping, Sequence

VARIABLE = "NERVIX_MODEL_EVIDENCE"
FILENAME = "models.json"


class EvidenceError(Exception):
    """A canonical runner did not retain complete evidence for this selection."""


class Evidence:
    def __init__(self, path: Path | None, mode: str, filter_text: str, inventory: Path) -> None:
        self.path = path
        self.content: dict[str, object] = {
            "mode": mode,
            "filter": filter_text,
            "inventory": str(inventory),
            "verdict": "running",
            "discovery": [],
            "selection": [],
            "checks": [],
        }
        self.checks: list[dict[str, object]] = []
        self.write()

    def write(self) -> None:
        self.content["checks"] = self.checks
        self.content["executed"] = len(self.checks)
        self.content["completed"] = sum(check["completed"] is True for check in self.checks)
        if self.path is not None:
            temporary = self.path.with_name(f".{self.path.name}.tmp")
            temporary.write_text(json.dumps(self.content, indent=2) + "\n", encoding="utf-8")
            os.replace(temporary, self.path)

    def discover(self, checks: Sequence[Mapping[str, object]]) -> None:
        self.content["discovery"] = list(checks)
        self.content["discovered"] = len(checks)
        self.write()

    def select(self, checks: Sequence[Mapping[str, object]]) -> None:
        self.content["selection"] = list(checks)
        self.content["selected"] = len(checks)
        self.write()

    def begin(self, identity: Mapping[str, object]) -> dict[str, object]:
        check = {**identity, "runs": [], "completed": False}
        self.checks.append(check)
        self.write()
        return check

    def finish(self, status: int) -> None:
        self.content["verdict"] = "complete" if status == 0 else "failed"
        self.write()


def identity(check: Mapping[str, object]) -> tuple[object, object, object]:
    return (check.get("package"), check.get("test"), check.get("invariant"))


def read_complete(path: Path, mode: str, filter_text: str) -> dict[str, object]:
    try:
        content = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise EvidenceError(f"cannot read canonical model evidence at {path}: {error}") from error
    if not isinstance(content, dict):
        raise EvidenceError(f"canonical model evidence at {path} is not an object")
    if content.get("mode") != mode or content.get("filter") != filter_text:
        raise EvidenceError("canonical model evidence has a different mode or filter")
    if content.get("verdict") != "complete":
        raise EvidenceError("canonical model exploration did not complete")
    discovery = content.get("discovery")
    selection = content.get("selection")
    checks = content.get("checks")
    if not all(isinstance(items, list) for items in (discovery, selection, checks)):
        raise EvidenceError("canonical model evidence has no discovery, selection or executions")
    if not selection or not all(isinstance(item, dict) for item in [*discovery, *selection, *checks]):
        raise EvidenceError("canonical model evidence has an empty or malformed selection")
    for check in [*discovery, *selection, *checks]:
        for field in ("package", "test"):
            if not isinstance(check.get(field), str) or not check[field]:
                raise EvidenceError(f"canonical model evidence lacks a required {field} identity")
        invariant = check.get("invariant")
        if invariant is not None and not isinstance(invariant, str):
            raise EvidenceError("canonical model evidence has a malformed invariant identity")
    for check in discovery:
        if type(check.get("ignored")) is not bool:
            raise EvidenceError("canonical discovery lacks the ignored-test verdict")
    selected = [identity(check) for check in selection]
    discovered = {identity(check) for check in discovery}
    executed = [identity(check) for check in checks]
    if len(set(selected)) != len(selected) or executed != selected or not set(selected) <= discovered:
        raise EvidenceError("canonical model executions do not match the discovered selection")
    if any(check["ignored"] and identity(check) in set(selected) for check in discovery):
        raise EvidenceError("canonical selection includes an ignored model")
    expected = {
        "discovered": len(discovery),
        "selected": len(selection),
        "executed": len(checks),
        "completed": len(checks),
    }
    if any(type(content.get(key)) is not int or content[key] != value for key, value in expected.items()):
        raise EvidenceError("canonical model completion counts do not match the selection")
    required = ["exploration", "nondeterminism"] if mode == "shuttle" else ["exploration"]
    for check in checks:
        runs = check.get("runs")
        if check.get("completed") is not True or not isinstance(runs, list):
            raise EvidenceError("canonical model has an incomplete exploration")
        if not all(isinstance(run, dict) for run in runs):
            raise EvidenceError("canonical model has malformed run evidence")
        if [run.get("name") for run in runs] != required:
            raise EvidenceError("canonical model is missing a required exploration or nondeterminism run")
        for run in runs:
            if run.get("completed") is not True or type(run.get("exit_status")) is not int or run["exit_status"] != 0:
                raise EvidenceError("canonical model has a failed exploration or nondeterminism run")
            if mode == "loom" and (
                not isinstance(check.get("invariant"), str) or not check["invariant"]
                or type(run.get("executions")) is not int or run["executions"] <= 0
                or not isinstance(run.get("bounds"), str) or not run["bounds"]
            ):
                raise EvidenceError("canonical Loom evidence lacks its invariant, executions or bounds")
            if mode == "shuttle":
                records = run.get("records")
                if not isinstance(records, list) or not records or not all(isinstance(record, str) and record for record in records):
                    raise EvidenceError("canonical Shuttle evidence lacks its harness completion records")
    return content
