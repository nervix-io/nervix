"""Require the compiled scenario runner to refuse empty inputs and filters.

These expected failures qualify its selection guard, never an ordinary scenario workload. Their
profiles and logs stay separate, and the previous scenario logs are restored afterwards.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess

ROOT = Path(__file__).resolve().parents[1]
LOGS = ("cucumber.log", "scenarios.log", "suite-summary.md")


def verify(returncode: int, output: str) -> None:
    """Reject a successful empty run, unrelated failure, or any attempted scenario work."""
    if returncode != 101:
        raise ValueError(f"expected the empty-selection failure exit 101, got {returncode}")
    if "no scenarios selected" not in output:
        raise ValueError("the child did not report the selection guard's failure")
    if not re.search(r"^0 scenarios$", output, re.MULTILINE):
        raise ValueError("the child did not report zero scenarios")
    attempts = re.findall(
        r"^Attempts: (\d+) completed, (\d+) active; retries: (\d+)\.$", output, re.MULTILINE
    )
    if not attempts or any(count != ("0", "0", "0") for count in attempts):
        raise ValueError("the empty-selection control attempted scenario work")


def qualify(binary: Path, evidence: Path) -> None:
    evidence.mkdir(parents=True, exist_ok=True)
    if any(evidence.iterdir()):
        raise ValueError(f"selection evidence directory must be empty: {evidence}")
    with binary.open("rb") as executable:
        digest = hashlib.file_digest(executable, "sha256").hexdigest()
    report = {
        "kind": "expected-failure selection controls",
        "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "binary": str(binary), "sha256": digest, "status": "running",
        "supervisor_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "scenario_workloads_qualified": 0,
        "counts": {"discovered": 2, "selected": 2, "executed": 0, "completed": 0},
        "controls": [],
    }
    manifest = evidence / "qualification.json"
    fixture = evidence / "unselected.feature"
    fixture.write_text("Feature: selection control\n  Scenario: deliberately unselected\n    Given no step may execute\n")
    previous = evidence / "previous-logs"
    previous.mkdir()
    for name in LOGS:
        source = ROOT / "tests/logs" / name
        if source.exists():
            shutil.copyfile(source, previous / name)
    cases = (
        ("empty-glob", ["--input", str(evidence / "missing*.feature")]),
        ("empty-name", ["--input", str(fixture), "--name", "^__no_selection_guard_match__$"]),
    )
    try:
        for name, selection in cases:
            attempt = evidence / name
            attempt.mkdir()
            command = [str(binary), *selection, "--concurrency", "1", "--retry", "0", "--suite-budget", "30s"]
            record = {"name": name, "command": command, "passed": False}
            report["controls"].append(record)
            report["counts"]["executed"] += 1
            manifest.write_text(json.dumps(report, indent=2) + "\n")
            environment = dict(os.environ, LLVM_PROFILE_FILE=str(attempt / "qualification-%p-%m.profraw"))
            try:
                with (attempt / "output.log").open("w") as output:
                    child = subprocess.run(command, cwd=ROOT, env=environment, stdout=output,
                                           stderr=subprocess.STDOUT, timeout=90)
            finally:
                for log in LOGS:
                    source = ROOT / "tests/logs" / log
                    if source.exists():
                        shutil.copyfile(source, attempt / log)
            record["returncode"] = child.returncode
            report["counts"]["completed"] += 1
            verify(child.returncode, (attempt / "output.log").read_text())
            record["passed"] = True
        report["status"] = "complete"
    except Exception as error:
        report.update(status="failed", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        for name in LOGS:
            source = ROOT / "tests/logs" / name
            if (previous / name).exists():
                shutil.copyfile(previous / name, source)
            else:
                source.unlink(missing_ok=True)
        manifest.write_text(json.dumps(report, indent=2) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("evidence", type=Path)
    args = parser.parse_args()
    qualify(args.binary.resolve(), args.evidence.resolve())
    print((args.evidence / "qualification.json").read_text(), end="")


if __name__ == "__main__":
    main()
