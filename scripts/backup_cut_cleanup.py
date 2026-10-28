"""Qualify backup fault cleanup without turning deliberately failed bodies into passing features.

This supervision check keeps its own verdict outside ordinary coverage. Its child runs the real
Cucumber binary and after hook. The failing fixture lives outside the ordinary feature tree.
"""

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess


FEATURE = "Backup cut cleanup after a failed body"
FAILURE = "backup-cut cleanup qualification: deliberate body failure"
ROOT = Path(__file__).resolve().parents[1]
CASE_PATTERN = rf'feature="{FEATURE}" scenario="A failed (Plexus|command|CLI) backup body releases a (capture|lease) gate on ([13]) nodes"'


@dataclass(frozen=True)
class CleanupCase:
    kind: str
    boundary: str
    nodes: int

    @classmethod
    def from_line(cls, line: str) -> CleanupCase:
        match = re.search(CASE_PATTERN, line)
        if match is None:
            raise ValueError(f"unexpected cleanup scenario: {line}")
        return cls(match.group(1).lower(), match.group(2), int(match.group(3)))


def verify(returncode: int, output: str, lifecycle: str, summary: str) -> None:
    """Require the intended failed bodies and complete cleanup of every actual cluster."""
    expected = Counter(CleanupCase(kind, boundary, size)
                       for kind, boundary in (("plexus", "capture"), ("command", "lease"), ("cli", "capture"))
                       for size in (1, 3))
    count = sum(expected.values())
    if returncode != 101:
        raise ValueError(f"expected the Cucumber failure exit 101, got {returncode}")
    if FAILURE not in output or not re.search(rf"^{count} scenarios \({count} failed\)$", output, re.MULTILINE):
        raise ValueError("the deliberate body failures were not reported as failed scenarios")
    if re.search(r"\b[1-9]\d* (?:hook|parsing) error", output):
        raise ValueError("the child reported a hook or parsing failure")
    if f"Attempts: {count} completed, 0 active; retries: 0," not in summary:
        raise ValueError("the qualification did not complete its exact attempts without retries")
    finished = [line for line in lifecycle.splitlines() if "scenario finished:" in line]
    if len(finished) != count:
        raise ValueError(f"expected {count} finished after hooks, got {len(finished)}")
    observed = Counter()
    for line in finished:
        case = CleanupCase.from_line(line)
        observed[case] += 1
        size = case.nodes
        if FAILURE not in line or not re.search(
            rf"body=step failed: .* teardown=stopped {size} of {size} node\(s\).*, 0 still stopping$",
            line,
        ):
            raise ValueError(f"failed-body cleanup did not stop every node: {line}")
    if observed != expected:
        raise ValueError(f"wrong boundary/topology coverage: {observed}")
    background = Counter()
    for line in lifecycle.splitlines():
        if "backup cleanup finished:" not in line:
            continue
        case = CleanupCase.from_line(line)
        ending = f"kind={case.kind}"
        if case.kind != "plexus":
            ending += " succeeded=true"
        if not line.endswith(ending):
            raise ValueError(f"unexpected background cleanup: {line}")
        background[case] += 1
    if background != expected:
        raise ValueError(f"background backup work did not finish for every case: {background}")
    if "backup cleanup unfinished:" in lifecycle or "backup cleanup failed:" in lifecycle:
        raise ValueError("the child reported unfinished or failed background backup work")
    if "scenario cleanup unfinished:" in lifecycle or "scenario teardown failed:" in lifecycle:
        raise ValueError("the child reported an unfinished or panicked node during cleanup")


def scenario_executable(manifest: Path) -> Path:
    executables = set()
    for line in manifest.read_text().splitlines():
        artifact = json.loads(line)
        if (artifact.get("reason") == "compiler-artifact"
                and artifact["target"]["name"] == "scenarios" and artifact.get("executable")):
            executables.add(Path(artifact["executable"]))
    if len(executables) != 1:
        raise ValueError(f"expected one built scenarios executable, got {executables}")
    return executables.pop()


def qualify(evidence: Path) -> None:
    executable = scenario_executable(evidence / "build.jsonl")
    sources = ("src/fault_injection/backup_cut.rs", "src/fault_injection/backup_cut_tests.rs",
               "tests/scenarios.rs", "tests/backup/cleanup.rs", "tests/common/cluster.rs",
               "tests/common/cluster_teardown.rs", "tests/harness_liveness.rs",
               "tests/fixtures/backup_cut_cleanup.feature", "scripts/backup_cut_cleanup.py", "justfile")
    for name in sources:
        destination = evidence / "source" / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / name, destination)
    (evidence / "revision.txt").write_bytes(subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT))
    (evidence / "source.patch").write_bytes(subprocess.check_output(["git", "diff", "HEAD", "--"], cwd=ROOT))
    with executable.open("rb") as binary:
        digest = hashlib.file_digest(binary, "sha256").hexdigest()
    (evidence / "executable.sha256").write_text(f"{digest}  {executable}\n")
    command = [str(executable), "--input", "tests/fixtures/backup_cut_cleanup.feature",
               "--concurrency", "4", "--retry", "0", "--suite-budget", "6m"]
    (evidence / "command.json").write_text(json.dumps(command) + "\n")
    logs = ("cucumber.log", "scenarios.log", "suite-summary.md")
    previous = evidence / "previous-run"
    previous.mkdir()
    for name in logs:
        source = ROOT / "tests/logs" / name
        if source.exists():
            shutil.copyfile(source, previous / name)
    try:
        with (evidence / "output.log").open("w") as output:
            child = subprocess.run(command, cwd=ROOT, stdout=output, stderr=subprocess.STDOUT, timeout=900)
        (evidence / "exit.txt").write_text(f"{child.returncode}\n")
    finally:
        for name in logs:
            source = ROOT / "tests/logs" / name
            if source.exists():
                shutil.copyfile(source, evidence / name)
            retained = previous / name
            if retained.exists():
                shutil.copyfile(retained, source)
    verify(child.returncode, (evidence / "output.log").read_text(),
           (evidence / "cucumber.log").read_text(), (evidence / "suite-summary.md").read_text())
    (evidence / "complete.txt").write_text(
        "Six deliberately failed bodies; capture and lease gates on one and three nodes; "
        "all twelve nodes stopped; no retries.\n"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("evidence", type=Path)
    evidence = parser.parse_args().evidence.resolve()
    print(f"Backup cut cleanup evidence: {evidence}", flush=True)
    qualify(evidence)
    print((evidence / "complete.txt").read_text(), end="")


if __name__ == "__main__":
    main()
