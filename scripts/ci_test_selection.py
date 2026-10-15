"""Select CI suites from the exact tested merge, independently of execution results."""

from __future__ import annotations

import argparse
import dataclasses
import json
import os
from pathlib import Path
import re
import subprocess


def documentation_path(path: str) -> bool:
    """Only known prose locations qualify; build inputs and test fixtures require suites."""
    if path in {"AGENTS.md", "README.md"}:
        return True
    return path.endswith(".md") and path.startswith(("docs/", ".agents/skills/"))


def commit_sha(value: str) -> str:
    if not re.fullmatch(r"[0-9a-f]{40}", value):
        raise ValueError("selection requires a full GitHub commit SHA")
    return value


@dataclasses.dataclass(frozen=True)
class TestSelection:
    event: str
    action: str | None
    tested_sha: str
    base_sha: str | None
    head_sha: str | None
    paths: tuple[str, ...] | None
    labels: tuple[str, ...]

    @classmethod
    def read(cls, event: str, payload: dict, tested_sha: str, root: Path = Path(".")) -> TestSelection:
        tested_sha = commit_sha(tested_sha)
        if event != "pull_request":
            return cls(event, payload.get("action"), tested_sha, None, None, None, ())
        pull = payload["pull_request"]
        base_sha = commit_sha(pull["base"]["sha"])
        head_sha = commit_sha(pull["head"]["sha"])
        checkout = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
        parents = subprocess.check_output(
            ["git", "show", "--no-patch", "--format=%P", tested_sha], cwd=root, text=True,
        ).split()
        if checkout != tested_sha or parents != [base_sha, head_sha]:
            raise ValueError("checkout must be the event's tested merge of its exact base and head")
        changed = subprocess.check_output([
            "git", "diff", "--no-ext-diff", "--no-renames", "--name-only", "-z",
            base_sha, tested_sha, "--",
        ], cwd=root)
        paths = tuple(os.fsdecode(path) for path in changed.split(b"\0") if path)
        labels = tuple(label["name"] for label in pull["labels"])
        return cls(event, payload.get("action"), tested_sha, base_sha, head_sha, paths, labels)

    @property
    def documentation_only(self) -> bool:
        return self.event == "pull_request" and bool(self.paths) and all(
            documentation_path(path) for path in self.paths
        )

    @property
    def bolero_required(self) -> bool:
        return not self.documentation_only or "fuzz" in self.labels

    def record(self) -> dict:
        return {
            "producer": "ci-test-selection",
            "verdict": "selected",
            "run": os.environ["GITHUB_RUN_ID"],
            "attempt": os.environ["GITHUB_RUN_ATTEMPT"],
            "job": os.environ["GITHUB_JOB"],
            "event": self.event,
            "action": self.action,
            "tested_sha": self.tested_sha,
            "base_sha": self.base_sha,
            "head_sha": self.head_sha,
            "paths": self.paths,
            "labels": self.labels,
            "documentation_only": self.documentation_only,
            "tests_required": not self.documentation_only,
            "bolero_required": self.bolero_required,
            "reason": "verified-documentation-only" if self.documentation_only else "suites-required",
        }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--record", type=Path, required=True)
    args = parser.parse_args()
    payload = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
    selection = TestSelection.read(
        os.environ["GITHUB_EVENT_NAME"], payload, os.environ["GITHUB_SHA"],
        Path(os.environ["GITHUB_WORKSPACE"]),
    )
    record = selection.record()
    args.record.parent.mkdir(parents=True, exist_ok=True)
    args.record.write_text(json.dumps(record, indent=2) + "\n")
    with open(os.environ["GITHUB_OUTPUT"], "a") as output:
        output.write(f"tests-required={str(not selection.documentation_only).lower()}\n")
        output.write(f"bolero-required={str(selection.bolero_required).lower()}\n")
    print(json.dumps(record, indent=2))


if __name__ == "__main__":
    main()
