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
class Checkout:
    """The tested repository, which only an explicit fetch of commit history extends.

    A filtered fetch turns the checkout into a partial clone, and a partial clone answers a question
    about an object it lacks by fetching it. Every read passes `--no-lazy-fetch`, so a commit the
    checkout does not hold stays unknown instead of becoming a network question.
    """

    root: Path

    def git(self, *args: str) -> str:
        return subprocess.check_output(["git", "--no-lazy-fetch", *args], cwd=self.root, text=True).strip()

    def fetch_history(self, commit: str) -> None:
        """Complete the history a shallow checkout truncated, as commits without trees or contents."""
        if self.git("rev-parse", "--is-shallow-repository") == "true":
            self.git(
                "-c", "protocol.version=2", "fetch", "--quiet", "--no-tags", "--no-recurse-submodules",
                "--filter=tree:0", "--unshallow", "origin", commit,
            )

    def is_ancestor(self, ancestor: str, descendant: str) -> bool:
        """Decide from the descendant's complete local history, which holds every one of its ancestors."""
        known = subprocess.run(
            ["git", "--no-lazy-fetch", "rev-parse", "--verify", "--quiet", f"{ancestor}^{{commit}}"],
            cwd=self.root, stdout=subprocess.DEVNULL,
        )
        if known.returncode != 0:
            return False
        ancestry = subprocess.run(
            ["git", "--no-lazy-fetch", "merge-base", "--is-ancestor", ancestor, descendant], cwd=self.root,
        )
        if ancestry.returncode not in (0, 1):
            raise subprocess.CalledProcessError(ancestry.returncode, ancestry.args)
        return ancestry.returncode == 0

    def changed_paths(self, base: str, tested: str) -> tuple[str, ...]:
        changed = subprocess.check_output([
            "git", "--no-lazy-fetch", "diff", "--no-ext-diff", "--no-renames", "--name-only", "-z",
            base, tested, "--",
        ], cwd=self.root)
        return tuple(os.fsdecode(path) for path in changed.split(b"\0") if path)


@dataclasses.dataclass(frozen=True)
class TestSelection:
    event: str
    action: str | None
    tested_sha: str
    event_base_sha: str | None
    tested_base_sha: str | None
    head_sha: str | None
    paths: tuple[str, ...] | None
    labels: tuple[str, ...]

    @classmethod
    def read(cls, event: str, payload: dict, tested_sha: str, root: Path = Path(".")) -> TestSelection:
        tested_sha = commit_sha(tested_sha)
        if event != "pull_request":
            return cls(
                event=event, action=payload.get("action"), tested_sha=tested_sha, event_base_sha=None,
                tested_base_sha=None, head_sha=None, paths=None, labels=(),
            )
        pull = payload["pull_request"]
        event_base_sha = commit_sha(pull["base"]["sha"])
        head_sha = commit_sha(pull["head"]["sha"])
        checkout = Checkout(root)
        if checkout.git("rev-parse", "HEAD") != tested_sha:
            raise ValueError("checkout must be the event's tested merge")
        parents = checkout.git("show", "--no-patch", "--format=%P", tested_sha).split()
        if len(parents) != 2 or parents[1] != head_sha:
            raise ValueError("the tested merge must merge the event's exact head into one base")
        tested_base_sha = parents[0]
        # The event names the base GitHub recorded for the pull request: the base branch's tip when it
        # opened, or the newest base commit a later head merged. GitHub builds the tested merge on the
        # base branch's tip whenever the head changes, so a base branch that advanced in between leaves
        # a tested base that descends from the event's base.
        if tested_base_sha != event_base_sha:
            checkout.fetch_history(tested_base_sha)
            if not checkout.is_ancestor(event_base_sha, tested_base_sha):
                raise ValueError("the tested merge's base must be the event's base or descend from it")
        paths = checkout.changed_paths(tested_base_sha, tested_sha)
        labels = tuple(label["name"] for label in pull["labels"])
        return cls(
            event=event, action=payload.get("action"), tested_sha=tested_sha, event_base_sha=event_base_sha,
            tested_base_sha=tested_base_sha, head_sha=head_sha, paths=paths, labels=labels,
        )

    @property
    def documentation_only(self) -> bool:
        return self.event == "pull_request" and bool(self.paths) and all(
            documentation_path(path) for path in self.paths
        )

    @property
    def bolero_required(self) -> bool:
        """A label that requests a Bolero mode calls Bolero even for verified prose."""
        return not self.documentation_only or "bolero-random" in self.labels or "fuzz" in self.labels

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
            "event_base_sha": self.event_base_sha,
            "tested_base_sha": self.tested_base_sha,
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
