#!/usr/bin/env python3

"""Decide whether one revision holds the complete verification evidence a release needs.

`tests/release-evidence.toml` declares the evidence methods a release is qualified by: the ordinary
checks every pull request runs, and the label-gated jobs of the Loom, Shuttle, Turmoil, Deloxide,
Bolero sanitizer, client conformance and external Chaos methods, the Chaos ones against both the
release image and the Deloxide diagnostic image of the revision. Each method names the label that
makes CI run it and the checks it must leave behind; a sharded check names the prefix of a matrix
whose shards must all be present.

`check --pr N` reads the pull request's head revision and labels and every check run GitHub recorded
on that revision. A revision qualifies only when the pull request carries every method's label and
the latest run of every required check, and of every shard of a sharded one, completed successfully
on it: a check that is missing, still running, skipped, cancelled, timed out or failed refuses the
revision, and so does a sharded check whose shards are not numbered completely. It also refuses the
revision while an owner of tracked locks in `tests/deloxide-inventory.toml` records a gap, a path the
diagnostic lane does not reach, instead of being reached by registered workloads alone.

It writes the register, every requirement with its verdict and every check with its conclusion, run
URL and times, to `target/release-evidence/<revision>/register.json` and `register.md`, and exits 0
when the evidence is complete, 1 when it is not, and 2 for a usage, inventory or GitHub error. The
register is evidence for the task that qualifies the revision; it does not belong in the repository.

Run it as `python3 -m scripts.release_evidence check --pr <number>`.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import tomllib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

INVENTORY = Path("tests/release-evidence.toml")
DELOXIDE_INVENTORY = Path("tests/deloxide-inventory.toml")
OUTPUT = Path("target/release-evidence")
# The repository the checks and pull requests belong to.
REPOSITORY = "nervix-io/nervix"
# GitHub returns at most this many check runs per page.
PAGE_SIZE = 100
# A pull request's head carries one run of each check per workflow attempt; this bounds how many
# pages a revision's check runs may take before the register refuses to read on.
MAX_PAGES = 20


class EvidenceError(Exception):
    """An inventory, input or GitHub answer the gate cannot judge."""


@dataclass(frozen=True)
class Method:
    name: str
    description: str
    label: str | None
    checks: tuple[str, ...]
    sharded: tuple[str, ...]


@dataclass(frozen=True)
class CheckRun:
    id: int
    name: str
    status: str
    conclusion: str | None
    started_at: str
    completed_at: str | None
    url: str

    def passed(self) -> bool:
        return self.status == "completed" and self.conclusion == "success"

    def verdict(self) -> str:
        if self.status != "completed":
            return self.status
        return self.conclusion or "unknown"


@dataclass(frozen=True)
class Requirement:
    """One required check, or one shard of a sharded check, with what the revision holds for it."""

    method: str
    check: str
    run: CheckRun | None
    problem: str | None


def parse_inventory(text: str) -> tuple[Method, ...]:
    document = tomllib.loads(text)
    unknown = set(document) - {"method"}
    if unknown:
        raise EvidenceError(f"the release evidence inventory has unknown tables: {sorted(unknown)}")
    tables = document.get("method")
    if not isinstance(tables, list) or not tables:
        raise EvidenceError("the release evidence inventory declares no method")
    methods: list[Method] = []
    names: set[str] = set()
    for index, table in enumerate(tables):
        where = f"method {index + 1}"
        if not isinstance(table, dict):
            raise EvidenceError(f"{where} is not a table")
        unknown = set(table) - {"name", "description", "label", "checks", "sharded"}
        if unknown:
            raise EvidenceError(f"{where} has unknown keys: {sorted(unknown)}")
        name = table.get("name")
        if not isinstance(name, str) or not re.fullmatch(r"[a-z][a-z0-9-]*", name):
            raise EvidenceError(f"{where} needs a lowercase name")
        if name in names:
            raise EvidenceError(f"method {name} is declared twice")
        names.add(name)
        description = table.get("description")
        if not isinstance(description, str) or not description.strip():
            raise EvidenceError(f"method {name} needs a description")
        label = table.get("label")
        if label is not None and (not isinstance(label, str) or not label.strip()):
            raise EvidenceError(f"method {name} has an empty label")
        checks = _strings(table.get("checks", []), f"the checks of method {name}")
        sharded = _strings(table.get("sharded", []), f"the sharded checks of method {name}")
        if not checks and not sharded:
            raise EvidenceError(f"method {name} requires no check")
        methods.append(Method(name, description.strip(), label, checks, sharded))
    return tuple(methods)


def _strings(value: object, what: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not all(isinstance(item, str) and item for item in value):
        raise EvidenceError(f"{what} must be a list of check names")
    if len(set(value)) != len(value):
        raise EvidenceError(f"{what} name a check twice")
    return tuple(value)


def owner_gaps(text: str) -> tuple[str, ...]:
    """The owners of tracked locks whose record names a path the diagnostic lane does not reach."""

    document = tomllib.loads(text)
    owners = document.get("owner", [])
    if not isinstance(owners, list):
        raise EvidenceError("the Deloxide inventory's owners are not a list of tables")
    gaps: list[str] = []
    for owner in owners:
        if not isinstance(owner, dict) or not isinstance(owner.get("path"), str):
            raise EvidenceError("a Deloxide owner record names no path")
        if owner.get("gap") is not None:
            gaps.append(owner["path"])
    return tuple(sorted(gaps))


def parse_check_runs(pages: Sequence[Mapping[str, object]]) -> tuple[CheckRun, ...]:
    runs: list[CheckRun] = []
    for page in pages:
        entries = page.get("check_runs")
        if not isinstance(entries, list):
            raise EvidenceError("a page of check runs holds no check_runs list")
        for entry in entries:
            if not isinstance(entry, dict):
                raise EvidenceError("a check run is not an object")
            try:
                runs.append(
                    CheckRun(
                        id=int(entry["id"]),
                        name=str(entry["name"]),
                        status=str(entry["status"]),
                        conclusion=None if entry.get("conclusion") is None else str(entry["conclusion"]),
                        started_at=str(entry["started_at"]),
                        completed_at=None if entry.get("completed_at") is None else str(entry["completed_at"]),
                        url=str(entry.get("html_url") or ""),
                    )
                )
            except (KeyError, TypeError, ValueError) as error:
                raise EvidenceError(f"a check run lacks a field the register needs: {error}") from error
    return tuple(runs)


def latest_runs(runs: Sequence[CheckRun]) -> dict[str, CheckRun]:
    """The most recent run of each check: a rerun of a failed job supersedes the run it repeats."""

    latest: dict[str, CheckRun] = {}
    for run in runs:
        current = latest.get(run.name)
        if current is None or (run.started_at, run.id) > (current.started_at, current.id):
            latest[run.name] = run
    return latest


def shard_requirements(method: str, prefix: str, latest: Mapping[str, CheckRun]) -> list[Requirement]:
    """Every shard of the matrix check `prefix (I of N)`, which needs shards 1 through N of one N."""

    pattern = re.compile(re.escape(prefix) + r" \((\d+) of (\d+)\)")
    shards: dict[int, CheckRun] = {}
    totals: set[int] = set()
    for name, run in latest.items():
        match = pattern.fullmatch(name)
        if match is None:
            continue
        shards[int(match.group(1))] = run
        totals.add(int(match.group(2)))
    if not shards:
        return [Requirement(method, f"{prefix} (every shard)", None, "no shard of this matrix ran on the revision")]
    if len(totals) != 1:
        return [
            Requirement(
                method,
                f"{prefix} (every shard)",
                None,
                f"its shards name different totals {sorted(totals)}, so the matrix is not one run",
            )
        ]
    total = totals.pop()
    requirements: list[Requirement] = []
    for shard in range(1, total + 1):
        name = f"{prefix} ({shard} of {total})"
        run = shards.get(shard)
        requirements.append(Requirement(method, name, run, _problem(run)))
    for shard in sorted(set(shards) - set(range(1, total + 1))):
        requirements.append(
            Requirement(method, f"{prefix} ({shard} of {total})", shards[shard], "a shard outside its matrix ran")
        )
    return requirements


def _problem(run: CheckRun | None) -> str | None:
    if run is None:
        return "it did not run on the revision"
    if run.passed():
        return None
    return f"its latest run is {run.verdict()}"


def judge(
    methods: Sequence[Method],
    labels: frozenset[str],
    runs: Sequence[CheckRun],
    gaps: Sequence[str],
) -> tuple[list[Requirement], list[str]]:
    """Every requirement with its verdict, and every reason the revision does not qualify."""

    latest = latest_runs(runs)
    requirements: list[Requirement] = []
    problems: list[str] = []
    for method in methods:
        if method.label is not None and method.label not in labels:
            problems.append(
                f"{method.name}: the pull request does not carry the `{method.label}` label, so CI never ran it"
            )
        for check in method.checks:
            run = latest.get(check)
            requirements.append(Requirement(method.name, check, run, _problem(run)))
        for prefix in method.sharded:
            requirements.extend(shard_requirements(method.name, prefix, latest))
    for requirement in requirements:
        if requirement.problem is not None:
            problems.append(f"{requirement.method}: `{requirement.check}`: {requirement.problem}")
    for path in gaps:
        problems.append(
            f"compliance: the tracked-lock owner {path} records a path the diagnostic lane does not reach"
        )
    return requirements, problems


def register(
    pull_request: int,
    revision: str,
    labels: frozenset[str],
    methods: Sequence[Method],
    requirements: Sequence[Requirement],
    gaps: Sequence[str],
    problems: Sequence[str],
) -> dict[str, object]:
    return {
        "pull_request": pull_request,
        "revision": revision,
        "labels": sorted(labels),
        "verdict": "qualified" if not problems else "refused",
        "methods": [
            {
                "name": method.name,
                "description": method.description,
                "label": method.label,
                "checks": [
                    {
                        "check": requirement.check,
                        "verdict": "passed" if requirement.problem is None else "refused",
                        "problem": requirement.problem,
                        "conclusion": None if requirement.run is None else requirement.run.verdict(),
                        "url": None if requirement.run is None else requirement.run.url,
                        "started_at": None if requirement.run is None else requirement.run.started_at,
                        "completed_at": None if requirement.run is None else requirement.run.completed_at,
                    }
                    for requirement in requirements
                    if requirement.method == method.name
                ],
            }
            for method in methods
        ],
        "compliance": {"deloxide_owner_gaps": list(gaps)},
        "problems": list(problems),
    }


def render_markdown(record: Mapping[str, object]) -> str:
    lines = [
        f"## Release evidence of revision `{record['revision']}`: {record['verdict']}",
        "",
        f"Pull request #{record['pull_request']}, labels: "
        + (", ".join(f"`{label}`" for label in record["labels"]) or "none"),  # type: ignore[union-attr]
        "",
        "| Method | Check | Verdict | Conclusion | Completed |",
        "| --- | --- | --- | --- | --- |",
    ]
    for method in record["methods"]:  # type: ignore[union-attr]
        for check in method["checks"]:
            name = check["check"]
            if check["url"]:
                name = f"[{name}]({check['url']})"
            lines.append(
                f"| {method['name']} | {name} | {check['verdict']} | {check['conclusion'] or '—'} "
                f"| {check['completed_at'] or '—'} |"
            )
    problems = record["problems"]
    if problems:
        lines += ["", "Refused because:", ""]
        lines += [f"- {problem}" for problem in problems]  # type: ignore[union-attr]
    return "\n".join(lines) + "\n"


def run_gh(arguments: Sequence[str]) -> str:
    try:
        completed = subprocess.run(
            ["gh", *arguments], check=True, capture_output=True, text=True, timeout=120
        )
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
        detail = getattr(error, "stderr", "") or str(error)
        raise EvidenceError(f"gh {' '.join(arguments)} failed: {detail.strip()}") from error
    return completed.stdout


def pull_request_head(number: int, gh: Callable[[Sequence[str]], str]) -> tuple[str, frozenset[str]]:
    answer = json.loads(
        gh(["pr", "view", str(number), "--repo", REPOSITORY, "--json", "headRefOid,labels"])
    )
    revision = answer.get("headRefOid")
    if not isinstance(revision, str) or not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise EvidenceError(f"pull request {number} has no head revision")
    labels = frozenset(label["name"] for label in answer.get("labels", []))
    return revision, labels


def check_run_pages(revision: str, gh: Callable[[Sequence[str]], str]) -> list[Mapping[str, object]]:
    pages: list[Mapping[str, object]] = []
    for page in range(1, MAX_PAGES + 1):
        answer = json.loads(
            gh(
                [
                    "api",
                    f"repos/{REPOSITORY}/commits/{revision}/check-runs?per_page={PAGE_SIZE}&page={page}",
                ]
            )
        )
        pages.append(answer)
        entries = answer.get("check_runs")
        if not isinstance(entries, list) or len(entries) < PAGE_SIZE:
            return pages
    raise EvidenceError(f"revision {revision} holds more than {MAX_PAGES * PAGE_SIZE} check runs")


def check(
    number: int,
    root: Path,
    gh: Callable[[Sequence[str]], str] | None = None,
) -> int:
    if gh is None:
        gh = run_gh
    methods = parse_inventory((root / INVENTORY).read_text(encoding="utf-8"))
    gaps = owner_gaps((root / DELOXIDE_INVENTORY).read_text(encoding="utf-8"))
    revision, labels = pull_request_head(number, gh)
    runs = parse_check_runs(check_run_pages(revision, gh))
    requirements, problems = judge(methods, labels, runs, gaps)
    record = register(number, revision, labels, methods, requirements, gaps, problems)
    directory = root / OUTPUT / revision
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "register.json").write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    markdown = render_markdown(record)
    (directory / "register.md").write_text(markdown, encoding="utf-8")
    sys.stdout.write(markdown)
    print(f"register: {directory / 'register.json'}")
    return 0 if not problems else 1


def main(arguments: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    check_parser = commands.add_parser("check", help="judge the evidence of a pull request's head revision")
    check_parser.add_argument("--pr", type=int, required=True)
    check_parser.add_argument("--root", type=Path, default=Path("."))
    options = parser.parse_args(arguments)
    try:
        return check(options.pr, options.root)
    except (EvidenceError, OSError, tomllib.TOMLDecodeError, json.JSONDecodeError) as error:
        print(f"release evidence error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
