#!/usr/bin/env python3
"""Report added executable line coverage from LCOV; publication and coverage are advisory.

`just coverage-patch <base> <report> [--report <another-report>]` measures the committed
patch from its merge base to HEAD. `--pr <number>` publishes through the authenticated
GitHub CLI. Reports must describe the checked-out sources, including line coordinates.
"""

from __future__ import annotations

import argparse
import ast
import html
import json
import os
import re
import subprocess
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

from scripts.native_coverage import RunnerError, lcov_records

COMMENT_MARKER = "<!-- nervix-patch-coverage -->"
ROOT = Path(__file__).resolve().parent.parent
HUNK = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@")
FILE_LIMIT = 100
RANGE_LIMIT = 20


def command(arguments: Sequence[str], *, root: Path = ROOT, input_text: str | None = None) -> str:
    result = subprocess.run(
        arguments, cwd=root, input=input_text, capture_output=True, text=True, timeout=60
    )
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or f"{arguments[0]} exited {result.returncode}")
    return result.stdout


@dataclass(frozen=True)
class Patch:
    base: str
    head: str
    lines: dict[str, set[int]]

    @classmethod
    def load(cls, root: Path, base: str, head: str) -> Patch:
        head_sha = command(["git", "rev-parse", "--verify", "--end-of-options", f"{head}^{{commit}}"], root=root).strip()
        base_sha = command(["git", "rev-parse", "--verify", "--end-of-options", f"{base}^{{commit}}"], root=root).strip()
        merge_base = command(["git", "merge-base", base_sha, head_sha], root=root).strip()
        diff = command(
            ["git", "-c", "core.quotePath=false", "diff", "--no-ext-diff", "--no-textconv",
             "--no-color", "--find-renames", "--unified=0", "--inter-hunk-context=0",
             "--src-prefix=a/", "--dst-prefix=b/", merge_base, head_sha, "--"],
            root=root,
        )
        added: dict[str, set[int]] = {}
        source: str | None = None
        header = False
        for line in diff.splitlines():
            if line.startswith("diff --git "):
                source = None
                header = True
            elif header and line.startswith("+++ "):
                path = line[4:].split("\t", 1)[0]
                if path.startswith('"'):
                    path = ast.literal_eval(path)
                if path != "/dev/null":
                    source = path.removeprefix("b/")
            elif match := HUNK.match(line):
                header = False
                count = int(match[2]) if match[2] is not None else 1
                if source is not None and count:
                    start = int(match[1])
                    added.setdefault(source, set()).update(range(start, start + count))
        return cls(merge_base, head_sha, added)


@dataclass(frozen=True)
class Coverage:
    lines: dict[str, dict[int, int]]

    @classmethod
    def load(cls, root: Path, reports: Sequence[Path], source_root: Path | None = None) -> Coverage:
        tracked = set(command(["git", "ls-files", "-z"], root=root).split("\0"))
        source_root = (source_root or root).resolve()
        merged: dict[str, dict[int, int]] = {}
        for report in reports:
            with (root / report).open(encoding="utf-8") as content:
                for record in lcov_records(content):
                    path = Path(record.source)
                    if path.is_absolute():
                        if not path.is_relative_to(source_root):
                            continue
                        path = path.relative_to(source_root)
                    source = path.as_posix()
                    if source not in tracked:
                        continue
                    lines = merged.setdefault(source, {})
                    for entry in record.lines:
                        if not entry.startswith("DA:"):
                            continue
                        fields = entry[3:].strip().split(",")
                        number, hits = int(fields[0]), int(fields[1])
                        if number < 1 or hits < 0:
                            raise ValueError(f"invalid LCOV line counter: {entry.strip()}")
                        lines[number] = max(lines.get(number, 0), hits)
        return cls(merged)

    def measure(self, patch: Patch) -> Measurement:
        files = []
        for path, added in sorted(patch.lines.items()):
            data = self.lines.get(path)
            if data is None:
                files.append(FileCoverage(path, None, 0, ()))
                continue
            executable = added.intersection(data)
            uncovered = tuple(sorted(number for number in executable if data[number] == 0))
            files.append(FileCoverage(path, len(executable), len(executable) - len(uncovered), uncovered))
        project_executable = sum(len(lines) for lines in self.lines.values())
        project_covered = sum(hits > 0 for lines in self.lines.values() for hits in lines.values())
        return Measurement(patch, tuple(files), project_executable, project_covered)


@dataclass(frozen=True)
class FileCoverage:
    path: str
    executable: int | None
    covered: int
    uncovered: tuple[int, ...]

    def row(self, repo: str | None, head: str) -> str:
        label = html.escape(self.path[:200]).replace("|", "&#124;").replace("\n", "\\n")
        name = f"<code>{label}</code>"
        if repo:
            name = f"[{name}](https://github.com/{repo}/blob/{head}/{quote(self.path)})"
        if self.executable is None:
            return f"| {name} | Unavailable | — | — |"
        if not self.executable:
            return f"| {name} | No executable additions | 0 / 0 | — |"
        ranges = []
        for number in self.uncovered:
            if ranges and number == ranges[-1][-1] + 1:
                ranges[-1].append(number)
            else:
                ranges.append([number])
        labels = []
        for group in ranges[:RANGE_LIMIT]:
            labels.append(str(group[0]) if len(group) == 1 else f"{group[0]}–{group[-1]}")
        if len(ranges) > RANGE_LIMIT:
            labels.append("…")
        missing = ", ".join(labels) or "—"
        return f"| {name} | {100 * self.covered / self.executable:.2f}% | {self.covered} / {self.executable} | {missing} |"


@dataclass(frozen=True)
class Measurement:
    patch: Patch
    files: tuple[FileCoverage, ...]
    project_executable: int
    project_covered: int

    @property
    def executable(self) -> int:
        return sum(item.executable or 0 for item in self.files)

    @property
    def covered(self) -> int:
        return sum(item.covered for item in self.files)

    def markdown(self, repo: str | None = None) -> str:
        lines = [COMMENT_MARKER, "## Patch coverage", ""]
        if self.executable:
            lines.append(f"**Patch line coverage: {100 * self.covered / self.executable:.2f}%** ({self.covered} / {self.executable} added executable lines).")
        else:
            lines.append("**No added executable lines appear in the supplied coverage reports.**")
        if self.project_executable:
            lines.extend(["", f"Project line coverage: **{100 * self.project_covered / self.project_executable:.2f}%** ({self.project_covered} / {self.project_executable} measured lines)."])
        lines.extend(["", f"Compared merge base `{self.patch.base}` to tested commit `{self.patch.head}`."])
        if self.files:
            lines.extend(["", "| File | Patch coverage | Covered / executable | Uncovered added lines |", "| --- | ---: | ---: | --- |"])
            for item in self.files[:FILE_LIMIT]:
                lines.append(item.row(repo, self.patch.head))
            if len(self.files) > FILE_LIMIT:
                lines.extend(["", f"Showing {FILE_LIMIT} of {len(self.files)} changed files; totals include every measured file."])
        lines.extend(["", "Only added lines with LCOV line counters enter the patch percentage. Files absent from LCOV are unavailable; non-executable lines in measured files are excluded.", "", "Advisory report: coverage percentages and reporting failures do not fail the build."])
        run_id = os.environ.get("GITHUB_RUN_ID")
        if repo and run_id:
            lines.extend(["", f"[Coverage artifacts and workflow run](https://github.com/{repo}/actions/runs/{run_id})."])
        return "\n".join(lines) + "\n"


@dataclass(frozen=True)
class Publication:
    repo: str
    pr: int
    head: str

    def publish(self, body: str) -> None:
        endpoint = f"repos/{self.repo}"
        pr = json.loads(command(["gh", "api", f"{endpoint}/pulls/{self.pr}"]))
        if pr["state"] != "open" or pr["head"]["sha"] != self.head:
            print("patch coverage: skipped publication because the PR closed or its head changed", file=sys.stderr)
            return
        if os.environ.get("GITHUB_ACTIONS") == "true":
            author = "github-actions[bot]"
        else:
            author = json.loads(command(["gh", "api", "user"]))["login"]
        pages = json.loads(command(["gh", "api", "--paginate", "--slurp", f"{endpoint}/issues/{self.pr}/comments?per_page=100"]))
        destination = f"{endpoint}/issues/{self.pr}/comments"
        method = "POST"
        for page in pages:
            for comment in page:
                if comment["body"].startswith(COMMENT_MARKER) and comment["user"]["login"] == author:
                    destination = f"{endpoint}/issues/comments/{comment['id']}"
                    method = "PATCH"
        command(["gh", "api", "--method", method, destination, "--input", "-"], input_text=json.dumps({"body": body}))


def main(arguments: Sequence[str] | None = None, *, root: Path = ROOT) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="origin/main")
    parser.add_argument("--head", default="HEAD")
    parser.add_argument("--report", type=Path, action="append")
    parser.add_argument("--source-root", type=Path)
    parser.add_argument("--output", type=Path, default=Path("target/patch-coverage.md"))
    parser.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY"))
    parser.add_argument("--pr", type=int)
    parser.add_argument("--pr-head", help="PR head SHA when coverage was collected from a synthetic merge commit")
    options = parser.parse_args(arguments)
    try:
        patch = Patch.load(root, options.base, options.head)
        coverage = Coverage.load(root, options.report or [root / "lcov-workspace.info"], options.source_root)
        measured = coverage.measure(patch)
        repo = options.repo
        body = measured.markdown(repo)
        output = root / options.output
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(body, encoding="utf-8")
        print(body)
        if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
            with Path(summary).open("a", encoding="utf-8") as content:
                content.write(body)
        if options.pr:
            if not repo:
                repo = command(["gh", "repo", "view", "--json", "nameWithOwner", "--jq", ".nameWithOwner"], root=root).strip()
            Publication(repo, options.pr, options.pr_head or patch.head).publish(body)
    except (OSError, RunnerError, RuntimeError, ValueError, SyntaxError, KeyError, subprocess.SubprocessError) as error:
        print(f"patch coverage: {error}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
