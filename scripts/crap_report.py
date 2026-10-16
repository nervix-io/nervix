#!/usr/bin/env python3
"""The advisory CRAP report of the merged ordinary workspace report.

`just check-coverage [report] [record]` runs the pinned cargo-crap over the repository's Rust
sources against the repository-relative `lcov-workspace.info` that `just coverage-merge` wrote, and
writes `target/crap-report/crap.json` and `target/crap-report/crap.md`. The coverage job uploads
both in the `coverage-merged` artifact, and the advisory patch-coverage comment carries the section
that `pull_request_section` renders from the JSON for the files a pull request changes.

Owns: the scope of the analysis, its vacuity verdict, the score above which the report lists a
function, and the report's JSON, Markdown and pull-request section.
May depend on: the pinned analyzer and its walk (`scripts/cargo_crap.py`), the merge's tracked files
and merge record (`scripts/coverage_merge.py`), and LCOV records (`scripts/native_coverage.py`).
Must not know: how the producers collect coverage, how the merge maps their reports, or how the
pull-request comment is published.

The analysis walks the whole repository root: the root package's directory holds every workspace
member, and the Cargo projects nested in it, such as the lint workspace, are walked too, because
some ordinary producers measure them. It applies no default exclusion, so test, bench and example
sources are scored like any other. A function in a file that the report holds no record of is
unmeasured: the report lists it with its complexity and never scores it as uncovered.

No CRAP score fails the check. It fails only when the analysis is vacuous or could not run:

- the merge record is missing, did not complete, or describes another revision;
- a record of the report names a source that this checkout does not track;
- the analyzer's walk cannot read a Rust file that the report holds;
- the analyzer measured functions of such a file without binding the file's record to them;
- the analysis matched no report file.

Either way the check writes its JSON report, and the Markdown report names its verdict.
"""

from __future__ import annotations

import argparse
import enum
import html
import json
import subprocess
import sys
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

from scripts import cargo_crap
from scripts import native_coverage as native
from scripts.coverage_merge import Checkout, MergeError, display, git

ROOT = Path(__file__).resolve().parent.parent
# A function scoring above this is listed. The score is advisory, so this selects rows, not a verdict.
THRESHOLD = 30
REPORT = Path("lcov-workspace.info")
RECORD = Path("target/coverage-merge/record.json")
OUTPUT = Path("target/crap-report")
JSON_REPORT = "crap.json"
MARKDOWN_REPORT = "crap.md"
# The functions a pull-request section lists before it points to the full report.
SECTION_LIMIT = 50
# The files a failure message names before it gives the rest as a count.
NAMED_LIMIT = 10


class Verdict(enum.StrEnum):
    """The outcome the check reports and exits with."""

    # Every Rust file that the report holds was analysed, and the analysis matched the report.
    COMPLETE = "complete"
    # The analysis cannot describe the report, so its scores would describe something else.
    VACUOUS = "vacuous"
    # The analysis could not run.
    FAILED = "failed"

    def outcome(self) -> str:
        """What the verdict states of the analysis, as a predicate."""
        if self is Verdict.FAILED:
            return "could not run"
        return f"is {self}"


class Mismatch(enum.StrEnum):
    """Why an analysis cannot describe the report it was given."""

    RECORD = "record"
    UNTRACKED = "untracked-sources"
    UNREADABLE = "unreadable-files"
    UNBOUND = "unbound-files"
    UNMATCHED = "no-match"


def named(files: Sequence[str]) -> str:
    """The files a message names, the first `NAMED_LIMIT` of them and a count of the rest."""

    if not files:
        return ""
    shown = ", ".join(files[:NAMED_LIMIT])
    if len(files) > NAMED_LIMIT:
        shown += f" and {len(files) - NAMED_LIMIT} more"
    return f": {shown}"


class VacuousAnalysis(Exception):
    """An analysis whose scores would not describe the report. The message names what to fix."""

    def __init__(self, mismatch: Mismatch, message: str, files: Sequence[str] = ()) -> None:
        self.mismatch = mismatch
        self.files = tuple(files)
        super().__init__(message + named(self.files))


def verify_record(root: Path, record: Path, revision: str) -> None:
    """Refuse a report whose merge record does not state a complete merge at this revision."""

    shown = display(record, root)
    if not record.is_file():
        raise VacuousAnalysis(
            Mismatch.RECORD,
            f"{shown} does not exist: the merge record names the revision the report describes; "
            "run `just coverage-merge`, or name the record of the report",
        )
    try:
        content = json.loads(record.read_text(encoding="utf-8"))
    except ValueError as error:
        raise VacuousAnalysis(Mismatch.RECORD, f"{shown} is not a merge record: {error}") from error
    if not isinstance(content, dict):
        raise VacuousAnalysis(Mismatch.RECORD, f"{shown} is not a merge record")
    verdict = content.get("verdict")
    if verdict != native.Verdict.COMPLETE:
        raise VacuousAnalysis(Mismatch.RECORD, f"{shown} records a {verdict} merge, not a complete one")
    merged = content.get("revision")
    if merged != revision:
        raise VacuousAnalysis(
            Mismatch.RECORD,
            f"{shown} states that the report was merged at revision {merged}, but this checkout is at "
            f"{revision}; analyse the report in a checkout of {merged}",
        )


def report_sources(report: Path) -> list[str]:
    """The sources the report holds records of, each once, in path order."""

    with report.open(encoding="utf-8") as content:
        sources = {record.source for record in native.lcov_records(content)}
    return sorted(sources)


@dataclass(frozen=True)
class Analysis:
    """One complete analysis of the merged report: what it scored and which report files it matched."""

    revision: str
    report_files: int
    report_rust_files: int
    # Report Rust files whose records the analyzer bound to the functions it measured in them.
    matched: tuple[str, ...]
    # Report Rust files the analyzer read without finding a function it scores.
    unscored: tuple[str, ...]
    scored: tuple[cargo_crap.Scored, ...]
    # Functions in files that the report holds no record of.
    unmeasured: tuple[cargo_crap.Function, ...]

    @classmethod
    def run(cls, root: Path, target: Path, report: Path, record: Path) -> Analysis:
        revision = git(root, "rev-parse", "HEAD").strip()
        verify_record(root, record, revision)
        checkout = Checkout.load(root)
        sources = report_sources(report)
        untracked = [source for source in sources if source not in checkout.tracked]
        if untracked:
            raise VacuousAnalysis(
                Mismatch.UNTRACKED,
                f"{display(report, root)} holds records of {len(untracked)} sources that this checkout "
                "does not track, so it is not the merged report of this checkout; "
                "`just coverage-merge` maps a report onto the repository",
                untracked,
            )
        rust = [source for source in sources if source.endswith(".rs")]
        unreadable = cargo_crap.unreachable_sources(root, rust)
        if unreadable:
            raise VacuousAnalysis(
                Mismatch.UNREADABLE,
                "the analyzer's walk cannot read these Rust files that the report holds, being hidden, "
                "symbolic links or ignored by Git",
                unreadable,
            )
        scores = cargo_crap.score(root, target, (), report)
        scored_files = {item.function.path for item in scores.scored}
        unmeasured_files = {function.path for function in scores.unmeasured}
        matched = []
        unscored = []
        unbound = []
        for source in rust:
            if source in unmeasured_files:
                unbound.append(source)
            elif source in scored_files:
                matched.append(source)
            else:
                unscored.append(source)
        if unbound:
            raise VacuousAnalysis(
                Mismatch.UNBOUND,
                "the analyzer measured functions in these files without binding the report's records of them",
                unbound,
            )
        if not matched:
            raise VacuousAnalysis(
                Mismatch.UNMATCHED,
                f"the analysis matched none of the {len(rust)} Rust files of {display(report, root)}",
            )
        scored = sorted(scores.scored, key=lambda item: item.function)
        unmeasured = sorted(scores.unmeasured)
        return cls(
            revision=revision, report_files=len(sources), report_rust_files=len(rust),
            matched=tuple(matched), unscored=tuple(unscored), scored=tuple(scored), unmeasured=tuple(unmeasured),
        )

    def describe(self, report: str) -> dict[str, object]:
        """The JSON report: the verdict, the scope, the totals and every function the analysis measured."""

        functions = []
        above = 0
        for item in self.scored:
            functions.append({
                "file": item.function.path, "function": item.function.name, "line": item.function.line,
                "complexity": item.function.complexity, "coverage": item.coverage, "crap": item.crap,
            })
            if item.crap > THRESHOLD:
                above += 1
        unmeasured = []
        for function in self.unmeasured:
            unmeasured.append({"file": function.path, "function": function.name, "line": function.line, "complexity": function.complexity})
        return {
            "verdict": str(Verdict.COMPLETE),
            "report": report,
            "revision": self.revision,
            "analyzer": f"{cargo_crap.CRATE} {cargo_crap.VERSION}",
            "threshold": THRESHOLD,
            "scope": {
                "report_files": self.report_files,
                "report_rust_files": self.report_rust_files,
                "matched_files": len(self.matched),
                "unscored_files": list(self.unscored),
                "unmeasured_files": sorted({function.path for function in self.unmeasured}),
            },
            "totals": {"measured": len(self.scored), "above_threshold": above, "unmeasured": len(self.unmeasured)},
            "functions": functions,
            "unmeasured": unmeasured,
        }


def cell(text: str) -> str:
    """Text safe inside a Markdown table cell."""
    return html.escape(text).replace("|", "&#124;")


def percent(value: float) -> str:
    return f"{value:.1f}%"


def counted(number: int, noun: str) -> str:
    """The number followed by the noun, in the plural unless the number is one."""
    if number == 1:
        return f"1 {noun}"
    return f"{number} {noun}s"


@dataclass(frozen=True)
class Listed:
    """One measured function as a written report lists it."""

    path: str
    name: str
    line: int
    complexity: float
    coverage: float
    crap: float

    @classmethod
    def read(cls, entry: Mapping[str, object]) -> Listed:
        return cls(
            str(entry["file"]), str(entry["function"]), int(entry["line"]),
            float(entry["complexity"]), float(entry["coverage"]), float(entry["crap"]),
        )

    def location(self, repo: str | None, head: str | None) -> str:
        label = f"<code>{cell(self.path)}:{self.line}</code>"
        if repo and head:
            return f"[{label}](https://github.com/{repo}/blob/{head}/{quote(self.path)}#L{self.line})"
        return label

    def row(self, repo: str | None = None, head: str | None = None) -> str:
        return (
            f"| <code>{cell(self.name)}</code> | {self.location(repo, head)} | {self.crap:.1f} | "
            f"{self.complexity:g} | {percent(self.coverage)} |"
        )


FUNCTION_TABLE = ("| Function | Location | CRAP | Complexity | Coverage |", "| --- | --- | ---: | ---: | ---: |")
ADVISORY = (
    "Advisory report: no CRAP score fails the build. The coverage job fails only when the analysis "
    "is vacuous, that is when it cannot describe the merged report."
)


@dataclass(frozen=True)
class Document:
    """A written CRAP report: the JSON the check wrote, and the Markdown rendered from it."""

    content: Mapping[str, object]

    @classmethod
    def read(cls, path: Path) -> Document:
        content = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(content, dict) or content.get("verdict") not in set(Verdict):
            raise ValueError(f"{path} is not a CRAP report: it states no verdict")
        return cls(content)

    @property
    def verdict(self) -> Verdict:
        return Verdict(self.content["verdict"])

    def failure(self) -> str:
        return str(self.content.get("failure", "the report records no cause"))

    def threshold(self) -> int:
        return int(self.content["threshold"])

    def above(self) -> list[Listed]:
        """The measured functions scoring above the threshold, highest score first."""
        functions = []
        for entry in self.content["functions"]:
            function = Listed.read(entry)
            if function.crap > self.threshold():
                functions.append(function)
        functions.sort(key=lambda function: (-function.crap, function.path, function.line))
        return functions

    def project(self) -> str:
        """The project's totals in two sentences."""
        totals = self.content["totals"]
        files = len(self.content["scope"]["unmeasured_files"])
        return (
            f"Project: {totals['above_threshold']} of {totals['measured']} measured functions above "
            f"{self.threshold()}. Unmeasured, with no ordinary coverage record and never scored: "
            f"{counted(totals['unmeasured'], 'function')} in {counted(files, 'file')}."
        )

    def markdown(self) -> str:
        """The full report the coverage job retains."""
        lines = ["# CRAP report", ""]
        if self.verdict is not Verdict.COMPLETE:
            lines.extend([f"**The analysis {self.verdict.outcome()}.** {self.failure()}", "", ADVISORY])
        else:
            lines.extend(self.summary())
            lines.extend(["", ADVISORY, ""])
            lines.extend(self.above_lines())
            lines.append("")
            lines.extend(self.unmeasured_lines())
            lines.append("")
            lines.extend(self.unscored_lines())
        return "\n".join(lines) + "\n"

    def summary(self) -> list[str]:
        scope = self.content["scope"]
        totals = self.content["totals"]
        return [
            f"**Complete.** {self.content['analyzer']} scored the functions of the repository's Rust sources "
            f"against `{self.content['report']}` at revision `{self.content['revision']}`: "
            f"**{totals['above_threshold']} of {totals['measured']} measured functions above {self.threshold()}.**",
            "",
            "| Measure | Count |",
            "| --- | ---: |",
            f"| Rust files in the report | {scope['report_rust_files']} |",
            f"| Matched by the analysis | {scope['matched_files']} |",
            f"| Read with no function the analyzer scores | {len(scope['unscored_files'])} |",
            f"| Measured functions | {totals['measured']} |",
            f"| Functions above {self.threshold()} | {totals['above_threshold']} |",
            f"| Unmeasured functions | {totals['unmeasured']} in {counted(len(scope['unmeasured_files']), 'file')} |",
        ]

    def above_lines(self) -> list[str]:
        lines = [f"## Functions above {self.threshold()}", ""]
        above = self.above()
        if not above:
            lines.append(f"No measured function scores above {self.threshold()}.")
            return lines
        lines.extend(FUNCTION_TABLE)
        for function in above:
            lines.append(function.row())
        return lines

    def unmeasured_lines(self) -> list[str]:
        lines = [
            "## Unmeasured functions",
            "",
            "The ordinary report holds no record of these files, so their functions are listed with their "
            "complexity and never scored.",
            "",
        ]
        complexities: dict[str, list[float]] = {}
        for entry in self.content["unmeasured"]:
            complexities.setdefault(str(entry["file"]), []).append(float(entry["complexity"]))
        if not complexities:
            lines.append("Every function the analysis measured is in a file the report holds a record of.")
            return lines
        lines.extend(["| File | Functions | Highest complexity |", "| --- | ---: | ---: |"])
        for path, measured in sorted(complexities.items()):
            lines.append(f"| <code>{cell(path)}</code> | {len(measured)} | {max(measured):g} |")
        return lines

    def unscored_lines(self) -> list[str]:
        lines = [
            "## Report files without a scored function",
            "",
            "The analyzer read these files and scores no function in them: it scores no `#[test]` function, "
            "nothing in a `#[cfg(test)]` module, and no function inside a macro, a trait's provided method "
            "or a function nested in another's body.",
            "",
        ]
        unscored = self.content["scope"]["unscored_files"]
        if not unscored:
            lines.append("None.")
            return lines
        for path in unscored:
            lines.append(f"- <code>{cell(path)}</code>")
        return lines

    def section(self, changed: Iterable[str], repo: str | None, head: str) -> str:
        """The section of the pull-request comment: the functions above the threshold in the files
        the pull request changes, the project's totals and the unmeasured count."""

        lines = ["## CRAP", ""]
        if self.verdict is not Verdict.COMPLETE:
            lines.extend([
                f"**The CRAP analysis {self.verdict.outcome()}**: {self.failure()}",
                "",
                "The coverage job fails until the analysis describes the merged report.",
            ])
            return "\n".join(lines) + "\n"
        files = set(changed)
        listed = [function for function in self.above() if function.path in files]
        unmeasured = [entry for entry in self.content["unmeasured"] if entry["file"] in files]
        if listed:
            lines.extend([f"**{counted(len(listed), 'function')} above {self.threshold()} in the files this pull request changes.**", ""])
            lines.extend(FUNCTION_TABLE)
            for function in listed[:SECTION_LIMIT]:
                lines.append(function.row(repo, head))
            if len(listed) > SECTION_LIMIT:
                lines.extend(["", f"Showing {SECTION_LIMIT} of {len(listed)}; the `coverage-merged` artifact's `crap.md` lists every one."])
        else:
            lines.append(f"**No function above {self.threshold()} in the files this pull request changes.**")
        if unmeasured:
            lines.extend(["", f"Unmeasured in the changed files, with no ordinary coverage record and never scored: {counted(len(unmeasured), 'function')}."])
        lines.extend(["", self.project(), "", ADVISORY])
        return "\n".join(lines) + "\n"


def pull_request_section(path: Path, changed: Iterable[str], repo: str | None, head: str) -> str:
    """The CRAP section of the pull-request comment, from the JSON report at `path`. A missing or
    unreadable report is stated in the section, never raised, because the comment is advisory."""

    try:
        document = Document.read(path)
        return document.section(changed, repo, head)
    except (OSError, ValueError, KeyError, TypeError) as error:
        return f"## CRAP\n\n**The CRAP report is unavailable**: {error}\n"


def main(arguments: Sequence[str] | None = None, *, root: Path = ROOT) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--report", type=Path, default=REPORT, help="the merged repository-relative report")
    parser.add_argument("--record", type=Path, default=RECORD, help="the merge record of that report")
    parser.add_argument("--output", type=Path, default=OUTPUT, help=f"the directory of {JSON_REPORT} and {MARKDOWN_REPORT}")
    options = parser.parse_args(arguments)
    report = root / options.report
    record = root / options.record
    output = root / options.output
    shown = display(report, root)
    content: dict[str, object] = {"report": shown, "analyzer": f"{cargo_crap.CRATE} {cargo_crap.VERSION}", "threshold": THRESHOLD}
    status = 1
    try:
        analysis = Analysis.run(root, cargo_crap.default_target(), report, record)
        content = analysis.describe(shown)
        status = 0
    except VacuousAnalysis as error:
        content.update({
            "verdict": str(Verdict.VACUOUS), "failure": str(error),
            "mismatch": {"kind": str(error.mismatch), "files": list(error.files)},
        })
    except (cargo_crap.AnalyzerError, MergeError, native.RunnerError, OSError, ValueError, KeyError,
            subprocess.SubprocessError) as error:
        content.update({"verdict": str(Verdict.FAILED), "failure": str(error)})
    output.mkdir(parents=True, exist_ok=True)
    (output / JSON_REPORT).write_text(json.dumps(content, indent=2) + "\n", encoding="utf-8")
    document = Document(content)
    (output / MARKDOWN_REPORT).write_text(document.markdown(), encoding="utf-8")
    if status:
        print(f"CRAP report: the analysis {document.verdict.outcome()}: {document.failure()}", file=sys.stderr)
        return status
    print(document.project())
    print(f"CRAP report: {display(output / JSON_REPORT, root)} and {display(output / MARKDOWN_REPORT, root)}")
    return status


if __name__ == "__main__":
    sys.exit(main())
