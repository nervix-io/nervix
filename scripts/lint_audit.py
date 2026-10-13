"""Report review candidates for code shaped around inaccurate lints, with attribution evidence.

The pinned compiler records resolved candidates beside its acquisition findings in every complete
analysis. This command reads the complete evidence `just ratchet` also uses, merges each
candidate across the configurations that compiled it, and attributes it to the commits that
introduced its lines and to the lint history around them. A candidate is review input: review
decides whether it is lint-caused, intentional, pre-existing or unrelated. The output under
`target/` is generated evidence, never an enforcement or approval input.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import dataclasses
import hashlib
import json
import os
import pathlib
import re
import subprocess
import sys
from typing import Any

from scripts import typed_ratchet

ROOT = pathlib.Path(__file__).resolve().parents[1]
BASELINE = "debt-baseline.json"


class AuditError(Exception):
    """The analysis, the repository history or the output could not be established."""


@dataclasses.dataclass(frozen=True)
class Rule:
    """A historical count, with the textual spelling its original counter matched."""

    name: str
    spelling: re.Pattern[str] | None


RULES = {
    rule.name: rule
    for rule in (
        Rule("data_plane_lock_acquisitions", re.compile(r"\.\s*(?:lock|read|write|entry)\s*\(")),
        Rule("bare_unwrap_and_expect", re.compile(r"\.\s*(?:unwrap\s*\(\s*\)|expect\s*\()")),
        Rule("clamped_arithmetic", re.compile(r"\b(?:saturating|wrapping)_[a-z_]+\s*\(")),
        Rule("as_casts", re.compile(r"\bas\s+(?:[ui](?:8|16|32|64|128|size)|f32|f64)\b")),
        Rule("discarded_results", re.compile(r"\blet\s+_\s*=")),
        Rule("result_string_errors", re.compile(r"\bResult\s*<[^;{}]*,\s*String\s*>")),
        Rule("bare_error_signatures", re.compile(r"->\s*(?:[A-Za-z_][A-Za-z0-9_]*\s*::\s*)*Result\s*<")),
        Rule("combinator_control_flow",
             re.compile(r"\.\s*(?:map_or_else|map_or|and_then|ok_or_else|unwrap_or_else|or_else)\s*\(")),
        Rule("model_matches_in_data_plane", re.compile(r"\bModel\s*::")),
        Rule("parser_imports_outside_edges", re.compile(r"\bnervix_nspl\s*::")),
        Rule("files_over_3000_lines", None),
    )
}

# The counts whose workarounds each candidate kind could plausibly be. Attribution still needs
# the introducing commit's evidence; this list only selects which history to read.
OWNERS: dict[str, tuple[str, ...]] = {
    "repeated_lookup": ("data_plane_lock_acquisitions",),
    "key_allocation": ("data_plane_lock_acquisitions",),
    "borrowed_copy": ("data_plane_lock_acquisitions", "bare_unwrap_and_expect", "combinator_control_flow"),
    "duplicated_branch": ("data_plane_lock_acquisitions", "combinator_control_flow", "bare_unwrap_and_expect"),
    "forwarding_wrapper": ("files_over_3000_lines", "data_plane_lock_acquisitions",
                           "model_matches_in_data_plane", "parser_imports_outside_edges"),
    "discarded_error": ("bare_unwrap_and_expect", "discarded_results", "result_string_errors",
                        "bare_error_signatures", "as_casts"),
    "disguised_clamp": ("clamped_arithmetic", "as_casts"),
    "relocated_acquisition": ("data_plane_lock_acquisitions",),
}

# The files the textual lock counter read, as each revision of it declared them.
LOCK_COUNTER_FILES = ("src/runtime/", "crates/interconnect/src/", "src/runtime_ack.rs", "src/metrics.rs")
LOCK_COUNTER_CONNECTORS = "crates/connectors/"
CONNECTORS_COUNTED = "364aa492"

SUGGESTIONS = ("suspected", "possible", "unattributed", "pre-existing", "uncommitted")


def git(root: pathlib.Path, *args: str) -> str:
    completed = subprocess.run(["git", *args], cwd=root, text=True, capture_output=True)
    if completed.returncode:
        raise AuditError(f"git {' '.join(args)} failed:\n{completed.stderr}")
    return completed.stdout


@dataclasses.dataclass(frozen=True)
class CountChange:
    before: int | None
    after: int | None

    def render(self) -> str:
        before = "none" if self.before is None else str(self.before)
        after = "none" if self.after is None else str(self.after)
        return f"{before}->{after}"


@dataclasses.dataclass(frozen=True)
class Commit:
    sha: str
    date: str
    subject: str


class History:
    """The debt baseline's history: when each count appeared and which commits lowered it."""

    def __init__(self, root: pathlib.Path) -> None:
        self.root = root
        self.changes: dict[str, dict[str, CountChange]] = {}
        self.introduced: dict[str, Commit] = {}
        self.commits: dict[str, Commit] = {}
        log = git(root, "log", "--reverse", "--format=%H%x09%ad%x09%s", "--date=short", "--", BASELINE)
        previous: dict[str, int] = {}
        for line in log.splitlines():
            sha, date, subject = line.split("\t", 2)
            commit = Commit(sha, date, subject)
            self.commits[sha] = commit
            try:
                current = json.loads(git(root, "show", f"{sha}:{BASELINE}"))
            except (AuditError, json.JSONDecodeError):
                current = {}
            changes = {}
            for name in sorted(set(previous) | set(current)):
                if previous.get(name) != current.get(name):
                    changes[name] = CountChange(previous.get(name), current.get(name))
                    if name not in previous and name in current and name not in self.introduced:
                        self.introduced[name] = commit
            self.changes[sha] = changes
            previous = current

    def event(self, sha: str, rule: str) -> str | None:
        """How a commit changed a count: its introduction or a lowering."""
        change = self.changes.get(sha, {}).get(rule)
        if change is None:
            return None
        if change.before is None:
            return f"introduced at {change.after}"
        if change.after is None:
            return f"retired from {change.before}"
        if change.after < change.before:
            return f"lowered {change.render()}"
        return f"raised {change.render()}"

    def commit(self, prefix: str) -> Commit | None:
        for sha, commit in self.commits.items():
            if sha.startswith(prefix):
                return commit
        return None

    def lowered(self, sha: str, rule: str) -> bool:
        change = self.changes.get(sha, {}).get(rule)
        if change is None or change.before is None or change.after is None:
            return False
        return change.after < change.before

    def predates(self, date: str, rule: str) -> bool:
        introduced = self.introduced.get(rule)
        return introduced is None or date < introduced.date


@dataclasses.dataclass(frozen=True)
class BlameLine:
    sha: str
    # The line's number in the file as its introducing commit wrote it.
    original: int
    path: str
    author_time: int
    subject: str

    @property
    def committed(self) -> bool:
        return not self.sha.startswith("0000000")


class Blame:
    """Line provenance at HEAD, computed once per file."""

    def __init__(self, root: pathlib.Path) -> None:
        self.root = root
        self.files: dict[str, list[BlameLine]] = {}
        self.dates: dict[str, str] = {}

    def load(self, paths: set[str]) -> None:
        missing = sorted(path for path in paths if path not in self.files)
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            for path, lines in zip(missing, executor.map(self.blame, missing)):
                self.files[path] = lines

    def blame(self, path: str) -> list[BlameLine]:
        # The working tree, so line numbers match the compiler's spans; uncommitted lines blame
        # to the all-zero commit.
        output = git(self.root, "blame", "--porcelain", "--", path)
        lines: list[BlameLine] = []
        commits: dict[str, dict[str, str]] = {}
        current: dict[str, str] = {}
        sha = ""
        original = 0
        for line in output.splitlines():
            if line.startswith("\t"):
                details = commits.setdefault(sha, {})
                details.update(current)
                current = {}
                lines.append(BlameLine(sha, original, details.get("filename", path),
                                       int(details.get("time", "0")), details.get("summary", "")))
                continue
            header = line.split(" ")
            if len(header) >= 3 and re.fullmatch(r"[0-9a-f]{40}", header[0]):
                sha = header[0]
                original = int(header[1])
                current = {}
                continue
            key, _, value = line.partition(" ")
            if key == "filename":
                current["filename"] = value
            elif key == "summary":
                current["summary"] = value
            elif key == "author-time":
                current["time"] = value
        return lines

    def date(self, sha: str) -> str:
        if sha not in self.dates:
            self.dates[sha] = git(self.root, "show", "-s", "--format=%ad", "--date=short", sha).strip()
        return self.dates[sha]

    def line(self, path: str, number: int) -> BlameLine | None:
        lines = self.files.get(path, [])
        if 1 <= number <= len(lines):
            return lines[number - 1]
        return None


@dataclasses.dataclass
class Hunk:
    """One changed region of a commit's diff, addressed by its new-side lines."""

    start: int
    length: int
    before: list[str] = dataclasses.field(default_factory=list)
    after: list[str] = dataclasses.field(default_factory=list)

    def contains(self, line: int) -> bool:
        # A pure deletion has no new-side lines; it sits before its start line.
        return self.start <= line < self.start + max(self.length, 1)


@dataclasses.dataclass(frozen=True)
class FileRevision:
    sha: str
    path: str


class Hunks:
    """The changed lines around one line of a commit's diff."""

    HEADER = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@")

    def __init__(self, root: pathlib.Path) -> None:
        self.root = root
        self.diffs: dict[FileRevision, list[Hunk]] = {}

    def parse(self, diff: str) -> list[Hunk]:
        hunks: list[Hunk] = []
        for line in diff.splitlines():
            header = self.HEADER.match(line)
            if header:
                length = 1
                if header.group(4) is not None:
                    length = int(header.group(4))
                hunks.append(Hunk(int(header.group(3)), length))
                continue
            if not hunks:
                continue
            if line.startswith("-") and not line.startswith("---"):
                hunks[-1].before.append(line[1:])
            elif line.startswith("+") and not line.startswith("+++"):
                hunks[-1].after.append(line[1:])
        return hunks

    def around(self, sha: str, path: str, line: int) -> Hunk:
        revision = FileRevision(sha, path)
        if revision not in self.diffs:
            diff = git(self.root, "show", "--format=", "--unified=0", sha, "--", path)
            self.diffs[revision] = self.parse(diff)
        for hunk in self.diffs[revision]:
            if hunk.contains(line):
                return hunk
        return Hunk(line, 0)


def candidate_lines(candidate: dict[str, Any]) -> list[int]:
    pattern = candidate["pattern"]
    spans = [candidate["span"]]
    spans.extend(operation["span"] for operation in pattern.get("operations", []))
    spans.extend(pattern.get("occurrences", []))
    lines = set()
    for span in spans:
        lines.update(range(span["line"], span["end_line"] + 1))
    return sorted(lines)


def semantic_fields(pattern: dict[str, Any]) -> dict[str, Any]:
    """Pattern facts without offsets, so an edit elsewhere in the file keeps the identity."""
    fields = {}
    for key, value in pattern.items():
        if key == "operations":
            fields[key] = [{"operation": item["operation"], "source": item["span"]["source"]} for item in value]
        elif key == "occurrences":
            fields[key] = [item["source"] for item in value]
        else:
            fields[key] = value
    return fields


def collect(evidence: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Merge candidates by site, owner and pattern across every configuration that compiled them."""
    merged: dict[str, dict[str, Any]] = {}
    for entry in evidence:
        configuration = entry["configuration"]["name"]
        for report in entry["reports"].values():
            for candidate in report["audit"]:
                site = candidate["span"]["site"]
                key = json.dumps([candidate["pattern"]["kind"], site["path"], site["start"], candidate["owner"],
                                  semantic_fields(candidate["pattern"])], sort_keys=True)
                record = merged.setdefault(key, {"candidate": candidate, "configurations": set(), "contexts": {}})
                record["configurations"].add(f"{configuration}:{report['crate_name']}")
                context = candidate["context"]
                record["contexts"][json.dumps(context, sort_keys=True)] = context
    results = []
    for record in merged.values():
        candidate = record["candidate"]
        results.append({
            "kind": candidate["pattern"]["kind"],
            "path": candidate["span"]["site"]["path"],
            "line": candidate["span"]["line"],
            "column": candidate["span"]["column"] + 1,
            "owner": candidate["owner"],
            "source": candidate["span"]["source"],
            "contexts": [record["contexts"][key] for key in sorted(record["contexts"])],
            "configurations": sorted(record["configurations"]),
            "pattern": candidate["pattern"],
            "span": candidate["span"],
            "expansion": candidate["expansion"],
        })
    results.sort(key=lambda item: (item["kind"], item["path"], item["line"], item["column"], item["owner"]))
    assign_identities(results)
    return results


def assign_identities(candidates: list[dict[str, Any]]) -> None:
    """Stable identities from kind, file, owner and pattern facts; repeats take an ordinal."""
    seen: dict[str, int] = {}
    for candidate in candidates:
        payload = json.dumps([candidate["kind"], candidate["path"], candidate["owner"], candidate["source"],
                              semantic_fields(candidate["pattern"])], sort_keys=True)
        digest = hashlib.sha256(payload.encode()).hexdigest()[:12]
        ordinal = seen.get(digest, 0)
        seen[digest] = ordinal + 1
        candidate["id"] = digest if ordinal == 0 else f"{digest}-{ordinal}"


def lock_counter_scope(path: str, date: str, history: History) -> bool:
    """Whether the textual lock counter read this file at a commit's date."""
    if path.startswith(LOCK_COUNTER_FILES):
        return True
    if not path.startswith(LOCK_COUNTER_CONNECTORS):
        return False
    connectors = history.commit(CONNECTORS_COUNTED)
    return connectors is not None and date >= connectors.date


def attribute(candidate: dict[str, Any], history: History, blame: Blame, hunks: Hunks) -> dict[str, Any]:
    """The commit that completed a candidate's lines, its diff, and the counts it touched."""
    path = candidate["path"]
    provenance = []
    for number in candidate_lines(candidate):
        line = blame.line(path, number)
        if line is not None:
            provenance.append(line)
    if not provenance or not all(line.committed for line in provenance):
        return {"suggestion": "uncommitted", "commit": None, "rules": []}
    # The most recent line change is the one that completed the pattern.
    introducing = max(provenance, key=lambda line: (line.author_time, line.sha))
    date = blame.date(introducing.sha)
    hunk = hunks.around(introducing.sha, introducing.path, introducing.original)
    before = "\n".join(hunk.before)
    after = "\n".join(hunk.after)
    rules = []
    strongest = "pre-existing"
    for name in OWNERS.get(candidate["kind"], ()):
        rule = RULES[name]
        event = history.event(introducing.sha, name)
        removed = False
        if rule.spelling is not None:
            removed = len(rule.spelling.findall(before)) > len(rule.spelling.findall(after))
        predates = history.predates(date, name)
        rules.append({"rule": name, "event": event, "spelling_removed": removed, "predates_rule": predates})
        if predates:
            continue
        if event is not None and (removed or rule.spelling is None):
            level = "suspected"
        elif event is not None or removed:
            level = "possible"
        else:
            level = "unattributed"
        if SUGGESTIONS.index(level) < SUGGESTIONS.index(strongest):
            strongest = level
    return {
        "suggestion": strongest,
        "commit": {"sha": introducing.sha, "date": date, "subject": introducing.subject},
        "commits": sorted({line.sha[:12] for line in provenance}),
        "before": hunk.before,
        "after": hunk.after,
        "rules": rules,
    }


def relocated_acquisitions(sites: list[dict[str, Any]], history: History, blame: Blame) -> list[dict[str, Any]]:
    """Acquisitions outside the textual lock counter's files, introduced by a commit that lowered
    that count: the scope move the counter rewarded."""
    candidates = []
    blame.load({site["site"]["path"] for site in sites})
    for site in sites:
        findings = [finding for values in site["configurations"].values() for finding in values]
        finding = findings[0]
        path = site["site"]["path"]
        line = blame.line(path, finding["span"]["line"])
        if line is None or not history.lowered(line.sha, "data_plane_lock_acquisitions"):
            continue
        commit = history.commits[line.sha]
        if lock_counter_scope(path, commit.date, history):
            continue
        candidates.append({
            "kind": "relocated_acquisition",
            "path": path,
            "line": finding["span"]["line"],
            "column": finding["span"]["column"] + 1,
            "owner": finding["owner"],
            "source": finding["span"]["source"],
            "contexts": sorted({json.dumps(item["context"], sort_keys=True) for item in findings}),
            "configurations": sorted(site["configurations"]),
            "pattern": {"kind": "relocated_acquisition", "receiver": finding["receiver"],
                        "operation": finding["operation"], "acquisition": finding["acquisition"]},
            "span": finding["span"],
            "expansion": finding["expansion"],
        })
    for candidate in candidates:
        candidate["contexts"] = [json.loads(context) for context in candidate["contexts"]]
    candidates.sort(key=lambda item: (item["path"], item["line"], item["column"], item["owner"]))
    assign_identities(candidates)
    return candidates


def context_name(contexts: list[dict[str, Any] | None]) -> str:
    names = sorted({"none" if context is None else context["context"] for context in contexts})
    return "/".join(names)


def detail(candidate: dict[str, Any]) -> str:
    pattern = candidate["pattern"]
    kind = candidate["kind"]
    if kind == "repeated_lookup":
        operations = " -> ".join(operation["operation"] for operation in pattern["operations"])
        guarded = "guarded" if pattern["guarded"] else "sequential"
        return f"`{pattern['receiver']}` key `{pattern['key']}`: {operations} ({guarded})"
    if kind == "key_allocation":
        return f"{pattern['operation']} with `{pattern['allocation']}` ({pattern['key_type']})"
    if kind == "borrowed_copy":
        return f"borrowed `{pattern['operation']}` of {pattern['value_type']}"
    if kind == "duplicated_branch":
        return f"{pattern['construct']}: `{pattern['statement']}` in {len(pattern['occurrences'])} branches"
    if kind == "forwarding_wrapper":
        flags = [name for name in ("contract", "implementation", "relocated") if pattern[name]]
        return f"to `{pattern['callee']}`, {pattern['callers']} callers {' '.join(flags)}".strip()
    if kind == "discarded_error":
        return f"{pattern['operation']} drops {pattern['error_type']} ({pattern['information']})"
    if kind == "disguised_clamp":
        return f"{pattern['operation']} falls back to `{pattern['fallback']}`"
    return f"{pattern['receiver']}::{pattern['operation']} ({pattern['acquisition']})"


def markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Lint workaround audit",
        "",
        f"Revision `{report['revision']}`; source identity `{report['source_identity']}`.",
        f"Configurations: {', '.join(report['configurations'])}.",
        "",
        "Candidates are review input. A suggestion summarizes attribution evidence; it is not a",
        "classification.",
        "",
        "| Kind | Candidates | Recurring | Suspected | Possible | Unattributed | Pre-existing |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for kind, counts in report["summary"].items():
        lines.append(f"| {kind} | {counts['total']} | {counts['recurring']} | {counts['suspected']} | "
                     f"{counts['possible']} | {counts['unattributed']} | {counts['pre-existing']} |")
    for kind in report["summary"]:
        lines.extend(["", f"## {kind}", "", "| ID | Location | Owner | Context | Detail | Suggestion | Commit |",
                      "| --- | --- | --- | --- | --- | --- | --- |"])
        for candidate in report["candidates"]:
            if candidate["kind"] != kind:
                continue
            attribution = candidate["attribution"]
            commit = attribution.get("commit") or {}
            introduced = f"`{commit['sha'][:8]}` {commit['date']}" if commit else "—"
            owner = candidate["owner"].replace("|", "\\|")
            text = detail(candidate).replace("|", "\\|")
            lines.append(f"| `{candidate['id']}` | `{candidate['path']}:{candidate['line']}` | `{owner}` | "
                         f"{context_name(candidate['contexts'])} | {text} | {attribution['suggestion']} | {introduced} |")
    return "\n".join(lines) + "\n"


def summarize(candidates: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    summary: dict[str, dict[str, int]] = {}
    for candidate in candidates:
        counts = summary.setdefault(candidate["kind"], {"total": 0, "recurring": 0, **{name: 0 for name in SUGGESTIONS}})
        counts["total"] += 1
        if any(context is not None and context["context"] in {"recurring", "bounded"} for context in candidate["contexts"]):
            counts["recurring"] += 1
        counts[candidate["attribution"]["suggestion"]] += 1
    return dict(sorted(summary.items()))


def analysis(args: argparse.Namespace) -> dict[str, Any]:
    destination = args.target_dir / "typed-ratchet/gate.json"
    arguments = ["--root", str(args.root), "--target-dir", str(args.target_dir)]
    if args.inventory or args.configuration:
        destination = args.target_dir / "lint-audit/inventory.json"
        arguments.append("--inventory")
        for configuration in args.configuration or []:
            arguments.extend(["--configuration", configuration])
    arguments.extend(["--output", str(destination)])
    if typed_ratchet.main(arguments):
        raise AuditError("the compiler analysis did not complete; see its output above")
    report = json.loads(destination.read_text())
    if report.get("complete") is not True:
        raise AuditError(f"incomplete compiler analysis in {destination}")
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=pathlib.Path, default=ROOT)
    parser.add_argument("--target-dir", type=pathlib.Path,
                        default=pathlib.Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target")))
    parser.add_argument("--inventory", action="store_true",
                        help="analyze with warning levels, so current diagnostics do not stop the audit")
    parser.add_argument("--configuration", action="append",
                        help="audit selected declared configurations (an inventory, not the complete matrix)")
    parser.add_argument("--output-dir", type=pathlib.Path)
    args = parser.parse_args(argv)
    args.root = args.root.resolve()
    args.target_dir = args.target_dir.resolve()
    try:
        report = analysis(args)
        candidates = collect(report["evidence"])
        history = History(args.root)
        blame = Blame(args.root)
        blame.load({candidate["path"] for candidate in candidates})
        hunks = Hunks(args.root)
        candidates.extend(relocated_acquisitions(report["findings"], history, blame))
        for candidate in candidates:
            candidate["attribution"] = attribute(candidate, history, blame, hunks)
        revision = git(args.root, "rev-parse", "HEAD").strip()
        output = {
            "revision": revision,
            "source_identity": report["source_identity"],
            "matrix_complete": report["matrix_complete"],
            "configurations": [entry["name"] for entry in report["configurations"]],
            "rules": {name: {"introduced": commit.sha, "date": commit.date}
                      for name, commit in sorted(history.introduced.items())},
            "summary": summarize(candidates),
            "candidates": candidates,
            "complete": True,
        }
        directory = args.output_dir or args.target_dir / "lint-audit" / report["source_identity"][:16]
        typed_ratchet.atomic_json(directory / "audit.json", output)
        (directory / "audit.md").write_text(markdown(output))
        for kind, counts in output["summary"].items():
            print(f"lint audit: {kind}: {counts['total']} candidates, {counts['recurring']} recurring, "
                  f"{counts['suspected']} suspected", flush=True)
        print(f"lint audit: {len(candidates)} candidates at {revision[:12]}; report: {directory / 'audit.md'}")
        return 0
    except (AuditError, typed_ratchet.AnalysisError, OSError, ValueError, KeyError) as error:
        print(f"lint audit: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
