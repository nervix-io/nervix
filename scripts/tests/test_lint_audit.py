"""Candidate merging, identities, lint history and attribution of the workaround audit."""

from __future__ import annotations

import json
import os
import pathlib
import subprocess
import tempfile
import unittest
from typing import Any
from unittest import mock

from scripts import lint_audit


def span(path: str, line: int, source: str, start: int = 0) -> dict[str, Any]:
    return {"site": {"path": path, "start": start}, "end": start + len(source), "line": line, "column": 4,
            "end_line": line, "end_column": 4 + len(source), "source": source}


def lookup(path: str, line: int, owner: str = "crate::Panes::admit", start: int = 0) -> dict[str, Any]:
    return {
        "owner": owner,
        "context": {"context": "recurring", "reason": "every row"},
        "span": span(path, line, "self.panes.get_mut(&pane)", start),
        "expansion": [],
        "pattern": {
            "kind": "repeated_lookup", "collection": "alloc::collections::btree::map::BTreeMap",
            "receiver": "self.panes", "key": "pane", "guarded": True,
            "operations": [
                {"operation": "get_mut", "span": span(path, line, "self.panes.get_mut(&pane)", start)},
                {"operation": "insert", "span": span(path, line + 2, "self.panes.insert(pane, values)", start + 80)},
            ],
        },
    }


def evidence(configuration: str, *candidates: dict[str, Any]) -> dict[str, Any]:
    return {"configuration": {"name": configuration},
            "reports": {"crate::lib": {"crate_name": "crate", "audit": list(candidates)}}}


class CollectTests(unittest.TestCase):
    def test_configurations_merge_and_identities_ignore_offsets(self) -> None:
        first = lookup("src/panes.rs", 10)
        merged = lint_audit.collect([evidence("ordinary", first), evidence("testing", first)])
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]["configurations"], ["ordinary:crate", "testing:crate"])
        self.assertEqual(merged[0]["contexts"], [{"context": "recurring", "reason": "every row"}])
        moved = lint_audit.collect([evidence("ordinary", lookup("src/panes.rs", 40, start=900))])
        self.assertEqual(moved[0]["id"], merged[0]["id"])
        other = lint_audit.collect([evidence("ordinary", lookup("src/panes.rs", 10, owner="crate::Panes::evict"))])
        self.assertNotEqual(other[0]["id"], merged[0]["id"])

    def test_repeated_patterns_in_one_owner_keep_distinct_identities(self) -> None:
        merged = lint_audit.collect([evidence("ordinary", lookup("src/panes.rs", 10), lookup("src/panes.rs", 30, start=500))])
        self.assertEqual(len(merged), 2)
        self.assertEqual(merged[1]["id"], merged[0]["id"] + "-1")

    def test_markdown_escapes_cells_and_counts_suggestions(self) -> None:
        candidate = lint_audit.collect([evidence("ordinary", lookup("src/panes.rs", 10, owner="crate::f::{closure|0}"))])[0]
        candidate["attribution"] = {"suggestion": "suspected", "commit": {"sha": "a" * 40, "date": "2026-09-24", "subject": "s"}}
        summary = lint_audit.summarize([candidate])
        self.assertEqual(summary["repeated_lookup"]["recurring"], 1)
        self.assertEqual(summary["repeated_lookup"]["suspected"], 1)
        text = lint_audit.markdown({"revision": "r", "source_identity": "i", "configurations": ["ordinary"],
                                    "summary": summary, "candidates": [candidate]})
        self.assertIn("closure\\|0", text)
        self.assertIn("get_mut -> insert (guarded)", text)


class HunkTests(unittest.TestCase):
    def test_an_excerpt_keeps_the_lines_around_the_candidate(self) -> None:
        written = lint_audit.Hunk(1, 200, [], [f"line {number}" for number in range(1, 201)])
        shown = written.excerpt(100, 2)
        self.assertEqual(shown.after, ["line 98", "line 99", "line 100", "line 101", "line 102"])
        self.assertEqual(shown.before, [])
        replaced = lint_audit.Hunk(10, 3, ["old a", "old b"], ["new a", "new b", "new c"])
        self.assertEqual(replaced.excerpt(11, 2).before, ["old a", "old b"])
        self.assertEqual(replaced.excerpt(11, 2).after, ["new a", "new b", "new c"])
        shorter = lint_audit.Hunk(1, 200, [f"old {number}" for number in range(1, 11)],
                                  [f"line {number}" for number in range(1, 201)])
        self.assertEqual(shorter.excerpt(150, 2).before, ["old 6", "old 7", "old 8", "old 9", "old 10"])


class RepositoryTests(unittest.TestCase):
    """Attribution over a scratch repository with a real lint history."""

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.directory.name)
        self.environment = dict(os.environ)
        self.environment.update({"GIT_AUTHOR_NAME": "Audit", "GIT_AUTHOR_EMAIL": "audit@example.invalid",
                                 "GIT_COMMITTER_NAME": "Audit", "GIT_COMMITTER_EMAIL": "audit@example.invalid"})
        self.git("init", "--quiet", "--initial-branch=main")
        (self.root / "src/runtime").mkdir(parents=True)
        self.commit("2026-09-01", "old code", {
            "src/runtime/old.rs": "fn old(map: &mut M) {\n    map.insert(1, 2);\n    map.get(&1);\n}\n",
        })
        self.commit("2026-09-14", "count lock acquisitions", {
            "debt-baseline.json": json.dumps({"data_plane_lock_acquisitions": 3}),
            "src/runtime/panes.rs": "fn admit(map: &mut M, pane: u64) {\n    map.entry(pane).or_default().push(1);\n}\n",
        })
        self.commit("2026-09-20", "lower the lock count", {
            "debt-baseline.json": json.dumps({"data_plane_lock_acquisitions": 2}),
            "src/runtime/panes.rs": ("fn admit(map: &mut M, pane: u64) {\n    if let Some(values) = map.get_mut(&pane) {\n"
                                     "        values.push(1);\n    } else {\n        map.insert(pane, vec![1]);\n    }\n}\n"),
            "src/registry.rs": "fn helper(shared: &Shared) {\n    shared.lock();\n}\n",
        })
        self.commit("2026-09-25", "unrelated feature", {
            "src/later.rs": "fn later(map: &mut M) {\n    map.remove(&1);\n    map.insert(1, 2);\n}\n",
        })

    def tearDown(self) -> None:
        self.directory.cleanup()

    def git(self, *args: str, date: str | None = None) -> str:
        environment = dict(self.environment)
        if date is not None:
            environment.update({"GIT_AUTHOR_DATE": f"{date}T12:00:00+00:00", "GIT_COMMITTER_DATE": f"{date}T12:00:00+00:00"})
        completed = subprocess.run(["git", *args], cwd=self.root, env=environment, text=True,
                                   capture_output=True, check=True)
        return completed.stdout

    def commit(self, date: str, subject: str, files: dict[str, str]) -> None:
        for name, content in files.items():
            (self.root / name).write_text(content)
        self.git("add", "--all")
        self.git("commit", "--quiet", "-m", subject, date=date)

    def candidate(self, path: str, first: int, second: int) -> dict[str, Any]:
        lines = (self.root / path).read_text().splitlines()
        candidate = lookup(path, first)
        candidate["span"] = span(path, first, lines[first - 1].strip())
        candidate["pattern"]["operations"][0]["span"] = span(path, first, lines[first - 1].strip())
        candidate["pattern"]["operations"][1]["span"] = span(path, second, lines[second - 1].strip())
        return lint_audit.collect([evidence("ordinary", candidate)])[0]

    def attribute(self, candidate: dict[str, Any],
                  pull_requests: lint_audit.PullRequests | None = None) -> dict[str, Any]:
        history = lint_audit.History(self.root)
        blame = lint_audit.Blame(self.root)
        blame.load({candidate["path"]})
        lines = lint_audit.LineHistory(self.root)
        lines.load({lint_audit.primary_range(candidate)})
        attribution = lint_audit.Attribution(history, blame, lint_audit.Hunks(self.root),
                                              lint_audit.CommitSpellings(self.root, history), lines,
                                              pull_requests)
        return attribution.attribute(candidate)

    def test_history_records_introductions_and_lowerings(self) -> None:
        history = lint_audit.History(self.root)
        introduced = history.introduced["data_plane_lock_acquisitions"]
        self.assertEqual((introduced.date, introduced.subject), ("2026-09-14", "count lock acquisitions"))
        lowering = [sha for sha, commit in history.commits.items() if commit.subject == "lower the lock count"][0]
        self.assertEqual(history.event(lowering, "data_plane_lock_acquisitions"), "lowered 3->2")
        self.assertTrue(history.lowered(lowering, "data_plane_lock_acquisitions"))
        self.assertTrue(history.predates("2026-09-01", "data_plane_lock_acquisitions"))
        self.assertFalse(history.predates("2026-09-20", "data_plane_lock_acquisitions"))

    def test_a_lowering_that_removed_the_governed_spelling_is_suspected(self) -> None:
        attribution = self.attribute(self.candidate("src/runtime/panes.rs", 2, 5))
        self.assertEqual(attribution["suggestion"], "suspected")
        self.assertEqual(attribution["commit"]["subject"], "lower the lock count")
        self.assertEqual(attribution["before"], ["    map.entry(pane).or_default().push(1);"])
        self.assertIn("        map.insert(pane, vec![1]);", attribution["after"])
        rule = attribution["rules"][0]
        self.assertEqual((rule["event"], rule["spelling_removed"], rule["predates_rule"]), ("lowered 3->2", True, False))

    def test_a_lowering_that_moved_the_spelling_out_of_another_file_is_suspected(self) -> None:
        self.commit("2026-09-21", "count old entry", {
            "debt-baseline.json": json.dumps({"data_plane_lock_acquisitions": 3}),
            "src/runtime/old.rs": "fn old(map: &mut M) {\n    map.entry(1).or_insert(2);\n}\n",
        })
        # The port removes the counted spelling from one file and writes the lookup pair in
        # another, so the candidate's own hunk removed nothing.
        self.commit("2026-09-22", "port offsets", {
            "debt-baseline.json": json.dumps({"data_plane_lock_acquisitions": 2}),
            "src/runtime/old.rs": "fn old(map: &mut M) {\n    map.insert(1, 2);\n}\n",
            "src/runtime/ported.rs": ("fn ported(map: &mut M, key: u64) {\n    if let Some(value) = map.get_mut(&key) {\n"
                                      "        *value += 1;\n    } else {\n        map.insert(key, 1);\n    }\n}\n"),
        })
        attribution = self.attribute(self.candidate("src/runtime/ported.rs", 2, 5))
        self.assertEqual(attribution["suggestion"], "suspected")
        rule = attribution["rules"][0]
        self.assertEqual((rule["event"], rule["spelling_removed"], rule["commit_spelling_removed"]),
                         ("lowered 3->2", False, True))

    def pull_request(self, rewritten: str) -> lint_audit.PullRequests:
        """A squash-merged pull request #7 whose second commit replaced an entry with `rewritten`."""
        self.git("remote", "add", "origin", "https://github.com/example/project.git")
        pages = {
            "repos/example/project/pulls/7/commits": [[{"sha": "a" * 40, "parents": [{}]},
                                                        {"sha": "b" * 40, "parents": [{}]},
                                                        {"sha": "c" * 40, "parents": [{}, {}]}]],
            f"repos/example/project/commits/{'a' * 40}": [{"files": [{
                "filename": "src/runtime/sketches.rs",
                "patch": "@@ -0,0 +1,3 @@\n+fn admit(map: &mut M, pane: i64) {\n+    map.entry(pane).or_insert_with(S::new).add(1);\n+}"}]}],
            f"repos/example/project/commits/{'b' * 40}": [{"files": [{
                "filename": "src/runtime/sketches.rs",
                "patch": f"@@ -2 +2,5 @@\n-    map.entry(pane).or_insert_with(S::new).add(1);\n+{rewritten}"}]}],
            f"repos/example/project/commits/{'d' * 40}/pulls": [[{"number": 3, "merged_at": None},
                                                                 {"number": 9, "merged_at": "2026-09-23T12:00:00Z"}]],
            f"repos/example/project/commits/{'e' * 40}/pulls": [[]],
        }
        pull_requests = lint_audit.PullRequests(self.root)
        pull_requests.api = lambda path: pages[path]
        return pull_requests

    def reader(self) -> lint_audit.PullRequests:
        self.git("remote", "add", "origin", "https://github.com/example/project.git")
        return lint_audit.PullRequests(self.root)

    @staticmethod
    def gh(status: int, text: str) -> subprocess.CompletedProcess[str]:
        """A `gh api` call that ended with `status`, writing `text` to standard output on success and to
        standard error otherwise."""
        if status == 0:
            return subprocess.CompletedProcess([], status, stdout=text, stderr="")
        return subprocess.CompletedProcess([], status, stdout="", stderr=text)

    def test_a_transient_github_failure_is_read_again(self) -> None:
        pull_requests = self.reader()
        run = mock.Mock(side_effect=[self.gh(1, "gh: HTTP 500\n"), self.gh(1, "read: connection reset by peer\n"),
                                     self.gh(0, '[{"sha": "a"}]')])
        with mock.patch.object(lint_audit.subprocess, "run", run), \
                mock.patch.object(lint_audit.time, "sleep") as sleep:
            self.assertEqual(pull_requests.api("repos/example/project/commits/a"), [{"sha": "a"}])
        self.assertEqual(run.call_count, 3)
        self.assertEqual([call.args for call in sleep.call_args_list], [(2,), (4,)])

    def test_a_github_failure_that_persists_ends_the_audit(self) -> None:
        pull_requests = self.reader()
        run = mock.Mock(side_effect=[self.gh(1, "gh: HTTP 502\n")] * 4)
        with mock.patch.object(lint_audit.subprocess, "run", run), \
                mock.patch.object(lint_audit.time, "sleep") as sleep:
            with self.assertRaisesRegex(lint_audit.AuditError, r"commits/a failed after 4 attempt\(s\):\ngh: HTTP 502"):
                pull_requests.api("repos/example/project/commits/a")
        self.assertEqual(run.call_count, 4)
        self.assertEqual([call.args for call in sleep.call_args_list], [(2,), (4,), (8,)])

    def test_a_final_github_failure_is_not_read_again(self) -> None:
        pull_requests = self.reader()
        run = mock.Mock(side_effect=[self.gh(1, "gh: Not Found (HTTP 404)\n")])
        with mock.patch.object(lint_audit.subprocess, "run", run), \
                mock.patch.object(lint_audit.time, "sleep") as sleep:
            with self.assertRaisesRegex(lint_audit.AuditError, r"failed after 1 attempt\(s\):\ngh: Not Found"):
                pull_requests.api("repos/example/project/commits/a")
        self.assertEqual(run.call_count, 1)
        sleep.assert_not_called()

    def squash_sketches(self) -> None:
        self.commit("2026-09-23", "feat(window): add pane sketches (#7)", {
            "src/runtime/sketches.rs": ("fn admit(map: &mut M, pane: i64) {\n    if let Some(sketch) = map.get_mut(&pane) {\n"
                                        "        sketch.add(1);\n    } else {\n        map.insert(pane, S::new());\n    }\n}\n"),
        })

    def test_a_pull_request_that_rewrote_the_spelling_before_its_squash_is_suspected(self) -> None:
        self.squash_sketches()
        pull_requests = self.pull_request("    if let Some(sketch) = map.get_mut(&pane) {")
        attribution = self.attribute(self.candidate("src/runtime/sketches.rs", 2, 5), pull_requests)
        self.assertEqual(attribution["suggestion"], "suspected")
        rewrite = attribution["rules"][0]["pull_request_rewrite"]
        self.assertEqual(rewrite["sha"], "b" * 40)
        self.assertEqual(rewrite["removed"], ["    map.entry(pane).or_insert_with(S::new).add(1);"])
        self.assertEqual(pull_requests.number("refactor: merged under another title", "d" * 40), 9)
        self.assertIsNone(pull_requests.number("refactor: pushed without a pull request", "e" * 40))

    def test_a_pull_request_rewrite_of_other_lines_is_not_this_candidates(self) -> None:
        self.squash_sketches()
        pull_requests = self.pull_request("    let other = map.len();")
        attribution = self.attribute(self.candidate("src/runtime/sketches.rs", 2, 5), pull_requests)
        # Written into a counted file while the spelling counted, but not rewritten from it.
        self.assertEqual(attribution["suggestion"], "possible")
        self.assertNotIn("pull_request_rewrite", attribution["rules"][0])
        self.assertTrue(attribution["rules"][0]["written_while_counted"])

    def test_a_later_change_that_carried_the_pattern_keeps_its_origin(self) -> None:
        path = self.root / "src/runtime/panes.rs"
        path.write_text(path.read_text().replace("values.push(1);", "values.push(2);")
                        .replace("map.get_mut(&pane)", "map.get_mut(&(pane))"))
        self.git("add", "--all")
        self.git("commit", "--quiet", "-m", "tune admission", date="2026-09-26")
        attribution = self.attribute(self.candidate("src/runtime/panes.rs", 2, 5))
        self.assertEqual(attribution["commit"]["subject"], "tune admission")
        self.assertEqual(attribution["suggestion"], "suspected")
        self.assertEqual(attribution["origin"]["subject"], "lower the lock count")

    def discarded(self, path: str, line: int) -> dict[str, Any]:
        source = (self.root / path).read_text().splitlines()[line - 1].strip()
        raw = {"owner": "crate::parse", "context": None, "span": span(path, line, source), "expansion": [],
               "pattern": {"kind": "discarded_error", "operation": "map_err",
                           "error_type": "std::num::ParseIntError", "information": "value"}}
        return lint_audit.collect([evidence("ordinary", raw)])[0]

    def test_an_error_count_lowered_in_another_file_does_not_explain_a_discard(self) -> None:
        self.commit("2026-09-21", "count string errors", {
            "debt-baseline.json": json.dumps({"data_plane_lock_acquisitions": 2, "result_string_errors": 2}),
            "src/convert.rs": ("fn convert(text: &str) -> Result<u8, String> {\n    let trimmed = text.trim();\n"
                               "    let length = trimmed.len();\n    trimmed.parse().map_err(|error| error.to_string())\n}\n"),
            "src/parse.rs": "fn parse(text: &str) -> Result<u8, String> {\n    text.parse().map_err(|error| error.to_string())\n}\n",
        })
        # The signature and the discard are separate hunks of one file; the other discard sits in a
        # file whose own changes removed no string failure.
        self.commit("2026-09-22", "type conversion failures", {
            "debt-baseline.json": json.dumps({"data_plane_lock_acquisitions": 2, "result_string_errors": 1}),
            "src/convert.rs": ("fn convert(text: &str) -> Result<u8, Report<ConvertError>> {\n    let trimmed = text.trim();\n"
                               "    let length = trimmed.len();\n    trimmed.parse().map_err(|_| Report::new(ConvertError))\n}\n"),
            "src/other.rs": "fn other(text: &str) -> Option<u8> {\n    text.parse().map_err(|_| Missing).ok()\n}\n",
        })
        same = self.attribute(self.discarded("src/convert.rs", 4))
        self.assertEqual(same["suggestion"], "suspected")
        rule = [item for item in same["rules"] if item["rule"] == "result_string_errors"][0]
        self.assertEqual((rule["spelling_removed"], rule["file_spelling_removed"]), (False, True))
        elsewhere = self.attribute(self.discarded("src/other.rs", 2))
        self.assertEqual(elsewhere["suggestion"], "possible")
        rule = [item for item in elsewhere["rules"] if item["rule"] == "result_string_errors"][0]
        self.assertEqual((rule["file_spelling_removed"], rule["commit_spelling_removed"]), (False, True))

    def test_error_signature_spelling_names_a_plain_failure_not_a_report(self) -> None:
        spelling = lint_audit.RULES["bare_error_signatures"].spelling
        self.assertTrue(spelling.search("fn parse() -> Result<u8, ParseError> {"))
        self.assertTrue(spelling.search("fn parse() -> std::result::Result<u8, crate::ParseError> {"))
        self.assertFalse(spelling.search("fn parse() -> Result<u8, Report<ParseError>> {"))
        self.assertFalse(spelling.search("fn parse() -> Result<u8, String> {"))

    def test_code_before_the_rule_and_unrelated_later_code(self) -> None:
        self.assertEqual(self.attribute(self.candidate("src/runtime/old.rs", 2, 3))["suggestion"], "pre-existing")
        self.assertEqual(self.attribute(self.candidate("src/later.rs", 2, 3))["suggestion"], "unattributed")

    def test_code_written_into_counted_files_only_while_the_spelling_counted_is_possible(self) -> None:
        lookup_pair = ("fn admit(map: &mut M, key: u64) {\n    if let Some(value) = map.get_mut(&key) {\n"
                       "        *value += 1;\n    } else {\n        map.insert(key, 1);\n    }\n}\n")
        self.commit("2026-09-26", "add counted admission", {"src/runtime/counted.rs": lookup_pair})
        self.commit("2026-09-27", "resolve the lock count", {
            "debt-baseline.json": json.dumps({"data_plane_lock_acquisitions": 1}),
        })
        self.commit("2026-09-28", "add resolved admission", {"src/runtime/resolved.rs": lookup_pair})
        resolution = self.git("rev-parse", "HEAD~1").strip()
        with mock.patch.object(lint_audit, "LOCK_COUNTER_RESOLVED", resolution[:8]):
            counted = self.attribute(self.candidate("src/runtime/counted.rs", 2, 5))
            resolved = self.attribute(self.candidate("src/runtime/resolved.rs", 2, 5))
        self.assertEqual(counted["suggestion"], "possible")
        self.assertTrue(counted["rules"][0]["written_while_counted"])
        self.assertEqual(resolved["suggestion"], "unattributed")
        self.assertFalse(resolved["rules"][0]["written_while_counted"])

    def test_uncommitted_lines_have_no_attribution(self) -> None:
        path = self.root / "src/later.rs"
        path.write_text(path.read_text().replace("map.remove(&1);", "map.remove(&1).is_some();"))
        self.assertEqual(self.attribute(self.candidate("src/later.rs", 2, 3))["suggestion"], "uncommitted")

    def test_relocated_acquisitions_require_a_lowering_outside_the_counted_files(self) -> None:
        history = lint_audit.History(self.root)
        blame = lint_audit.Blame(self.root)
        finding = {"owner": "crate::helper", "receiver": "lock_api::mutex::Mutex", "operation": "lock",
                   "acquisition": "exclusive", "context": None, "expansion": [],
                   "span": span("src/registry.rs", 2, "lock")}
        inside = dict(finding, span=span("src/runtime/panes.rs", 2, "get_mut"))
        sites = [{"site": finding["span"]["site"], "configurations": {"ordinary:crate": [finding]}},
                 {"site": inside["span"]["site"], "configurations": {"ordinary:crate": [inside]}}]
        relocated = lint_audit.relocated_acquisitions(sites, history, blame)
        self.assertEqual([candidate["path"] for candidate in relocated], ["src/registry.rs"])
        self.assertEqual(relocated[0]["pattern"]["receiver"], "lock_api::mutex::Mutex")

    def test_main_writes_the_complete_report_from_compiler_evidence(self) -> None:
        candidate = self.candidate("src/runtime/panes.rs", 2, 5)
        raw = lookup("src/runtime/panes.rs", 2)
        raw["pattern"]["operations"] = [
            {"operation": operation["operation"], "span": operation["span"]}
            for operation in candidate["pattern"]["operations"]
        ]
        target = self.root / "target"
        gate = {"complete": True, "matrix_complete": True, "source_identity": "0123456789abcdef0123",
                "configurations": [{"name": "ordinary"}], "findings": [],
                "evidence": [evidence("ordinary", raw)]}

        def analysis(arguments: list[str]) -> int:
            destination = pathlib.Path(arguments[arguments.index("--output") + 1])
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_text(json.dumps(gate))
            return 0

        with mock.patch.object(lint_audit.typed_ratchet, "main", side_effect=analysis):
            self.assertEqual(lint_audit.main(["--root", str(self.root), "--target-dir", str(target)]), 0)
        directory = target / "lint-audit/0123456789abcdef"
        report = json.loads((directory / "audit.json").read_text())
        self.assertTrue(report["complete"])
        self.assertEqual(report["rules"]["data_plane_lock_acquisitions"]["date"], "2026-09-14")
        self.assertEqual([item["attribution"]["suggestion"] for item in report["candidates"]], ["suspected"])
        text = (directory / "audit.md").read_text()
        self.assertIn("| repeated_lookup | 1 | 1 | 1 | 0 | 0 | 0 |", text)
        self.assertIn("data_plane_lock_acquisitions: lowered 3->2, hunk removed spelling, written while counted", text)

    def test_an_incomplete_analysis_fails(self) -> None:
        with mock.patch.object(lint_audit.typed_ratchet, "main", return_value=1):
            self.assertEqual(lint_audit.main(["--root", str(self.root), "--target-dir", str(self.root / "target")]), 1)


if __name__ == "__main__":
    unittest.main()
