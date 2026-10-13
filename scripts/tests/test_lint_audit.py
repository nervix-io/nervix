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
            "src/runtime/later.rs": "fn later(map: &mut M) {\n    map.remove(&1);\n    map.insert(1, 2);\n}\n",
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
        candidate["pattern"]["operations"][1]["span"] = span(path, second, lines[second - 1].strip())
        return lint_audit.collect([evidence("ordinary", candidate)])[0]

    def attribute(self, candidate: dict[str, Any]) -> dict[str, Any]:
        history = lint_audit.History(self.root)
        blame = lint_audit.Blame(self.root)
        blame.load({candidate["path"]})
        return lint_audit.attribute(candidate, history, blame, lint_audit.Hunks(self.root))

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

    def test_code_before_the_rule_and_unrelated_later_code(self) -> None:
        self.assertEqual(self.attribute(self.candidate("src/runtime/old.rs", 2, 3))["suggestion"], "pre-existing")
        self.assertEqual(self.attribute(self.candidate("src/runtime/later.rs", 2, 3))["suggestion"], "unattributed")

    def test_uncommitted_lines_have_no_attribution(self) -> None:
        path = self.root / "src/runtime/later.rs"
        path.write_text(path.read_text().replace("map.remove(&1);", "map.remove(&1).is_some();"))
        self.assertEqual(self.attribute(self.candidate("src/runtime/later.rs", 2, 3))["suggestion"], "uncommitted")

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
        self.assertIn("| repeated_lookup | 1 | 1 | 1 | 0 | 0 | 0 |", (directory / "audit.md").read_text())

    def test_an_incomplete_analysis_fails(self) -> None:
        with mock.patch.object(lint_audit.typed_ratchet, "main", return_value=1):
            self.assertEqual(lint_audit.main(["--root", str(self.root), "--target-dir", str(self.root / "target")]), 1)


if __name__ == "__main__":
    unittest.main()
