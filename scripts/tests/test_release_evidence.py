"""Exercise the release evidence gate on recorded check runs, without GitHub."""

from __future__ import annotations

import io
import json
import re
import shutil
import tempfile
import unittest
from collections.abc import Sequence
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock

from scripts import release_evidence
from scripts.release_evidence import CheckRun, EvidenceError, Method

ROOT = Path(__file__).resolve().parents[2]
REVISION = "0123456789abcdef0123456789abcdef01234567"


def workflow_job(workflow: str, job: str) -> str:
    """The lines of one top-level job of a workflow, up to the next job at the same indentation."""
    start = re.search(rf"^  {re.escape(job)}:$", workflow, re.M)
    assert start is not None, f"the workflow defines no job {job!r}"
    end = re.compile(r"^  [A-Za-z0-9_-]+:$", re.M).search(workflow, start.end())
    return workflow[start.end() : end.start() if end else len(workflow)]


SMALL_INVENTORY = """
[[method]]
name = "ordinary"
description = "Every pull request's checks"
checks = ["checks", "tests"]

[[method]]
name = "loom"
description = "Models and their weakening"
label = "loom"
checks = ["loom"]
sharded = ["loom-qualification"]
"""


def run(name: str, conclusion: str | None = "success", started: str = "2026-10-09T10:00:00Z",
        identity: int = 1, status: str = "completed") -> dict[str, object]:
    return {
        "id": identity,
        "name": name,
        "status": status,
        "conclusion": conclusion,
        "started_at": started,
        "completed_at": None if status != "completed" else "2026-10-09T11:00:00Z",
        "html_url": f"https://github.com/nervix-io/nervix/runs/{identity}",
    }


def runs(*entries: dict[str, object]) -> tuple[CheckRun, ...]:
    return release_evidence.parse_check_runs([{"check_runs": list(entries)}])


class InventoryTests(unittest.TestCase):
    def test_the_repository_inventory_declares_every_method_with_its_label(self) -> None:
        methods = release_evidence.parse_inventory((ROOT / release_evidence.INVENTORY).read_text())
        labels = {method.name: method.label for method in methods}
        self.assertEqual(
            labels,
            {
                "ordinary": None,
                "release-images": None,
                "bolero": "fuzz",
                "loom": "loom",
                "shuttle": "shuttle",
                "turmoil": "turmoil",
                "deloxide": "deloxide",
                "client-conformance": "client-conformance",
                "chaos-smoke": "chaos",
                "chaos-soak": "chaos-soak",
            },
        )

    def test_every_required_check_names_a_job_the_workflows_define(self) -> None:
        workflows = {
            path.name: path.read_text() for path in (ROOT / ".github/workflows").glob("*.yaml")
        }
        names = set()
        for text in workflows.values():
            names.update(match.strip().strip("'\"") for match in re.findall(r"^    name: (.+)$", text, re.M))
        methods = {
            method.name: method
            for method in release_evidence.parse_inventory((ROOT / release_evidence.INVENTORY).read_text())
        }
        # A matrix job's checks are its matrix values, read from the workflow, so a value added
        # there and missing here fails.
        build = workflow_job(workflows["docker-build.yaml"], "build-arch")
        images = re.findall(r"^\s+- image: (\S+)$", build, re.M)
        arches = re.findall(r"^\s+arch: (\S+)$", build, re.M)
        self.assertEqual(len(images), len(arches))
        self.assertIn("build-${{ matrix.image }}-${{ matrix.arch }}", names)
        builds = [f"build-{image}-{arch}" for image, arch in zip(images, arches)]
        self.assertEqual(sorted(methods["release-images"].checks), sorted(builds))
        names.update(builds)
        deloxide = workflow_job(workflows["check.yaml"], "deloxide")
        selections = re.search(r"^\s+selection: \[(.+)\]$", deloxide, re.M)
        self.assertIsNotNone(selections)
        self.assertIn("deloxide (${{ matrix.selection }})", names)
        lanes = [f"deloxide ({selection.strip()})" for selection in selections.group(1).split(",")]
        self.assertEqual(sorted(methods["deloxide"].checks), sorted(lanes))
        names.update(lanes)
        # The reusable Chaos workflow names its verdict job after its suite and image kind.
        self.assertIn("${{ inputs.suite }} ${{ inputs.image-kind }} verdict", names)
        for caller in ("chaos-smoke", "chaos-smoke-diagnostic", "chaos-soak", "chaos-soak-diagnostic"):
            suite = caller.split("-")[1]
            kind = "deloxide-order" if caller.endswith("diagnostic") else "ordinary"
            names.add(f"{caller} / {suite} {kind} verdict")
            self.assertIn(f"image-kind: {kind}", workflows["docker-build.yaml"])
        self.assertIn("bolero-random", names)
        names.update(f"bolero / {job}" for job in ("bolero-random", "bolero-fuzz", "bolero-gate"))
        for method in methods.values():
            for check in method.checks:
                with self.subTest(method=method.name, check=check):
                    self.assertIn(check, names)
            for prefix in method.sharded:
                with self.subTest(method=method.name, sharded=prefix):
                    self.assertTrue(any(name.startswith(f"{prefix} (") for name in names))

    def test_invalid_inventories_are_refused(self) -> None:
        cases = {
            "": "declares no method",
            "[other]\n": "unknown tables",
            "[[method]]\nname = 'a'\ndescription = 'd'\nchecks = ['x']\nextra = 1\n": "unknown keys",
            "[[method]]\nname = 'A'\ndescription = 'd'\nchecks = ['x']\n": "lowercase name",
            "[[method]]\nname = 'a'\ndescription = ' '\nchecks = ['x']\n": "needs a description",
            "[[method]]\nname = 'a'\ndescription = 'd'\nlabel = ''\nchecks = ['x']\n": "empty label",
            "[[method]]\nname = 'a'\ndescription = 'd'\n": "requires no check",
            "[[method]]\nname = 'a'\ndescription = 'd'\nchecks = ['x', 'x']\n": "name a check twice",
            "[[method]]\nname = 'a'\ndescription = 'd'\nchecks = [1]\n": "list of check names",
            "[[method]]\nname = 'a'\ndescription = 'd'\nchecks = ['x']\n"
            "[[method]]\nname = 'a'\ndescription = 'd'\nchecks = ['y']\n": "declared twice",
            "method = [1]\n": "is not a table",
        }
        for text, message in cases.items():
            with self.subTest(text=text):
                with self.assertRaisesRegex(EvidenceError, message):
                    release_evidence.parse_inventory(text)

    def test_owner_records_with_a_gap_are_unresolved_compliance(self) -> None:
        text = """
[[owner]]
path = "src/b.rs"
gap = "the lane does not reach its restart path"

[[owner]]
path = "src/a.rs"
workloads = ["probe.one"]
"""
        self.assertEqual(release_evidence.owner_gaps(text), ("src/b.rs",))
        self.assertEqual(release_evidence.owner_gaps((ROOT / release_evidence.DELOXIDE_INVENTORY).read_text()), ())
        with self.assertRaisesRegex(EvidenceError, "names no path"):
            release_evidence.owner_gaps("[[owner]]\nworkloads = []\n")
        with self.assertRaisesRegex(EvidenceError, "not a list of tables"):
            release_evidence.owner_gaps("owner = 1\n")


class JudgementTests(unittest.TestCase):
    def setUp(self) -> None:
        self.methods = release_evidence.parse_inventory(SMALL_INVENTORY)

    def complete(self) -> list[dict[str, object]]:
        return [
            run("checks", identity=1),
            run("tests", identity=2),
            run("loom", identity=3),
            run("loom-qualification (1 of 2)", identity=4),
            run("loom-qualification (2 of 2)", identity=5),
        ]

    def judge(self, entries: Sequence[dict[str, object]], labels: frozenset[str] = frozenset({"loom"}),
              gaps: Sequence[str] = ()) -> list[str]:
        _, problems = release_evidence.judge(self.methods, labels, runs(*entries), gaps)
        return problems

    def test_complete_evidence_qualifies(self) -> None:
        requirements, problems = release_evidence.judge(
            self.methods, frozenset({"loom"}), runs(*self.complete()), ()
        )
        self.assertEqual(problems, [])
        self.assertEqual(
            [requirement.check for requirement in requirements],
            ["checks", "tests", "loom", "loom-qualification (1 of 2)", "loom-qualification (2 of 2)"],
        )

    def test_a_missing_label_means_the_method_never_ran(self) -> None:
        problems = self.judge(self.complete(), labels=frozenset())
        self.assertEqual(problems, ["loom: the pull request does not carry the `loom` label, so CI never ran it"])

    def test_every_unfinished_or_unsuccessful_check_refuses_the_revision(self) -> None:
        for conclusion, status, verdict in (
            ("failure", "completed", "failure"),
            ("skipped", "completed", "skipped"),
            ("cancelled", "completed", "cancelled"),
            ("timed_out", "completed", "timed_out"),
            (None, "in_progress", "in_progress"),
            (None, "completed", "unknown"),
        ):
            with self.subTest(verdict=verdict):
                entries = self.complete()
                entries[1] = run("tests", conclusion=conclusion, status=status, identity=2)
                self.assertEqual(self.judge(entries), [f"ordinary: `tests`: its latest run is {verdict}"])

    def test_a_missing_check_refuses_the_revision(self) -> None:
        entries = [entry for entry in self.complete() if entry["name"] != "checks"]
        self.assertEqual(self.judge(entries), ["ordinary: `checks`: it did not run on the revision"])

    def test_the_latest_run_of_a_check_decides(self) -> None:
        entries = self.complete()
        entries.append(run("tests", conclusion="failure", started="2026-10-09T09:00:00Z", identity=9))
        self.assertEqual(self.judge(entries), [])
        entries.append(run("tests", conclusion="cancelled", started="2026-10-09T12:00:00Z", identity=10))
        self.assertEqual(self.judge(entries), ["ordinary: `tests`: its latest run is cancelled"])

    def test_a_matrix_needs_every_shard_of_one_total(self) -> None:
        entries = [entry for entry in self.complete() if entry["name"] != "loom-qualification (2 of 2)"]
        self.assertEqual(
            self.judge(entries), ["loom: `loom-qualification (2 of 2)`: it did not run on the revision"]
        )
        entries = [entry for entry in self.complete() if not str(entry["name"]).startswith("loom-q")]
        self.assertEqual(
            self.judge(entries),
            ["loom: `loom-qualification (every shard)`: no shard of this matrix ran on the revision"],
        )
        entries = self.complete() + [run("loom-qualification (3 of 3)", identity=11)]
        self.assertEqual(
            self.judge(entries),
            ["loom: `loom-qualification (every shard)`: its shards name different totals [2, 3], so the matrix is not one run"],
        )
        entries = self.complete() + [run("loom-qualification (3 of 2)", identity=12)]
        self.assertEqual(
            self.judge(entries), ["loom: `loom-qualification (3 of 2)`: a shard outside its matrix ran"]
        )
        entries = self.complete()
        entries[4] = run("loom-qualification (2 of 2)", conclusion="failure", identity=5)
        self.assertEqual(self.judge(entries), ["loom: `loom-qualification (2 of 2)`: its latest run is failure"])

    def test_owner_gaps_refuse_the_revision(self) -> None:
        self.assertEqual(
            self.judge(self.complete(), gaps=("src/a.rs",)),
            ["compliance: the tracked-lock owner src/a.rs records a path the diagnostic lane does not reach"],
        )

    def test_malformed_check_runs_are_refused(self) -> None:
        with self.assertRaisesRegex(EvidenceError, "no check_runs list"):
            release_evidence.parse_check_runs([{}])
        with self.assertRaisesRegex(EvidenceError, "not an object"):
            release_evidence.parse_check_runs([{"check_runs": [1]}])
        with self.assertRaisesRegex(EvidenceError, "lacks a field"):
            release_evidence.parse_check_runs([{"check_runs": [{"name": "x"}]}])


class FakeGitHub:
    """Answers the gate's gh calls from recorded answers, and records each call."""

    def __init__(self, labels: Sequence[str], entries: Sequence[dict[str, object]], page_size: int = 100):
        self.labels = labels
        self.entries = list(entries)
        self.page_size = page_size
        self.calls: list[list[str]] = []

    def __call__(self, arguments: Sequence[str]) -> str:
        self.calls.append(list(arguments))
        if arguments[:2] == ["pr", "view"]:
            return json.dumps({"headRefOid": REVISION, "labels": [{"name": label} for label in self.labels]})
        page = int(re.search(r"page=(\d+)$", arguments[1]).group(1))  # type: ignore[union-attr]
        start = (page - 1) * self.page_size
        return json.dumps({"check_runs": self.entries[start : start + self.page_size]})


class CheckCommandTests(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        (self.root / "tests").mkdir()
        (self.root / release_evidence.INVENTORY).write_text(SMALL_INVENTORY)
        shutil.copy(ROOT / release_evidence.DELOXIDE_INVENTORY, self.root / release_evidence.DELOXIDE_INVENTORY)
        self.entries = JudgementTests.complete(self)  # type: ignore[arg-type]

    def check(self, github: FakeGitHub) -> tuple[int, str]:
        output = io.StringIO()
        with redirect_stdout(output):
            status = release_evidence.check(712, self.root, github)
        return status, output.getvalue()

    def test_a_qualified_revision_writes_its_register(self) -> None:
        github = FakeGitHub(["loom", "chaos"], self.entries)
        status, output = self.check(github)
        self.assertEqual(status, 0)
        directory = self.root / release_evidence.OUTPUT / REVISION
        record = json.loads((directory / "register.json").read_text())
        self.assertEqual(record["verdict"], "qualified")
        self.assertEqual(record["pull_request"], 712)
        self.assertEqual(record["labels"], ["chaos", "loom"])
        self.assertEqual(record["compliance"], {"deloxide_owner_gaps": []})
        loom = record["methods"][1]
        self.assertEqual(loom["name"], "loom")
        self.assertEqual(
            loom["checks"][0],
            {
                "check": "loom",
                "verdict": "passed",
                "problem": None,
                "conclusion": "success",
                "url": "https://github.com/nervix-io/nervix/runs/3",
                "started_at": "2026-10-09T10:00:00Z",
                "completed_at": "2026-10-09T11:00:00Z",
            },
        )
        markdown = (directory / "register.md").read_text()
        self.assertIn(f"## Release evidence of revision `{REVISION}`: qualified", markdown)
        self.assertIn("| loom | [loom](https://github.com/nervix-io/nervix/runs/3) | passed | success |", markdown)
        self.assertEqual(output.splitlines()[0], markdown.splitlines()[0])
        self.assertEqual(github.calls[0][:3], ["pr", "view", "712"])

    def test_a_refused_revision_names_every_reason(self) -> None:
        entries = [entry for entry in self.entries if entry["name"] != "tests"]
        status, output = self.check(FakeGitHub([], entries))
        self.assertEqual(status, 1)
        self.assertIn("Refused because:", output)
        self.assertIn("- loom: the pull request does not carry the `loom` label", output)
        self.assertIn("- ordinary: `tests`: it did not run on the revision", output)
        self.assertIn("| ordinary | tests | refused | — | — |", output)

    def test_check_runs_are_read_page_by_page(self) -> None:
        filler = [run(f"other {index}", identity=100 + index) for index in range(5)]
        github = FakeGitHub(["loom"], filler + self.entries, page_size=5)
        with mock.patch.object(release_evidence, "PAGE_SIZE", 5):
            status, _ = self.check(github)
        self.assertEqual(status, 0)
        self.assertEqual(len(github.calls), 4)
        with mock.patch.object(release_evidence, "PAGE_SIZE", 5), mock.patch.object(release_evidence, "MAX_PAGES", 2):
            with self.assertRaisesRegex(EvidenceError, "more than 10 check runs"):
                self.check(FakeGitHub(["loom"], filler + self.entries, page_size=5))

    def test_a_pull_request_without_a_head_revision_is_an_error(self) -> None:
        def github(arguments: Sequence[str]) -> str:
            return json.dumps({"headRefOid": "main", "labels": []})

        with self.assertRaisesRegex(EvidenceError, "has no head revision"):
            release_evidence.check(1, self.root, github)

    def test_main_reports_errors_with_status_two(self) -> None:
        errors = io.StringIO()
        with redirect_stderr(errors), mock.patch.object(
            release_evidence, "run_gh", side_effect=EvidenceError("gh is not authenticated")
        ):
            status = release_evidence.main(["check", "--pr", "3", "--root", str(self.root)])
        self.assertEqual(status, 2)
        self.assertIn("release evidence error: gh is not authenticated", errors.getvalue())

    def test_main_runs_the_check_command(self) -> None:
        github = FakeGitHub(["loom"], self.entries)
        output = io.StringIO()
        with redirect_stdout(output), mock.patch.object(release_evidence, "run_gh", github):
            status = release_evidence.main(["check", "--pr", "4", "--root", str(self.root)])
        self.assertEqual(status, 0)
        self.assertIn("qualified", output.getvalue())

    def test_gh_failures_are_evidence_errors(self) -> None:
        with mock.patch("subprocess.run", side_effect=OSError("gh: not found")):
            with self.assertRaisesRegex(EvidenceError, "gh pr view 1 failed: gh: not found"):
                release_evidence.run_gh(["pr", "view", "1"])
        completed = mock.Mock(stdout='{"ok": true}\n')
        with mock.patch("subprocess.run", return_value=completed) as invoked:
            self.assertEqual(release_evidence.run_gh(["api", "x"]), '{"ok": true}\n')
        self.assertEqual(invoked.call_args.args[0], ["gh", "api", "x"])


class MethodTests(unittest.TestCase):
    def test_methods_keep_their_declared_order(self) -> None:
        methods = release_evidence.parse_inventory(SMALL_INVENTORY)
        self.assertEqual(
            methods,
            (
                Method("ordinary", "Every pull request's checks", None, ("checks", "tests"), ()),
                Method("loom", "Models and their weakening", "loom", ("loom",), ("loom-qualification",)),
            ),
        )


if __name__ == "__main__":
    unittest.main()
