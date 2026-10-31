"""Exercise source selection, GitHub event snapshots and validation-only reruns."""

from __future__ import annotations

import contextlib
import dataclasses
import io
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import unittest
from unittest import mock

from scripts import ci_test_selection as selection
from scripts.tests import test_bolero_coverage


ROOT = Path(__file__).resolve().parents[2]

# A base branch commit that lands after the event recorded its base, before GitHub builds the merge.
BASE_ADVANCE = {"src/engine.rs": "fn advance() {}\n"}


class MergeFixture:
    """A base branch, a pull request branch, and GitHub's tested merge of the pull request's head."""

    def __init__(self, directory: Path) -> None:
        self.directory = directory
        self.root = directory / "source"
        self.initialize_repository(self.root)
        self.write("README.md", "Nervix\n")
        self.write("src/runtime.rs", "fn run() {}\n")
        self.git("add", ".")
        self.git("commit", "-m", "base")
        self.base = self.git("rev-parse", "HEAD")
        self.git("switch", "-c", "change")

    def initialize_repository(self, root: Path, *, bare: bool = False) -> None:
        arguments = ["init", "--quiet", "--initial-branch=main"]
        if bare:
            arguments.append("--bare")
        self.git(*arguments, str(root), cwd=self.directory)
        # Persist this in every repository: selection's own fetch and receive-pack on the bare
        # origin also run maintenance. Waiting for their callers must wait for every writer.
        for setting in ("maintenance.autoDetach", "gc.autoDetach"):
            self.git("config", setting, "false", cwd=root)

    def git(self, *args: str, cwd: Path | None = None) -> str:
        return subprocess.check_output([
            "git", "-c", "user.name=CI selection test", "-c", "user.email=ci@example.com",
            *args,
        ], cwd=cwd or self.root, text=True, stderr=subprocess.PIPE).strip()

    def write(self, path: str, content: str) -> None:
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)

    def advance_base(self, changes: dict[str, str]) -> str:
        """Land a commit on the base branch after the event recorded the pull request's base."""
        self.git("switch", "main")
        for path, content in changes.items():
            self.write(path, content)
        self.git("add", "-A")
        self.git("commit", "-m", "advance base")
        advanced = self.git("rev-parse", "HEAD")
        self.git("switch", "change")
        return advanced

    def commit_change(self) -> str:
        self.git("add", "-A")
        self.git("commit", "--allow-empty", "-m", "change")
        return self.git("rev-parse", "HEAD")

    def event(self, head: str, labels: tuple[str, ...] = (), action: str = "synchronize") -> dict:
        """The event names the base GitHub recorded before the base branch moved on."""
        return {
            "action": action,
            "pull_request": {
                "base": {"sha": self.base}, "head": {"sha": head},
                "labels": [{"name": label} for label in labels],
            },
        }

    def merge(self, labels: tuple[str, ...] = (), action: str = "synchronize",
              advanced_base: dict[str, str] | None = None) -> tuple[dict, str]:
        """Commit the change and merge it as GitHub does, into the base branch's current tip."""
        head = self.commit_change()
        if advanced_base:
            self.advance_base(advanced_base)
        self.git("switch", "--detach", "main")
        self.git("merge", "--no-ff", "change", "-m", "tested merge")
        return self.event(head, labels, action), self.git("rev-parse", "HEAD")

    def publish(self, tested: str) -> Path:
        """Serve the branches and the tested merge's pull ref from a remote that filters, as GitHub does."""
        origin = self.directory / "origin.git"
        if not origin.exists():
            self.initialize_repository(origin, bare=True)
            self.git("config", "uploadpack.allowFilter", "true", cwd=origin)
        self.git("push", "--quiet", "--force", origin.as_uri(), "refs/heads/main:refs/heads/main",
                 "refs/heads/change:refs/heads/change", f"{tested}:refs/pull/1/merge")
        return origin

    def shallow_checkout(self, tested: str, name: str = "checkout") -> Path:
        """Check the tested merge out exactly as actions/checkout does with fetch-depth 2."""
        origin = self.publish(tested)
        checkout = self.directory / name
        self.initialize_repository(checkout)
        self.git("remote", "add", "origin", origin.as_uri(), cwd=checkout)
        self.git("-c", "protocol.version=2", "fetch", "--no-tags", "--prune", "--no-recurse-submodules",
                 "--depth=2", "origin", f"+{tested}:refs/remotes/pull/1/merge", cwd=checkout)
        self.git("checkout", "--quiet", "--force", "refs/remotes/pull/1/merge", cwd=checkout)
        return checkout


class MergeFixtureLifetimeTests(unittest.TestCase):
    def test_repository_writers_finish_before_temporary_directory_cleanup(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            trace = root / "git-trace.jsonl"
            configuration = root / "gitconfig"
            configuration.write_text(
                "[maintenance]\n\tauto = true\n\tautoDetach = true\n"
                "[maintenance \"gc\"]\n\tenabled = false\n"
                "[maintenance \"commit-graph\"]\n\tenabled = true\n\tauto = -1\n"
                "[gc]\n\tautoDetach = true\n"
            )
            with mock.patch.dict(os.environ, {
                "GIT_TRACE2_EVENT": str(trace), "GIT_CONFIG_GLOBAL": str(configuration),
                "GIT_CONFIG_NOSYSTEM": "1",
            }):
                fixture = MergeFixture(root)
                fixture.write("README.md", "Updated prose\n")
                payload, tested = fixture.merge(advanced_base=BASE_ADVANCE)
                checkout = fixture.shallow_checkout(tested)
                first = selection.TestSelection.read("pull_request", payload, tested, checkout)
                rerun = fixture.shallow_checkout(tested, "rerun")
                self.assertEqual(selection.TestSelection.read("pull_request", payload, tested, rerun), first)

            events = [json.loads(line) for line in trace.read_text().splitlines()]
            maintenance = [event for event in events if event["event"] == "start"
                           and "maintenance" in event["argv"]]
            self.assertTrue(maintenance, "the regression must exercise real Git maintenance")
            # Git traces this region before forking, so the check does not race a detached child.
            detached = [event for event in events if event["event"] == "region_enter"
                        and event.get("label") == "detach"]
            self.assertEqual(detached, [], "a repository writer can outlive its fixture command")
            for repository in (fixture.root / ".git", root / "origin.git", checkout / ".git", rerun / ".git"):
                graphs = repository / "objects/info/commit-graphs"
                self.assertTrue(list(graphs.glob("graph-*.graph")), f"maintenance did not write in {repository}")


class AdvancedBaseSelectionTests(unittest.TestCase):
    """GitHub records a pull request's base when it opens, but merges each new head into the current tip."""

    def test_tested_merge_into_an_advanced_base_selects_from_that_base(self) -> None:
        for shallow in (False, True):
            for path, documentation_only in (("README.md", True), ("src/runtime.rs", False)):
                with self.subTest(shallow=shallow, path=path), tempfile.TemporaryDirectory() as directory:
                    fixture = MergeFixture(Path(directory))
                    fixture.write(path, "Changed by the pull request\n")
                    payload, tested = fixture.merge(advanced_base=BASE_ADVANCE)
                    advanced = fixture.git("rev-parse", "main")
                    root = fixture.shallow_checkout(tested) if shallow else fixture.root
                    actual = selection.TestSelection.read("pull_request", payload, tested, root)
                    self.assertEqual(actual.event_base_sha, fixture.base)
                    self.assertEqual(actual.tested_base_sha, advanced)
                    self.assertEqual(actual.head_sha, payload["pull_request"]["head"]["sha"])
                    self.assertEqual(actual.paths, (path,))
                    self.assertEqual(actual.documentation_only, documentation_only)
                    self.assertEqual(actual.bolero_required, not documentation_only)

    def test_shallow_checkout_fetches_commit_history_only_to_prove_an_advanced_base(self) -> None:
        for advanced in (False, True):
            with self.subTest(advanced=advanced), tempfile.TemporaryDirectory() as directory:
                fixture = MergeFixture(Path(directory))
                fixture.write("README.md", "Updated prose\n")
                payload, tested = fixture.merge(advanced_base=BASE_ADVANCE if advanced else None)
                checkout = fixture.shallow_checkout(tested)
                actual = selection.TestSelection.read("pull_request", payload, tested, checkout)
                self.assertEqual(actual.paths, ("README.md",))
                self.assertTrue(actual.documentation_only)
                event_base_tree = subprocess.run(
                    ["git", "--no-lazy-fetch", "cat-file", "-e", f"{fixture.base}^{{tree}}"],
                    cwd=checkout, capture_output=True,
                )
                if advanced:
                    # The proof fetched the tested base's ancestry as commits; their trees stay remote.
                    event_base = fixture.git("--no-lazy-fetch", "cat-file", "-t", fixture.base, cwd=checkout)
                    self.assertEqual(event_base, "commit")
                    self.assertNotEqual(event_base_tree.returncode, 0)
                else:
                    # The event's own base is the tested merge's parent, so nothing more is fetched.
                    shallow = fixture.git("rev-parse", "--is-shallow-repository", cwd=checkout)
                    self.assertEqual(shallow, "true")
                    self.assertEqual(event_base_tree.returncode, 0)

    def test_history_fetch_failure_propagates_without_a_selection(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = MergeFixture(Path(directory))
            fixture.write("README.md", "Updated prose\n")
            payload, tested = fixture.merge(advanced_base=BASE_ADVANCE)
            checkout = fixture.shallow_checkout(tested)
            shutil.rmtree(fixture.directory / "origin.git")
            with self.assertRaises(subprocess.CalledProcessError):
                selection.TestSelection.read("pull_request", payload, tested, checkout)

    def test_reruns_reach_the_same_selection_after_the_base_branch_advances_again(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = MergeFixture(Path(directory))
            fixture.write("docs/src/guide.md", "Updated prose\n")
            payload, tested = fixture.merge(("fuzz",), "labeled", advanced_base=BASE_ADVANCE)
            checkout = fixture.shallow_checkout(tested)
            first = selection.TestSelection.read("pull_request", payload, tested, checkout)
            # A rerun of the same job on its own runner reads the same event and checkout.
            self.assertEqual(selection.TestSelection.read("pull_request", payload, tested, checkout), first)
            fixture.advance_base({"src/later.rs": "fn later() {}\n"})
            rerun = fixture.shallow_checkout(tested, "rerun")
            self.assertEqual(selection.TestSelection.read("pull_request", payload, tested, rerun), first)
            self.assertTrue(first.documentation_only)
            self.assertTrue(first.bolero_required)

    def test_stale_or_tampered_identities_fail_instead_of_excluding_tests(self) -> None:
        for shallow in (False, True):
            with self.subTest(shallow=shallow), tempfile.TemporaryDirectory() as directory:
                fixture = MergeFixture(Path(directory))
                fixture.write("README.md", "Updated prose\n")
                payload, tested = fixture.merge(advanced_base=BASE_ADVANCE)
                tested_base = fixture.git("rev-parse", "main")
                head = payload["pull_request"]["head"]["sha"]
                # After GitHub built this tested merge, the head and the base branch both move on.
                fixture.git("switch", "change")
                fixture.write("docs/later.md", "Later prose\n")
                newer_head = fixture.commit_change()
                newer_base = fixture.advance_base({"src/later.rs": "fn later() {}\n"})
                fixture.git("switch", "-c", "elsewhere", fixture.base)
                fixture.write("src/elsewhere.rs", "fn elsewhere() {}\n")
                diverged = fixture.commit_change()
                if shallow:
                    root = fixture.shallow_checkout(tested)
                else:
                    root = fixture.root
                    fixture.git("switch", "--detach", tested)
                for which, value, message in (
                    ("head", newer_head, "exact head"), ("head", "a" * 40, "exact head"),
                    ("head", tested_base, "exact head"), ("base", newer_base, "base must be"),
                    ("base", diverged, "base must be"), ("base", "a" * 40, "base must be"),
                    ("base", head, "base must be"),
                ):
                    changed = json.loads(json.dumps(payload))
                    changed["pull_request"][which]["sha"] = value
                    with self.subTest(which=which, value=value), self.assertRaisesRegex(ValueError, message):
                        selection.TestSelection.read("pull_request", changed, tested, root)
                with self.assertRaises(KeyError):
                    selection.TestSelection.read("pull_request", {}, tested, root)
                fixture.git("checkout", "--quiet", "--detach", tested_base, cwd=root)
                with self.assertRaisesRegex(ValueError, "checkout must be"):
                    selection.TestSelection.read("pull_request", payload, tested, root)

    def test_arbitrary_merges_fail_instead_of_excluding_tests(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = MergeFixture(Path(directory))
            fixture.write("README.md", "Updated prose\n")
            head = fixture.commit_change()
            fixture.advance_base(BASE_ADVANCE)
            event = fixture.event(head)
            with self.subTest(merge="the head itself"):
                with self.assertRaisesRegex(ValueError, "exact head into one base"):
                    selection.TestSelection.read("pull_request", event, head, fixture.root)
            with self.subTest(merge="the base merged into the head"):
                fixture.git("switch", "--detach", "change")
                fixture.git("merge", "--no-ff", "-m", "base into head", "main")
                tested = fixture.git("rev-parse", "HEAD")
                with self.assertRaisesRegex(ValueError, "exact head into one base"):
                    selection.TestSelection.read("pull_request", event, tested, fixture.root)
            with self.subTest(merge="an octopus merge"):
                fixture.git("switch", "-c", "side", fixture.base)
                fixture.write("docs/side.md", "Side prose\n")
                fixture.commit_change()
                fixture.git("switch", "--detach", "main")
                fixture.git("merge", "--no-ff", "-m", "octopus", "change", "side")
                tested = fixture.git("rev-parse", "HEAD")
                with self.assertRaisesRegex(ValueError, "exact head into one base"):
                    selection.TestSelection.read("pull_request", event, tested, fixture.root)
            with self.subTest(merge="the head merged into an unrelated history"):
                fixture.git("switch", "--orphan", "unrelated")
                fixture.write("docs/unrelated.md", "Unrelated prose\n")
                fixture.commit_change()
                fixture.git("merge", "--no-ff", "--allow-unrelated-histories", "-m", "unrelated", "change")
                tested = fixture.git("rev-parse", "HEAD")
                with self.assertRaisesRegex(ValueError, "base must be"):
                    selection.TestSelection.read("pull_request", event, tested, fixture.root)

    def test_ancestry_failure_propagates_instead_of_deciding(self) -> None:
        checkout = selection.Checkout(Path("."))
        answers = [subprocess.CompletedProcess(["git", "rev-parse"], 0),
                   subprocess.CompletedProcess(["git", "merge-base"], 128)]
        with mock.patch.object(selection.subprocess, "run", side_effect=answers), \
                self.assertRaises(subprocess.CalledProcessError):
            checkout.is_ancestor("a" * 40, "b" * 40)


class SourceSelectionTests(unittest.TestCase):
    def test_verified_prose_locations_exclude_ordinary_suites(self) -> None:
        for path in ("AGENTS.md", "README.md", "docs/src/property-testing-and-fuzzing.md",
                     "docs/review notes/évidence\n.md", ".agents/skills/plexus/SKILL.md"):
            with self.subTest(path=path), tempfile.TemporaryDirectory() as directory:
                fixture = MergeFixture(Path(directory))
                fixture.write(path, "Updated prose\n")
                payload, tested = fixture.merge()
                actual = selection.TestSelection.read("pull_request", payload, tested, fixture.root)
                self.assertTrue(actual.documentation_only)
                self.assertFalse(actual.bolero_required)
                self.assertEqual(actual.paths, (path,))

    def test_code_tests_and_unknown_configuration_require_suites_with_no_docker(self) -> None:
        for path in ("src/runtime.rs", "tests/features/runtime/output.feature", "scripts/ci_test_selection.py",
                     ".github/workflows/check.yaml", "Cargo.toml", "docs/theme/script.js",
                     "docs/book.toml", "tests/corpus/case.md"):
            with self.subTest(path=path), tempfile.TemporaryDirectory() as directory:
                fixture = MergeFixture(Path(directory))
                fixture.write(path, "changed\n")
                fixture.write("README.md", "also changed\n")
                payload, tested = fixture.merge(("no-docker",))
                actual = selection.TestSelection.read("pull_request", payload, tested, fixture.root)
                self.assertFalse(actual.documentation_only)
                self.assertTrue(actual.bolero_required)
                self.assertIn(path, actual.paths)

    def test_rename_or_delete_of_product_code_cannot_be_hidden_in_documentation(self) -> None:
        for rename in (False, True):
            with self.subTest(rename=rename), tempfile.TemporaryDirectory() as directory:
                fixture = MergeFixture(Path(directory))
                if rename:
                    (fixture.root / "docs").mkdir()
                    fixture.git("mv", "src/runtime.rs", "docs/runtime.md")
                else:
                    fixture.git("rm", "src/runtime.rs")
                payload, tested = fixture.merge()
                actual = selection.TestSelection.read("pull_request", payload, tested, fixture.root)
                self.assertIn("src/runtime.rs", actual.paths)
                self.assertFalse(actual.documentation_only)
                self.assertTrue(actual.bolero_required)

    def test_empty_diff_is_not_proof_of_documentation_only_work(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = MergeFixture(Path(directory))
            payload, tested = fixture.merge()
            actual = selection.TestSelection.read("pull_request", payload, tested, fixture.root)
            self.assertEqual(actual.paths, ())
            self.assertFalse(actual.documentation_only)
            self.assertTrue(actual.bolero_required)

    def test_label_events_preserve_explicit_bolero_requests(self) -> None:
        for advanced in (False, True):
            for action, labels, required in (
                ("opened", (), False), ("labeled", ("no-docker",), False),
                ("labeled", ("fuzz",), True), ("synchronize", ("fuzz", "no-docker"), True),
                ("labeled", ("bolero-random",), True), ("synchronize", ("bolero-random", "no-docker"), True),
                ("labeled", ("bolero-random", "fuzz"), True),
                ("unlabeled", (), False), ("reopened", (), False),
            ):
                with self.subTest(advanced=advanced, action=action, labels=labels), \
                        tempfile.TemporaryDirectory() as directory:
                    fixture = MergeFixture(Path(directory))
                    fixture.write("README.md", "Updated prose\n")
                    advanced_base = BASE_ADVANCE if advanced else None
                    payload, tested = fixture.merge(labels, action, advanced_base)
                    actual = selection.TestSelection.read("pull_request", payload, tested, fixture.root)
                    self.assertTrue(actual.documentation_only)
                    self.assertEqual(actual.paths, ("README.md",))
                    self.assertEqual(actual.bolero_required, required)
                    self.assertEqual(actual.labels, labels)
                    self.assertEqual(actual.action, action)

    def test_non_pr_events_require_execution_without_a_prose_exclusion(self) -> None:
        for event in ("workflow_dispatch", "push"):
            with self.subTest(event=event):
                actual = selection.TestSelection.read(event, {}, "a" * 40)
                self.assertFalse(actual.documentation_only)
                self.assertTrue(actual.bolero_required)
                self.assertIsNone(actual.paths)
                self.assertIsNone(actual.event_base_sha)
                self.assertIsNone(actual.tested_base_sha)

    def test_commit_identity_is_not_a_ref_or_command_argument(self) -> None:
        for value in ("main", "", "--help", "a" * 39, "g" * 40, "a" * 40 + "\n"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                selection.commit_sha(value)

    def test_git_failure_propagates_without_a_selection(self) -> None:
        with mock.patch.object(selection.subprocess, "check_output", side_effect=subprocess.CalledProcessError(1, "git")):
            with self.assertRaises(subprocess.CalledProcessError):
                selection.TestSelection.read("pull_request", {
                    "pull_request": {"base": {"sha": "a" * 40}, "head": {"sha": "b" * 40}},
                }, "c" * 40)

    def test_selection_record_and_outputs_do_not_claim_executed_properties(self) -> None:
        for advanced in (False, True):
            for path, required in (("README.md", False), ("src/runtime.rs", True)):
                with self.subTest(advanced=advanced, path=path), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    fixture = MergeFixture(root)
                    fixture.write(path, "changed\n")
                    advanced_base = BASE_ADVANCE if advanced else None
                    payload, tested = fixture.merge(advanced_base=advanced_base)
                    tested_base = fixture.git("rev-parse", "main")
                    checkout = fixture.shallow_checkout(tested)
                    event_file = root / "event.json"
                    event_file.write_text(json.dumps(payload))
                    record_path = root / "evidence/selection.json"
                    outputs = root / "outputs"
                    environment = {
                        "GITHUB_EVENT_NAME": "pull_request", "GITHUB_EVENT_PATH": str(event_file),
                        "GITHUB_SHA": tested, "GITHUB_WORKSPACE": str(checkout), "GITHUB_RUN_ID": "100",
                        "GITHUB_RUN_ATTEMPT": "2", "GITHUB_JOB": "test-selection", "GITHUB_OUTPUT": str(outputs),
                    }
                    with mock.patch.dict(os.environ, environment), \
                            mock.patch("sys.argv", ["ci_test_selection", "--record", str(record_path)]), \
                            contextlib.redirect_stdout(io.StringIO()):
                        selection.main()
                    record = json.loads(record_path.read_text())
                    self.assertEqual(record["verdict"], "selected")
                    self.assertEqual(record["run"], "100")
                    self.assertEqual(record["attempt"], "2")
                    self.assertEqual(record["job"], "test-selection")
                    self.assertEqual(record["tested_sha"], tested)
                    self.assertEqual(record["event_base_sha"], fixture.base)
                    self.assertEqual(record["tested_base_sha"], tested_base)
                    self.assertEqual(record["tested_base_sha"] != record["event_base_sha"], advanced)
                    self.assertEqual(record["head_sha"], payload["pull_request"]["head"]["sha"])
                    self.assertEqual(record["paths"], [path])
                    self.assertEqual(record["labels"], [])
                    self.assertEqual(record["tests_required"], required)
                    self.assertEqual(record["bolero_required"], required)
                    self.assertEqual(record["documentation_only"], not required)
                    reason = "suites-required" if required else "verified-documentation-only"
                    self.assertEqual(record["reason"], reason)
                    requirement = str(required).lower()
                    self.assertEqual(outputs.read_text(),
                                     f"tests-required={requirement}\nbolero-required={requirement}\n")


class CheckWorkflowSelectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.check = (ROOT / ".github/workflows/check.yaml").read_text()

    def job(self, name: str) -> str:
        return re.split(r"\n  [a-z][a-z-]*:\n", self.check.split(f"\n  {name}:\n", 1)[1], maxsplit=1)[0]

    def test_ordinary_producers_wait_for_the_same_verified_selection(self) -> None:
        for name in ("tests", "scenarios-build", "extra-tests", "diagnostic-evidence"):
            with self.subTest(job=name):
                job = self.job(name)
                self.assertIn("needs: [test-selection]", job)
                self.assertIn("needs.test-selection.outputs.tests-required == 'true'", job)
        # The scenario shards run only from a build the selection required.
        self.assertIn("needs: [scenarios-build]", self.job("scenarios"))
        self.assertIn("needs: [tests, scenarios, extra-tests]", self.job("coverage"))
        for name in ("checks", "conventional-pr-title"):
            with self.subTest(job=name):
                self.assertNotIn("needs:", self.job(name))

    def test_selection_retains_verified_paths_and_job_identity_before_calling_suites(self) -> None:
        job = self.job("test-selection")
        self.assertIn("fetch-depth: 2", job)
        self.assertIn("persist-credentials: true", job)
        self.assertIn("run: python3 scripts/ci_test_selection.py --record target/ci-test-selection.json", job)
        self.assertIn("tests-required: ${{ steps.selection.outputs.tests-required }}", job)
        self.assertIn("bolero-required: ${{ steps.selection.outputs.bolero-required }}", job)
        self.assertIn("uses: ./.github/actions/ci-artifact-upload", job)
        self.assertIn("name: ci-test-selection", job)
        self.assertIn("if-no-files-found: error", job)

    def test_every_pull_request_whose_diff_requires_suites_measures_the_selection(self) -> None:
        extras = self.job("extra-tests")
        self.assertIn("run: just coverage-ci-selection\n", extras)
        upload = extras.split("- name: Upload native extra coverage\n", 1)[1].split("- name:", 1)[0]
        self.assertIn("            target/ci-selection/python.lcov\n", upload)

    def test_mode_labels_remain_independent_of_ordinary_prose_exclusion(self) -> None:
        for job, label in (("deloxide", "deloxide"), ("client-conformance", "client-conformance"),
                           ("shuttle", "shuttle"), ("loom", "loom"),
                           ("loom-qualification", "loom"), ("turmoil", "turmoil")):
            with self.subTest(job=job):
                self.assertIn(f"if: contains(github.event.pull_request.labels.*.name, '{label}')", self.job(job))

    def test_validation_only_rerun_keeps_source_selection_and_required_cancellation_fails(self) -> None:
        gate = test_bolero_coverage.WorkflowTests()
        gate.setUp()
        for documentation_only, labels in ((False, ()), (False, ("bolero-random",)), (True, ())):
            with self.subTest(documentation_only=documentation_only, labels=labels), \
                    tempfile.TemporaryDirectory() as directory:
                current = selection.TestSelection(
                    event="pull_request", action="opened", tested_sha="a" * 40, event_base_sha="b" * 40,
                    tested_base_sha="b" * 40, head_sha="c" * 40,
                    paths=("README.md",) if documentation_only else ("src/runtime.rs",), labels=labels,
                )
                # Re-evaluating unrelated always() jobs must not call a source-excluded workflow.
                self.assertIn("needs: [test-selection]", self.job("bolero"))
                self.assertIn("if: needs.test-selection.outputs.bolero-required == 'true'", self.job("bolero"))
                if current.bolero_required:
                    # A cancelled randomized job is neither the requested success nor a deliberate skip.
                    requested = "bolero-random" in current.labels
                    result = subprocess.run(["bash", "-e", "-c", gate.script], cwd=directory, env={
                        **os.environ, "RANDOM_RESULT": "cancelled", "RANDOM_REQUIRED": str(requested).lower(),
                        "FUZZ_RESULT": "skipped", "COMBINED_RESULT": "skipped", "FUZZ_REQUIRED": "false",
                    }, capture_output=True)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertFalse((Path(directory) / "selection.json").exists())
                else:
                    rerun = dataclasses.replace(current)
                    self.assertFalse(rerun.bolero_required)


if __name__ == "__main__":
    unittest.main()
