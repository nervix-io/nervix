"""Exercise patch line accounting and advisory PR publication through real Git diffs."""

from __future__ import annotations

import io
import json
import subprocess
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock

from scripts import patch_coverage


class PatchCoverageTests(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.git("init", "--quiet")
        (self.root / ".git" / "info" / "exclude").write_text("*.lcov\n")
        self.git("config", "user.name", "Coverage Test")
        self.git("config", "user.email", "coverage@example.com")
        self.source = self.root / "src" / "sample.py"
        self.source.parent.mkdir()
        self.source.write_text("def answer():\n    return 1\n")
        self.commit()
        self.base = self.git("rev-parse", "HEAD").strip()

    def git(self, *arguments: str) -> str:
        return subprocess.run(
            ["git", *arguments], cwd=self.root, check=True, capture_output=True, text=True
        ).stdout

    def commit(self) -> None:
        self.git("add", ".")
        self.git("commit", "--quiet", "-m", "test: record source")

    def report(self, name: str, source: str, lines: dict[int, int]) -> Path:
        report = self.root / name
        entries = "".join(f"DA:{line},{hits}\n" for line, hits in lines.items())
        report.write_text(f"TN:\nSF:{source}\n{entries}end_of_record\n")
        return report

    def measure(self, *reports: Path, source_root: Path | None = None):
        patch = patch_coverage.Patch.load(self.root, self.base, "HEAD")
        roots = [source_root] if source_root is not None else None
        return patch_coverage.Coverage.load(self.root, reports, roots).measure(patch)

    def test_only_added_executable_lines_count_and_repeated_reports_union_hits(self) -> None:
        self.source.write_text("def answer():\n    result = 2\n\n    return result\n")
        self.commit()
        unit = self.report("unit.lcov", str(self.source), {1: 3, 2: 1, 4: 0})
        scenario = self.report("scenario.lcov", "src/sample.py", {1: 2, 2: 0, 4: 2})
        measured = self.measure(unit, scenario)
        self.assertEqual(measured.executable, 2)
        self.assertEqual(measured.covered, 2)
        self.assertEqual(measured.project_executable, 3)
        self.assertEqual(measured.project_covered, 3)
        self.assertEqual(measured.files[0].uncovered, ())
        markdown = measured.markdown()
        self.assertIn("100.00%", markdown)
        self.assertIn("2 / 2", markdown)

    def test_uncovered_lines_and_unmeasured_files_are_explicit(self) -> None:
        self.source.write_text("def answer():\n    result = 2\n    return result\n")
        (self.root / "README.md").write_text("# Documentation\n")
        self.commit()
        report = self.report("coverage.lcov", "src/sample.py", {1: 2, 2: 0, 3: 1})
        measured = self.measure(report)
        self.assertEqual((measured.covered, measured.executable), (1, 2))
        by_path = {item.path: item for item in measured.files}
        self.assertEqual(by_path["src/sample.py"].uncovered, (2,))
        self.assertIsNone(by_path["README.md"].executable)
        markdown = measured.markdown()
        self.assertIn("50.00%", markdown)
        self.assertIn("Unavailable", markdown)
        self.assertIn("Advisory", markdown)
        self.assertIn(measured.patch.head, markdown)

    def test_a_pure_rename_adds_no_lines_and_renamed_edits_use_the_destination(self) -> None:
        destination = self.source.with_name('renamed "雪".py')
        self.source.rename(destination)
        self.commit()
        report = self.report("coverage.lcov", str(destination), {1: 1, 2: 1})
        measured = self.measure(report)
        self.assertEqual(measured.files, ())
        self.assertEqual(measured.executable, 0)
        self.assertIn("No added executable lines", measured.markdown())
        destination.write_text("def answer():\n    return 2\n")
        self.commit()
        measured = self.measure(report)
        self.assertEqual(measured.files[0].path, 'src/renamed "雪".py')
        self.assertEqual((measured.covered, measured.executable), (1, 1))

    def test_hunks_are_not_confused_by_source_that_looks_like_a_file_header(self) -> None:
        self.source.write_text("def answer():\n++ b/text\n    return 2\n")
        self.commit()
        report = self.report("coverage.lcov", "src/sample.py", {1: 1, 2: 0, 3: 1})
        measured = self.measure(report)
        self.assertEqual(measured.files[0].path, "src/sample.py")
        self.assertEqual((measured.covered, measured.executable), (1, 2))

    def test_non_utf8_corpus_content_preserves_paths_and_source_line_coverage(self) -> None:
        corpus = self.root / "corpus" / "雪"
        corpus.parent.mkdir()
        corpus.write_bytes(b"\xff\xfe\n")
        self.source.write_text("def answer():\n    return 2\n")
        self.commit()
        report = self.report("coverage.lcov", "src/sample.py", {1: 1, 2: 1})
        measured = self.measure(report)
        self.assertEqual(measured.patch.lines["corpus/雪"], {1})
        self.assertEqual((measured.covered, measured.executable), (1, 1))
        by_path = {item.path: item for item in measured.files}
        self.assertIsNone(by_path["corpus/雪"].executable)
        self.assertEqual(by_path["src/sample.py"].covered, 1)

    def test_deleted_lines_and_changes_on_the_base_branch_do_not_enter_the_patch(self) -> None:
        self.git("checkout", "--quiet", "-b", "patch")
        self.source.write_text("def answer():\n    return 2\n")
        self.commit()
        self.git("checkout", "--quiet", "-b", "base", self.base)
        (self.root / "unrelated.py").write_text("answer = 42\n")
        self.commit()
        comparison = self.git("rev-parse", "HEAD").strip()
        self.git("checkout", "--quiet", "patch")
        report = self.report("coverage.lcov", "src/sample.py", {1: 1, 2: 1})
        patch = patch_coverage.Patch.load(self.root, comparison, "HEAD")
        self.assertEqual(patch.base, self.base)
        measured = patch_coverage.Coverage.load(self.root, [report]).measure(patch)
        self.assertEqual([item.path for item in measured.files], ["src/sample.py"])
        self.assertEqual(measured.executable, 1)
        self.source.write_text("def answer():\n")
        self.commit()
        self.assertEqual(self.measure(report).executable, 0)

    def test_source_root_remapping_is_explicit_and_external_files_do_not_count(self) -> None:
        self.source.write_text("def answer():\n    return 2\n")
        self.commit()
        report = self.report("coverage.lcov", "/ci/workspace/src/sample.py", {1: 1, 2: 1})
        external = self.report("external.lcov", "/dependency/src/sample.py", {1: 0, 2: 0})
        measured = self.measure(report, external, source_root=Path("/ci/workspace"))
        self.assertEqual((measured.covered, measured.executable), (1, 1))
        self.assertEqual(measured.project_executable, 2)
        self.assertIsNone(self.measure(report).files[0].executable)

    def test_path_modules_measure_the_tracked_file_their_dot_dot_segments_name(self) -> None:
        restore = self.root / "tests" / "restore.py"
        restore.parent.mkdir()
        restore.write_text("def restore():\n    return 1\n")
        self.commit()
        report = self.report("coverage.lcov", "/ci/workspace/src/../tests/restore.py", {1: 1, 2: 0})
        measured = self.measure(report, source_root=Path("/ci/workspace"))
        self.assertEqual(
            [(item.path, item.covered, item.executable) for item in measured.files],
            [("tests/restore.py", 1, 2)],
        )

    def test_artifact_source_roots_union_reports_from_distinct_ci_workspaces(self) -> None:
        self.source.write_text("def answer():\n    result = 2\n    return result\n")
        self.commit()
        unit = self.report("unit.lcov", "/unit/_work/nervix/nervix/src/sample.py", {1: 1, 2: 1, 3: 0})
        scenario = self.report("scenario.lcov", "/scenario/work/nervix/nervix/src/sample.py", {1: 1, 2: 0, 3: 1})
        unit_root = self.root / "unit-source-root.txt"
        unit_root.write_text("/unit/_work/nervix/nervix\n")
        scenario_root = self.root / "scenario-source-root.txt"
        scenario_root.write_text("/scenario/work/nervix/nervix\n")
        output = self.root / "target" / "patch.md"
        with redirect_stdout(io.StringIO()):
            status = patch_coverage.main(
                ["--base", self.base, "--report", str(unit), "--report", str(scenario),
                 "--source-root-file", str(unit_root), "--source-root-file", str(scenario_root),
                 "--output", str(output)],
                root=self.root,
            )
        self.assertEqual(status, 0)
        self.assertIn("100.00%", output.read_text())
        self.assertIn("2 / 2", output.read_text())

    def test_missing_and_malformed_reports_leave_the_command_successful_with_diagnostics(self) -> None:
        malformed = self.root / "broken.lcov"
        malformed.write_text("SF:src/sample.py\nDA:2,no-hits\nend_of_record\n")
        for report in (self.root / "missing.lcov", malformed):
            with self.subTest(report=report), redirect_stderr(io.StringIO()) as errors:
                status = patch_coverage.main(
                    ["--base", self.base, "--report", str(report)], root=self.root
                )
                self.assertEqual(status, 0)
                self.assertIn("patch coverage:", errors.getvalue())

    def test_an_unreadable_checkout_is_advisory(self) -> None:
        failure = patch_coverage.MergeError("git ls-files failed: not a repository")
        with mock.patch.object(patch_coverage.Checkout, "load", side_effect=failure):
            with redirect_stderr(io.StringIO()) as errors:
                status = patch_coverage.main(["--base", self.base], root=self.root)
        self.assertEqual(status, 0)
        self.assertIn("patch coverage: git ls-files failed", errors.getvalue())

    def test_invalid_source_root_metadata_is_advisory_and_never_supplies_a_default(self) -> None:
        metadata = self.root / "source-root.txt"
        for value in ("", "relative/workspace"):
            metadata.write_text(value)
            with self.subTest(value=value), redirect_stderr(io.StringIO()) as errors:
                status = patch_coverage.main(
                    ["--base", self.base, "--source-root-file", str(metadata)], root=self.root
                )
                self.assertEqual(status, 0)
                self.assertIn("must contain an absolute coverage source root", errors.getvalue())

    def test_zero_coverage_succeeds_and_retains_a_markdown_report_and_step_summary(self) -> None:
        self.source.write_text("def answer():\n    return 2\n")
        self.commit()
        report = self.report("coverage.lcov", "src/sample.py", {1: 0, 2: 0})
        output = self.root / "target" / "patch.md"
        summary = self.root / "summary.md"
        with mock.patch.dict("os.environ", {"GITHUB_STEP_SUMMARY": str(summary)}):
            with redirect_stdout(io.StringIO()):
                status = patch_coverage.main(
                    ["--base", self.base, "--report", str(report), "--output", str(output)],
                    root=self.root,
                )
        self.assertEqual(status, 0)
        self.assertIn("0.00%", output.read_text())
        self.assertEqual(summary.read_text(), output.read_text())

    def test_a_file_the_patch_only_shortens_is_changed_without_added_lines(self) -> None:
        shortened = self.root / "src" / "shortened.py"
        shortened.write_text("def first():\n    return 1\n\ndef second():\n    return 2\n")
        self.commit()
        base = self.git("rev-parse", "HEAD").strip()
        shortened.write_text("def first():\n    return 1\n")
        self.source.write_text("def answer():\n    return 2\n")
        (self.root / "src" / "added.py").write_text("value = 1\n")
        self.commit()
        patch = patch_coverage.Patch.load(self.root, base, "HEAD")
        self.assertEqual(sorted(patch.lines), ["src/added.py", "src/sample.py"])
        self.assertEqual(patch.removals, frozenset({"src/shortened.py"}))
        self.assertEqual(patch.files, frozenset({"src/added.py", "src/sample.py", "src/shortened.py"}))
        # A deleted file is not held by the tested commit, so the patch changes nothing there.
        shortened.unlink()
        self.commit()
        self.assertNotIn("src/shortened.py", patch_coverage.Patch.load(self.root, base, "HEAD").files)

    def test_the_crap_section_for_the_changed_files_follows_the_patch_section(self) -> None:
        self.source.write_text("def answer():\n    return 2\n")
        self.commit()
        report = self.report("coverage.lcov", "src/sample.py", {1: 1, 2: 1})
        crap = self.root / "target" / "crap.json"
        crap.parent.mkdir()
        crap.write_text(json.dumps({
            "verdict": "complete", "report": "lcov-workspace.info", "revision": "r", "analyzer": "cargo-crap 0.6.1",
            "threshold": 30,
            "scope": {"report_files": 2, "report_rust_files": 2, "matched_files": 2, "unscored_files": [], "unmeasured_files": []},
            "totals": {"measured": 2, "above_threshold": 2, "unmeasured": 0},
            "functions": [
                {"file": "src/sample.py", "function": "answer", "line": 1, "complexity": 31.0, "coverage": 0.0, "crap": 992.0},
                {"file": "src/unchanged.rs", "function": "elsewhere", "line": 1, "complexity": 40.0, "coverage": 0.0, "crap": 1640.0},
            ],
            "unmeasured": [],
        }))
        output = self.root / "target" / "patch.md"
        with redirect_stdout(io.StringIO()):
            status = patch_coverage.main(
                ["--base", self.base, "--report", str(report), "--output", str(output), "--crap", "target/crap.json"],
                root=self.root,
            )
        body = output.read_text()
        self.assertEqual(status, 0)
        self.assertLess(body.index("## Patch coverage"), body.index("## CRAP"))
        self.assertIn("| <code>answer</code> | <code>src/sample.py:1</code> | 992.0 | 31 | 0.0% |", body)
        self.assertNotIn("elsewhere", body)
        self.assertIn("Project: 2 of 2 measured functions above 30.", body)

    def test_a_missing_crap_report_is_stated_and_the_comment_is_still_written(self) -> None:
        self.source.write_text("def answer():\n    return 2\n")
        self.commit()
        report = self.report("coverage.lcov", "src/sample.py", {1: 1, 2: 1})
        output = self.root / "target" / "patch.md"
        with redirect_stdout(io.StringIO()):
            status = patch_coverage.main(
                ["--base", self.base, "--report", str(report), "--output", str(output), "--crap", "target/missing.json"],
                root=self.root,
            )
        self.assertEqual(status, 0)
        self.assertIn("100.00%", output.read_text())
        self.assertIn("**The CRAP report is unavailable**", output.read_text())


class PublicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.publisher = patch_coverage.Publication("owner/repository", 12, "current-head")
        self.pr = {"state": "open", "head": {"sha": "current-head"}}

    def test_scoped_publications_update_only_their_own_current_head_comments(self) -> None:
        scopes = [patch_coverage.Scope.ORDINARY, patch_coverage.Scope.TURMOIL]
        comments = [[
            {"id": index, "body": scope.marker(), "user": {"login": "github-actions[bot]"}}
            for index, scope in enumerate(scopes, start=1)
        ]]
        measured = patch_coverage.Coverage({}).measure(patch_coverage.Patch("base", "head", {}, frozenset()))
        for index, scope in enumerate(scopes, start=1):
            with self.subTest(scope=scope):
                body = measured.markdown(scope=scope)
                self.assertTrue(body.startswith(scope.marker()))
                self.assertIn(scope.title(), body)
                publisher = patch_coverage.Publication("owner/repository", 12, "current-head", scope)
                with mock.patch.dict("os.environ", {"GITHUB_ACTIONS": "true"}):
                    with mock.patch.object(patch_coverage, "command", side_effect=[json.dumps(self.pr), json.dumps(comments), "{}"]) as commands:
                        publisher.publish(body)
                self.assertIn(f"repos/owner/repository/issues/comments/{index}", commands.call_args.args[0])
                self.assertEqual(json.loads(commands.call_args.kwargs["input_text"]), {"body": body})

    def test_scoped_cli_retains_the_report_and_publishes_with_the_pr_head_fence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "turmoil.md"
            patch = patch_coverage.Patch("base", "tested-merge", {}, frozenset())
            with mock.patch.object(patch_coverage.Patch, "load", return_value=patch):
                with mock.patch.object(patch_coverage.Coverage, "load", return_value=patch_coverage.Coverage({})):
                    with mock.patch.object(patch_coverage.Publication, "publish") as publish, redirect_stdout(io.StringIO()):
                        status = patch_coverage.main(
                            ["--scope", "turmoil", "--repo", "owner/repository", "--pr", "12",
                             "--pr-head", "current-head", "--output", str(output)], root=root,
                        )
            self.assertEqual(status, 0)
            body = output.read_text()
            self.assertTrue(body.startswith(patch_coverage.Scope.TURMOIL.marker()))
            self.assertIn("Turmoil patch coverage", body)
            publish.assert_called_once_with(body)

    def test_new_comment_is_created_with_the_exact_markdown_body(self) -> None:
        body = patch_coverage.COMMENT_MARKER + "\n## Patch coverage\n"
        with mock.patch.dict("os.environ", {"GITHUB_ACTIONS": "true"}):
            with mock.patch.object(patch_coverage, "command", side_effect=[json.dumps(self.pr), "[[]]", "{}"]) as commands:
                self.publisher.publish(body)
        arguments = commands.call_args.args[0]
        self.assertIn("POST", arguments)
        self.assertIn("repos/owner/repository/issues/12/comments", arguments)
        self.assertEqual(json.loads(commands.call_args.kwargs["input_text"]), {"body": body})

    def test_paginated_comments_update_the_publishers_marked_comment(self) -> None:
        comments = [
            [{"id": 1, "body": "Other discussion", "user": {"login": "reviewer"}}],
            [{"id": 2, "body": patch_coverage.COMMENT_MARKER, "user": {"login": "github-actions[bot]"}}],
        ]
        with mock.patch.dict("os.environ", {"GITHUB_ACTIONS": "true"}):
            with mock.patch.object(patch_coverage, "command", side_effect=[json.dumps(self.pr), json.dumps(comments), "{}"]) as commands:
                self.publisher.publish("updated report")
        arguments = commands.call_args.args[0]
        self.assertIn("PATCH", arguments)
        self.assertIn("repos/owner/repository/issues/comments/2", arguments)

    def test_manual_publication_resolves_the_authenticated_author(self) -> None:
        comments = [[{"id": 3, "body": patch_coverage.COMMENT_MARKER, "user": {"login": "publisher"}}]]
        with mock.patch.dict("os.environ", {"GITHUB_ACTIONS": "false"}):
            with mock.patch.object(patch_coverage, "command", side_effect=[json.dumps(self.pr), '{"login":"publisher"}', json.dumps(comments), "{}"]) as commands:
                self.publisher.publish("manual report")
        self.assertIn("repos/owner/repository/issues/comments/3", commands.call_args.args[0])

    def test_a_stale_run_or_closed_pull_request_does_not_publish(self) -> None:
        for pr in ({"state": "open", "head": {"sha": "newer-head"}}, {"state": "closed", "head": {"sha": "current-head"}}):
            with self.subTest(pr=pr), redirect_stderr(io.StringIO()):
                with mock.patch.object(patch_coverage, "command", return_value=json.dumps(pr)) as commands:
                    self.publisher.publish("report")
                    self.assertEqual(commands.call_count, 1)

    def test_publication_errors_are_advisory_and_the_local_report_survives(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "report.md"
            patch = patch_coverage.Patch("base", "head", {}, frozenset())
            coverage = patch_coverage.Coverage({})
            with mock.patch.object(patch_coverage.Patch, "load", return_value=patch):
                with mock.patch.object(patch_coverage.Coverage, "load", return_value=coverage):
                    with mock.patch.object(patch_coverage.Publication, "publish", side_effect=RuntimeError("403: read-only token")):
                        with redirect_stderr(io.StringIO()) as errors, redirect_stdout(io.StringIO()):
                            status = patch_coverage.main(
                                ["--repo", "owner/repository", "--pr", "12", "--output", str(output)],
                                root=root,
                            )
            self.assertEqual(status, 0)
            self.assertIn("read-only token", errors.getvalue())
            self.assertIn("Advisory", output.read_text())

    def test_repository_discovery_failure_also_retains_the_local_report(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "report.md"
            patch = patch_coverage.Patch("base", "head", {}, frozenset())
            with mock.patch.dict("os.environ", {"GITHUB_REPOSITORY": ""}):
                with mock.patch.object(patch_coverage.Patch, "load", return_value=patch):
                    with mock.patch.object(patch_coverage.Coverage, "load", return_value=patch_coverage.Coverage({})):
                        with mock.patch.object(patch_coverage, "command", side_effect=OSError("gh unavailable")):
                            with redirect_stderr(io.StringIO()), redirect_stdout(io.StringIO()):
                                status = patch_coverage.main(["--pr", "12", "--output", str(output)], root=root)
            self.assertEqual(status, 0)
            self.assertIn("Advisory", output.read_text())


class WorkflowTests(unittest.TestCase):
    def test_ci_runs_the_advisory_command_with_the_pr_head_and_retains_its_report(self) -> None:
        from scripts.tests.test_native_coverage import job_section, step_section

        workflow = (patch_coverage.ROOT / ".github/workflows/check.yaml").read_text()
        coverage = job_section(workflow, "coverage")
        report = step_section(coverage, "Report patch line coverage")
        self.assertIn("continue-on-error: true", report)
        self.assertIn("if: always()", report)
        self.assertIn('just coverage-patch "$BASE_SHA" lcov-workspace.info', report)
        self.assertIn("--crap target/crap-report/crap.json", report)
        self.assertIn('--pr-head "$PR_HEAD_SHA"', report)
        self.assertIn("pull-requests: write", coverage)
        self.assertIn("fetch-depth: 0", coverage)
        self.assertIn("target/patch-coverage.md", coverage)
        # The merged report is repository-relative, so it needs no producer's checkout root.
        self.assertNotIn("--source-root-file", report)
        extras = job_section(workflow, "extra-tests")
        self.assertIn("just coverage-patch-runner", step_section(extras, "Patch coverage reporter"))
        self.assertIn("target/patch-coverage/python.lcov", extras)


if __name__ == "__main__":
    unittest.main()
