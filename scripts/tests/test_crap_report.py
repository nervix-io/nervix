"""The advisory CRAP report over fixture repositories, with the pinned analyzer, and its CI contract."""

from __future__ import annotations

import io
import json
import subprocess
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from scripts import cargo_crap, crap_report
from scripts.tests.test_native_coverage import job_section, step_section
from scripts.tests.test_ratchet import branching

ROOT = Path(__file__).resolve().parents[2]
RECORD = Path("target/coverage-merge/record.json")


def git(root: Path, *arguments: str) -> str:
    completed = subprocess.run(["git", *arguments], cwd=root, capture_output=True, text=True, check=True)
    return completed.stdout


def write_repository(root: Path, sources: dict[str, str]) -> str:
    """Commit the fixture's sources, record a complete merge at that revision, and return it."""

    (root / "Cargo.toml").write_text('[package]\nname="crap-fixture"\nversion="0.1.0"\n', encoding="utf-8")
    (root / ".gitignore").write_text("/target/\n/lcov-workspace.info\n", encoding="utf-8")
    for path, body in sources.items():
        target = root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(body, encoding="utf-8")
    git(root, "init", "-q", "-b", "main")
    git(root, "add", "-A")
    git(root, "-c", "user.name=CRAP Test", "-c", "user.email=crap@example.com", "commit", "-q", "-m", "fixture")
    revision = git(root, "rev-parse", "HEAD").strip()
    write_record(root, {"verdict": "complete", "revision": revision})
    return revision


def write_record(root: Path, content: object) -> None:
    record = root / RECORD
    record.parent.mkdir(parents=True, exist_ok=True)
    record.write_text(json.dumps(content), encoding="utf-8")


def write_report(root: Path, records: dict[str, dict[int, int]]) -> Path:
    """A report holding one record of line hits for each source, as the merge writes it."""

    report = root / "lcov-workspace.info"
    content = []
    for source, lines in records.items():
        content.append(f"SF:{source}\n")
        for line, hits in sorted(lines.items()):
            content.append(f"DA:{line},{hits}\n")
        content.append("end_of_record\n")
    report.write_text("".join(content), encoding="utf-8")
    return report


def covering(body: str, hits: int) -> dict[int, int]:
    """Line hits for every line of a source."""
    return {line: hits for line in range(1, body.count("\n") + 1)}


def check(root: Path, *arguments: str) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        status = crap_report.main(list(arguments), root=root)
    return status, out.getvalue(), err.getvalue()


def written(root: Path) -> dict[str, object]:
    return json.loads((root / crap_report.OUTPUT / crap_report.JSON_REPORT).read_text(encoding="utf-8"))


# A function that scores 31 + 31² = 992 uncovered, and one that scores 1 covered.
COMPLEX = branching("complex", 31)
SIMPLE = "fn simple() -> u32 {\n    1\n}\n"
ONLY_TESTS = "#[test]\nfn passes() {\n    assert_eq!(1, 1);\n}\n"


class CompleteAnalysisTests(unittest.TestCase):
    """The pinned analyzer scores the fixture, so these qualify the analysis and its report."""

    def test_matched_files_are_scored_and_unmeasured_ones_are_listed_but_never_scored(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            revision = write_repository(root, {
                "src/lib.rs": COMPLEX,
                "src/simple.rs": SIMPLE,
                "src/unmeasured.rs": branching("unmeasured", 40),
                "tests/only_tests.rs": ONLY_TESTS,
                "scripts/tool.py": "print(1)\n",
            })
            write_report(root, {
                "src/lib.rs": covering(COMPLEX, 0),
                "src/simple.rs": covering(SIMPLE, 1),
                "tests/only_tests.rs": covering(ONLY_TESTS, 1),
                "scripts/tool.py": {1: 1},
            })

            status, output, _ = check(root)
            report = written(root)

        self.assertEqual(status, 0)
        self.assertEqual(report["verdict"], "complete")
        self.assertEqual(report["revision"], revision)
        self.assertEqual(report["scope"], {
            "report_files": 4,
            "report_rust_files": 3,
            "matched_files": 2,
            "unscored_files": ["tests/only_tests.rs"],
            "unmeasured_files": ["src/unmeasured.rs"],
        })
        self.assertEqual(report["totals"], {"measured": 2, "above_threshold": 1, "unmeasured": 1})
        self.assertEqual(report["functions"], [
            {"file": "src/lib.rs", "function": "complex", "line": 1, "complexity": 31.0, "coverage": 0.0, "crap": 992.0},
            {"file": "src/simple.rs", "function": "simple", "line": 1, "complexity": 1.0, "coverage": 100.0, "crap": 1.0},
        ])
        # Complexity 40 would score 1640 uncovered; without a record it is listed and not scored.
        self.assertEqual(report["unmeasured"], [{"file": "src/unmeasured.rs", "function": "unmeasured", "line": 1, "complexity": 40.0}])
        self.assertIn("Project: 1 of 2 measured functions above 30.", output)

    def test_a_file_without_a_record_never_takes_the_coverage_of_a_file_of_the_same_name(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(root, {"src/lib.rs": COMPLEX, "crates/other/src/lib.rs": branching("other", 31)})
            # The analyzer binds a relative `src/lib.rs` to every file whose path ends with it, so
            # the other crate's file would have been scored as fully covered.
            write_report(root, {"src/lib.rs": covering(COMPLEX, 1)})

            status, _, _ = check(root)
            report = written(root)

        self.assertEqual(status, 0)
        self.assertEqual([entry["file"] for entry in report["functions"]], ["src/lib.rs"])
        self.assertEqual(report["unmeasured"], [{"file": "crates/other/src/lib.rs", "function": "other", "line": 1, "complexity": 31.0}])

    def test_the_markdown_report_lists_functions_above_30_unmeasured_and_unscored_files(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(root, {
                "src/lib.rs": COMPLEX, "src/unmeasured.rs": SIMPLE, "tests/only_tests.rs": ONLY_TESTS,
            })
            write_report(root, {"src/lib.rs": covering(COMPLEX, 0), "tests/only_tests.rs": covering(ONLY_TESTS, 1)})

            check(root)
            markdown = (root / crap_report.OUTPUT / crap_report.MARKDOWN_REPORT).read_text(encoding="utf-8")

        self.assertIn("**1 of 1 measured functions above 30.**", markdown)
        self.assertIn("| <code>complex</code> | <code>src/lib.rs:1</code> | 992.0 | 31 | 0.0% |", markdown)
        self.assertIn("| <code>src/unmeasured.rs</code> | 1 | 1 |", markdown)
        self.assertIn("- <code>tests/only_tests.rs</code>", markdown)
        self.assertIn("no CRAP score fails the build", markdown)

    def test_a_second_run_over_the_same_report_writes_the_same_reports(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(root, {"src/lib.rs": COMPLEX + SIMPLE, "src/unmeasured.rs": SIMPLE})
            write_report(root, {"src/lib.rs": covering(COMPLEX + SIMPLE, 1)})
            reports = []
            for _ in range(2):
                self.assertEqual(check(root)[0], 0)
                output = root / crap_report.OUTPUT
                reports.append(((output / crap_report.JSON_REPORT).read_bytes(), (output / crap_report.MARKDOWN_REPORT).read_bytes()))

        self.assertEqual(reports[0], reports[1])

    def test_a_score_above_30_never_fails_the_check(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(root, {"src/lib.rs": branching("worst", 60)})
            write_report(root, {"src/lib.rs": covering(branching("worst", 60), 0)})

            status, _, _ = check(root)
            report = written(root)

        self.assertEqual(status, 0)
        self.assertEqual(report["totals"]["above_threshold"], 1)


class VacuousAnalysisTests(unittest.TestCase):
    """Every way the analysis can fail to describe the report fails the check and says why."""

    def vacuous(self, root: Path, *arguments: str) -> tuple[dict[str, object], str]:
        status, _, error = check(root, *arguments)
        self.assertEqual(status, 1)
        report = written(root)
        self.assertEqual(report["verdict"], "vacuous")
        markdown = (root / crap_report.OUTPUT / crap_report.MARKDOWN_REPORT).read_text(encoding="utf-8")
        self.assertIn("**The analysis is vacuous.**", markdown)
        return report, error

    def test_a_report_whose_paths_come_from_another_checkout_is_vacuous(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "checkout"
            root.mkdir()
            write_repository(root, {"src/lib.rs": COMPLEX})
            other = Path(directory) / "runner/_work/nervix/nervix"
            write_report(root, {f"{other}/src/lib.rs": covering(COMPLEX, 1)})

            report, error = self.vacuous(root)

        self.assertEqual(report["mismatch"], {"kind": "untracked-sources", "files": [f"{other}/src/lib.rs"]})
        self.assertIn("not the merged report of this checkout", error)
        self.assertIn(f"{other}/src/lib.rs", error)

    def test_a_failure_names_the_first_files_and_counts_the_rest(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(root, {"src/lib.rs": COMPLEX})
            names = [f"src/missing_{index:02}.rs" for index in range(crap_report.NAMED_LIMIT + 2)]
            write_report(root, {name: {1: 1} for name in names})

            report, error = self.vacuous(root)

        self.assertEqual(report["mismatch"]["files"], names)
        self.assertIn(", ".join(names[: crap_report.NAMED_LIMIT]) + " and 2 more", error)

    def test_a_report_merged_at_another_revision_is_vacuous(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(root, {"src/lib.rs": COMPLEX})
            write_report(root, {"src/lib.rs": covering(COMPLEX, 1)})
            write_record(root, {"verdict": "complete", "revision": "0" * 40})

            report, error = self.vacuous(root)

        self.assertEqual(report["mismatch"]["kind"], "record")
        self.assertIn(f"merged at revision {'0' * 40}", error)

    def test_a_missing_failed_or_unreadable_merge_record_is_vacuous(self) -> None:
        cases = [
            ("does not exist", None),
            ("records a failed merge", {"verdict": "failed", "revision": "r"}),
            ("is not a merge record", "[]"),
            ("is not a merge record: Expecting", "{"),
        ]
        for expected, content in cases:
            with self.subTest(expected), TemporaryDirectory() as directory:
                root = Path(directory)
                write_repository(root, {"src/lib.rs": COMPLEX})
                write_report(root, {"src/lib.rs": covering(COMPLEX, 1)})
                if content is None:
                    (root / RECORD).unlink()
                elif isinstance(content, str):
                    (root / RECORD).write_text(content, encoding="utf-8")
                else:
                    write_record(root, content)

                report, error = self.vacuous(root)

            self.assertEqual(report["mismatch"]["kind"], "record")
            self.assertIn(expected, error)

    def test_a_report_file_the_walk_cannot_read_is_vacuous(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "src").mkdir()
            (root / "src/linked.rs").symlink_to("lib.rs")
            write_repository(root, {"src/lib.rs": COMPLEX})
            write_report(root, {"src/lib.rs": covering(COMPLEX, 1), "src/linked.rs": covering(COMPLEX, 1)})

            report, _ = self.vacuous(root)

        self.assertEqual(report["mismatch"], {"kind": "unreadable-files", "files": ["src/linked.rs"]})

    def test_a_measured_file_whose_record_the_analyzer_did_not_bind_is_vacuous(self) -> None:
        unbound = cargo_crap.Function("src/lib.rs", "complex", 1, 31.0)
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(root, {"src/lib.rs": COMPLEX})
            write_report(root, {"src/lib.rs": covering(COMPLEX, 1)})
            scores = cargo_crap.Scores(scored=(), unmeasured=(unbound,))
            with mock.patch("scripts.crap_report.cargo_crap.score", return_value=scores):
                report, error = self.vacuous(root)

        self.assertEqual(report["mismatch"], {"kind": "unbound-files", "files": ["src/lib.rs"]})
        self.assertIn("without binding the report's records of them", error)

    def test_a_report_without_a_matched_rust_file_is_vacuous(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(root, {"src/lib.rs": COMPLEX, "scripts/tool.py": "print(1)\n"})
            write_report(root, {"scripts/tool.py": {1: 1}})

            report, error = self.vacuous(root)

        self.assertEqual(report["mismatch"], {"kind": "no-match", "files": []})
        self.assertIn("matched none of the 0 Rust files", error)


class FailedAnalysisTests(unittest.TestCase):
    def test_an_analysis_that_could_not_run_fails_the_check(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(root, {"src/lib.rs": COMPLEX})
            write_report(root, {"src/lib.rs": covering(COMPLEX, 1)})
            with mock.patch("scripts.cargo_crap.executable", return_value=root / "missing/cargo-crap"):
                status, _, error = check(root)
            report = written(root)

        self.assertEqual(status, 1)
        self.assertEqual(report["verdict"], "failed")
        self.assertIn("cargo-crap 0.6.1 is not installed", error)

    def test_a_malformed_report_fails_the_check(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(root, {"src/lib.rs": COMPLEX})
            (root / "lcov-workspace.info").write_text("SF:src/lib.rs\nDA:1,1\n", encoding="utf-8")

            status, _, error = check(root)

        self.assertEqual(status, 1)
        self.assertIn("is incomplete", error)


def document(**overrides: object) -> crap_report.Document:
    """A complete written report with two functions above 30 in two files and one unmeasured."""

    content: dict[str, object] = {
        "verdict": "complete", "report": "lcov-workspace.info", "revision": "abc", "analyzer": "cargo-crap 0.6.1",
        "threshold": 30,
        "scope": {"report_files": 3, "report_rust_files": 3, "matched_files": 3, "unscored_files": [], "unmeasured_files": ["src/mode.rs"]},
        "totals": {"measured": 3, "above_threshold": 2, "unmeasured": 1},
        "functions": [
            {"file": "src/a.rs", "function": "Owner::worse", "line": 7, "complexity": 40.0, "coverage": 10.0, "crap": 1206.4},
            {"file": "src/a.rs", "function": "fine", "line": 30, "complexity": 2.0, "coverage": 100.0, "crap": 2.0},
            {"file": "src/b.rs", "function": "bad", "line": 3, "complexity": 31.0, "coverage": 0.0, "crap": 992.0},
        ],
        "unmeasured": [{"file": "src/mode.rs", "function": "modeled", "line": 1, "complexity": 3.0}],
    }
    content.update(overrides)
    return crap_report.Document(content)


class SectionTests(unittest.TestCase):
    def test_the_section_lists_functions_above_30_of_the_changed_files_only(self) -> None:
        section = document().section({"src/a.rs", "src/mode.rs", "README.md"}, "nervix-io/nervix", "head")

        self.assertIn("**1 function above 30 in the files this pull request changes.**", section)
        self.assertIn(
            "| <code>Owner::worse</code> | [<code>src/a.rs:7</code>](https://github.com/nervix-io/nervix/blob/head/src/a.rs#L7)"
            " | 1206.4 | 40 | 10.0% |",
            section,
        )
        self.assertNotIn("src/b.rs", section)
        self.assertNotIn("<code>fine</code>", section)
        self.assertIn("Unmeasured in the changed files, with no ordinary coverage record and never scored: 1 function.", section)
        self.assertIn(
            "Project: 2 of 3 measured functions above 30. Unmeasured, with no ordinary coverage record and never scored: "
            "1 function in 1 file.",
            section,
        )
        self.assertIn("no CRAP score fails the build", section)

    def test_changes_without_a_function_above_30_say_so(self) -> None:
        section = document().section({"docs/src/index.md"}, None, "head")

        self.assertIn("**No function above 30 in the files this pull request changes.**", section)
        self.assertNotIn("| Function |", section)
        self.assertNotIn("Unmeasured in the changed files", section)

    def test_the_section_shows_its_limit_and_points_to_the_full_report(self) -> None:
        functions = [
            {"file": "src/a.rs", "function": f"f{index}", "line": index, "complexity": 31.0, "coverage": 0.0, "crap": 992.0}
            for index in range(1, crap_report.SECTION_LIMIT + 3)
        ]
        section = document(functions=functions).section({"src/a.rs"}, None, "head")

        self.assertEqual(section.count("| <code>f"), crap_report.SECTION_LIMIT)
        self.assertIn(f"Showing {crap_report.SECTION_LIMIT} of {crap_report.SECTION_LIMIT + 2}", section)

    def test_a_vacuous_report_names_its_failure_instead_of_scores(self) -> None:
        section = document(verdict="vacuous", failure="the analysis matched none of the 3 Rust files").section({"src/a.rs"}, None, "head")

        self.assertIn("**The CRAP analysis is vacuous**: the analysis matched none of the 3 Rust files", section)
        self.assertNotIn("| Function |", section)

    def test_a_report_of_an_analysis_that_could_not_run_says_so(self) -> None:
        failed = document(verdict="failed", failure="cargo-crap 0.6.1 is not installed")

        self.assertIn("**The CRAP analysis could not run**: cargo-crap 0.6.1 is not installed", failed.section((), None, "head"))
        self.assertIn("**The analysis could not run.** cargo-crap 0.6.1 is not installed", failed.markdown())

    def test_a_missing_or_unreadable_report_is_stated_in_the_section(self) -> None:
        with TemporaryDirectory() as directory:
            missing = Path(directory) / "crap.json"
            self.assertIn("**The CRAP report is unavailable**", crap_report.pull_request_section(missing, (), None, "head"))
            missing.write_text('{"verdict": "unknown"}', encoding="utf-8")
            self.assertIn("states no verdict", crap_report.pull_request_section(missing, (), None, "head"))

    def test_a_report_without_offenders_unmeasured_or_unscored_files_says_so(self) -> None:
        fine = {"file": "src/a.rs", "function": "fine", "line": 1, "complexity": 2.0, "coverage": 100.0, "crap": 2.0}
        scope = {"report_files": 1, "report_rust_files": 1, "matched_files": 1, "unscored_files": [], "unmeasured_files": []}
        markdown = document(
            functions=[fine], unmeasured=[], scope=scope, totals={"measured": 1, "above_threshold": 0, "unmeasured": 0},
        ).markdown()

        self.assertIn("No measured function scores above 30.", markdown)
        self.assertIn("Every function the analysis measured is in a file the report holds a record of.", markdown)
        self.assertIn("## Report files without a scored function", markdown)
        self.assertTrue(markdown.endswith("None.\n"))

    def test_the_section_is_rendered_from_the_written_report(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "crap.json"
            path.write_text(json.dumps(dict(document().content)), encoding="utf-8")
            section = crap_report.pull_request_section(path, {"src/b.rs"}, None, "head")

        self.assertIn("| <code>bad</code> | <code>src/b.rs:3</code> | 992.0 | 31 | 0.0% |", section)


class ContractTests(unittest.TestCase):
    """The recipe and the coverage job run the report as the documentation states."""

    def recipes(self) -> dict[str, object]:
        dumped = subprocess.run(["just", "--dump", "--dump-format", "json"], cwd=ROOT, capture_output=True, text=True, check=True)
        return json.loads(dumped.stdout)["recipes"]

    def test_check_coverage_runs_the_report_over_the_merged_report_and_its_record(self) -> None:
        recipe = self.recipes()["check-coverage"]
        body = json.dumps(recipe["body"])

        self.assertIn("scripts.crap_report", body)
        self.assertIn("--record", body)
        self.assertIn("crap-report", body)
        self.assertIn("install-cargo-crap", [dependency["recipe"] for dependency in recipe["dependencies"]])
        self.assertEqual([parameter["name"] for parameter in recipe["parameters"]], ["report", "record"])

    def test_the_coverage_job_reports_crap_before_the_comment_that_carries_its_section(self) -> None:
        workflow = (ROOT / ".github/workflows/check.yaml").read_text(encoding="utf-8")
        coverage = job_section(workflow, "coverage")
        report = step_section(coverage, "Report CRAP")
        comment = step_section(coverage, "Report patch line coverage")

        self.assertIn("run: just check-coverage", report)
        # The step carries the verdict: it is neither always run nor allowed to fail.
        self.assertNotIn("continue-on-error", report)
        self.assertLess(coverage.index("- name: Merge coverage"), coverage.index("- name: Report CRAP"))
        self.assertLess(coverage.index("- name: Report CRAP"), coverage.index("- name: Report patch line coverage"))
        self.assertIn("--crap target/crap-report/crap.json", comment)
        upload = step_section(coverage, "Upload merged coverage")
        self.assertIn("target/crap-report/crap.json", upload)
        self.assertIn("target/crap-report/crap.md", upload)

    def test_the_report_has_runner_coverage_in_required_ci(self) -> None:
        workflow = (ROOT / ".github/workflows/check.yaml").read_text(encoding="utf-8")
        extras = job_section(workflow, "extra-tests")

        self.assertIn("just coverage-crap-report-runner", step_section(extras, "CRAP report"))
        self.assertIn("target/crap-report/python.lcov", step_section(extras, "Upload native extra coverage"))


if __name__ == "__main__":
    unittest.main()
