from __future__ import annotations

import io
import json
import subprocess
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from scripts.ratchet import BASELINE_FILE, COUNTS, main, measure_sources, write_baseline


def write_repository(root: Path, sources: dict[str, str]) -> None:
    """Stage the fixture's initial sources before testing tracked and new authored files."""

    (root / "Cargo.toml").write_text('[package]\nname="ratchet-fixture"\nversion="0.1.0"\n')
    for path, body in sources.items():
        target = root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(body, encoding="utf-8")
    subprocess.run(["git", "init", "-q", "-b", "main"], cwd=root, check=True)
    subprocess.run(["git", "add", "-A"], cwd=root, check=True)


def count(root: Path, name: str) -> int:
    return len(measure_sources(root)[name])


# Every measurement also analyzes complexity, which refuses a repository with no function, so a
# fixture whose sources declare none adds this one.
ANCHOR = {"src/anchor.rs": "fn anchor() {}\n"}


def run(*arguments: str) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        status = main(list(arguments))
    return status, out.getvalue(), err.getvalue()


class RatchetGateTests(unittest.TestCase):
    def setUp(self) -> None:
        patch = mock.patch("scripts.ratchet.check_synchronization", return_value=None)
        patch.start()
        self.addCleanup(patch.stop)

    def test_a_deliberate_increase_fails_and_names_the_count(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(root, {"src/lib.rs": "fn width(x: u64) -> u32 { x as u32 }\n"})

            write_baseline(root / BASELINE_FILE, {count.name: len(measure_sources(root).get(count.name, [])) for count in COUNTS})
            status, _, _ = run("--root", str(root), "--update")
            self.assertEqual(status, 0)
            self.assertEqual(json.loads((root / BASELINE_FILE).read_text())["as_casts"], 1)

            (root / "src" / "lib.rs").write_text(
                "fn width(x: u64) -> u32 { x as u32 }\nfn depth(y: u64) -> u16 { y as u16 }\n",
                encoding="utf-8",
            )

            status, report, errors = run("--root", str(root))
            self.assertEqual(status, 1)
            # One stream, so a CI log cannot print the failure above the table it refers to.
            self.assertEqual(errors, "")
            self.assertLess(report.index("as_casts  "), report.index("as_casts: 1 -> 2 (+1)"))
            before = (root / BASELINE_FILE).read_bytes()
            status, report, _ = run("--root", str(root), "--update")
            self.assertEqual(status, 1)
            self.assertIn("debt rose", report)
            self.assertEqual((root / BASELINE_FILE).read_bytes(), before)

    def test_failed_compiler_analysis_cannot_rewrite_the_baseline(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(root, {"src/lib.rs": "fn owner() {}\n"})
            write_baseline(root / BASELINE_FILE, {count.name: 0 for count in COUNTS})
            before = (root / BASELINE_FILE).read_bytes()
            with mock.patch("scripts.ratchet.check_synchronization", side_effect=SystemExit("compiler diagnostics failed")):
                with self.assertRaisesRegex(SystemExit, "compiler diagnostics failed"):
                    run("--root", str(root), "--update")
            self.assertEqual((root / BASELINE_FILE).read_bytes(), before)

    def test_an_unchanged_tree_passes(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(root, {"src/lib.rs": "fn width(x: u64) -> u32 { x as u32 }\n"})

            write_baseline(root / BASELINE_FILE, {count.name: len(measure_sources(root).get(count.name, [])) for count in COUNTS})
            status, report, errors = run("--root", str(root))

            self.assertEqual(status, 0)
            self.assertEqual(errors, "")
            self.assertIn("as_casts", report)

    def test_a_decrease_passes_and_asks_for_the_baseline_to_be_updated(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(root, {"src/lib.rs": "fn width(x: u64) -> u32 { x as u32 }\n"})

            write_baseline(root / BASELINE_FILE, {count.name: len(measure_sources(root).get(count.name, [])) for count in COUNTS})
            (root / "src" / "lib.rs").write_text("fn width(x: u64) -> u32 { 0 }\n", encoding="utf-8")

            status, report, _ = run("--root", str(root))

            self.assertEqual(status, 0)
            self.assertIn("just ratchet --update", report)
            self.assertIn("as_casts: 1 -> 0", report)

    def test_a_baseline_that_does_not_name_every_count_is_rejected(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(root, {"src/lib.rs": "fn nothing() {}\n"})
            (root / BASELINE_FILE).write_text('{"as_casts": 0}\n', encoding="utf-8")

            with self.assertRaises(SystemExit) as raised:
                run("--root", str(root))

            self.assertIn("just ratchet --update", str(raised.exception))

    def test_show_lists_the_sites_behind_a_count(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(root, {"src/lib.rs": "fn width(x: u64) -> u32 {\n    x as u32\n}\n"})

            status, listing, _ = run("--root", str(root), "--show", "as_casts")

            self.assertEqual(status, 0)
            self.assertEqual(listing, "src/lib.rs:2: as_casts\n")


class CountTests(unittest.TestCase):

    def test_tunable_aliases_stay_in_their_lexical_scope(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(root, {**ANCHOR, "src/lib.rs": """
use std::time::Duration;
mod inner { type Duration = String; const LABEL: Duration = make(); }
const WAIT: Duration = Duration::from_secs(3);
fn run() { use std::time::Duration as Budget; Budget::from_millis(2); }
#[cfg(test)] mod tests { type Duration = String; }
"""})
            self.assertEqual([site.line for site in measure_sources(root)["hidden_tunables"]], [4, 5])

    def test_hidden_tunables_cover_every_governed_constant_type(self) -> None:
        types = ["Duration", "usize", "u64", "u32", "u16", "u8", "i64", "i32", "f64", "NonZeroU8", "NonZeroUsize", "ByteUnit"]
        with TemporaryDirectory() as directory:
            root = Path(directory)
            source = "\n".join(f"const C{index}: {kind} = make();" for index, kind in enumerate(types))
            write_repository(root, {**ANCHOR, "src/lib.rs": source})
            self.assertEqual(count(root, "hidden_tunables"), len(types))

    def test_fixed_contracts_cannot_hide_malformed_or_misplaced_annotations(self) -> None:
        for annotation in [
            '#[cfg_attr(nervix_lint, nervix::constant(fixed, reason = ""))] const C: usize = 1;',
            '#[cfg_attr(nervix_lint, nervix::constant(variable, reason = "wire"))] const C: usize = 1;',
            '#[cfg_attr(nervix_lint, nervix::constant(fixed, reason = "wire"))] fn f() {}',
            '#[cfg_attr(any(), nervix::constant(fixed, reason = "wire"))] const C: usize = 1;',
        ]:
            with self.subTest(annotation=annotation), TemporaryDirectory() as directory:
                root = Path(directory)
                write_repository(root, {**ANCHOR, "src/lib.rs": annotation})
                with self.assertRaisesRegex(SystemExit, "invalid fixed-constant contract"):
                    measure_sources(root)

    def test_hidden_tunables_count_numeric_constants_and_literal_policy_calls(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(root, {**ANCHOR, "src/policy.rs": """
use std::time::Duration as Budget;
const WAIT: Budget = Budget::from_secs(30);
const DEPTH: usize = 64;
const NAME: &str = "Duration::from_secs(99)";
#[cfg(any())] const OTHER: std::num::NonZeroU32 = nonzero!(8u32);
fn configure() {
    let wait = Budget::from_millis(5);
    let queue = mpsc::channel(16);
    let rows = Vec::with_capacity(128);
    let permits = Semaphore::new(4);
    let dynamic = Vec::with_capacity(rows.len());
}
"""})
            sites = measure_sources(root)["hidden_tunables"]
            self.assertEqual([site.line for site in sites], [3, 4, 6, 8, 9, 10, 11])
            (root / "src/new.rs").write_text("const NEW: u64 = 12;\n")
            self.assertEqual(count(root, "hidden_tunables"), 8)

    def test_hidden_tunable_exclusions_are_narrow_and_source_owned(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(root, {**ANCHOR, "src/policy.rs": """
#[cfg_attr(nervix_lint, nervix::constant(fixed, reason = "the frame ABI reserves four bytes"))]
const WIDTH: usize = 4;
struct Local { budget: Setting<Budget> }
impl Default for Local { fn default() -> Self { Self { budget: Duration::from_secs(7) } } }
impl SettingDefinition for Budget {
    fn default_value() -> Duration { Duration::from_secs(8) }
    fn other() { Duration::from_secs(9); }
}
#[cfg(test)] mod checks { const ONLY: usize = 10; }
#[test] fn probe() { Duration::from_secs(11); }
struct Ordinary;
impl Default for Ordinary { fn default() -> Self { Duration::from_secs(12); Self } }
""", "tests/probe.rs": "const LIMIT: usize = 1;", "benches/probe.rs": "const LIMIT: usize = 1;",
                "examples/probe.rs": "const LIMIT: usize = 1;", "crates/arbitrary/src/lib.rs": "const LIMIT: usize = 1;",
                "crates/model-harness/src/lib.rs": "const LIMIT: usize = 1;", "crates/test-environment/src/lib.rs": "const LIMIT: usize = 1;",
                "crates/benchmark/src/lib.rs": "const LIMIT: usize = 1;", "tools/probe/src/lib.rs": "const LIMIT: usize = 1;"})
            self.assertEqual([site.line for site in measure_sources(root)["hidden_tunables"]], [8, 13])

    def test_hidden_tunable_gate_refuses_an_added_policy_literal(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(root, {"src/lib.rs": "fn configure() {}\n"})
            write_baseline(root / BASELINE_FILE, {item.name: len(measure_sources(root)[item.name]) for item in COUNTS})
            (root / "src/lib.rs").write_text("fn configure() { mpsc::channel(32); }\n")
            with mock.patch("scripts.ratchet.check_synchronization", return_value=None):
                status, report, _ = run("--root", str(root))
            self.assertEqual(status, 1)
            self.assertIn("hidden_tunables: 0 -> 1", report)

    def test_malformed_source_cannot_report_a_clean_structural_count(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(root, {"src/lib.rs": "fn {"})
            with self.assertRaisesRegex(SystemExit, r"src/lib.rs:1:"):
                measure_sources(root)

    def test_file_size_includes_final_lines_and_new_authored_sources(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(root, {**ANCHOR, "src/lib.rs": "// line\n" * 3000 + "// final"})
            self.assertEqual(count(root, "files_over_3000_lines"), 1)
            (root / "src/new.rs").write_text("// line\n" * 3001)
            self.assertEqual(count(root, "files_over_3000_lines"), 2)

    def test_cast_syntax_preserves_nested_casts_and_raw_identifiers(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(root, {"src/lib.rs": """
struct Label { r#as: u32 }
fn r#as(r#as: u32) -> u32 { r#as }
fn cast_in_path() { <[u8; { 1u8 as usize }] as Default>::default(); }
#[cfg(any())]
fn inactive(value: u64) -> u32 { value as u32 }
macro_rules! cast { ($value:expr) => { $value as u16 }; }
"""})
            self.assertEqual(count(root, "as_casts"), 3)

    def test_testing_gates_belong_to_struct_fields_including_tuple_fields(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(root, {**ANCHOR, "src/lib.rs": """
struct Tuple(#[cfg(feature = "testing")] u32);
#[cfg(any())]
struct Inactive { #[cfg(all(feature = "testing", feature = "other"))] value: u32 }
struct Stable { #[cfg_attr(feature = "testing", doc = "test note")] value: u32 }
enum Choice { Data { #[cfg(feature = "testing")] value: u32 } }
"""})
            self.assertEqual(count(root, "testing_feature_struct_fields"), 2)

    def test_as_casts_cover_new_sources_and_ignore_non_expression_syntax(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(
                root,
                {
                    "src/lib.rs": """
use std::sync::Arc as StdArc;

#[rkyv(as = ArchivedSnapshot)]
struct SnapshotView;

#[rkyv(as=ArchivedOffsets)]
struct OffsetView;

/// Widens a counter, such as a backlog depth.
fn width(x: u64) -> u32 {
    let label = "reported as milliseconds";
    let _ = label;
    x as u32
}

fn qualified() -> u8 {
    <u8 as Default>::default()
}

fn nested_qualified<'src>() -> <&'src [u8] as Widen<'src>>::Output {
    <Reader<'src> as Label<'src, &'src [u8], Pattern<'src, u8>>>::expected()
}

#[cfg(test)]
mod tests {
    #[test]
    fn widens() {
        assert_eq!(super::width(1u64 as u64), 1);
    }
}
""",
                    "tests/integration.rs": "fn helper(x: u64) -> u32 { x as u32 }\n",
                },
            )
            (root / "src" / "untracked.rs").write_text("fn f(x: u64) { x as u32; }\n")

            self.assertEqual(count(root, "as_casts"), 2)


    def test_testing_feature_counts_struct_fields_but_not_gated_statements(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(
                root,
                {
                    "src/lib.rs": """
pub struct Runtime {
    #[cfg(feature = "testing")]
    pub emitter_faults: EmitterFaultInjector,
    pub shutdown: CancellationToken,
}

impl Runtime {
    #[cfg(feature = "testing")]
    pub fn arm(&self) {}

    pub fn emit(&self) {
        #[cfg(feature = "testing")]
        self.emitter_faults.take();
    }
}
""",
                },
            )

            self.assertEqual(count(root, "testing_feature_struct_fields"), 1)







    def test_files_over_3000_lines_count_in_crate_tests_but_not_the_test_tree(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(
                root,
                {
                    **ANCHOR,
                    "src/big.rs": "// line\n" * 3001,
                    "src/runtime/tests.rs": "// line\n" * 3001,
                    "src/small.rs": "// line\n" * 2999,
                    "tests/scenarios.rs": "// line\n" * 3001,
                },
            )

            self.assertEqual(count(root, "files_over_3000_lines"), 2)


COMPLEXITY = "functions_over_complexity_30"


def branching(name: str, complexity: int) -> str:
    """A function the analyzer scores at exactly `complexity`: one path, plus one per `if`."""

    decisions = "".join(
        f"    if value == {index} {{\n        return {index};\n    }}\n" for index in range(complexity - 1)
    )
    return f"fn {name}(value: u32) -> u32 {{\n{decisions}    0\n}}\n"


def complex_functions(root: Path) -> list[str]:
    return [site.detail.split()[0] for site in measure_sources(root).get(COMPLEXITY, [])]


class ComplexityCountTests(unittest.TestCase):
    """The count runs the pinned analyzer over the fixture, so these fixtures qualify its scope."""

    def test_only_a_function_above_30_is_counted(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            at_limit = branching("at_limit", 30)
            write_repository(root, {"src/lib.rs": at_limit + branching("above_limit", 31)})

            sites = measure_sources(root).get(COMPLEXITY, [])

            line = at_limit.count("\n") + 1
            self.assertEqual(
                [site.render() for site in sites],
                [f"src/lib.rs:{line}: above_limit has cyclomatic complexity 31"],
            )

    def test_a_function_in_an_inactive_configuration_is_counted(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(root, {"src/lib.rs": (
                "#[cfg(any())]\n" + branching("never_compiled", 31)
                + '#[cfg(feature = "absent")]\nmod gated {\n' + branching("gated", 31) + "}\n"
            )})

            self.assertEqual(complex_functions(root), ["never_compiled", "gated"])

    def test_a_function_inside_a_macro_is_not_counted(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(root, {"src/lib.rs": (
                "macro_rules! define {\n    () => {\n" + branching("generated", 31) + "    };\n}\n"
                + "define!();\n"
                + "cfg_if::cfg_if! {\n" + branching("invoked", 31) + "}\n"
                + branching("authored", 31)
            )})

            self.assertEqual(complex_functions(root), ["authored"])

    def test_provided_trait_methods_nested_functions_and_closures_are_not_counted(self) -> None:
        closure = " ".join(f"if value == {index} {{ return {index}; }}" for index in range(31))
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(root, {"src/lib.rs": (
                "trait Route {\n" + branching("provided", 31) + "}\n"
                + "impl Route for u8 {\n" + branching("implemented", 31) + "}\n"
                + "fn outer() -> u32 {\n" + branching("nested", 31)
                + f"    let closure = |value: u32| {{ {closure} 0 }};\n    closure(1)\n}}\n"
            )})

            self.assertEqual(complex_functions(root), ["u8::implemented"])

    def test_eligibility_matches_the_other_structural_counts(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(root, {
                ".gitignore": "/src/generated.rs\n",
                "src/lib.rs": branching("product", 31),
                "src/runtime/tests.rs": branching("tests_file_helper", 31),
                "src/codec_tests.rs": branching("suffix_tests_helper", 31),
                "src/test_fixtures.rs": branching("fixture_helper", 31),
                "src/checks.rs": (
                    "#[test]\n" + branching("test_function", 31)
                    + "#[cfg(test)]\nmod tests {\n" + branching("test_module_helper", 31) + "}\n"
                ),
                "tests/integration.rs": branching("integration", 31),
                "benches/load.rs": branching("bench", 31),
                "crates/tool/examples/demo.rs": branching("example", 31),
                "crates/tool/tests/suite.rs": branching("crate_suite", 31),
            })
            (root / "src/authored.rs").write_text(branching("newly_authored", 31), encoding="utf-8")
            (root / "src/generated.rs").write_text(branching("ignored", 31), encoding="utf-8")

            self.assertEqual(
                sorted(complex_functions(root)),
                ["fixture_helper", "newly_authored", "product", "suffix_tests_helper", "tests_file_helper"],
            )


class ComplexityGateTests(unittest.TestCase):
    def setUp(self) -> None:
        patch = mock.patch("scripts.ratchet.check_synchronization", return_value=None)
        patch.start()
        self.addCleanup(patch.stop)

    def test_a_new_function_above_30_fails_the_gate_naming_it(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(root, {"src/lib.rs": branching("existing", 31)})
            write_baseline(root / BASELINE_FILE, {count.name: len(measure_sources(root).get(count.name, [])) for count in COUNTS})
            (root / "src/added.rs").write_text(branching("added", 40), encoding="utf-8")

            status, report, _ = run("--root", str(root))

            self.assertEqual(status, 1)
            self.assertIn(f"{COMPLEXITY}: 1 -> 2 (+1)", report)
            self.assertIn("src/added.rs:1: added has cyclomatic complexity 40", report)

    def test_growing_an_existing_offender_keeps_the_count_and_lowering_it_is_recorded(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(root, {"src/lib.rs": branching("first", 31) + branching("second", 31)})
            write_baseline(root / BASELINE_FILE, {count.name: len(measure_sources(root).get(count.name, [])) for count in COUNTS})
            (root / "src/lib.rs").write_text(branching("first", 60) + branching("second", 31), encoding="utf-8")
            self.assertEqual(run("--root", str(root))[0], 0)

            (root / "src/lib.rs").write_text(branching("first", 60) + branching("second", 30), encoding="utf-8")
            status, report, _ = run("--root", str(root), "--update")

            self.assertEqual(status, 0)
            self.assertIn(f"{COMPLEXITY}: 2 -> 1", report)
            self.assertEqual(json.loads((root / BASELINE_FILE).read_text())[COMPLEXITY], 1)

    def test_show_lists_function_file_line_and_complexity(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(root, {"src/lib.rs": "// lead\n" + branching("wide", 33)})

            status, listing, _ = run("--root", str(root), "--show", COMPLEXITY)

            self.assertEqual(status, 0)
            self.assertEqual(listing, "src/lib.rs:2: wide has cyclomatic complexity 33\n")

    def test_a_failed_analysis_cannot_rewrite_the_baseline(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(root, {"src/lib.rs": branching("existing", 31)})
            write_baseline(root / BASELINE_FILE, {count.name: 1 for count in COUNTS})
            before = (root / BASELINE_FILE).read_bytes()
            with mock.patch("scripts.cargo_crap.executable", return_value=root / "missing/cargo-crap"):
                with self.assertRaisesRegex(SystemExit, "not installed"):
                    run("--root", str(root), "--update")
            self.assertEqual((root / BASELINE_FILE).read_bytes(), before)


def fake_analyzer(directory: Path, *, version: str = "0.6.1", report: object | None = None,
                  stderr: str = "", status: int = 0) -> Path:
    """An executable that answers `--version` like the analyzer and replays one analysis."""

    if report is None:
        report = {"version": version, "entries": [
            {"file": "./src/lib.rs", "function": "existing", "line": 1, "cyclomatic": 31.0, "coverage": None, "crap": 992.0},
        ]}
    output = report if isinstance(report, str) else json.dumps(report)
    script = directory / "cargo-crap"
    script.write_text(
        "#!/usr/bin/env python3\nimport sys\n"
        f"if sys.argv[1:] == ['--version']:\n    print('cargo-crap {version}')\n    sys.exit(0)\n"
        f"sys.stdout.write({output!r})\nsys.stderr.write({stderr!r})\nsys.exit({status})\n",
        encoding="utf-8",
    )
    script.chmod(0o755)
    return script


class ComplexityAnalysisFailureTests(unittest.TestCase):
    """Every way the analysis can miss functions fails the count instead of reading as no debt."""

    def measure_with(self, analyzer: Path | None = None, sources: dict[str, str] | None = None) -> str:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "repository"
            root.mkdir()
            write_repository(root, sources or {"src/lib.rs": branching("existing", 31)})
            if analyzer is None:
                return self.failure(root)
            with mock.patch("scripts.cargo_crap.executable", return_value=analyzer):
                return self.failure(root)

    def failure(self, root: Path) -> str:
        with self.assertRaises(SystemExit) as raised:
            measure_sources(root)
        return str(raised.exception)

    def test_a_missing_analyzer_names_the_install_command(self) -> None:
        with TemporaryDirectory() as directory:
            message = self.measure_with(Path(directory) / "cargo-crap")
        self.assertIn("cargo-crap 0.6.1 is not installed", message)
        self.assertIn("run `just install-cargo-crap`", message)

    def test_another_analyzer_version_is_refused_with_the_install_command(self) -> None:
        with TemporaryDirectory() as directory:
            message = self.measure_with(fake_analyzer(Path(directory), version="0.6.2"))
        self.assertIn("is cargo-crap 0.6.2, but this repository pins 0.6.1", message)
        self.assertIn("run `just install-cargo-crap`", message)

    def test_a_failing_analyzer_fails_the_count(self) -> None:
        with TemporaryDirectory() as directory:
            message = self.measure_with(fake_analyzer(Path(directory), report="", stderr="Error: walk failed\n", status=2))
        self.assertIn("cargo-crap failed with exit 2", message)
        self.assertIn("Error: walk failed", message)

    def test_a_source_the_analyzer_skipped_fails_the_count(self) -> None:
        skipped = "warning: could not analyze ./src/lib.rs: parsing ./src/lib.rs\n"
        with TemporaryDirectory() as directory:
            message = self.measure_with(fake_analyzer(Path(directory), stderr=skipped))
        self.assertIn("skipped sources it could not analyze", message)
        self.assertIn("./src/lib.rs", message)

    def test_output_that_is_not_a_report_fails_the_count(self) -> None:
        with TemporaryDirectory() as directory:
            message = self.measure_with(fake_analyzer(Path(directory), report="Function  CRAP\n"))
        self.assertIn("wrote no JSON report", message)

    def test_an_empty_analysis_fails_the_count(self) -> None:
        with TemporaryDirectory() as directory:
            message = self.measure_with(fake_analyzer(Path(directory), report={"version": "0.6.1", "entries": []}))
        self.assertIn("measured no function", message)

    def test_a_repository_without_functions_fails_the_count(self) -> None:
        message = self.measure_with(sources={"src/lib.rs": "pub struct Empty;\n"})
        self.assertIn("measured no function", message)

    def test_a_report_of_another_version_or_weight_is_refused(self) -> None:
        entry = {"file": "./src/lib.rs", "function": "existing", "line": 1, "cyclomatic": 31.0}
        with TemporaryDirectory() as directory:
            message = self.measure_with(fake_analyzer(Path(directory), report={"version": "0.7.0", "entries": [entry]}))
        self.assertIn("wrote a version '0.7.0' report", message)
        with TemporaryDirectory() as directory:
            message = self.measure_with(
                fake_analyzer(Path(directory), report={"version": "0.6.1", "entries": [entry], "try_weight": 0.5})
            )
        self.assertIn("weighted `?` at 0.5", message)

    def test_a_configuration_in_the_analyzers_reach_is_refused(self) -> None:
        message = self.measure_with(sources={
            ".cargo-crap.toml": 'exclude = ["src/**"]\n',
            "src/lib.rs": branching("existing", 31),
        })
        self.assertIn(".cargo-crap.toml would change what the analyzer measures", message)

    def test_a_measured_file_outside_source_discovery_fails_the_count(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(root, {"src/lib.rs": branching("existing", 31)})
            # Git lists a nested repository as one directory, while the analyzer walks into it.
            nested = root / "vendor/clone"
            nested.mkdir(parents=True)
            subprocess.run(["git", "init", "-q", "-b", "main"], cwd=nested, check=True)
            (nested / "lib.rs").write_text(branching("cloned", 31), encoding="utf-8")

            message = self.failure(root)

        self.assertIn("source discovery does not govern", message)
        self.assertIn("vendor/clone/lib.rs", message)

    def test_a_discovered_file_the_analyzer_cannot_read_fails_the_count(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(root, {
                ".gitignore": "/src/forced.rs\n",
                "src/lib.rs": branching("existing", 31),
                "src/.hidden/inner.rs": branching("hidden", 31),
            })
            (root / "src/forced.rs").write_text(branching("forced", 31), encoding="utf-8")
            (root / "src/linked.rs").symlink_to("lib.rs")
            subprocess.run(["git", "add", "--force", "src/forced.rs", "src/linked.rs"], cwd=root, check=True)

            message = self.failure(root)

        self.assertIn("cannot read these discovered Rust files", message)
        for path in ("src/.hidden/inner.rs", "src/forced.rs", "src/linked.rs"):
            self.assertIn(path, message)


if __name__ == "__main__":
    unittest.main()
