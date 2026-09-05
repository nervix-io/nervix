from __future__ import annotations

import io
import json
import subprocess
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts.ratchet import BASELINE_FILE, main, measure


def write_repository(root: Path, sources: dict[str, str]) -> None:
    """Stage `sources` in a git repository, because the counts read tracked files only."""

    for path, body in sources.items():
        target = root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(body, encoding="utf-8")
    subprocess.run(["git", "init", "-q", "-b", "main"], cwd=root, check=True)
    subprocess.run(["git", "add", "-A"], cwd=root, check=True)


def count(root: Path, name: str) -> int:
    return len(measure(root)[name])


def run(*arguments: str) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        status = main(list(arguments))
    return status, out.getvalue(), err.getvalue()


class RatchetGateTests(unittest.TestCase):
    def test_a_deliberate_increase_fails_and_names_the_count(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(root, {"src/lib.rs": "fn width(x: u64) -> u32 { x as u32 }\n"})

            status, _, _ = run("--root", str(root), "--update")
            self.assertEqual(status, 0)
            self.assertEqual(json.loads((root / BASELINE_FILE).read_text())["as_casts"], 1)

            (root / "src" / "lib.rs").write_text(
                "fn width(x: u64) -> u32 { x as u32 }\nfn depth(y: u64) -> u16 { y as u16 }\n",
                encoding="utf-8",
            )

            status, _, errors = run("--root", str(root))
            self.assertEqual(status, 1)
            self.assertIn("as_casts: 1 -> 2 (+1)", errors)

    def test_an_unchanged_tree_passes(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(root, {"src/lib.rs": "fn width(x: u64) -> u32 { x as u32 }\n"})

            run("--root", str(root), "--update")
            status, report, errors = run("--root", str(root))

            self.assertEqual(status, 0)
            self.assertEqual(errors, "")
            self.assertIn("as_casts", report)

    def test_a_decrease_passes_and_asks_for_the_baseline_to_be_updated(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(root, {"src/lib.rs": "fn width(x: u64) -> u32 { x as u32 }\n"})

            run("--root", str(root), "--update")
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
            self.assertEqual(listing, "src/lib.rs:2: x as u32\n")


class CountTests(unittest.TestCase):
    def test_as_casts_ignore_comments_literals_imports_and_unit_tests(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(
                root,
                {
                    "src/lib.rs": """
use std::sync::Arc as StdArc;

/// Widens a counter, such as a backlog depth.
fn width(x: u64) -> u32 {
    let label = "reported as milliseconds";
    let _ = label;
    x as u32
}

fn qualified() -> u8 {
    <u8 as Default>::default()
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

            self.assertEqual(count(root, "as_casts"), 1)

    def test_bare_unwrap_and_expect_exclude_unit_tests(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(
                root,
                {
                    "src/lib.rs": """
fn read(value: Option<u8>) -> u8 {
    let first = value.unwrap();
    let second = value.expect("checked above");
    let kept = value.unwrap_or_default();
    first + second + kept
}

#[cfg(test)]
mod tests {
    #[test]
    fn reads() {
        Some(1u8).unwrap();
    }
}
""",
                },
            )

            self.assertEqual(count(root, "bare_unwrap_and_expect"), 2)

    def test_result_string_errors_span_lines_and_ignore_other_errors(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(
                root,
                {
                    "src/lib.rs": """
struct Envelope {
    result: Result<Vec<u8>, String>,
}

fn parse(
    value: &str,
) -> Result<
    Vec<u8>,
    String,
> {
    Ok(value.as_bytes().to_vec())
}

fn typed(value: &str) -> Result<Vec<u8>, ParseError> {
    Ok(value.as_bytes().to_vec())
}

fn nested(value: &str) -> Result<Vec<String>, ParseError> {
    Ok(vec![value.to_owned()])
}
""",
                },
            )

            self.assertEqual(count(root, "result_string_errors"), 2)

    def test_bare_error_signatures_skip_reported_and_foreign_errors(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(
                root,
                {
                    "src/error.rs": "pub enum StoreError { Missing }\n",
                    "src/lib.rs": """
fn bare() -> Result<(), StoreError> {
    Ok(())
}

fn reported() -> Result<(), Report<StoreError>> {
    Ok(())
}

fn foreign() -> Result<(), std::io::Error> {
    Ok(())
}

fn associated() -> Result<(), Self::Error> {
    Ok(())
}
""",
                },
            )

            self.assertEqual(count(root, "bare_error_signatures"), 1)

    def test_string_node_ids_cover_fields_parameters_and_aliases(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(
                root,
                {
                    "src/lib.rs": """
pub type NodeId = String;

pub struct Placement {
    pub primary_node: Option<String>,
    pub assigned_nodes: Vec<String>,
    pub domain: String,
}

fn assign(target_node_id: String) -> Placement {
    Placement {
        primary_node: Some(String::new()),
        assigned_nodes: Vec::new(),
        domain: String::new(),
    }
}
""",
                },
            )

            self.assertEqual(count(root, "string_node_ids"), 4)

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

    def test_parser_references_are_allowed_at_the_edges_only(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(
                root,
                {
                    "src/application.rs": "fn parse() { nervix_nspl::parse_expression(\"a\"); }\n",
                    "crates/nervix-cli/src/lib.rs": "use nervix_nspl::Token;\n",
                    "src/runtime/mod.rs": "use nervix_nspl::vm_program::Program;\n",
                    "crates/nervix-vm/src/lib.rs": "fn lower(p: nervix_nspl::vm_program::Program) {}\n",
                },
            )

            self.assertEqual(count(root, "parser_imports_outside_edges"), 2)

    def test_model_matches_exclude_the_planner_and_the_rest_of_the_tree(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(
                root,
                {
                    "src/runtime/emitters.rs": "fn sink(m: Model) { matches!(m, Model::ClientKafka(_)); }\n",
                    "src/runtime/planning.rs": "fn plan(m: Model) { matches!(m, Model::SourceKafka(_)); }\n",
                    "src/registry/mod.rs": "fn check(m: Model) { matches!(m, Model::Relay(_)); }\n",
                },
            )

            self.assertEqual(count(root, "model_matches_in_data_plane"), 1)

    def test_files_over_3000_lines_count_in_crate_tests_but_not_the_test_tree(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(
                root,
                {
                    "src/big.rs": "// line\n" * 3001,
                    "src/runtime/tests.rs": "// line\n" * 3001,
                    "src/small.rs": "// line\n" * 2999,
                    "tests/scenarios.rs": "// line\n" * 3001,
                },
            )

            self.assertEqual(count(root, "files_over_3000_lines"), 2)


if __name__ == "__main__":
    unittest.main()
