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
    """Stage `sources` in a git repository, because the counts read tracked files only."""

    (root / "Cargo.toml").write_text('[package]\nname="ratchet-fixture"\nversion="0.1.0"\n')
    for path, body in sources.items():
        target = root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(body, encoding="utf-8")
    subprocess.run(["git", "init", "-q", "-b", "main"], cwd=root, check=True)
    subprocess.run(["git", "add", "-A"], cwd=root, check=True)


def count(root: Path, name: str) -> int:
    return len(measure_sources(root)[name])


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
            self.assertEqual(listing, "src/lib.rs:2: x as u32\n")


class CountTests(unittest.TestCase):

    def test_as_casts_ignore_comments_literals_imports_attributes_and_unit_tests(self) -> None:
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

            self.assertEqual(count(root, "as_casts"), 1)


    def test_combinator_control_flow_spares_pipelines_and_single_adapters(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(
                root,
                {
                    "src/lib.rs": """
fn describe(state: Option<State>, raw: &str) -> Result<Vec<u8>, ParseError> {
    let width = state.map(|state| state.width).unwrap_or_default();
    let label = state.map_or("absent", State::label);
    let parsed = raw.trim().parse::<u64>().map_err(ParseError::Invalid)?;
    let scaled = state.and_then(|state| {
        let scaled = state.width * parsed;
        NonZeroUsize::new(scaled)
    });
    let parts = raw
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(str::as_bytes)
        .collect::<Vec<_>>();
    let first = parts.first().ok_or_else(|| ParseError::Empty)?;
    let bound = scaled.unwrap_or(1);
    // state.map(State::label).unwrap_or_default() is written here as prose only.
    let quoted = "state.map(State::label).unwrap_or_default()";
    Ok(first.to_vec())
}

#[cfg(test)]
mod tests {
    #[test]
    fn describes() {
        assert_eq!(Some(1u8).map(u16::from).unwrap_or_default(), 1);
    }
}
""",
                },
            )

            self.assertEqual(count(root, "combinator_control_flow"), 3)



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
                    "src/big.rs": "// line\n" * 3001,
                    "src/runtime/tests.rs": "// line\n" * 3001,
                    "src/small.rs": "// line\n" * 2999,
                    "tests/scenarios.rs": "// line\n" * 3001,
                },
            )

            self.assertEqual(count(root, "files_over_3000_lines"), 2)


if __name__ == "__main__":
    unittest.main()
