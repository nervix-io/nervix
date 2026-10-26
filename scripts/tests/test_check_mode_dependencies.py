from __future__ import annotations

import io
import json
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Sequence

from scripts.check_mode_dependencies import (
    CheckError,
    Commands,
    Root,
    main,
    parse_tree,
)

PACKAGES = ["nervix-client-wire", "nervix-engine", "nervix-models", "nervix-web-console"]

ORDINARY = """\
nervix-engine v0.1.0-dev (/repository/crates/engine) default
nervix-primitives v0.1.0-dev (/repository/crates/primitives) default,native
tokio v1.53.1 default,macros,rt,sync
nervix-primitives-macros v0.1.0-dev (/repository/crates/primitives/macros) (proc-macro)
arrow-buffer v58.4.0  (*)
"""

PORTABLE = """\
nervix-models v0.1.0-dev (/repository/crates/models) default
nervix-primitives v0.1.0-dev (/repository/crates/primitives) default
triomphe v0.1.16 default,serde,stable_deref_trait,std
"""


class FakeCargo(Commands):
    """Answers `cargo metadata` and `cargo tree` from canned graphs, recording every call."""

    graphs: dict[tuple[str, ...], str] = {}
    failing: tuple[str, ...] | None = None

    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self.calls: list[list[str]] = []

    def run(self, arguments: Sequence[str]) -> str:
        arguments = list(arguments)
        self.calls.append(arguments)
        if arguments[:2] == ["cargo", "metadata"]:
            return json.dumps({"packages": [{"name": name} for name in PACKAGES]})
        selection = tuple(arguments[2 : arguments.index("--edges")])
        if selection == self.failing:
            raise CheckError(f"`{' '.join(arguments)}` failed with status 101")
        for key, output in self.graphs.items():
            if all(part in selection for part in key):
                return output
        if "--target" in selection or "nervix-models" in selection:
            return PORTABLE
        return ORDINARY


def run(graphs: dict[tuple[str, ...], str], failing: tuple[str, ...] | None = None) -> tuple[int, str]:
    class Cargo(FakeCargo):
        pass

    Cargo.graphs = graphs
    Cargo.failing = failing
    out = io.StringIO()
    with redirect_stdout(out), redirect_stderr(out):
        status = main(["--root", "/repository"], commands_for=Cargo)
    return status, out.getvalue()


class ParseTests(unittest.TestCase):
    def test_every_line_shape_cargo_prints_is_read(self) -> None:
        nodes = parse_tree(ORDINARY)
        self.assertEqual(
            [node.name for node in nodes],
            [
                "nervix-engine",
                "nervix-primitives",
                "tokio",
                "nervix-primitives-macros",
                "arrow-buffer",
            ],
        )
        self.assertEqual(nodes[1].features, frozenset({"default", "native"}))
        self.assertEqual(nodes[3].features, frozenset())
        self.assertEqual(nodes[4].features, frozenset())

    def test_an_unreadable_line_stops_the_check(self) -> None:
        with self.assertRaises(CheckError):
            parse_tree("not a dependency line\n")

    def test_a_root_describes_and_selects_its_graph(self) -> None:
        root = Root(package="nervix-web-console", default_features=False, target="wasm32-unknown-unknown")
        self.assertEqual(
            root.arguments(),
            [
                "--package",
                "nervix-web-console",
                "--no-default-features",
                "--target",
                "wasm32-unknown-unknown",
            ],
        )
        self.assertEqual(
            root.describe(), "nervix-web-console with no default features for wasm32-unknown-unknown"
        )
        self.assertEqual(Root(package=None, default_features=True).arguments(), ["--workspace"])


class CheckTests(unittest.TestCase):
    def test_ordinary_and_portable_graphs_pass(self) -> None:
        status, report = run({})
        self.assertEqual(status, 0, report)
        # The workspace and four packages, each with and without default features, and three
        # portable roots the same way.
        self.assertIn("16 ordinary graphs contain no execution mode", report)

    def test_every_root_is_read_with_and_without_default_features(self) -> None:
        class Cargo(FakeCargo):
            pass

        Cargo.graphs = {}
        Cargo.failing = None
        calls: list[list[str]] = []

        class Recording(Cargo):
            def run(self, arguments: Sequence[str]) -> str:
                calls.append(list(arguments))
                return super().run(arguments)

        with redirect_stdout(io.StringIO()):
            self.assertEqual(main(["--root", "/repository"], commands_for=Recording), 0)
        trees = [call for call in calls if "tree" in call]
        for call in trees:
            self.assertEqual(call[-2:], ["--color", "never"])
        selections = {tuple(call[2 : call.index("--edges")]) for call in trees}
        self.assertIn(("--workspace",), selections)
        self.assertIn(("--workspace", "--no-default-features"), selections)
        for package in PACKAGES:
            self.assertIn(("--package", package), selections)
            self.assertIn(("--package", package, "--no-default-features"), selections)
        self.assertIn(
            ("--package", "nervix-web-console", "--target", "wasm32-unknown-unknown"), selections
        )
        self.assertIn(
            (
                "--package",
                "nervix-client-wire",
                "--no-default-features",
                "--target",
                "wasm32-unknown-unknown",
            ),
            selections,
        )

    def test_a_model_checker_in_a_package_graph_fails(self) -> None:
        status, report = run(
            {("nervix-engine", "--no-default-features"): ORDINARY + "loom v0.7.2 checkpoint\n"}
        )
        self.assertEqual(status, 1)
        self.assertIn(
            "the normal graph of nervix-engine with no default features contains `loom`, which "
            "only a modeled or diagnostic build may contain",
            report,
        )

    def test_a_simulator_or_a_modeled_wrapper_fails(self) -> None:
        status, report = run(
            {
                ("--workspace",): ORDINARY
                + "turmoil v0.7.2 \n"
                + "shuttle-tokio v1.0.0 default,sync\n"
            }
        )
        self.assertEqual(status, 1)
        self.assertIn("contains `turmoil`", report)
        self.assertIn("contains `shuttle-tokio`", report)

    def test_the_deadlock_detector_in_a_package_graph_fails(self) -> None:
        status, report = run({("nervix-engine",): ORDINARY + "deloxide v1.1.0 \n"})
        self.assertEqual(status, 1)
        self.assertIn(
            "the normal graph of nervix-engine with default features contains `deloxide`, which "
            "only a modeled or diagnostic build may contain",
            report,
        )

    def test_the_diagnostic_mode_on_the_boundary_fails(self) -> None:
        status, report = run(
            {
                ("nervix-engine",): ORDINARY.replace(
                    "default,native", "default,deloxide,native"
                )
            }
        )
        self.assertEqual(status, 1)
        self.assertIn(
            "the normal graph of nervix-engine with default features enables "
            "`nervix-primitives/deloxide`, which only a modeled, diagnostic or test build selects",
            report,
        )

    def test_a_mode_or_the_paused_clock_on_the_boundary_fails(self) -> None:
        status, report = run(
            {
                ("nervix-engine",): ORDINARY.replace(
                    "default,native", "default,native,shuttle,test-util"
                )
            }
        )
        self.assertEqual(status, 1)
        self.assertIn(
            "the normal graph of nervix-engine with default features enables "
            "`nervix-primitives/shuttle`",
            report,
        )
        self.assertIn("enables `nervix-primitives/test-util`", report)

    def test_a_native_library_in_a_portable_graph_fails(self) -> None:
        status, report = run(
            {
                ("nervix-web-console", "--target"): PORTABLE + "mio v1.0.4 net,os-poll\n",
                ("nervix-models",): PORTABLE.replace(
                    "nervix-primitives v0.1.0-dev (/repository/crates/primitives) default",
                    "nervix-primitives v0.1.0-dev (/repository/crates/primitives) default,native",
                ),
            }
        )
        self.assertEqual(status, 1)
        self.assertIn(
            "the portable graph of nervix-web-console with default features for "
            "wasm32-unknown-unknown contains `mio`, a native runtime or network library",
            report,
        )
        self.assertIn(
            "the portable graph of nervix-models with default features enables "
            "`nervix-primitives/native`",
            report,
        )

    def test_a_graph_cargo_cannot_produce_fails_the_check(self) -> None:
        status, report = run({}, failing=("--package", "nervix-engine"))
        self.assertEqual(status, 1)
        self.assertIn("failed with status 101", report)


if __name__ == "__main__":
    unittest.main()
