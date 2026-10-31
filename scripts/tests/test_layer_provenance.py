"""Import provenance stays visible before cfg selection and compiler resolution."""

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from scripts import check_layer_provenance as boundary


def layer(kind: str) -> str:
    return f'#![cfg_attr(nervix_lint, nervix::layer({kind}, reason="test owner"))]\n'


class LayerProvenanceTests(unittest.TestCase):
    def check(self, consumer: str, extra: dict[str, str] | None = None,
              dependencies: str = 'syntax = { package = "syntax-engine", path = "syntax" }') -> list[str]:
        files = {
            "Cargo.toml": '[package]\nname="consumer"\nversion="0.1.0"\n[dependencies]\n' + dependencies,
            "src/lib.rs": consumer,
            "syntax/Cargo.toml": '[package]\nname="syntax-engine"\nversion="0.1.0"',
            "syntax/src/lib.rs": layer("language") + "pub struct Token;",
            **(extra or {}),
        }
        with TemporaryDirectory() as directory:
            root = Path(directory)
            for path, text in files.items():
                target = root / path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(text)
            return boundary.check(root, list(files))

    def test_renamed_inactive_dependency_requires_an_edge_owner(self) -> None:
        problems = self.check(layer("engine") + '#[cfg(any())]\nuse syntax as hidden;')
        self.assertTrue(any("src/lib.rs:" in problem for problem in problems), problems)
        self.assertTrue(any("Cargo.toml:" in problem for problem in problems), problems)

    def test_inline_edge_and_out_of_line_modules_inherit_source_ownership(self) -> None:
        owner = layer("control_plane") + '''
#[cfg_attr(nervix_lint, nervix::layer(edge, reason="session adapter"))]
mod adapter { use syntax as parsed; mod nested; }
mod worker;
'''
        problems = self.check(owner, {
            "src/adapter/nested.rs": "use syntax::Token;",
            "src/worker.rs": "#[cfg(any())]\nextern crate syntax as parser;",
        })
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("src/worker.rs:", problems[0])

    def test_edge_language_harness_and_plain_vocabulary_are_allowed(self) -> None:
        for kind in ("edge", "language", "harness"):
            self.assertEqual(self.check(layer(kind) + "use syntax::{Token as Input};"), [])
        self.assertEqual(self.check(layer("engine") + "use values::Plan;", {
            "syntax/src/lib.rs": layer("vocabulary") + "pub struct Plan;",
        }, dependencies='values = { package = "syntax-engine", path = "syntax" }'), [])

    def test_a_package_of_tests_is_checked_from_its_test_targets(self) -> None:
        manifest = (
            '[package]\nname="suite"\nversion="0.1.0"\n'
            '[[test]]\nname="suite"\npath="suite.rs"\n'
            '[dev-dependencies]\nsyntax = { package = "syntax-engine", path = "../syntax" }'
        )
        declared = {"tests/Cargo.toml": manifest, "tests/suite.rs": layer("harness") + "mod steps;\n",
                    "tests/steps.rs": "use syntax::Token;"}
        self.assertEqual(self.check(layer("engine"), declared, dependencies=""), [])
        undeclared = {**declared, "tests/suite.rs": "mod steps;\n"}
        problems = self.check(layer("engine"), undeclared, dependencies="")
        self.assertTrue(any("tests/steps.rs:" in problem for problem in problems), problems)
        self.assertTrue(any("tests/Cargo.toml:" in problem for problem in problems), problems)

    def test_a_test_target_of_a_package_with_a_library_is_not_a_root(self) -> None:
        manifest = (
            '[package]\nname="plain"\nversion="0.1.0"\n'
            '[[test]]\nname="plain"\npath="tests/plain.rs"\n'
            '[dev-dependencies]\nsyntax = { package = "syntax-engine", path = "../syntax" }'
        )
        files = {"plain/Cargo.toml": manifest, "plain/src/lib.rs": layer("harness"),
                 "plain/tests/plain.rs": "use syntax::Token;"}
        self.assertEqual(self.check(layer("engine"), files, dependencies=""), [])

    def test_comments_and_literals_do_not_declare_an_owner(self) -> None:
        problems = self.check(layer("engine") + '''
// #[cfg_attr(nervix_lint, nervix::layer(edge, reason="fake owner"))]
const TEXT: &str = "syntax::Token";
use syntax::Token;
''')
        self.assertEqual(len(problems), 2, problems)

    def test_raw_literal_contract_cannot_approve_an_inactive_import(self) -> None:
        problems = self.check(layer("engine") + '''
const TEXT: &str = r##"#[cfg_attr(nervix_lint, nervix::layer(edge, reason="fake owner"))]"##;
#[cfg(any())] use syntax::Token;
''')
        self.assertEqual(len(problems), 2, problems)

    def test_grouped_crate_renaming_keeps_import_provenance(self) -> None:
        problems = self.check(layer("engine") + "#[cfg(any())] use {syntax as grammar};")
        self.assertEqual(len(problems), 2, problems)

    def test_moving_source_keeps_the_same_contract_and_dependency_rule(self) -> None:
        for destination in ("relocated.rs", "new_tree/intake.rs"):
            problems = self.check(layer("engine") + f'#[path="{destination}"] mod module;', {
                "src/" + destination: layer("edge") + "use syntax::Token;",
            })
            self.assertEqual(problems, [])

    def test_target_specific_and_workspace_alias_dependencies_remain_visible(self) -> None:
        problems = self.check(layer("engine"), dependencies='''
[workspace.dependencies]
grammar = { package="syntax-engine", path="syntax" }
[target.'cfg(any())'.dependencies]
grammar = { workspace=true }
''')
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("grammar", problems[0])

    def test_explicit_path_in_a_non_root_file_keeps_the_actual_owner(self) -> None:
        problems = self.check(layer("edge") + "mod bridge;", {
            "src/bridge.rs": layer("engine") + '#[path="intake.rs"] mod worker;',
            "src/intake.rs": "#[cfg(any())] use syntax::Token;",
        })
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("src/intake.rs:", problems[0])

    def test_inline_path_override_controls_nested_source_provenance(self) -> None:
        problems = self.check(layer("edge") + '''
#[path="worker_files"]
#[cfg_attr(nervix_lint, nervix::layer(engine, reason="execution worker"))]
mod worker { #[path="intake.rs"] mod input; }
''', {"src/worker_files/intake.rs": "#[cfg(any())] use syntax::Token;"})
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("src/worker_files/intake.rs:", problems[0])
