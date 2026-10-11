"""Execute source-contract fixtures through the configured compiler wrapper."""

from __future__ import annotations

import os
import json
import pathlib
import subprocess
import tempfile
import unittest

from scripts.typed_ratchet import ROOT, TOOLCHAIN, Runner


class CompilerContractTests(unittest.TestCase):
    def test_stable_accepts_gated_contracts_and_expression_expectations(self) -> None:
        source = (ROOT / "tools/nervix-lint/fixtures/tests/semantics/expected_expression.rs").read_text()
        # Stable qualification needs stable metadata, so this syntax probe supplies a plain value.
        source = source.replace("nervix_lint_fixtures::MapAlias", "std::collections::BTreeMap<u32, u32>")
        source = source.replace("nervix_lint_fixtures::expect_lint!", "expect_lint!")
        source = source.replace("drop(value);", "assert!(value.is_none());")
        source = source.replace("//! A single acquisition expression carries its own reviewed exception.", "")
        source = (ROOT / "crates/primitives/src/lint.rs").read_text() + "\n" + source
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "contract.rs"
            path.write_text(source)
            compiler = subprocess.run(["rustup", "which", "rustc"], check=True,
                                      text=True, capture_output=True).stdout.strip()
            result = subprocess.run([os.environ.get("RUSTC_WRAPPER", "kache"), compiler,
                "--edition=2024", "--crate-type=lib", "--emit=metadata", "-Dwarnings",
                "--check-cfg=cfg(nervix_lint)", "--out-dir", directory, str(path)],
                text=True, capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr)

    def compile(self, name: str, *arguments: str, crate_name: str = "contract",
                source: pathlib.Path | None = None, source_root: pathlib.Path = ROOT) -> subprocess.CompletedProcess[str]:
        target = pathlib.Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target"))
        runner = Runner(ROOT, target)
        output = target / "typed-ratchet/contracts" / name
        output.mkdir(parents=True, exist_ok=True)
        env = dict(runner.environment)
        env.update({
            "NERVIX_LINT_ROOT": str(source_root),
            "NERVIX_LINT_IDENTITY": runner.identity,
            "NERVIX_LINT_CONFIGURATION": "compiler-contract",
            "NERVIX_LINT_REPORTS": str(output),
            "NERVIX_LINT_DRIVER": str(runner.driver),
            "KACHE_KEY_SALT": runner.identity + ":contract:" + name,
        })
        compiler = subprocess.run(["rustup", "which", "--toolchain", TOOLCHAIN, "rustc"],
                                  check=True, text=True, capture_output=True).stdout.strip()
        wrapper = env.get("RUSTC_WRAPPER", "kache")
        command = [wrapper, str(runner.wrapper), compiler, "--edition=2024", "--crate-name", crate_name,
                   "--crate-type=lib", "--emit=metadata", "--out-dir", str(output),
                   str(source or ROOT / "tools/nervix-lint/fixtures/tests/semantics" / (name + ".rs")),
                   *arguments]
        fixture = json.loads((target / "typed-ratchet/fixture-ordinary.json").read_text())
        metadata = next(path for entry in fixture["evidence"] for path in entry["files"]
                        if pathlib.Path(path).name.startswith("libnervix_lint_fixtures-") and path.endswith(".rmeta"))
        build = pathlib.Path(fixture["evidence"][0]["build_directory"])
        command.extend(["--extern", "nervix_lint_fixtures=" + metadata, "-L", "dependency=" + str(build / "debug/deps")])
        directories = {path.parent for path in (build / "debug").rglob("lib*.rmeta")}
        directories.update(path.parent for path in (build / "debug").rglob("lib*.so"))
        for directory in sorted(directories):
            command.extend(["-L", "dependency=" + str(directory)])
        for report in fixture["evidence"][0]["reports"].values():
            for index, argument in enumerate(report["arguments"][:-1]):
                if argument == "-L":
                    command.extend(["-L", report["arguments"][index + 1]])
        return subprocess.run(command, env=env, cwd=ROOT, text=True, capture_output=True)

    def test_recurring_caller_cannot_enter_lifecycle_contract(self) -> None:
        result = self.compile("hot_calls_lifecycle")
        self.assertNotEqual(result.returncode, 0, result.stderr)
        self.assertIn("nervix::lifecycle_call", result.stderr)

    def test_typed_error_and_outcome_diagnostics(self) -> None:
        for name, diagnostic in (
            ("error_nested_alias", "nervix::bare_error_signature"),
            ("error_discarded_named", "nervix::discarded_outcome"),
            ("error_unwrap_ufcs", "nervix::bare_panic"),
            ("error_associated", "nervix::bare_error_signature"),
            ("error_stream", "nervix::bare_error_signature"),
            ("error_closure", "nervix::bare_error_signature"),
            ("error_generic", "nervix::bare_error_signature"),
            ("error_async_block", "nervix::bare_error_signature"),
            ("error_async_closure", "nervix::bare_error_signature"),
            ("error_boxed_failure", "nervix::bare_error_signature"),
            ("error_borrowed_failure", "nervix::bare_error_signature"),
            ("error_optional_report", "nervix::bare_error_signature"),
            ("error_discard_qualified", "nervix::discarded_outcome"),
            ("error_discard_wrapper", "nervix::discarded_outcome"),
            ("error_discard_box", "nervix::discarded_outcome"),
            ("error_discard_vec", "nervix::discarded_outcome"),
            ("error_discard_assignment", "nervix::discarded_outcome"),
            ("error_nested_contract", "nervix::bare_error_signature"),
            ("error_discard_partial", "nervix::discarded_outcome"),
            ("error_panic_deref", "nervix::bare_panic"),
            ("error_panic_alias", "nervix::bare_panic"),
            ("error_boundary_malformed", "nervix::invalid_contract"),
            ("error_boundary_broad", "nervix::invalid_contract"),
            ("error_expectation_unfulfilled", "expectation is unfulfilled"),
        ):
            with self.subTest(name=name):
                result = self.compile(name)
                self.assertNotEqual(result.returncode, 0, result.stderr)
                self.assertIn(diagnostic, result.stderr.replace("-", "_"))

        for name in ("error_custom_apis", "error_handled", "error_reported", "error_outcomes", "error_expected", "error_storage_values", "error_resource_lifetimes", "error_return_contract"):
            with self.subTest(name=name):
                result = self.compile(name)
                self.assertEqual(result.returncode, 0, result.stderr)

    def test_error_classifications_cross_crate_metadata(self) -> None:
        target = pathlib.Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target"))
        for callee, caller, accepted in (("error_metadata_callee", "error_metadata_caller", True),
                                         ("error_metadata_failure", "error_metadata_rejected", False)):
            with self.subTest(callee=callee):
                result = self.compile(callee, crate_name="callee")
                self.assertEqual(result.returncode, 0, result.stderr)
                metadata = target / "typed-ratchet/contracts" / callee / "libcallee.rmeta"
                result = self.compile(caller, "--extern=callee=" + str(metadata))
                if accepted:
                    self.assertEqual(result.returncode, 0, result.stderr)
                else:
                    self.assertNotEqual(result.returncode, 0, result.stderr)
                    self.assertIn("nervix::bare_error_signature", result.stderr.replace("-", "_"))

    def test_source_contracts_and_narrow_expectations(self) -> None:
        for name in ("lifecycle_lock", "bounded_lock", "expected_lock", "expected_expression", "inherited_override", "contracted_dispatch", "contracted_trait", "bounded_task_body", "lifecycle_task_body"):
            with self.subTest(name=name):
                result = self.compile(name)
                self.assertEqual(result.returncode, 0, result.stderr)
        for name, diagnostic in (
            ("recurring_lock", "nervix::sync_acquisition"),
            ("inherited_recurring", "nervix::sync_acquisition"),
            ("helper_effect", "nervix::sync_acquisition"),
            ("inherited_lifecycle_helper", "nervix::sync_acquisition"),
            ("task_body", "nervix::sync_acquisition"),
            ("iterator_effect", "nervix::sync_acquisition"),
            ("callback_effect", "nervix::sync_acquisition"),
            ("generic_trait", "nervix::sync_acquisition"),
            ("unrelated_acquisition", "nervix::sync_acquisition"),
            ("unfulfilled_expectation", "expectation is unfulfilled"),
            ("unfulfilled_macro", "expectation is unfulfilled"),
            ("wide_binding", "distinct operations"),
            ("wide_macro", "distinct operations"),
            ("broad_expectation", "blanket suppression"),
            ("undocumented_expectation", "reason-bearing expect"),
            ("blanket_suppression", "blanket suppression"),
            ("malformed_contract", "bounded protocols require a key"),
            ("conflicting_contract", "conflicting context contracts"),
            ("conflicting_dispatch", "conflicting dispatch contracts"),
            ("trait_override", "cannot weaken"),
            ("trait_override_outside", "cannot weaken"),
            ("misplaced_contract", "context belongs on"),
            ("parameter_contract", "context belongs on"),
            ("generic_parameter_contract", "context belongs on"),
            ("variant_contract", "context belongs on"),
            ("ambiguous_task_binding", "binding owning one anonymous body"),
            ("unknown_annotation", "unknown Nervix source annotation"),
            ("unknown_dispatch", "nervix::unknown_effect"),
            ("generic_callback", "nervix::unknown_effect"),
            ("dynamic_trait", "nervix::unknown_effect"),
            ("warnings_suppression", "blanket suppression"),
            ("warnings_expectation", "blanket suppression"),
            ("hot_creates_lifecycle_task", "nervix::lifecycle_call"),
        ):
            with self.subTest(name=name):
                result = self.compile(name)
                self.assertNotEqual(result.returncode, 0, result.stderr)
                self.assertIn(diagnostic, result.stderr)

    def test_warn_and_deny_use_normal_lint_levels(self) -> None:
        result = self.compile("recurring_lock", "-Wnervix::sync_acquisition")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("warning:", result.stderr)
        self.assertIn("sync-acquisition", result.stderr)
        result = self.compile("recurring_lock", "-Dnervix::sync_acquisition")
        self.assertNotEqual(result.returncode, 0, result.stderr)

    def test_command_line_suppression_cannot_disable_the_gate(self) -> None:
        for level in ("-Anervix::sync_acquisition", "-Awarnings", "--cap-lints=allow", "--cap-lints=warn"):
            with self.subTest(level=level):
                result = self.compile("recurring_lock", level)
                self.assertNotEqual(result.returncode, 0, result.stderr)
                self.assertIn("rejects lint caps and blanket allow", result.stderr)

    def test_cross_crate_metadata_and_renamed_import(self) -> None:
        result = self.compile("cross_crate_callee", crate_name="callee")
        self.assertEqual(result.returncode, 0, result.stderr)
        target = pathlib.Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target"))
        metadata = target / "typed-ratchet/contracts/cross_crate_callee/libcallee.rmeta"
        argument = "--extern=callee=" + str(metadata)
        result = self.compile("cross_crate_caller", argument)
        self.assertNotEqual(result.returncode, 0, result.stderr)
        self.assertIn("nervix::lifecycle_call", result.stderr)
        result = self.compile("cross_crate_expected", argument)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_changed_caller_callee_and_moved_source_are_reconsidered(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            source = root / "owner.rs"
            cold = '''
#![cfg_attr(nervix_lint, nervix::context(lifecycle, reason = "installation default"))]
pub fn helper(map: &nervix_lint_fixtures::MapAlias) { drop(map.get(&1)); }
pub fn caller(map: &nervix_lint_fixtures::MapAlias) { helper(map); }
'''
            source.write_text(cold)
            result = self.compile("changed_source", source=source, source_root=root)
            self.assertEqual(result.returncode, 0, result.stderr)
            source.write_text(cold.replace("pub fn caller", '#[cfg_attr(nervix_lint, nervix::context(recurring, reason = "handles each batch"))]\npub fn caller'))
            result = self.compile("changed_source", source=source, source_root=root)
            self.assertNotEqual(result.returncode, 0, result.stderr)
            self.assertIn("nervix::sync_acquisition", result.stderr)
            moved = root / "relocated.rs"
            source.rename(moved)
            moved.write_text(moved.read_text().replace("helper", "renamed_helper"))
            result = self.compile("changed_source", source=moved, source_root=root)
            self.assertNotEqual(result.returncode, 0, result.stderr)
            self.assertIn("nervix::sync_acquisition", result.stderr)
            self.assertIn("relocated.rs", result.stderr)

            callee = root / "callee.rs"
            declaration = '#[cfg_attr(nervix_lint, nervix::context(lifecycle, reason = "publishes a lifetime"))]\npub fn install() {}\n'
            callee.write_text(declaration)
            result = self.compile("changed_callee", crate_name="changed_callee", source=callee, source_root=root)
            self.assertEqual(result.returncode, 0, result.stderr)
            target = pathlib.Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target"))
            metadata = target / "typed-ratchet/contracts/changed_callee/libchanged_callee.rmeta"
            caller = root / "caller.rs"
            caller.write_text('#[cfg_attr(nervix_lint, nervix::context(recurring, reason = "handles each batch"))]\npub fn run() { use changed_callee::install as renamed; renamed(); }\n')
            argument = "--extern=changed_callee=" + str(metadata)
            result = self.compile("changed_caller", argument, source=caller, source_root=root)
            self.assertNotEqual(result.returncode, 0, result.stderr)
            self.assertIn("nervix::lifecycle_call", result.stderr)
            callee.write_text(declaration.replace("lifecycle", "recurring"))
            result = self.compile("changed_callee", crate_name="changed_callee", source=callee, source_root=root)
            self.assertEqual(result.returncode, 0, result.stderr)
            result = self.compile("changed_caller", argument, source=caller, source_root=root)
            self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
