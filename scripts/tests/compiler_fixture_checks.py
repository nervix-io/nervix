"""Compiler API fixtures, run by their prepared just recipes."""

from __future__ import annotations

import json
import os
import pathlib
import unittest

from scripts.typed_ratchet import ROOT

TARGET = pathlib.Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target"))


class CompilerFixtureTests(unittest.TestCase):
    def test_compiler_distinguishes_real_acquisitions(self) -> None:
        path = TARGET / "typed-ratchet/fixture-ordinary.json"
        self.assertTrue(path.is_file(), "run just test-typed-ratchet-compiler to collect actual compiler evidence")
        report = json.loads(path.read_text())
        self.assertTrue(report["complete"])
        owners = {}
        for site in report["findings"]:
            for finding in (finding for values in site["configurations"].values() for finding in values):
                owner = finding["owner"].split("::")[-1]
                owners.setdefault(owner, []).append(finding)
        self.assertEqual(len(owners.get("locked_collection", [])), 1)
        self.assertEqual(owners.get("owned_collections", []), [])
        self.assertEqual(owners.get("io_operations", []), [])
        self.assertEqual(owners.get("custom_methods", []), [])
        self.assertEqual(len(owners.get("shared_map_operations", [])), 11)
        self.assertEqual(len(owners.get("aliases_ufcs_and_deref", [])), 2)
        self.assertEqual(len(owners.get("access", [])), 1)
        self.assertEqual(len(owners.get("read_write_lock", [])), 4)
        self.assertEqual(len(owners.get("borrowed_iteration", [])), 1)
        self.assertEqual(len(owners.get("reserve_shared_map", [])), 1)
        self.assertEqual(owners.get("owned_iteration", []), [])
        expansions = [finding for values in owners.values() for finding in values if finding["expansion"]]
        self.assertTrue(expansions, "authored tokens passed through the external macro retain expansion provenance")

    def test_generated_acquisitions_are_explicit_exclusions(self) -> None:
        report = json.loads((TARGET / "typed-ratchet/fixture-ordinary.json").read_text())
        self.assertGreater(sum(item["excluded_generated"] for entry in report["evidence"] for item in entry["reports"].values()), 0)

class ModeledFixtureTests(unittest.TestCase):
    def test_selected_backends_preserve_authored_operations(self) -> None:
        for mode, expected in (("shuttle", 24), ("loom", 25), ("turmoil", 25)):
            with self.subTest(mode=mode):
                report = json.loads((TARGET / f"typed-ratchet/fixture-{mode}.json").read_text())
                self.assertTrue(report["complete"])
                self.assertEqual(len(report["findings"]), expected)
                findings = [finding for site in report["findings"] for values in site["configurations"].values() for finding in values]
                self.assertEqual(sum("::borrowed_iteration" in finding["owner"] for finding in findings), 1)
                if mode == "shuttle":
                    self.assertTrue(any(finding["receiver"].startswith("shuttle_dashmap_impl") for finding in findings))
                    self.assertTrue(any(finding["receiver"].startswith("nervix_primitives::collections::scheduled") for finding in findings))
