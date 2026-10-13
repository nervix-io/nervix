"""Execute the audit fixtures through the configured compiler wrapper and check their candidates.

Each fixture pairs patterns the audit reports with controls that differ in the resolved fact the
audit relies on: the collection's owner, the key's value, branch reachability, the failure's
information or the forwarded arguments.
"""

from __future__ import annotations

import json
import os
import pathlib
import unittest
from typing import Any

from scripts.tests import compiler_contract_checks
from scripts.typed_ratchet import ROOT


class CompilerAuditTests(unittest.TestCase):
    # Reuse the contract harness's wrapper invocation without collecting its tests here.
    compile = compiler_contract_checks.CompilerContractTests.compile

    def candidates(self, name: str) -> list[dict[str, Any]]:
        result = self.compile(name)
        self.assertEqual(result.returncode, 0, result.stderr)
        target = pathlib.Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target"))
        report = json.loads((target / "typed-ratchet/contracts" / name / "contract.json").read_text())
        self.assertTrue(report["complete"])
        return report["audit"]

    @staticmethod
    def by_owner(candidates: list[dict[str, Any]], kind: str) -> dict[str, list[dict[str, Any]]]:
        owners: dict[str, list[dict[str, Any]]] = {}
        for candidate in candidates:
            if candidate["pattern"]["kind"] == kind:
                owners.setdefault(candidate["owner"].split("::")[-1], []).append(candidate)
        return owners

    def test_repeated_lookups_follow_receiver_key_and_branches(self) -> None:
        candidates = self.candidates("audit_lookups")
        lookups = self.by_owner(candidates, "repeated_lookup")
        # Different keys, exclusive branches, a changed key, another collection and a custom
        # type with map-like names are controls.
        self.assertEqual(set(lookups), {"admit", "total", "shared", "qualified"})
        admit = lookups["admit"][0]
        self.assertEqual(admit["pattern"]["collection"], "alloc::collections::btree::map::BTreeMap")
        self.assertEqual(admit["pattern"]["receiver"], "self.panes")
        self.assertEqual(admit["pattern"]["key"], "pane")
        self.assertTrue(admit["pattern"]["guarded"])
        self.assertEqual([operation["operation"] for operation in admit["pattern"]["operations"]], ["get_mut", "insert"])
        self.assertEqual([operation["span"]["source"] for operation in admit["pattern"]["operations"]],
                         ["self.panes.get_mut(&pane)", "self.panes.insert(pane, values)"])
        self.assertEqual(admit["context"]["context"], "recurring")
        total = lookups["total"][0]["pattern"]
        self.assertEqual([operation["operation"] for operation in total["operations"]], ["contains_key", "index"])
        self.assertTrue(total["guarded"])
        shared = lookups["shared"][0]
        self.assertEqual(shared["pattern"]["collection"], "dashmap::DashMap")
        self.assertEqual(shared["context"]["context"], "lifecycle")
        self.assertEqual(lookups["qualified"][0]["pattern"]["collection"], "std::collections::hash::map::HashMap")
        duplicated = self.by_owner(candidates, "duplicated_branch")
        self.assertEqual(list(duplicated), ["admit"])
        self.assertEqual(duplicated["admit"][0]["pattern"]["statement"], "values.push(value)")
        self.assertEqual(len(duplicated["admit"][0]["pattern"]["occurrences"]), 2)

    def test_key_allocations_and_borrowed_copies(self) -> None:
        candidates = self.candidates("audit_allocations")
        allocations = self.by_owner(candidates, "key_allocation")
        self.assertEqual(set(allocations), {"formatted", "counted"})
        formatted = allocations["formatted"][0]["pattern"]
        self.assertEqual((formatted["operation"], formatted["allocation"], formatted["key_type"]),
                         ("contains_key", "to_string", "std::string::String"))
        counted = allocations["counted"][0]["pattern"]
        self.assertEqual((counted["operation"], counted["allocation"]), ("entry", "clone"))
        copies = self.by_owner(candidates, "borrowed_copy")
        self.assertEqual(set(copies), {"measured"})
        # Formatting a number is not a copy; converting borrowed text into a String is, whether
        # the text is a `str` or a `Cow` of one.
        self.assertEqual(sorted(candidate["pattern"]["operation"] for candidate in copies["measured"]),
                         ["clone", "to_owned", "to_string", "to_string"])
        self.assertEqual(sorted(candidate["pattern"]["value_type"] for candidate in copies["measured"]
                                if candidate["pattern"]["operation"] == "to_string"),
                         ["std::string::String", "std::string::String"])
        self.assertNotIn("borrowed", {owner for kind in ("repeated_lookup", "discarded_error", "disguised_clamp")
                                      for owner in self.by_owner(candidates, kind)})

    def test_discarded_errors_and_disguised_clamps(self) -> None:
        candidates = self.candidates("audit_errors")
        discarded = self.by_owner(candidates, "discarded_error")
        # A failure kept by its arm or cause, and zero-sized failures, are controls.
        self.assertEqual(set(discarded), {"dropped"})
        operations = sorted(candidate["pattern"]["operation"] for candidate in discarded["dropped"])
        self.assertEqual(operations, ["Result::map_err", "Result::ok", "Result::unwrap_or_default",
                                      "if let Ok", "let Ok else", "match Err(_)"])
        for candidate in discarded["dropped"]:
            self.assertEqual(candidate["pattern"]["error_type"], "std::num::ParseIntError")
            self.assertEqual(candidate["pattern"]["information"], "value")
        clamps = self.by_owner(candidates, "disguised_clamp")
        self.assertEqual(set(clamps), {"clamped"})
        self.assertEqual(sorted((candidate["pattern"]["operation"], candidate["pattern"]["fallback"])
                                for candidate in clamps["clamped"]),
                         [("checked_sub", "0"), ("try_from", "u64::MAX")])

    def test_duplicated_branches_require_every_exhaustive_branch(self) -> None:
        candidates = self.candidates("audit_branches")
        duplicated = self.by_owner(candidates, "duplicated_branch")
        # Type-dispatched arms and constructed values are controls.
        self.assertEqual(set(duplicated), {"record", "classify"})
        record = duplicated["record"][0]["pattern"]
        self.assertEqual((record["construct"], record["statement"]), ("if", "self.entries.push(value)"))
        classify = duplicated["classify"][0]["pattern"]
        self.assertEqual((classify["construct"], len(classify["occurrences"])), ("match", 3))

    def test_forwarding_wrappers_pass_parameters_unchanged(self) -> None:
        candidates = self.candidates("audit_wrappers")
        wrappers = self.by_owner(candidates, "forwarding_wrapper")
        # Transformed, reordered and library-forwarding bodies and a newtype constructor are
        # controls.
        self.assertEqual(set(wrappers), {"push", "combined"})
        push = wrappers["push"][0]["pattern"]
        self.assertTrue(push["callee"].endswith("::append"))
        self.assertEqual((push["callers"], push["implementation"], push["relocated"], push["contract"]),
                         (1, False, False, False))
        combined = wrappers["combined"][0]["pattern"]
        self.assertTrue(combined["callee"].endswith("::combine"))
        self.assertEqual(combined["callers"], 1)


if __name__ == "__main__":
    unittest.main()
