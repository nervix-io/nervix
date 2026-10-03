from __future__ import annotations

import io
import json
import os
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Callable, Mapping, Sequence

from scripts.loom_models import (
    GENERATED_INPUTS,
    Commands,
    Discovery,
    Inventory,
    Outcome,
    RunnerError,
    Weakening,
    completion,
    copy_working_tree,
    exploration_bounds,
    listed_tests,
    parse_inventory,
    qualification_failure,
    run_models,
    select,
    weaken,
    weakenings,
)

PUBLICATION_TEST = "cancellation::loom_models::loom_publication"
DISARM_TEST = "cancellation::loom_models::loom_disarm"

INVENTORY = f"""
[[invariant]]
id = "execution.cancellation.publication"
package = "nervix-execution"
test = "{PUBLICATION_TEST}"
claim = "A job that observes cancellation observes what was written before it."

[[invariant]]
id = "execution.cancellation.disarm"
package = "nervix-execution"
test = "{DISARM_TEST}"
claim = "A disarmed obligation never cancels."

[[qualification]]
id = "execution.cancellation.relaxed-cancel"
invariant = "execution.cancellation.publication"
path = "crates/execution/src/cancellation.rs"
original = "store(true, Ordering::Release)"
weakened = "store(true, Ordering::Relaxed)"
failure = "observed its cancellation without the write"
"""


def inventory() -> Inventory:
    return parse_inventory(INVENTORY)


def completed(invariant_id: str, executions: int = 6) -> str:
    bounds = "preemption bound: none, branch limit: 1000, thread limit: 5"
    return (
        f"nervix-model-harness: exploring loom invariant {invariant_id} to exhaustion ({bounds})\n"
        f"nervix-model-harness: loom invariant {invariant_id} explored to exhaustion in "
        f"{executions} executions ({bounds})\n"
    )


class InventoryTests(unittest.TestCase):
    def test_the_inventory_registers_invariants_and_qualifications(self) -> None:
        parsed = inventory()
        self.assertEqual(parsed.packages(), ["nervix-execution"])
        self.assertEqual(
            parsed.invariant("execution.cancellation.disarm").test, DISARM_TEST
        )
        self.assertEqual(len(parsed.qualifications), 1)

    def test_an_invariant_registered_twice_is_refused(self) -> None:
        duplicated = INVENTORY + (
            "\n[[invariant]]\n"
            'id = "execution.cancellation.publication"\n'
            'package = "nervix-execution"\n'
            'test = "cancellation::loom_models::loom_again"\n'
            'claim = "The same invariant again."\n'
        )
        with self.assertRaisesRegex(RunnerError, "registered twice"):
            parse_inventory(duplicated)

    def test_a_malformed_invariant_name_is_refused(self) -> None:
        with self.assertRaisesRegex(RunnerError, "dot-separated words"):
            parse_inventory(INVENTORY.replace("execution.cancellation.disarm", "Disarm"))

    def test_a_test_not_named_as_a_model_is_refused(self) -> None:
        with self.assertRaisesRegex(RunnerError, "not named `loom_"):
            parse_inventory(INVENTORY.replace(DISARM_TEST, "cancellation::disarm"))

    def test_a_qualification_of_an_unknown_invariant_is_refused(self) -> None:
        with self.assertRaisesRegex(RunnerError, "unknown invariant"):
            parse_inventory(
                INVENTORY.replace(
                    'invariant = "execution.cancellation.publication"',
                    'invariant = "execution.cancellation.missing"',
                )
            )

    def test_a_missing_field_names_its_entry(self) -> None:
        with self.assertRaisesRegex(RunnerError, "invariant execution.cancellation.disarm needs"):
            parse_inventory(INVENTORY.replace('claim = "A disarmed obligation never cancels."', ""))

    def test_an_empty_inventory_is_refused(self) -> None:
        with self.assertRaisesRegex(RunnerError, "registers no invariant"):
            parse_inventory("")


class SelectionTests(unittest.TestCase):
    def discovery(
        self, models: Sequence[str] = (PUBLICATION_TEST, DISARM_TEST), ignored: Sequence[str] = ()
    ) -> list[Discovery]:
        return [Discovery("nervix-execution", tuple(models), tuple(ignored))]

    def test_listed_tests_are_the_terse_test_lines(self) -> None:
        output = (
            "   Compiling nervix-execution v0.1.0\n"
            f"{PUBLICATION_TEST}: test\n"
            "workers::tests::ordinary: test\n"
            "benches::relay: benchmark\n"
        )
        self.assertEqual(listed_tests(output), [PUBLICATION_TEST, "workers::tests::ordinary"])

    def test_the_whole_gate_selects_every_registered_model(self) -> None:
        selected = select(inventory(), self.discovery(), "")
        self.assertEqual([model.test for model in selected], [PUBLICATION_TEST, DISARM_TEST])

    def test_a_registered_model_that_is_not_discovered_fails_the_gate(self) -> None:
        with self.assertRaisesRegex(RunnerError, "disarm is not discovered"):
            select(inventory(), self.discovery(models=[PUBLICATION_TEST]), "")

    def test_an_ignored_registered_model_fails_the_gate(self) -> None:
        with self.assertRaisesRegex(RunnerError, "execution.cancellation.disarm is ignored"):
            select(inventory(), self.discovery(ignored=[DISARM_TEST]), "")

    def test_an_unregistered_model_fails_the_gate(self) -> None:
        unregistered = "cancellation::loom_models::loom_unregistered"
        with self.assertRaisesRegex(RunnerError, "loom_unregistered is not registered"):
            select(
                inventory(),
                self.discovery(models=[PUBLICATION_TEST, DISARM_TEST, unregistered]),
                "",
            )

    def test_a_filter_selects_by_test_name_or_invariant(self) -> None:
        by_test = select(inventory(), self.discovery(), "loom_disarm")
        by_invariant = select(inventory(), self.discovery(), "cancellation.publication")
        self.assertEqual([model.test for model in by_test], [DISARM_TEST])
        self.assertEqual([model.test for model in by_invariant], [PUBLICATION_TEST])

    def test_a_filter_does_not_require_the_other_invariants(self) -> None:
        selected = select(inventory(), self.discovery(models=[DISARM_TEST]), "disarm")
        self.assertEqual([model.test for model in selected], [DISARM_TEST])

    def test_a_filter_that_selects_nothing_fails(self) -> None:
        with self.assertRaisesRegex(RunnerError, "no Loom model matching `absent`"):
            select(inventory(), self.discovery(), "absent")

    def test_a_filter_refuses_an_unregistered_model(self) -> None:
        unregistered = "cancellation::loom_models::loom_unregistered"
        with self.assertRaisesRegex(RunnerError, "not registered"):
            select(
                inventory(),
                self.discovery(models=[PUBLICATION_TEST, unregistered]),
                "unregistered",
            )


class RecordTests(unittest.TestCase):
    def test_completion_is_the_record_of_the_models_own_invariant(self) -> None:
        output = completed("execution.cancellation.publication", executions=17)
        record = completion(output, "execution.cancellation.publication")
        assert record is not None
        self.assertEqual(record.executions, 17)
        self.assertIn("preemption bound: none", record.bounds)
        self.assertIsNone(completion(output, "execution.cancellation.disarm"))

    def test_records_are_found_after_the_test_harness_prefix(self) -> None:
        output = f"test {PUBLICATION_TEST} ... " + completed("execution.cancellation.publication")
        self.assertEqual(
            exploration_bounds(output, "execution.cancellation.publication"),
            "preemption bound: none, branch limit: 1000, thread limit: 5",
        )
        self.assertIsNotNone(completion(output, "execution.cancellation.publication"))

    def test_a_resumed_search_is_not_a_completion(self) -> None:
        output = (
            "nervix-model-harness: loom invariant execution.cancellation.publication resumed from "
            "checkpoint target/checkpoint.json and explored 3 executions (preemption bound: none)\n"
        )
        self.assertIsNone(completion(output, "execution.cancellation.publication"))

    def test_a_weakening_applies_exactly_once(self) -> None:
        qualification = inventory().qualifications[0]
        source = "fn cancel(&self) { self.cancelled.store(true, Ordering::Release); }"
        self.assertIn("Ordering::Relaxed", weaken(source, qualification))
        with self.assertRaisesRegex(RunnerError, "occurs 0 times"):
            weaken("fn cancel(&self) {}", qualification)
        with self.assertRaisesRegex(RunnerError, "occurs 2 times"):
            weaken(source + source, qualification)

    def test_a_qualification_needs_the_model_to_fail_for_its_reason(self) -> None:
        qualification = inventory().qualifications[0]
        ran = "     Running unittests src/lib.rs\n"
        self.assertIsNone(
            qualification_failure(
                Outcome(101, ran + "panicked: the job observed its cancellation without the write"),
                qualification,
            )
        )
        self.assertIn("passed", qualification_failure(Outcome(0, ran), qualification) or "")
        self.assertIn(
            "did not build",
            qualification_failure(Outcome(101, "error[E0425]"), qualification) or "",
        )
        self.assertIn(
            "without reporting",
            qualification_failure(Outcome(101, ran + "panicked: other"), qualification) or "",
        )


class WeakeningTests(unittest.TestCase):
    def test_qualifications_that_apply_one_weakening_share_it_in_inventory_order(self) -> None:
        shared = (
            "\n[[qualification]]\n"
            'id = "execution.cancellation.relaxed-cancel-again"\n'
            'invariant = "execution.cancellation.disarm"\n'
            'path = "crates/execution/src/cancellation.rs"\n'
            'original = "store(true, Ordering::Release)"\n'
            'weakened = "store(true, Ordering::Relaxed)"\n'
            'failure = "a disarmed obligation cancelled its job"\n'
            "\n[[qualification]]\n"
            'id = "execution.cancellation.relaxed-observation"\n'
            'invariant = "execution.cancellation.publication"\n'
            'path = "crates/execution/src/cancellation.rs"\n'
            'original = "load(Ordering::Acquire)"\n'
            'weakened = "load(Ordering::Relaxed)"\n'
            'failure = "observed its cancellation without the write"\n'
        )
        grouped = weakenings(parse_inventory(INVENTORY + shared).qualifications)
        self.assertEqual(
            [weakening for weakening, _ in grouped],
            [
                Weakening(
                    path="crates/execution/src/cancellation.rs",
                    original="store(true, Ordering::Release)",
                    weakened="store(true, Ordering::Relaxed)",
                ),
                Weakening(
                    path="crates/execution/src/cancellation.rs",
                    original="load(Ordering::Acquire)",
                    weakened="load(Ordering::Relaxed)",
                ),
            ],
        )
        self.assertEqual(
            [[qualification.id for qualification in group] for _, group in grouped],
            [
                [
                    "execution.cancellation.relaxed-cancel",
                    "execution.cancellation.relaxed-cancel-again",
                ],
                ["execution.cancellation.relaxed-observation"],
            ],
        )


class CopyTests(unittest.TestCase):
    def working_tree(self, files: Mapping[str, str]) -> tuple[Path, Commands]:
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name) / "root"
        for relative, text in files.items():
            path = root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="utf-8")

        def respond(arguments: Sequence[str]) -> Outcome:
            self.assertEqual(arguments[:2], ["git", "ls-files"])
            return Outcome(0, "".join(f"{relative}\0" for relative in files))

        return root, ScriptedCommands(root, respond)

    def test_the_copy_gives_every_file_a_fresh_time_and_links_the_generated_inputs(self) -> None:
        root, commands = self.working_tree({"Cargo.toml": "[workspace]\n", "src/lib.rs": "//\n"})
        old = 1_000_000_000
        os.utime(root / "src/lib.rs", (old, old))
        for generated in GENERATED_INPUTS:
            (root / generated).mkdir(parents=True)
            (root / generated / "index.html").write_text("<html/>", encoding="utf-8")
        destination = root.parent / "copy"
        copy_working_tree(commands, destination)
        self.assertEqual((destination / "src/lib.rs").read_text(encoding="utf-8"), "//\n")
        self.assertGreater((destination / "src/lib.rs").stat().st_mtime, old)
        for generated in GENERATED_INPUTS:
            self.assertTrue((destination / generated).is_symlink())
            self.assertEqual(
                (destination / generated / "index.html").read_text(encoding="utf-8"), "<html/>"
            )

    def test_a_missing_generated_input_stops_the_copy(self) -> None:
        root, commands = self.working_tree({"Cargo.toml": "[workspace]\n"})
        with self.assertRaisesRegex(RunnerError, "is missing; build it"):
            copy_working_tree(commands, root.parent / "copy")


class ScriptedCommands(Commands):
    """Answers each command from `respond` and records what ran."""

    def __init__(self, root: Path, respond: Callable[[Sequence[str]], Outcome]) -> None:
        super().__init__(root)
        self.respond = respond
        self.commands: list[list[str]] = []

    def run(
        self,
        arguments: Sequence[str],
        *,
        environment: Mapping[str, str] | None = None,
        cwd: Path | None = None,
        echo: bool = True,
    ) -> Outcome:
        self.commands.append(list(arguments))
        return self.respond(arguments)


def listing(*tests: str) -> Outcome:
    return Outcome(0, "".join(f"{test}: test\n" for test in tests))


class RunTests(unittest.TestCase):
    def run_gate(self, respond: Callable[[Sequence[str]], Outcome]) -> tuple[int, str, Path]:
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        (root / "Cargo.lock").write_text(
            '[[package]]\nname = "loom"\nversion = "0.7.2"\n', encoding="utf-8"
        )
        commands = ScriptedCommands(root, respond)
        out = io.StringIO()
        with redirect_stdout(out), redirect_stderr(out):
            status = run_models(commands, inventory(), root / "target", "")
        return status, out.getvalue(), root / "target"

    def test_a_gate_passes_when_every_model_completes(self) -> None:
        def respond(arguments: Sequence[str]) -> Outcome:
            if "--ignored" in arguments:
                return listing()
            if "--list" in arguments:
                return listing(PUBLICATION_TEST, DISARM_TEST)
            if PUBLICATION_TEST in arguments:
                return Outcome(0, completed("execution.cancellation.publication"))
            return Outcome(0, completed("execution.cancellation.disarm", executions=1))

        status, report, target = self.run_gate(respond)
        self.assertEqual(status, 0, report)
        self.assertIn("discovered 2, selected 2, executed 2, completed 2", report)
        failures = target / "loom-failures" / "nervix-execution"
        self.assertFalse((failures / PUBLICATION_TEST).exists())
        self.assertFalse((failures / DISARM_TEST).exists())

    def test_a_model_that_passes_without_completing_fails_with_its_evidence(self) -> None:
        def respond(arguments: Sequence[str]) -> Outcome:
            if "--ignored" in arguments:
                return listing()
            if "--list" in arguments:
                return listing(PUBLICATION_TEST, DISARM_TEST)
            if arguments[:2] == ["git", "rev-parse"]:
                return Outcome(0, "0123abcd\n")
            if arguments[:2] in (["git", "status"], ["rustc", "-vV"]):
                return Outcome(0, "")
            if PUBLICATION_TEST in arguments:
                return Outcome(0, "test result: ok. 1 passed\n")
            return Outcome(0, completed("execution.cancellation.disarm", executions=1))

        status, report, target = self.run_gate(respond)
        self.assertEqual(status, 1)
        self.assertIn("completed 1", report)
        self.assertIn("passed without the record of an exhaustive exploration", report)
        evidence = target / "loom-failures" / "nervix-execution" / PUBLICATION_TEST
        metadata = json.loads((evidence / "metadata.json").read_text(encoding="utf-8"))
        self.assertEqual(metadata["invariant"], "execution.cancellation.publication")
        self.assertEqual(metadata["revision"], "0123abcd")
        self.assertEqual(metadata["loom"], "0.7.2")
        self.assertTrue((evidence / "output.log").is_file())


if __name__ == "__main__":
    unittest.main()
