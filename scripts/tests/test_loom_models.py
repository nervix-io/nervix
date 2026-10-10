from __future__ import annotations

import io
import json
import os
import unittest
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import asdict
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
    Shard,
    Weakening,
    completion,
    copy_working_tree,
    exploration_bounds,
    listed_tests,
    parse_inventory,
    qualify,
    qualify_one,
    qualification_failure,
    replay,
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
        self.environments: list[dict[str, str]] = []

    def run(
        self,
        arguments: Sequence[str],
        *,
        environment: Mapping[str, str] | None = None,
        cwd: Path | None = None,
        echo: bool = True,
    ) -> Outcome:
        self.commands.append(list(arguments))
        self.environments.append(dict(environment or {}))
        return self.respond(arguments)


def listing(*tests: str) -> Outcome:
    return Outcome(0, "".join(f"{test}: test\n" for test in tests))


SECOND_WEAKENING = (
    "\n[[qualification]]\n"
    'id = "execution.cancellation.relaxed-observation"\n'
    'invariant = "execution.cancellation.publication"\n'
    'path = "crates/execution/src/cancellation.rs"\n'
    'original = "load(Ordering::Acquire)"\n'
    'weakened = "load(Ordering::Relaxed)"\n'
    'failure = "observed its cancellation without the write"\n'
)


class QualificationTests(unittest.TestCase):
    def test_a_successful_qualification_retains_its_counterexample_and_replay(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "Cargo.lock").write_text(
                '[[package]]\nname = "loom"\nversion = "0.7.2"\n', encoding="utf-8"
            )
            manifest = root / "copy/Cargo.toml"
            manifest.parent.mkdir()
            manifest.write_text("[workspace]\n", encoding="utf-8")
            target = root / "target"
            registered = inventory()
            qualification = registered.qualifications[0]
            directory = target / "loom-qualification" / qualification.id
            checkpoint_value = '{"pos": 0}\n'
            replay_value = '{"pos": 1}\n'
            bounds = "preemption bound: none, branch limit: 1000, thread limit: 5"
            output = (
                "Running unittests src/lib.rs\n"
                f"nervix-model-harness: exploring loom invariant {qualification.invariant} "
                f"to exhaustion ({bounds})\n"
                f"thread panicked: {qualification.failure}\n"
            )

            def respond(arguments: Sequence[str]) -> Outcome:
                if list(arguments) == ["git", "rev-parse", "HEAD"]:
                    return Outcome(0, "f" * 40 + "\n")
                if list(arguments) == ["git", "status", "--porcelain"]:
                    return Outcome(0, "")
                if list(arguments) == ["rustc", "-vV"]:
                    return Outcome(0, "rustc qualification-fixture\n")
                self.assertEqual(arguments[0], "cargo")
                checkpoint = Path(commands.environments[-1]["LOOM_CHECKPOINT_FILE"])
                if checkpoint.name == "checkpoint.json":
                    checkpoint.write_text(checkpoint_value, encoding="utf-8")
                else:
                    self.assertEqual(checkpoint.read_text(encoding="utf-8"), checkpoint_value)
                    checkpoint.write_text(replay_value, encoding="utf-8")
                return Outcome(101, output)

            commands = ScriptedCommands(root, respond)
            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                problem = qualify_one(
                    commands, registered, target, qualification, manifest,
                    {"CARGO_TARGET_DIR": str(root / "build")},
                )
            self.assertIsNone(problem)
            self.assertTrue(
                (directory / "checkpoint.json").is_file(), "successful counterexample was discarded"
            )
            self.assertEqual((directory / "checkpoint.json").read_text(), checkpoint_value)
            self.assertEqual((directory / "replay-checkpoint.json").read_text(), replay_value)
            self.assertEqual((directory / "output.log").read_text(), output)
            self.assertEqual((directory / "replay.log").read_text(), output)
            metadata = json.loads((directory / "metadata.json").read_text())
            self.assertEqual(metadata["qualification"], asdict(qualification))
            self.assertEqual(metadata["qualification_status"], "passed")
            self.assertEqual(metadata["revision"], "f" * 40)
            self.assertFalse(metadata["working_tree_modified"])
            self.assertEqual(metadata["loom"], "0.7.2")
            self.assertEqual(metadata["test"], PUBLICATION_TEST)
            self.assertEqual(metadata["exploration"], bounds)
            self.assertEqual(metadata["checkpoint"], "checkpoint.json")
            self.assertEqual(metadata["exit_status"], 101)
            self.assertEqual(metadata["checkpoint_replay"]["exit_status"], 101)
            self.assertEqual(metadata["checkpoint_replay"]["checkpoint"], "replay-checkpoint.json")
            self.assertEqual(metadata["checkpoint_replay"]["command"], metadata["command"])

    def qualify_tree(
        self, inventory_text: str, shard: Shard
    ) -> tuple[int, list[str], ScriptedCommands, Path]:
        """Qualifies a tree whose one source both weakenings apply to, and returns the status, the
        source each build saw, the commands and the build directory."""

        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        (root / "Cargo.toml").write_text("[workspace]\n", encoding="utf-8")
        source = root / "crates/execution/src/cancellation.rs"
        source.parent.mkdir(parents=True)
        source.write_text("store(true, Ordering::Release) load(Ordering::Acquire)", encoding="utf-8")
        for generated in GENERATED_INPUTS:
            (root / generated).mkdir(parents=True)
        build = root / "target/loom-qualification-build"
        copied = build / "tree/crates/execution/src/cancellation.rs"
        built: list[str] = []

        def respond(arguments: Sequence[str]) -> Outcome:
            if arguments[:2] == ["git", "ls-files"]:
                return Outcome(0, "Cargo.toml\0crates/execution/src/cancellation.rs\0")
            built.append(copied.read_text(encoding="utf-8"))
            return Outcome(0, "     Running unittests src/lib.rs\n")

        commands = ScriptedCommands(root, respond)
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            status = qualify(commands, parse_inventory(inventory_text), root / "target", shard)
        return status, built, commands, build

    def test_each_weakening_builds_apart_from_restored_source_and_leaves_no_copy(self) -> None:
        status, built, commands, build = self.qualify_tree(INVENTORY + SECOND_WEAKENING, Shard(1, 1))

        self.assertEqual(status, 1)
        self.assertEqual(
            built,
            [
                "store(true, Ordering::Relaxed) load(Ordering::Acquire)",
                "store(true, Ordering::Release) load(Ordering::Relaxed)",
            ],
        )
        self.assertFalse((build / "tree").exists())
        builds = [
            (command, environment)
            for command, environment in zip(commands.commands, commands.environments)
            if command[0] == "cargo"
        ]
        self.assertEqual(len(builds), 2)
        for command, environment in builds:
            self.assertEqual(command[command.index("--profile") + 1], "loom")
            self.assertEqual(environment["CARGO_TARGET_DIR"], str(build / "target"))
            self.assertEqual(environment["CARGO_INCREMENTAL"], "1")

    def test_shards_split_the_distinct_weakenings(self) -> None:
        _, first, _, _ = self.qualify_tree(INVENTORY + SECOND_WEAKENING, Shard(1, 2))
        _, second, _, _ = self.qualify_tree(INVENTORY + SECOND_WEAKENING, Shard(2, 2))

        self.assertEqual(first, ["store(true, Ordering::Relaxed) load(Ordering::Acquire)"])
        self.assertEqual(second, ["store(true, Ordering::Release) load(Ordering::Relaxed)"])

    def test_a_shard_that_selects_no_weakening_fails(self) -> None:
        with self.assertRaisesRegex(RunnerError, "selects none of the 1 weakenings"):
            self.qualify_tree(INVENTORY, Shard(2, 2))

    def test_a_shard_is_a_number_of_a_count(self) -> None:
        self.assertEqual(Shard.parse("2/3"), Shard(2, 3))
        for text in ("0/2", "3/2", "1/0", "1", "a/b", "/2"):
            with self.subTest(text=text), self.assertRaises(RunnerError):
                Shard.parse(text)


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
            self.assertIn("--profile", arguments)
            self.assertEqual(arguments[arguments.index("--profile") + 1], "loom")
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
        self.assertFalse((failures / "execution.cancellation.publication").exists())
        self.assertFalse((failures / "execution.cancellation.disarm").exists())

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
        evidence = (
            target / "loom-failures" / "nervix-execution" / "execution.cancellation.publication"
        )
        metadata = json.loads((evidence / "metadata.json").read_text(encoding="utf-8"))
        self.assertEqual(metadata["invariant"], "execution.cancellation.publication")
        self.assertEqual(metadata["test"], PUBLICATION_TEST)
        self.assertEqual(metadata["revision"], "0123abcd")
        self.assertEqual(metadata["loom"], "0.7.2")
        self.assertTrue((evidence / "output.log").is_file())


class ReplayTests(unittest.TestCase):
    def test_replay_uses_the_model_profile_and_preserves_the_recorded_checkpoint(self) -> None:
        with TemporaryDirectory() as temporary:
            directory = Path(temporary)
            checkpoint = directory / "checkpoint.json"
            checkpoint.write_text('{"pos": 0}\n', encoding="utf-8")
            (directory / "metadata.json").write_text(
                json.dumps({"invariant": "execution.cancellation.publication"}),
                encoding="utf-8",
            )

            def respond(arguments: Sequence[str]) -> Outcome:
                self.assertIn("--profile", arguments)
                self.assertEqual(arguments[arguments.index("--profile") + 1], "loom")
                self.assertIn(PUBLICATION_TEST, arguments)
                self.assertIn("--exact", arguments)
                (directory / "replay-checkpoint.json").write_text(
                    '{"pos": 1}\n', encoding="utf-8"
                )
                return Outcome(101, "the recorded assertion failed\n")

            commands = ScriptedCommands(directory, respond)
            with redirect_stderr(io.StringIO()):
                status = replay(commands, inventory(), directory)

            self.assertEqual(status, 101)
            self.assertEqual(checkpoint.read_text(encoding="utf-8"), '{"pos": 0}\n')
            self.assertEqual(
                (directory / "replay-checkpoint.json").read_text(encoding="utf-8"),
                '{"pos": 1}\n',
            )


if __name__ == "__main__":
    unittest.main()
