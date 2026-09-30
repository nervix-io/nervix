from __future__ import annotations

import io
import json
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Callable, Mapping, Sequence

from scripts.shuttle_checks import (
    COMPLETED,
    FORCED_FAILURE,
    REPLAY_CHECK_PACKAGE,
    REPLAY_CHECK_TEST,
    REPLAYED,
    Commands,
    Discovery,
    Inventory,
    Outcome,
    RunnerError,
    listed_tests,
    main,
    parse_inventory,
    replay,
    replay_check,
    run_checks,
    select,
)

WAIT_QUEUE = REPLAY_CHECK_TEST
CANCELLATION = "tests::shuttle_checks::shuttle_running_job_observes_cancellation"
FLUSH = "runtime::force_flush::shuttle_tests::shuttle_stale_completions_never_clear"

INVENTORY = f"""
[[package]]
name = "{REPLAY_CHECK_PACKAGE}"
checks = ["{WAIT_QUEUE}", "{CANCELLATION}"]

[[package]]
name = "nervix-server"
checks = ["{FLUSH}"]
"""


def inventory() -> Inventory:
    return parse_inventory(INVENTORY)


def completed_output() -> str:
    return (
        "running 1 test\n"
        f"{COMPLETED} random 100 of 100 schedules, PCT 100 of 100 schedules at depth 3 "
        "(step limit 10000)\n"
        "test result: ok. 1 passed; 0 failed\n"
    )


class InventoryTests(unittest.TestCase):
    def test_the_inventory_registers_checks_by_package(self) -> None:
        parsed = inventory()
        self.assertEqual(parsed.packages(), [REPLAY_CHECK_PACKAGE, "nervix-server"])
        self.assertTrue(parsed.is_registered("nervix-server", FLUSH))
        self.assertFalse(parsed.is_registered(REPLAY_CHECK_PACKAGE, FLUSH))

    def test_a_check_registered_twice_is_refused(self) -> None:
        with self.assertRaisesRegex(RunnerError, "registers .* twice"):
            parse_inventory(INVENTORY.replace(f'"{CANCELLATION}"', f'"{WAIT_QUEUE}"'))

    def test_a_package_registered_twice_is_refused(self) -> None:
        with self.assertRaisesRegex(RunnerError, "package nervix-server is registered twice"):
            parse_inventory(INVENTORY + f'\n[[package]]\nname = "nervix-server"\nchecks = ["{FLUSH}"]\n')

    def test_a_test_that_is_not_a_check_is_refused(self) -> None:
        with self.assertRaisesRegex(RunnerError, "does not|not a test whose full name contains"):
            parse_inventory(INVENTORY.replace(FLUSH, "runtime::force_flush::tests::ordinary"))

    def test_a_package_without_checks_or_an_empty_inventory_is_refused(self) -> None:
        with self.assertRaisesRegex(RunnerError, "registers no check"):
            parse_inventory('[[package]]\nname = "nervix-server"\nchecks = []\n')
        with self.assertRaisesRegex(RunnerError, "registers no package"):
            parse_inventory("")

    def test_an_unknown_key_is_refused(self) -> None:
        with self.assertRaisesRegex(RunnerError, "unknown keys: skip"):
            parse_inventory(INVENTORY + "skip = true\n")


class SelectionTests(unittest.TestCase):
    def discoveries(
        self,
        execution: Sequence[str] = (WAIT_QUEUE, CANCELLATION),
        server: Sequence[str] = (FLUSH,),
        ignored: Sequence[str] = (),
    ) -> list[Discovery]:
        return [
            Discovery(REPLAY_CHECK_PACKAGE, tuple(execution), tuple(ignored)),
            Discovery("nervix-server", tuple(server), tuple(ignored)),
        ]

    def test_listed_tests_are_the_terse_test_lines(self) -> None:
        output = f"   Compiling nervix-server\n{FLUSH}: test\nbench::relay: benchmark\n"
        self.assertEqual(listed_tests(output), [FLUSH])

    def test_the_whole_gate_selects_every_registered_check(self) -> None:
        selected = select(inventory(), self.discoveries(), "")
        self.assertEqual([check.test for check in selected], [WAIT_QUEUE, CANCELLATION, FLUSH])

    def test_a_registered_check_that_disappeared_fails_the_gate(self) -> None:
        with self.assertRaisesRegex(RunnerError, f"{CANCELLATION} is not discovered"):
            select(inventory(), self.discoveries(execution=[WAIT_QUEUE]), "")

    def test_an_ignored_registered_check_fails_the_gate(self) -> None:
        with self.assertRaisesRegex(RunnerError, f"{FLUSH} is ignored"):
            select(inventory(), self.discoveries(ignored=[FLUSH]), "")

    def test_an_unregistered_check_fails_the_gate(self) -> None:
        unregistered = "runtime::relay::shuttle_tests::shuttle_new_race"
        with self.assertRaisesRegex(RunnerError, "shuttle_new_race is not registered"):
            select(inventory(), self.discoveries(server=[FLUSH, unregistered]), "")

    def test_a_filter_may_match_nothing_in_one_package_of_a_nonempty_selection(self) -> None:
        selected = select(inventory(), self.discoveries(), "stale_completions")
        self.assertEqual([check.package for check in selected], ["nervix-server"])

    def test_a_filter_does_not_require_the_other_checks(self) -> None:
        selected = select(inventory(), self.discoveries(execution=[WAIT_QUEUE]), "wait_queue")
        self.assertEqual([check.test for check in selected], [WAIT_QUEUE])

    def test_a_filter_that_selects_nothing_in_any_package_fails(self) -> None:
        with self.assertRaisesRegex(RunnerError, "no Shuttle check matching `absent` was selected"):
            select(inventory(), self.discoveries(), "absent")

    def test_a_filter_refuses_an_unregistered_check(self) -> None:
        unregistered = "runtime::relay::shuttle_tests::shuttle_new_race"
        with self.assertRaisesRegex(RunnerError, "not registered"):
            select(inventory(), self.discoveries(server=[FLUSH, unregistered]), "new_race")


class ScriptedCommands(Commands):
    """Answers each command from `respond` and records what ran, with its environment."""

    def __init__(self, root: Path, respond: Callable[[Sequence[str], Mapping[str, str]], Outcome]) -> None:
        super().__init__(root)
        self.respond = respond
        self.commands: list[tuple[list[str], dict[str, str]]] = []

    def run(
        self,
        arguments: Sequence[str],
        *,
        environment: Mapping[str, str] | None = None,
        echo: bool = True,
    ) -> Outcome:
        environment = dict(environment or {})
        self.commands.append((list(arguments), environment))
        return self.respond(arguments, environment)


def listing(*tests: str) -> Outcome:
    return Outcome(0, "".join(f"{test}: test\n" for test in tests))


def repository(test: unittest.TestCase) -> Path:
    directory = TemporaryDirectory()
    test.addCleanup(directory.cleanup)
    root = Path(directory.name)
    (root / "Cargo.lock").write_text(
        '[[package]]\nname = "shuttle"\nversion = "0.9.4"\n', encoding="utf-8"
    )
    return root


def environment_answers(arguments: Sequence[str]) -> Outcome | None:
    """The answers every scripted run gives to Git and the toolchain."""

    if arguments[:2] == ["git", "rev-parse"]:
        return Outcome(0, "0123abcd\n")
    if arguments[:2] in (["git", "status"], ["rustc", "-vV"]):
        return Outcome(0, "")
    return None


class RunTests(unittest.TestCase):
    def run_gate(
        self,
        respond: Callable[[Sequence[str], Mapping[str, str]], Outcome],
        filter_text: str = "",
    ) -> tuple[int, str, Path, ScriptedCommands]:
        root = repository(self)
        commands = ScriptedCommands(root, respond)
        out = io.StringIO()
        with redirect_stdout(out), redirect_stderr(out):
            status = run_checks(commands, inventory(), root / "target", filter_text)
        return status, out.getvalue(), root / "target", commands

    @staticmethod
    def listings(arguments: Sequence[str]) -> Outcome | None:
        if "--ignored" in arguments:
            return listing()
        if "--list" in arguments:
            if "nervix-server" in arguments:
                return listing(FLUSH)
            return listing(WAIT_QUEUE, CANCELLATION, "workers::tests::ordinary")
        return environment_answers(arguments)

    def test_a_gate_passes_when_every_check_completes_both_runs(self) -> None:
        def respond(arguments: Sequence[str], environment: Mapping[str, str]) -> Outcome:
            return self.listings(arguments) or Outcome(0, completed_output())

        status, report, target, commands = self.run_gate(respond)
        self.assertEqual(status, 0, report)
        self.assertIn("discovered 3, selected 3, executed 3, completed 3", report)
        runs = [environment for arguments, environment in commands.commands if "--exact" in arguments]
        self.assertEqual(len(runs), 6)
        nondeterminism = [run for run in runs if run.get("SHUTTLE_CHECK_NONDETERMINISM") == "1"]
        self.assertEqual(len(nondeterminism), 3)
        for run in runs:
            self.assertIn("shuttle-failures", run["SHUTTLE_TRACE_DIR"])
        self.assertFalse((target / "shuttle-failures" / "nervix-server" / FLUSH).exists())

    def test_every_check_runs_alone_in_its_own_process(self) -> None:
        def respond(arguments: Sequence[str], environment: Mapping[str, str]) -> Outcome:
            return self.listings(arguments) or Outcome(0, completed_output())

        _, _, _, commands = self.run_gate(respond)
        exact = [arguments for arguments, _ in commands.commands if "--exact" in arguments]
        self.assertIn(
            [
                "cargo",
                "test",
                "--package",
                "nervix-server",
                "--features",
                "shuttle",
                "--lib",
                FLUSH,
                "--",
                "--exact",
                "--nocapture",
                "--test-threads=1",
            ],
            exact,
        )

    def test_a_check_that_passes_without_completing_fails_with_its_evidence(self) -> None:
        def respond(arguments: Sequence[str], environment: Mapping[str, str]) -> Outcome:
            answer = self.listings(arguments)
            if answer is not None:
                return answer
            if FLUSH in arguments and "SHUTTLE_CHECK_NONDETERMINISM" in environment:
                return Outcome(0, "running 0 tests\ntest result: ok. 0 passed\n")
            return Outcome(0, completed_output())

        status, report, target, _ = self.run_gate(respond)
        self.assertEqual(status, 1)
        self.assertIn("executed 3, completed 2", report)
        self.assertIn(
            f"{FLUSH} passed without the record of a completed exploration in its nondeterminism run",
            report,
        )
        evidence = target / "shuttle-failures" / "nervix-server" / FLUSH
        metadata = json.loads((evidence / "nondeterminism.json").read_text(encoding="utf-8"))
        self.assertEqual(metadata["test"], FLUSH)
        self.assertEqual(metadata["run"], "nondeterminism")
        self.assertEqual(metadata["revision"], "0123abcd")
        self.assertEqual(metadata["shuttle"], "0.9.4")
        self.assertTrue((evidence / "nondeterminism.log").is_file())

    def test_a_failing_check_keeps_its_schedule_and_names_its_replay(self) -> None:
        def respond(arguments: Sequence[str], environment: Mapping[str, str]) -> Outcome:
            answer = self.listings(arguments)
            if answer is not None:
                return answer
            if WAIT_QUEUE in arguments and "SHUTTLE_CHECK_NONDETERMINISM" not in environment:
                directory = Path(environment["SHUTTLE_TRACE_DIR"])
                (directory / "schedule-1").write_text("91021", encoding="utf-8")
                return Outcome(101, "panicked: deadlock detected\n")
            return Outcome(0, completed_output())

        status, report, target, _ = self.run_gate(respond)
        self.assertEqual(status, 1)
        self.assertIn("failed with exit status 101 in its exploration run", report)
        evidence = target / "shuttle-failures" / REPLAY_CHECK_PACKAGE / WAIT_QUEUE
        metadata = json.loads((evidence / "exploration.json").read_text(encoding="utf-8"))
        self.assertEqual(metadata["schedules"], ["schedule-1"])
        self.assertEqual(metadata["replay"], [f"just test-shuttle-replay {evidence / 'schedule-1'}"])

    def test_a_filter_runs_only_its_checks(self) -> None:
        def respond(arguments: Sequence[str], environment: Mapping[str, str]) -> Outcome:
            return self.listings(arguments) or Outcome(0, completed_output())

        status, report, _, commands = self.run_gate(respond, "stale_completions")
        self.assertEqual(status, 0, report)
        self.assertIn("discovered 3, selected 1, executed 1, completed 1", report)
        exact = [arguments for arguments, _ in commands.commands if "--exact" in arguments]
        self.assertEqual({arguments[7] for arguments in exact}, {FLUSH})


class ReplayTests(unittest.TestCase):
    def test_a_replay_runs_exactly_the_check_the_schedule_belongs_to(self) -> None:
        root = repository(self)
        schedule = root / "target" / "shuttle-failures" / "nervix-server" / FLUSH / "schedule-1"
        schedule.parent.mkdir(parents=True)
        schedule.write_text("91021", encoding="utf-8")

        def respond(arguments: Sequence[str], environment: Mapping[str, str]) -> Outcome:
            self.assertEqual(environment["SHUTTLE_TRACE_FILE"], str(schedule))
            self.assertIn(FLUSH, arguments)
            return Outcome(101, f"{REPLAYED} {schedule}\npanicked: deadlock detected\n")

        out = io.StringIO()
        with redirect_stdout(out), redirect_stderr(out):
            status = replay(ScriptedCommands(root, respond), inventory(), schedule)
        self.assertEqual(status, 101)
        self.assertIn("reproduced", out.getvalue())

    def test_a_schedule_outside_a_registered_check_is_refused(self) -> None:
        root = repository(self)
        schedule = root / "elsewhere" / "schedule-1"
        schedule.parent.mkdir(parents=True)
        schedule.write_text("91021", encoding="utf-8")
        with self.assertRaisesRegex(RunnerError, "is not in"):
            replay(ScriptedCommands(root, lambda *_: Outcome(0, "")), inventory(), schedule)

    def test_a_replay_that_ran_nothing_fails(self) -> None:
        root = repository(self)
        schedule = root / "target" / "shuttle-failures" / "nervix-server" / FLUSH / "schedule-1"
        schedule.parent.mkdir(parents=True)
        schedule.write_text("91021", encoding="utf-8")
        out = io.StringIO()
        with redirect_stdout(out), redirect_stderr(out):
            status = replay(
                ScriptedCommands(root, lambda *_: Outcome(0, "running 0 tests\n")), inventory(), schedule
            )
        self.assertEqual(status, 1)
        self.assertIn("did not replay", out.getvalue())


class ReplayCheckTests(unittest.TestCase):
    def scripted(self, persisted: int = 1, reproduces: bool = True) -> Callable[..., Outcome]:
        def respond(arguments: Sequence[str], environment: Mapping[str, str]) -> Outcome:
            if "SHUTTLE_TRACE_DIR" in environment:
                directory = Path(environment["SHUTTLE_TRACE_DIR"])
                for index in range(persisted):
                    (directory / f"schedule-{index}").write_text("91021", encoding="utf-8")
                return Outcome(101, f"panicked: {FORCED_FAILURE}\n")
            if environment.get("SHUTTLE_FORCE_FAILURE") == "1":
                if not reproduces:
                    return Outcome(0, f"{REPLAYED} schedule\n")
                return Outcome(101, f"{REPLAYED} schedule\npanicked: {FORCED_FAILURE}\n")
            return Outcome(0, f"{REPLAYED} schedule\n")

        return respond

    def replay_check(self, respond: Callable[..., Outcome]) -> tuple[int, str]:
        root = repository(self)
        (root / "target").mkdir()
        out = io.StringIO()
        with redirect_stdout(out), redirect_stderr(out):
            try:
                status = replay_check(ScriptedCommands(root, respond), inventory(), root / "target")
            except RunnerError as error:
                return 1, str(error)
        return status, out.getvalue()

    def test_a_forced_failure_persists_one_schedule_that_reproduces_it(self) -> None:
        status, report = self.replay_check(self.scripted())
        self.assertEqual(status, 0, report)
        self.assertIn("the schedule reproduced the failure in a fresh process", report)

    def test_more_than_one_persisted_schedule_fails(self) -> None:
        status, report = self.replay_check(self.scripted(persisted=2))
        self.assertEqual(status, 1)
        self.assertIn("expected one persisted schedule, found 2", report)

    def test_a_schedule_that_does_not_reproduce_fails(self) -> None:
        status, report = self.replay_check(self.scripted(reproduces=False))
        self.assertEqual(status, 1)
        self.assertIn("did not reproduce the forced failure", report)


class CommandTests(unittest.TestCase):
    def test_packages_prints_the_inventory_packages(self) -> None:
        root = repository(self)
        inventory_path = root / "crates" / "model-harness" / "shuttle-inventory.toml"
        inventory_path.parent.mkdir(parents=True)
        inventory_path.write_text(INVENTORY, encoding="utf-8")
        out = io.StringIO()
        with redirect_stdout(out):
            status = main(["--root", str(root), "packages"])
        self.assertEqual(status, 0)
        self.assertEqual(out.getvalue().split(), [REPLAY_CHECK_PACKAGE, "nervix-server"])

    def test_a_run_needs_a_target_directory(self) -> None:
        root = repository(self)
        inventory_path = root / "crates" / "model-harness" / "shuttle-inventory.toml"
        inventory_path.parent.mkdir(parents=True)
        inventory_path.write_text(INVENTORY, encoding="utf-8")
        out = io.StringIO()
        with redirect_stdout(out), redirect_stderr(out):
            status = main(["--root", str(root), "run"])
        self.assertEqual(status, 1)
        self.assertIn("needs --target-dir", out.getvalue())


if __name__ == "__main__":
    unittest.main()
