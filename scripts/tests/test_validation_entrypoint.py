"""The CI validation shards include the Rust structural gate on the checked-out revision."""

from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
import unittest
from pathlib import Path

from scripts.tests.test_native_coverage import job_section, step_section


def dependencies(recipes: dict[str, dict], name: str) -> set[str]:
    """Every recipe a named recipe depends on, transitively."""
    found: set[str] = set()
    pending = [name]
    while pending:
        current = pending.pop()
        for dependency in recipes[current]["dependencies"]:
            recipe = dependency["recipe"]
            if recipe not in found:
                found.add(recipe)
                pending.append(recipe)
    return found


REPOSITORY = Path(__file__).resolve().parents[2]

# The recipe that runs the gate, and the command it runs: the gate measures the repository holding
# the working directory, and an argument such as `--show` or `--root` would replace its verdict.
GATE_RECIPE = "ratchet"
# The caller's build directory also owns the instrumented native-coverage runner.
CARGO_TARGET_DIRECTORY = Path(os.environ.get("CARGO_TARGET_DIR", str(REPOSITORY / "target")))
GATE_COMMAND = [str(CARGO_TARGET_DIRECTORY / "typed-ratchet/source/debug/nervix-ratchet"), "gate"]


class ValidationEntrypointTests(unittest.TestCase):
    """The Check workflow's `checks` job runs the gate, and every other check of `just validate`.

    A passing `checks` result stands in for local validation only while its shards, together, run
    on the checked-out revision what `just validate` runs: each check in at least one shard,
    formatting checked instead of applied, and the gate in exactly one shard, after that shard's
    lints, with no argument. The gate refusals themselves are tested by the Rust owner.
    """

    def setUp(self) -> None:
        dumped = subprocess.run(
            ["just", "--dump", "--dump-format", "json"], cwd=REPOSITORY, capture_output=True, text=True, check=True,
        )
        self.recipes = json.loads(dumped.stdout)["recipes"]
        workflow = (REPOSITORY / ".github/workflows/check.yaml").read_text(encoding="utf-8")
        self.job = job_section(workflow, "checks")

    def dry_run(self, *arguments: str) -> list[str]:
        """The command lines `just` evaluates for `arguments`, without running them."""

        completed = subprocess.run(
            ["just", "--dry-run", *arguments], cwd=REPOSITORY, capture_output=True, text=True, timeout=120,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        return completed.stderr.splitlines()

    def nested_recipe(self, *arguments: str) -> str:
        """The recipe a `run-with-jobs` entry point hands to its nested `just`, its last argument."""

        command = shlex.split(self.dry_run(*arguments)[-1])
        self.assertEqual(Path(command[0]).name, "just", command)
        return command[-1]

    def shard_recipes(self) -> dict[str, str]:
        """Each shard of the job's matrix, as its Validate step passes it, and the part it runs."""

        matrix = re.search(r"^        shard: \[(?P<values>[^\]]+)\]\n", self.job, re.MULTILINE)
        assert matrix is not None, "the checks job declares no shard matrix"
        values = [value.strip() for value in matrix.group("values").split(",")]
        shards = {}
        for value in values:
            shard = f"{value}/{len(values)}"
            shards[shard] = self.nested_recipe("validate-ci", "4", shard)
        return shards

    def work(self, *roots: str) -> set[str]:
        """What running `roots` runs: each recipe with a body, and each dependency given arguments.

        A recipe without a body only groups others, so how each side groups its checks never
        matters. A dependency with arguments is one particular run of its recipe, such as one
        Clippy lane, so a lane that one side drops is missing from that side's work.
        """

        reached = set(roots)
        for root in roots:
            reached |= dependencies(self.recipes, root)
        work = set()
        for name in reached:
            recipe = self.recipes[name]
            if recipe["body"]:
                work.add(name)
            for dependency in recipe["dependencies"]:
                if dependency["arguments"]:
                    work.add(f"{name} -> {dependency['recipe']} {json.dumps(dependency['arguments'])}")
        return work

    def gate_runs(self, recipe: str) -> int:
        return sum(1 for line in self.dry_run(recipe)
                   if line.startswith("gate=(") and line.endswith(")")
                   and shlex.split(line[len("gate=("):-1]) == GATE_COMMAND)

    def test_every_shard_validates_the_checked_out_revision(self) -> None:
        # The job runs for every pull request and push, and no shard's failure cancels the other.
        self.assertIsNone(re.search(r"^    if:", self.job, re.MULTILINE))
        self.assertIn("      fail-fast: false\n", self.job)
        checkout = step_section(self.job, "Checkout")
        self.assertIn("uses: actions/checkout@v4", checkout)
        self.assertIsNone(re.search(r"^          (ref|path|repository):", checkout, re.MULTILINE))
        validate = step_section(self.job, "Validate")
        self.assertIn('          just validate-ci 4 "${SHARD}"\n', validate)
        self.assertIn("          SHARD: ${{ matrix.shard }}/${{ strategy.job-total }}\n", validate)
        for key in ("if", "continue-on-error", "working-directory"):
            with self.subTest(key=key):
                self.assertIsNone(re.search(rf"^        {key}:", validate, re.MULTILINE))
        shards = self.shard_recipes()
        self.assertGreater(len(shards), 1)
        self.assertEqual(len(set(shards.values())), len(shards), shards)

    def test_exactly_one_shard_runs_the_gate_after_its_lints_with_no_argument(self) -> None:
        shards = self.shard_recipes()
        gated = [shard for shard, recipe in shards.items() if GATE_RECIPE in dependencies(self.recipes, recipe)]
        self.assertEqual(len(gated), 1, shards)
        for shard, recipe in shards.items():
            with self.subTest(shard=shard):
                self.assertEqual(self.gate_runs(recipe), 1 if shard in gated else 0)
        part = self.recipes[shards[gated[0]]]
        order = [dependency["recipe"] for dependency in part["dependencies"]]
        # The part names the gate itself, so the gate never runs only because another check reads
        # its catalog. Its dependencies run in order: everything before the gate has finished when
        # it starts, and everything after it reads what it wrote.
        self.assertIn(GATE_RECIPE, order)
        self.assertNotIn("parallel", part["attributes"])
        for later in order[order.index(GATE_RECIPE) + 1:]:
            with self.subTest(after=later):
                self.assertIn(GATE_RECIPE, dependencies(self.recipes, later))
        reached = dependencies(self.recipes, shards[gated[0]]) | {shards[gated[0]]}
        for name in sorted(reached):
            for dependency in self.recipes[name]["dependencies"]:
                if dependency["recipe"] == GATE_RECIPE:
                    with self.subTest(dependent=name):
                        self.assertEqual(dependency["arguments"], [])

    def test_the_shards_together_run_every_check_validate_runs(self) -> None:
        shards = list(self.shard_recipes().values())
        local = self.nested_recipe("validate", "4")
        everything = self.nested_recipe("validate-ci", "4")
        self.assertEqual(self.gate_runs(local), 1)
        self.assertEqual(self.gate_runs(everything), 1)
        self.assertIn("fmt", dependencies(self.recipes, local))
        self.assertTrue(any("fmt-check" in dependencies(self.recipes, shard) for shard in shards))
        validated = self.work(local) - self.work("fmt")
        checked = self.work(*shards) - self.work("fmt-check")
        self.assertEqual(sorted(validated - checked), [], "`just validate` runs these, and no CI shard does")
        self.assertEqual(sorted(checked - validated), [], "a CI shard runs these, and `just validate` does not")
        # Without a shard, `just validate-ci` runs both parts.
        self.assertEqual(self.work(everything), self.work(*shards))


if __name__ == "__main__":
    unittest.main()
