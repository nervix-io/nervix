"""Source coverage of live libFuzzer campaigns, using the shared native LLVM exporter.

Build probes, ordinary discovery, replay and deliberate failure qualification never supply
campaign counters. Each selected target exports its actual hashed executable before the next
target builds. The aggregate is complete only after every selected campaign and export succeeds.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import os
import pathlib
import re
import tomllib
from collections.abc import Callable
from contextlib import contextmanager
from typing import TYPE_CHECKING, Iterator

from scripts import native_coverage as native

if TYPE_CHECKING:
    from scripts.bolero import Inventory, Target


def build_environment() -> dict[str, str]:
    # Encoded flags take precedence over cargo-bolero's sanitizer RUSTFLAGS.
    if "CARGO_ENCODED_RUSTFLAGS" in os.environ:
        raise native.RunnerError("CARGO_ENCODED_RUSTFLAGS would override cargo-bolero's sanitizer flags")
    return {
        "RUSTFLAGS": (os.environ.get("RUSTFLAGS", "") + " -C instrument-coverage").strip(),
        "LLVM_PROFILE_FILE": "/dev/null",
    }


@contextmanager
def suppressed_profiles() -> Iterator[None]:
    previous = os.environ.get("LLVM_PROFILE_FILE")
    os.environ["LLVM_PROFILE_FILE"] = "/dev/null"
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("LLVM_PROFILE_FILE", None)
        else:
            os.environ["LLVM_PROFILE_FILE"] = previous


@dataclasses.dataclass(frozen=True)
class FuzzWorkspace(native.Workspace):
    def build(self) -> pathlib.Path:
        # cargo-bolero chooses target/fuzz/build_<flag hash>, even for an isolated manifest.
        return self.root / "target/fuzz"


def fingerprint_prefix(test_target: str) -> str:
    """How Cargo begins the name of a test executable's build fingerprint: by a library's or a
    binary's unit tests, or by an integration test."""
    if test_target == "lib":
        return "test-lib-"
    if test_target.startswith("bin:"):
        return "test-bin-"
    return "test-integration-test-"


def target_identity(target: Target, root: pathlib.Path) -> dict[str, object]:
    result = dataclasses.asdict(target)
    for key, value in result.items():
        if isinstance(value, pathlib.Path):
            result[key] = str(value.relative_to(root))
    return result


def normalize_report(path: pathlib.Path, root: pathlib.Path) -> None:
    lines = path.read_text().splitlines(keepends=True)
    for index, line in enumerate(lines):
        if line.startswith("SF:"):
            source = pathlib.Path(line[3:].strip())
            if source.is_absolute():
                lines[index] = f"SF:{source.relative_to(root)}\n"
    path.write_text("".join(lines))


def corpus_identity(directory: pathlib.Path) -> dict[str, object]:
    """The checked-in seeds a campaign starts from, by name and content."""
    digest = hashlib.sha256()
    inputs = 0
    for seed in sorted(directory.iterdir()):
        if seed.is_file() and not seed.name.startswith("."):
            digest.update(seed.name.encode() + b"\0" + hashlib.sha256(seed.read_bytes()).digest())
            inputs += 1
    return {"inputs": inputs, "sha256": digest.hexdigest()}


class Campaign:
    def __init__(
        self, root: pathlib.Path, inventory: Inventory, selected: tuple[Target, ...], duration: int,
        partition: dict[str, object] | None,
    ) -> None:
        registered = {target.id: target for target in inventory.targets}
        identities = {target.id: target for target in selected}
        if not selected or len(identities) != len(selected):
            raise native.RunnerError("a campaign requires a nonempty unique selection")
        for target in selected:
            if registered.get(target.id) != target:
                raise native.RunnerError(f"{target.id}: campaign target differs from the inventory")
        if type(duration) is not int or duration <= 0:
            raise native.RunnerError("campaign duration must be a positive integer")
        self.workspace = FuzzWorkspace(root=root, target=root / "target", mode="fuzz")
        self.inventory = inventory
        self.selected = selected
        self.commands = native.Commands(root)
        self.started = native.utc_now()
        self.run = native.run_identity(os.environ, self.started, os.getpid())
        producer = native.Producer("bolero-fuzz", "fuzz", (), "fuzz-all", ())
        name = self.run.attempt_name()
        if partition is not None:
            name = f"{name}-shard-{partition['number']}-of-{partition['count']}"
        self.path = self.workspace.new_attempt(producer, inventory.nightly, name)
        self.inventory_file = root / "tests/bolero-targets.toml"
        self.inventory_digest = hashlib.sha256(self.inventory_file.read_bytes()).hexdigest()
        self.record = native.Record(self.path / native.RECORD, {
            "producer": "bolero-fuzz",
            "mode": "fuzz",
            "verdict": "running",
            "stage": "prepare",
            "started_at": native.timestamp(self.started),
            "run": self.run.describe(),
            "event": os.environ.get("BOLERO_EVENT_NAME"),
            "labels": json.loads(os.environ.get("BOLERO_LABELS", "[]")),
            "tested_sha": os.environ.get("BOLERO_TESTED_SHA"),
            "requested_toolchain": inventory.nightly,
            "inventory": {"file": str(self.inventory_file.relative_to(root)), "sha256": self.inventory_digest,
                          "targets": [target_identity(target, root) for target in inventory.targets]},
            "selection": [target.id for target in selected],
            "shard": partition,
            "counts": {"discovered": len(inventory.targets), "selected": len(selected), "executed": 0, "completed": 0},
            "bounds": {"seconds_per_target": duration, "rss_limit_mb": 2048},
            "targets": {},
        })
        self.record.write()
        self.lock = self.workspace.build_lock()

    def __enter__(self) -> Campaign:
        try:
            self.lock.__enter__()
            try:
                environment = {**os.environ, "RUSTUP_TOOLCHAIN": self.inventory.nightly}
                self.toolchain = native.load_toolchain(self.commands, environment)
                self.packages = native.load_packages(self.commands)
                self.record.content["toolchain"] = self.toolchain.describe()
                self.record.content["revision"] = native.load_revision(self.commands).describe()
                self.record.content["instrumentation"] = build_environment()
                self.record.content["rustc_wrapper"] = os.environ.get("RUSTC_WRAPPER", "Cargo configuration")
                self.record.write()
            except BaseException:
                self.lock.__exit__(None, None, None)
                raise
        except BaseException as error:
            self.fail(error)
            raise
        return self

    def __exit__(self, kind, error, traceback) -> None:
        try:
            if error is not None:
                self.fail(error)
            elif self.record.content["verdict"] != "complete":
                self.fail(native.RunnerError("campaign ended before aggregate export"))
        finally:
            self.lock.__exit__(kind, error, traceback)

    def fail(self, error: BaseException) -> None:
        verdict = native.Verdict.FAILED
        if isinstance(error, (KeyboardInterrupt, native.Interrupted)):
            verdict = native.Verdict.INTERRUPTED
        self.record.content["failure"] = {"stage": self.record.content["stage"], "detail": str(error)}
        self.record.conclude(verdict, native.utc_now())

    @contextmanager
    def target(self, target: Target, artifacts: pathlib.Path) -> Iterator[TargetCoverage]:
        if target not in self.selected or target.id in self.record.content["targets"]:
            raise native.RunnerError(f"{target.id}: unexpected or repeated campaign target")
        measured = TargetCoverage(self, target, artifacts)
        try:
            yield measured
            if measured.record.content["verdict"] != "complete":
                raise native.RunnerError(f"{target.id}: campaign did not finish its export")
        except BaseException as error:
            verdict = native.Verdict.INTERRUPTED if isinstance(error, (KeyboardInterrupt, native.Interrupted)) else native.Verdict.FAILED
            measured.record.content["failure"] = {"stage": measured.record.content["stage"], "detail": str(error)}
            measured.record.conclude(verdict, native.utc_now())
            raise

    def export(self, path: pathlib.Path, record: native.Record) -> native.Exported:
        if hashlib.sha256(self.inventory_file.read_bytes()).hexdigest() != self.inventory_digest:
            raise native.RunnerError("inventory changed during the live campaign")
        record.content["stage"] = "export"
        record.write()
        exported = native.export(self.commands, self.workspace, self.toolchain, self.packages, path)
        normalize_report(path / native.REPORT, self.workspace.root)
        record.content["execution"] = exported.selection.describe(self.workspace)
        record.content["profiles"] = exported.selection.describe_profiles()
        record.content["sources"] = exported.sources.describe(exported.policy)
        record.content["export_warnings"] = list(exported.warnings)
        return exported

    def complete(self) -> None:
        targets = self.record.content["targets"]
        if set(targets) != {target.id for target in self.selected}:
            raise native.RunnerError("selected campaign targets are missing")
        for target in targets.values():
            completion = self.path / target["completion"]
            if json.loads(completion.read_text())["verdict"] != "complete":
                raise native.RunnerError("a selected campaign target is incomplete")
        counts = self.record.content["counts"]
        if counts["executed"] != len(self.selected) or counts["completed"] != len(self.selected):
            raise native.RunnerError("campaign execution/completion counts differ from selection")
        self.export(self.path, self.record)
        self.record.conclude(native.Verdict.COMPLETE, native.utc_now())
        print(f"Bolero live Rust source coverage: {self.path / native.REPORT}", flush=True)


class TargetCoverage:
    def __init__(self, campaign: Campaign, target: Target, artifacts: pathlib.Path) -> None:
        self.campaign = campaign
        self.target = target
        self.path = campaign.path / "targets" / target.id
        (self.path / native.PROFILES).mkdir(parents=True)
        self.record = native.Record(self.path / native.RECORD, {
            "producer": "bolero-fuzz", "mode": "fuzz", "verdict": "running", "stage": "build",
            "revision": campaign.record.content["revision"], "run": campaign.run.describe(),
            "toolchain": campaign.toolchain.describe(), "inventory_sha256": campaign.inventory_digest,
            "target": target_identity(target, campaign.workspace.root), "corpus": corpus_identity(target.corpus),
            "bounds": campaign.record.content["bounds"], "artifacts": campaign.workspace.display(artifacts),
        })
        self.record.write()
        campaign.record.content["targets"][target.id] = {"completion": str(self.record.path.relative_to(campaign.path))}
        campaign.record.write()

    def execute(self, binary: pathlib.Path, flags: list[str]) -> dict[str, str]:
        binary = binary.resolve()
        # Read the exact test executable's Cargo fingerprint, rather than guessing a debug binary.
        fingerprint = binary.parent.parent / "fingerprint"
        kind = fingerprint_prefix(self.target.test_target)
        files = list(fingerprint.glob(f"{kind}*.json"))
        if len(files) != 1:
            raise native.RunnerError(f"{self.target.id}: exact fuzz build fingerprint is missing or ambiguous")
        built = json.loads(files[0].read_text())
        rustflags = built["rustflags"]
        if not native.instruments_coverage(rustflags) or f"-Zsanitizer={self.campaign.inventory.sanitizer}" not in rustflags:
            raise native.RunnerError(f"{self.target.id}: build lacks source coverage or the selected sanitizer")
        if "fuzzing" not in rustflags or not any("sanitizer-coverage" in flag for flag in rustflags):
            raise native.RunnerError(f"{self.target.id}: build lacks libFuzzer feedback")
        manifest = self.target.manifest if self.target.manifest is not None else self.campaign.workspace.root / "Cargo.toml"
        self.record.content["build"] = {
            "executable": self.campaign.workspace.display(binary),
            "fingerprint": self.campaign.workspace.display(files[0]), "rustflags": rustflags,
            "profile": "fuzz", "cargo_fingerprint": built,
            "profile_manifest": self.campaign.workspace.display(manifest),
            "profile_settings": tomllib.loads(manifest.read_text())["profile"]["fuzz"],
        }
        arguments = [self.target.test, "--exact", "--nocapture", "--quiet", "--test-threads", "1"]
        entry = {"executable": str(binary), "arguments": arguments, "build_id": native.build_id(binary)}
        for path in (self.path, self.campaign.path):
            with (path / native.EXECUTIONS).open("a") as stream:
                stream.write(json.dumps(entry) + "\n")
        self.record.content["engine_flags"] = flags
        self.record.content["stage"] = "execute"
        self.record.write()
        self.campaign.record.content["counts"]["executed"] += 1
        self.campaign.record.write()
        return {"LLVM_PROFILE_FILE": str(self.path / native.PROFILES / native.PROFILE_PATTERN)}

    def complete(self, output: str, seconds: float) -> None:
        match = re.search(r"#(\d+)\s+DONE", output)
        if not match or int(match[1]) == 0:
            raise native.RunnerError(f"{self.target.id}: no completed live libFuzzer inputs")
        self.record.content["cost"] = {"campaign_seconds": seconds, "inputs": int(match[1]), "inputs_per_second": int(match[1]) / seconds}
        self.campaign.export(self.path, self.record)
        for profile in (self.path / native.PROFILES).glob("*.profraw"):
            os.link(profile, self.campaign.path / native.PROFILES / f"{self.target.id}-{profile.name}")
        self.record.conclude(native.Verdict.COMPLETE, native.utc_now())
        self.campaign.record.content["counts"]["completed"] += 1
        self.campaign.record.write()


@dataclasses.dataclass(frozen=True)
class ShardEvidence:
    """One shard's campaign record and the directory holding its report and target records."""

    directory: pathlib.Path
    record: dict[str, object]

    @property
    def number(self) -> int:
        return self.record["shard"]["number"]


def shard_evidence(evidence: pathlib.Path) -> list[ShardEvidence]:
    """Every shard campaign record below `evidence`; target records hold no selection."""
    found = []
    for path in sorted(evidence.rglob(native.RECORD)):
        content = json.loads(path.read_text())
        if content.get("producer") != "bolero-fuzz" or "selection" not in content:
            continue
        if not isinstance(content.get("shard"), dict):
            raise native.RunnerError(f"{path} records a campaign without a shard plan")
        found.append(ShardEvidence(path.parent, content))
    if not found:
        raise native.RunnerError(f"no shard campaign record was found under {evidence}")
    return found


def shared(shards: list[ShardEvidence], field: str) -> object:
    """A field every shard of one run must record identically."""
    values = [shard.record.get(field) for shard in shards]
    if any(value != values[0] for value in values):
        raise native.RunnerError(f"the shards record different {field}: {values}")
    return values[0]


def check_shard(
    shard: ShardEvidence, digest: str, plan: tuple[tuple[Target, ...], ...], plan_sha256: str
) -> None:
    record = shard.record
    number = shard.number
    if record.get("verdict") != str(native.Verdict.COMPLETE):
        raise native.RunnerError(f"shard {number} did not complete: {record.get('verdict')}")
    if record["inventory"]["sha256"] != digest:
        raise native.RunnerError(f"shard {number} ran another inventory than the tested revision's")
    if record["shard"]["plan_sha256"] != plan_sha256:
        raise native.RunnerError(f"shard {number} ran another shard plan than the inventory's")
    expected = [target.id for target in plan[number - 1]]
    if record["selection"] != expected:
        raise native.RunnerError(f"shard {number} selected {record['selection']}, its plan holds {expected}")
    counts = record["counts"]
    if not counts["selected"] == counts["executed"] == counts["completed"] == len(expected):
        raise native.RunnerError(f"shard {number} counts {counts} differ from its {len(expected)} targets")
    for target_id in expected:
        completion = shard.directory / record["targets"][target_id]["completion"]
        if json.loads(completion.read_text()).get("verdict") != str(native.Verdict.COMPLETE):
            raise native.RunnerError(f"shard {number}: {target_id} has no complete campaign record")
    if not (shard.directory / native.REPORT).is_file():
        raise native.RunnerError(f"shard {number} has no coverage report")


def combine(
    root: pathlib.Path, inventory: Inventory, evidence: pathlib.Path, output: pathlib.Path,
    plan_for: Callable[[int], tuple[tuple[Target, ...], ...]],
    plan_digest: Callable[[tuple[tuple[Target, ...], ...]], str],
) -> pathlib.Path:
    """Prove that one run's shard campaigns covered the whole inventory once, and merge them.

    Each shard records its plan, selection and per-target completion. The combined record is
    complete only when shards 1 to N of one plan, run at the tested revision and inventory, each
    completed exactly their planned targets, so together they ran every registered target once.
    """
    inventory_file = root / "tests/bolero-targets.toml"
    digest = hashlib.sha256(inventory_file.read_bytes()).hexdigest()
    output.mkdir(parents=True)
    record = native.Record(output / native.RECORD, {
        "producer": "bolero-fuzz", "mode": "fuzz", "scope": "combined shards", "verdict": "running",
        "stage": "combine", "started_at": native.timestamp(native.utc_now()),
        "tested_sha": os.environ.get("BOLERO_TESTED_SHA"),
        "inventory": {"file": str(inventory_file.relative_to(root)), "sha256": digest},
    })
    record.write()
    try:
        shards = shard_evidence(evidence)
        counts = {shard.record["shard"]["count"] for shard in shards}
        if len(counts) != 1:
            raise native.RunnerError(f"the shards name different shard counts: {sorted(counts)}")
        count = counts.pop()
        numbers = sorted(shard.number for shard in shards)
        if numbers != list(range(1, count + 1)):
            raise native.RunnerError(f"shards {numbers} are not shards 1 to {count} each once")
        plan = plan_for(count)
        plan_sha256 = plan_digest(plan)
        shards.sort(key=lambda shard: shard.number)
        for shard in shards:
            check_shard(shard, digest, plan, plan_sha256)
        tested = shared(shards, "tested_sha")
        if record.content["tested_sha"] is not None and tested != record.content["tested_sha"]:
            raise native.RunnerError(f"the shards tested {tested}, not {record.content['tested_sha']}")
        selected = [target_id for shard in shards for target_id in shard.record["selection"]]
        if sorted(selected) != sorted(target.id for target in inventory.targets):
            raise native.RunnerError("the shards together did not select every registered target once")
        # A rerun of one shard runs in a later attempt of the same run, so the shards share a run,
        # and the combined record names the latest attempt among them.
        run = [{key: value for key, value in shard.record["run"].items() if key != "job"} for shard in shards]
        identities = [{key: value for key, value in entry.items() if key != "attempt"} for entry in run]
        if any(identity != identities[0] for identity in identities):
            raise native.RunnerError(f"the shards come from different runs: {run}")
        latest = max(run, key=lambda entry: int(entry.get("attempt", 0)))
        merge_lcov([shard.directory / native.REPORT for shard in shards], output / native.REPORT)
        record.content.update({
            "run": latest,
            "revision": shared(shards, "revision"),
            "toolchain": shared(shards, "toolchain"),
            "event": shared(shards, "event"),
            "labels": shared(shards, "labels"),
            "bounds": shared(shards, "bounds"),
            "plan_sha256": plan_sha256,
            "selection": [target.id for target in inventory.targets],
            "counts": {"discovered": len(inventory.targets), "selected": len(selected),
                       "executed": len(selected), "completed": len(selected)},
            "shards": [{
                "number": shard.number,
                "record": str(shard.directory.relative_to(evidence) / native.RECORD),
                "selected": len(shard.record["selection"]),
                "started_at": shard.record.get("started_at"),
                "finished_at": shard.record.get("finished_at"),
            } for shard in shards],
            "targets": {
                target_id: {
                    "shard": shard.number,
                    "completion": str(shard.directory.relative_to(evidence) / shard.record["targets"][target_id]["completion"]),
                }
                for shard in shards for target_id in shard.record["selection"]
            },
        })
        record.conclude(native.Verdict.COMPLETE, native.utc_now())
    except BaseException as error:
        record.content["failure"] = {"stage": "combine", "detail": str(error)}
        record.conclude(native.Verdict.FAILED, native.utc_now())
        raise
    return output


@dataclasses.dataclass(frozen=True)
class FunctionRecord:
    """A function's `FN` line: its name, the line it starts on, and the line it ends on when the
    producer records that. LLVM writes `FN:<start>,<name>`; coverage.py writes
    `FN:<start>,<end>,<name>`."""

    name: str
    start: int
    end: int | None

    @classmethod
    def parse(cls, value: str) -> FunctionRecord:
        start, _, rest = value.partition(",")
        end, separator, name = rest.partition(",")
        if separator and end.isdigit():
            return cls(name=name, start=int(start), end=int(end))
        return cls(name=rest, start=int(start), end=None)

    def line(self) -> str:
        if self.end is None:
            return f"FN:{self.start},{self.name}"
        return f"FN:{self.start},{self.end},{self.name}"


@dataclasses.dataclass
class SourceCounts:
    """One source file's counters, summed over every report that measured it."""

    functions: dict[str, FunctionRecord] = dataclasses.field(default_factory=dict)
    function_hits: dict[str, int] = dataclasses.field(default_factory=dict)
    lines: dict[int, int] = dataclasses.field(default_factory=dict)
    branches: dict[tuple[int, str, str], int | None] = dataclasses.field(default_factory=dict)


SUMMARY_LINES = frozenset({"FNF", "FNH", "LF", "LH", "BRF", "BRH"})


def merge_lcov(reports: list[pathlib.Path], output: pathlib.Path) -> None:
    """Sum the counters of reports from separate executions into one report.

    Fuzz shards run disjoint campaigns and the ordinary producers run separate processes, so a
    line, function or branch was executed the sum of the times each report counted it. Summary
    lines are recomputed from the merged counters.
    """
    sources: dict[str, SourceCounts] = {}
    for report in reports:
        current: SourceCounts | None = None
        for raw in report.read_text().splitlines():
            line = raw.strip()
            if not line or line.startswith("TN:"):
                continue
            if line == "end_of_record":
                if current is None:
                    raise native.RunnerError(f"{report}: end_of_record outside a record")
                current = None
                continue
            kind, separator, value = line.partition(":")
            if kind == "SF" and separator:
                if current is not None:
                    raise native.RunnerError(f"{report}: the record before {value} has no end_of_record")
                current = sources.setdefault(value, SourceCounts())
                continue
            if current is None:
                raise native.RunnerError(f"{report}: LCOV line outside a record: {line}")
            if kind == "FN":
                function = FunctionRecord.parse(value)
                current.functions.setdefault(function.name, function)
            elif kind == "FNDA":
                hits, _, name = value.partition(",")
                current.function_hits[name] = current.function_hits.get(name, 0) + int(hits)
            elif kind == "DA":
                fields = value.split(",")
                number = int(fields[0])
                current.lines[number] = current.lines.get(number, 0) + int(fields[1])
            elif kind == "BRDA":
                number, block, branch, taken = value.split(",")
                key = (int(number), block, branch)
                merged = current.branches.get(key)
                # `-` marks a branch whose block never ran; any execution elsewhere replaces it.
                if taken != "-":
                    merged = (merged or 0) + int(taken)
                current.branches[key] = merged
            elif kind not in SUMMARY_LINES:
                raise native.RunnerError(f"{report}: unsupported LCOV line: {line}")
        if current is not None:
            raise native.RunnerError(f"{report}: the last record has no end_of_record")
    lines = []
    for source in sorted(sources):
        counts = sources[source]
        unknown = set(counts.function_hits) - set(counts.functions)
        if unknown:
            raise native.RunnerError(f"{source}: function counts without a function line: {sorted(unknown)}")
        functions = sorted(counts.functions.values(), key=lambda function: (function.start, function.name))
        lines.append(f"SF:{source}")
        lines.extend(function.line() for function in functions)
        lines.extend(f"FNDA:{counts.function_hits.get(function.name, 0)},{function.name}" for function in functions)
        lines.append(f"FNF:{len(functions)}")
        lines.append(f"FNH:{sum(1 for function in functions if counts.function_hits.get(function.name, 0) > 0)}")
        if counts.branches:
            for (number, block, branch), taken in sorted(counts.branches.items()):
                lines.append(f"BRDA:{number},{block},{branch},{'-' if taken is None else taken}")
            lines.append(f"BRF:{len(counts.branches)}")
            lines.append(f"BRH:{sum(1 for taken in counts.branches.values() if taken)}")
        lines.extend(f"DA:{number},{hits}" for number, hits in sorted(counts.lines.items()))
        lines.append(f"LF:{len(counts.lines)}")
        lines.append(f"LH:{sum(1 for hits in counts.lines.values() if hits > 0)}")
        lines.append("end_of_record")
    output.write_text("\n".join(lines) + "\n")
