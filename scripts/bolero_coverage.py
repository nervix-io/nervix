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


class Campaign:
    def __init__(self, root: pathlib.Path, inventory: Inventory, selected: tuple[Target, ...], duration: int) -> None:
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
        self.path = self.workspace.new_attempt(producer, inventory.nightly, self.run.attempt_name())
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
            "target": target_identity(target, campaign.workspace.root),
            "bounds": campaign.record.content["bounds"], "artifacts": campaign.workspace.display(artifacts),
        })
        self.record.write()
        campaign.record.content["targets"][target.id] = {"completion": str(self.record.path.relative_to(campaign.path))}
        campaign.record.write()

    def execute(self, binary: pathlib.Path, flags: list[str]) -> dict[str, str]:
        binary = binary.resolve()
        # Read the exact test executable's Cargo fingerprint, rather than guessing a debug binary.
        fingerprint = binary.parent.parent / "fingerprint"
        kind = "test-lib-" if self.target.test_target == "lib" else "test-integration-test-"
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
