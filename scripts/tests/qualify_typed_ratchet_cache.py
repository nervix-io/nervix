"""Qualify the real compiler/cache composition and trybuild in two isolated worktrees."""

from __future__ import annotations

import json
import os
import pathlib
import shutil
import subprocess
import time
import uuid

from scripts.typed_ratchet import AnalysisError, Configuration, ROOT, Runner, TOOLING, atomic_json, command


def qualify(root: pathlib.Path, target: pathlib.Path) -> dict:
    runner = Runner(root, target)
    configuration = Configuration("fixture-ordinary", str(TOOLING / "fixtures/Cargo.toml"), ())
    first = runner.analyze(configuration, fresh=True)
    repeated = runner.analyze(configuration, fresh=True)
    assert repeated["reports"] == first["reports"], "Cargo-fresh analysis changed its complete reports"
    messages = [json.loads(line) for line in pathlib.Path(repeated["cargo_log"]).read_text().splitlines()]
    assert any(item.get("reason") == "compiler-artifact" and item.get("fresh") is True for item in messages)
    build = pathlib.Path(first["build_directory"])
    reports = pathlib.Path(first["report_directory"])
    salt = first["cache_namespace"]
    # Kache 0.28 executes workspace-wrapper chains uncached, while ordinary dependencies remain
    # cached. Qualify an actual dependency hit and separately remove the complete side report
    # from a Cargo-fresh authored target. Neither case may turn missing evidence into zero.
    started = int(time.time() * 1000)
    hits = []
    for _ in range(3):
        command(["cargo", "+nightly-2026-09-17", "clean", "--manifest-path", configuration.manifest, "--target-dir", str(build), "--package", "nervix-lint-fixture-macros"], cwd=root, env=runner.environment)
        cached = runner.cargo(configuration, build, reports, salt)
        runner.validate_reports(configuration, first["expected"], cached, reports)
        cache = json.loads(command(["kache", "report", "--format", "json", "--since", "15m", "--root", str(target / "typed-ratchet")], cwd=root))
        hits = [event for event in cache["all_events"] if event.get("crate_name") == "nervix_lint_fixture_macros" and "hit" in event.get("result", "") and event.get("start_unix_ms", 0) >= started]
        if hits:
            break
    assert hits, "the configured cache did not restore the warmed fixture dependency"
    for path in reports.glob("*.json"):
        path.unlink()
    recovered = runner.analyze(configuration, fresh=True)
    assert recovered["complete"]
    assert recovered["cache_namespace"] != salt, "missing reports did not establish a new analysis namespace"
    subprocess.run(["just", "test-typed-ratchet-ui"], cwd=root, check=True)
    subprocess.run(["just", "test-typed-ratchet-ui"], cwd=root, check=True)
    return {"root": str(root), "identity": runner.identity, "fresh": first, "cargo_fresh": repeated, "kache_hits": hits, "missing_side_report_recovered": True, "recovered": recovered, "trybuild": "passed twice with configured wrapper"}


def main() -> None:
    target = pathlib.Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target")).resolve()
    directory = target / "typed-ratchet/cache-qualification" / uuid.uuid4().hex
    directory.mkdir(parents=True)
    second_root = directory / "worktree"
    first = qualify(ROOT, target)
    command(["git", "worktree", "add", "--detach", str(second_root), "HEAD"], cwd=ROOT)
    try:
        shutil.copytree(ROOT / TOOLING, second_root / TOOLING,
                        dirs_exist_ok=True, ignore=shutil.ignore_patterns("target", "__pycache__"))
        for name in ("justfile", "Cargo.toml", "scripts/typed_ratchet.py", "scripts/typed_lint_wrapper.py", "scripts/ratchet.py"):
            shutil.copy2(ROOT / name, second_root / name)
        second_target = directory / "second-target"
        binaries = second_target / "typed-ratchet/driver/debug"
        binaries.mkdir(parents=True)
        for name in ("nervix-lint-driver", "nervix-lint-report"):
            shutil.copy2(target / "typed-ratchet/driver/debug" / name, binaries / name)
        previous_target = os.environ.get("CARGO_TARGET_DIR")
        os.environ["CARGO_TARGET_DIR"] = str(second_target)
        try:
            second = qualify(second_root, second_target)
        finally:
            if previous_target is None:
                os.environ.pop("CARGO_TARGET_DIR")
            else:
                os.environ["CARGO_TARGET_DIR"] = previous_target
        assert first["identity"] != second["identity"], "worktree identity was not part of the cache key"
        runner = Runner(second_root, second_target)
        configuration = Configuration("fixture-ordinary", str(TOOLING / "fixtures/Cargo.toml"), ())
        evidence = second["recovered"]
        crate_name = next(iter(evidence["reports"].values()))["crate_name"]
        path = next(iter(pathlib.Path(evidence["report_directory"]).glob(crate_name + "-*.json")))
        report = json.loads(path.read_text())
        report["identity"] = first["identity"]
        atomic_json(path, report)
        messages = [json.loads(line) for line in pathlib.Path(evidence["cargo_log"]).read_text().splitlines()]
        try:
            runner.validate_reports(configuration, evidence["expected"], messages, pathlib.Path(evidence["report_directory"]))
        except AnalysisError as error:
            assert "cross-worktree" in str(error), error
        else:
            raise AssertionError("a side report from another worktree was accepted")
        atomic_json(directory / "qualification.json", {"complete": True, "first": first, "second": second, "cross_worktree_rejected": True, "wrapper": "configured RUSTC_WRAPPER preserved"})
        print(f"typed ratchet cache qualification: {directory / 'qualification.json'}")
    finally:
        command(["git", "worktree", "remove", "--force", str(second_root)], cwd=ROOT)


if __name__ == "__main__":
    main()
