"""Qualify current compiler artifacts and paired doctests in two isolated worktrees."""

from __future__ import annotations

import json
import os
import pathlib
import shutil
import signal
import subprocess
import sys
import time
import uuid

from scripts.typed_ratchet import AnalysisError, Configuration, ROOT, Runner, TOOLCHAIN, TOOLING, atomic_json, command

ORDINARY = Configuration("fixture-ordinary", str(TOOLING / "fixtures/Cargo.toml"), ())


def qualify(root: pathlib.Path, target: pathlib.Path) -> dict:
    runner = Runner(root, target)
    first = runner.analyze(ORDINARY, fresh=True)
    repeated = runner.analyze(ORDINARY, fresh=True)
    assert repeated["reports"] == first["reports"], "Cargo-fresh analysis changed its complete reports"
    messages = [json.loads(line) for line in pathlib.Path(repeated["cargo_log"]).read_text().splitlines()]
    assert any(item.get("reason") == "compiler-artifact" and item.get("fresh") is True for item in messages)
    build, reports = pathlib.Path(first["build_directory"]), pathlib.Path(first["report_directory"])
    salt = first["cache_namespace"]
    # Kache executes workspace-wrapper chains uncached; ordinary dependencies still use its cache.
    started, hits = int(time.time() * 1000), []
    for _ in range(3):
        command(["cargo", f"+{TOOLCHAIN}", "clean", "--manifest-path", ORDINARY.manifest,
                 "--target-dir", str(build), "--package", "nervix-lint-fixture-macros"], cwd=root, env=runner.environment)
        cached = runner.cargo(ORDINARY, build, reports, salt)
        runner.validate_reports(ORDINARY, first["expected"], cached, reports)
        cache = json.loads(command(["kache", "report", "--format", "json", "--since", "15m", "--root", str(target / "typed-ratchet")], cwd=root))
        hits = [event for event in cache["all_events"] if event.get("crate_name") == "nervix_lint_fixture_macros"
                and "hit" in event.get("result", "") and event.get("start_unix_ms", 0) >= started]
        if hits:
            break
    assert hits, "the configured cache did not restore the warmed fixture dependency"
    for path in reports.glob("*.json"):
        path.unlink()
    recovered = runner.analyze(ORDINARY, fresh=True)
    assert recovered["complete"] and recovered["cache_namespace"] != salt
    for _ in range(2):
        command(["just", "test-typed-ratchet-docs"], cwd=root, env=runner.environment)
    return {"root": str(root), "identity": runner.identity, "fresh": first, "cargo_fresh": repeated,
            "kache_hits": hits, "missing_side_report_recovered": True, "recovered": recovered,
            "paired_doctests": "passed twice with the configured wrapper"}


def copy_current_tree(root: pathlib.Path, destination: pathlib.Path) -> None:
    names = set(command(["git", "diff", "--name-only", "-z", "HEAD"], cwd=root).split("\0"))
    names.update(command(["git", "ls-files", "-z", "--others", "--exclude-standard"], cwd=root).split("\0"))
    for name in sorted(names - {""}):
        source, target = root / name, destination / name
        if source.is_file():
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
        else:
            target.unlink(missing_ok=True)


def findings(evidence: dict) -> list[dict]:
    return [finding for report in evidence["reports"].values() for finding in report["findings"]]


def qualify_changes(root: pathlib.Path, target: pathlib.Path) -> dict:
    source = root / TOOLING / "fixtures/src/lib.rs"
    baseline = source.read_text()
    previous = Runner(root, target)
    original = previous.analyze(ORDINARY)
    operation = "pub fn current_effect(map: &MapAlias) { drop(map.get(&1)); }\n"
    source.write_text(baseline + "\n" + operation)
    changed = Runner(root, target)
    added = changed.analyze(ORDINARY)
    assert changed.identity != previous.identity
    assert len(findings(added)) == len(findings(original)) + 1
    recurring = '#[cfg_attr(nervix_lint, nervix::context(recurring, reason = "handles each batch"))]\n'
    source.write_text(baseline + "\n" + recurring + operation)
    rejected = Runner(root, target)
    try:
        rejected.analyze(ORDINARY)
    except AnalysisError as error:
        assert "compiler run failed" in str(error), error
        assert "nervix::sync_acquisition" in (rejected.work / ORDINARY.name / "cargo.jsonl").read_text()
        assert not (rejected.work / ORDINARY.name / "completion.json").exists()
    else:
        raise AssertionError("a changed recurring annotation reused the cold result")
    bounded = '#[cfg_attr(nervix_lint, nervix::context(bounded, reason = "one retained protocol", key = "this map", bound = "one key lookup"))]\n'
    source.write_text(baseline + "\n" + bounded + operation)
    annotated = Runner(root, target).analyze(ORDINARY)
    assert any(item["context"]["context"] == "bounded" for item in findings(annotated))
    moved_source = source.with_name("current_effect.rs")
    moved_source.write_text("use super::MapAlias;\n" + bounded + operation.replace("current_effect", "renamed_effect"))
    source.write_text(baseline + "\nmod current_effect;\n")
    moved = Runner(root, target).analyze(ORDINARY)
    assert any(item["span"]["site"]["path"].endswith("/current_effect.rs") for item in findings(moved))
    rule = root / TOOLING / "report/src/rules.rs"
    rule.write_text(rule.read_text() + "\n// Cache qualification revisits the current rule source.\n")
    command(["just", "typed-ratchet-build"], cwd=root, env=Runner(root, target).environment)
    rule_changed = Runner(root, target).analyze(ORDINARY)
    assert rule_changed["cache_namespace"] != moved["cache_namespace"]
    dependency = root / TOOLING / "fixture-macros/src/lib.rs"
    dependency.write_text(dependency.read_text() + "\n// Changed authored macro dependency.\n")
    dependency_changed = Runner(root, target).analyze(ORDINARY)
    assert dependency_changed["cache_namespace"] != rule_changed["cache_namespace"]
    shuttle = Configuration("fixture-shuttle", ORDINARY.manifest, (), features=("shuttle",))
    configured = Runner(root, target).analyze(shuttle)
    assert configured["configuration"]["features"] == ["shuttle"]
    assert any("shuttle" in item["receiver"] for item in findings(configured))
    return {"changed_source": added, "changed_annotation_rejected": True, "bounded_annotation": annotated,
            "moved_and_renamed_source": moved, "changed_rule": rule_changed,
            "changed_dependency": dependency_changed, "changed_configuration": configured}


def qualify_interruption(root: pathlib.Path, target: pathlib.Path, directory: pathlib.Path) -> dict:
    output, log = directory / "interrupted.json", directory / "interrupted.log"
    runner = Runner(root, target)
    with log.open("w") as stream:
        process = subprocess.Popen([sys.executable, "-m", "scripts.typed_ratchet", "--root", str(root),
            "--target-dir", str(target), "--fixture-mode", "ordinary", "--recompile", "--output", str(output)],
            cwd=root, env=runner.environment, stdout=stream, stderr=stream, start_new_session=True)
        try:
            deadline = time.monotonic() + 30
            while "typed ratchet: fixture-ordinary: cargo" not in log.read_text():
                assert process.poll() is None, "compiler run finished before interruption"
                assert time.monotonic() < deadline, "compiler run never reached Cargo"
                time.sleep(0.01)
            os.killpg(process.pid, signal.SIGTERM)
            assert process.wait(timeout=30) != 0
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
    assert not output.exists(), "an interrupted run published a complete output"
    assert not (runner.work / ORDINARY.name / "completion.json").exists()
    recovered = runner.analyze(ORDINARY, fresh=True)
    assert recovered["complete"]
    return {"interrupted_output_absent": True, "resumed": recovered, "log": str(log)}


def main() -> None:
    target = pathlib.Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target")).resolve()
    directory = target / "typed-ratchet/cache-qualification" / uuid.uuid4().hex
    directory.mkdir(parents=True)
    second_root = directory / "worktree"
    first = qualify(ROOT, target)
    command(["git", "worktree", "add", "--detach", str(second_root), "HEAD"], cwd=ROOT)
    try:
        copy_current_tree(ROOT, second_root)
        second_target = directory / "second-target"
        binaries = second_target / "typed-ratchet/driver/debug"
        binaries.mkdir(parents=True)
        for name in ("nervix-lint-driver", "nervix-lint-report"):
            shutil.copy2(target / "typed-ratchet/driver/debug" / name, binaries / name)
        previous_target = os.environ.get("CARGO_TARGET_DIR")
        os.environ["CARGO_TARGET_DIR"] = str(second_target)
        try:
            second = qualify(second_root, second_target)
            assert first["identity"] != second["identity"], "worktree identity was not part of the cache key"
            runner = Runner(second_root, second_target)
            evidence = second["recovered"]
            crate_name = next(iter(evidence["reports"].values()))["crate_name"]
            path = next(iter(pathlib.Path(evidence["report_directory"]).glob(crate_name + "-*.json")))
            report = json.loads(path.read_text())
            report["identity"] = first["identity"]
            atomic_json(path, report)
            messages = [json.loads(line) for line in pathlib.Path(evidence["cargo_log"]).read_text().splitlines()]
            try:
                runner.validate_reports(ORDINARY, evidence["expected"], messages, pathlib.Path(evidence["report_directory"]))
            except AnalysisError as error:
                assert "cross-worktree" in str(error), error
            else:
                raise AssertionError("a side report from another worktree was accepted")
            changes = qualify_changes(second_root, second_target)
            interrupted = qualify_interruption(second_root, second_target, directory)
        finally:
            if previous_target is None:
                os.environ.pop("CARGO_TARGET_DIR")
            else:
                os.environ["CARGO_TARGET_DIR"] = previous_target
        atomic_json(directory / "qualification.json", {"complete": True, "first": first, "second": second,
            "changes": changes, "interruption": interrupted, "cross_worktree_rejected": True,
            "wrapper": "configured RUSTC_WRAPPER preserved"})
        print(f"typed ratchet cache qualification: {directory / 'qualification.json'}")
    finally:
        command(["git", "worktree", "remove", "--force", str(second_root)], cwd=ROOT)


if __name__ == "__main__":
    main()
