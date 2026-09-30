"""Run the pinned compiler acquisition inventory and require complete, current evidence.

The ordinary Rust source scanner retains its disjoint rules. This module owns Cargo execution,
worktree/cache identity and completion; the driver owns resolved API classification. Inventory
mode reports acquisitions without claiming that reviewed policy scopes have passed.
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import os
import pathlib
import re
import subprocess
import sys
import tomllib
import uuid
from typing import Any

from scripts.ratchet import RustFile

ROOT = pathlib.Path(__file__).resolve().parents[1]
TOOLCHAIN = "nightly-2026-09-17"
TOOLING = pathlib.Path("tools/nervix-lint")


class AnalysisError(Exception):
    """Compiler, declared coverage, freshness or scope validation failed."""


@dataclasses.dataclass(frozen=True)
class Configuration:
    name: str
    manifest: str
    packages: tuple[str, ...]
    kinds: tuple[str, ...] = ("lib",)
    features: tuple[str, ...] = ()
    target: str | None = None

    def cargo_arguments(self) -> list[str]:
        args = ["--manifest-path", self.manifest]
        if self.packages:
            for package in self.packages:
                args.extend(["--package", package])
        else:
            args.append("--workspace")
        for kind in self.kinds:
            args.append("--bins" if kind == "bin" else "--lib")
        if self.features:
            args.extend(["--features", " ".join(self.features)])
        if self.target:
            args.extend(["--target", self.target])
        return args


def digest(path: pathlib.Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def encode(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def atomic_json(path: pathlib.Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f".{os.getpid()}.tmp")
    temporary.write_bytes(encode(value) + b"\n")
    temporary.replace(path)


def command(args: list[str], *, cwd: pathlib.Path, env: dict[str, str] | None = None) -> str:
    completed = subprocess.run(args, cwd=cwd, env=env, text=True, capture_output=True)
    if completed.returncode:
        raise AnalysisError(f"{' '.join(args)} failed:\n{completed.stderr or completed.stdout}")
    return completed.stdout


def compiler_identity(root: pathlib.Path) -> str:
    return command(["rustup", "run", TOOLCHAIN, "rustc", "-vV"], cwd=root)


def source_inputs(root: pathlib.Path) -> dict[str, str]:
    tracked = command(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=root
    )
    inputs = {}
    for name in sorted(set(tracked.split("\0"))):
        path = root / name
        # The reviewed policy is validated on every invocation and fingerprinted separately.
        if name in {"debt-baseline.json", str(TOOLING / "scopes.json"), str(TOOLING / "review-context.json")}:
            continue
        if name and path.is_file() and path.suffix in {".rs", ".toml", ".lock", ".fbs", ".proto", ".json"}:
            inputs[name] = digest(path)
    return inputs


def load_configurations(root: pathlib.Path) -> list[Configuration]:
    with (root / TOOLING / "configurations.toml").open("rb") as stream:
        data = tomllib.load(stream)
    configurations = []
    names = set()
    for item in data["configuration"]:
        allowed = {"name", "manifest", "packages", "kinds", "features", "target"}
        if set(item) - allowed or not {"name", "manifest", "packages"} <= set(item):
            raise AnalysisError("configuration has missing or unknown fields")
        if item["name"] in names:
            raise AnalysisError(f"duplicate configuration {item['name']}")
        names.add(item["name"])
        configuration = Configuration(
            name=item["name"],
            manifest=item["manifest"],
            packages=tuple(item["packages"]),
            kinds=tuple(item.get("kinds", ["lib"])),
            features=tuple(item.get("features", [])),
            target=item.get("target"),
        )
        if not configuration.kinds or set(configuration.kinds) - {"lib", "bin"}:
            raise AnalysisError(f"unsupported targets in {configuration.name}")
        modes = set(configuration.features) & {"loom", "shuttle", "turmoil"}
        if len(modes) > 1:
            raise AnalysisError(f"incompatible modes in {configuration.name}")
        configurations.append(configuration)
    if not configurations:
        raise AnalysisError("no declared configurations")
    return configurations


class Runner:
    def __init__(self, root: pathlib.Path, target: pathlib.Path) -> None:
        self.root = root.resolve()
        self.target = target.resolve()
        self.driver = self.target / "typed-ratchet/driver/debug/nervix-lint-driver"
        self.wrapper = self.root / "scripts/typed_lint_wrapper.py"
        if not self.driver.is_file():
            raise AnalysisError("missing compiler driver; run just typed-ratchet-build")
        self.compiler = compiler_identity(self.root)
        self.inputs = source_inputs(self.root)
        self.environment = dict(os.environ)
        if self.environment.get("RUSTC_WORKSPACE_WRAPPER"):
            raise AnalysisError("RUSTC_WORKSPACE_WRAPPER is already set; cannot compose another driver")
        sysroot = command(["rustup", "run", TOOLCHAIN, "rustc", "--print", "sysroot"], cwd=self.root).strip()
        library_path = str(pathlib.Path(sysroot) / "lib")
        previous = self.environment.get("LD_LIBRARY_PATH")
        self.environment["LD_LIBRARY_PATH"] = library_path + (":" + previous if previous else "")
        payload = {
            "root": str(self.root),
            "compiler": self.compiler,
            "driver": digest(self.driver),
            "workspace_wrapper": digest(self.wrapper),
            "report_validator": digest(self.target / "typed-ratchet/driver/debug/nervix-lint-report"),
            "inputs": self.inputs,
            "cargo_configuration": self.cargo_configuration(),
            "environment": {
                key: value for key, value in self.environment.items()
                if key in {"RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "RUSTC", "RUSTC_WRAPPER", "KACHE_KEY_SALT"}
                or key.startswith(("CARGO_BUILD_", "CARGO_TARGET_", "CARGO_PROFILE_"))
            },
        }
        self.identity = hashlib.sha256(encode(payload)).hexdigest()
        self.work = self.target / "typed-ratchet/evidence" / self.identity
        self.work.mkdir(parents=True, exist_ok=True)
        atomic_json(self.work / "identity.json", payload)

    def cargo_configuration(self) -> dict[str, str]:
        paths = []
        cargo_home = pathlib.Path(self.environment.get("CARGO_HOME", pathlib.Path.home() / ".cargo"))
        paths.extend([cargo_home / "config", cargo_home / "config.toml"])
        for ancestor in [self.root, *self.root.parents]:
            paths.extend([ancestor / ".cargo/config", ancestor / ".cargo/config.toml"])
        return {str(path): digest(path) for path in paths if path.is_file()}

    def expected_targets(self, configuration: Configuration) -> dict[str, dict[str, Any]]:
        args = ["cargo", f"+{TOOLCHAIN}", "metadata", "--format-version", "1", "--no-deps", "--manifest-path", configuration.manifest]
        metadata = json.loads(command(args, cwd=self.root, env=self.environment))
        members = set(metadata["workspace_members"])
        requested = set(configuration.packages)
        targets = {}
        selected = set()
        for package in metadata["packages"]:
            if package["id"] not in members:
                continue
            required = not requested or package["name"] in requested
            if required:
                selected.add(package["name"])
            for target in package["targets"]:
                kind = "lib" if set(target["kind"]) & {"lib", "rlib", "cdylib", "staticlib", "proc-macro"} else target["kind"][0]
                if kind not in configuration.kinds:
                    continue
                if not required and kind != "lib":
                    continue
                key = package["id"] + "::" + target["name"] + "::" + kind
                targets[key] = {"package": package["name"], "source": target["src_path"], "kind": kind, "required": required}
        if requested - selected:
            raise AnalysisError(f"unknown requested packages: {sorted(requested - selected)}")
        if not targets:
            raise AnalysisError(f"{configuration.name}: no declared targets")
        return targets

    def cargo(self, configuration: Configuration, build: pathlib.Path, reports: pathlib.Path, salt: str) -> list[dict[str, Any]]:
        env = dict(self.environment)
        env.update({
            "CARGO_TARGET_DIR": str(build),
            "RUSTC_WORKSPACE_WRAPPER": str(self.wrapper),
            "NERVIX_LINT_DRIVER": str(self.driver),
            "NERVIX_LINT_ROOT": str(self.root),
            "NERVIX_LINT_IDENTITY": self.identity,
            "NERVIX_LINT_CONFIGURATION": configuration.name,
            "NERVIX_LINT_REPORTS": str(reports),
            "KACHE_KEY_SALT": salt,
        })
        reports.mkdir(parents=True, exist_ok=True)
        args = ["cargo", f"+{TOOLCHAIN}", "check", *configuration.cargo_arguments(), "--message-format=json"]
        log = reports.parent / "cargo.jsonl"
        print(f"typed ratchet: {configuration.name}: {' '.join(args)}", flush=True)
        with log.open("w") as stream:
            process = subprocess.Popen(args, cwd=self.root, env=env, stdout=stream, stderr=None, text=True)
            status = process.wait()
        messages = []
        for line in log.read_text().splitlines():
            try:
                message = json.loads(line)
            except json.JSONDecodeError:
                continue
            if message.get("reason") == "compiler-message" and message["message"]["level"] == "error":
                print(message["message"].get("rendered", message["message"]["message"]), file=sys.stderr)
            messages.append(message)
        if status or not any(message.get("reason") == "build-finished" and message.get("success") is True for message in messages):
            raise AnalysisError(f"{configuration.name}: compiler run failed or did not finish; evidence: {log}")
        return messages

    def validate_reports(self, configuration: Configuration, expected: dict[str, Any], messages: list[dict[str, Any]], reports: pathlib.Path) -> dict[str, Any]:
        covered = set()
        accepted = {}
        files = {}
        sources: dict[str, RustFile] = {}
        for message in messages:
            if message.get("reason") != "compiler-artifact":
                continue
            target = message["target"]
            kind = "lib" if set(target["kind"]) & {"lib", "rlib", "cdylib", "staticlib", "proc-macro"} else target["kind"][0]
            key = message["package_id"] + "::" + target["name"] + "::" + kind
            if key not in expected:
                continue
            covered.add(key)
            name = target["name"].replace("-", "_")
            candidates = []
            for artifact in message["filenames"]:
                path = pathlib.Path(artifact)
                stem = path.name.split(".", 1)[0]
                stem = stem.removeprefix("lib")
                if stem.startswith(name + "-"):
                    candidate = reports / (stem + ".json")
                    if candidate.is_file():
                        candidates.append(candidate)
                if not path.is_file():
                    raise AnalysisError(f"{configuration.name}: missing Cargo artifact {path}")
                files[str(path)] = digest(path)
            candidates = sorted(set(candidates))
            if len(candidates) != 1:
                raise AnalysisError(f"{configuration.name}: {key}: missing or ambiguous complete compiler report")
            path = candidates[0]
            report = json.loads(path.read_text())
            if report.get("complete") is not True or report.get("identity") != self.identity:
                raise AnalysisError(f"{configuration.name}: partial, stale or cross-worktree report {path}")
            if report.get("compiler") != self.compiler or report.get("configuration") != configuration.name:
                raise AnalysisError(f"{configuration.name}: mismatched compiler/configuration report {path}")
            if report.get("crate_name") != name:
                raise AnalysisError(f"{configuration.name}: mismatched target report {path}")
            actual_source = pathlib.Path(report["crate_source"])
            if not actual_source.is_absolute():
                actual_source = self.root / actual_source
            if actual_source.resolve() != pathlib.Path(target["src_path"]).resolve():
                raise AnalysisError(f"{configuration.name}: mismatched source report {path}")
            for finding in report["findings"]:
                site = finding["span"]["site"]
                source_path = self.root / site["path"]
                if not source_path.is_file():
                    raise AnalysisError(f"{configuration.name}: missing authored source {source_path}")
                if site["path"] not in sources:
                    sources[site["path"]] = RustFile(site["path"], source_path.read_text())
                source = sources[site["path"]]
                # RustFile offsets are Unicode characters; compiler BytePos offsets are bytes.
                prefix = source_path.read_bytes()[:site["start"]].decode()
                origins = source.macro_origins(len(prefix))
                for origin in origins:
                    if origin not in finding["expansion"]:
                        finding["expansion"].append(origin)
            accepted[key] = report
            files[str(path)] = digest(path)
        required = {key for key, target in expected.items() if target.get("required", True)}
        if required - covered:
            raise AnalysisError(f"{configuration.name}: incomplete declared target coverage: {sorted(required - covered)}")
        return {"configuration": json.loads(encode(dataclasses.asdict(configuration))), "expected": expected, "reports": accepted, "files": files, "complete": True}

    def clean_authored_artifacts(self, configuration: Configuration, expected: dict[str, Any], build: pathlib.Path) -> None:
        packages = sorted({target["package"] for target in expected.values()})
        args = ["cargo", f"+{TOOLCHAIN}", "clean", "--manifest-path", configuration.manifest, "--target-dir", str(build)]
        for package in packages:
            args.extend(["--package", package])
        # Only this isolated analysis build's authored artifacts are discarded. Dependency
        # artifacts and kache's shared storage remain available in the approved namespace.
        command(args, cwd=self.root, env=self.environment)

    def analyze(self, configuration: Configuration, *, fresh: bool = False) -> dict[str, Any]:
        directory = self.work / configuration.name
        directory.mkdir(parents=True, exist_ok=True)
        completion = directory / "completion.json"
        expected = self.expected_targets(configuration)
        previous = None
        if completion.is_file():
            evidence = json.loads(completion.read_text())
            if evidence.get("complete") is True and evidence.get("expected") == expected:
                if evidence.get("files") and all(pathlib.Path(path).is_file() and digest(pathlib.Path(path)) == value for path, value in evidence["files"].items()):
                    log = pathlib.Path(evidence["cargo_log"])
                    messages = [json.loads(line) for line in log.read_text().splitlines()]
                    validated = self.validate_reports(configuration, expected, messages, pathlib.Path(evidence["report_directory"]))
                    if validated["reports"] != evidence["reports"]:
                        raise AnalysisError(f"{configuration.name}: completion disagrees with its compiler reports")
                    previous = evidence
                    if not fresh:
                        return evidence
        completion.unlink(missing_ok=True)
        salt = self.environment.get("KACHE_KEY_SALT", "") + ":nervix-analysis:" + self.identity
        build = self.target / "typed-ratchet/build" / configuration.name
        reports = directory / "reports"
        if previous:
            build = pathlib.Path(previous["build_directory"])
            reports = pathlib.Path(previous["report_directory"])
            salt = previous["cache_namespace"]
        messages = self.cargo(configuration, build, reports, salt)
        try:
            evidence = self.validate_reports(configuration, expected, messages, reports)
        except AnalysisError as failure:
            # Kache can restore compiler outputs without our side report. Re-establish analysis
            # through its documented key-salt namespace; RUSTC_WRAPPER stays configured throughout.
            attempt = uuid.uuid4().hex
            print(f"typed ratchet: {failure}; re-establishing compiler analysis in cache namespace {attempt}", flush=True)
            self.clean_authored_artifacts(configuration, expected, build)
            reports = directory / "reanalysis" / attempt / "reports"
            salt += ":" + attempt
            messages = self.cargo(configuration, build, reports, salt)
            evidence = self.validate_reports(configuration, expected, messages, reports)
        if source_inputs(self.root) != self.inputs:
            raise AnalysisError("source/configuration/dependency inputs changed during compiler analysis")
        log = reports.parent / "cargo.jsonl"
        evidence.update({"build_directory": str(build), "report_directory": str(reports), "cache_namespace": salt, "cargo_log": str(log)})
        evidence["files"][str(log)] = digest(log)
        atomic_json(completion, evidence)
        required = sum(target.get("required", True) for target in expected.values())
        print(f"typed ratchet: {configuration.name}: {required} required targets and {len(evidence['reports'])} observed workspace targets complete", flush=True)
        return evidence


def review_inputs(inputs: dict[str, str]) -> dict[str, str]:
    """Bind frequency reviews to callers and feature inputs as well as acquisition bodies.

    This selects the product source context, not a hotness policy. Tooling implementation changes
    invalidate compiler evidence; its catalog and declared configurations invalidate reviews too.
    """
    declarations = {str(TOOLING / "catalog.json"), str(TOOLING / "configurations.toml"), str(TOOLING / "rust-toolchain.toml")}
    return {name: value for name, value in inputs.items()
            if not name.startswith(str(TOOLING) + "/") or name in declarations}


def policy(runner: Runner, evidence: list[dict[str, Any]], *, inventory: bool) -> list[dict[str, Any]]:
    if not inventory:
        context = json.loads((runner.root / TOOLING / "review-context.json").read_text())
        if not isinstance(context, dict) or set(context) != {"inputs"}:
            raise AnalysisError("invalid review context; expected reviewed input fingerprints")
        reviewed = context["inputs"]
        if not isinstance(reviewed, dict) or any(not isinstance(name, str) or not isinstance(value, str)
                                                 for name, value in reviewed.items()):
            raise AnalysisError("invalid review context; expected source names and fingerprints")
        current = review_inputs(runner.inputs)
        if reviewed != current:
            changed = sorted(name for name in set(reviewed) | set(current)
                             if reviewed.get(name) != current.get(name))
            raise AnalysisError(f"stale review context; re-review affected callers and scopes: {changed}")
    reports = [report for configuration in evidence for report in configuration["reports"].values()]
    scopes = [] if inventory else json.loads((runner.root / TOOLING / "scopes.json").read_text())
    payload = {"reports": reports, "scopes": scopes, "source_sha256": runner.inputs}
    executable = runner.target / "typed-ratchet/driver/debug/nervix-lint-report"
    args = [str(executable), *( ["--inventory"] if inventory else [])]
    if os.environ.get("NERVIX_NATIVE_COVERAGE_ATTEMPT"):
        args = [sys.executable, str(runner.root / "scripts/native_coverage.py"), "exec", *args]
    completed = subprocess.run(args, input=encode(payload), capture_output=True)
    if completed.returncode:
        raise AnalysisError(f"report/scope validation failed:\n{completed.stderr.decode()}")
    return json.loads(completed.stdout)


def render_finding(classified: dict[str, Any]) -> str:
    acquisition, scope = classified["acquisition"], classified["scope"]
    findings = [finding for values in acquisition["configurations"].values() for finding in values]
    operations = sorted({f"{finding['receiver_type']}.{finding['operation']} ({finding['acquisition']})" for finding in findings})
    origins = sorted({origin["macro_name"] + " at " + origin["call_site"] for finding in findings for origin in finding["expansion"]})
    text = f"data_plane_lock_acquisitions: {', '.join(operations)}; {scope['disposition']['class']}; owner {', '.join(scope['owners'])}; frequency {scope['frequency']}; {scope['rationale']}; configurations {', '.join(acquisition['configurations'])}"
    if scope["disposition"]["class"] == "bounded_protocol":
        text += f"; key {scope['disposition']['key']}; bound {scope['disposition']['bound']}"
    if scope["disposition"]["class"] == "debt":
        text += "; delivery " + scope["disposition"]["delivery"]
    if origins:
        text += "; expansions " + ", ".join(origins)
    return text


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=pathlib.Path, default=ROOT)
    parser.add_argument("--target-dir", type=pathlib.Path, default=pathlib.Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target")))
    parser.add_argument("--configuration", action="append")
    parser.add_argument("--fixture-mode", choices=["ordinary", "shuttle", "loom", "turmoil"])
    parser.add_argument("--fresh", action="store_true", help="run Cargo even when complete evidence already exists")
    parser.add_argument("--recompile", action="store_true", help="discard authored artifacts in the isolated analysis build before running Cargo")
    parser.add_argument("--inventory", action="store_true", help="report acquisitions; do not claim the reviewed policy gate passed")
    parser.add_argument("--output", type=pathlib.Path)
    parser.add_argument("--show", action="store_true", help="print every classified finding with its review")
    args = parser.parse_args(argv)
    try:
        runner = Runner(args.root, args.target_dir)
        if args.fixture_mode:
            features = () if args.fixture_mode == "ordinary" else (args.fixture_mode,)
            configurations = [Configuration("fixture-" + args.fixture_mode, str(TOOLING / "fixtures/Cargo.toml"), (), features=features)]
        else:
            configurations = load_configurations(args.root)
            if args.configuration:
                if not args.inventory:
                    raise AnalysisError("selected configurations are inventory only; the policy gate requires the whole declared matrix")
                selected = set(args.configuration)
                configurations = [configuration for configuration in configurations if configuration.name in selected]
                if {configuration.name for configuration in configurations} != selected:
                    raise AnalysisError("unknown requested configuration")
        evidence = []
        for configuration in configurations:
            if args.recompile:
                runner.clean_authored_artifacts(configuration, runner.expected_targets(configuration), runner.target / "typed-ratchet/build" / configuration.name)
            if "turmoil" in configuration.features:
                nested_output = runner.work / "turmoil.json"
                nested = ["just", "typed-ratchet-turmoil", "--root", str(args.root), "--target-dir", str(args.target_dir), "--inventory", "--configuration", configuration.name, "--output", str(nested_output)]
                if args.fixture_mode:
                    nested = ["just", "typed-ratchet-turmoil", "--fixture-mode", "turmoil", "--inventory", "--output", str(nested_output)]
                if args.fresh:
                    nested.append("--fresh")
                if args.recompile:
                    nested.append("--recompile")
                if "tokio_unstable" not in os.environ.get("RUSTFLAGS", ""):
                    completed = subprocess.run(nested, cwd=args.root)
                    if completed.returncode:
                        raise AnalysisError("Turmoil configuration failed through its just recipe")
                    separate = json.loads(nested_output.read_text())
                    if separate.get("complete") is not True or separate.get("compiler") != runner.compiler or separate.get("root") != str(runner.root):
                        raise AnalysisError("Turmoil evidence is incomplete or belongs to another compiler/worktree")
                    if [entry["configuration"]["name"] for entry in separate["evidence"]] != [configuration.name]:
                        raise AnalysisError("Turmoil evidence covers different configurations")
                    evidence.extend(separate["evidence"])
                    continue
            evidence.append(runner.analyze(configuration, fresh=args.fresh or args.recompile))
        sites = policy(runner, evidence, inventory=args.inventory)
        report = {"identity": runner.identity, "compiler": runner.compiler, "root": str(runner.root), "configurations": [entry["configuration"] for entry in evidence], "evidence": evidence, "findings": sites, "complete": True, "policy_checked": not args.inventory}
        if not args.inventory:
            report["policy_sha256"] = digest(runner.root / TOOLING / "scopes.json")
            report["review_context_sha256"] = digest(runner.root / TOOLING / "review-context.json")
            report["debt"] = sum(site["scope"]["disposition"]["class"] == "debt" for site in sites)
            if args.show:
                for site in sites:
                    acquisition = site["acquisition"]
                    finding = next(iter(acquisition["configurations"].values()))[0]
                    print(f"{acquisition['site']['path']}:{finding['span']['line']}:{finding['span']['column'] + 1}: {render_finding(site)}")
        destination = args.output or runner.work / "inventory.json"
        atomic_json(destination, report)
        print(f"typed ratchet: {len(report['findings'])} authored acquisition sites; inventory: {destination}")
        return 0
    except (AnalysisError, OSError, ValueError, KeyError) as error:
        print(f"typed ratchet: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
