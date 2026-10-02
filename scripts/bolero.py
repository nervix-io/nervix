"""Inventory, ordinary regression, and coverage-guided Bolero target runner."""

from __future__ import annotations

import argparse
import dataclasses
import datetime
import hashlib
import json
import os
import pathlib
import re
import shlex
import shutil
import signal
import subprocess
import sys
import time
import tomllib
from collections import Counter
from typing import Any, Iterator

ROOT = pathlib.Path(__file__).resolve().parents[1]
INVENTORY = ROOT / "tests/bolero-targets.toml"
RUNS = ROOT / "target/bolero/runs"
QUALIFICATION = ROOT / "tests/bolero-qualification/Cargo.toml"
TARGET_NAME = re.compile(r"^[a-z][a-z0-9-]*$")
TEST_LINE = re.compile(r"^(\S+): test$", re.MULTILINE)
# Cargo names every test executable it builds: a library's or binary's as `unittests <root>`, an
# integration test's by its source path.
EXECUTABLE = re.compile(r"Executable (?P<description>[^(]+?) \((?P<path>[^)]+)\)")
MACRO = re.compile(r"\bbolero::check!\s*\(")
UNQUALIFIED_CHECK = re.compile(r"(?<![:\w])check!\s*\(")
FUNCTION = re.compile(r"\bfn\s+([A-Za-z_][A-Za-z_0-9]*)\s*\(")


class BoleroError(Exception):
    """A validation or execution failure that should fail the just recipe."""


@dataclasses.dataclass(frozen=True)
class Target:
    id: str
    package: str
    test_target: str
    test: str
    source: pathlib.Path
    features: tuple[str, ...]
    domain_version: int
    corpus: pathlib.Path
    random_iterations: int
    max_input_bytes: int
    case_timeout_seconds: int
    invariant: str
    manifest: pathlib.Path | None = None

    @property
    def work_dir(self) -> pathlib.Path:
        return self.corpus.parent

    @property
    def crashes(self) -> pathlib.Path:
        return self.work_dir / "crashes"


@dataclasses.dataclass(frozen=True)
class Inventory:
    cargo_bolero: str
    nightly: str
    sanitizer: str
    pr_fuzz_seconds: int
    campaign_fuzz_seconds: int
    targets: tuple[Target, ...]


def positive_int(value: Any, name: str) -> int:
    if type(value) is not int or value <= 0:
        raise BoleroError(f"{name} must be a positive integer")
    return value


def repo_path(value: Any, name: str) -> pathlib.Path:
    if not isinstance(value, str) or not value:
        raise BoleroError(f"{name} must be a repository-relative path")
    path = pathlib.Path(value)
    if path.is_absolute() or ".." in path.parts or path.as_posix() != value:
        raise BoleroError(f"{name} must be a normalized repository-relative path")
    return ROOT / path


def load_inventory(path: pathlib.Path = INVENTORY) -> Inventory:
    with path.open("rb") as file:
        data = tomllib.load(file)
    if set(data) != {"tool", "target"}:
        raise BoleroError("inventory must have only tool and target sections")
    tool = data["tool"]
    if set(tool) != {
        "cargo_bolero",
        "nightly",
        "sanitizer",
        "pr_fuzz_seconds",
        "campaign_fuzz_seconds",
    }:
        raise BoleroError("tool section has missing or unknown fields")
    if not re.fullmatch(r"\d+\.\d+\.\d+", tool["cargo_bolero"]):
        raise BoleroError("cargo_bolero must pin one exact version")
    if not re.fullmatch(r"nightly-\d{4}-\d{2}-\d{2}", tool["nightly"]):
        raise BoleroError("nightly must pin a dated toolchain")
    if tool["sanitizer"] not in {"address", "memory", "thread"}:
        raise BoleroError("sanitizer must name a real sanitizer")
    targets = []
    for item in data["target"]:
        expected = {
            "id",
            "package",
            "test_target",
            "test",
            "source",
            "features",
            "domain_version",
            "corpus",
            "random_iterations",
            "max_input_bytes",
            "case_timeout_seconds",
            "invariant",
            "manifest",
        }
        if set(item) != expected:
            raise BoleroError(f"target {item.get('id')} has missing or unknown fields")
        if not TARGET_NAME.fullmatch(item["id"]):
            raise BoleroError(f"invalid target id: {item['id']}")
        if not item["test"].split("::")[-1].startswith("bolero_"):
            raise BoleroError(f"{item['id']}: test name must start with bolero_")
        if item["test_target"] != "lib" and not re.fullmatch(
            r"test:[A-Za-z_][A-Za-z_0-9-]*", item["test_target"]
        ):
            raise BoleroError(f"{item['id']}: invalid cargo test target")
        if not isinstance(item["features"], list) or any(
            not isinstance(feature, str)
            or feature in {"loom", "shuttle", "turmoil"}
            for feature in item["features"]
        ):
            raise BoleroError(f"{item['id']}: invalid or modeled feature")
        source = repo_path(item["source"], "source")
        corpus = repo_path(item["corpus"], "corpus")
        expected_work_dir = source.parent / "__fuzz__" / item["test"].replace("::", "__")
        if corpus != expected_work_dir / "corpus":
            raise BoleroError(f"{item['id']}: corpus differs from Bolero's actual work directory")
        if not source.is_file():
            raise BoleroError(f"{item['id']}: missing source {source}")
        if not isinstance(item["invariant"], str) or not item["invariant"].strip():
            raise BoleroError(f"{item['id']}: invariant is required")
        targets.append(
            Target(
                id=item["id"],
                package=item["package"],
                test_target=item["test_target"],
                test=item["test"],
                source=source,
                features=tuple(item["features"]),
                domain_version=positive_int(item["domain_version"], "domain_version"),
                corpus=corpus,
                random_iterations=positive_int(
                    item["random_iterations"], "random_iterations"
                ),
                max_input_bytes=positive_int(item["max_input_bytes"], "max_input_bytes"),
                case_timeout_seconds=positive_int(
                    item["case_timeout_seconds"], "case_timeout_seconds"
                ),
                invariant=item["invariant"],
                manifest=repo_path(item["manifest"], "manifest"),
            )
        )
    if not targets:
        raise BoleroError("inventory contains no targets")
    for label, values in {
        "id": [target.id for target in targets],
        "package/test": [
            (target.package, target.test_target, target.test) for target in targets
        ],
        "corpus": [target.corpus for target in targets],
    }.items():
        duplicates = [value for value, count in Counter(values).items() if count > 1]
        if duplicates:
            raise BoleroError(f"duplicate {label}: {duplicates}")
    return Inventory(
        cargo_bolero=tool["cargo_bolero"],
        nightly=tool["nightly"],
        sanitizer=tool["sanitizer"],
        pr_fuzz_seconds=positive_int(tool["pr_fuzz_seconds"], "pr_fuzz_seconds"),
        campaign_fuzz_seconds=positive_int(
            tool["campaign_fuzz_seconds"], "campaign_fuzz_seconds"
        ),
        targets=tuple(targets),
    )


def package_manifests() -> dict[str, pathlib.Path]:
    with (ROOT / "Cargo.toml").open("rb") as file:
        workspace = tomllib.load(file)["workspace"]
    members = list(workspace["members"])
    for tooling in workspace.get("metadata", {}).get("tooling", {}).get("workspaces", []):
        path = repo_path(tooling, "tooling workspace")
        with (path / "Cargo.toml").open("rb") as file:
            isolated = tomllib.load(file)["workspace"]["members"]
        members.extend(str((path / member).relative_to(ROOT)) for member in isolated)
    result = {}
    for member in members:
        path = ROOT / member / "Cargo.toml"
        with path.open("rb") as file:
            manifest = tomllib.load(file)
        name = manifest["package"]["name"]
        if declares_bolero(manifest):
            result[name] = path
    return result


def declares_bolero(manifest: dict[str, Any]) -> bool:
    for key in ("dependencies", "dev-dependencies", "build-dependencies"):
        if "bolero" in manifest.get(key, {}):
            return True
    for target in manifest.get("target", {}).values():
        if declares_bolero(target):
            return True
    return False


def package_rust_sources(manifest: pathlib.Path) -> Iterator[pathlib.Path]:
    for directory, children, files in os.walk(manifest.parent):
        parent = pathlib.Path(directory)
        # Nested Cargo packages own their Rust, including a workspace's qualification crate.
        # Build output and Git metadata are not authored sources of the package being checked.
        children[:] = sorted(
            child for child in children
            if child not in {".git", "target"}
            and not (parent / child / "Cargo.toml").is_file()
        )
        for name in sorted(files):
            if name.endswith(".rs"):
                yield parent / name


def static_targets(manifest: pathlib.Path) -> dict[str, pathlib.Path]:
    found = {}
    for source in package_rust_sources(manifest):
        content = source.read_text()
        for macro in MACRO.finditer(content):
            preceding = list(FUNCTION.finditer(content, 0, macro.start()))
            if not preceding:
                raise BoleroError(f"{source}: Bolero macro has no owning function")
            function = preceding[-1].group(1)
            if not function.startswith("bolero_"):
                raise BoleroError(f"{source}: Bolero property {function} must start with bolero_")
            if function in found:
                raise BoleroError(f"duplicate Bolero function {function}")
            found[function] = source
        if UNQUALIFIED_CHECK.search(content) or re.search(
            r"\buse\s+bolero::(?:check\b|\{[^}]*\bcheck\b)", content
        ):
            raise BoleroError(f"{source}: qualify bolero::check! for inventory scanning")
    return found


def command(
    args: list[str],
    *,
    env: dict[str, str] | None = None,
    timeout: int | None = None,
    log: pathlib.Path | None = None,
    allowed_status: tuple[int, ...] = (0,),
) -> subprocess.CompletedProcess[str]:
    print("+ " + " ".join(args), flush=True)
    options = {
        "cwd": ROOT,
        "env": {**os.environ, **(env or {})},
        "start_new_session": True,
    }
    if log:
        log.parent.mkdir(parents=True, exist_ok=True)
        with log.open("w+b") as stream:
            process = subprocess.Popen(
                args, stdout=stream, stderr=subprocess.STDOUT, **options
            )
            try:
                process.wait(timeout=timeout)
            except subprocess.TimeoutExpired as error:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait()
                stream.write(f"\ntimeout after {timeout}s\n".encode())
                raise BoleroError(f"command timed out after {timeout}s: {args}") from error
            stream.flush()
            stream.seek(max(0, stream.tell() - 1_000_000))
            output = stream.read().decode(errors="replace")
        result = subprocess.CompletedProcess(args, process.returncode, output, "")
    else:
        process = subprocess.Popen(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            errors="replace",
            **options,
        )
        try:
            stdout, stderr = process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired as error:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.communicate()
            raise BoleroError(f"command timed out after {timeout}s: {args}") from error
        result = subprocess.CompletedProcess(args, process.returncode, stdout, stderr)
    output = result.stdout + result.stderr
    if result.returncode not in allowed_status:
        print(output[-12000:], file=sys.stderr)
        raise BoleroError(f"command exited {result.returncode}: {args}")
    return result


def cargo_test_args(target: Target) -> list[str]:
    args = ["cargo", "test", "--package", target.package]
    if target.manifest:
        args.extend(["--manifest-path", str(target.manifest)])
    args.extend(cargo_target_args(target.test_target))
    if target.features:
        args.extend(["--features", ",".join(target.features)])
    return args


def cargo_target_args(test_target: str) -> list[str]:
    if test_target == "lib":
        return ["--lib"]
    return ["--test", test_target.removeprefix("test:")]


def listed_tests(package: str, test_target: str, *, ignored: bool, manifest: pathlib.Path | None = None) -> list[str]:
    args = [
        "cargo",
        "test",
        "--package",
        package,
        *cargo_target_args(test_target),
    ]
    if manifest:
        args.extend(["--manifest-path", str(manifest)])
    args.extend(["bolero_", "--"])
    if ignored:
        args.append("--ignored")
    args.extend(["--list", "--format", "terse"])
    return TEST_LINE.findall(command(args).stdout)


def compiled_targets(package: str, test_target: str, manifest: pathlib.Path | None = None) -> list[dict[str, Any]]:
    args = [
        "cargo",
        "test",
        "--package",
        package,
        *cargo_target_args(test_target),
    ]
    if manifest:
        args.extend(["--manifest-path", str(manifest)])
    args.extend(["bolero_", "--", "--nocapture"])
    result = command(args, env={"CARGO_BOLERO_SELECT": "all"})
    found = []
    for line in result.stdout.splitlines():
        if not line.startswith('{"__bolero_target":'):
            continue
        found.append(json.loads(line))
    return found


def discover(inventory: Inventory) -> None:
    manifests = package_manifests()
    expected_packages = {target.package for target in inventory.targets}
    if set(manifests) != expected_packages:
        raise BoleroError(
            f"Bolero package mismatch: declared={sorted(manifests)}, "
            f"registered={sorted(expected_packages)}"
        )
    registered = {
        (target.package, target.test_target, target.test): target
        for target in inventory.targets
    }
    compiled = []
    for package, manifest in sorted(manifests.items()):
        source_functions = static_targets(manifest)
        registered_functions = {
            target.test.split("::")[-1]: target.source
            for target in inventory.targets
            if target.package == package
        }
        if source_functions != registered_functions:
            raise BoleroError(
                f"{package}: source macros {source_functions} differ from inventory "
                f"{registered_functions}"
            )
        test_targets = {
            target.test_target
            for target in inventory.targets
            if target.package == package
        }
        for test_target in sorted(test_targets):
            names = listed_tests(package, test_target, ignored=False, manifest=manifest)
            ignored = listed_tests(package, test_target, ignored=True, manifest=manifest)
            if ignored:
                raise BoleroError(f"{package}: ignored Bolero targets: {ignored}")
            targets = compiled_targets(package, test_target, manifest=manifest)
            if Counter(names) != Counter(item["test_name"] for item in targets):
                raise BoleroError(
                    f"{package}: listed Bolero tests {names} differ from compiled targets "
                    f"{[item['test_name'] for item in targets]}"
                )
            compiled.extend((test_target, item) for item in targets)
    identities = [
        (item["package_name"], test_target, item["test_name"])
        for test_target, item in compiled
    ]
    if len(identities) != len(set(identities)):
        raise BoleroError("duplicate compiled Bolero target")
    if set(identities) != set(registered):
        raise BoleroError(
            f"compiled targets {sorted(identities)} differ from inventory "
            f"{sorted(registered)}"
        )
    for test_target, item in compiled:
        target = registered[(item["package_name"], test_target, item["test_name"])]
        if pathlib.Path(item["work_dir"]).resolve() != target.work_dir.resolve():
            raise BoleroError(f"{target.id}: compiled work directory differs from inventory")
    print(
        f"Bolero discovered={len(compiled)} selected={len(compiled)} "
        "executed=0 completed=0"
    )


def select(inventory: Inventory, query: str | None) -> tuple[Target, ...]:
    if query is None:
        selected = inventory.targets
    else:
        selected = tuple(
            target
            for target in inventory.targets
            if query in target.id or query in target.test
        )
    if not selected:
        raise BoleroError(f"no Bolero targets selected by {query!r}")
    return selected


def run_dir(target: Target) -> pathlib.Path:
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = RUNS / target.id / f"{stamp}-{time.time_ns()}"
    path.mkdir(parents=True, exist_ok=False)
    return path


def metadata(
    path: pathlib.Path,
    target: Target,
    args: list[str],
    result: str,
    elapsed: float,
) -> None:
    revision = command(["git", "rev-parse", "HEAD"]).stdout.strip()
    inventory = load_inventory()
    saved_inputs = []
    for folder in (path / "crashes", path):
        if not folder.is_dir():
            continue
        for input_file in folder.iterdir():
            if input_file.is_file() and (
                folder.name == "crashes" or input_file.name == "minimized"
            ):
                saved_inputs.append(input_file)
    replay_commands = []
    for input_file in saved_inputs:
        if target.manifest == QUALIFICATION:
            replay_commands.append("just qualify-bolero")
        else:
            replay_commands.append(
                shlex.join(
                    [
                        "just",
                        "fuzz-replay",
                        target.id,
                        str(input_file.relative_to(ROOT)),
                    ]
                )
            )
    data = {
        "revision": revision,
        "target": target.id,
        "package": target.package,
        "test_target": target.test_target,
        "test": target.test,
        "domain_version": target.domain_version,
        "features": target.features,
        "toolchain": inventory.nightly,
        "sanitizer": inventory.sanitizer,
        "rustflags": os.environ.get("RUSTFLAGS", ""),
        "rustc_wrapper": os.environ.get("RUSTC_WRAPPER"),
        "command": args,
        "result": result,
        "elapsed_seconds": round(elapsed, 3),
        "saved_inputs": {
            str(input_file.relative_to(ROOT)): hashlib.sha256(
                input_file.read_bytes()
            ).hexdigest()
            for input_file in saved_inputs
        },
        "replay_commands": replay_commands,
        "campaign_replay_limit": "A random seed reproduces one case, not a whole campaign.",
    }
    (path / "metadata.json").write_text(json.dumps(data, indent=2) + "\n")


def test_targets(inventory: Inventory, selected: tuple[Target, ...]) -> None:
    executed = 0
    completed = 0
    for target in selected:
        args = cargo_test_args(target) + ["--", "--exact", target.test, "--nocapture"]
        env = {
            "BOLERO_RANDOM_ITERATIONS": str(target.random_iterations),
            "BOLERO_RANDOM_MAX_LEN": str(target.max_input_bytes),
            "BOLERO_RANDOM_TEST_TIME_MS": "120000",
        }
        executed += 1
        result = command(args, env=env, timeout=240)
        output = result.stdout + result.stderr
        if not re.search(r"test result: ok\. 1 passed; 0 failed", output):
            raise BoleroError(f"{target.id}: ordinary test did not execute exactly once")
        inputs = re.search(r"corpus inputs: (\d+) \| rng inputs: (\d+)", output)
        if not inputs:
            raise BoleroError(f"{target.id}: Bolero did not report input counts")
        corpus_count, random_count = (int(value) for value in inputs.groups())
        checked_in = sum(
            1 for seed in target.corpus.iterdir()
            if seed.is_file() and not seed.name.startswith(".")
        )
        if checked_in == 0 or corpus_count < checked_in:
            raise BoleroError(f"{target.id}: checked-in corpus was not replayed")
        if random_count != target.random_iterations:
            raise BoleroError(
                f"{target.id}: ran {random_count} random cases, "
                f"expected {target.random_iterations}"
            )
        completed += 1
        print(f"{target.id}: ordinary randomized and corpus replay passed")
    print(
        f"Bolero discovered={len(inventory.targets)} selected={len(selected)} "
        f"executed={executed} completed={completed}"
    )


def bolero_args(inventory: Inventory, target: Target) -> list[str]:
    args = [
        "cargo",
        "bolero",
        "test",
        "--package",
        target.package,
        "--engine",
        "libfuzzer",
        "--sanitizer",
        inventory.sanitizer,
        "--toolchain",
        inventory.nightly,
        "--profile",
        "fuzz",
        "--max-input-length",
        str(target.max_input_bytes),
        "--timeout",
        f"{target.case_timeout_seconds}s",
    ]
    if target.manifest:
        args.extend(["--manifest-path", str(target.manifest)])
    if target.features:
        args.extend(["--features", " ".join(target.features)])
    return args


def verify_tool(inventory: Inventory) -> None:
    result = command(["cargo", "bolero", "--version"])
    if inventory.cargo_bolero not in result.stdout + result.stderr:
        raise BoleroError(
            f"cargo-bolero {inventory.cargo_bolero} required, got "
            f"{result.stdout + result.stderr}"
        )
    command(["rustup", "run", inventory.nightly, "rustc", "--version"])


def resolved_test_target(target: Target) -> tuple[pathlib.Path, pathlib.Path, str]:
    """Read Cargo's current target identity and source root for binary selection."""
    manifest = target.manifest or package_manifests()[target.package]
    result = command([
        "cargo", "metadata", "--no-deps", "--format-version", "1",
        "--manifest-path", str(manifest),
    ])
    packages = [
        package for package in json.loads(result.stdout)["packages"]
        if package["name"] == target.package
    ]
    if len(packages) != 1:
        raise BoleroError(f"{target.id}: expected one Cargo package {target.package}")
    package = packages[0]
    library_kinds = {"lib", "rlib", "dylib", "cdylib", "staticlib", "proc-macro"}
    targets = [
        item for item in package["targets"]
        if (
            target.test_target == "lib" and library_kinds.intersection(item["kind"])
        ) or (
            target.test_target != "lib" and "test" in item["kind"]
            and item["name"] == target.test_target.removeprefix("test:")
        )
    ]
    if len(targets) != 1:
        raise BoleroError(f"{target.id}: expected one Cargo {target.test_target} target")
    selected = targets[0]
    return (
        pathlib.Path(package["manifest_path"]).parent,
        pathlib.Path(selected["src_path"]).resolve(),
        selected["name"].replace("-", "_") + "-",
    )


def build_instrumented(
    inventory: Inventory, target: Target, path: pathlib.Path
) -> pathlib.Path:
    budget_name = "BOLERO_BUILD_TIMEOUT_SECONDS"
    try:
        build_timeout = int(os.environ.get(budget_name, "1800"))
    except ValueError as error:
        raise BoleroError(f"{budget_name} must be a positive integer") from error
    positive_int(build_timeout, budget_name)
    empty_corpus = path / "build-corpus"
    empty_crashes = path / "build-crashes"
    empty_corpus.mkdir()
    empty_crashes.mkdir()
    package_root, source, prefix = resolved_test_target(target)
    args = bolero_args(inventory, target) + [
        "--runs",
        "0",
        "--corpus-dir",
        str(empty_corpus),
        "--crashes-dir",
        str(empty_crashes),
        target.test,
    ]
    print(f"{target.id}: instrumented build deadline {build_timeout}s", flush=True)
    build = command(args, timeout=build_timeout, log=path / "build.log")
    executables = EXECUTABLE.findall(build.stdout)
    matches = [
        ROOT / executable
        for label, executable in executables
        if label.startswith("unittests ") == (target.test_target == "lib")
        if (package_root / label.removeprefix("unittests ").strip()).resolve() == source
        if pathlib.Path(executable).name.startswith(prefix)
    ]
    if len(matches) != 1 or not matches[0].is_file():
        raise BoleroError(f"{target.id}: expected one instrumented {target.test_target} binary")
    return matches[0]


def run_instrumented(
    binary: pathlib.Path,
    target: Target,
    flags: list[str],
    *,
    timeout: int,
    log: pathlib.Path,
    allowed_status: tuple[int, ...] = (0,),
) -> subprocess.CompletedProcess[str]:
    args = [
        str(binary),
        target.test,
        "--exact",
        "--nocapture",
        "--quiet",
        "--test-threads",
        "1",
    ]
    return command(
        args,
        env={
            "BOLERO_LIBFUZZER_ARGS": " ".join(flags),
            "BOLERO_TEST_NAME": target.test,
            "BOLERO_LIBTEST_HARNESS": "1",
        },
        timeout=timeout,
        log=log,
        allowed_status=allowed_status,
    )


def fuzz_targets(
    inventory: Inventory, selected: tuple[Target, ...], duration: int
) -> None:
    verify_tool(inventory)
    executed = 0
    completed = 0
    for target in selected:
        path = run_dir(target)
        started = time.monotonic()
        executed += 1
        try:
            binary = build_instrumented(inventory, target, path)
        except BoleroError:
            metadata(
                path,
                target,
                bolero_args(inventory, target),
                "failed build",
                time.monotonic() - started,
            )
            raise
        runtime_corpus = path / "corpus"
        runtime_corpus.mkdir()
        for seed in target.corpus.iterdir():
            if seed.is_file() and not seed.name.startswith("."):
                shutil.copy2(seed, runtime_corpus / seed.name)
        crashes = path / "crashes"
        crashes.mkdir()
        flags = [
            str(runtime_corpus),
            str(crashes),
            f"-artifact_prefix={crashes}/",
            f"-timeout={target.case_timeout_seconds}",
            f"-max_len={target.max_input_bytes}",
            f"-max_total_time={duration}",
            "-rss_limit_mb=2048",
        ]
        engine_args = [
            "BOLERO_LIBFUZZER_ARGS=" + " ".join(flags),
            str(binary),
            target.test,
        ]
        try:
            result = run_instrumented(
                binary,
                target,
                flags,
                timeout=duration + max(30, target.case_timeout_seconds * 3),
                log=path / "fuzz.log",
            )
            if "DONE" not in result.stdout + result.stderr:
                raise BoleroError(f"{target.id}: libFuzzer did not report completion")
        except BoleroError:
            metadata(path, target, engine_args, "failed", time.monotonic() - started)
            raise
        metadata(path, target, engine_args, "passed", time.monotonic() - started)
        completed += 1
        print(f"{target.id}: libFuzzer completed; artifacts: {path}")
    print(
        f"Bolero discovered={len(inventory.targets)} selected={len(selected)} "
        f"executed={executed} completed={completed}"
    )


def exact_target(inventory: Inventory, target_id: str) -> Target:
    found = [target for target in inventory.targets if target.id == target_id]
    if len(found) != 1:
        raise BoleroError(f"unknown Bolero target: {target_id}")
    return found[0]


def replay(target: Target, failure: pathlib.Path) -> int:
    if not failure.is_file():
        raise BoleroError(f"input does not exist: {failure}")
    if failure.stat().st_size > target.max_input_bytes:
        raise BoleroError(f"{target.id}: input exceeds {target.max_input_bytes} bytes")
    target.crashes.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(failure.read_bytes()).hexdigest()
    destination = target.crashes / f"replay-{digest}"
    if failure.resolve() != destination.resolve():
        shutil.copy2(failure, destination)
    args = cargo_test_args(target) + ["--", "--exact", target.test, "--nocapture"]
    env = {
        "BOLERO_RANDOM_ITERATIONS": "0",
        "BOLERO_RANDOM_MAX_LEN": str(target.max_input_bytes),
    }
    result = command(args, env=env, timeout=240, allowed_status=(0, 101))
    output = result.stdout + result.stderr
    if hashlib.sha256(destination.read_bytes()).hexdigest() != digest:
        raise BoleroError(f"{target.id}: replay bytes changed during staging")
    if result.returncode != 0 and not re.search(
        r"test result: FAILED\. 0 passed; 1 failed", output
    ):
        raise BoleroError(f"{target.id}: replay failed outside the selected property")
    if result.returncode == 0 and not re.search(
        r"corpus inputs: [1-9]\d* \| rng inputs: 0", output
    ):
        raise BoleroError(f"{target.id}: saved input was not replayed")
    print(output[-4000:])
    return result.returncode


def reduce_failure(
    inventory: Inventory, target: Target, failure: pathlib.Path
) -> pathlib.Path:
    verify_tool(inventory)
    if not failure.is_file():
        raise BoleroError(f"input does not exist: {failure}")
    if failure.stat().st_size > target.max_input_bytes:
        raise BoleroError(f"{target.id}: input exceeds {target.max_input_bytes} bytes")
    path = run_dir(target)
    minimized = path / "minimized"
    input_copy = path / "input"
    shutil.copy2(failure, input_copy)
    started = time.monotonic()
    flags: list[str] = []
    try:
        binary = build_instrumented(inventory, target, path)
        flags = [
            str(input_copy),
            "-minimize_crash=1",
            f"-exact_artifact_path={minimized}",
            f"-timeout={target.case_timeout_seconds}",
            "-max_total_time=60",
        ]
        run_instrumented(
            binary,
            target,
            flags,
            timeout=1800,
            log=path / "reduce.log",
            allowed_status=tuple(range(256)),
        )
        if not minimized.is_file() or minimized.stat().st_size > failure.stat().st_size:
            raise BoleroError(f"{target.id}: libFuzzer did not save a minimized failure")
        if replay(target, minimized) == 0:
            raise BoleroError(f"{target.id}: minimized input no longer fails the property")
    except BoleroError:
        metadata(
            path,
            target,
            ["BOLERO_LIBFUZZER_ARGS=" + " ".join(flags)],
            "failed",
            time.monotonic() - started,
        )
        raise
    metadata(
        path,
        target,
        ["BOLERO_LIBFUZZER_ARGS=" + " ".join(flags), str(binary), target.test],
        "minimized",
        time.monotonic() - started,
    )
    print(f"{target.id}: minimized failure: {minimized}")
    return minimized


def qualify(inventory: Inventory) -> None:
    """Exercise intentional failure, saved input, minimization, replay and timeout."""
    verify_tool(inventory)
    fixture = Target(
        id="qualification-assertion",
        package="bolero-qualification",
        test_target="lib",
        test="tests::bolero_qualification_fails_on_marker",
        source=QUALIFICATION.parent / "src/lib.rs",
        features=(),
        domain_version=1,
        corpus=QUALIFICATION.parent
        / "src/__fuzz__/tests__bolero_qualification_fails_on_marker/corpus",
        random_iterations=1,
        max_input_bytes=16,
        case_timeout_seconds=1,
        invariant="The deliberate marker failure must be detected, saved, minimized and replayed.",
        manifest=QUALIFICATION,
    )
    path = run_dir(fixture)
    corpus = path / "corpus"
    crashes = path / "crashes"
    corpus.mkdir()
    crashes.mkdir()
    (corpus / "marker").write_bytes(b"\x42\x00")
    args = bolero_args(inventory, fixture) + [
        "--runs",
        "1",
        "-T",
        "5s",
        "--corpus-dir",
        str(corpus),
        "--crashes-dir",
        str(crashes),
        fixture.test,
    ]
    started = time.monotonic()
    try:
        result = command(
            args,
            timeout=1800,
            log=path / "assertion.log",
            allowed_status=tuple(range(256)),
        )
        saved = [item for item in crashes.iterdir() if item.is_file()]
        if result.returncode == 0 or not saved:
            raise BoleroError("intentional failure did not fail and save a crash input")
        if b"\x42" not in saved[0].read_bytes():
            raise BoleroError("saved crash input lost the failing marker")
        minimized = reduce_failure(inventory, fixture, saved[0])
        if replay(fixture, minimized) == 0:
            raise BoleroError("saved failure replay unexpectedly succeeded")

        timeout_target = dataclasses.replace(
            fixture,
            id="qualification-timeout",
            test="tests::bolero_qualification_times_out_on_marker",
        )
        timeout_path = run_dir(timeout_target)
        timeout_corpus = timeout_path / "corpus"
        timeout_crashes = timeout_path / "crashes"
        timeout_corpus.mkdir()
        timeout_crashes.mkdir()
        (timeout_corpus / "marker").write_bytes(b"\x55")
        timeout_args = bolero_args(inventory, timeout_target) + [
            "--runs",
            "1",
            "-T",
            "5s",
            "--corpus-dir",
            str(timeout_corpus),
            "--crashes-dir",
            str(timeout_crashes),
            timeout_target.test,
        ]
        try:
            timeout_result = command(
                timeout_args,
                timeout=10,
                log=timeout_path / "timeout.log",
                allowed_status=tuple(range(256)),
            )
        except BoleroError as error:
            if "timed out" not in str(error):
                raise
            if "seed corpus" not in (timeout_path / "timeout.log").read_text():
                raise BoleroError("timeout occurred before the case started") from error
        else:
            if timeout_result.returncode == 0 or "timeout" not in (
                timeout_result.stdout + timeout_result.stderr
            ).lower():
                raise BoleroError("case timeout did not fail the fuzz engine")
        metadata(timeout_path, timeout_target, timeout_args, "expected timeout", 0)

        invalid = bolero_args(inventory, fixture) + [
            "--engine-args=-bolero_qualification_unknown_flag=1",
            fixture.test,
        ]
        engine_result = command(
            invalid,
            timeout=1800,
            log=path / "engine-failure.log",
            allowed_status=tuple(range(256)),
        )
        if engine_result.returncode == 0:
            raise BoleroError("engine failure unexpectedly succeeded")

        missing = bolero_args(inventory, fixture) + [
            "tests::bolero_qualification_missing_target"
        ]
        missing_result = command(
            missing,
            timeout=1800,
            log=path / "missing-target.log",
            allowed_status=tuple(range(256)),
        )
        if missing_result.returncode == 0:
            raise BoleroError("missing target unexpectedly succeeded")
    except BoleroError:
        metadata(path, fixture, args, "failed qualification", time.monotonic() - started)
        raise
    metadata(path, fixture, args, "qualified", time.monotonic() - started)
    print(f"Bolero failure path qualified; artifacts: {path}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="action", required=True)
    subparsers.add_parser("validate")
    subparsers.add_parser("list")
    subparsers.add_parser("qualify")
    test = subparsers.add_parser("test")
    test.add_argument("filter", nargs="?")
    fuzz = subparsers.add_parser("fuzz")
    fuzz.add_argument("target")
    fuzz.add_argument("duration", nargs="?", type=int)
    fuzz_all = subparsers.add_parser("fuzz-all")
    fuzz_all.add_argument("duration", nargs="?", type=int)
    for action in ("replay", "reduce"):
        selected = subparsers.add_parser(action)
        selected.add_argument("target")
        selected.add_argument("failure", type=pathlib.Path)
    args = parser.parse_args()
    inventory = load_inventory()
    discover(inventory)
    if args.action == "validate":
        return 0
    if args.action == "list":
        for target in inventory.targets:
            print(f"{target.id}\t{target.package}\t{target.test_target}\t{target.test}")
        return 0
    if args.action == "qualify":
        qualify(inventory)
        return 0
    if args.action == "test":
        test_targets(inventory, select(inventory, args.filter))
        return 0
    if args.action in ("fuzz", "fuzz-all"):
        selected = (
            (exact_target(inventory, args.target),)
            if args.action == "fuzz"
            else inventory.targets
        )
        duration = (
            args.duration
            if args.duration is not None
            else inventory.pr_fuzz_seconds
        )
        positive_int(duration, "duration")
        fuzz_targets(inventory, selected, duration)
        return 0
    target = exact_target(inventory, args.target)
    if args.action == "replay":
        return replay(target, args.failure)
    reduce_failure(inventory, target, args.failure)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (BoleroError, OSError, KeyError, ValueError, tomllib.TOMLDecodeError) as error:
        print(f"Bolero: {error}", file=sys.stderr)
        sys.exit(1)
