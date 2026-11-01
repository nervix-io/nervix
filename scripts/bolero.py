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
from collections.abc import Callable
from typing import Any, Iterator

from scripts import bolero_coverage as coverage
from scripts.rust_source import RustFile, _end_of_block

ROOT = pathlib.Path(__file__).resolve().parents[1]
INVENTORY = ROOT / "tests/bolero-targets.toml"
RUNS = ROOT / "target/bolero/runs"
QUALIFICATION = ROOT / "tests/bolero-qualification/Cargo.toml"
TARGET_NAME = re.compile(r"^[a-z][a-z0-9-]*$")
# A registered test target other than the library's unit tests, `lib`: the unit tests of one
# binary, `bin:<name>`, or one integration test, `test:<name>`, each named as Cargo names it.
NAMED_TEST_TARGET = re.compile(r"(?:bin|test):[A-Za-z_][A-Za-z_0-9-]*")
LIBRARY_KINDS = frozenset({"lib", "rlib", "dylib", "cdylib", "staticlib", "proc-macro"})
TEST_LINE = re.compile(r"^(\S+): test$", re.MULTILINE)
# Cargo names every test executable it builds: a library's or binary's as `unittests <root>`, an
# integration test's by its source path.
EXECUTABLE = re.compile(r"Executable (?P<description>[^(]+?) \((?P<path>[^)]+)\)")
MACRO = re.compile(r"\bbolero::check!\s*\(")
UNQUALIFIED_CHECK = re.compile(r"(?<![:\w])check!\s*\(")
FUNCTION = re.compile(r"\bfn\s+([A-Za-z_][A-Za-z_0-9]*)\s*\(")
# Symbolizing NEW_FUNC progress can exhaust a short campaign's deadline. Crash stacks
# still use the configured symbolizer when function progress is disabled.
LIBFUZZER_PROGRESS_FLAG = "-print_funcs=0"


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

    @property
    def harness(self) -> Harness:
        return Harness(self.manifest, self.package, self.test_target, self.features)


@dataclasses.dataclass(frozen=True)
class Harness:
    """One compiled test executable. Every target it holds shares one instrumented build."""

    manifest: pathlib.Path | None
    package: str
    test_target: str
    features: tuple[str, ...]

    def describe(self) -> str:
        features = ",".join(self.features) or "no features"
        return f"{self.package} {self.test_target} ({features})"

    def sort_key(self) -> tuple[str, str, str, tuple[str, ...]]:
        return (str(self.manifest), self.package, self.test_target, self.features)


@dataclasses.dataclass(frozen=True)
class Inventory:
    cargo_bolero: str
    nightly: str
    sanitizer: str
    pr_fuzz_seconds: int
    # Targets whose harness CI builds ahead of its campaigns with one Cargo build job, because the
    # build exceeds the campaign's build deadline and its compiler memory.
    prepared_targets: tuple[str, ...]
    targets: tuple[Target, ...]

    def prepared_harnesses(self) -> frozenset[Harness]:
        by_id = {target.id: target for target in self.targets}
        return frozenset(by_id[target_id].harness for target_id in self.prepared_targets)


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
        "prepared_targets",
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
        if item["test_target"] != "lib" and not NAMED_TEST_TARGET.fullmatch(item["test_target"]):
            raise BoleroError(f"{item['id']}: invalid cargo test target")
        if not isinstance(item["features"], list) or any(
            not isinstance(feature, str)
            or feature in {"loom", "shuttle", "turmoil", "deloxide"}
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
        prepared_targets=prepared_targets(tool["prepared_targets"], targets),
        targets=tuple(targets),
    )


def prepared_targets(value: Any, targets: list[Target]) -> tuple[str, ...]:
    """Each named target stands for one distinct registered harness that CI prepares first."""
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise BoleroError("prepared_targets must list registered target ids")
    by_id = {target.id: target for target in targets}
    unknown = [item for item in value if item not in by_id]
    if unknown:
        raise BoleroError(f"prepared_targets names unregistered targets: {unknown}")
    harnesses = Counter(by_id[item].harness for item in value)
    shared = sorted(harness.describe() for harness, count in harnesses.items() if count > 1)
    if shared:
        raise BoleroError(f"prepared_targets names one harness more than once: {shared}")
    return tuple(value)


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
    """Authored package sources stop at nested manifests and Cargo build output."""
    excluded = {ROOT / "target", ROOT / ".git", manifest.parent / "target"}
    if target_dir := os.environ.get("CARGO_TARGET_DIR"):
        excluded.add(pathlib.Path(target_dir).resolve())
    for directory, children, files in os.walk(manifest.parent):
        path = pathlib.Path(directory)
        # A nested manifest owns its sources; hidden directories are metadata or dependencies.
        # Only known build targets are excluded, so an authored `src/target` remains visible.
        children[:] = sorted(
            child for child in children
            if not child.startswith(".")
            and path / child not in excluded
            and child not in {"node_modules", "__fuzz__"}
            and not (path / child / "Cargo.toml").is_file()
        )
        for filename in sorted(files):
            if filename.endswith(".rs"):
                yield path / filename


def function_body(code: str, parameters: int) -> tuple[int, int] | None:
    """Return the span of the body of a function whose parameter list opens at `parameters`.

    `code` has comments and literal contents blanked. The signature ends at the first `{` or `;`
    outside its brackets, so a `;` inside an array type does not end it, while one that ends a
    declaration without a body, such as a trait method's, leaves the function without one.
    """
    depth = 1
    index = parameters
    length = len(code)
    while index < length:
        char = code[index]
        if char in "([":
            depth += 1
        elif char in ")]":
            depth -= 1
        elif depth == 0 and char == "{":
            return index, _end_of_block(code, index)
        elif depth == 0 and char == ";":
            return None
        index += 1
    return None


def owning_function(code: str, offset: int) -> str | None:
    """Name the innermost function whose body holds `offset`.

    `code` has comments and literal contents blanked, so their braces and words do not count. The
    nearest preceding function whose body holds `offset` is the innermost one; a nested item
    function written before `offset` ends before it and does not own it.
    """
    preceding = list(FUNCTION.finditer(code, 0, offset))
    for function in reversed(preceding):
        body = function_body(code, function.end())
        if body is not None and body[0] < offset < body[1]:
            return function.group(1)
    return None


def macro_owners(source: pathlib.Path, content: str) -> list[str]:
    """Name the function that owns each Bolero macro of one source file, in source order."""
    macros = list(MACRO.finditer(content))
    if not macros:
        return []
    code = RustFile(str(source), content).code
    owners = []
    for macro in macros:
        owner = owning_function(code, macro.start())
        if owner is None:
            raise BoleroError(f"{source}: Bolero macro has no owning function")
        owners.append(owner)
    return owners


def static_targets(manifest: pathlib.Path) -> dict[str, pathlib.Path]:
    found = {}
    for source in package_rust_sources(manifest):
        content = source.read_text()
        for function in macro_owners(source, content):
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
            except BaseException:
                coverage.native.stop(process, signal.SIGINT)
                raise
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
        except BaseException:
            coverage.native.stop(process, signal.SIGINT)
            raise
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
    """Cargo's selector of a registered test target: `--lib`, `--bin <name>` or `--test <name>`."""
    if test_target == "lib":
        return ["--lib"]
    kind, name = test_target.split(":", 1)
    return [f"--{kind}", name]


def is_cargo_target(test_target: str, item: dict[str, Any]) -> bool:
    """Whether `item`, one target of a package's Cargo metadata, is the registered test target."""
    if test_target == "lib":
        return bool(LIBRARY_KINDS.intersection(item["kind"]))
    kind, name = test_target.split(":", 1)
    return kind in item["kind"] and item["name"] == name


def builds_unit_tests(test_target: str) -> bool:
    """Whether Cargo builds the test target as unit tests, which it names `unittests <root>`: a
    library's or a binary's, rather than an integration test it names by its source path."""
    return test_target == "lib" or test_target.startswith("bin:")


def listed_tests(
    package: str, test_target: str, *, ignored: bool, features: tuple[str, ...],
    manifest: pathlib.Path | None = None,
) -> list[str]:
    args = [
        "cargo",
        "test",
        "--package",
        package,
        *cargo_target_args(test_target),
    ]
    if features:
        args.extend(["--features", ",".join(features)])
    if manifest:
        args.extend(["--manifest-path", str(manifest)])
    args.extend(["bolero_", "--"])
    if ignored:
        args.append("--ignored")
    args.extend(["--list", "--format", "terse"])
    return TEST_LINE.findall(command(args).stdout)


def compiled_targets(
    package: str, test_target: str, *, features: tuple[str, ...],
    manifest: pathlib.Path | None = None,
) -> list[dict[str, Any]]:
    args = [
        "cargo",
        "test",
        "--package",
        package,
        *cargo_target_args(test_target),
    ]
    if features:
        args.extend(["--features", ",".join(features)])
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


def discover_package(
    package: str, manifest: pathlib.Path, targets: tuple[Target, ...]
) -> list[tuple[str, dict[str, Any]]]:
    """Compare one package's source macros and compiled targets with its registered targets."""
    source_functions = static_targets(manifest)
    registered_functions = {
        target.test.split("::")[-1]: target.source
        for target in targets
    }
    if source_functions != registered_functions:
        raise BoleroError(
            f"{package}: source macros {source_functions} differ from inventory "
            f"{registered_functions}"
        )
    test_targets = {target.test_target for target in targets}
    compiled = []
    for test_target in sorted(test_targets):
        features = tuple(sorted({
            feature
            for target in targets
            if target.test_target == test_target
            for feature in target.features
        }))
        names = listed_tests(package, test_target, ignored=False, features=features, manifest=manifest)
        ignored = listed_tests(package, test_target, ignored=True, features=features, manifest=manifest)
        if ignored:
            raise BoleroError(f"{package}: ignored Bolero targets: {ignored}")
        found = compiled_targets(package, test_target, features=features, manifest=manifest)
        if Counter(names) != Counter(item["test_name"] for item in found):
            raise BoleroError(
                f"{package}: listed Bolero tests {names} differ from compiled targets "
                f"{[item['test_name'] for item in found]}"
            )
        compiled.extend((test_target, item) for item in found)
    return compiled


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
        targets = tuple(target for target in inventory.targets if target.package == package)
        compiled.extend(discover_package(package, manifest, targets))
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


# Estimates from sanitizer job 114178003433, which ran all 159 targets on a 16-vCPU runner: a
# prepared server harness took up to 1,710 s to build with one Cargo build job, a library harness
# at most about 180 s, and each campaign about 12 s beyond its engine time exporting its coverage.
PREPARED_BUILD_SECONDS = 1800
HARNESS_BUILD_SECONDS = 180
CAMPAIGN_OVERHEAD_SECONDS = 15


@dataclasses.dataclass(frozen=True)
class Shard:
    """Part `number` of `count` of the sanitizer inventory, numbered from 1."""

    number: int
    count: int

    @classmethod
    def parse(cls, text: str) -> Shard:
        number, separator, count = text.partition("/")
        if not separator or not number.isdigit() or not count.isdigit():
            raise BoleroError(f"a shard is NUMBER/COUNT, such as 1/3, not `{text}`")
        shard = cls(number=int(number), count=int(count))
        if shard.count < 1 or not 1 <= shard.number <= shard.count:
            raise BoleroError(f"shard {text} names no part: NUMBER runs from 1 to COUNT")
        return shard

    def describe(self) -> dict[str, int]:
        return {"number": self.number, "count": self.count}


@dataclasses.dataclass(frozen=True)
class HarnessEstimate:
    harness: Harness
    targets: tuple[Target, ...]
    seconds: int


def harness_estimates(inventory: Inventory) -> list[HarnessEstimate]:
    """Every harness with its estimated build and campaign time at the inventory's PR duration."""
    members: dict[Harness, list[Target]] = {}
    for target in inventory.targets:
        members.setdefault(target.harness, []).append(target)
    prepared = inventory.prepared_harnesses()
    estimates = []
    for harness, targets in members.items():
        build = PREPARED_BUILD_SECONDS if harness in prepared else HARNESS_BUILD_SECONDS
        campaigns = len(targets) * (inventory.pr_fuzz_seconds + CAMPAIGN_OVERHEAD_SECONDS)
        estimates.append(HarnessEstimate(harness, tuple(targets), build + campaigns))
    return estimates


def shard_plan(inventory: Inventory, count: int) -> tuple[tuple[Target, ...], ...]:
    """Deal whole harnesses to `count` shards, the longest estimated harness first.

    A harness is never split, so a shard builds only its own harnesses. Each harness goes to the
    shard holding the least estimated time, the lower-numbered one on a tie, and harnesses of equal
    estimate keep their sorted order, so every runner computes the same plan from the inventory.
    """
    positive_int(count, "shard count")
    estimates = harness_estimates(inventory)
    if count > len(estimates):
        raise BoleroError(
            f"{count} shards exceed the inventory's {len(estimates)} harnesses, "
            "so a shard would select no target"
        )
    estimates.sort(key=lambda estimate: (-estimate.seconds, estimate.harness.sort_key()))
    loads = [0] * count
    harnesses: list[set[Harness]] = [set() for _ in range(count)]
    for estimate in estimates:
        lightest = loads.index(min(loads))
        loads[lightest] += estimate.seconds
        harnesses[lightest].add(estimate.harness)
    plan = []
    for assigned in harnesses:
        plan.append(tuple(target for target in inventory.targets if target.harness in assigned))
    return tuple(plan)


def plan_digest(plan: tuple[tuple[Target, ...], ...]) -> str:
    identities = [[target.id for target in shard] for shard in plan]
    return hashlib.sha256(json.dumps(identities).encode()).hexdigest()


def describe_plan(inventory: Inventory, count: int) -> dict[str, Any]:
    plan = shard_plan(inventory, count)
    seconds = {estimate.harness: estimate.seconds for estimate in harness_estimates(inventory)}
    shards = []
    for number, targets in enumerate(plan, start=1):
        harnesses = sorted({target.harness for target in targets}, key=Harness.sort_key)
        shards.append({
            "number": number,
            "estimated_seconds": sum(seconds[harness] for harness in harnesses),
            "prepared_targets": [
                target_id for target_id in inventory.prepared_targets
                if exact_target(inventory, target_id).harness in harnesses
            ],
            "harnesses": [harness.describe() for harness in harnesses],
            "targets": [target.id for target in targets],
        })
    return {"count": count, "plan_sha256": plan_digest(plan), "shards": shards}


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
    details: dict[str, Any] | None = None,
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
            continue
        # The ordinary assertion replays a property failure; a sanitizer finding reproduces only in
        # the instrumented build, so both exact replays are recorded.
        for recipe in ("fuzz-replay", "fuzz-replay-sanitizer"):
            replay_commands.append(
                shlex.join(["just", recipe, target.id, str(input_file.relative_to(ROOT))])
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
        **(details or {}),
    }
    (path / "metadata.json").write_text(json.dumps(data, indent=2) + "\n")


def build_harness(target: Target, built: set[tuple[str, ...]]) -> None:
    """Compile the ordinary harness that runs `target`, unless `built` already holds it."""
    harness = tuple(cargo_test_args(target))
    if harness in built:
        return
    print(f"{target.id}: ordinary build deadline 1800s", flush=True)
    command([*harness, "--no-run"], timeout=1800)
    built.add(harness)


def build_targets(selected: tuple[Target, ...]) -> None:
    """Compile every ordinary harness the selected targets run, as `test` builds them, and run
    none of them, so a later `test` compiles nothing."""
    built: set[tuple[str, ...]] = set()
    for target in selected:
        build_harness(target, built)
    print(f"Bolero ordinary builds: {len(built)} harnesses for {len(selected)} targets", flush=True)


def test_targets(inventory: Inventory, selected: tuple[Target, ...]) -> None:
    executed = 0
    completed = 0
    prepared: set[tuple[str, ...]] = set()
    for target in selected:
        build_harness(target, prepared)
        args = cargo_test_args(target)
        args += ["--", "--exact", target.test, "--nocapture"]
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
        f"--engine-args={LIBFUZZER_PROGRESS_FLAG}",
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


def scoped_build_env(target: Target, path: pathlib.Path) -> dict[str, str]:
    """Scope cargo-bolero's pinned Rustup Cargo calls to the registered test harness."""
    rustup = shutil.which("rustup")
    if rustup is None:
        raise BoleroError("rustup is required for the pinned sanitizer toolchain")
    directory = path / "scope-bin"
    directory.mkdir()
    # Kache identifies compiler mode by the rustc executable name. Retain that name for the
    # run-local adapter, and retain its exact source in the campaign evidence.
    compiler_adapter = directory / "rustc"
    compiler_adapter.write_bytes(pathlib.Path(__file__).with_name("bolero_rustc.py").read_bytes())
    compiler_adapter.chmod(0o700)
    adapter = directory / "rustup"
    adapter.write_text(
        f"#!{sys.executable}\n"
        "import hashlib,os,pathlib,subprocess,sys\n"
        "args=sys.argv[1:]\n"
        "if len(args)>=4 and args[0]=='run' and args[2:4]==['cargo','test']:\n"
        f"    args[4:4]={cargo_target_args(target.test_target)!r}\n"
        f"    compiler_adapter={str(compiler_adapter)!r}\n"
        f"    os.environ['NERVIX_BOLERO_RUSTC']=subprocess.check_output([{rustup!r},'which','--toolchain',args[1],'rustc'],text=True).strip()\n"
        "    os.environ['RUSTC']=compiler_adapter\n"
        "    identity=hashlib.sha256(pathlib.Path(compiler_adapter).read_bytes()).hexdigest()[:16]\n"
        "    if '--target-dir' in args:\n"
        "        index=args.index('--target-dir')+1\n"
        "        args[index]+='-locations-'+identity\n"
        f"os.execv({rustup!r},[{rustup!r},*args])\n"
    )
    adapter.chmod(0o700)
    return {"PATH": str(directory) + os.pathsep + os.environ["PATH"]}


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
    targets = [
        item for item in package["targets"]
        if is_cargo_target(target.test_target, item)
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
    inventory: Inventory, target: Target, path: pathlib.Path, *, timeout: int | None = None
) -> pathlib.Path:
    budget_name = "BOLERO_BUILD_TIMEOUT_SECONDS"
    try:
        build_timeout = int(os.environ.get(budget_name, "1800")) if timeout is None else timeout
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
    build = command(
        args, env={**scoped_build_env(target, path), **coverage.build_environment()},
        timeout=build_timeout, log=path / "build.log"
    )
    executables = EXECUTABLE.findall(build.stdout)
    matches = [
        ROOT / executable
        for label, executable in executables
        if label.startswith("unittests ") == builds_unit_tests(target.test_target)
        if (package_root / label.removeprefix("unittests ").strip()).resolve() == source
        if pathlib.Path(executable).name.startswith(prefix)
    ]
    if len(matches) != 1 or not matches[0].is_file():
        raise BoleroError(f"{target.id}: expected one instrumented {target.test_target} binary")
    return matches[0]


def prepare_shard(inventory: Inventory, shard: Shard) -> None:
    """Prepare each declared harness this shard's campaigns build, through the shared build path."""
    harnesses = {target.harness for target in shard_plan(inventory, shard.count)[shard.number - 1]}
    prepared = []
    for target_id in inventory.prepared_targets:
        target = exact_target(inventory, target_id)
        if target.harness in harnesses:
            prepared.append(target)
    if not prepared:
        print(f"Bolero shard {shard.number}/{shard.count} builds no prepared harness", flush=True)
        return
    for target in prepared:
        prepare_target(inventory, target)


def prepare_target(inventory: Inventory, target: Target) -> None:
    """Record the selected sanitizer build without claiming campaign completion."""
    verify_tool(inventory)
    path = run_dir(target)
    started = time.monotonic()
    result = "failed preparation"
    try:
        binary = build_instrumented(inventory, target, path)
        result = "instrumented preparation"
    finally:
        metadata(path, target, bolero_args(inventory, target), result, time.monotonic() - started)
    print(f"{target.id}: prepared {binary}; artifacts: {path}")


def run_instrumented(
    binary: pathlib.Path,
    target: Target,
    flags: list[str],
    *,
    timeout: int,
    log: pathlib.Path,
    allowed_status: tuple[int, ...] = (0,),
    environment: dict[str, str] | None = None,
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
            **(environment or {}),
        },
        timeout=timeout,
        log=log,
        allowed_status=allowed_status,
    )


def fuzz_targets(
    inventory: Inventory,
    selected: tuple[Target, ...],
    duration: int,
    shard: Shard | None = None,
) -> None:
    verify_tool(inventory)
    partition = None
    if shard is not None:
        partition = {
            **shard.describe(),
            "plan_sha256": plan_digest(shard_plan(inventory, shard.count)),
        }
    with coverage.Campaign(ROOT, inventory, selected, duration, partition) as campaign:
        for target in selected:
            path = run_dir(target)
            started = time.monotonic()
            with campaign.target(target, path) as measured:
                try:
                    binary = build_instrumented(inventory, target, path)
                except BaseException:
                    metadata(path, target, bolero_args(inventory, target), "failed build", time.monotonic() - started)
                    raise
                runtime_corpus = path / "corpus"
                runtime_corpus.mkdir()
                for seed in target.corpus.iterdir():
                    if seed.is_file() and not seed.name.startswith("."):
                        shutil.copy2(seed, runtime_corpus / seed.name)
                crashes = path / "crashes"
                crashes.mkdir()
                flags = [
                    str(runtime_corpus), str(crashes), f"-artifact_prefix={crashes}/",
                    f"-timeout={target.case_timeout_seconds}", f"-max_len={target.max_input_bytes}",
                    f"-max_total_time={duration}", "-rss_limit_mb=2048", LIBFUZZER_PROGRESS_FLAG,
                ]
                engine_args = ["BOLERO_LIBFUZZER_ARGS=" + " ".join(flags), str(binary), target.test]
                try:
                    environment = measured.execute(binary, flags)
                    engine_started = time.monotonic()
                    result = run_instrumented(
                        binary, target, flags,
                        timeout=duration + max(30, target.case_timeout_seconds * 3),
                        log=path / "fuzz.log", environment=environment,
                    )
                    output = result.stdout + result.stderr
                    if "DONE" not in output:
                        raise BoleroError(f"{target.id}: libFuzzer did not report completion")
                    measured.complete(output, time.monotonic() - engine_started)
                except BaseException:
                    metadata(path, target, engine_args, "failed", time.monotonic() - started)
                    raise
                metadata(path, target, engine_args, "passed", time.monotonic() - started)
                print(f"{target.id}: libFuzzer completed; artifacts: {path}")
        campaign.complete()
        counts = campaign.record.content["counts"]
        print(f"Bolero discovered={counts['discovered']} selected={counts['selected']} executed={counts['executed']} completed={counts['completed']}")


def exact_target(inventory: Inventory, target_id: str) -> Target:
    found = [target for target in inventory.targets if target.id == target_id]
    if len(found) != 1:
        raise BoleroError(f"unknown Bolero target: {target_id}")
    return found[0]


def replay(target: Target, failure: pathlib.Path) -> int:
    checked_input(target, failure)
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
    # Bolero omits zero counters: corpus-only completion ends after the corpus count.
    if result.returncode == 0 and not re.search(
        r"corpus inputs: [1-9]\d*(?:\r?\n|$)", output
    ):
        raise BoleroError(f"{target.id}: saved input was not replayed")
    print(output[-4000:])
    return result.returncode


def verify_replay_record(target: Target, failure: pathlib.Path) -> None:
    """Refuse a saved input that no longer matches the run record that saved it.

    An input the runner saved lives in a run directory beside that run's `metadata.json`, which
    holds the input's digest. A missing or unreadable record, another target's record or a changed
    input is a corrupt replay record: replaying it would test bytes no run ever failed on. Inputs
    outside the run directories carry no record and are replayed as given.
    """
    location = failure.absolute()
    if not location.is_relative_to(RUNS):
        return
    parts = location.relative_to(RUNS).parts
    if len(parts) < 3:
        raise BoleroError(f"corrupt replay record: {failure} is not inside a run directory")
    if parts[0] != target.id:
        raise BoleroError(f"corrupt replay record: {failure} was saved by {parts[0]}, not {target.id}")
    record = RUNS / parts[0] / parts[1] / "metadata.json"
    try:
        content = json.loads(record.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise BoleroError(f"corrupt replay record: {record} cannot be read: {error}") from error
    if not isinstance(content, dict) or content.get("target") != target.id:
        raise BoleroError(f"corrupt replay record: {record} is not a record of {target.id}")
    saved = content.get("saved_inputs")
    name = str(location.relative_to(ROOT))
    if not isinstance(saved, dict) or name not in saved:
        raise BoleroError(f"corrupt replay record: {record} does not hold {name}")
    digest = hashlib.sha256(failure.read_bytes()).hexdigest()
    if saved[name] != digest:
        raise BoleroError(
            f"corrupt replay record: {name} has digest {digest}, its record holds {saved[name]}"
        )


def checked_input(target: Target, failure: pathlib.Path) -> None:
    if not failure.is_file():
        raise BoleroError(f"input does not exist: {failure}")
    if failure.stat().st_size > target.max_input_bytes:
        raise BoleroError(f"{target.id}: input exceeds {target.max_input_bytes} bytes")


def single_input_flags(target: Target, path: pathlib.Path, staged: pathlib.Path) -> list[str]:
    """libFuzzer runs a file argument once; its crash artifacts stay in the run directory."""
    return [
        str(staged),
        f"-artifact_prefix={path}/",
        f"-timeout={target.case_timeout_seconds}",
        LIBFUZZER_PROGRESS_FLAG,
    ]


def replay_sanitizer(inventory: Inventory, target: Target, failure: pathlib.Path) -> int:
    """Run the exact saved input once through the target's instrumented sanitizer build.

    An assertion failure reproduces in ordinary replay too; a sanitizer finding only here.
    """
    verify_tool(inventory)
    checked_input(target, failure)
    path = run_dir(target)
    staged = path / "input"
    shutil.copy2(failure, staged)
    flags = single_input_flags(target, path, staged)
    started = time.monotonic()
    try:
        binary = build_instrumented(inventory, target, path)
        result = run_instrumented(
            binary,
            target,
            flags,
            timeout=max(60, target.case_timeout_seconds * 3),
            log=path / "replay.log",
            allowed_status=tuple(range(256)),
        )
    except BoleroError:
        metadata(path, target, ["BOLERO_LIBFUZZER_ARGS=" + " ".join(flags)], "failed",
                 time.monotonic() - started)
        raise
    outcome = "reproduced" if result.returncode != 0 else "not reproduced"
    metadata(
        path,
        target,
        ["BOLERO_LIBFUZZER_ARGS=" + " ".join(flags), str(binary), target.test],
        f"instrumented replay {outcome}",
        time.monotonic() - started,
        {"replayed_input_sha256": hashlib.sha256(staged.read_bytes()).hexdigest()},
    )
    print((result.stdout + result.stderr)[-4000:])
    print(f"{target.id}: instrumented replay {outcome}; artifacts: {path}")
    return result.returncode


def reduce_failure(
    inventory: Inventory, target: Target, failure: pathlib.Path
) -> pathlib.Path:
    verify_tool(inventory)
    checked_input(target, failure)
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
            LIBFUZZER_PROGRESS_FLAG,
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
        # The minimum must still fail where it was found, in the instrumented build. Ordinary
        # replay is recorded beside it: a sanitizer finding passes there by nature.
        verification = run_instrumented(
            binary,
            target,
            single_input_flags(target, path, minimized),
            timeout=max(60, target.case_timeout_seconds * 3),
            log=path / "verify.log",
            allowed_status=tuple(range(256)),
        )
        if verification.returncode == 0:
            raise BoleroError(f"{target.id}: minimized input no longer fails the instrumented target")
        ordinary = "fails" if replay(target, minimized) != 0 else "passes"
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
        {"ordinary_replay": ordinary},
    )
    print(f"{target.id}: minimized failure: {minimized}; ordinary replay {ordinary}")
    return minimized


SANITIZER_MARKER = b"\x33"
# A generated discovery package holds these properties; each case names which carry `#[ignore]`.
DISCOVERY_PROPERTY = """
    #[test]{attribute}
    fn {name}() {{
        bolero::check!().with_iterations(0).for_each(|bytes: &[u8]| {{
            std::hint::black_box(bytes);
        }});
    }}
"""


def qualification_target(target_id: str, test: str) -> Target:
    """A deliberately failing target of the standalone qualification package."""
    return Target(
        id=target_id,
        package="bolero-qualification",
        test_target="lib",
        test=test,
        source=QUALIFICATION.parent / "src/lib.rs",
        features=(),
        domain_version=1,
        corpus=QUALIFICATION.parent / "src/__fuzz__" / test.replace("::", "__") / "corpus",
        random_iterations=1,
        max_input_bytes=16,
        case_timeout_seconds=1,
        invariant="A deliberate failure must be detected, saved, minimized and replayed.",
        manifest=QUALIFICATION,
    )


def marker_campaign(
    inventory: Inventory, target: Target, path: pathlib.Path, marker: bytes
) -> list[str]:
    """One bounded campaign whose corpus holds only the marker that makes the target fail."""
    corpus = path / "corpus"
    crashes = path / "crashes"
    corpus.mkdir()
    crashes.mkdir()
    (corpus / "marker").write_bytes(marker)
    return bolero_args(inventory, target) + [
        "--runs",
        "1",
        "-T",
        "5s",
        "--corpus-dir",
        str(corpus),
        "--crashes-dir",
        str(crashes),
        target.test,
    ]


def saved_crashes(path: pathlib.Path) -> list[pathlib.Path]:
    return sorted(item for item in (path / "crashes").iterdir() if item.is_file())


class Qualification:
    """Every way a run must fail, each case with what it observed and the evidence it kept."""

    def __init__(self, path: pathlib.Path) -> None:
        self.path = path
        self.cases: list[dict[str, Any]] = []

    def record(
        self, case: str, requirement: str, observed: str, *evidence: pathlib.Path
    ) -> None:
        kept = []
        for item in evidence:
            kept.append(str(item.relative_to(ROOT)) if item.is_relative_to(ROOT) else str(item))
        self.cases.append(
            {"case": case, "requirement": requirement, "observed": observed, "evidence": kept}
        )

    def expect_refusal(
        self, case: str, requirement: str, reason: str, action: Callable[[], object]
    ) -> None:
        try:
            action()
        except (BoleroError, coverage.native.RunnerError) as error:
            if reason not in str(error):
                raise BoleroError(f"{case} failed for another reason: {error}") from error
            self.record(case, requirement, str(error))
            return
        raise BoleroError(f"{case}: {requirement}, but it succeeded")

    def write(self, verdict: str) -> None:
        content = {"verdict": verdict, "cases": self.cases}
        (self.path / "qualification.json").write_text(json.dumps(content, indent=2) + "\n")


def qualify(inventory: Inventory) -> None:
    """Exercise every way a run must fail, keeping each case's evidence in one record."""
    verify_tool(inventory)
    fixture = qualification_target(
        "qualification-assertion", "tests::bolero_qualification_fails_on_marker"
    )
    path = run_dir(fixture)
    qualification = Qualification(path)
    args = marker_campaign(inventory, fixture, path, b"\x42\x00")
    started = time.monotonic()
    try:
        result = command(
            args,
            timeout=1800,
            log=path / "assertion.log",
            allowed_status=tuple(range(256)),
        )
        saved = saved_crashes(path)
        if result.returncode == 0 or not saved:
            raise BoleroError("intentional failure did not fail and save a crash input")
        if b"\x42" not in saved[0].read_bytes():
            raise BoleroError("saved crash input lost the failing marker")
        qualification.record(
            "property-mismatch", "a failing assertion fails the campaign and saves its input",
            f"exit status {result.returncode}", path / "assertion.log", saved[0],
        )
        minimized = reduce_failure(inventory, fixture, saved[0])
        if replay(fixture, minimized) == 0:
            raise BoleroError("saved failure replay unexpectedly succeeded")
        qualification.record(
            "minimization-and-replay",
            "the minimized input fails the instrumented target and the ordinary assertion",
            f"{saved[0].stat().st_size} bytes minimized to {minimized.stat().st_size}", minimized,
        )
        qualify_replay_records(fixture, minimized, qualification)
        qualify_sanitizer(inventory, qualification)
        qualify_timeout(inventory, fixture, qualification)
        qualify_engine(inventory, fixture, path, qualification)
        qualify_discovery(path, qualification)
        qualify_selection(inventory, qualification)
    except BoleroError:
        qualification.write("failed")
        metadata(path, fixture, args, "failed qualification", time.monotonic() - started)
        raise
    qualification.write("qualified")
    metadata(path, fixture, args, "qualified", time.monotonic() - started)
    print(
        f"Bolero failure path qualified: {len(qualification.cases)} cases; artifacts: {path}"
    )


def qualify_replay_records(
    fixture: Target, minimized: pathlib.Path, qualification: Qualification
) -> None:
    """An input that matches its run record replays; a corrupt record refuses replay."""
    verify_replay_record(fixture, minimized)
    corrupt = run_dir(fixture)
    (corrupt / "crashes").mkdir()
    changed = corrupt / "crashes" / "input"
    original = minimized.read_bytes()
    changed.write_bytes(bytes([original[0] ^ 0xFF]) + original[1:])
    record = {
        "target": fixture.id,
        "saved_inputs": {
            str(changed.relative_to(ROOT)): hashlib.sha256(original).hexdigest()
        },
    }
    (corrupt / "metadata.json").write_text(json.dumps(record, indent=2) + "\n")
    qualification.expect_refusal(
        "corrupt-replay-input", "replay refuses an input that differs from its run record",
        "has digest", lambda: verify_replay_record(fixture, changed),
    )
    unreadable = run_dir(fixture)
    (unreadable / "crashes").mkdir()
    kept = unreadable / "crashes" / "input"
    kept.write_bytes(original)
    (unreadable / "metadata.json").write_text("{")
    qualification.expect_refusal(
        "unreadable-replay-record", "replay refuses an input whose run record cannot be read",
        "cannot be read", lambda: verify_replay_record(fixture, kept),
    )


def qualify_sanitizer(inventory: Inventory, qualification: Qualification) -> None:
    """A memory error fails the campaign with its report, and the exact input replays it."""
    target = qualification_target(
        "qualification-sanitizer", "tests::bolero_qualification_reports_memory_error_on_marker"
    )
    path = run_dir(target)
    args = marker_campaign(inventory, target, path, SANITIZER_MARKER)
    started = time.monotonic()
    result = command(
        args, timeout=1800, log=path / "sanitizer.log", allowed_status=tuple(range(256))
    )
    saved = saved_crashes(path)
    report = (path / "sanitizer.log").read_text(errors="replace")
    findings = [line for line in report.splitlines() if "ERROR: AddressSanitizer" in line]
    outcome = "expected sanitizer finding"
    if result.returncode == 0 or not saved or not findings:
        outcome = "missing sanitizer finding"
    metadata(path, target, args, outcome, time.monotonic() - started)
    if outcome != "expected sanitizer finding":
        raise BoleroError("a memory error did not fail the campaign with its report and input")
    qualification.record(
        "sanitizer-finding", "an AddressSanitizer report fails the campaign and saves its input",
        findings[0].strip(), path / "sanitizer.log", saved[0],
    )
    if replay_sanitizer(inventory, target, saved[0]) == 0:
        raise BoleroError("the instrumented replay did not reproduce the sanitizer finding")
    qualification.record(
        "sanitizer-replay", "the instrumented exact replay reproduces the sanitizer finding",
        "replay failed", saved[0],
    )


def qualify_timeout(inventory: Inventory, fixture: Target, qualification: Qualification) -> None:
    timeout_target = dataclasses.replace(
        fixture,
        id="qualification-timeout",
        test="tests::bolero_qualification_times_out_on_marker",
    )
    timeout_path = run_dir(timeout_target)
    timeout_args = marker_campaign(inventory, timeout_target, timeout_path, b"\x55")
    observed = "the campaign deadline stopped the stuck case"
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
        observed = f"the engine failed the case with exit status {timeout_result.returncode}"
    metadata(timeout_path, timeout_target, timeout_args, "expected timeout", 0)
    qualification.record(
        "timeout", "a case exceeding its timeout fails the campaign", observed,
        timeout_path / "timeout.log",
    )


def qualify_engine(
    inventory: Inventory, fixture: Target, path: pathlib.Path, qualification: Qualification
) -> None:
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
    qualification.record(
        "engine-failure", "an engine that cannot start fails the campaign",
        f"exit status {engine_result.returncode}", path / "engine-failure.log",
    )
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
    qualification.record(
        "missing-engine-target", "a campaign naming no compiled target fails",
        f"exit status {missing_result.returncode}", path / "missing-target.log",
    )


def discovery_package(
    directory: pathlib.Path, properties: dict[str, str]
) -> tuple[pathlib.Path, dict[str, Target]]:
    """Write a standalone package holding `properties` and the targets that would register it."""
    source = directory / "src"
    source.mkdir(parents=True)
    manifest = directory / "Cargo.toml"
    shutil.copy2(QUALIFICATION, manifest)
    shutil.copy2(QUALIFICATION.parent / "Cargo.lock", directory / "Cargo.lock")
    body = []
    for name, attribute in properties.items():
        body.append(DISCOVERY_PROPERTY.format(name=name, attribute=attribute))
    (source / "lib.rs").write_text(
        "//! A generated discovery qualification package.\n\n"
        "#[cfg(test)]\nmod tests {" + "".join(body) + "}\n"
    )
    targets = {}
    for name in properties:
        test = f"tests::{name}"
        targets[name] = dataclasses.replace(
            qualification_target(f"qualification-{name}", test),
            source=source / "lib.rs",
            corpus=source / "__fuzz__" / test.replace("::", "__") / "corpus",
            manifest=manifest,
        )
    return manifest, targets


def qualify_discovery(path: pathlib.Path, qualification: Qualification) -> None:
    """Compiled discovery of generated packages accepts the exact registration and nothing else."""
    package = "bolero-qualification"
    manifest, targets = discovery_package(path / "discovery-registered", {"bolero_registered": ""})
    compiled = discover_package(package, manifest, (targets["bolero_registered"],))
    work_dirs = [pathlib.Path(item["work_dir"]).resolve() for _, item in compiled]
    if work_dirs != [targets["bolero_registered"].work_dir.resolve()]:
        raise BoleroError(f"discovery of the registered package found {compiled}")
    qualification.record(
        "discovery-control", "discovery accepts a package whose registration is exact",
        "one compiled target in its registered work directory", manifest,
    )
    manifest, targets = discovery_package(
        path / "discovery-ignored", {"bolero_registered": "", "bolero_ignored": "\n    #[ignore]"}
    )
    qualification.expect_refusal(
        "discovery-ignored", "discovery fails on an ignored property", "ignored Bolero targets",
        lambda: discover_package(package, manifest, tuple(targets.values())),
    )
    manifest, targets = discovery_package(
        path / "discovery-unregistered", {"bolero_registered": "", "bolero_unregistered": ""}
    )
    qualification.expect_refusal(
        "discovery-unregistered", "discovery fails on a property the inventory lacks",
        "differ from inventory",
        lambda: discover_package(package, manifest, (targets["bolero_registered"],)),
    )
    manifest, targets = discovery_package(path / "discovery-missing", {"bolero_registered": ""})
    absent = dataclasses.replace(
        targets["bolero_registered"], id="qualification-missing", test="tests::bolero_missing"
    )
    qualification.expect_refusal(
        "discovery-missing", "discovery fails on a registered property the source lacks",
        "differ from inventory",
        lambda: discover_package(package, manifest, (targets["bolero_registered"], absent)),
    )


def qualify_selection(inventory: Inventory, qualification: Qualification) -> None:
    """A selection, shard or campaign that would run nothing fails instead of passing."""
    qualification.expect_refusal(
        "empty-selection", "a filter that selects no target fails", "no Bolero targets selected",
        lambda: select(inventory, "qualification selects no registered target"),
    )
    harnesses = len(harness_estimates(inventory))
    qualification.expect_refusal(
        "empty-shard", "a shard plan that would leave a shard without targets fails",
        "would select no target", lambda: shard_plan(inventory, harnesses + 1),
    )
    qualification.expect_refusal(
        "empty-campaign", "a campaign with no selected target fails",
        "nonempty unique selection",
        lambda: coverage.Campaign(ROOT, inventory, (), inventory.pr_fuzz_seconds, None),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="action", required=True)
    subparsers.add_parser("validate")
    subparsers.add_parser("list")
    subparsers.add_parser("qualify")
    plan = subparsers.add_parser("plan")
    plan.add_argument("count", type=int)
    prepare = subparsers.add_parser("prepare")
    prepare.add_argument("target")
    prepare_shard_parser = subparsers.add_parser("prepare-shard")
    prepare_shard_parser.add_argument("shard", type=Shard.parse)
    test = subparsers.add_parser("test")
    test.add_argument("filter", nargs="?")
    build = subparsers.add_parser("build")
    build.add_argument("filter", nargs="?")
    fuzz = subparsers.add_parser("fuzz")
    fuzz.add_argument("target")
    fuzz.add_argument("duration", nargs="?", type=int)
    fuzz_all = subparsers.add_parser("fuzz-all")
    fuzz_all.add_argument("duration", nargs="?", type=int)
    fuzz_all.add_argument("--shard", type=Shard.parse, default=Shard(1, 1))
    for action in ("replay", "replay-sanitizer", "reduce"):
        selected = subparsers.add_parser(action)
        selected.add_argument("target")
        selected.add_argument("failure", type=pathlib.Path)
    combine = subparsers.add_parser("combine")
    combine.add_argument("evidence", type=pathlib.Path)
    combine.add_argument("output", type=pathlib.Path)
    args = parser.parse_args()
    inventory = load_inventory()
    # Planning and combining shard evidence read only the inventory and the records, so they run
    # without a Rust toolchain. Every other action first compares the inventory with discovery.
    if args.action == "plan":
        print(json.dumps(describe_plan(inventory, args.count), indent=2))
        return 0
    if args.action == "combine":
        combined = coverage.combine(
            ROOT, inventory, args.evidence, args.output,
            lambda count: shard_plan(inventory, count), plan_digest,
        )
        print(f"Bolero combined shard evidence: {combined}")
        return 0
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
    if args.action == "prepare":
        prepare_target(inventory, exact_target(inventory, args.target))
        return 0
    if args.action == "prepare-shard":
        prepare_shard(inventory, args.shard)
        return 0
    if args.action == "test":
        test_targets(inventory, select(inventory, args.filter))
        return 0
    if args.action == "build":
        build_targets(select(inventory, args.filter))
        return 0
    if args.action in ("fuzz", "fuzz-all"):
        duration = (
            args.duration
            if args.duration is not None
            else inventory.pr_fuzz_seconds
        )
        positive_int(duration, "duration")
        if args.action == "fuzz":
            fuzz_targets(inventory, (exact_target(inventory, args.target),), duration)
            return 0
        selected = shard_plan(inventory, args.shard.count)[args.shard.number - 1]
        fuzz_targets(inventory, selected, duration, args.shard)
        return 0
    target = exact_target(inventory, args.target)
    # A saved input inside a run directory must still match the record that saved it.
    verify_replay_record(target, args.failure)
    if args.action == "replay":
        return replay(target, args.failure)
    if args.action == "replay-sanitizer":
        return replay_sanitizer(inventory, target, args.failure)
    reduce_failure(inventory, target, args.failure)
    return 0

if __name__ == "__main__":
    try:
        with coverage.suppressed_profiles(), coverage.native.interruptible():
            sys.exit(main())
    except (BoleroError, coverage.native.RunnerError, OSError, KeyError, ValueError, tomllib.TOMLDecodeError) as error:
        print(f"Bolero: {error}", file=sys.stderr)
        sys.exit(1)
    except (KeyboardInterrupt, coverage.native.Interrupted):
        print("Bolero: interrupted; retained campaigns are incomplete", file=sys.stderr)
        sys.exit(130)
