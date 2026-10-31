#!/usr/bin/env python3

"""Carry one build of the scenario suite to the shards that run it.

CI runs the scenario suite in shards. One job builds the instrumented scenario executable and
everything a run of it reads, and `pack` writes them into one archive. Each shard job checks out the
same revision, restores the archive with `unpack`, and runs its share of the suite through the
`--shard INDEX/COUNT` option of the scenario executable, which divides the scenario runs between
the shards. `matrix` writes the shard indices of a workflow matrix from the number of shards, so
the number is the only thing a workflow changes to run more or fewer shards.

The archive holds these files, under their paths below the checkout:

- every executable the build left in `target/llvm-cov-target/debug` that `cargo llvm-cov report`
  reads as an object: a file with an execute bit, outside the incremental, fingerprint and build
  script directories, whose name without its extensions names a workspace package or target,
  matched as cargo-llvm-cov 0.8 matches it. Those are the scenario executable, the CLI, the paced
  simulation driver, the shared client binding, the deadlock report tool and the formatter, so a
  shard's report reads the objects the build's own report would read;
- the Playwright driver that the build script of playwright-rs downloaded into its output
  directory, whose path the scenario executable holds. A scenario executable that names a driver
  the build did not leave fails the pack, rather than leaving a shard to whatever driver it finds;
- the ONNX fixtures and the ONNX Runtime package the ONNX scenarios load, and the WASM guests the
  WASM processor scenarios upload;
- `target/scenario-suite/manifest.json`, which names the revision and the checkout the build ran in,
  and every file with its size.

Executables hold absolute paths below the checkout they were built in, such as the tools a
scenario starts and the Playwright driver, so a shard must run in a checkout at the same path.
`unpack` refuses an archive of another revision or of another checkout path, checks that every file
the manifest names was restored at its size, and refuses an executable or library whose shared
libraries this runner lacks, before any scenario starts.

A shard compiles nothing. It runs the scenario executable directly rather than through Cargo, which
would compare the restored files with sources whose modification times the checkout set and
rebuild them. `executable` prints the path of the scenario executable among the manifest's files.

`write_archive` and `restore_archive` serve any archive of a build, and
`scripts/unit_test_archive.py` carries the workspace's unit tests with them.
"""

from __future__ import annotations

import argparse
import json
import mmap
import os
import re
import shutil
import subprocess
import sys
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
COVERAGE_DEBUG = Path("target/llvm-cov-target/debug")
MANIFEST = Path("target/scenario-suite/manifest.json")
# The directories cargo-llvm-cov does not search for objects: Cargo's incremental compilation and
# fingerprints, and build scripts with their output, which hold no workspace object.
SKIPPED_DIRECTORIES = frozenset({"incremental", ".fingerprint", "fingerprint"})
BUILD_DIRECTORY = "build"
# Files with these extensions are never objects, whatever their mode.
NON_OBJECT_EXTENSIONS = frozenset({".d", ".rlib", ".rmeta"})
# The build script of playwright-rs downloads its driver into its output directory, and the crate
# holds that directory's path from compile time. A build whose download failed names none.
PLAYWRIGHT_DRIVERS = "build/playwright-rs-*/out/playwright-driver/playwright-*"
PLAYWRIGHT_DRIVER_PATH = b"/out/playwright-driver/"
ONNX_FIXTURES = Path("tests/fixtures/onnx")
WASM_GUESTS = (
    Path("examples/wasm-processors/rust-guest/target/wasm32-unknown-unknown/release/nervix_wasm_processor_rust_guest.wasm"),
    Path("examples/wasm-processors/go-guest/nervix_wasm_processor_go_guest.wasm"),
)
# The executable the `scenarios` test target builds into, named for its target and hash.
SCENARIO_EXECUTABLE = re.compile(r"^scenarios-[0-9a-f]+$")
# The shard counts a workflow matrix may hold; GitHub runs at most 256 jobs of one matrix.
MAXIMUM_SHARDS = 256
COMPRESSION = ("zstd", "-T0", "-3")
ELF_MAGIC = b"\x7fELF"


class ShardError(Exception):
    """A pack, unpack or matrix request that cannot be carried out, with the reason."""


@dataclass(frozen=True)
class SuiteFile:
    """One file of the archive: its path below the checkout and its size."""

    path: str
    size: int

    def to_json(self) -> dict[str, object]:
        return {"path": self.path, "size": self.size}

    @staticmethod
    def from_json(entry: object) -> SuiteFile:
        if not isinstance(entry, dict):
            raise ShardError(f"a manifest file entry must be an object, not {entry!r}")
        path = entry.get("path")
        size = entry.get("size")
        if not isinstance(path, str) or not isinstance(size, int) or isinstance(size, bool):
            raise ShardError(f"a manifest file entry needs a path and a size: {entry!r}")
        return SuiteFile(path, size)


@dataclass(frozen=True)
class Manifest:
    """What an archive holds and where it was built."""

    revision: str
    root: str
    files: tuple[SuiteFile, ...]

    def to_json(self) -> dict[str, object]:
        return {
            "revision": self.revision,
            "root": self.root,
            "files": [file.to_json() for file in self.files],
        }

    @staticmethod
    def read(path: Path) -> Manifest:
        try:
            document = json.loads(path.read_text())
        except FileNotFoundError as error:
            raise ShardError(f"{path} is missing; restore its archive first") from error
        except json.JSONDecodeError as error:
            raise ShardError(f"{path} is not JSON: {error}") from error
        if not isinstance(document, dict):
            raise ShardError(f"{path} must hold a JSON object")
        revision = document.get("revision")
        root = document.get("root")
        files = document.get("files")
        if not isinstance(revision, str) or not isinstance(root, str):
            raise ShardError(f"{path} must name its revision and its root")
        if not isinstance(files, list):
            raise ShardError(f"{path} must list its files")
        return Manifest(revision, root, tuple(SuiteFile.from_json(entry) for entry in files))

    def total_size(self) -> int:
        total = 0
        for file in self.files:
            total += file.size
        return total


def matrix(count: int) -> list[int]:
    """The shard indices, counted from one, of a matrix of `count` shards."""
    if count < 1 or count > MAXIMUM_SHARDS:
        raise ShardError(f"a scenario run has from 1 to {MAXIMUM_SHARDS} shards, not {count}")
    return list(range(1, count + 1))


def workspace_targets(root: Path) -> set[str]:
    """The names of every workspace package and target, as `cargo metadata` lists them."""
    completed = subprocess.run(
        ["cargo", "metadata", "--no-deps", "--format-version", "1"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    metadata = json.loads(completed.stdout)
    names: set[str] = set()
    for package in metadata["packages"]:
        names.add(package["name"])
        for target in package["targets"]:
            names.add(target["name"])
    return names


def object_pattern(names: set[str]) -> re.Pattern[str]:
    """Matches a file name without its extensions that names one of `names`, as cargo-llvm-cov's
    object filter does: an optional `lib` prefix, the name with `-` and `_` interchangeable, and an
    optional hash."""
    alternatives = []
    for name in sorted(names):
        parts = [re.escape(part) for part in name.split("-")]
        alternatives.append("(-|_)".join(parts))
    return re.compile(r"^(lib)?(" + "|".join(alternatives) + r")(-[0-9a-f]+)?$")


def full_stem(name: str) -> str:
    """A file name without any of its extensions: `libnervix_client_ffi.so` is
    `libnervix_client_ffi`."""
    stem = name
    while True:
        shorter = Path(stem).stem
        if shorter == stem:
            return stem
        stem = shorter


def is_object(path: Path, pattern: re.Pattern[str]) -> bool:
    """Whether cargo-llvm-cov reads `path` as an object: a regular file with an execute bit, not a
    dependency, library metadata or lock file, whose full stem matches `pattern`."""
    if path.suffix in NON_OBJECT_EXTENSIONS:
        return False
    try:
        mode = path.stat().st_mode
    except FileNotFoundError:
        return False
    if not path.is_file() or mode & 0o111 == 0:
        return False
    return pattern.match(full_stem(path.name)) is not None


def coverage_objects(root: Path, pattern: re.Pattern[str]) -> list[Path]:
    """Every object below the coverage build's debug directory, relative to `root`."""
    debug = root / COVERAGE_DEBUG
    if not debug.is_dir():
        raise ShardError(f"{debug} is missing; build the tests first")
    objects = []
    for directory, subdirectories, files in os.walk(debug):
        current = Path(directory)
        kept = []
        for subdirectory in subdirectories:
            if subdirectory in SKIPPED_DIRECTORIES:
                continue
            if current == debug and subdirectory == BUILD_DIRECTORY:
                continue
            kept.append(subdirectory)
        subdirectories[:] = sorted(kept)
        for name in sorted(files):
            path = current / name
            if is_object(path, pattern):
                objects.append(path.relative_to(root))
    return objects


def scenario_executable(objects: Sequence[Path]) -> Path:
    """The one scenario executable among `objects`."""
    executables = []
    for path in objects:
        if path.parent == COVERAGE_DEBUG / "deps" and SCENARIO_EXECUTABLE.match(path.name):
            executables.append(path)
    if len(executables) != 1:
        raise ShardError(
            f"the coverage build must leave exactly one scenario executable in {COVERAGE_DEBUG / 'deps'}, "
            f"not {[str(path) for path in executables]}"
        )
    return executables[0]


def files_below(root: Path, directory: Path) -> list[Path]:
    """Every file below `directory`, relative to `root`, symbolic links included as links."""
    found = []
    for current, subdirectories, files in os.walk(root / directory):
        subdirectories.sort()
        for name in sorted(files):
            found.append((Path(current) / name).relative_to(root))
    return found


def playwright_drivers(root: Path, executable: Path) -> list[Path]:
    """The Playwright driver directories the scenario executable names, relative to `root`."""
    candidates = sorted((root / COVERAGE_DEBUG).glob(PLAYWRIGHT_DRIVERS))
    named = []
    with (root / executable).open("rb") as file, mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as image:
        for candidate in candidates:
            if image.find(str(candidate).encode()) != -1:
                named.append(candidate.relative_to(root))
        names_a_driver = image.find(PLAYWRIGHT_DRIVER_PATH) != -1
    if names_a_driver and not named:
        raise ShardError(
            f"{executable} names a Playwright driver below {COVERAGE_DEBUG / 'build'}, "
            f"and the build left none of {[str(path) for path in candidates]}"
        )
    return named


def runtime_inputs(root: Path, executable: Path) -> list[Path]:
    """The generated files a scenario run reads beside its executables."""
    inputs: list[Path] = []
    for driver in playwright_drivers(root, executable):
        inputs.extend(files_below(root, driver))
    inputs.extend(generated_inputs(root))
    return inputs


def generated_inputs(root: Path) -> list[Path]:
    """The generated files any test may read: the ONNX fixtures and runtime and the WASM guests."""
    inputs: list[Path] = []
    fixtures = sorted((root / ONNX_FIXTURES).glob("*.onnx"))
    if not fixtures:
        raise ShardError(f"{ONNX_FIXTURES} holds no ONNX fixture; run `just generate-test-onnx` first")
    for fixture in fixtures:
        inputs.append(fixture.relative_to(root))
    runtime = onnxruntime_package(root)
    inputs.extend(files_below(root, runtime))
    for guest in WASM_GUESTS:
        if not (root / guest).is_file():
            raise ShardError(f"{guest} is missing; run `just wasm-processor-guests` first")
        inputs.append(guest)
    return inputs


def onnxruntime_package(root: Path) -> Path:
    """The ONNX Runtime package directory whose library the scenarios load, relative to `root`."""
    completed = subprocess.run(
        ["bash", "scripts/download_onnxruntime.sh", "--print-path"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    library = Path(completed.stdout.strip())
    if not library.is_file():
        raise ShardError(f"{library} is missing; run `just download-onnxruntime` first")
    # The library lies in the package's `lib` directory.
    package = library.parent.parent
    return package.relative_to(root)


def revision(root: Path) -> str:
    completed = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, check=True, capture_output=True, text=True)
    return completed.stdout.strip()


def write_archive(root: Path, archive: Path, manifest_path: Path, paths: Sequence[Path]) -> Manifest:
    """Write the files at `paths`, and a manifest of them at `manifest_path`, into one archive."""
    if shutil.which(COMPRESSION[0]) is None:
        raise ShardError(f"{COMPRESSION[0]} is not installed")
    files = []
    for path in paths:
        files.append(SuiteFile(path.as_posix(), (root / path).lstat().st_size))
    manifest = Manifest(revision(root), str(root.resolve()), tuple(files))
    written = root / manifest_path
    written.parent.mkdir(parents=True, exist_ok=True)
    written.write_text(json.dumps(manifest.to_json(), indent=1) + "\n")
    listing = "\0".join([*[file.path for file in files], manifest_path.as_posix()]) + "\0"
    archive.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    with archive.open("wb") as output:
        tar = subprocess.Popen(
            ["tar", "--create", "--file=-", "--directory", str(root), "--null", "--files-from=-"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        compress = subprocess.Popen([*COMPRESSION, "--stdout"], stdin=tar.stdout, stdout=output)
        if tar.stdin is None or tar.stdout is None:
            raise ShardError("tar was started without its pipes")
        tar.stdout.close()
        tar.stdin.write(listing.encode())
        tar.stdin.close()
        compress_status = compress.wait()
        tar_status = tar.wait()
    if tar_status != 0 or compress_status != 0:
        raise ShardError(f"writing {archive} failed: tar exited {tar_status}, {COMPRESSION[0]} exited {compress_status}")
    elapsed = time.monotonic() - started
    print(
        f"packed {len(files)} files, {manifest.total_size() / 2**30:.2f} GiB, into {archive}: "
        f"{archive.stat().st_size / 2**30:.2f} GiB in {elapsed:.1f}s"
    )
    return manifest


def restore_archive(
    root: Path, archive: Path, manifest_path: Path, environment: Mapping[str, str] | None = None
) -> Manifest:
    """Restore an archive into the checkout at `root` and check what it restored, resolving shared
    libraries in the `environment` its executables will run in."""
    if shutil.which(COMPRESSION[0]) is None:
        raise ShardError(f"{COMPRESSION[0]} is not installed")
    if not archive.is_file():
        raise ShardError(f"{archive} is missing")
    started = time.monotonic()
    with archive.open("rb") as source:
        decompress = subprocess.Popen([COMPRESSION[0], "--decompress", "-T0", "--stdout"], stdin=source, stdout=subprocess.PIPE)
        tar = subprocess.Popen(["tar", "--extract", "--file=-", "--directory", str(root)], stdin=decompress.stdout)
        if decompress.stdout is None:
            raise ShardError(f"{COMPRESSION[0]} was started without its output pipe")
        decompress.stdout.close()
        tar_status = tar.wait()
        decompress_status = decompress.wait()
    if tar_status != 0 or decompress_status != 0:
        raise ShardError(
            f"restoring {archive} failed: {COMPRESSION[0]} exited {decompress_status}, tar exited {tar_status}"
        )
    manifest = Manifest.read(root / manifest_path)
    current = revision(root)
    if manifest.revision != current:
        raise ShardError(f"{archive} was built from revision {manifest.revision}, and this checkout is {current}")
    checkout = str(root.resolve())
    if manifest.root != checkout:
        raise ShardError(
            f"{archive} was built in {manifest.root}, and this checkout is {checkout}; its executables "
            f"hold absolute paths below the checkout they were built in"
        )
    for file in manifest.files:
        path = root / file.path
        try:
            size = path.lstat().st_size
        except FileNotFoundError as error:
            raise ShardError(f"{archive} did not restore {file.path}") from error
        if size != file.size:
            raise ShardError(f"{archive} restored {file.path} with {size} bytes, not {file.size}")
    missing = missing_libraries(root, manifest, environment)
    if missing:
        lines = "\n".join(f"  {path}: {', '.join(libraries)}" for path, libraries in missing.items())
        raise ShardError(f"this runner lacks shared libraries the restored files load:\n{lines}")
    elapsed = time.monotonic() - started
    print(
        f"restored {len(manifest.files)} files, {manifest.total_size() / 2**30:.2f} GiB, of revision "
        f"{manifest.revision} in {elapsed:.1f}s"
    )
    return manifest


def pack(root: Path, archive: Path) -> Manifest:
    """Write the archive of the scenario suite's build and its runtime inputs."""
    objects = coverage_objects(root, object_pattern(workspace_targets(root)))
    executable = scenario_executable(objects)
    print(f"scenario executable {executable}")
    return write_archive(root, archive, MANIFEST, [*objects, *runtime_inputs(root, executable)])


def unpack(root: Path, archive: Path) -> Manifest:
    """Restore the scenario suite's archive into the checkout at `root`."""
    return restore_archive(root, archive, MANIFEST)


def missing_libraries(
    root: Path, manifest: Manifest, environment: Mapping[str, str] | None = None
) -> dict[str, list[str]]:
    """The shared libraries each restored ELF file loads that `ldd` cannot find on this runner."""
    missing: dict[str, list[str]] = {}
    for file in manifest.files:
        path = root / file.path
        if path.is_symlink() or not is_elf(path):
            continue
        completed = subprocess.run(["ldd", str(path)], capture_output=True, text=True, env=environment)
        absent = []
        for line in completed.stdout.splitlines():
            if "not found" in line:
                absent.append(line.split("=>", 1)[0].strip())
        if absent:
            missing[file.path] = absent
    return missing


def is_elf(path: Path) -> bool:
    with path.open("rb") as file:
        return file.read(len(ELF_MAGIC)) == ELF_MAGIC


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    matrix_command = commands.add_parser("matrix", help="print the shard indices of a matrix as JSON")
    matrix_command.add_argument("count", type=int)
    pack_command = commands.add_parser("pack", help="write the archive the shards restore")
    pack_command.add_argument("archive", type=Path)
    unpack_command = commands.add_parser("unpack", help="restore the archive into this checkout")
    unpack_command.add_argument("archive", type=Path)
    commands.add_parser("executable", help="print the restored scenario executable's path")
    arguments = parser.parse_args(argv)
    try:
        if arguments.command == "matrix":
            print(json.dumps(matrix(arguments.count)))
        elif arguments.command == "pack":
            pack(ROOT, arguments.archive)
        elif arguments.command == "unpack":
            unpack(ROOT, arguments.archive)
        else:
            files = Manifest.read(ROOT / MANIFEST).files
            print(ROOT / scenario_executable([Path(file.path) for file in files]))
    except ShardError as error:
        print(f"scenario shards: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
