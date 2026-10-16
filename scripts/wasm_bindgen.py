"""The wasm-bindgen CLI the web console build runs, at the version Cargo.lock locks.

trunk takes a `wasm-bindgen` from PATH when its version equals the one the console's lock file
selects. Otherwise it downloads the release archive from GitHub, once and without retry, so a short
DNS or transport failure there fails the whole build. The console build therefore installs that
exact version with `cargo install --locked` through the configured compiler wrapper, puts it first
on PATH and runs trunk offline. Cargo verifies the crate against the registry index's checksum and
bounds its own network retries. Offline, trunk fails at once when the binary is missing or reports
another version, instead of downloading one.

This file needs only the standard library, so the image build runs it by path:
`python3 scripts/wasm_bindgen.py install` prints the directory to put first on PATH.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import tomllib
from pathlib import Path
from typing import Sequence

CRATE = "wasm-bindgen-cli"
BINARY = "wasm-bindgen"
# The library crate whose locked version selects the CLI, as trunk selects it.
LIBRARY = "wasm-bindgen"
REPORTED_VERSION = re.compile(r"wasm-bindgen (\d+\.\d+\.\d+)\b")
ROOT = Path(__file__).resolve().parents[1]


class ToolError(Exception):
    """The pinned wasm-bindgen CLI could not be selected, installed or verified."""


def locked_version(root: Path) -> str:
    """The one wasm-bindgen version the repository's lock file selects."""

    lock = tomllib.loads((root / "Cargo.lock").read_text(encoding="utf-8"))
    versions = sorted({package["version"] for package in lock.get("package", []) if package["name"] == LIBRARY})
    if not versions:
        raise ToolError(f"{root / 'Cargo.lock'} locks no wasm-bindgen version")
    if len(versions) > 1:
        raise ToolError(f"{root / 'Cargo.lock'} locks several wasm-bindgen versions: {', '.join(versions)}")
    return versions[0]


def default_target() -> Path:
    configured = os.environ.get("CARGO_TARGET_DIR")
    if configured:
        return Path(configured)
    return ROOT / "target"


def install_root(target: Path, version: str) -> Path:
    return target / "tools" / f"{BINARY}-{version}"


def bin_dir(target: Path, version: str) -> Path:
    return install_root(target, version) / "bin"


def executable(target: Path, version: str) -> Path:
    return bin_dir(target, version) / BINARY


def install_command(target: Path, version: str) -> list[str]:
    return ["cargo", "install", "--locked", "--force", "--version", version,
            "--root", str(install_root(target, version)), CRATE]


def installed_version(path: Path) -> str | None:
    """The version the binary at `path` reports, or None when nothing is installed there."""

    if not path.exists():
        return None
    try:
        completed = subprocess.run([str(path), "--version"], capture_output=True, text=True)
    except OSError as error:
        raise ToolError(f"{path} could not be run: {error}") from error
    reported = REPORTED_VERSION.match(completed.stdout)
    if completed.returncode != 0 or reported is None:
        raise ToolError(f"{path} did not report a wasm-bindgen version")
    return reported.group(1)


def install(root: Path, target: Path) -> Path:
    """Install the locked version unless it is there already; return the directory to put first on PATH."""

    version = locked_version(root)
    path = executable(target, version)
    if installed_version(path) == version:
        return bin_dir(target, version)
    command = install_command(target, version)
    try:
        # Cargo's own output goes to standard error, so standard output carries only the result.
        completed = subprocess.run(command, cwd=root, stdout=sys.stderr)
    except OSError as error:
        raise ToolError(f"cargo could not be run to install {CRATE} {version}: {error}") from error
    if completed.returncode != 0:
        raise ToolError(f"installing {CRATE} {version} failed with exit {completed.returncode}")
    reported = installed_version(path)
    if reported != version:
        raise ToolError(f"{path} reports {reported}, but Cargo.lock locks {version}")
    return bin_dir(target, version)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["install"], help="install the locked CLI and print its bin directory")
    parser.parse_args(argv)
    try:
        directory = install(ROOT, default_target())
    except ToolError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    print(directory)
    return 0


if __name__ == "__main__":
    sys.exit(main())
