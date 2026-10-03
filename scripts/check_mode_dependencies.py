#!/usr/bin/env python3

"""Keep every execution mode out of every ordinary dependency graph, and the portable graphs portable.

A consumer builds a package on its own, and Cargo unifies features only within the graph of the
root it builds, so the workspace-wide graph alone can hide a package whose own graph selects a mode.
For the workspace and for every package in it, with default features and without them, the normal
dependency graph contains no model checker, simulator or modeled wrapper: no Loom, no Shuttle or
Shuttle wrapper, no Turmoil. Nor does it enable one of the primitive boundary's execution modes, or
its `test-util` capability, whose paused clock no product runs on.

The vocabulary and the browser's packages are portable: the vocabulary crate for the native target,
and the browser console and the wire crate it decodes with for the browser's target, with default
features and without them. Their graphs contain no async runtime or network library, and never
enable the boundary's `native` capability.

Run it from the repository root as `python3 -m scripts.check_mode_dependencies`, which is what
`just validate-execution-mode-dependencies` does.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

RULE = "execution mode dependencies"
BOUNDARY = "nervix-primitives"
# The model checkers, the simulator, and the wrappers that adapt a library to a model checker.
ENGINES = frozenset({"loom", "shuttle", "turmoil"})
ENGINE_WRAPPER_PREFIX = "shuttle-"
# The boundary's features no ordinary graph enables: every execution mode, and the controls of a
# paused clock.
MODELED_FEATURES = frozenset({"loom", "shuttle", "turmoil", "test-util"})
BROWSER_TARGET = "wasm32-unknown-unknown"


@dataclass(frozen=True)
class PortableRoot:
    """A package whose graph builds for the browser, and the target it is checked for."""

    package: str
    target: str | None


PORTABLE_ROOTS = (
    PortableRoot("nervix-models", None),
    PortableRoot("nervix-client-wire", BROWSER_TARGET),
    PortableRoot("nervix-web-console", BROWSER_TARGET),
)
# What a portable graph never contains: an async runtime, a network library, or anything of a mode.
NATIVE_PACKAGES = frozenset({"mio", "socket2", "tokio"})
NATIVE_CAPABILITY = "native"

# `name vVERSION`, then any parenthesized source or `(proc-macro)` marker, then the enabled
# features, which may be empty, then the `(*)` of a package already shown.
_LINE = re.compile(
    r"^(?P<name>[A-Za-z0-9_-]+) v(?P<version>\S+)(?: \([^)]*\))*"
    r"(?: (?P<features>[A-Za-z0-9_,:/.+-]+))?(?:\s+\(\*\))?\s*$"
)


class CheckError(Exception):
    """A graph Cargo could not produce, which stops the check instead of passing it."""


@dataclass(frozen=True)
class Node:
    """One package of a dependency graph, with the features the graph enables on it."""

    name: str
    features: frozenset[str]


def parse_tree(output: str) -> list[Node]:
    """The packages of `cargo tree --prefix none --format '{p} {f}'` output, one per line."""

    nodes: list[Node] = []
    for line in output.splitlines():
        if not line.strip():
            continue
        match = _LINE.match(line.rstrip())
        if match is None:
            raise CheckError(f"cannot read the dependency graph line `{line}`")
        features = match.group("features") or ""
        nodes.append(
            Node(
                name=match.group("name"),
                features=frozenset(feature for feature in features.split(",") if feature),
            )
        )
    return nodes


@dataclass(frozen=True)
class Root:
    """One graph to check: the workspace or a package, with or without default features, for a
    target."""

    package: str | None
    default_features: bool
    target: str | None = None

    def arguments(self) -> list[str]:
        arguments = ["--workspace"] if self.package is None else ["--package", self.package]
        if not self.default_features:
            arguments.append("--no-default-features")
        if self.target is not None:
            arguments += ["--target", self.target]
        return arguments

    def describe(self) -> str:
        scope = "the workspace" if self.package is None else self.package
        defaults = "default features" if self.default_features else "no default features"
        target = f" for {self.target}" if self.target is not None else ""
        return f"{scope} with {defaults}{target}"


def ordinary_problems(root: Root, nodes: Iterable[Node]) -> list[str]:
    """Why an ordinary graph is not one: a model checker, simulator or wrapper in it, or a mode or
    paused-clock feature of the boundary enabled."""

    problems: list[str] = []
    seen: set[str] = set()
    for node in nodes:
        if node.name in ENGINES or node.name.startswith(ENGINE_WRAPPER_PREFIX):
            if node.name not in seen:
                seen.add(node.name)
                problems.append(
                    f"{RULE}: the normal graph of {root.describe()} contains `{node.name}`, which "
                    "only a modeled build may contain"
                )
        if node.name == BOUNDARY:
            for feature in sorted(node.features & MODELED_FEATURES):
                key = f"{BOUNDARY}/{feature}"
                if key in seen:
                    continue
                seen.add(key)
                problems.append(
                    f"{RULE}: the normal graph of {root.describe()} enables `{key}`, which only a "
                    "modeled or test build selects"
                )
    return problems


def portable_problems(root: Root, nodes: Iterable[Node]) -> list[str]:
    """Why a portable graph is not one: a runtime or network library in it, or the boundary's
    native capability enabled."""

    problems: list[str] = []
    seen: set[str] = set()
    for node in nodes:
        if node.name in NATIVE_PACKAGES and node.name not in seen:
            seen.add(node.name)
            problems.append(
                f"{RULE}: the portable graph of {root.describe()} contains `{node.name}`, a "
                "native runtime or network library"
            )
        if node.name == BOUNDARY and NATIVE_CAPABILITY in node.features:
            key = f"{BOUNDARY}/{NATIVE_CAPABILITY}"
            if key not in seen:
                seen.add(key)
                problems.append(
                    f"{RULE}: the portable graph of {root.describe()} enables `{key}`, the "
                    "capability of native targets"
                )
    return problems


class Commands:
    """How the check reaches Cargo. Tests substitute a double."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def run(self, arguments: Sequence[str]) -> str:
        completed = subprocess.run(
            list(arguments),
            cwd=self.root,
            capture_output=True,
            text=True,
            env=dict(os.environ),
        )
        if completed.returncode != 0:
            raise CheckError(
                f"`{' '.join(arguments)}` failed with status {completed.returncode}:\n"
                f"{completed.stderr.strip()}"
            )
        return completed.stdout


def workspace_packages(commands: Commands) -> list[str]:
    metadata = json.loads(
        commands.run(["cargo", "metadata", "--no-deps", "--format-version", "1"])
    )
    return sorted(package["name"] for package in metadata["packages"])


def graph(commands: Commands, root: Root) -> list[Node]:
    return parse_tree(
        commands.run(
            [
                "cargo",
                "tree",
                *root.arguments(),
                "--edges",
                "normal",
                "--prefix",
                "none",
                "--format",
                "{p} {f}",
                # The environment may ask Cargo for color, as CI does, which would wrap markers such
                # as `(*)` in escape sequences the graph lines are read without.
                "--color",
                "never",
            ]
        )
    )


def ordinary_roots(packages: Sequence[str]) -> list[Root]:
    roots: list[Root] = []
    for package in [None, *packages]:
        for default_features in (True, False):
            roots.append(Root(package=package, default_features=default_features))
    return roots


def portable_roots(packages: Sequence[str]) -> list[Root]:
    roots: list[Root] = []
    for portable in PORTABLE_ROOTS:
        if portable.package not in packages:
            raise CheckError(f"the portable root {portable.package} is not a workspace package")
        for default_features in (True, False):
            roots.append(
                Root(
                    package=portable.package,
                    default_features=default_features,
                    target=portable.target,
                )
            )
    return roots


def check(commands: Commands) -> tuple[list[str], int]:
    """Every problem of every graph, and how many graphs were read."""

    packages = workspace_packages(commands)
    problems: list[str] = []
    graphs = 0
    for root in ordinary_roots(packages):
        problems.extend(ordinary_problems(root, graph(commands, root)))
        graphs += 1
    for root in portable_roots(packages):
        nodes = graph(commands, root)
        problems.extend(ordinary_problems(root, nodes))
        problems.extend(portable_problems(root, nodes))
        graphs += 1
    return problems, graphs


def repository_root() -> Path:
    completed = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"], capture_output=True, check=True, text=True
    )
    return Path(completed.stdout.strip())


def main(
    argv: Sequence[str] | None = None,
    commands_for: type[Commands] = Commands,
) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", type=Path, default=None)
    arguments = parser.parse_args(argv)
    commands = commands_for(arguments.root or repository_root())
    try:
        problems, graphs = check(commands)
    except CheckError as error:
        print(f"{RULE}: {error}", file=sys.stderr)
        return 1
    for problem in problems:
        print(problem, file=sys.stderr)
    if problems:
        print(
            f"{len(problems)} {RULE} violation(s) in {graphs} graphs. A model checker, simulator "
            "or modeled wrapper belongs only to a modeled build, and a portable graph builds for "
            "the browser.",
            file=sys.stderr,
        )
        return 1
    print(
        f"{RULE}: {graphs} ordinary graphs contain no execution mode, and the portable graphs "
        "stay portable"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
