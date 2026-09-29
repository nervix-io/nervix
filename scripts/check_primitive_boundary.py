#!/usr/bin/env python3

"""Hold every Nervix atomic to the primitive boundary, and every mode feature to its owner.

`nervix-primitives` selects the atomics, orderings and fences of a build for its execution mode.
Code that reaches the standard library's, Shuttle's or Loom's atomics any other way escapes that
selection: a modeled build would run it on real atomics, and a check would claim coverage it does
not have. The source rules reject every such path in every tracked or new Rust file outside the
owner, including tests, benchmarks, examples and macro bodies, whether written as an import, a
renamed or grouped import, a glob, a fully qualified path, or through an alias of `std`, `core` or
their `sync` module. Comments and literal contents are blanked first, and conditional compilation is
ignored, so an inactive `cfg` branch is checked like an active one.

A real atomic that must stay outside every model is reached through `nervix_primitives::unmodeled`
and needs a permission in `crates/primitives/unmodeled-permissions.toml` naming the file, the items
it uses, its owner, the reason and the verification limit. A use without a permission, an item the
permission does not list, and a permission nothing uses all fail.

The manifest rules keep mode selection in one place. Only the owner selects Loom, and the harness
runs it, so no other package may depend on `loom`. A package that owns a `loom`, `shuttle` or
`turmoil` feature depends on `nervix-primitives` directly and forwards the mode to it, and forwards
it to every workspace dependency that owns the same mode, so the whole graph of that package uses
one backend even when it is built on its own.

Run it from the repository root as `python3 -m scripts.check_primitive_boundary`, which is what
`just validate-primitive-boundary` does.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tomllib
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Iterator, Mapping, Sequence

from scripts.ratchet import RustFile, Site

RULE = "primitive boundary"
OWNER = "nervix-primitives"
OWNER_SOURCES = "crates/primitives/"
PERMISSIONS = PurePosixPath("crates/primitives/unmodeled-permissions.toml")
HARNESS = "nervix-model-harness"
MODES = ("loom", "shuttle", "turmoil")
SELECTED = ("nervix_primitives", "sync", "atomic")
UNMODELED = ("nervix_primitives", "unmodeled", "sync", "atomic")
PERMISSION_FIELDS = ("path", "items", "owner", "reason", "limit")

_USE_ITEM = re.compile(r"(?<![A-Za-z0-9_])use\s+(?P<tree>[^;]*);")
_EXTERN_CRATE = re.compile(
    r"(?<![A-Za-z0-9_])extern\s+crate\s+(?P<name>[A-Za-z_][A-Za-z0-9_]*)"
    r"(?:\s+as\s+(?P<alias>[A-Za-z_][A-Za-z0-9_]*))?\s*;"
)
_TOKEN = re.compile(r"\s*(::|[{},*]|\$?[A-Za-z_][A-Za-z0-9_]*)")
_QUALIFIED_ATOMIC = re.compile(
    r"(?P<root>\$?[A-Za-z_][A-Za-z0-9_]*)\s*::\s*sync\s*::\s*atomic(?![A-Za-z0-9_])"
)
_BARE_ATOMIC = re.compile(r"(?<![A-Za-z0-9_$])sync\s*::\s*atomic(?![A-Za-z0-9_])")
_UNMODELED_PATH = re.compile(
    r"(?<![A-Za-z0-9_:])nervix_primitives\s*::\s*unmodeled(?![A-Za-z0-9_])"
    r"(?:\s*::\s*sync\s*::\s*atomic\s*::\s*(?P<item>[A-Za-z_][A-Za-z0-9_]*))?"
)
_PRIMITIVES_ALIAS = re.compile(r"(?<![A-Za-z0-9_:])nervix_primitives\s+as\s+")


def _selected_replacement(path: Sequence[str]) -> str:
    tail = list(path[3:])
    if not tail or tail == ["*"]:
        return "::".join(SELECTED)
    return "::".join([*SELECTED, *tail])


@dataclass(frozen=True)
class UseLeaf:
    """One imported path of a `use` item, with the name it is bound to."""

    path: tuple[str, ...]
    alias: str | None


class UseTreeError(ValueError):
    pass


def use_leaves(tree: str) -> list[UseLeaf]:
    """Expand the tree of one `use` item into the full paths it imports."""

    tokens: list[str] = []
    position = 0
    while position < len(tree):
        match = _TOKEN.match(tree, position)
        if match is None:
            if tree[position:].strip():
                raise UseTreeError(f"cannot read `{tree.strip()}`")
            break
        tokens.append(match.group(1))
        position = match.end()

    leaves: list[UseLeaf] = []

    def parse(index: int, prefix: tuple[str, ...]) -> int:
        if index < len(tokens) and tokens[index] == "::":
            index += 1
        if index >= len(tokens):
            raise UseTreeError(f"`{tree.strip()}` ends early")
        token = tokens[index]
        if token == "{":
            index += 1
            while tokens[index] != "}":
                index = parse(index, prefix)
                if tokens[index] == ",":
                    index += 1
            return index + 1
        if token == "*":
            leaves.append(UseLeaf(prefix + ("*",), None))
            return index + 1
        index += 1
        if index < len(tokens) and tokens[index] == "::":
            return parse(index + 1, prefix + (token,))
        alias = None
        if index + 1 < len(tokens) and tokens[index] == "as":
            alias = tokens[index + 1]
            index += 2
        if token == "self":
            leaves.append(UseLeaf(prefix, alias))
        else:
            leaves.append(UseLeaf(prefix + (token,), alias))
        return index

    index = parse(0, ())
    if index != len(tokens):
        raise UseTreeError(f"cannot read `{tree.strip()}`")
    return leaves


def _contains_sync_atomic(path: Sequence[str]) -> bool:
    return any(path[index : index + 2] == ("sync", "atomic") for index in range(len(path) - 1))


@dataclass
class FileUses:
    """What one file takes from the boundary's unmodeled path, and where."""

    items: dict[str, int] = field(default_factory=dict)


def check_source(file: RustFile) -> tuple[list[Site], FileUses]:
    """Return the file's boundary violations and the unmodeled items it uses."""

    violations: list[Site] = []
    unmodeled = FileUses()
    code = file.code
    use_spans: list[tuple[int, int]] = []
    sync_aliases: set[str] = set()

    for match in _USE_ITEM.finditer(code):
        use_spans.append((match.start(), match.end()))
        try:
            leaves = use_leaves(match.group("tree"))
        except UseTreeError as error:
            violations.append(file.site(match.start(), f"{RULE}: {error}"))
            continue
        for leaf in leaves:
            path = leaf.path
            if path[:1] == ("nervix_primitives",):
                if path[:2] == ("nervix_primitives", "unmodeled"):
                    if path[: len(UNMODELED)] != UNMODELED or len(path) <= len(UNMODELED):
                        violations.append(
                            file.site(
                                match.start(),
                                f"{RULE}: import unmodeled atomics by name from "
                                f"{'::'.join(UNMODELED)}, not `{'::'.join(path)}`",
                            )
                        )
                        continue
                    item = path[len(UNMODELED)]
                    if item == "*":
                        violations.append(
                            file.site(
                                match.start(),
                                f"{RULE}: import unmodeled atomics by name, not with a glob",
                            )
                        )
                        continue
                    unmodeled.items.setdefault(item, file.line_of(match.start()))
                elif len(path) == 1 and leaf.alias is not None:
                    violations.append(
                        file.site(
                            match.start(),
                            f"{RULE}: `nervix_primitives` is renamed to `{leaf.alias}`; name the "
                            "boundary by its own name",
                        )
                    )
                continue
            if _contains_sync_atomic(path):
                violations.append(
                    file.site(
                        match.start(),
                        f"{RULE}: `{'::'.join(path)}` bypasses the boundary; use "
                        f"`{_selected_replacement(path)}`",
                    )
                )
                continue
            if path in (("std", "sync", "*"), ("core", "sync", "*"), ("std", "*"), ("core", "*")):
                violations.append(
                    file.site(
                        match.start(),
                        f"{RULE}: `use {'::'.join(path)}` brings the atomic module into scope; "
                        "import the items it needs by name",
                    )
                )
                continue
            if path in (("std", "sync"), ("core", "sync")):
                sync_aliases.add(leaf.alias or "sync")
            if path in (("std",), ("core",)) and leaf.alias is not None:
                violations.append(
                    file.site(
                        match.start(),
                        f"{RULE}: `{path[0]}` is renamed to `{leaf.alias}`, which hides its atomic "
                        "module from this check",
                    )
                )

    for match in _EXTERN_CRATE.finditer(code):
        name = match.group("name")
        alias = match.group("alias")
        if alias is not None and name in ("std", "core"):
            violations.append(
                file.site(
                    match.start(),
                    f"{RULE}: `extern crate {name} as {alias}` hides its atomic module from this "
                    "check",
                )
            )
        if alias is not None and name == "nervix_primitives":
            violations.append(
                file.site(
                    match.start(),
                    f"{RULE}: `nervix_primitives` is renamed to `{alias}`; name the boundary by "
                    "its own name",
                )
            )

    outside_uses = list(code)
    for start, end in use_spans:
        for index in range(start, end):
            if outside_uses[index] != "\n":
                outside_uses[index] = " "
    body = "".join(outside_uses)

    for match in _QUALIFIED_ATOMIC.finditer(body):
        root = match.group("root")
        if root in ("nervix_primitives", "unmodeled"):
            continue
        violations.append(
            file.site(
                match.start(),
                f"{RULE}: `{root}::sync::atomic` bypasses the boundary; use "
                f"`{'::'.join(SELECTED)}`",
            )
        )
    for match in _BARE_ATOMIC.finditer(body):
        preceding = body[: match.start()].rstrip()
        if preceding.endswith(":"):
            # A qualified path, which the rule above has already judged by its root.
            continue
        violations.append(
            file.site(
                match.start(),
                f"{RULE}: `sync::atomic` reaches the atomic module through an imported `sync`; "
                f"use `{'::'.join(SELECTED)}`",
            )
        )
    for alias in sorted(sync_aliases - {"sync"}):
        for match in re.finditer(
            rf"(?<![A-Za-z0-9_:]){re.escape(alias)}\s*::\s*atomic(?![A-Za-z0-9_])", body
        ):
            violations.append(
                file.site(
                    match.start(),
                    f"{RULE}: `{alias}::atomic` reaches the atomic module through a renamed "
                    f"`sync`; use `{'::'.join(SELECTED)}`",
                )
            )
    for match in _PRIMITIVES_ALIAS.finditer(body):
        violations.append(
            file.site(match.start(), f"{RULE}: name the boundary by its own name")
        )
    for match in _UNMODELED_PATH.finditer(body):
        item = match.group("item")
        if item is None:
            violations.append(
                file.site(
                    match.start(),
                    f"{RULE}: name an unmodeled atomic by its full path, "
                    f"`{'::'.join(UNMODELED)}::<item>`",
                )
            )
            continue
        unmodeled.items.setdefault(item, file.line_of(match.start()))
    return violations, unmodeled


@dataclass(frozen=True)
class Permission:
    path: str
    items: frozenset[str]
    owner: str
    reason: str
    limit: str


def parse_permissions(text: str) -> list[Permission]:
    document = tomllib.loads(text)
    permissions: list[Permission] = []
    seen: set[str] = set()
    for index, table in enumerate(document.get("permission", [])):
        context = f"{PERMISSIONS} permission #{index + 1}"
        for key in PERMISSION_FIELDS:
            if key not in table:
                raise ValueError(f"{context} needs `{key}`")
        items = table["items"]
        if not isinstance(items, list) or not items or not all(
            isinstance(item, str) and item for item in items
        ):
            raise ValueError(f"{context} lists no items")
        for key in ("path", "owner", "reason", "limit"):
            if not isinstance(table[key], str) or not table[key].strip():
                raise ValueError(f"{context} needs a non-empty `{key}`")
        if table["path"] in seen:
            raise ValueError(f"{context} repeats {table['path']}")
        seen.add(table["path"])
        permissions.append(
            Permission(
                path=table["path"],
                items=frozenset(items),
                owner=table["owner"],
                reason=table["reason"],
                limit=table["limit"],
            )
        )
    unknown = sorted(set(document) - {"permission"})
    if unknown:
        raise ValueError(f"{PERMISSIONS} has unknown tables: {', '.join(unknown)}")
    return permissions


def check_permissions(
    uses: Mapping[str, FileUses], permissions: Sequence[Permission]
) -> list[str]:
    """Match every unmodeled use against a permission, and every permission against a use."""

    problems: list[str] = []
    by_path = {permission.path: permission for permission in permissions}
    for path, file_uses in sorted(uses.items()):
        if not file_uses.items:
            continue
        permission = by_path.get(path)
        if permission is None:
            first_line = min(file_uses.items.values())
            problems.append(
                f"{path}:{first_line}: {RULE}: unmodeled atomics need a permission in "
                f"{PERMISSIONS}"
            )
            continue
        for item, line in sorted(file_uses.items.items()):
            if item not in permission.items:
                problems.append(
                    f"{path}:{line}: {RULE}: the permission in {PERMISSIONS} does not list "
                    f"unmodeled `{item}`"
                )
    for permission in permissions:
        used = uses.get(permission.path)
        used_items = set(used.items) if used is not None else set()
        if not used_items:
            problems.append(
                f"{PERMISSIONS}: stale permission: {permission.path} uses no unmodeled atomic"
            )
            continue
        for item in sorted(permission.items - used_items):
            problems.append(
                f"{PERMISSIONS}: stale permission: {permission.path} does not use unmodeled "
                f"`{item}`"
            )
    return problems


@dataclass(frozen=True)
class Package:
    name: str
    manifest: str
    features: Mapping[str, Sequence[str]]
    normal: frozenset[str]
    every_kind: Mapping[str, Mapping[str, object]]


def _dependency_tables(document: Mapping[str, object]) -> Iterator[tuple[str, Mapping[str, object]]]:
    for kind in ("dependencies", "dev-dependencies", "build-dependencies"):
        table = document.get(kind)
        if isinstance(table, dict):
            yield kind, table
    targets = document.get("target")
    if isinstance(targets, dict):
        for target in targets.values():
            if not isinstance(target, dict):
                continue
            for kind in ("dependencies", "dev-dependencies", "build-dependencies"):
                table = target.get(kind)
                if isinstance(table, dict):
                    yield kind, table


def _package_of(key: str, entry: object, workspace: Mapping[str, object]) -> str:
    if isinstance(entry, dict):
        package = entry.get("package")
        if isinstance(package, str):
            return package
        if entry.get("workspace") is True:
            declared = workspace.get(key)
            if isinstance(declared, dict) and isinstance(declared.get("package"), str):
                return declared["package"]
    return key


def load_packages(root: Path) -> list[Package]:
    document = tomllib.loads((root / "Cargo.toml").read_text(encoding="utf-8"))
    workspace = document.get("workspace", {})
    workspace_dependencies = workspace.get("dependencies", {})
    packages: list[Package] = []
    for member in workspace.get("members", []):
        manifest = PurePosixPath(member) / "Cargo.toml"
        member_document = tomllib.loads((root / manifest).read_text(encoding="utf-8"))
        name = member_document["package"]["name"]
        normal: set[str] = set()
        every_kind: dict[str, dict[str, object]] = {}
        for kind, table in _dependency_tables(member_document):
            for key, entry in table.items():
                package = _package_of(key, entry, workspace_dependencies)
                every_kind.setdefault(package, {})[kind] = entry
                if kind == "dependencies":
                    normal.add(package)
        packages.append(
            Package(
                name=name,
                manifest=str(manifest),
                features=member_document.get("features", {}),
                normal=frozenset(normal),
                every_kind=every_kind,
            )
        )
    return packages


def check_manifests(packages: Sequence[Package]) -> list[str]:
    problems: list[str] = []
    by_name = {package.name: package for package in packages}
    for package in packages:
        loom = package.every_kind.get("loom")
        if loom is not None:
            if package.name not in (OWNER, HARNESS):
                problems.append(
                    f"{package.manifest}: {RULE}: only {OWNER} selects Loom and only {HARNESS} "
                    "runs it; depend on them instead of `loom`"
                )
            else:
                for kind, entry in loom.items():
                    if not (isinstance(entry, dict) and entry.get("optional") is True):
                        problems.append(
                            f"{package.manifest}: {RULE}: `loom` must be an optional {kind} "
                            "entry, so no ordinary graph contains it"
                        )
        if package.name == OWNER:
            continue
        for mode in MODES:
            enabled = package.features.get(mode)
            if enabled is None:
                continue
            forwarded = set(enabled)
            if OWNER not in package.normal:
                problems.append(
                    f"{package.manifest}: {RULE}: `{mode}` is a mode feature, so the package "
                    f"depends on {OWNER} directly to select it"
                )
            elif not forwarded & {f"{OWNER}/{mode}", f"{OWNER}?/{mode}"}:
                problems.append(
                    f"{package.manifest}: {RULE}: feature `{mode}` does not forward "
                    f"`{OWNER}/{mode}`"
                )
            for dependency in sorted(package.normal):
                owner = by_name.get(dependency)
                if owner is None or dependency == OWNER or mode not in owner.features:
                    continue
                if not forwarded & {f"{dependency}/{mode}", f"{dependency}?/{mode}"}:
                    problems.append(
                        f"{package.manifest}: {RULE}: feature `{mode}` does not forward "
                        f"`{dependency}/{mode}`, so that dependency would run another mode"
                    )
    return problems


def rust_sources(root: Path) -> list[str]:
    completed = subprocess.run(
        ["git", "-C", str(root), "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
        capture_output=True,
        check=True,
        text=True,
    )
    return sorted(
        entry
        for entry in completed.stdout.split("\0")
        if entry.endswith(".rs") and (root / entry).is_file()
    )


def check(root: Path) -> list[str]:
    problems: list[str] = []
    uses: dict[str, FileUses] = {}
    for path in rust_sources(root):
        if path.startswith(OWNER_SOURCES):
            continue
        file = RustFile(path, (root / path).read_text(encoding="utf-8"))
        violations, file_uses = check_source(file)
        problems.extend(site.render() for site in violations)
        uses[path] = file_uses
    permissions_file = root / PERMISSIONS
    try:
        permissions = parse_permissions(permissions_file.read_text(encoding="utf-8"))
    except (OSError, ValueError, tomllib.TOMLDecodeError) as error:
        return [*problems, f"{PERMISSIONS}: {RULE}: {error}"]
    problems.extend(check_permissions(uses, permissions))
    problems.extend(check_manifests(load_packages(root)))
    return problems


def repository_root() -> Path:
    completed = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"], capture_output=True, check=True, text=True
    )
    return Path(completed.stdout.strip())


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", type=Path, default=None)
    arguments = parser.parse_args(argv)
    root = arguments.root or repository_root()
    problems = check(root)
    for problem in problems:
        print(problem, file=sys.stderr)
    if problems:
        print(
            f"{len(problems)} {RULE} violation(s). Every atomic, ordering and fence comes from "
            f"{'::'.join(SELECTED)}; a real atomic outside every model comes from "
            f"{'::'.join(UNMODELED)} with a permission in {PERMISSIONS}.",
            file=sys.stderr,
        )
        return 1
    print(f"{RULE}: every atomic goes through {OWNER}, and every mode feature is forwarded")
    return 0


if __name__ == "__main__":
    sys.exit(main())
