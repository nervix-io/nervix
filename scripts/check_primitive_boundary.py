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

A selected atomic belongs to one model execution, so it never lives in a `static`, which outlives
every execution, and it is never constructed in a const context, which Loom's atomics do not
support. The static rules reject a `static`, including one a `thread_local!` declares, whose
declared type names a selected atomic type directly, through a wrapper, an array, a reference, a
module path or a local type alias. They also reject a `static` or `const` initializer, a `const fn`
body and an inline `const` block that construct one. A bare atomic type name counts as selected
unless the file imports it only from the unmodeled path. The rules read declared types and
constructions, so a struct that holds an atomic hides it from them when it is built lazily; the rule
still applies to it.

A real atomic that must stay outside every model is reached through `nervix_primitives::unmodeled`
and needs a permission in `crates/primitives/unmodeled-permissions.toml` naming the file, the items
it uses, its owner, the reason and the verification limit. A use without a permission, an item the
permission does not list, and a permission nothing uses all fail. A real atomic may live in a
`static`.

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
# The atomic types `nervix_primitives::sync::atomic` selects for the build's execution mode.
ATOMIC_TYPES = frozenset(
    {
        "AtomicBool",
        "AtomicI8",
        "AtomicI16",
        "AtomicI32",
        "AtomicI64",
        "AtomicIsize",
        "AtomicPtr",
        "AtomicU8",
        "AtomicU16",
        "AtomicU32",
        "AtomicU64",
        "AtomicUsize",
    }
)

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
_IDENTIFIER = r"\$?[A-Za-z_][A-Za-z0-9_]*"
_PATH = re.compile(rf"(?<![A-Za-z0-9_$])(?:::\s*)?{_IDENTIFIER}(?:\s*::\s*{_IDENTIFIER})*")
# A lifetime keeps its tick after literals are blanked, so `'static` is never an item.
_STATIC_ITEM = re.compile(
    rf"(?<![A-Za-z0-9_$'])static\s+(?:mut\s+|ref\s+)?(?P<name>{_IDENTIFIER})\s*:"
)
_CONST_ITEM = re.compile(rf"(?<![A-Za-z0-9_$*])const\s+(?P<name>{_IDENTIFIER})\s*:")
_CONST_FN = re.compile(
    rf"(?<![A-Za-z0-9_$])const\s+(?:unsafe\s+)?(?:extern\s+(?:\"[^\"]*\"\s+)?)?fn\s+(?P<name>{_IDENTIFIER})"
)
_CONST_BLOCK = re.compile(r"(?<![A-Za-z0-9_$])const\s*\{")
_TYPE_ALIAS = re.compile(rf"(?<![A-Za-z0-9_$])type\s+(?P<name>{_IDENTIFIER})\b")
_TURBOFISH_NEW = re.compile(
    rf"(?P<path>{_PATH.pattern})\s*::\s*<[^;{{}}]*?>\s*::\s*new(?![A-Za-z0-9_])"
)
_OPENERS = {"(": ")", "[": "]", "{": "}", "<": ">"}
_CLOSERS = frozenset(_OPENERS.values())


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


def _segments(path: str) -> list[str]:
    segments: list[str] = []
    for segment in path.split("::"):
        stripped = segment.strip()
        if stripped:
            segments.append(stripped)
    return segments


@dataclass
class AtomicNames:
    """The local names one file binds to atomic types: selected ones, and real unmodeled ones."""

    selected: set[str] = field(default_factory=set)
    unmodeled: set[str] = field(default_factory=set)

    def bind(self, leaf: UseLeaf) -> None:
        path = leaf.path
        if len(path) == len(SELECTED) + 1 and path[: len(SELECTED)] == SELECTED:
            item = path[-1]
            if item == "*":
                self.selected.update(ATOMIC_TYPES)
            elif item in ATOMIC_TYPES:
                self.selected.add(leaf.alias or item)
        elif len(path) == len(UNMODELED) + 1 and path[: len(UNMODELED)] == UNMODELED:
            item = path[-1]
            if item in ATOMIC_TYPES:
                self.unmodeled.add(leaf.alias or item)

    def bind_aliases(self, code: str) -> None:
        """Bind every local type alias whose target names a selected atomic type."""

        targets: dict[str, str] = {}
        for match in _TYPE_ALIAS.finditer(code):
            equals = _end_of_type(code, match.end())
            if equals >= len(code) or code[equals] != "=":
                continue
            target_end = _end_of_type(code, equals + 1)
            targets[match.group("name")] = code[equals + 1 : target_end]
        changed = True
        while changed:
            changed = False
            for name, target in targets.items():
                if name not in self.selected and self.names_selected(target):
                    self.selected.add(name)
                    changed = True

    def is_selected(self, path: str) -> bool:
        """Whether `path` names a selected atomic type. A bare type name is selected unless this
        file imports it only from the unmodeled path."""

        segments = _segments(path)
        last = segments[-1]
        if len(segments) == 1:
            if last in self.selected:
                return True
            return last in ATOMIC_TYPES and last not in self.unmodeled
        if tuple(segments[-1 - len(UNMODELED) : -1]) == UNMODELED:
            return False
        return last in ATOMIC_TYPES

    def names_selected(self, text: str) -> bool:
        for match in _PATH.finditer(text):
            if self.is_selected(match.group()):
                return True
        return False

    def constructs_selected(self, text: str) -> bool:
        for match in _PATH.finditer(text):
            segments = _segments(match.group())
            if len(segments) < 2 or segments[-1] != "new":
                continue
            if self.is_selected("::".join(segments[:-1])):
                return True
        for match in _TURBOFISH_NEW.finditer(text):
            if self.is_selected(match.group("path")):
                return True
        return False


def _end_of_type(code: str, start: int) -> int:
    """Where the type that begins at `start` ends: at the first `=`, `;`, `,` or `{` outside its
    brackets, or at a bracket that closes one opened before it."""

    closing: list[str] = []
    index = start
    while index < len(code):
        char = code[index]
        if code.startswith("->", index):
            index += 2
            continue
        if char in "([<":
            closing.append(_OPENERS[char])
        elif char in ")]>":
            if not closing:
                return index
            closing.pop()
        elif not closing and char in "=;,{}":
            return index
        index += 1
    return index


def _end_of_expression(code: str, start: int) -> int:
    """Where the expression that begins at `start` ends: at the first `;` outside its brackets, or
    at a bracket that closes one opened before it. Angle brackets are comparisons here."""

    closing: list[str] = []
    index = start
    while index < len(code):
        char = code[index]
        if char in "([{":
            closing.append(_OPENERS[char])
        elif char in ")]}":
            if not closing:
                return index
            closing.pop()
        elif not closing and char == ";":
            return index
        index += 1
    return index


def _end_of_block(code: str, start: int) -> int:
    """The offset just past the brace that closes the one at `start`."""

    depth = 0
    for index in range(start, len(code)):
        if code[index] == "{":
            depth += 1
        elif code[index] == "}":
            depth -= 1
            if depth == 0:
                return index + 1
    return len(code)


def _body_of_fn(code: str, start: int) -> tuple[int, int] | None:
    """The span of the body of the function whose signature continues at `start`, if it has one."""

    depth = 0
    index = start
    while index < len(code):
        char = code[index]
        if char in "([":
            depth += 1
        elif char in ")]":
            depth -= 1
        elif depth == 0 and char == ";":
            return None
        elif depth == 0 and char == "{":
            return index, _end_of_block(code, index)
        index += 1
    return None


def _is_generic_parameter(code: str, start: int) -> bool:
    preceding = code[:start].rstrip()
    return preceding.endswith("<") or preceding.endswith(",")


def check_statics(file: RustFile, names: AtomicNames) -> list[Site]:
    """Reject a selected atomic held by a static, or constructed in a const context."""

    violations: list[Site] = []
    code = file.code
    names.bind_aliases(code)
    owner_hint = (
        "give it an owner that lives exactly as long as its state, or make it a real atomic from "
        f"{'::'.join(UNMODELED)} with a permission"
    )
    for match in _STATIC_ITEM.finditer(code):
        name = match.group("name")
        type_end = _end_of_type(code, match.end())
        if names.names_selected(code[match.end() : type_end]):
            violations.append(
                file.site(
                    match.start(),
                    f"{RULE}: static `{name}` holds a selected atomic, which outlives every model "
                    f"execution; {owner_hint}",
                )
            )
            continue
        if type_end < len(code) and code[type_end] == "=":
            initializer_end = _end_of_expression(code, type_end + 1)
            if names.constructs_selected(code[type_end + 1 : initializer_end]):
                violations.append(
                    file.site(
                        match.start(),
                        f"{RULE}: static `{name}` constructs a selected atomic, which outlives "
                        f"every model execution; {owner_hint}",
                    )
                )
    for match in _CONST_ITEM.finditer(code):
        if _is_generic_parameter(code, match.start()):
            continue
        name = match.group("name")
        type_end = _end_of_type(code, match.end())
        constructs = False
        if type_end < len(code) and code[type_end] == "=":
            initializer_end = _end_of_expression(code, type_end + 1)
            constructs = names.constructs_selected(code[type_end + 1 : initializer_end])
        if constructs or names.names_selected(code[match.end() : type_end]):
            violations.append(
                file.site(
                    match.start(),
                    f"{RULE}: const `{name}` makes a selected atomic in a const context, and "
                    "Loom's atomics have no const constructor",
                )
            )
    for match in _CONST_FN.finditer(code):
        body = _body_of_fn(code, match.end())
        if body is None:
            continue
        body_start, body_end = body
        if names.constructs_selected(code[body_start:body_end]):
            violations.append(
                file.site(
                    match.start(),
                    f"{RULE}: const fn `{match.group('name')}` constructs a selected atomic, and "
                    "Loom's atomics have no const constructor; make the function non-const",
                )
            )
    for match in _CONST_BLOCK.finditer(code):
        brace = match.end() - 1
        if names.constructs_selected(code[brace : _end_of_block(code, brace)]):
            violations.append(
                file.site(
                    match.start(),
                    f"{RULE}: a const block constructs a selected atomic, and Loom's atomics have "
                    "no const constructor",
                )
            )
    return violations


@dataclass
class FileUses:
    """What one file takes from the boundary's unmodeled path, and where."""

    items: dict[str, int] = field(default_factory=dict)


def check_source(file: RustFile) -> tuple[list[Site], FileUses]:
    """Return the file's boundary violations and the unmodeled items it uses."""

    violations: list[Site] = []
    unmodeled = FileUses()
    names = AtomicNames()
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
            names.bind(leaf)
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
    violations.extend(check_statics(file, names))
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
            f"{'::'.join(SELECTED)}, and none of those atomics lives in a static; a real atomic "
            f"outside every model comes from {'::'.join(UNMODELED)} with a permission in "
            f"{PERMISSIONS}.",
            file=sys.stderr,
        )
        return 1
    print(f"{RULE}: every atomic goes through {OWNER}, and every mode feature is forwarded")
    return 0


if __name__ == "__main__":
    sys.exit(main())
