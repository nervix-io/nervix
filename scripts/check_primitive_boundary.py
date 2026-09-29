#!/usr/bin/env python3

"""Hold every governed Nervix primitive to the primitive boundary, and every mode to its owner.

`nervix-primitives` selects the execution-sensitive primitives of a build for its execution mode:
atomics, orderings and fences; async and thread-blocking synchronization; tasks, the async runtime
and its attributes and `select!`; streams over channels; publication; concurrent collections;
threads and thread-local storage. Code that reaches one of them any other way escapes that
selection: a modeled build would run it on a real primitive, or on a backend the rest of the graph
does not use, and a check would claim coverage it does not have. The source rules reject every such
path in every tracked or new Rust file outside the owner, including tests, benchmarks, examples and
macro bodies, whether written as an import, a renamed or grouped import, a glob, a fully qualified
path, an attribute, a renamed crate, or through an alias of `std`, `core` or their `sync` module.
Comments and literal contents are blanked first, and conditional compilation is ignored, so an
inactive `cfg` branch is checked like an active one. Each violation names the approved path.

Tokio's timers, networking, I/O, filesystem, processes, signals and its pure `pin!` and `join!`
macros are not governed here yet; `extern crate shuttle_tokio as tokio` remains the one accepted
alias, because it still selects Shuttle's timers for the crates that use them.

A selected atomic belongs to one model execution, so it never lives in a `static`, which outlives
every execution, and it is never constructed in a const context, which Loom's atomics do not
support. The static rules reject a `static`, including one a `thread_local!` declares, whose
declared type names a selected atomic type directly, through a wrapper, an array, a reference, a
module path or a local type alias. They also reject a `static` or `const` initializer, a `const fn`
body and an inline `const` block that construct one. A bare atomic type name counts as selected
unless the file imports it only from the unmodeled path. The rules read declared types and
constructions, so a struct that holds an atomic hides it from them when it is built lazily; the rule
still applies to it.

A real primitive that must stay outside every model is reached through
`nervix_primitives::unmodeled` and needs a permission in `crates/primitives/unmodeled-permissions.toml`
naming the file, the items it uses by their paths below `unmodeled`, its owner, the reason and the
verification limit. A use without a permission, an item the permission does not list, and a
permission nothing uses all fail. A real atomic may live in a `static`.

The manifest rules keep mode selection in one place. Only the owner selects Loom, and the harness
runs it, so no other package may depend on `loom`. Only the owner depends on the libraries whose
families it selects and on their Shuttle wrappers. A package that owns a `loom`, `shuttle` or
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
UNMODELED_ROOT = ("nervix_primitives", "unmodeled")
UNMODELED = UNMODELED_ROOT + ("sync", "atomic")
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

ATOMIC_ITEMS = (
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
    "Ordering",
)
# Every item `nervix_primitives::unmodeled` provides, by its path below `unmodeled`. A permission
# lists items by these paths.
UNMODELED_ITEMS = frozenset(
    {("sync", "atomic", item) for item in ATOMIC_ITEMS}
    | {
        ("sync", "LazyLock"),
        ("sync", "Once"),
        ("sync", "OnceLock"),
        ("sync", "Mutex"),
        ("sync", "mpsc"),
        ("sync", "watch"),
        ("runtime", "Builder"),
        ("runtime", "Runtime"),
        ("thread", "Builder"),
        ("thread", "sleep"),
        ("task", "consume_budget"),
        ("task", "spawn"),
        ("time", "sleep"),
        ("select",),
        ("task_local",),
    }
)


@dataclass(frozen=True)
class Route:
    """A governed path outside the owner, and the boundary path that replaces it."""

    prefix: tuple[str, ...]
    replacement: tuple[str, ...]


# The families beyond atomics, which have rules of their own below. The longest matching prefix
# decides a path's replacement.
ROUTES = (
    Route(("tokio", "sync"), ("nervix_primitives", "sync")),
    Route(("tokio", "task"), ("nervix_primitives", "task")),
    Route(("tokio", "spawn"), ("nervix_primitives", "task", "spawn")),
    Route(("tokio", "task_local"), ("nervix_primitives", "unmodeled", "task_local")),
    Route(("tokio", "runtime"), ("nervix_primitives", "runtime")),
    Route(("tokio", "select"), ("nervix_primitives", "select")),
    Route(("tokio", "test"), ("nervix_primitives", "test")),
    Route(("tokio", "main"), ("nervix_primitives", "main")),
    Route(("tokio_util", "sync"), ("nervix_primitives", "sync")),
    Route(("tokio_util", "task"), ("nervix_primitives", "task")),
    Route(("tokio_stream",), ("nervix_primitives", "stream")),
    Route(("parking_lot",), ("nervix_primitives", "sync", "blocking")),
    Route(("dashmap", "mapref", "entry"), ("nervix_primitives", "collections", "dash_map")),
    Route(("dashmap",), ("nervix_primitives", "collections")),
    Route(("concurrent_queue",), ("nervix_primitives", "collections")),
    Route(("arc_swap",), ("nervix_primitives", "publication")),
    Route(("flume",), ("nervix_primitives", "sync", "blocking", "mpsc")),
    Route(("std", "thread"), ("nervix_primitives", "thread")),
    Route(("std", "thread_local"), ("nervix_primitives", "thread_local")),
    Route(("shuttle", "thread"), ("nervix_primitives", "thread")),
    Route(("shuttle", "thread_local"), ("nervix_primitives", "thread_local")),
    Route(("shuttle", "lazy_static"), ("nervix_primitives", "sync", "blocking")),
    Route(("shuttle", "future", "spawn"), ("nervix_primitives", "task", "spawn")),
    Route(("shuttle", "future", "yield_now"), ("nervix_primitives", "task", "yield_now")),
    Route(("loom", "thread"), ("nervix_primitives", "thread")),
    Route(("loom", "thread_local"), ("nervix_primitives", "thread_local")),
    Route(("loom", "lazy_static"), ("nervix_primitives", "sync", "blocking")),
    Route(("shuttle_tokio",), ("nervix_primitives",)),
    Route(("shuttle_tokio_util",), ("nervix_primitives",)),
    Route(("shuttle_tokio_stream",), ("nervix_primitives", "stream")),
    Route(("shuttle_parking_lot",), ("nervix_primitives", "sync", "blocking")),
    Route(("shuttle_dashmap",), ("nervix_primitives", "collections")),
)
# The `sync` modules whose non-atomic items are the thread-blocking family, and what stays allowed
# in them: shared ownership is not governed here.
SYNC_MODULES = (("std", "sync"), ("core", "sync"), ("shuttle", "sync"), ("loom", "sync"))
SYNC_UNGOVERNED = frozenset({"Arc", "Weak", "atomic"})
BLOCKING = ("nervix_primitives", "sync", "blocking")
GOVERNED_ROOTS = frozenset(
    {route.prefix[0] for route in ROUTES} | {module[0] for module in SYNC_MODULES}
)
# What Loom models: in a Loom build every other family is the ordinary library, outside every model,
# so Loom model code, a module compiled only for Loom, names only these. A module prefix admits every
# item below it; an item admits only itself.
LOOM_MODELED_MODULES = (("nervix_primitives", "sync", "atomic"),)
LOOM_MODELED_THREAD = frozenset({"Builder", "JoinHandle", "current", "park", "spawn", "yield_now"})
LOOM_MODELED_ITEMS = frozenset(
    {("nervix_primitives", "thread", item) for item in LOOM_MODELED_THREAD}
    | {("nervix_primitives", "thread_local")}
)
# The one accepted crate alias: it still selects Shuttle's timers for the crates that use them.
ACCEPTED_ALIAS = ("shuttle_tokio", "tokio")
# Packages whose families the owner selects. No other package depends on them.
OWNER_ONLY_PACKAGES = frozenset(
    {
        "arc-swap",
        "concurrent-queue",
        "dashmap",
        "flume",
        "parking_lot",
        "shuttle-dashmap",
        "shuttle-parking_lot",
        "shuttle-tokio-stream",
        "shuttle-tokio-util",
        "tokio-stream",
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
    r"(?<![A-Za-z0-9_:])nervix_primitives\s*::\s*unmodeled(?P<rest>(?:\s*::\s*[A-Za-z_][A-Za-z0-9_]*)*)"
)
_PRIMITIVES_ALIAS = re.compile(r"(?<![A-Za-z0-9_:])nervix_primitives\s+as\s+")
_QUALIFIED_PATH = re.compile(
    r"(?<![A-Za-z0-9_$])(?P<path>[A-Za-z_][A-Za-z0-9_]*(?:\s*::\s*[A-Za-z_][A-Za-z0-9_]*)+)"
)
_BARE_THREAD_LOCAL = re.compile(r"(?<![A-Za-z0-9_:$])thread_local\s*!")
# A `mod` item with a body, and the attributes in front of it.
_MODULE_WITH_BODY = re.compile(
    r"(?P<attributes>(?:#\[[^\]]*\]\s*)*)(?:pub(?:\s*\([^)]*\))?\s+)?mod\s+"
    r"(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*\{"
)
_NEGATED_CFG = re.compile(r"not\s*\([^()]*\)")
_LOOM_FEATURE = re.compile(r"feature\s*=\s*\"loom\"")
_CFG_ATTRIBUTE = re.compile(r"#\[\s*cfg\s*\((?P<condition>.*?)\)\s*\]", re.S)
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


def route_of(path: Sequence[str]) -> tuple[str, ...] | None:
    """The boundary path that replaces a governed `path`, or `None` when it is not governed.

    Atomic paths have rules of their own and are not answered here.
    """

    path = tuple(path)
    best: Route | None = None
    for route in ROUTES:
        if path[: len(route.prefix)] == route.prefix:
            if best is None or len(route.prefix) > len(best.prefix):
                best = route
    if best is not None:
        return best.replacement + path[len(best.prefix) :]
    for module in SYNC_MODULES:
        if path[: len(module)] == module and len(path) > len(module):
            if path[len(module)] not in SYNC_UNGOVERNED:
                return BLOCKING + path[len(module) :]
    return None


def governs_below(path: Sequence[str]) -> bool:
    """Whether a governed path lies below `path`, so a glob over it or a new name for it reaches
    governed items the rules could no longer see."""

    path = tuple(path)
    for route in ROUTES:
        if len(path) < len(route.prefix) and route.prefix[: len(path)] == path:
            return True
    return any(len(path) <= len(module) and module[: len(path)] == path for module in SYNC_MODULES)


def _unmodeled_item(rest: Sequence[str]) -> tuple[str, ...] | None:
    """The unmodeled item a path below `unmodeled` names, by the longest prefix that is one."""

    rest = tuple(rest)
    for length in range(len(rest), 0, -1):
        if rest[:length] in UNMODELED_ITEMS:
            return rest[:length]
    return None


def _segments(path: str) -> tuple[str, ...]:
    """The segments of a path, however it is spaced."""

    segments: list[str] = []
    for segment in path.split("::"):
        stripped = segment.strip()
        if stripped:
            segments.append(stripped)
    return tuple(segments)


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


def _loom_only(attributes: str) -> bool:
    """Whether `attributes` compile their item only for Loom: a `cfg` that requires the `loom`
    feature outside every `not(...)`."""

    for match in _CFG_ATTRIBUTE.finditer(attributes):
        condition = match.group("condition")
        previous = None
        while previous != condition:
            previous = condition
            condition = _NEGATED_CFG.sub("", condition)
        if _LOOM_FEATURE.search(condition):
            return True
    return False


def _loom_modeled(path: Sequence[str]) -> bool:
    path = tuple(path)
    if path in LOOM_MODELED_ITEMS:
        return True
    return any(path[: len(module)] == module for module in LOOM_MODELED_MODULES)


def check_loom_models(file: RustFile) -> list[Site]:
    """Reject a family Loom does not model in a module compiled only for Loom.

    In a Loom build such a family is the ordinary library, which no model observes, so a model that
    named one would run a real primitive silently. What an owner the model drives uses internally
    is outside this check: a Loom claim excludes it.
    """

    violations: list[Site] = []
    for module in _MODULE_WITH_BODY.finditer(file.literals):
        if not _loom_only(module.group("attributes")):
            continue
        brace = module.end() - 1
        start, end = brace, _end_of_block(file.code, brace)
        block = file.code[start:end]
        thread_aliases: set[str] = set()

        def reject(offset: int, path: Sequence[str]) -> None:
            violations.append(
                file.site(
                    start + offset,
                    f"{RULE}: Loom model code names `{'::'.join(path)}`, which a Loom build takes "
                    "from the ordinary library, outside every model; a Loom model uses only "
                    "atomics, the threads it spawns, joins, parks and yields, and thread-local "
                    "storage",
                )
            )

        use_spans: list[tuple[int, int]] = []
        for match in _USE_ITEM.finditer(block):
            use_spans.append((match.start(), match.end()))
            try:
                leaves = use_leaves(match.group("tree"))
            except UseTreeError:
                continue
            for leaf in leaves:
                path = leaf.path
                if path[:1] != ("nervix_primitives",):
                    continue
                if path == ("nervix_primitives", "thread"):
                    thread_aliases.add(leaf.alias or "thread")
                    continue
                if not _loom_modeled(path):
                    reject(match.start(), path)
        outside_uses = list(block)
        for use_start, use_end in use_spans:
            for index in range(use_start, use_end):
                if outside_uses[index] != "\n":
                    outside_uses[index] = " "
        body = "".join(outside_uses)
        for match in _QUALIFIED_PATH.finditer(body):
            path = _segments(match.group("path"))
            if path[0] == "nervix_primitives":
                if path[:2] == ("nervix_primitives", "thread") and len(path) > 2:
                    path = path[:3]
                if not _loom_modeled(path):
                    reject(match.start(), path)
            elif path[0] in thread_aliases and path[1] not in LOOM_MODELED_THREAD:
                reject(match.start(), ("nervix_primitives", "thread", path[1]))
    return violations


def check_source(file: RustFile) -> tuple[list[Site], FileUses]:
    """Return the file's boundary violations and the unmodeled items it uses."""

    violations: list[Site] = []
    unmodeled = FileUses()
    names = AtomicNames()
    code = file.code
    use_spans: list[tuple[int, int]] = []
    sync_aliases: set[str] = set()
    sync_module_aliases: set[str] = set()

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
                if path[:2] == UNMODELED_ROOT:
                    rest = path[len(UNMODELED_ROOT) :]
                    if rest[-1:] == ("*",):
                        violations.append(
                            file.site(
                                match.start(),
                                f"{RULE}: import unmodeled items by name, not with a glob",
                            )
                        )
                        continue
                    if rest not in UNMODELED_ITEMS:
                        violations.append(
                            file.site(
                                match.start(),
                                f"{RULE}: import unmodeled items by name, such as "
                                f"`{'::'.join(UNMODELED)}::AtomicUsize`, not `{'::'.join(path)}`",
                            )
                        )
                        continue
                    unmodeled.items.setdefault("::".join(rest), file.line_of(match.start()))
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
            if path in SYNC_MODULES:
                sync_module_aliases.add(leaf.alias or "sync")
                continue
            if path in (("std",), ("core",)) and leaf.alias is not None:
                violations.append(
                    file.site(
                        match.start(),
                        f"{RULE}: `{path[0]}` is renamed to `{leaf.alias}`, which hides its atomic "
                        "module from this check",
                    )
                )
                continue
            replacement = route_of(path)
            if replacement is not None:
                violations.append(
                    file.site(
                        match.start(),
                        f"{RULE}: `{'::'.join(path)}` bypasses the boundary; use "
                        f"`{'::'.join(replacement)}`",
                    )
                )
                continue
            if path[-1:] == ("*",) and governs_below(path[:-1]):
                violations.append(
                    file.site(
                        match.start(),
                        f"{RULE}: `use {'::'.join(path)}` brings governed primitives into scope; "
                        "import them by name from `nervix_primitives`",
                    )
                )
                continue
            if leaf.alias is not None and governs_below(path):
                violations.append(
                    file.site(
                        match.start(),
                        f"{RULE}: `{'::'.join(path)}` is renamed to `{leaf.alias}`, which hides "
                        "the governed primitives below it from this check",
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
        if (
            alias is not None
            and name in GOVERNED_ROOTS - {"std", "core"}
            and (name, alias) != ACCEPTED_ALIAS
        ):
            violations.append(
                file.site(
                    match.start(),
                    f"{RULE}: `extern crate {name} as {alias}` selects a backend outside the "
                    "boundary; use `nervix_primitives`",
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
    for match in _QUALIFIED_PATH.finditer(body):
        path = _segments(match.group("path"))
        root = path[0]
        if root in sync_module_aliases and path[1] not in SYNC_UNGOVERNED:
            violations.append(
                file.site(
                    match.start(),
                    f"{RULE}: `{'::'.join(path)}` reaches thread-blocking synchronization through "
                    f"an imported `sync` module; use `{'::'.join(BLOCKING + path[1:])}`",
                )
            )
            continue
        if root not in GOVERNED_ROOTS:
            continue
        replacement = route_of(path)
        if replacement is None:
            continue
        violations.append(
            file.site(
                match.start(),
                f"{RULE}: `{'::'.join(path)}` bypasses the boundary; use "
                f"`{'::'.join(replacement)}`",
            )
        )
    for match in _BARE_THREAD_LOCAL.finditer(body):
        violations.append(
            file.site(
                match.start(),
                f"{RULE}: `thread_local!` is the standard library's; use "
                "`nervix_primitives::thread_local!`",
            )
        )
    for match in _PRIMITIVES_ALIAS.finditer(body):
        violations.append(
            file.site(match.start(), f"{RULE}: name the boundary by its own name")
        )
    for match in _UNMODELED_PATH.finditer(body):
        rest = _segments(match.group("rest"))
        item = _unmodeled_item(rest)
        if item is None:
            violations.append(
                file.site(
                    match.start(),
                    f"{RULE}: name an unmodeled item by its path, such as "
                    f"`{'::'.join(UNMODELED)}::AtomicUsize`",
                )
            )
            continue
        unmodeled.items.setdefault("::".join(item), file.line_of(match.start()))
    violations.extend(check_statics(file, names))
    violations.extend(check_loom_models(file))
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
                f"{path}:{first_line}: {RULE}: unmodeled items need a permission in "
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
                f"{PERMISSIONS}: stale permission: {permission.path} uses no unmodeled item"
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
        if package.name != OWNER:
            for dependency in sorted(OWNER_ONLY_PACKAGES & set(package.every_kind)):
                problems.append(
                    f"{package.manifest}: {RULE}: only {OWNER} depends on `{dependency}`, whose "
                    "family it selects for the execution mode; take the family from it instead"
                )
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
            f"{len(problems)} {RULE} violation(s). Every governed primitive comes from {OWNER}, and "
            "no selected atomic lives in a static; a real primitive outside every model comes from "
            f"{'::'.join(UNMODELED_ROOT)} with a permission in {PERMISSIONS}.",
            file=sys.stderr,
        )
        return 1
    print(
        f"{RULE}: every governed primitive goes through {OWNER}, and every mode feature is "
        "forwarded"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
