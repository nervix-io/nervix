#!/usr/bin/env python3

"""Hold every governed Nervix primitive to the primitive boundary, and every mode to its owner.

`nervix-primitives` selects the execution-sensitive primitives of a build for its execution mode:
atomics, orderings and fences; shared ownership; async and thread-blocking synchronization, including
waker registration; tasks, the async runtime and its attributes and `select!`; timers and the
monotonic clock; sockets; streams over channels; publication; concurrent collections; threads and
thread-local storage. Code that reaches one of them any other way escapes that selection: a modeled
build would run it on a real primitive, or on a backend the rest of the graph does not use, a
simulated host would wait on the operating system's clock or reach its network, and a check would
claim coverage it does not have. The source rules reject every such path in every tracked or new
Rust file outside the owner, including tests, benchmarks, examples and macro bodies, whether written
as an import, a renamed or grouped import, a glob, a fully qualified path, an attribute, a renamed
crate, or through an alias of `std`, `core` or their `sync`, `time` or `net` module. Comments and
literal contents are blanked first, and conditional compilation is ignored, so an inactive `cfg`
branch is checked like an active one. Each violation names the approved path.

The `futures` crates' channels, locks, executors, waker registration, `select!` and abort handles
are governed too: a native task takes those families from the boundary, which selects them from
Tokio, and the browser console, whose event loop no mode runs, reaches the `futures` ones through
`nervix_primitives::unmodeled::futures` under a permission. Their pure combinators, like Tokio's I/O
traits, filesystem, process and signal modules and its `pin!` and `join!`, are real in every mode
and are not governed here. Guest code, the WASM guest SDK and every guest library built on it, is
compiled into a user's WASM guest, where no mode exists and no Nervix process runs, so its sources
are outside the source rules.

Resolving a name through the operating system, with `tokio::net::lookup_host` or a
`ToSocketAddrs` trait, goes around the node's resolver, so those paths are rejected too, naming the
resolver instead. The boundary's CPU-job mechanism, `nervix_primitives::task::spawn_cpu`, belongs to
the bounded executor, which admits, charges and cancels every job it runs; any other file that names
it is rejected, so the mechanism gives no caller a way around admission.

An execution mode is a feature of the boundary and never a global cfg, which every crate of a build
reads, Tokio's included. A bare `loom`, `shuttle` or `turmoil` in a `cfg` predicate is rejected, and
so is `--cfg loom`, `--cfg shuttle` or `--cfg turmoil` in any `justfile` recipe, Cargo
configuration, workflow or build script. Tokio's unstable runtime controls belong to the Turmoil
build alone: `--cfg tokio_unstable` may appear only in a `justfile` recipe whose name names Turmoil.

A selected atomic belongs to one model execution, so it never lives in a `static`, which outlives
every execution, and it is never constructed in a const context, which Loom's atomics do not
support. The static rules reject a `static`, including one a `thread_local!` declares, whose
declared type names a selected atomic type directly, through a wrapper, an array, a reference, a
module path or a local type alias. They also reject a `static` or `const` initializer, a `const fn`
body and an inline `const` block that construct one. A bare atomic type name counts as selected
unless the file imports it only from the unmodeled path. The rules read declared types and
constructions, so a struct that holds an atomic hides it from them when it is built lazily; the rule
still applies to it.

Loom models atomics, its threads and thread-local storage, and nothing else, so Loom model code, an
inline module or a module file whose declaration compiles it only for Loom, names no other selected
family: in a Loom build that family is the ordinary library, outside every model. Shared ownership,
real in every mode, and permitted unmodeled primitives are named explicitly, and pass.

A real primitive that must stay outside every model is reached through
`nervix_primitives::unmodeled` and needs a permission in `crates/primitives/unmodeled-permissions.toml`
naming the one Rust file, the items it uses by their paths below `unmodeled`, its owner, the reason
and the verification limit. A use without a permission, an item the permission does not list, a
permission for a directory, a glob or a file the boundary does not govern, an item the unmodeled path
does not have, and a permission nothing uses all fail. A real atomic may live in a `static`.

The manifest rules keep mode selection in one place, in every tracked or new manifest, whether or
not the workspace lists it. Only the owner selects Loom, and the harness runs it, so no other
package may depend on `loom`. Only the owner depends on the libraries whose families it selects and
on their Shuttle wrappers. No other package renames a governed crate, which would hide its paths from
the source rules. Turmoil is also a runner, so beside the owner, a package whose harness drives a
simulation may depend on it, as an optional dependency its own `turmoil` feature enables, and never
names its network. A package that owns a `loom`, `shuttle` or `turmoil` feature depends on
`nervix-primitives` directly and forwards the mode to it, and forwards it to every workspace
dependency that owns the same mode, so the whole graph of that package uses one backend even when it
is built on its own.

Run it from the repository root as `python3 -m scripts.check_primitive_boundary`, which is what
`just validate-primitive-boundary` does.
"""

from __future__ import annotations

import argparse
import posixpath
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
        ("time", "Instant"),
        ("net", "TcpListener"),
        ("futures", "mpsc"),
        ("futures", "select"),
        ("futures", "AbortHandle"),
        ("futures", "Abortable"),
        ("select",),
        ("task_local",),
    }
)


@dataclass(frozen=True)
class Route:
    """A governed path outside the owner, and the path that replaces it."""

    prefix: tuple[str, ...]
    replacement: tuple[str, ...]
    # What reaching the governed path does, in a violation's words.
    verb: str = "bypasses the boundary"
    # Whether the replacement names the whole replacing item, so the rest of a longer governed path
    # is not carried over to it.
    exact: bool = False


RESOLVES_AROUND = "resolves names around the node's resolver"
NODE_RESOLVER = ("nervix_dns", "DnsResolver")
CANCELLATION = ("nervix_primitives", "sync", "CancellationToken")


# The families beyond atomics, which have rules of their own below. The longest matching prefix
# decides a path's replacement.
ROUTES = (
    Route(("tokio", "sync"), ("nervix_primitives", "sync")),
    Route(("tokio", "task"), ("nervix_primitives", "task")),
    Route(("tokio", "spawn"), ("nervix_primitives", "task", "spawn")),
    Route(("tokio", "task_local"), ("nervix_primitives", "unmodeled", "task_local")),
    Route(("tokio", "runtime"), ("nervix_primitives", "runtime")),
    Route(("tokio", "time"), ("nervix_primitives", "time")),
    Route(("tokio", "time", "Duration"), ("std", "time", "Duration")),
    Route(("tokio", "net"), ("nervix_primitives", "net")),
    Route(("tokio", "net", "lookup_host"), NODE_RESOLVER, RESOLVES_AROUND, exact=True),
    Route(("tokio", "net", "ToSocketAddrs"), NODE_RESOLVER, RESOLVES_AROUND, exact=True),
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
    Route(("std", "time", "Instant"), ("nervix_primitives", "time", "Instant")),
    Route(("std", "net", "TcpListener"), ("nervix_primitives", "net", "TcpListener")),
    Route(("std", "net", "TcpStream"), ("nervix_primitives", "net", "TcpStream")),
    Route(("std", "net", "UdpSocket"), ("nervix_primitives", "net", "UdpSocket")),
    Route(("std", "net", "ToSocketAddrs"), NODE_RESOLVER, RESOLVES_AROUND, exact=True),
    Route(("std", "os", "unix", "net"), ("nervix_primitives", "net")),
    Route(("turmoil", "net"), ("nervix_primitives", "net")),
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
    # Shared ownership: the same library types in every mode, reached through the boundary.
    Route(("triomphe",), ("nervix_primitives", "sync")),
    *(
        Route((*module, "Arc"), ("nervix_primitives", "sync", "StdArc"))
        for module in (("std", "sync"), ("alloc", "sync"), ("shuttle", "sync"), ("loom", "sync"))
    ),
    *(
        Route((*module, "Weak"), ("nervix_primitives", "sync", "StdWeak"))
        for module in (("std", "sync"), ("alloc", "sync"), ("shuttle", "sync"), ("loom", "sync"))
    ),
    # The `futures` crates' own synchronization, executors, waker registration, `select!` and abort
    # handles, whose native families the boundary selects from Tokio. The browser console, which
    # runs in no execution mode, reaches the ones it needs through `unmodeled::futures`.
    *(
        route
        for facade in (("futures",), ("futures_util",))
        for route in (
            Route((*facade, "channel"), ("nervix_primitives", "sync")),
            Route((*facade, "lock"), ("nervix_primitives", "sync")),
            Route((*facade, "executor"), ("nervix_primitives", "runtime")),
            Route(
                (*facade, "task", "AtomicWaker"),
                ("nervix_primitives", "sync", "AtomicWaker"),
                exact=True,
            ),
            Route((*facade, "select"), ("nervix_primitives", "select"), exact=True),
            Route((*facade, "select_biased"), ("nervix_primitives", "select"), exact=True),
            Route(
                (*facade, "future", "AbortHandle"),
                ("nervix_primitives", "task", "AbortHandle"),
                exact=True,
            ),
            Route((*facade, "future", "Abortable"), CANCELLATION, exact=True),
            Route((*facade, "future", "AbortRegistration"), CANCELLATION, exact=True),
            Route((*facade, "future", "abortable"), CANCELLATION, exact=True),
            Route((*facade, "stream", "abortable"), CANCELLATION, exact=True),
        )
    ),
    Route(("futures_channel",), ("nervix_primitives", "sync")),
    Route(("futures_executor",), ("nervix_primitives", "runtime")),
    Route(("futures_core", "task", "__internal", "AtomicWaker"), ("nervix_primitives", "sync", "AtomicWaker"), exact=True),
    Route(("atomic_waker",), ("nervix_primitives", "sync")),
)
# The `sync` modules whose items, beyond the atomics and shared ownership that have routes of their
# own, are the thread-blocking family.
SYNC_MODULES = (
    ("std", "sync"),
    ("core", "sync"),
    ("alloc", "sync"),
    ("shuttle", "sync"),
    ("loom", "sync"),
)
SYNC_UNGOVERNED = frozenset({"atomic"})
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
# What Loom model code may name beside what Loom models, because naming it hides nothing: shared
# ownership, which is real in every mode and whose reference counts no claim relies on, and the
# unmodeled path, whose real primitives each carry a permission.
LOOM_REAL_ITEMS = frozenset(
    {("nervix_primitives", "sync", item) for item in ("Arc", "StdArc", "StdWeak")}
)
# Modules below which the routes govern some items but not others, such as `std::time`, whose
# `Instant` is governed and whose `Duration` is a value. A file that imports one of them reaches its
# governed items through the name it binds.
PARTLY_GOVERNED_MODULES = (("std", "time"), ("std", "net"), ("std", "os", "unix"))
# Items of the boundary only their owners may name, each with the files that own it and what it is.
CONFINED = {
    ("nervix_primitives", "task", "spawn_cpu"): (
        frozenset({"crates/execution/src/workers.rs"}),
        "the bounded executor's mechanism for an admitted CPU job",
    ),
}
# A cfg passed to every crate of a build: a `--cfg` flag, in any of the spellings a recipe, Cargo
# configuration or workflow uses, or one a build script emits. Only Tokio's unstable runtime
# controls may be such a cfg, and only in the Turmoil recipes.
_GLOBAL_CFG = re.compile(
    r"--cfg(?:\s*=\s*|[\"',\s]+)[\"']?(?P<name>[A-Za-z_][A-Za-z0-9_]*)"
    r"|rustc-cfg=(?P<emitted>[A-Za-z_][A-Za-z0-9_]*)"
)
TOKIO_UNSTABLE = "tokio_unstable"
JUSTFILE = "justfile"
CONFIGURATION_GLOBS = (".cargo/config.toml", ".cargo/config", ".github/workflows/*.yaml", ".github/workflows/*.yml")
# A recipe header starts at the first column and ends its name and parameters with a colon that does
# not begin an assignment.
_RECIPE = re.compile(r"^@?(?P<name>[A-Za-z_][A-Za-z0-9_-]*)[^:\n]*:(?!=)")
# Packages whose families the owner selects. No other package depends on them.
OWNER_ONLY_PACKAGES = frozenset(
    {
        "arc-swap",
        "atomic-waker",
        "concurrent-queue",
        "dashmap",
        "flume",
        "futures-channel",
        "futures-executor",
        "parking_lot",
        "shuttle-dashmap",
        "shuttle-parking_lot",
        "shuttle-tokio",
        "shuttle-tokio-stream",
        "shuttle-tokio-util",
        "tokio-stream",
        "triomphe",
    }
)
# Turmoil is a runner as well as the network the owner selects, so a package whose harness drives a
# simulation may depend on it behind its own `turmoil` feature.
TURMOIL = "turmoil"
# The WASM guest SDK. It and every guest library built on it are compiled into a user's WASM guest,
# a single-threaded program inside the host's sandbox where no execution mode exists and that no
# Nervix process runs, so their sources are outside the source rules; their manifests are checked.
GUEST_SDK = "nervix-wasm-sdk"

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
# A `mod` item without a body, and the attributes in front of it.
_MODULE_DECLARATION = re.compile(
    r"(?P<attributes>(?:#\[[^\]]*\]\s*)*)(?:pub(?:\s*\([^)]*\))?\s+)?mod\s+"
    r"(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*;"
)
_PATH_ATTRIBUTE = re.compile(r"#\[\s*path\s*=\s*\"(?P<path>[^\"]*)\"\s*\]")
# Files whose out-of-line modules live beside them rather than in a directory named after them:
# module roots, and the crate roots Cargo finds in these directories.
_MODULE_ROOT_FILES = frozenset({"build.rs", "lib.rs", "main.rs", "mod.rs"})
_CRATE_ROOT_DIRECTORIES = frozenset({"bin", "benches", "examples", "tests"})
_CFG_PREDICATE = re.compile(r"(?<![A-Za-z0-9_])cfg(?:_attr)?\s*!?\s*\(")
_MODE_CFG_NAME = re.compile(r"(?<![A-Za-z0-9_])(?P<name>loom|shuttle|turmoil)(?![A-Za-z0-9_])")
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


@dataclass(frozen=True)
class Routed:
    """Where a governed path goes instead, and what reaching it directly does."""

    replacement: tuple[str, ...]
    verb: str

    def describe(self, path: Sequence[str]) -> str:
        return f"`{'::'.join(path)}` {self.verb}; use `{'::'.join(self.replacement)}`"


def route_of(path: Sequence[str]) -> Routed | None:
    """The path that replaces a governed `path`, or `None` when it is not governed.

    Atomic paths have rules of their own and are not answered here.
    """

    path = tuple(path)
    best: Route | None = None
    for route in ROUTES:
        if path[: len(route.prefix)] == route.prefix:
            if best is None or len(route.prefix) > len(best.prefix):
                best = route
    if best is not None:
        if best.exact:
            return Routed(best.replacement, best.verb)
        return Routed(best.replacement + path[len(best.prefix) :], best.verb)
    for module in SYNC_MODULES:
        if path[: len(module)] == module and len(path) > len(module):
            if path[len(module)] not in SYNC_UNGOVERNED:
                return Routed(BLOCKING + path[len(module) :], "bypasses the boundary")
    return None


def confined_item(path: Sequence[str]) -> tuple[str, ...] | None:
    """The confined boundary item `path` names, or `None`."""

    for item in CONFINED:
        if tuple(path[: len(item)]) == item:
            return item
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
    """Whether Loom model code may name `path`: Loom models it, or it is explicitly real."""

    path = tuple(path)
    if path in LOOM_MODELED_ITEMS or path[:3] in LOOM_REAL_ITEMS:
        return True
    if path[: len(UNMODELED_ROOT)] == UNMODELED_ROOT:
        return True
    return any(path[: len(module)] == module for module in LOOM_MODELED_MODULES)


def check_loom_models(file: RustFile, loom_only_file: bool = False) -> list[Site]:
    """Reject a family Loom does not model in a module compiled only for Loom.

    In a Loom build such a family is the ordinary library, which no model observes, so a model that
    named one would run a real primitive silently. What an owner the model drives uses internally
    is outside this check: a Loom claim excludes it. A module compiled only for Loom is an inline
    module whose attributes require the `loom` feature, or, when `loom_only_file` says so, the
    whole file, which a `mod` declaration compiled only for Loom brought in.
    """

    violations: list[Site] = []
    blocks: list[tuple[int, int]] = []
    if loom_only_file:
        blocks.append((0, len(file.code)))
    else:
        for module in _MODULE_WITH_BODY.finditer(file.literals):
            if not _loom_only(module.group("attributes")):
                continue
            brace = module.end() - 1
            blocks.append((brace, _end_of_block(file.code, brace)))
    for start, end in blocks:
        _check_loom_block(file, start, end, violations)
    return violations


def _check_loom_block(file: RustFile, start: int, end: int, violations: list[Site]) -> None:
    """Reject every family Loom does not model that the code between `start` and `end` names."""

    block = file.code[start:end]
    thread_aliases: set[str] = set()

    def reject(offset: int, path: Sequence[str]) -> None:
        violations.append(
            file.site(
                start + offset,
                f"{RULE}: Loom model code names `{'::'.join(path)}`, which a Loom build takes "
                "from the ordinary library, outside every model; a Loom model uses only "
                "atomics, the threads it spawns, joins, parks and yields, thread-local storage, "
                "shared ownership and permitted unmodeled primitives",
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


def check_source(file: RustFile, loom_only_file: bool = False) -> tuple[list[Site], FileUses]:
    """Return the file's boundary violations and the unmodeled items it uses. `loom_only_file` says
    a `mod` declaration compiled only for Loom brought the whole file in."""

    violations: list[Site] = []
    unmodeled = FileUses()
    names = AtomicNames()
    code = file.code
    use_spans: list[tuple[int, int]] = []
    sync_aliases: set[str] = set()
    # Names this file binds to a `sync` module or a partly governed module, and to a boundary module
    # holding a confined item, each with the module's path.
    module_aliases: dict[str, tuple[str, ...]] = {}
    confining_aliases: dict[str, tuple[str, ...]] = {}

    def confine(offset: int, item: tuple[str, ...]) -> None:
        owners, meaning = CONFINED[item]
        if file.path in owners:
            return
        violations.append(
            file.site(
                offset,
                f"{RULE}: `{'::'.join(item)}` is {meaning}; only "
                f"{', '.join(sorted(owners))} may name it",
            )
        )

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
            item = confined_item(path)
            if item is not None:
                confine(match.start(), item)
            for confined in CONFINED:
                parent = confined[:-1]
                if path == parent:
                    confining_aliases[leaf.alias or parent[-1]] = parent
                elif path == parent + ("*",):
                    confine(match.start(), confined)
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
                module_aliases[leaf.alias or "sync"] = path
                continue
            if path in PARTLY_GOVERNED_MODULES and leaf.alias is None:
                module_aliases[path[-1]] = path
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
            routed = route_of(path)
            if routed is not None:
                violations.append(file.site(match.start(), f"{RULE}: {routed.describe(path)}"))
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
        if alias is not None and name in GOVERNED_ROOTS - {"std", "core"}:
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
        item = confined_item(path)
        if item is not None:
            confine(match.start(), item)
            continue
        if root in confining_aliases:
            expanded = confining_aliases[root] + path[1:]
            item = confined_item(expanded)
            if item is not None:
                confine(match.start(), item)
                continue
        if root in module_aliases:
            module = module_aliases[root]
            routed = route_of(module + path[1:])
            if routed is not None:
                violations.append(
                    file.site(
                        match.start(),
                        f"{RULE}: `{'::'.join(path)}` reaches `{'::'.join(module + path[1:])}` "
                        f"through an imported `{'::'.join(module)}` module; "
                        f"use `{'::'.join(routed.replacement)}`",
                    )
                )
            continue
        if root not in GOVERNED_ROOTS:
            continue
        routed = route_of(path)
        if routed is None:
            continue
        violations.append(file.site(match.start(), f"{RULE}: {routed.describe(path)}"))
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
    violations.extend(check_loom_models(file, loom_only_file))
    violations.extend(check_mode_cfgs(file))
    return violations, unmodeled


def _end_of_group(code: str, start: int) -> int:
    """The offset just past the parenthesis that closes the one at `start`."""

    depth = 0
    for index in range(start, len(code)):
        if code[index] == "(":
            depth += 1
        elif code[index] == ")":
            depth -= 1
            if depth == 0:
                return index + 1
    return len(code)


def check_mode_cfgs(file: RustFile) -> list[Site]:
    """Reject an execution mode selected through a bare `cfg` rather than a feature.

    A cfg such as `loom` is global: `--cfg loom` reaches every crate of the build, and Tokio and
    other dependencies read the same name and change their own behavior. A mode is selected only
    through the primitive boundary's feature, which each package forwards, so a bare mode cfg in a
    predicate is a second, global selection path.
    """

    violations: list[Site] = []
    for match in _CFG_PREDICATE.finditer(file.code):
        opening = match.end() - 1
        predicate = file.code[opening : _end_of_group(file.code, opening)]
        for name in _MODE_CFG_NAME.finditer(predicate):
            mode = name.group("name")
            violations.append(
                file.site(
                    opening + name.start(),
                    f"{RULE}: `cfg({mode})` selects an execution mode through a global cfg that "
                    f"every crate of the build reads, Tokio's included; select it with "
                    f'`feature = "{mode}"`, which the package forwards to {OWNER}',
                )
            )
    return violations


def _module_directory(declaring: PurePosixPath) -> PurePosixPath:
    """The directory in which the modules a file declares out of line live."""

    if declaring.name in _MODULE_ROOT_FILES or declaring.parent.name in _CRATE_ROOT_DIRECTORIES:
        return declaring.parent
    return declaring.parent / declaring.stem


def declared_modules(file: RustFile, sources: frozenset[str]) -> Iterator[tuple[str, bool]]:
    """Each source file that `file` declares as an out-of-line module, and whether its declaration
    compiles it only for Loom. A declaration whose file is not among `sources` is skipped."""

    declaring = PurePosixPath(file.path)
    directory = _module_directory(declaring)
    for match in _MODULE_DECLARATION.finditer(file.literals):
        attributes = match.group("attributes")
        path_attribute = _PATH_ATTRIBUTE.search(attributes)
        if path_attribute is not None:
            candidates = [declaring.parent / path_attribute.group("path")]
        else:
            name = match.group("name")
            candidates = [directory / f"{name}.rs", directory / name / "mod.rs"]
        for candidate in candidates:
            normalized = posixpath.normpath(str(candidate))
            if normalized in sources:
                yield normalized, _loom_only(attributes)
                break


def loom_only_files(files: Mapping[str, RustFile]) -> frozenset[str]:
    """The files a `mod` declaration compiled only for Loom brings in, with every module they
    declare in turn."""

    sources = frozenset(files)
    declared = {path: list(declared_modules(file, sources)) for path, file in files.items()}
    pending: list[str] = []
    for children in declared.values():
        for child, only_for_loom in children:
            if only_for_loom:
                pending.append(child)
    loom_only: set[str] = set()
    while pending:
        path = pending.pop()
        if path in loom_only:
            continue
        loom_only.add(path)
        for child, _ in declared.get(path, ()):
            pending.append(child)
    return frozenset(loom_only)


@dataclass(frozen=True)
class Permission:
    path: str
    items: frozenset[str]
    owner: str
    reason: str
    limit: str


def _is_one_rust_file(path: str) -> bool:
    """Whether `path` names a single Rust file relative to the repository root."""

    if not path.endswith(".rs") or path.startswith("/") or any(char in path for char in "*?[]"):
        return False
    return ".." not in PurePosixPath(path).parts


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
        if not _is_one_rust_file(table["path"]):
            raise ValueError(
                f"{context} names `{table['path']}`; a permission covers one Rust file, never a "
                "directory, a glob or a path outside the repository"
            )
        for item in items:
            if tuple(item.split("::")) not in UNMODELED_ITEMS:
                raise ValueError(
                    f"{context} lists `{item}`, which is not an item of "
                    f"`{'::'.join(UNMODELED_ROOT)}`"
                )
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
    """Match every unmodeled use against a permission, and every permission against a use.

    `uses` holds every file the source rules read, so a permission for any other path, such as a
    deleted file, the boundary's own sources or guest code, permits nothing.
    """

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
        if used is None:
            problems.append(
                f"{PERMISSIONS}: stale permission: {permission.path} is not a Rust file the "
                "boundary governs"
            )
            continue
        used_items = set(used.items)
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
class Dependency:
    """One dependency entry of a manifest: the name the package's code uses, and the package."""

    key: str
    package: str
    kind: str
    entry: object


@dataclass(frozen=True)
class Package:
    name: str
    manifest: str
    # The package's directory relative to the repository root, with a trailing slash, or an empty
    # string for the root package.
    directory: str
    features: Mapping[str, Sequence[str]]
    normal: frozenset[str]
    every_kind: Mapping[str, Mapping[str, object]]
    dependencies: tuple[Dependency, ...]
    crate_types: frozenset[str]


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


def tracked_files(root: Path) -> list[str]:
    """Every tracked file, and every new file Git does not ignore."""

    completed = subprocess.run(
        ["git", "-C", str(root), "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
        capture_output=True,
        check=True,
        text=True,
    )
    return sorted(
        entry for entry in completed.stdout.split("\0") if entry and (root / entry).is_file()
    )


def load_packages(root: Path, files: Sequence[str]) -> list[Package]:
    """Every package a tracked or new manifest declares: the workspace members, including new ones,
    and the standalone packages that have a workspace of their own."""

    root_document = tomllib.loads((root / "Cargo.toml").read_text(encoding="utf-8"))
    root_dependencies = root_document.get("workspace", {}).get("dependencies", {})
    packages: list[Package] = []
    for manifest in files:
        manifest_path = PurePosixPath(manifest)
        if manifest_path.name != "Cargo.toml":
            continue
        document = tomllib.loads((root / manifest).read_text(encoding="utf-8"))
        package_table = document.get("package")
        if not isinstance(package_table, dict):
            continue
        # A standalone package inherits from the workspace its own manifest declares.
        own_workspace = document.get("workspace")
        if isinstance(own_workspace, dict) and manifest != "Cargo.toml":
            workspace_dependencies = own_workspace.get("dependencies", {})
        else:
            workspace_dependencies = root_dependencies
        normal: set[str] = set()
        every_kind: dict[str, dict[str, object]] = {}
        dependencies: list[Dependency] = []
        for kind, table in _dependency_tables(document):
            for key, entry in table.items():
                package = _package_of(key, entry, workspace_dependencies)
                every_kind.setdefault(package, {})[kind] = entry
                dependencies.append(Dependency(key=key, package=package, kind=kind, entry=entry))
                if kind == "dependencies":
                    normal.add(package)
        library = document.get("lib", {})
        crate_types = library.get("crate-type", []) if isinstance(library, dict) else []
        parent = str(manifest_path.parent)
        packages.append(
            Package(
                name=package_table["name"],
                manifest=manifest,
                directory="" if parent == "." else f"{parent}/",
                features=document.get("features", {}),
                normal=frozenset(normal),
                every_kind=every_kind,
                dependencies=tuple(dependencies),
                crate_types=frozenset(crate_types),
            )
        )
    return packages


def guest_directories(packages: Sequence[Package]) -> tuple[str, ...]:
    """The directories of guest code: the WASM guest SDK, and every guest library built on it."""

    directories: list[str] = []
    for package in packages:
        is_sdk = package.name == GUEST_SDK
        is_guest = GUEST_SDK in package.every_kind and "cdylib" in package.crate_types
        if (is_sdk or is_guest) and package.directory:
            directories.append(package.directory)
    return tuple(sorted(directories))


def _crate_name(package: str) -> str:
    return package.replace("-", "_")


# The packages whose paths the source rules read, by crate name: a manifest that renames one hides
# those paths behind a name the rules do not know.
GOVERNED_CRATES = (
    GOVERNED_ROOTS
    | {_crate_name(OWNER)}
    | {_crate_name(package) for package in OWNER_ONLY_PACKAGES}
) - {"std", "core", "alloc"}


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
            for dependency in package.dependencies:
                crate = _crate_name(dependency.package)
                if crate not in GOVERNED_CRATES or _crate_name(dependency.key) == crate:
                    continue
                problems.append(
                    f"{package.manifest}: {RULE}: the {dependency.kind} entry `{dependency.key}` "
                    f"renames `{dependency.package}`, which hides its governed paths from the "
                    "source rules; depend on it by its own name"
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
        turmoil = package.every_kind.get(TURMOIL)
        if turmoil is not None:
            enabled = package.features.get(TURMOIL, ())
            for kind, entry in turmoil.items():
                optional = isinstance(entry, dict) and entry.get("optional") is True
                if not optional or kind != "dependencies":
                    problems.append(
                        f"{package.manifest}: {RULE}: `turmoil` must be an optional dependencies "
                        f"entry, not a {kind} one, so no ordinary graph contains it"
                    )
                elif package.name != OWNER and f"dep:{TURMOIL}" not in enabled:
                    problems.append(
                        f"{package.manifest}: {RULE}: only {OWNER} selects Turmoil's network; a "
                        "package whose harness runs Turmoil enables it through its own `turmoil` "
                        "feature"
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


def _global_cfgs(text: str) -> Iterator[str]:
    """The name of every cfg `text` passes to a whole build, as a flag or from a build script."""

    for match in _GLOBAL_CFG.finditer(text):
        yield match.group("name") or match.group("emitted")


def check_global_cfgs(root: Path, files: Sequence[str]) -> list[str]:
    """Hold Tokio's unstable runtime controls to the Turmoil recipes, and every execution mode to
    its feature.

    A cfg a build passes reaches every crate in it. `tokio_unstable` changes how Tokio schedules
    and reports, so only a Turmoil recipe passes it. An execution mode is never a global cfg: Tokio
    and other dependencies read the same names and would change their own behavior, so `--cfg
    loom`, `--cfg shuttle` and `--cfg turmoil` fail in every recipe, configuration, workflow and
    build script.
    """

    problems: list[str] = []

    def reject_mode(location: str, mode: str) -> None:
        problems.append(
            f"{location}: {RULE}: `--cfg {mode}` selects an execution mode for every crate of a "
            f"build, Tokio's included; select it with the `{mode}` feature a package forwards to "
            f"{OWNER}"
        )

    justfile = root / JUSTFILE
    if justfile.is_file():
        recipe: str | None = None
        for number, line in enumerate(justfile.read_text(encoding="utf-8").splitlines(), start=1):
            header = _RECIPE.match(line)
            if header is not None:
                recipe = header.group("name")
            if line.lstrip().startswith("#"):
                continue
            for name in _global_cfgs(line):
                if name in MODES:
                    reject_mode(f"{JUSTFILE}:{number}", name)
                elif name == TOKIO_UNSTABLE and (recipe is None or TURMOIL not in recipe):
                    problems.append(
                        f"{JUSTFILE}:{number}: {RULE}: `--cfg tokio_unstable` changes how Tokio "
                        f"schedules and reports, so only a Turmoil recipe passes it, and "
                        f"`{recipe}` is not one"
                    )
    for pattern in CONFIGURATION_GLOBS:
        for path in sorted(root.glob(pattern)):
            if not path.is_file():
                continue
            relative = path.relative_to(root)
            for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
                if line.lstrip().startswith("#"):
                    continue
                for name in _global_cfgs(line):
                    if name in MODES:
                        reject_mode(f"{relative}:{number}", name)
                    elif name == TOKIO_UNSTABLE:
                        problems.append(
                            f"{relative}:{number}: {RULE}: `--cfg tokio_unstable` would change how "
                            "Tokio schedules and reports in every build this configures; only a "
                            "Turmoil recipe passes it"
                        )
    for path in files:
        if PurePosixPath(path).name != "build.rs":
            continue
        for name in sorted(set(_global_cfgs((root / path).read_text(encoding="utf-8")))):
            if name in MODES:
                reject_mode(path, name)
            elif name == TOKIO_UNSTABLE:
                problems.append(
                    f"{path}: {RULE}: a build script that sets `tokio_unstable` changes how Tokio "
                    "schedules and reports in every build; only a Turmoil recipe passes it"
                )
    return problems


def check(root: Path) -> list[str]:
    problems: list[str] = []
    files = tracked_files(root)
    packages = load_packages(root, files)
    guests = guest_directories(packages)
    governed: dict[str, RustFile] = {}
    for path in files:
        if not path.endswith(".rs") or path.startswith(OWNER_SOURCES) or path.startswith(guests):
            continue
        governed[path] = RustFile(path, (root / path).read_text(encoding="utf-8"))
    loom_only = loom_only_files(governed)
    uses: dict[str, FileUses] = {}
    for path, file in governed.items():
        violations, file_uses = check_source(file, loom_only_file=path in loom_only)
        problems.extend(site.render() for site in violations)
        uses[path] = file_uses
    permissions_file = root / PERMISSIONS
    try:
        permissions = parse_permissions(permissions_file.read_text(encoding="utf-8"))
    except (OSError, ValueError, tomllib.TOMLDecodeError) as error:
        return [*problems, f"{PERMISSIONS}: {RULE}: {error}"]
    problems.extend(check_permissions(uses, permissions))
    problems.extend(check_manifests(packages))
    problems.extend(check_global_cfgs(root, files))
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
