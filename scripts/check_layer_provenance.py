#!/usr/bin/env python3
"""Check language import/dependency provenance before cfg selection.

The compiler owns resolved semantic references. This complementary check owns the crate path in
authored imports (including inactive code and macro tokens) and every manifest dependency table.
Architectural owners come from source contracts and module declarations, never filename lists.
Plain vocabulary or primitive re-exports have no language provenance and remain usable.
"""

from __future__ import annotations

import posixpath
import re
import tomllib
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from scripts.check_primitive_boundary import (UseTreeError, _module_directory, binary_roots,
                                              load_packages, tracked_files, use_leaves)
from scripts.rust_source import RustFile, _end_of_block, _end_of_item


PERMITTED = frozenset({"language", "edge", "harness"})
CONTRACT = re.compile(
    r'#\s*(?P<inner>!)?\s*\[\s*(?:cfg_attr\s*\(\s*nervix_lint\s*,\s*)?'
    r'nervix\s*::\s*layer\s*\(\s*(?P<kind>[a-z_]+)\s*,\s*reason\s*=\s*'
    r'"(?P<reason>(?:[^"\\]|\\.)+)"\s*,?\s*\)\s*\)?\s*\]', re.S,
)
MODULE = re.compile(
    r'(?P<attributes>(?:#\s*\[[^\]]*\]\s*)*)'
    r'(?:pub(?:\s*\([^)]*\))?\s+)?mod\s+(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*(?P<end>[;{])'
)
PATH = re.compile(r'#\s*\[\s*path\s*=\s*"(?P<path>[^"]+)"\s*\]')


@dataclass(frozen=True)
class Owner:
    start: int
    end: int
    kind: str


def owners(file: RustFile) -> list[Owner]:
    scopes: list[Owner] = []
    for match in CONTRACT.finditer(file.literals):
        if file.code[match.start()] != "#" or not match.group("reason").strip():
            continue
        start = match.start()
        if match.group("inner"):
            # Inner attributes govern their containing module, or the crate root.
            blocks = [index for index, token in enumerate(file.code[:start])
                      if token == "{" and _end_of_block(file.code, index) > start]
            opening = blocks[-1] if blocks else None
            end = _end_of_block(file.code, opening) if opening is not None else len(file.code)
            start = opening if opening is not None else 0
        else:
            end = _end_of_item(file.code, start)
        scopes.append(Owner(start, end, match.group("kind")))
    return scopes


def owner_at(scopes: list[Owner], offset: int, inherited: str | None) -> str | None:
    containing = [scope for scope in scopes if scope.start <= offset < scope.end]
    return min(containing, key=lambda scope: scope.end - scope.start).kind if containing else inherited


def module_children(file: RustFile, paths: frozenset[str]) -> list[tuple[str, int]]:
    """Include inactive and #[path] modules, with inline module directories preserved."""
    declaring = PurePosixPath(file.path)
    directory = _module_directory(declaring)
    inline: list[tuple[int, int, PurePosixPath]] = []
    children: list[tuple[str, int]] = []
    for match in MODULE.finditer(file.literals):
        if file.code[match.end() - 1] not in ";{":
            continue
        containing = [scope for scope in inline if scope[0] <= match.start() < scope[1]]
        current = containing[-1][2] if containing else directory
        attribute = PATH.search(match.group("attributes"))
        # An explicit out-of-line path is relative to its source file; inside an inline
        # module it is relative to that module's directory, including path overrides.
        target = ((containing[-1][2] if containing else declaring.parent) / attribute.group("path")
                  if attribute else current / match.group("name"))
        if match.group("end") == "{":
            inline.append((match.end() - 1, _end_of_block(file.code, match.end() - 1), target))
            continue
        candidates = [target] if attribute else [target.with_suffix(".rs"), target / "mod.rs"]
        for candidate in candidates:
            path = posixpath.normpath(str(candidate))
            if path in paths:
                children.append((path, match.start()))
                break
    return children


def check(root: Path, paths: list[str] | None = None) -> list[str]:
    paths = tracked_files(root) if paths is None else paths
    sources = {path: RustFile(path, (root / path).read_text()) for path in paths if path.endswith(".rs")}
    packages = load_packages(root, paths)
    scopes = {path: owners(file) for path, file in sources.items()}
    roots: dict[str, list[str]] = {}
    for package in packages:
        document = tomllib.loads((root / package.manifest).read_text())
        library = package.directory + document.get("lib", {}).get("path", "src/lib.rs")
        entries = [path for path in [library, *binary_roots(package, frozenset(sources)).values()]
                   if path in sources]
        # A package of tests alone is built from nothing else, so its test targets are its roots.
        if not entries:
            declared = (target.get("path", "") for target in document.get("test", []))
            entries = [package.directory + path for path in declared if package.directory + path in sources]
        roots[package.name] = entries
    language = {name for name, entries in roots.items()
                if any(owner_at(scopes[path], 0, None) == "language" for path in entries)}
    problems: set[str] = set()
    for package in packages:
        dependencies = {dependency.key.replace("-", "_"): dependency for dependency in package.dependencies
                        if dependency.package in language}
        if not dependencies:
            continue
        pending = [(path, None) for path in roots[package.name]]
        reached: set[tuple[str, str | None]] = set()
        approved = False
        while pending:
            path, inherited = pending.pop()
            if (path, inherited) in reached:
                continue
            reached.add((path, inherited))
            file = sources[path]
            own = scopes[path]
            approved |= owner_at(own, 0, inherited) in PERMITTED or any(scope.kind in PERMITTED for scope in own)
            for child, offset in module_children(file, frozenset(sources)):
                pending.append((child, owner_at(own, offset, inherited)))
            imported: dict[str, set[int]] = {alias: set() for alias in dependencies}
            for item in re.finditer(r'\buse\s+([^;]*);', file.code):
                try:
                    leaves = use_leaves(item[1])
                except (UseTreeError, IndexError):
                    # Unexpanded token substitutions are not concrete import paths. Qualified
                    # authored paths below remain visible; active expansions belong to rustc.
                    continue
                for leaf in leaves:
                    if leaf.path and leaf.path[0] in imported:
                        imported[leaf.path[0]].add(item.start())
            for alias in dependencies:
                # Capturing imports avoids treating literals, variable names or external crate
                # re-exports of plain primitives as semantic violations.
                pattern = re.compile(rf'(?<![\w$]){re.escape(alias)}\s*::|\b(?:use|extern\s+crate)\s+{re.escape(alias)}\b')
                offsets = imported[alias] | {match.start() for match in pattern.finditer(file.code)}
                for offset in offsets:
                    if owner_at(own, offset, inherited) not in PERMITTED:
                        problems.add(f"{path}:{file.line_of(offset)}: language import provenance `{alias}` requires a source-owned language, edge or harness layer")
        if not approved:
            for alias, dependency in dependencies.items():
                problems.add(f"{package.manifest}: language dependency `{dependency.key}` has no source-owned language, edge or harness consumer")
    return sorted(problems)


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    problems = check(root)
    for problem in problems:
        print(problem)
    return int(bool(problems))


if __name__ == "__main__":
    raise SystemExit(main())
