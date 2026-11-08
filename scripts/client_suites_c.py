"""Compiler-owned syntax for the C and C++ suites of the shared Nervix binding.

Owns resolving header declarations, actual suite references and the named test table through
Clang's JSON syntax tree. Depends on the checked header, suite sources and a native Clang.
Must not know what a host test asserts, how it reaches a cluster or whether its run passed.
No object code is emitted and no suite or binding is loaded by this source inspection.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator


@dataclass
class Inspection:
    surface: set[str] = field(default_factory=set)
    references: set[str] = field(default_factory=set)
    tests: dict[str, str] = field(default_factory=dict)
    problems: list[str] = field(default_factory=list)

    @property
    def missing(self) -> set[str]:
        return self.surface - self.references


@dataclass(frozen=True)
class NativeSuite:
    root: Path
    host: str

    @property
    def directory(self) -> Path:
        return self.root / "tests/client_conformance" / self.host / "suite"

    @property
    def header(self) -> Path:
        return self.root / "crates/client-ffi/include/nervix_client.h"

    @property
    def source(self) -> Path:
        suffix = "cpp" if self.host == "cpp" else "c"
        return self.directory / f"main.{suffix}"

    @staticmethod
    def nodes(node: dict) -> Iterator[dict]:
        yield node
        for child in node.get("inner", []):
            yield from NativeSuite.nodes(child)

    def syntax(self, path: Path, *, header: bool = False) -> dict:
        cpp = self.host == "cpp" and not header
        name = "clang++" if cpp else "clang"
        # CI installs LLVM 23 with versioned driver names; local toolchains may provide the
        # ordinary driver name instead. Both emit the compiler-resolved tree of this source.
        compiler = shutil.which(f"{name}-23") or shutil.which(name)
        if compiler is None:
            raise ValueError("Clang is required for the C/C++ suite surface reader")
        arguments = [compiler, "-x", "c++" if cpp else "c", "-std=c++17" if cpp else "-std=c11",
                     "-pthread", "-I", str(self.header.parent), "-fsyntax-only",
                     "-Xclang", "-ast-dump=json"]
        if not header:
            arguments.extend(["-include", str(self.header)])
        arguments.append(str(path))
        completed = subprocess.run(
            arguments,
            cwd=self.root, capture_output=True, text=True, check=False, timeout=120,
        )
        if completed.returncode:
            raise ValueError(f"{self.host} suite compiler refused {path}:\n{completed.stderr}")
        try:
            return json.loads(completed.stdout)
        except json.JSONDecodeError as error:
            raise ValueError(f"{self.host} suite compiler returned unreadable syntax: {error}") from error

    @staticmethod
    def declared_surface(tree: dict) -> set[str]:
        surface = set()
        for node in NativeSuite.nodes(tree):
            name = node.get("name", "")
            kind = node.get("kind")
            if kind in {"TypedefDecl", "FunctionDecl"} and name.startswith("nx_"):
                surface.add(name)
            elif kind == "EnumConstantDecl" and name.startswith("NX_"):
                surface.add(name)
        return surface

    @staticmethod
    def function_definition(node: dict) -> bool:
        return node.get("kind") == "FunctionDecl" and any(
            child.get("kind") == "CompoundStmt" for child in node.get("inner", [])
        )

    def inspect(self) -> Inspection:
        report = Inspection()
        try:
            header = self.syntax(self.header, header=True)
            report.surface = self.declared_surface(header)
            types = {node.get("name") for node in self.nodes(header)
                     if node.get("kind") == "TypedefDecl"}
            if not report.surface:
                raise ValueError("the C header declares no binding surface")
            tree = self.syntax(self.source)
        except (ValueError, OSError, subprocess.TimeoutExpired) as error:
            report.problems.append(str(error))
            return report

        # The forced include precedes all suite declarations. Its include guard then prevents the
        # source's ordinary include from declaring the API twice. The first declaration of each
        # header member is consequently the actual API, even if a suite uses another namespace.
        # References carry declaration identities, so a local same-name pointer or enumerator does
        # not satisfy a function/constant. Types below are Clang-resolved declaration types, never
        # words found in comments, string literals or unparsed source.
        declarations = {}
        definitions = {}
        table = None
        for node in self.nodes(tree):
            name = node.get("name", "")
            if name in report.surface and node.get("kind") in {
                "FunctionDecl", "TypedefDecl", "EnumConstantDecl"
            }:
                declarations.setdefault(name, node["id"])
            if self.function_definition(node):
                if name.startswith("nx_") and node.get("mangledName", name) == name:
                    report.problems.append(f"a suite defines binding function {name}")
                if name.startswith("test_"):
                    definitions[name] = node
            if node.get("kind") == "VarDecl" and name == "suite_tests":
                if table is not None:
                    report.problems.append("the suite declares its test table twice")
                table = node

        # Only definitions in the suite reference the binding. Merely including the header never
        # makes a declaration count as a test reference. Helpers and RAII deleters are definitions
        # too, and their resolved calls count in both languages.
        for node in self.nodes(tree):
            if self.function_definition(node):
                self.references(node, declarations, types, report)
        self.test_table(table, definitions, report)
        return report

    def references(self, function: dict, declarations: dict[str, str], types: set[str],
                   report: Inspection) -> None:
        def visit(node: dict, aliases: set[str]) -> None:
            if node.get("kind") == "DeclRefExpr":
                referred = node.get("referencedDecl", {})
                name = referred.get("name")
                if name in declarations and referred.get("id") == declarations[name]:
                    report.references.add(name)
            if node.get("kind") in {"VarDecl", "ParmVarDecl", "TypedefDecl", "TypeAliasDecl"}:
                declared = node.get("type", {}).get("qualType", "")
                for name in re.findall(r"(?<![\w:])(?:::)?(nx_[A-Za-z0-9_]+)\b", declared):
                    if name in types and name not in aliases:
                        report.references.add(name)
            scoped = set(aliases)
            for child in node.get("inner", []):
                declared = [child]
                if child.get("kind") == "DeclStmt":
                    declared = child.get("inner", [])
                for declaration in declared:
                    if declaration.get("kind") not in {"TypedefDecl", "TypeAliasDecl"}:
                        continue
                    name = declaration.get("name")
                    if name in report.surface:
                        scoped.add(name)
                visit(child, scoped)
        visit(function, set())

    def test_table(self, table: dict | None, definitions: dict[str, dict], report: Inspection) -> None:
        if table is None:
            report.problems.append(f"the {self.host} suite has no suite_tests table")
            return
        entries = []
        initializers = [child for child in table.get("inner", [])
                        if child.get("kind") == "InitListExpr"]
        for node in initializers[0].get("inner", []) if initializers else []:
            if node.get("kind") != "InitListExpr":
                report.problems.append("the suite's test table has an unreadable entry")
                continue
            strings = []
            functions = []
            for child in self.nodes(node):
                if child.get("kind") == "StringLiteral":
                    try:
                        strings.append(json.loads(child["value"]))
                    except json.JSONDecodeError:
                        report.problems.append("the test table requires plain UTF-8 tier and identity strings")
                        return
                if child.get("kind") == "DeclRefExpr":
                    referred = child.get("referencedDecl", {})
                    if referred.get("kind") == "FunctionDecl":
                        functions.append(referred.get("name"))
            if len(strings) == 2 and len(functions) == 1:
                entries.append((strings, functions[0]))
        if not entries:
            report.problems.append(f"the {self.host} suite's test table is empty or unreadable")
        for strings, function in entries:
            tier, identity = strings
            if identity != function:
                report.problems.append(f"test table identity {identity} names function {function}")
            if identity in report.tests:
                report.problems.append(f"the suite registers {identity} twice")
            if tier not in {"boundary", "cluster"}:
                report.problems.append(f"{identity} has unknown tier {tier}")
            if function not in definitions:
                report.problems.append(f"the suite lacks a definition of {function}")
            report.tests[identity] = tier
        for identity in sorted(set(definitions) - set(report.tests)):
            report.problems.append(f"suite function {identity} is not in its test table")
