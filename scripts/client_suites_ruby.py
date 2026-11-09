"""The magnus surface and the Ruby suite's parsed references.

Owns reading native Ruby registrations and comparing them to Prism's source references.
Depends on the shared Rust lexer, Ruby's bundled parser and the existing suite inventory.
Must not know how a cluster or a native Ruby extension is built or run.
"""

from __future__ import annotations

import ast
import json
import re
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

from scripts.rust_source import RustFile, _end_of_block, blank


@dataclass
class RubyAnalysis:
    surface: set[str] = field(default_factory=set)
    references: set[str] = field(default_factory=set)
    tests: dict[str, str] = field(default_factory=dict)
    parents: dict[str, str] = field(default_factory=dict)
    problems: list[str] = field(default_factory=list)


_STRING = r'"(?:[^"\\]|\\.)*"'
_FUNCTION = re.compile(r"\bfn\s+(?P<name>\w+)\s*(?:<[^{};]*>)?\s*\(")
_ASSIGNMENT = re.compile(
    r"\blet\s+(?:mut\s+)?(?P<variable>\w+)(?:\s*:\s*[\w:]+)?\s*=\s*"
    r"(?:(?P<helper>crate::class)\s*\(|(?P<receiver>\w+)\s*\.\s*"
    r"(?P<operation>define_class|define_module|define_error|const_get)\s*\()"
)
_REGISTRATION = re.compile(
    r"\b(?P<receiver>\w+)\s*\.\s*(?P<operation>define_method|define_singleton_method|"
    r"define_attr|const_set)\s*\("
)
_ENUM = re.compile(r"\bhost_enum!\s*\{\s*(?P<name>\w+)\s+from\s+\w+\s*\{")


def _literal(rust: RustFile, offset: int) -> tuple[str, int] | None:
    """Read a quoted registration name from the existing lexer's comment-free view."""

    match = re.match(r"\s*(" + _STRING + ")", rust.literals[offset:])
    if match is None:
        return None
    value = ast.literal_eval(match.group(1))
    return value, offset + match.end()


def _definitions(rust: RustFile, module: str, analysis: RubyAnalysis) -> None:
    code = rust.code
    # Declarative enum invocations own their complete constant set. The macro definition is
    # syntax, not a second runtime registration whose metavariables are public Ruby names.
    definitions = []
    for macro in re.finditer(r"\bmacro_rules!\s+\w+\s*\{", code):
        definitions.append((macro.start(), _end_of_block(code, macro.end() - 1)))
    ordinary = blank(code, definitions)
    for match in _ENUM.finditer(ordinary):
        name = f"{module}::{match.group('name')}"
        analysis.surface.add(name)
        end = _end_of_block(code, match.end() - 1)
        body = rust.literals[match.end():end]
        for constant in re.finditer(r"\b\w+\s*=\s*\d+\s+as\s*(" + _STRING + ")", body):
            analysis.surface.add(f"{name}::{ast.literal_eval(constant.group(1))}")
    for function in _FUNCTION.finditer(ordinary):
        # The crate's class helper is a declaration template, like the enum macro above.
        # Its literal call sites are read by _ASSIGNMENT; the parameter is not another class.
        if Path(rust.path).name == "lib.rs" and function.group("name") == "class":
            continue
        opening = ordinary.find("{", function.end())
        if opening < 0:
            continue
        end = _end_of_block(ordinary, opening)
        owners = {"module": module, "client": module}
        events = list(_ASSIGNMENT.finditer(ordinary, opening, end))
        events.extend(_REGISTRATION.finditer(ordinary, opening, end))
        for match in sorted(events, key=lambda item: item.start()):
            if match.re is _ASSIGNMENT:
                if match.group("helper"):
                    arguments = rust.literals[match.end():]
                    name_match = re.match(r"\s*\w+\s*,\s*\w+\s*,\s*(" + _STRING + ")", arguments)
                    if name_match is None:
                        analysis.problems.append(f"{rust.path}:{rust.line_of(match.start())}: Ruby class has no literal name")
                        continue
                    name = ast.literal_eval(name_match.group(1))
                    owner = f"{module}::{name}"
                else:
                    literal = _literal(rust, match.end())
                    if literal is None:
                        if match.group("operation") != "const_get":
                            analysis.problems.append(f"{rust.path}:{rust.line_of(match.start())}: Ruby declaration has no literal name")
                        continue
                    name, _ = literal
                    receiver = match.group("receiver")
                    if receiver == "ruby":
                        owner = name
                    else:
                        parent = owners.get(receiver)
                        if parent is None:
                            analysis.problems.append(f"{rust.path}:{rust.line_of(match.start())}: Ruby declaration has no resolved parent")
                            continue
                        owner = f"{parent}::{name}"
                    if match.group("operation") == "define_error":
                        analysis.parents[owner] = f"{module}::ClientError"
                owners[match.group("variable")] = owner
                if match.group("helper") or match.group("operation") != "const_get":
                    analysis.surface.add(owner)
                continue
            literal = _literal(rust, match.end())
            if literal is None:
                analysis.problems.append(f"{rust.path}:{rust.line_of(match.start())}: Ruby registration has no literal name")
                continue
            name, after = literal
            receiver = match.group("receiver")
            if receiver not in owners:
                analysis.problems.append(f"{rust.path}:{rust.line_of(match.start())}: Ruby registration has no resolved class")
                continue
            owner = owners[receiver]
            operation = match.group("operation")
            if operation == "const_set":
                analysis.surface.add(f"{owner}::{name}")
            elif operation == "define_singleton_method":
                analysis.surface.add(f"{owner}.{name}")
            elif operation == "define_method":
                analysis.surface.add(f"{owner}#{name}")
            else:
                mode = re.match(r"\s*,\s*Attr::(\w+)", ordinary[after:])
                if mode is None:
                    analysis.problems.append(f"{rust.path}:{rust.line_of(match.start())}: Ruby attribute has no declared access")
                    continue
                if mode.group(1) in {"Read", "ReadWrite"}:
                    analysis.surface.add(f"{owner}#{name}")
                if mode.group(1) in {"Write", "ReadWrite"}:
                    analysis.surface.add(f"{owner}#{name}=")
        # Exception names are declared once in the registration loop's array.
        body = ordinary[opening:end]
        for loop in re.finditer(r"\bfor\s+(?P<variable>\w+)\s+in\s*\[", body):
            start = opening + loop.end()
            close = ordinary.find("]", start, end)
            if close < 0:
                continue
            following = ordinary[close:end]
            pattern = r"\b\w+\.define_error\(\s*" + re.escape(loop.group("variable")) + r"\s*,"
            if re.search(pattern, following) is None:
                continue
            for literal in re.finditer(_STRING, rust.literals[start:close]):
                name = f"{module}::{ast.literal_eval(literal.group())}"
                analysis.surface.add(name)
                analysis.parents[name] = f"{module}::ClientError"


def read_surface(root: Path, sources: str, module: str) -> RubyAnalysis:
    analysis = RubyAnalysis(surface={module, module.partition("::")[0]})
    for path in sorted((root / sources).rglob("*.rs")):
        if "tests" in path.parts or path.name == "tests.rs":
            continue
        rust = RustFile(str(path.relative_to(root)), path.read_text())
        _definitions(rust, module, analysis)
    if len(analysis.surface) < 3:
        analysis.problems.append("Ruby source analysis discovered no native registrations")
    return analysis


def analyze(root: Path, sources: str, suite: str, module: str) -> RubyAnalysis:
    analysis = read_surface(root, sources, module)
    process = subprocess.run(
        ["ruby", str(Path(__file__).with_suffix(".rb")), str(root / suite), module],
        capture_output=True, text=True, check=False,
    )
    if process.returncode:
        analysis.problems.append(f"Ruby suite source analysis failed:\n{process.stderr}{process.stdout}")
        return analysis
    decoded = json.loads(process.stdout)
    analysis.problems.extend(decoded["problems"])
    analysis.tests = decoded["tests"]
    for reference in decoded["references"]:
        if "#" in reference:
            owner, member = reference.split("#", 1)
            seen = set()
            while reference not in analysis.surface and owner in analysis.parents and owner not in seen:
                seen.add(owner)
                owner = analysis.parents[owner]
                reference = f"{owner}#{member}"
        analysis.references.add(reference)
    if not analysis.tests:
        analysis.problems.append("Ruby suite declares no minitest tests")
    return analysis
