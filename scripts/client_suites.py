"""The language-level client suites: each host binding held to the suite that uses it.

Owns reading a binding's public surface from its source, the members a suite references, the
inventory of suite tests, the binding's typed stub, the shape of its guide chapter, and judging
one tier's results against the inventory. Depends on the binding sources, the suite sources, the
guide chapters, the inventory and mypy. Must not know how a suite reaches a cluster; the scenario
harness starts the cluster tier and holds its run to the same inventory.

`validate` holds, for every host: every public member of the binding to a reference in the suite,
measured by a mypy plugin over the suite's typed sources, so a member counts only when a test reads
or calls it on a value of its own class; the inventory to the tests the suite defines; the typed
stub to the binding's source, member by member and keyword by keyword; and the guide to the
section list every guide shares, with every code block an include of an anchored suite test.
`check-run` holds the result lines of one tier's run to the inventory.
"""

from __future__ import annotations

import argparse
import ast
import configparser
import os
import re
import subprocess
import sys
import tempfile
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

from scripts.rust_source import RustFile, _end_of_block

# The prefix of every line a suite's reporter prints for one test: `NERVIX-SUITE <outcome> <id>`.
RESULT_PREFIX = "NERVIX-SUITE "

# The outcomes a reporter prints. Only `passed` satisfies the inventory.
OUTCOMES = frozenset({"passed", "failed", "skipped"})

# The tiers a suite test belongs to, which are also the directories it lives in.
TIERS = ("boundary", "cluster")

# The sections every guide chapter has, in this order.
GUIDE_SECTIONS = (
    "Install",
    "Connect",
    "Execute Plexus",
    "Complete A Statement",
    "Subscribe And Read Rows",
    "Follow A Domain Clock",
    "Publish Through A Client Ingestor",
    "Consume A Client Emitter",
    "Back Up And Restore",
    "Errors",
    "Threads, Cancellation And Deadlines",
    "Lifetimes And Memory",
    "Arrow Interop",
    "What The Library Does Not Expose",
    "A Complete Application",
)

INVENTORY = "tests/client-suites-inventory.toml"

PLUGIN = Path(__file__).with_name("client_suites_mypy_plugin.py")


@dataclass(frozen=True)
class Host:
    """Where one host's binding, stub, suite and guide live."""

    name: str
    module: str
    sources: str
    stub: str
    suite: str
    guide: str
    # Further sources the suite's type check covers, whose references do not count.
    typed: tuple[str, ...] = ()


HOSTS = {
    "java": Host(
        name="java",
        module="io.nervix.client",
        sources="crates/client-jvm/java/io/nervix/client/Nervix.java",
        stub="",
        suite="tests/client_conformance/java/suite",
        guide="docs/src/client-library-jvm.md",
    ),
    "python": Host(
        name="python",
        module="nervix_client",
        sources="crates/client-python/src",
        stub="crates/client-python/nervix_client.pyi",
        suite="tests/client_conformance/python/suite",
        guide="docs/src/client-library-python.md",
        typed=("examples/paced-simulation/python/paced_simulation.py",),
    ),
}


class Problems:
    """Every finding of one validation, each naming what has to change."""

    def __init__(self) -> None:
        self.found: list[str] = []

    def add(self, message: str) -> None:
        self.found.append(message)

    def extend(self, messages: list[str]) -> None:
        self.found.extend(messages)


# ---- The binding's public surface --------------------------------------------------------------


@dataclass(frozen=True)
class Parameter:
    name: str
    keyword_only: bool
    has_default: bool


@dataclass
class Member:
    """One Python-visible member of a class: a method, a property, a constructor or an
    enumeration member, with the parameters a caller passes when it is called."""

    name: str
    kind: str
    parameters: tuple[Parameter, ...] | None = None


@dataclass
class PyClass:
    name: str
    kind: str
    options: frozenset[str] = frozenset()
    members: dict[str, Member] = field(default_factory=dict)
    base: str | None = None

    def option_dunders(self) -> set[str]:
        """The members a class has because of how it is declared rather than what it defines."""

        dunders = set()
        if "eq" in self.options:
            dunders.add("__eq__")
        if "hash" in self.options:
            dunders.add("__hash__")
        if self.kind == "enum":
            dunders.add("__int__")
        return dunders


@dataclass
class Surface:
    classes: dict[str, PyClass] = field(default_factory=dict)
    constants: set[str] = field(default_factory=set)

    def names(self) -> set[str]:
        return set(self.classes) | self.constants

    def referenceable(self) -> set[str]:
        """Every `Class.member` a suite has to reference. A buffer's release is the runtime's
        counterpart of the export a suite references, and the members a declaration implies are
        behaviours a suite asserts rather than names it can read."""

        members = set()
        for py_class in self.classes.values():
            for member in py_class.members:
                if member == "__release_buffer__":
                    continue
                members.add(f"{py_class.name}.{member}")
        return members


# Rust names of dunder methods whose Python name differs: the buffer protocol's slots.
PYTHON_DUNDERS = {"__getbuffer__": "__buffer__", "__releasebuffer__": "__release_buffer__"}

_PYCLASS = re.compile(r"#\[pyclass(?:\((?P<options>[^\]]*)\))?\]")
_ITEM = re.compile(r"\b(?P<kind>struct|enum)\s+(?P<name>\w+)")
_PYMETHODS = re.compile(r"#\[pymethods\]\s*impl\s+(?P<name>\w+)\s*\{")
_FN = re.compile(r"\bfn\s+(?P<name>\w+)\s*(?:<[^>]*>)?\s*\(")
_ATTRIBUTE = re.compile(r"#\[(?P<body>[^\]]*(?:\[[^\]]*\][^\]]*)*)\]")
_HOST_ENUM = re.compile(r"host_enum!\s*\{")
_HOST_ENUM_HEAD = re.compile(r"(?P<name>\w+)\s+from\s+\w+\s*\{")
_HOST_ENUM_VARIANT = re.compile(r"\w+\s*=\s*\d+\s+as\s+\"(?P<python>[A-Za-z0-9_]+)\"")
_ENUM_VARIANT_NAME = re.compile(r"#\[pyo3\(name\s*=\s*\"(?P<python>[A-Za-z0-9_]+)\"\)\]")
_ADD_CLASS = re.compile(r"add_class::<(?P<name>\w+)>")
_ADD_TYPE = re.compile(r"\.add\(\s*\"(?P<python>\w+)\",\s*py\.get_type::<(?P<name>\w+)>\(\)")
_ADD_VALUE = re.compile(r"\.add\(\s*\"(?P<python>\w+)\",(?!\s*py\.get_type)")
_EXCEPTION = re.compile(
    r"create_exception!\(\s*\w+,\s*(?P<name>\w+),\s*(?P<base>\w+),", re.MULTILINE
)
_SETATTR = re.compile(r"\.setattr\(\s*\"(?P<name>\w+)\"")
_SIGNATURE = re.compile(r"pyo3\s*\(\s*signature\s*=\s*\((?P<parameters>.*)\)\s*\)\s*$", re.S)
_NAME = re.compile(r"pyo3\s*\(\s*name\s*=\s*\"(?P<name>\w+)\"\s*\)")
_GETTER = re.compile(r"^getter(?:\((?:r#)?(?P<name>\w+)\))?$")


def split_top_level(text: str) -> list[str]:
    """`text` split at the commas outside every bracket."""

    parts: list[str] = []
    depth = 0
    current: list[str] = []
    for char in text:
        if char in "([{<":
            depth += 1
        elif char in ")]}>":
            depth -= 1
        if char == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
            continue
        current.append(char)
    tail = "".join(current).strip()
    if tail:
        parts.append(tail)
    return parts


def signature_parameters(text: str) -> tuple[Parameter, ...]:
    """The parameters a `#[pyo3(signature = (...))]` attribute lists."""

    parameters = []
    keyword_only = False
    for part in split_top_level(text):
        if part == "*":
            keyword_only = True
            continue
        name, _, default = part.partition("=")
        parameters.append(Parameter(name.strip(), keyword_only, bool(default.strip())))
    return tuple(parameters)


def receiver(parameter: str) -> bool:
    """Whether a Rust parameter is the receiver, the GIL token or another argument no caller
    passes."""

    name = parameter.split(":", 1)[0].strip()
    if name.startswith("mut "):
        name = name[4:].strip()
    return name in {"self", "&self", "&mut self", "slf", "py", "_py"} or name.startswith("&")


def fn_parameters(code: str, open_paren: int) -> tuple[Parameter, ...]:
    """The parameters of the Rust function whose parameter list opens at `open_paren`."""

    depth = 0
    index = open_paren
    while index < len(code):
        if code[index] == "(":
            depth += 1
        elif code[index] == ")":
            depth -= 1
            if depth == 0:
                break
        index += 1
    parameters = []
    for part in split_top_level(code[open_paren + 1 : index]):
        if receiver(part):
            continue
        name = part.split(":", 1)[0].strip().lstrip("_")
        parameters.append(Parameter(name, False, False))
    return tuple(parameters)


def attributes_before(code: str, literals: str, start: int, floor: int) -> list[str]:
    """The bodies of the attributes written directly before the item at `start`, after `floor`."""

    region = literals[floor:start]
    code_region = code[floor:start]
    bodies = []
    for match in _ATTRIBUTE.finditer(code_region):
        bodies.append(" ".join(region[match.start("body") : match.end("body")].split()))
    return bodies


def method_member(attributes: list[str], name: str, parameters: tuple[Parameter, ...]) -> Member:
    """The Python member a `#[pymethods]` function declares."""

    kind = "method"
    python = name
    for body in attributes:
        getter = _GETTER.match(body)
        if getter is not None:
            kind = "property"
            python = getter.group("name") or name
        elif body == "new":
            kind = "constructor"
            python = "__init__"
        elif body == "staticmethod":
            kind = "staticmethod"
        renamed = _NAME.match(body)
        if renamed is not None:
            python = renamed.group("name")
        signature = _SIGNATURE.match(body)
        if signature is not None:
            parameters = signature_parameters(signature.group("parameters"))
    python = PYTHON_DUNDERS.get(python, python)
    if kind == "property":
        return Member(python, kind)
    return Member(python, kind, parameters)


def read_methods(rust: RustFile, surface: dict[str, list[Member]]) -> None:
    code, literals = rust.product, rust.literals
    for block in _PYMETHODS.finditer(code):
        open_index = block.end() - 1
        end = _end_of_block(code, open_index)
        members = surface.setdefault(block.group("name"), [])
        floor = open_index + 1
        depth = 0
        index = open_index + 1
        while index < end - 1:
            char = code[index]
            if char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    floor = index + 1
            elif depth == 0:
                function = _FN.match(code, index)
                if function is not None and (index == 0 or not code[index - 1].isalnum()):
                    attributes = attributes_before(code, literals, index, floor)
                    parameters = fn_parameters(code, function.end() - 1)
                    members.append(method_member(attributes, function.group("name"), parameters))
                    index = function.end()
                    continue
            index += 1


def read_classes(rust: RustFile, classes: dict[str, PyClass]) -> None:
    code, literals = rust.product, rust.literals
    for match in _PYCLASS.finditer(code):
        if "macro_rules!" in code[max(0, match.start() - 400) : match.start()]:
            continue
        options_text = literals[match.start("options") : match.end("options")] if match.group(
            "options"
        ) else ""
        options = frozenset(
            option.split("=", 1)[0].strip() for option in split_top_level(options_text)
        )
        item = _ITEM.search(code, match.end())
        if item is None:
            continue
        py_class = PyClass(item.group("name"), "class", options)
        if item.group("kind") == "enum":
            py_class.kind = "enum"
            body_start = code.index("{", item.end())
            body = literals[body_start : _end_of_block(code, body_start)]
            for variant in _ENUM_VARIANT_NAME.finditer(body):
                py_class.members[variant.group("python")] = Member(variant.group("python"), "member")
        classes[py_class.name] = py_class


def host_enum_options(rust: RustFile) -> frozenset[str]:
    definition = rust.product.find("macro_rules! host_enum")
    if definition < 0:
        return frozenset()
    match = _PYCLASS.search(rust.product, definition)
    if match is None or not match.group("options"):
        return frozenset()
    options_text = rust.literals[match.start("options") : match.end("options")]
    return frozenset(option.split("=", 1)[0].strip() for option in split_top_level(options_text))


def read_host_enums(rust: RustFile, classes: dict[str, PyClass]) -> None:
    options = host_enum_options(rust)
    code, literals = rust.product, rust.literals
    for match in _HOST_ENUM.finditer(code):
        end = _end_of_block(code, match.end() - 1)
        body = literals[match.end() : end]
        head = _HOST_ENUM_HEAD.search(body)
        if head is None:
            continue
        py_class = PyClass(head.group("name"), "enum", options)
        for variant in _HOST_ENUM_VARIANT.finditer(body):
            python = variant.group("python")
            py_class.members[python] = Member(python, "member")
        classes[py_class.name] = py_class


def read_registrations(rust: RustFile, registered: set[str], exceptions: dict[str, str],
                       constants: set[str], attributes: set[str]) -> None:
    literals = rust.literals
    for match in _ADD_CLASS.finditer(rust.product):
        registered.add(match.group("name"))
    for match in _ADD_TYPE.finditer(literals):
        exceptions[match.group("python")] = match.group("name")
    for match in _ADD_VALUE.finditer(literals):
        constants.add(match.group("python"))
    for match in _SETATTR.finditer(literals):
        attributes.add(match.group("name"))


def read_surface(root: Path, host: Host) -> Surface:
    """The public surface a binding's source declares: every class it registers with its members,
    every exception class and every module constant."""

    classes: dict[str, PyClass] = {}
    methods: dict[str, list[Member]] = {}
    registered: set[str] = set()
    exception_names: dict[str, str] = {}
    constants: set[str] = set()
    attributes: set[str] = set()
    bases: dict[str, str] = {}
    for path in sorted((root / host.sources).glob("*.rs")):
        if path.name == "tests.rs":
            continue
        rust = RustFile(path.relative_to(root).as_posix(), path.read_text())
        read_classes(rust, classes)
        read_host_enums(rust, classes)
        read_methods(rust, methods)
        read_registrations(rust, registered, exception_names, constants, attributes)
        for match in _EXCEPTION.finditer(rust.literals):
            bases[match.group("name")] = match.group("base")
    surface = Surface()
    for name in sorted(registered):
        py_class = classes.get(name, PyClass(name, "class"))
        for member in methods.get(name, []):
            py_class.members[member.name] = member
        surface.classes[name] = py_class
    for python, name in exception_names.items():
        base = bases.get(name, "Exception")
        exception = PyClass(python, "exception", base="Exception" if base == "PyException" else base)
        if exception.base == "Exception":
            for attribute in sorted(attributes):
                exception.members[attribute] = Member(attribute, "attribute")
        surface.classes[python] = exception
    surface.constants = constants
    return surface


# ---- The typed stub ----------------------------------------------------------------------------


def stub_parameters(function: ast.FunctionDef) -> tuple[Parameter, ...]:
    arguments = function.args
    positional = arguments.posonlyargs + arguments.args
    defaults = [False] * (len(positional) - len(arguments.defaults)) + [True] * len(
        arguments.defaults
    )
    parameters = []
    for argument, has_default in zip(positional, defaults):
        if argument.arg in {"self", "cls"}:
            continue
        parameters.append(Parameter(argument.arg, False, has_default))
    for argument, default in zip(arguments.kwonlyargs, arguments.kw_defaults):
        parameters.append(Parameter(argument.arg, True, default is not None))
    return tuple(parameters)


@dataclass
class StubClass:
    name: str
    bases: list[str]
    members: dict[str, tuple[Parameter, ...] | None]


def read_stub(path: Path) -> tuple[dict[str, StubClass], set[str]]:
    """The public classes and module constants a stub declares, with each class's members and the
    parameters of each of its functions."""

    tree = ast.parse(path.read_text(), str(path))
    classes: dict[str, StubClass] = {}
    constants: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and not node.name.startswith("_"):
            members: dict[str, tuple[Parameter, ...] | None] = {}
            for item in node.body:
                if isinstance(item, ast.FunctionDef):
                    decorators = {ast.unparse(decorator) for decorator in item.decorator_list}
                    members[item.name] = None if "property" in decorators else stub_parameters(item)
                elif isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name):
                    members[item.target.id] = None
            bases = [ast.unparse(base) for base in node.bases]
            classes[node.name] = StubClass(node.name, bases, members)
        elif (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and not node.target.id.startswith("_")
        ):
            constants.add(node.target.id)
    return classes, constants


def compare_parameters(where: str, source: tuple[Parameter, ...],
                       stub: tuple[Parameter, ...]) -> list[str]:
    if source == stub:
        return []
    return [
        f"{where} takes ({render_parameters(source)}) in the binding but "
        f"({render_parameters(stub)}) in the stub"
    ]


def render_parameters(parameters: tuple[Parameter, ...]) -> str:
    rendered = []
    keyword_only = False
    for parameter in parameters:
        if parameter.keyword_only and not keyword_only:
            rendered.append("*")
            keyword_only = True
        rendered.append(f"{parameter.name}=..." if parameter.has_default else parameter.name)
    return ", ".join(rendered)


# Protocol slots whose Python signature the runtime fixes, whatever the Rust function takes.
SLOTS = frozenset({"__buffer__", "__release_buffer__"})


def check_stub(surface: Surface, stub: Path) -> list[str]:
    """Holds the stub to the binding's surface: the same classes and constants, each class with
    exactly the members its source and declaration give it, and every function with the same
    parameters, keyword boundary and defaults."""

    if not stub.is_file():
        return [f"{stub} is missing; the binding ships its typed stub"]
    classes, constants = read_stub(stub)
    problems = []
    for missing in sorted(surface.names() - set(classes) - constants):
        problems.append(f"{stub.name} does not declare {missing}, which the binding exports")
    for extra in sorted((set(classes) | constants) - surface.names()):
        problems.append(f"{stub.name} declares {extra}, which the binding does not export")
    for name, py_class in sorted(surface.classes.items()):
        declared = classes.get(name)
        if declared is None:
            continue
        expected = set(py_class.members) | py_class.option_dunders()
        for missing in sorted(expected - set(declared.members)):
            problems.append(f"{stub.name} does not declare {name}.{missing}")
        for extra in sorted(set(declared.members) - expected):
            problems.append(f"{stub.name} declares {name}.{extra}, which the binding lacks")
        if py_class.kind == "exception" and py_class.base not in declared.bases:
            problems.append(f"{stub.name} does not derive {name} from {py_class.base}")
        for member in py_class.members.values():
            stub_parameters_of = declared.members.get(member.name)
            if member.parameters is None or stub_parameters_of is None or member.name in SLOTS:
                continue
            problems.extend(
                compare_parameters(f"{name}.{member.name}", member.parameters, stub_parameters_of)
            )
    return problems


# ---- The suite and its inventory --------------------------------------------------------------


def discover_tests(root: Path, host: Host) -> tuple[dict[str, str], list[str]]:
    """Every test the suite defines, by its pytest node id relative to the suite, with its tier,
    and the problems that make an identity unstable."""

    suite = root / host.suite
    tests: dict[str, str] = {}
    problems = []
    for path in sorted(suite.rglob("test_*.py")):
        relative = path.relative_to(suite).as_posix()
        tier = relative.split("/", 1)[0]
        if tier not in TIERS:
            problems.append(f"{host.suite}/{relative} is outside the boundary and cluster tiers")
            continue
        tree = ast.parse(path.read_text(), str(path))
        for node in tree.body:
            if isinstance(node, ast.FunctionDef) and node.name.startswith("test_"):
                for decorator in node.decorator_list:
                    if "parametrize" in ast.unparse(decorator):
                        problems.append(
                            f"{relative}::{node.name} is parametrized; every suite test is one "
                            "stable identity"
                        )
                tests[f"{relative}::{node.name}"] = tier
    return tests, problems


def read_inventory(root: Path) -> dict[str, dict[str, str]]:
    """The registered tests of every host: identity to tier."""

    path = root / INVENTORY
    data = tomllib.loads(path.read_text())
    registered: dict[str, dict[str, str]] = {}
    for entry in data.get("test", []):
        tests = registered.setdefault(entry["host"], {})
        if entry["identity"] in tests:
            raise ValueError(f"{INVENTORY} registers {entry['identity']} twice")
        tests[entry["identity"]] = entry["tier"]
    return registered


def check_inventory(root: Path, host: Host, registered: dict[str, str]) -> list[str]:
    tests, problems = discover_tests(root, host)
    return problems + compare_inventory(host, registered, tests)


def compare_inventory(host: Host, registered: dict[str, str], tests: dict[str, str]) -> list[str]:
    """Hold discovered stable identities and their tiers to the shared inventory."""

    problems = []
    for missing in sorted(set(tests) - set(registered)):
        problems.append(f"{INVENTORY} does not register {host.name} test {missing}")
    for extra in sorted(set(registered) - set(tests)):
        problems.append(f"{INVENTORY} registers {host.name} test {extra}, which the suite lacks")
    for identity, tier in sorted(registered.items()):
        if identity in tests and tests[identity] != tier:
            problems.append(f"{INVENTORY} registers {identity} as {tier}, but it is {tests[identity]}")
    return problems


def check_run(registered: dict[str, str], tier: str, lines: list[str]) -> list[str]:
    """Holds one tier's result lines to the inventory: every registered test of the tier ran once
    and passed, nothing was skipped, and nothing ran that the inventory does not register."""

    results: dict[str, list[str]] = {}
    for line in lines:
        if not line.startswith(RESULT_PREFIX):
            continue
        outcome, _, identity = line[len(RESULT_PREFIX) :].strip().partition(" ")
        if outcome not in OUTCOMES or not identity:
            return [f"unreadable result line: {line.strip()}"]
        results.setdefault(identity, []).append(outcome)
    expected = {identity for identity, of in registered.items() if of == tier}
    problems = []
    for identity in sorted(expected - set(results)):
        problems.append(f"registered {tier} test {identity} did not run")
    for identity, outcomes in sorted(results.items()):
        if identity not in expected:
            problems.append(f"{identity} ran but is not a registered {tier} test")
        elif outcomes != ["passed"]:
            problems.append(f"{identity} reported {', '.join(outcomes)}")
    return problems


# ---- Typed references --------------------------------------------------------------------------


def suite_names(root: Path, host: Host) -> set[str]:
    """Every module-level name of the binding the suite's sources name: `nx.Name`, a name imported
    from the module, or that name used bare."""

    names: set[str] = set()
    for path in sorted((root / host.suite).rglob("*.py")):
        tree = ast.parse(path.read_text(), str(path))
        aliases = {host.module}
        imported: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == host.module:
                        aliases.add(alias.asname or alias.name)
            elif isinstance(node, ast.ImportFrom) and node.module == host.module:
                for alias in node.names:
                    imported.add(alias.name)
        names |= imported
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Attribute)
                and isinstance(node.value, ast.Name)
                and node.value.id in aliases
            ):
                names.add(node.attr)
    return names


def mypy_config(root: Path, host: Host, directory: Path) -> Path:
    """The suite's own mypy configuration, with the reference plugin added."""

    parser = configparser.ConfigParser()
    parser.read(root / host.suite / "mypy.ini")
    if not parser.has_section("mypy"):
        parser.add_section("mypy")
    parser.set("mypy", "plugins", str(PLUGIN))
    parser.set("mypy", "mypy_path", f"{(root / host.stub).parent}:{root / host.suite}")
    parser.set("mypy", "cache_dir", os.devnull)
    parser.set("mypy", "incremental", "False")
    config = directory / "mypy.ini"
    with config.open("w") as handle:
        parser.write(handle)
    return config


def typed_references(root: Path, host: Host) -> tuple[set[str], list[str]]:
    """Every `Class.member` of the binding the suite's sources read or call, as mypy types them,
    and the type errors mypy reports over the suite and the other typed sources."""

    with tempfile.TemporaryDirectory() as directory:
        references = Path(directory) / "references.tsv"
        config = mypy_config(root, host, Path(directory))
        files = [str(root / host.suite)] + [str(root / typed) for typed in host.typed]
        environment = dict(os.environ, NERVIX_CLIENT_SUITE_REFERENCES=str(references),
                           NERVIX_CLIENT_SUITE_MODULE=host.module)
        completed = subprocess.run(
            [sys.executable, "-m", "mypy", "--config-file", str(config), *files],
            cwd=root,
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )
        problems = []
        if completed.returncode != 0:
            problems.append(
                f"mypy --strict failed over the {host.name} suite and its typed sources:\n"
                f"{completed.stdout}{completed.stderr}"
            )
        suite = (root / host.suite).resolve()
        recorded: set[str] = set()
        if references.is_file():
            for line in references.read_text().splitlines():
                path, _, member = line.partition("\t")
                if path and Path(path).resolve().is_relative_to(suite):
                    recorded.add(member)
        return recorded, problems


def check_references(root: Path, host: Host, surface: Surface) -> list[str]:
    """Holds the suite to the whole surface: every class is named or has a member a test reaches,
    every constant is named, and every member is read or called on a value of its own class."""

    references, problems = typed_references(root, host)
    named = suite_names(root, host)
    reached = {reference.split(".", 1)[0] for reference in references}
    for name in sorted(surface.names() - named - reached):
        problems.append(f"the {host.name} suite never names {host.module}.{name}")
    for member in sorted(surface.referenceable() - references):
        problems.append(f"no {host.name} suite test reads or calls {host.module}.{member}")
    return problems


# ---- Guides ------------------------------------------------------------------------------------

_HEADING = re.compile(r"^## (?P<title>.+?)\s*$", re.MULTILINE)
_FENCE = re.compile(r"^[ \t]*```[^\n]*\n(?P<body>.*?)^[ \t]*```[ \t]*$", re.MULTILINE | re.DOTALL)
_INCLUDE = re.compile(r"\{\{#include\s+(?P<path>[^:}\s]+)(?::(?P<anchor>[\w-]+))?\s*\}\}")


def check_guide(root: Path, host: Host) -> list[str]:
    """Holds a guide to the sections every guide has, in order, with every code block one include
    of an anchored test that exists in the suite."""

    path = root / host.guide
    if not path.is_file():
        return [f"{host.guide} is missing; every host has a guide chapter"]
    text = path.read_text()
    problems = []
    sections = tuple(match.group("title") for match in _HEADING.finditer(text))
    if sections != GUIDE_SECTIONS:
        problems.append(
            f"{host.guide} has the sections {list(sections)}, not {list(GUIDE_SECTIONS)} in order"
        )
    suite = (root / host.suite).resolve()
    for block in _FENCE.finditer(text):
        lines = [line for line in block.group("body").splitlines() if line.strip()]
        include = _INCLUDE.fullmatch(lines[0].strip()) if len(lines) == 1 else None
        line = text.count("\n", 0, block.start()) + 1
        if include is None:
            problems.append(f"{host.guide}:{line}: a code block is not one include of a suite test")
            continue
        problems.extend(check_include(path, suite, include, line, host))
    return problems


def check_include(guide: Path, suite: Path, include: re.Match[str], line: int,
                  host: Host) -> list[str]:
    target = (guide.parent / include.group("path")).resolve()
    where = f"{host.guide}:{line}"
    if not target.is_relative_to(suite):
        return [f"{where}: {include.group('path')} is not a source of the {host.name} suite"]
    if not target.is_file():
        return [f"{where}: {include.group('path')} does not exist"]
    anchor = include.group("anchor")
    if anchor is None:
        return [f"{where}: an include names no anchor of {include.group('path')}"]
    source = target.read_text()
    if f"ANCHOR: {anchor}" not in source or f"ANCHOR_END: {anchor}" not in source:
        return [f"{where}: {include.group('path')} has no anchor {anchor}"]
    return []


# ---- Commands ----------------------------------------------------------------------------------


@dataclass
class JavaAnalysis:
    """Declared members and resolved suite references from the Java compiler's source model."""

    surface: set[str] = field(default_factory=set)
    references: set[str] = field(default_factory=set)
    tests: dict[str, str] = field(default_factory=dict)
    problems: list[str] = field(default_factory=list)


def analyze_java(root: Path, host: Host) -> JavaAnalysis:
    """Read the prepared Java analyzer; a validation never builds or runs the native library."""

    artifacts = Path(os.environ.get("NERVIX_CLIENT_CONFORMANCE_DIR", root / "target/client-conformance"))
    checker = Path(os.environ.get("NERVIX_CLIENT_SUITE_JAVA_CHECKER", root / "target/client-suites/java"))
    junit = artifacts / "java/junit-platform-console-standalone.jar"
    result = subprocess.run(
        ["java", "-cp", str(checker), "SurfaceCheck", str(root / host.sources),
         str(root / host.suite), str(junit)],
        capture_output=True, text=True, check=False,
    )
    analysis = JavaAnalysis()
    if result.returncode:
        analysis.problems.append(f"JVM suite source analysis failed:\n{result.stderr}{result.stdout}")
        return analysis
    for line in result.stdout.splitlines():
        kind, value = line.split("\t", 1)
        if kind == "SURFACE":
            analysis.surface.add(value)
        elif kind == "REFERENCE":
            analysis.references.add(value)
        elif kind == "TEST":
            tier, identity = value.split("\t", 1)
            if identity in analysis.tests:
                analysis.problems.append(f"JVM suite defines {identity} twice")
            analysis.tests[identity] = tier
        elif kind == "PROBLEM":
            analysis.problems.append(value)
        else:
            raise ValueError(f"unknown JVM suite analysis result: {line}")
    if not analysis.surface:
        analysis.problems.append("JVM source analysis discovered no public binding members")
    if not analysis.tests:
        analysis.problems.append("JVM suite declares no JUnit tests")
    return analysis


def validate(root: Path, hosts: list[Host]) -> list[str]:
    problems = Problems()
    registered = read_inventory(root)
    for host in hosts:
        if host.name == "java":
            analysis = analyze_java(root, host)
            problems.extend(analysis.problems)
            for missing in sorted(analysis.surface - analysis.references):
                problems.add(f"{host.suite} does not reference JVM member {missing}")
            problems.extend(compare_inventory(host, registered.get(host.name, {}), analysis.tests))
            problems.extend(check_guide(root, host))
            continue
        surface = read_surface(root, host)
        problems.extend(check_stub(surface, root / host.stub))
        problems.extend(check_inventory(root, host, registered.get(host.name, {})))
        problems.extend(check_guide(root, host))
        problems.extend(check_references(root, host, surface))
    return problems.found


def main(arguments: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--root", type=Path, default=Path.cwd())
    commands = parser.add_subparsers(dest="command", required=True)
    validating = commands.add_parser("validate", help="hold every host's suite to its binding")
    validating.add_argument("--host", action="append", choices=sorted(HOSTS))
    running = commands.add_parser("check-run", help="hold one tier's results to the inventory")
    running.add_argument("--host", required=True, choices=sorted(HOSTS))
    running.add_argument("--tier", required=True, choices=TIERS)
    running.add_argument("results", type=Path)
    options = parser.parse_args(arguments)
    root = options.root.resolve()
    if options.command == "validate":
        hosts = [HOSTS[name] for name in options.host or sorted(HOSTS)]
        problems = validate(root, hosts)
    else:
        registered = read_inventory(root).get(options.host, {})
        problems = check_run(registered, options.tier, options.results.read_text().splitlines())
    for problem in problems:
        print(problem, file=sys.stderr)
    if problems:
        print(f"{len(problems)} client suite problem(s)", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
