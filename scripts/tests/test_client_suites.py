"""The client suite check over a fixture binding: its surface read from pyo3 source, the members a
typed suite references, the stub, the inventory, the guide and a tier's results."""

from __future__ import annotations

import contextlib
import io
import tempfile
import textwrap
import unittest
from pathlib import Path

from scripts import client_suites

SOURCE = '''
use pyo3::{create_exception, exceptions::PyException, prelude::*};

/// A reading.
#[pyclass(frozen, eq, module = "fixture")]
#[derive(PartialEq)]
pub(crate) struct Gauge {
    value: u32,
}

#[pymethods]
impl Gauge {
    #[new]
    #[pyo3(signature = (value, *, scale = 1))]
    fn new(value: u32, scale: u32) -> Self {
        Self { value: value * scale }
    }

    /// The reading; a `{` in a comment or "}" in a string never ends the block.
    #[getter]
    fn value(&self) -> u32 {
        let _text = "}";
        self.value
    }

    #[getter(r#type)]
    fn kind(&self) -> u32 {
        0
    }

    fn reset(&self, py: Python<'_>, to: u32) -> PyResult<()> {
        drop(py);
        drop(to);
        Ok(())
    }

    #[pyo3(name = "describe")]
    fn render(&self) -> String {
        format!("Gauge({})", self.value)
    }

    fn __repr__(&self) -> String {
        format!("Gauge({})", self.value)
    }
}

/// Another class with a member of the same name.
#[pyclass(frozen, module = "fixture")]
pub(crate) struct Meter {
    value: u32,
}

#[pymethods]
impl Meter {
    #[getter]
    fn value(&self) -> u32 {
        self.value
    }

    #[staticmethod]
    fn zero() -> Meter {
        Meter { value: 0 }
    }
}

macro_rules! host_enum {
    ($name:ident from $host:ident { $($variant:ident = $value:literal as $python:literal,)+ }) => {
        #[pyclass(eq, eq_int, frozen, hash, module = "fixture")]
        pub(crate) enum $name { $(#[pyo3(name = $python)] $variant = $value,)+ }
    };
}

host_enum! {
    Mode from Mode {
        Fast = 1 as "FAST",
        Slow = 2 as "SLOW",
    }
}

create_exception!(fixture, FixtureError, PyException, "Every failure.");
create_exception!(fixture, BrokenError, FixtureError, "A broken one.");

fn annotate(value: &Bound<'_, PyAny>) -> PyResult<()> {
    value.setattr("kind", 1)?;
    Ok(())
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = module.py();
    module.add("FixtureError", py.get_type::<FixtureError>())?;
    module.add("BrokenError", py.get_type::<BrokenError>())?;
    module.add_class::<Gauge>()?;
    module.add_class::<Meter>()?;
    module.add_class::<Mode>()?;
    module.add("EMPTY", Meter { value: 0 })?;
    Ok(())
}
'''

STUB = '''
from typing import ClassVar, Final, final

class FixtureError(Exception):
    kind: int

class BrokenError(FixtureError): ...

@final
class Mode:
    FAST: ClassVar[Mode]
    SLOW: ClassVar[Mode]
    def __int__(self) -> int: ...
    def __eq__(self, other: object) -> bool: ...
    def __hash__(self) -> int: ...

@final
class Gauge:
    def __init__(self, value: int, *, scale: int = 1) -> None: ...
    @property
    def value(self) -> int: ...
    @property
    def type(self) -> int: ...
    def reset(self, to: int) -> None: ...
    def describe(self) -> str: ...
    def __repr__(self) -> str: ...
    def __eq__(self, other: object) -> bool: ...

@final
class Meter:
    @property
    def value(self) -> int: ...
    @staticmethod
    def zero() -> Meter: ...

EMPTY: Final[Meter]
'''

SUITE = '''
import fixture as fx


def test_every_member() -> None:
    gauge = fx.Gauge(1, scale=2)
    assert gauge.value + gauge.type == 2
    gauge.reset(0)
    print(gauge.describe(), repr(gauge), fx.Meter.zero().value, fx.EMPTY)
    assert fx.Mode.FAST != fx.Mode.SLOW
    try:
        raise fx.BrokenError()
    except fx.FixtureError as failure:
        print(failure.kind)
'''

GUIDE_SECTIONS = "\n".join(f"## {section}\n\nProse.\n" for section in client_suites.GUIDE_SECTIONS)

MYPY_CONFIG = """
[mypy]
strict = True
explicit_package_bases = True
"""


class Fixture:
    """A repository holding one fixture host: its source, stub, suite, guide and inventory."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.host = client_suites.Host(
            name="fixture",
            module="fixture",
            sources="crates/fixture/src",
            stub="crates/fixture/fixture.pyi",
            suite="tests/fixture/suite",
            guide="docs/src/client-library-fixture.md",
        )
        self.write("crates/fixture/src/lib.rs", SOURCE)
        self.write("crates/fixture/fixture.pyi", STUB)
        self.write("tests/fixture/suite/mypy.ini", MYPY_CONFIG)
        self.write("tests/fixture/suite/boundary/test_members.py", SUITE)
        anchored = "# ANCHOR: example\nprint('example')\n# ANCHOR_END: example\n"
        self.write("tests/fixture/suite/cluster/test_example.py", f"def test_example() -> None:\n    {anchored.replace(chr(10), chr(10) + '    ')}\n")
        include = "```python\n{{#include ../../tests/fixture/suite/cluster/test_example.py:example}}\n```\n"
        self.write("docs/src/client-library-fixture.md", GUIDE_SECTIONS.replace("## Install\n\nProse.\n", f"## Install\n\n{include}", 1))
        self.register(
            ("boundary", "boundary/test_members.py::test_every_member"),
            ("cluster", "cluster/test_example.py::test_example"),
        )

    def write(self, relative: str, text: str) -> None:
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(text))

    def register(self, *tests: tuple[str, str]) -> None:
        entries = [
            f'[[test]]\nhost = "fixture"\ntier = "{tier}"\nidentity = "{identity}"\n'
            for tier, identity in tests
        ]
        self.write(client_suites.INVENTORY, "\n".join(entries))

    def surface(self) -> client_suites.Surface:
        return client_suites.read_surface(self.root, self.host)


class ClientSuitesTest(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.fixture = Fixture(Path(directory.name))

    def test_the_surface_reads_members_parameters_enumerations_exceptions_and_constants(self) -> None:
        surface = self.fixture.surface()
        gauge = surface.classes["Gauge"]
        self.assertEqual(set(gauge.members), {"__init__", "value", "type", "reset", "describe", "__repr__"})
        self.assertEqual(gauge.option_dunders(), {"__eq__"})
        constructor = gauge.members["__init__"].parameters
        self.assertEqual(
            constructor,
            (
                client_suites.Parameter("value", False, False),
                client_suites.Parameter("scale", True, True),
            ),
        )
        self.assertEqual(gauge.members["reset"].parameters, (client_suites.Parameter("to", False, False),))
        self.assertEqual(surface.classes["Meter"].members["zero"].kind, "staticmethod")
        mode = surface.classes["Mode"]
        self.assertEqual((mode.kind, set(mode.members)), ("enum", {"FAST", "SLOW"}))
        self.assertEqual(mode.option_dunders(), {"__eq__", "__hash__", "__int__"})
        self.assertEqual(surface.classes["FixtureError"].base, "Exception")
        self.assertEqual(set(surface.classes["FixtureError"].members), {"kind"})
        self.assertEqual(surface.classes["BrokenError"].base, "FixtureError")
        self.assertEqual(surface.constants, {"EMPTY"})

    def test_a_matching_stub_passes_and_a_drifting_stub_is_named(self) -> None:
        stub = self.fixture.root / self.fixture.host.stub
        self.assertEqual(client_suites.check_stub(self.fixture.surface(), stub), [])
        drifted = STUB.replace("def reset(self, to: int)", "def reset(self, target: int)").replace(
            "    def describe(self) -> str: ...\n", "    def extra(self) -> str: ...\n"
        )
        self.fixture.write(self.fixture.host.stub, drifted.replace("EMPTY: Final[Meter]\n", ""))
        problems = client_suites.check_stub(self.fixture.surface(), stub)
        self.assertIn("fixture.pyi does not declare EMPTY, which the binding exports", problems)
        self.assertIn("fixture.pyi does not declare Gauge.describe", problems)
        self.assertIn("fixture.pyi declares Gauge.extra, which the binding lacks", problems)
        self.assertIn("Gauge.reset takes (to) in the binding but (target) in the stub", problems)

    def test_an_unreferenced_member_is_named_and_a_member_reached_on_another_class_does_not_count(
        self,
    ) -> None:
        surface = self.fixture.surface()
        self.assertEqual(client_suites.check_references(self.fixture.root, self.fixture.host, surface), [])
        partial = SUITE.replace("    gauge.reset(0)\n", "").replace(
            "gauge.value + gauge.type", "fx.Meter.zero().value + gauge.type"
        )
        self.fixture.write("tests/fixture/suite/boundary/test_members.py", partial)
        problems = client_suites.check_references(self.fixture.root, self.fixture.host, surface)
        self.assertEqual(
            problems,
            [
                "no fixture suite test reads or calls fixture.Gauge.reset",
                "no fixture suite test reads or calls fixture.Gauge.value",
            ],
        )

    def test_a_suite_mypy_refuses_is_reported_with_its_errors(self) -> None:
        self.fixture.write("tests/fixture/suite/boundary/test_members.py", SUITE + "\nwrong: int = 'text'\n")
        problems = client_suites.check_references(
            self.fixture.root, self.fixture.host, self.fixture.surface()
        )
        self.assertTrue(problems[0].startswith("mypy --strict failed over the fixture suite"))
        self.assertIn("Incompatible types in assignment", problems[0])

    def test_the_inventory_is_held_to_the_suite(self) -> None:
        registered = client_suites.read_inventory(self.fixture.root)["fixture"]
        self.assertEqual(client_suites.check_inventory(self.fixture.root, self.fixture.host, registered), [])
        self.fixture.register(
            ("cluster", "boundary/test_members.py::test_every_member"),
            ("cluster", "cluster/test_example.py::test_gone"),
        )
        registered = client_suites.read_inventory(self.fixture.root)["fixture"]
        problems = client_suites.check_inventory(self.fixture.root, self.fixture.host, registered)
        self.assertEqual(
            problems,
            [
                f"{client_suites.INVENTORY} does not register fixture test cluster/test_example.py::test_example",
                f"{client_suites.INVENTORY} registers fixture test cluster/test_example.py::test_gone, which the suite lacks",
                f"{client_suites.INVENTORY} registers boundary/test_members.py::test_every_member as cluster, but it is boundary",
            ],
        )
        self.fixture.write(
            "tests/fixture/suite/cluster/test_example.py",
            "import pytest\n\n@pytest.mark.parametrize('n', [1])\ndef test_example(n: int) -> None:\n    pass\n",
        )
        _, unstable = client_suites.discover_tests(self.fixture.root, self.fixture.host)
        self.assertEqual(
            unstable,
            ["cluster/test_example.py::test_example is parametrized; every suite test is one stable identity"],
        )

    def test_a_guide_missing_a_section_or_naming_an_absent_anchor_is_named(self) -> None:
        guide = self.fixture.host.guide
        self.assertEqual(client_suites.check_guide(self.fixture.root, self.fixture.host), [])
        text = (self.fixture.root / guide).read_text()
        broken = (
            text.replace("## Errors\n\nProse.\n", "")
            .replace(":example}}", ":missing}}")
            .replace("## Connect\n\nProse.\n", "## Connect\n\n```python\nprint('not included')\n```\n")
        )
        self.fixture.write(guide, broken)
        problems = client_suites.check_guide(self.fixture.root, self.fixture.host)
        self.assertEqual(len(problems), 3, problems)
        self.assertIn("not ", problems[0])
        self.assertTrue(problems[1].endswith("has no anchor missing"), problems[1])
        self.assertTrue(problems[2].endswith("a code block is not one include of a suite test"), problems[2])

    def test_an_include_outside_the_suite_is_refused(self) -> None:
        guide = self.fixture.host.guide
        text = (self.fixture.root / guide).read_text()
        self.fixture.write("docs/src/elsewhere.py", "# ANCHOR: example\n# ANCHOR_END: example\n")
        self.fixture.write(guide, text.replace("../../tests/fixture/suite/cluster/test_example.py", "elsewhere.py"))
        problems = client_suites.check_guide(self.fixture.root, self.fixture.host)
        self.assertEqual(len(problems), 1)
        self.assertTrue(problems[0].endswith("elsewhere.py is not a source of the fixture suite"), problems[0])

    def test_a_run_is_held_to_the_registered_tests_of_its_tier(self) -> None:
        registered = {"a.py::test_one": "boundary", "a.py::test_two": "boundary", "b.py::test_three": "cluster"}
        passed = ["NERVIX-SUITE passed a.py::test_one", "progress line", "NERVIX-SUITE passed a.py::test_two"]
        self.assertEqual(client_suites.check_run(registered, "boundary", passed), [])
        lines = [
            "NERVIX-SUITE failed a.py::test_one",
            "NERVIX-SUITE skipped b.py::test_three",
        ]
        self.assertEqual(
            client_suites.check_run(registered, "boundary", lines),
            [
                "registered boundary test a.py::test_two did not run",
                "a.py::test_one reported failed",
                "b.py::test_three ran but is not a registered boundary test",
            ],
        )
        self.assertEqual(
            client_suites.check_run(registered, "boundary", ["NERVIX-SUITE vanished a.py::test_one"]),
            ["unreadable result line: NERVIX-SUITE vanished a.py::test_one"],
        )

    def test_the_command_line_fails_on_a_problem_and_passes_without_one(self) -> None:
        results = self.fixture.root / "results.txt"
        results.write_text("NERVIX-SUITE passed boundary/test_members.py::test_every_member\n")
        command = ["--root", str(self.fixture.root), "check-run", "--host", "python", "--tier", "boundary"]
        reported = io.StringIO()
        with contextlib.redirect_stderr(reported):
            self.assertEqual(client_suites.main(command + [str(results)]), 1)
        self.assertIn("is not a registered boundary test", reported.getvalue())
        self.fixture.write(
            client_suites.INVENTORY,
            '[[test]]\nhost = "python"\ntier = "boundary"\nidentity = "boundary/test_members.py::test_every_member"\n',
        )
        self.assertEqual(client_suites.main(command + [str(results)]), 0)


if __name__ == "__main__":
    unittest.main()
