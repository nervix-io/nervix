"""The wheel as an application installs it: it loads with nothing else installed, and it carries
its types."""

from __future__ import annotations

import subprocess
from pathlib import Path

import nervix_client as nx
from support import WAIT_SECONDS

# Imports the package where nothing else is installed, and prints the Arrow libraries loading it
# brought in.
LOAD = """
import importlib.util
import sys

import nervix_client

for library in ("pyarrow", "polars"):
    if importlib.util.find_spec(library) is not None:
        raise SystemExit(f"the bare environment holds {library}")
print(sorted(name for name in sys.modules if name.split(".")[0] in ("pyarrow", "polars")))
"""


def test_the_package_loads_without_pyarrow_or_polars(bare_python: Path) -> None:
    loaded = subprocess.run(
        [str(bare_python), "-I", "-c", LOAD],
        capture_output=True,
        text=True,
        timeout=WAIT_SECONDS,
        check=False,
    )
    assert loaded.returncode == 0, loaded.stderr
    assert loaded.stdout.strip() == "[]"


def test_the_package_carries_its_stub_and_typed_marker() -> None:
    package = Path(nx.__file__).parent
    assert (package / "__init__.pyi").is_file()
    assert (package / "py.typed").is_file()
