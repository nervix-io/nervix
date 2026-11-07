"""Fixtures of the boundary tier, which needs no cluster."""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import pytest

from support import SilentListener


@pytest.fixture
def silent_listener() -> Iterator[SilentListener]:
    """A listener that accepts connections and never answers them."""

    listener = SilentListener()
    yield listener
    listener.close()


@pytest.fixture(scope="session")
def bare_python() -> Path:
    """An interpreter whose environment holds the package and nothing else, which
    `just build-client-python` builds."""

    interpreter = os.environ.get("NERVIX_SUITE_BARE_PYTHON")
    if not interpreter:
        raise RuntimeError(
            "the boundary tier needs NERVIX_SUITE_BARE_PYTHON; `just test-client-python-api` sets it"
        )
    return Path(interpreter)
