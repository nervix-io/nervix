"""Run the cluster tier with control pipes that survive pytest's ordinary fd capture.

Owns the two duplicated descriptors until every pytest teardown ends. Depends on pytest and the
suite's action adapter; it knows neither the binding nor the cluster's fault implementation.
"""

from __future__ import annotations

import os
import sys
from contextlib import ExitStack

import pytest

from actions import SuitePipes


class ActionPipes:
    def __init__(self, pipes: SuitePipes) -> None:
        self.pipes = pipes

    @pytest.fixture(scope="session")
    def suite_action_pipes(self) -> SuitePipes:
        return self.pipes


def main() -> int:
    with ExitStack() as resources:
        input_fd = os.dup(0)
        resources.callback(os.close, input_fd)
        output_fd = os.dup(1)
        resources.callback(os.close, output_fd)
        pipes = SuitePipes(input_fd, output_fd)
        return int(pytest.main(sys.argv[1:], plugins=[ActionPipes(pipes)]))


if __name__ == "__main__":
    raise SystemExit(main())
