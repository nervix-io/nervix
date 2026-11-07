"""The pytest suite of `nervix_client`, the Python package of the Nervix client.

The suite has two tiers, each a directory: `boundary` needs no cluster and runs through
`just test-client-python-api`; `cluster` runs against the scenario cluster that
`tests/features/runtime/client_suites.feature` starts. Every test is registered by its node id in
`tests/client-suites-inventory.toml`, and whoever runs a tier holds its results to that inventory.

This file owns the result contract every host's suite shares: one line per test on standard
output, `NERVIX-SUITE <outcome> <node id>`, where the outcome is `passed`, `failed` or `skipped`. A
line is printed once a test's teardown ends, so it covers setup, the test and teardown together.
"""

from __future__ import annotations

import pytest

RESULT_PREFIX = "NERVIX-SUITE"


class Results:
    """Prints each test's outcome once its teardown ends."""

    def __init__(self, config: pytest.Config) -> None:
        self.config = config
        self.outcomes: dict[str, str] = {}

    def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
        if report.failed:
            outcome = "failed"
        elif report.skipped:
            outcome = "skipped"
        else:
            outcome = "passed"
        previous = self.outcomes.get(report.nodeid, "passed")
        if previous == "passed":
            self.outcomes[report.nodeid] = outcome
        if report.when != "teardown":
            return
        final = self.outcomes.pop(report.nodeid)
        terminal = self.config.pluginmanager.get_plugin("terminalreporter")
        line = f"{RESULT_PREFIX} {final} {report.nodeid}"
        if terminal is None:
            print(line, flush=True)
        else:
            # The reporter leaves its progress characters on the current line, so a result line
            # starts on a line of its own.
            terminal.write(f"\n{line}\n", flush=True)


def pytest_configure(config: pytest.Config) -> None:
    config.pluginmanager.register(Results(config), "nervix-suite-results")
