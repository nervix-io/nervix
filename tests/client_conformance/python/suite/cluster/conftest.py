"""Fixtures of the cluster tier, which runs against the cluster the scenario started."""

from __future__ import annotations

import uuid
from collections.abc import Iterator

import pytest

import nervix_client as nx
from actions import Harness, SuitePipes
from support import WAIT_SECONDS, Cluster, Relay


@pytest.fixture(scope="session")
def cluster() -> Cluster:
    return Cluster.from_environment()


@pytest.fixture(scope="session")
def harness(suite_action_pipes: SuitePipes) -> Harness:
    """The channel to the scenario's declared testing-build actions."""

    return Harness(
        within=WAIT_SECONDS,
        input_fd=suite_action_pipes.input_fd,
        output_fd=suite_action_pipes.output_fd,
    )


@pytest.fixture
def subscription_opening_refusal(harness: Harness) -> Iterator[Harness]:
    """Always clears unused node refusals before another suite test can open a subscription."""

    try:
        yield harness
    finally:
        harness.clear_subscription_opening_refusals()


@pytest.fixture
def clock_attachment_refusal(harness: Harness) -> Iterator[Harness]:
    """Clears unused clock refusals even when the test fails."""

    try:
        yield harness
    finally:
        harness.clear_clock_attachment_refusals()


@pytest.fixture
def session(cluster: Cluster) -> Iterator[nx.Session]:
    """A session in the scenario's domain, released when the test ends."""

    with cluster.connect() as session:
        yield session


@pytest.fixture
def relay(cluster: Cluster) -> Iterator[Relay]:
    """A TCP relay in front of the leader's session listener. A session opened through it is not
    redirected, so cutting the relay ends it."""

    relay = Relay(cluster.leader)
    yield relay
    relay.close()


@pytest.fixture
def unique() -> str:
    """A suffix that makes the names a test creates its own."""

    return uuid.uuid4().hex[:12]

