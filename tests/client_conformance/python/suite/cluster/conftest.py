"""Fixtures of the cluster tier, which runs against the cluster the scenario started."""

from __future__ import annotations

import uuid
from collections.abc import Iterator

import pytest

import nervix_client as nx
from support import Cluster, Relay


@pytest.fixture(scope="session")
def cluster() -> Cluster:
    return Cluster.from_environment()


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

