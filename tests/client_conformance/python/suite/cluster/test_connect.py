"""Connecting to the cluster: authentication and TLS requirements on both listener transports,
certificate trust, seeds and timeouts, and typed connection failures."""

from __future__ import annotations

from urllib.parse import urlsplit

import pytest

import nervix_client as nx
from support import Cluster, completed, deadline, unused_port


def test_a_session_opens_with_the_certificate_authority(cluster: Cluster) -> None:
    authority = cluster.ca_certificate
    server, domain = cluster.server, cluster.domain
    username, password = cluster.username, cluster.password
    # ANCHOR: connect
    with nx.Session(
        server,
        domain=domain,
        username=username,
        password=password,
        ca_certificate=authority,
    ) as session:
        outcome = session.execute(session.prepare("LIST DOMAINS;"))
        assert outcome.disposition == nx.Disposition.COMPLETED
        assert domain in outcome.message
    # ANCHOR_END: connect


def test_every_tls_requirement_matches_the_listener_transport(cluster: Cluster) -> None:
    for requirement in (nx.Tls.PREFERRED, nx.Tls.REQUIRED):
        if requirement == nx.Tls.REQUIRED and urlsplit(cluster.server).scheme == "http":
            with pytest.raises(nx.ConnectError):
                nx.Session(cluster.server, tls=requirement, cancel=deadline())
            continue
        with nx.Session(
            cluster.server,
            domain=cluster.domain,
            username=cluster.username,
            password=cluster.password,
            tls=requirement,
            ca_certificate=cluster.ca_certificate,
            cancel=deadline(),
        ) as session:
            completed(session, "LIST DOMAINS;")
    # The same listener named over plaintext is refused before anything is sent.
    plaintext = cluster.server.replace("https://", "http://", 1)
    with pytest.raises(nx.ConnectError):
        nx.Session(plaintext, tls=nx.Tls.REQUIRED)


def test_connection_trust_matches_the_listener_transport(cluster: Cluster) -> None:
    if urlsplit(cluster.server).scheme == "http":
        # A plaintext listener needs no certificate authority; authentication still applies.
        with nx.Session(
            cluster.server,
            domain=cluster.domain,
            username=cluster.username,
            password=cluster.password,
            cancel=deadline(),
        ) as session:
            completed(session, "LIST DOMAINS;")
        return
    # The one server refuses the handshake at once; the timeouts only bound a hang.
    with pytest.raises(nx.ConnectError) as untrusted:
        nx.Session(
            cluster.server,
            domain=cluster.domain,
            username=cluster.username,
            password=cluster.password,
            connect_timeout=30.0,
            retry_timeout=60.0,
            cancel=deadline(),
        )
    assert untrusted.value.kind == nx.FailureKind.CONNECT
    assert untrusted.value.message


def test_a_wrong_password_is_refused(cluster: Cluster) -> None:
    # ANCHOR: wrong_password
    with pytest.raises(nx.ConnectError) as refused:
        nx.Session(
            cluster.server,
            domain=cluster.domain,
            username=cluster.username,
            password="not the password",
            ca_certificate=cluster.ca_certificate,
            cancel=deadline(),
        )
    assert refused.value.kind == nx.FailureKind.CONNECT
    # ANCHOR_END: wrong_password
    assert "not the password" not in refused.value.message


def test_a_seed_reaches_the_cluster_when_the_server_does_not(cluster: Cluster) -> None:
    scheme = urlsplit(cluster.server).scheme
    unreachable = f"{scheme}://127.0.0.1:{unused_port()}"
    # ANCHOR: seeds
    with nx.Session(
        unreachable,
        domain=cluster.domain,
        username=cluster.username,
        password=cluster.password,
        ca_certificate=cluster.ca_certificate,
        seeds=list(cluster.seeds),
        connect_timeout=5.0,
        request_timeout=30.0,
        retry_timeout=60.0,
        backup_wait_timeout=300.0,
        cancel=deadline(),
    ) as session:
        completed(session, "LIST DOMAINS;")
    # ANCHOR_END: seeds


def test_a_cluster_no_server_or_seed_reaches_fails_to_connect(cluster: Cluster) -> None:
    servers = [f"https://127.0.0.1:{unused_port()}" for _ in range(3)]
    # Each address refuses the connection at once, and the session tries each of them once.
    with pytest.raises(nx.ConnectError) as unreachable:
        nx.Session(
            servers[0],
            domain=cluster.domain,
            username=cluster.username,
            password=cluster.password,
            ca_certificate=cluster.ca_certificate,
            seeds=servers[1:],
            connect_timeout=30.0,
            retry_timeout=60.0,
            cancel=deadline(),
        )
    assert unreachable.value.kind == nx.FailureKind.CONNECT
