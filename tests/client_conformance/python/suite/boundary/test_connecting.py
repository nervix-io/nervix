"""Opening a session without a cluster: every option the session cannot use is refused before
anything is sent, a session that requires TLS refuses a plaintext address, and a server nothing
listens on fails to connect."""

from __future__ import annotations

import pytest

import nervix_client as nx
from support import SilentListener, unused_port


def test_an_option_the_session_cannot_use_is_refused_before_anything_is_sent(
    silent_listener: SilentListener,
) -> None:
    server = silent_listener.uri
    with pytest.raises(nx.InvalidArgumentError) as half_credentials:
        nx.Session(server, username="alice")
    assert "password" in half_credentials.value.message
    with pytest.raises(nx.InvalidArgumentError) as seed:
        nx.Session(server, seeds=["not a url"])
    assert "seeds" in seed.value.message
    with pytest.raises(nx.InvalidArgumentError) as negative:
        nx.Session(server, connect_timeout=-1.0)
    assert "connect_timeout" in negative.value.message
    with pytest.raises(nx.InvalidArgumentError):
        nx.Session(server, request_timeout=0.0)
    with pytest.raises(nx.InvalidArgumentError):
        nx.Session(server, retry_timeout=float("inf"))
    with pytest.raises(nx.InvalidArgumentError):
        nx.Session(server, backup_wait_timeout=-0.5)
    with pytest.raises(nx.InvalidArgumentError):
        nx.Session(server, domain="not a domain")
    with pytest.raises(nx.InvalidArgumentError):
        nx.Session("http://127.0.0.1:1/a/path")
    assert not silent_listener.accepted


def test_a_session_that_requires_tls_refuses_a_plaintext_address_before_connecting(
    silent_listener: SilentListener,
) -> None:
    # ANCHOR: tls_required
    with pytest.raises(nx.ConnectError) as refused:
        nx.Session(silent_listener.uri, tls=nx.Tls.REQUIRED)
    assert refused.value.kind == nx.FailureKind.CONNECT
    # ANCHOR_END: tls_required
    with pytest.raises(nx.ConnectError):
        nx.Session(
            "https://127.0.0.1:1", seeds=[silent_listener.uri], tls=nx.Tls.PREFERRED
        )
    assert not silent_listener.accepted


def test_a_server_nothing_listens_on_fails_to_connect() -> None:
    server = f"http://127.0.0.1:{unused_port()}"
    with pytest.raises(nx.ConnectError) as refused:
        nx.Session(server, connect_timeout=1.0, retry_timeout=1.0)
    assert refused.value.kind == nx.FailureKind.CONNECT
    assert refused.value.execution_reference is None
    assert refused.value.open_refusal is None
