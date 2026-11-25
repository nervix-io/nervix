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
    with pytest.raises(nx.InvalidArgumentError) as keepalive:
        nx.Session(server, keepalive_interval=float("nan"))
    assert "keepalive_interval" in keepalive.value.message
    with pytest.raises(nx.InvalidArgumentError):
        nx.Session(server, keepalive_timeout=0.0)
    for window_bytes in (0, -1, 1 << 32):
        with pytest.raises(nx.InvalidArgumentError) as window:
            nx.Session(server, stream_window_bytes=window_bytes)
        assert "stream_window_bytes" in window.value.message
    with pytest.raises(nx.InvalidArgumentError) as narrow:
        nx.Session(server, connection_window_bytes=65_534)
    assert "connection-window-bytes" in narrow.value.message
    for capacity in (0, -1):
        with pytest.raises(nx.InvalidArgumentError) as queue:
            nx.Session(server, call_queue_capacity=capacity)
        assert "call_queue_capacity" in queue.value.message
    for limit in (0, -1, 1 << 32):
        with pytest.raises(nx.InvalidArgumentError) as header_list:
            nx.Session(server, header_list_bytes=limit)
        assert "header_list_bytes" in header_list.value.message
        with pytest.raises(nx.InvalidArgumentError) as send_buffer:
            nx.Session(server, send_buffer_bytes=limit)
        assert "send_buffer_bytes" in send_buffer.value.message
        with pytest.raises(nx.InvalidArgumentError) as error_resets:
            nx.Session(server, max_error_reset_streams=limit)
        assert "max_error_reset_streams" in error_resets.value.message
        with pytest.raises(nx.InvalidArgumentError) as remembered:
            nx.Session(server, remembered_reset_streams=limit)
        assert "remembered_reset_streams" in remembered.value.message
    with pytest.raises(nx.InvalidArgumentError) as duration:
        nx.Session(server, reset_stream_duration=float("nan"))
    assert "reset_stream_duration" in duration.value.message
    with pytest.raises(nx.InvalidArgumentError) as hasty:
        nx.Session(server, connection_attempt_delay=0.005)
    assert "connection-attempt-delay" in hasty.value.message
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
