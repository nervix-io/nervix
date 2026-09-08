//! Regression coverage for connection cancellation and task lifetime.

use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    pin::Pin,
    sync::{
        Arc as StdArc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

use tokio::{
    io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::{Notify, mpsc},
    time::{sleep, timeout},
};
use tokio_util::sync::CancellationToken;

use super::*;
use crate::connection::{
    ReconnectBackoff, drive_connection, establish_outbound_connection, exchange_introductions,
    register_connected_peer, unregister_connected_peer,
};

#[derive(Default)]
struct IoObservation {
    observe_read: AtomicBool,
    require_partial_read: AtomicBool,
    read_observed: Notify,
    observe_partial_write: AtomicBool,
    partial_write_observed: Notify,
}

impl IoObservation {
    fn observe_next_read(&self, require_partial: bool) {
        self.require_partial_read
            .store(require_partial, Ordering::Release);
        self.observe_read.store(true, Ordering::Release);
    }

    fn observe_next_partial_write(&self) {
        self.observe_partial_write.store(true, Ordering::Release);
    }
}

struct ObservedIo {
    stream: DuplexStream,
    observation: StdArc<IoObservation>,
}

impl AsyncRead for ObservedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let requested = buffer.remaining();
        let filled_before = buffer.filled().len();
        let result = Pin::new(&mut self.stream).poll_read(context, buffer);
        if let Poll::Ready(Ok(())) = &result {
            let read = buffer.filled().len() - filled_before;
            let require_partial = self
                .observation
                .require_partial_read
                .load(Ordering::Acquire);
            if read > 0
                && (!require_partial || read < requested)
                && self.observation.observe_read.swap(false, Ordering::AcqRel)
            {
                self.observation.read_observed.notify_waiters();
            }
        }
        result
    }
}

impl AsyncWrite for ObservedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let result = Pin::new(&mut self.stream).poll_write(context, buffer);
        if let Poll::Ready(Ok(written)) = &result
            && *written > 0
            && *written < buffer.len()
            && self
                .observation
                .observe_partial_write
                .swap(false, Ordering::AcqRel)
        {
            self.observation.partial_write_observed.notify_waiters();
        }
        result
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.stream).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.stream).poll_shutdown(context)
    }
}

fn framed_wire_bytes(envelope: &WireEnvelope) -> Vec<u8> {
    let payload = encode_wire_envelope(envelope).expect("test wire envelope should encode");
    let frame_size = u32::try_from(payload.len()).expect("test frame should fit in u32");
    let mut frame = frame_size.to_be_bytes().to_vec();
    frame.extend(payload);
    frame
}

async fn wait_until_connected(inner: &TransportInner, node: &ClusterNodeName) {
    timeout(Duration::from_secs(1), async {
        loop {
            tokio::task::consume_budget().await;
            if inner.connected_peers.contains_key(node) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("connection handshake should complete");
}

#[tokio::test]
async fn partial_frame_read_survives_an_opposite_direction_write() {
    let identity_a = test_identity(&ClusterNodeName::parse("node-a").expect("valid name"));
    let identity_b = test_identity(&ClusterNodeName::parse("node-b").expect("valid name"));
    let (incoming_tx, mut incoming_rx) = mpsc::channel(1);
    let inner = test_inner(identity_a, verifier_for(&[&identity_b]), incoming_tx, 1);
    let (connection_stream, mut peer_stream) = tokio::io::duplex(64 * 1024);
    let observation = StdArc::new(IoObservation::default());
    let observed_stream = ObservedIo {
        stream: connection_stream,
        observation: observation.clone(),
    };
    let peer_addr = "127.0.0.1:12345".parse().expect("valid peer address");
    let (outgoing_tx, outgoing_rx) = mpsc::channel(1);
    let cancel = CancellationToken::new();
    let reply_handle = ConnectionHandle::new(
        peer_addr,
        outgoing_tx.clone(),
        cancel.clone(),
        inner.admission_closed.clone(),
        inner.options.queue_admission_timeout,
    );
    let connection_inner = inner.clone();
    let connection_cancel = cancel.clone();
    let connection_task = tokio::spawn(async move {
        let mut outgoing_rx = outgoing_rx;
        let mut retry_payload = None;
        let established =
            exchange_introductions(&connection_inner, Box::new(observed_stream)).await?;
        let peer_node_id = established.peer_node_id.clone();
        register_connected_peer(&connection_inner, &peer_node_id, &reply_handle);
        let result = drive_connection(
            connection_inner.clone(),
            peer_addr,
            reply_handle.clone(),
            established,
            &connection_cancel,
            &mut outgoing_rx,
            &mut retry_payload,
        )
        .await;
        unregister_connected_peer(&connection_inner, &peer_node_id, &reply_handle);
        result
    });

    let introduction = read_wire_envelope(&mut peer_stream, DEFAULT_MAX_FRAME_BYTES)
        .await
        .expect("read local introduction");
    assert!(matches!(introduction, WireEnvelope::Introduction(_)));
    write_wire_envelope(
        &mut peer_stream,
        &WireEnvelope::Introduction(identity_b.signed_introduction()),
    )
    .await
    .expect("write peer introduction");
    wait_until_connected(&inner, identity_b.node_id()).await;

    let expected = Envelope::RelayPayload(dummy_stream_payload("fragmented_read"));
    let frame = framed_wire_bytes(&WireEnvelope::Payload(expected.clone()));
    let split_at = 12;
    let partial_read_observed = observation.read_observed.notified();
    observation.observe_next_read(true);
    peer_stream
        .write_all(&frame[..split_at])
        .await
        .expect("write first frame fragment");
    partial_read_observed.await;
    sleep(PING_INTERVAL + Duration::from_millis(25)).await;

    outgoing_tx
        .send(Envelope::Control(ControlEnvelope::Terminate))
        .await
        .expect("queue opposite-direction frame");
    loop {
        match read_wire_envelope(&mut peer_stream, DEFAULT_MAX_FRAME_BYTES)
            .await
            .expect("read opposite-direction frame")
        {
            WireEnvelope::Ping => {}
            WireEnvelope::Payload(Envelope::Control(ControlEnvelope::Terminate)) => break,
            other => panic!("unexpected opposite-direction frame: {other:?}"),
        }
    }
    peer_stream
        .write_all(&frame[split_at..])
        .await
        .expect("write remaining frame fragment");

    let received = recv_one(&mut incoming_rx).await;
    assert_eq!(received.envelope, expected);

    cancel.cancel();
    connection_task
        .await
        .expect("connection task should join")
        .expect("connection should stop cleanly");
}

#[tokio::test]
async fn partial_frame_write_survives_an_opposite_direction_read() {
    let identity_a = test_identity(&ClusterNodeName::parse("node-a").expect("valid name"));
    let identity_b = test_identity(&ClusterNodeName::parse("node-b").expect("valid name"));
    let (incoming_tx, _incoming_rx) = mpsc::channel(1);
    let inner = test_inner(identity_a, verifier_for(&[&identity_b]), incoming_tx, 1);
    let (connection_stream, mut peer_stream) = tokio::io::duplex(64);
    let observation = StdArc::new(IoObservation::default());
    let observed_stream = ObservedIo {
        stream: connection_stream,
        observation: observation.clone(),
    };
    let peer_addr = "127.0.0.1:12345".parse().expect("valid peer address");
    let (outgoing_tx, outgoing_rx) = mpsc::channel(1);
    let cancel = CancellationToken::new();
    let reply_handle = ConnectionHandle::new(
        peer_addr,
        outgoing_tx.clone(),
        cancel.clone(),
        inner.admission_closed.clone(),
        inner.options.queue_admission_timeout,
    );
    let connection_inner = inner.clone();
    let connection_cancel = cancel.clone();
    let connection_task = tokio::spawn(async move {
        let mut outgoing_rx = outgoing_rx;
        let mut retry_payload = None;
        let established =
            exchange_introductions(&connection_inner, Box::new(observed_stream)).await?;
        let peer_node_id = established.peer_node_id.clone();
        register_connected_peer(&connection_inner, &peer_node_id, &reply_handle);
        let result = drive_connection(
            connection_inner.clone(),
            peer_addr,
            reply_handle.clone(),
            established,
            &connection_cancel,
            &mut outgoing_rx,
            &mut retry_payload,
        )
        .await;
        unregister_connected_peer(&connection_inner, &peer_node_id, &reply_handle);
        result
    });

    let introduction = read_wire_envelope(&mut peer_stream, DEFAULT_MAX_FRAME_BYTES)
        .await
        .expect("read local introduction");
    assert!(matches!(introduction, WireEnvelope::Introduction(_)));
    write_wire_envelope(
        &mut peer_stream,
        &WireEnvelope::Introduction(identity_b.signed_introduction()),
    )
    .await
    .expect("write peer introduction");
    wait_until_connected(&inner, identity_b.node_id()).await;

    let mut payload = dummy_stream_payload("fragmented_write");
    payload.batch_ipc = vec![7; 4 * 1024];
    let expected = Envelope::RelayPayload(payload);
    let partial_write_observed = observation.partial_write_observed.notified();
    observation.observe_next_partial_write();
    outgoing_tx
        .send(expected.clone())
        .await
        .expect("queue large frame");
    partial_write_observed.await;
    sleep(PING_INTERVAL + Duration::from_millis(25)).await;

    let opposite_read_observed = observation.read_observed.notified();
    observation.observe_next_read(false);
    write_wire_envelope(&mut peer_stream, &WireEnvelope::Ping)
        .await
        .expect("write opposite-direction ping");
    opposite_read_observed.await;

    loop {
        match read_wire_envelope(&mut peer_stream, DEFAULT_MAX_FRAME_BYTES)
            .await
            .expect("read large frame without framing corruption")
        {
            WireEnvelope::Ping => {}
            WireEnvelope::Payload(envelope) => {
                assert_eq!(envelope, expected);
                break;
            }
            other => panic!("unexpected frame while waiting for payload: {other:?}"),
        }
    }

    cancel.cancel();
    connection_task
        .await
        .expect("connection task should join")
        .expect("connection should stop cleanly");
}

#[tokio::test]
async fn silent_inbound_handshake_cannot_prevent_shutdown() {
    let identity = test_identity(&ClusterNodeName::parse("node-a").expect("valid name"));
    let (transport, _incoming) = Transport::bind(
        "127.0.0.1:0".parse().expect("valid listen address"),
        TransportMode::Plain,
        None,
        identity,
        PeerVerifier::new(|_| None),
        TransportOptions::default(),
    )
    .await
    .expect("bind transport");
    let mut silent_peer = TcpStream::connect(transport.local_addr())
        .await
        .expect("connect silent peer");
    let introduction = read_wire_envelope(&mut silent_peer, DEFAULT_MAX_FRAME_BYTES)
        .await
        .expect("server should begin its handshake");
    assert!(matches!(introduction, WireEnvelope::Introduction(_)));

    timeout(Duration::from_secs(2), transport.shutdown())
        .await
        .expect("silent handshake must not leave shutdown waiting indefinitely");
}

#[tokio::test]
async fn peer_departure_releases_its_outbound_connection_permit() {
    let options = TransportOptions {
        max_connections: 1,
        ..TransportOptions::default()
    };
    let node_a = ClusterNodeName::parse("node-a").expect("valid name");
    let node_b = ClusterNodeName::parse("node-b").expect("valid name");
    let node_c = ClusterNodeName::parse("node-c").expect("valid name");
    let identity_a = test_identity(&node_a);
    let identity_b = test_identity(&node_b);
    let identity_c = test_identity(&node_c);
    let (transport_a, _incoming_a) = Transport::bind(
        "127.0.0.1:0".parse().expect("valid listen address"),
        TransportMode::Plain,
        None,
        identity_a.clone(),
        verifier_for(&[&identity_b, &identity_c]),
        options.clone(),
    )
    .await
    .expect("bind transport a");
    let silent_departed_peer = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind silent departed peer");
    let departed_addr = silent_departed_peer
        .local_addr()
        .expect("read departed peer address");
    let (transport_c, mut incoming_c) = Transport::bind(
        "127.0.0.1:0".parse().expect("valid listen address"),
        TransportMode::Plain,
        None,
        identity_c,
        verifier_for(&[&identity_a]),
        options,
    )
    .await
    .expect("bind transport c");

    transport_a.replace_live_nodes(&BTreeSet::from([node_b.clone(), node_c.clone()]));
    transport_a
        .send(
            &node_b,
            departed_addr,
            "localhost",
            TransportMode::Plain,
            Envelope::RelayPayload(dummy_stream_payload("before_departure")),
        )
        .await
        .expect("queue data while the first peer's introduction is incomplete");

    transport_a.replace_live_nodes(&BTreeSet::from([node_c]));
    timeout(Duration::from_secs(2), async {
        loop {
            tokio::task::consume_budget().await;
            match transport_a
                .send(
                    transport_c.node_id(),
                    transport_c.local_addr(),
                    "localhost",
                    TransportMode::Plain,
                    Envelope::RelayPayload(dummy_stream_payload("after_departure")),
                )
                .await
            {
                Ok(()) => break,
                Err(TransportError::PoolExhausted) => tokio::task::yield_now().await,
                Err(error) => panic!("unexpected connection error: {error}"),
            }
        }
    })
    .await
    .expect("departed peer should release its connection permit");
    let received = recv_one(&mut incoming_c).await;
    assert_eq!(
        received.envelope,
        Envelope::RelayPayload(dummy_stream_payload("after_departure"))
    );

    transport_a.shutdown().await;
    transport_c.shutdown().await;
    drop(silent_departed_peer);
}

#[tokio::test]
async fn repeated_address_replacement_reaps_drivers_and_reuses_the_permit() {
    let options = TransportOptions {
        max_connections: 1,
        ..TransportOptions::default()
    };
    let node_a = ClusterNodeName::parse("node-a").expect("valid name");
    let node_b = ClusterNodeName::parse("node-b").expect("valid name");
    let identity_a = test_identity(&node_a);
    let identity_b = test_identity(&node_b);
    let (transport_a, _incoming_a) = Transport::bind(
        "127.0.0.1:0".parse().expect("valid listen address"),
        TransportMode::Plain,
        None,
        identity_a.clone(),
        verifier_for(&[&identity_b]),
        options.clone(),
    )
    .await
    .expect("bind transport a");
    transport_a.replace_live_nodes(&BTreeSet::from([node_b.clone()]));
    let mut peers = Vec::new();

    for replacement in 0..4 {
        tokio::task::consume_budget().await;
        let (peer, mut incoming) = Transport::bind(
            "127.0.0.1:0".parse().expect("valid listen address"),
            TransportMode::Plain,
            None,
            identity_b.clone(),
            verifier_for(&[&identity_a]),
            options.clone(),
        )
        .await
        .expect("bind replacement peer");
        let target = PeerTarget::new(peer.local_addr(), "localhost", TransportMode::Plain);
        transport_a.replace_outbound_targets(&BTreeMap::from([(
            node_b.clone(),
            BTreeSet::from([target.clone()]),
        )]));
        timeout(Duration::from_secs(2), async {
            loop {
                tokio::task::consume_budget().await;
                match transport_a
                    .send(
                        &node_b,
                        target.addr,
                        &target.server_name,
                        target.mode,
                        Envelope::RelayPayload(dummy_stream_payload("replacement")),
                    )
                    .await
                {
                    Ok(()) => break,
                    Err(TransportError::PoolExhausted) => tokio::task::yield_now().await,
                    Err(error) => panic!("unexpected replacement connection error: {error}"),
                }
            }
        })
        .await
        .expect("replacement address should acquire the released permit");
        let received = recv_one(&mut incoming).await;
        assert_eq!(
            received.envelope,
            Envelope::RelayPayload(dummy_stream_payload("replacement")),
            "replacement {replacement} should receive exactly one payload"
        );
        timeout(Duration::from_secs(2), async {
            loop {
                tokio::task::consume_budget().await;
                if transport_a.active_outbound_connections().await == 1
                    && transport_a.inner.tasks.len() <= 2
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("retired connection driver should be reaped");
        peers.push((peer, incoming));
    }

    transport_a.shutdown().await;
    for (peer, _incoming) in peers {
        peer.shutdown().await;
    }
}

#[tokio::test]
async fn send_queue_admission_is_deadline_bound() {
    let (tx, _rx) = mpsc::channel(1);
    let handle = ConnectionHandle::new(
        "127.0.0.1:12345".parse().expect("valid peer address"),
        tx,
        CancellationToken::new(),
        CancellationToken::new(),
        Duration::from_millis(25),
    );
    handle
        .send(Envelope::Control(ControlEnvelope::Terminate))
        .await
        .expect("first frame should fill the queue");
    let error = handle
        .send(Envelope::Control(ControlEnvelope::Terminate))
        .await
        .expect_err("second frame should reach the admission deadline");
    assert!(matches!(
        error,
        TransportError::QueueAdmissionTimeout { .. }
    ));
}

#[tokio::test]
async fn silent_outbound_tls_handshake_reaches_the_setup_deadline() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind silent tls peer");
    let peer_addr = listener.local_addr().expect("read silent peer address");
    let silent_peer = tokio::spawn(async move {
        let (_stream, _addr) = listener.accept().await.expect("accept tls connection");
        std::future::pending::<()>().await;
    });
    let options = TransportOptions {
        connection_setup_timeout: Duration::from_millis(50),
        ..TransportOptions::default()
    };
    let identity = test_identity(&ClusterNodeName::parse("node-a").expect("valid name"));
    let (transport, _incoming) = Transport::bind(
        "127.0.0.1:0".parse().expect("valid listen address"),
        TransportMode::Tls,
        Some(test_tls()),
        identity,
        PeerVerifier::new(|_| None),
        options,
    )
    .await
    .expect("bind transport");
    let key = ConnectionKey {
        peer_node_id: ClusterNodeName::parse("node-b").expect("valid peer name"),
        addr: peer_addr,
        server_name: "localhost".to_string(),
        mode: TransportMode::Tls,
    };
    let error = match establish_outbound_connection(
        &transport.inner,
        &key,
        &CancellationToken::new(),
    )
    .await
    {
        Ok(_) => panic!("silent TLS handshake should time out"),
        Err(error) => error,
    };
    assert!(matches!(
        error.current_context(),
        TransportError::ConnectionSetupTimeout { .. }
    ));

    silent_peer.abort();
    let _ = silent_peer.await;
    transport.shutdown().await;
}

#[tokio::test]
async fn outbound_connection_rejects_a_different_authenticated_peer() {
    let node_a = ClusterNodeName::parse("node-a").expect("valid name");
    let node_b = ClusterNodeName::parse("node-b").expect("valid name");
    let node_c = ClusterNodeName::parse("node-c").expect("valid name");
    let identity_a = test_identity(&node_a);
    let identity_b = test_identity(&node_b);
    let identity_c = test_identity(&node_c);
    let (transport_a, _incoming_a) = Transport::bind(
        "127.0.0.1:0".parse().expect("valid listen address"),
        TransportMode::Plain,
        None,
        identity_a.clone(),
        verifier_for(&[&identity_b, &identity_c]),
        TransportOptions::default(),
    )
    .await
    .expect("bind transport a");
    let (transport_c, mut incoming_c) = Transport::bind(
        "127.0.0.1:0".parse().expect("valid listen address"),
        TransportMode::Plain,
        None,
        identity_c,
        verifier_for(&[&identity_a]),
        TransportOptions::default(),
    )
    .await
    .expect("bind transport c");

    transport_a
        .send(
            &node_b,
            transport_c.local_addr(),
            "localhost",
            TransportMode::Plain,
            Envelope::RelayPayload(dummy_stream_payload("wrong_peer")),
        )
        .await
        .expect("queue data for the expected peer");

    assert!(
        timeout(Duration::from_millis(300), incoming_c.recv())
            .await
            .is_err(),
        "data addressed to node-b must not reach authenticated node-c"
    );
    assert!(!transport_a.is_connected_to(&node_b));
    assert!(!transport_a.is_connected_to(&node_c));

    transport_a.shutdown().await;
    transport_c.shutdown().await;
}

#[test]
fn reconnect_backoff_is_exponential_jittered_and_capped() {
    let initial = Duration::from_millis(200);
    let max = Duration::from_secs(5);
    let mut backoff = ReconnectBackoff::new(initial, max);
    for nominal in [
        Duration::from_millis(200),
        Duration::from_millis(400),
        Duration::from_millis(800),
        Duration::from_millis(1_600),
        Duration::from_millis(3_200),
        Duration::from_secs(5),
        Duration::from_secs(5),
    ] {
        let delay = backoff.next_delay();
        assert!(delay >= nominal / 2);
        assert!(delay <= nominal);
    }
    backoff.reset();
    let reset = backoff.next_delay();
    assert!(reset >= initial / 2);
    assert!(reset <= initial);
}
