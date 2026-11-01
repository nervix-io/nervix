//! Real-transport qualification of the scenario-local WebSocket peer.
//!
//! Layer: test harness.
//! - **Owns.** Cancelled openings, replacement payload delivery, established-reset assertions,
//!   scripted subprotocol answers and ping reports ordered around a held signaling handshake.
//! - **Depends on.** The shared WebSocket peer, execution primitives and the public protocol.
//! - **Must not know.** Scenario state, connector internals or runtime ownership handles.

use std::{
    io,
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};

use futures_util::{SinkExt as _, StreamExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::{
    net::{TcpListener, TcpStream},
    time::timeout,
};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        Error as WebsocketError, Message,
        client::IntoClientRequest as _,
        error::{ProtocolError, SubProtocolError},
        handshake::client::Request,
    },
};

use crate::websocket_peer::{PEER_WAIT, PeerEvent, SubprotocolAnswer, WebsocketPeer};

async fn connect(address: SocketAddr) -> WebSocketStream<TcpStream> {
    let mut request = format!("ws://{address}/events")
        .into_client_request()
        .assured("the bound fixture publishes a WebSocket endpoint");
    request.headers_mut().insert(
        "authorization",
        "Bearer fixture"
            .parse()
            .assured("the fixture token is a header value"),
    );
    let stream = TcpStream::connect(address)
        .await
        .assured("the client reaches the listening fixture");
    let (socket, response) = tokio_tungstenite::client_async(request, stream)
        .await
        .assured("the fixture accepts its authorized upgrade");
    assert_eq!(
        response
            .headers()
            .get("x-upgrade")
            .assured("the fixture adds its response header"),
        "ready"
    );
    socket
}

async fn answer_probe(socket: &mut WebSocketStream<TcpStream>) {
    assert!(matches!(
        socket
            .next()
            .await
            .assured("the fixture sends its probe")
            .assured("the probe is a valid frame"),
        Message::Ping(_)
    ));
    socket
        .flush()
        .await
        .assured("the client flushes its queued Pong");
}

async fn receive_payload(socket: &mut WebSocketStream<TcpStream>) {
    let text = socket
        .next()
        .await
        .assured("the text payload arrives")
        .assured("the text frame is valid");
    let binary = socket
        .next()
        .await
        .assured("the binary payload arrives")
        .assured("the binary frame is valid");
    assert!(matches!(text, Message::Text(ref value) if value == "{\"user_id\":42}"));
    assert!(matches!(binary, Message::Binary(ref value) if value == b"{\"user_id\":43}"));
}

#[nervix_primitives::test]
async fn an_upgraded_opening_can_end_before_its_probe_without_taking_the_payload() {
    timeout(PEER_WAIT, async {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .assured("the fixture binds");
        let address = listener.local_addr().assured("the listener has an address");
        let mut peer = WebsocketPeer::declaration(listener);
        let opening = connect(address).await;
        drop(opening);
        assert!(matches!(peer.next().await, PeerEvent::OpeningCancelled));

        let mut socket = connect(address).await;
        answer_probe(&mut socket).await;
        peer.send_payload();
        receive_payload(&mut socket).await;
        socket.close(None).await.assured("the client sends Close");
        assert!(matches!(
            socket
                .next()
                .await
                .assured("the peer acknowledges the client Close")
                .assured("the Close acknowledgement is a valid frame"),
            Message::Close(_)
        ));
        assert!(matches!(peer.next().await, PeerEvent::ReceivedClose));
        peer.stop().await;
    })
    .await
    .assured("the fixture qualifies within its existing budget");
}

#[nervix_primitives::test]
async fn an_established_declaration_connection_reset_still_fails_the_fixture() {
    timeout(PEER_WAIT, async {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .assured("the fixture binds");
        let address = listener.local_addr().assured("the listener has an address");
        let mut peer = WebsocketPeer::declaration(listener);
        let mut socket = connect(address).await;
        answer_probe(&mut socket).await;
        peer.send_payload();
        receive_payload(&mut socket).await;
        drop(socket);
        let failure = peer
            .join()
            .await
            .assured("the fixture task joins")
            .expect_err("an established transport failure remains fatal");
        let cause = failure
            .get_ref()
            .and_then(|error| error.downcast_ref::<WebsocketError>())
            .assured("the fixture retains its typed protocol cause");
        match cause {
            WebsocketError::Protocol(ProtocolError::ResetWithoutClosingHandshake) => {}
            WebsocketError::Io(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::ConnectionReset
                        | io::ErrorKind::BrokenPipe
                        | io::ErrorKind::ConnectionAborted
                ) => {}
            other => panic!("unexpected transport ending: {other:?}"),
        }
    })
    .await
    .assured("the fixture qualifies within its existing budget");
}

fn offering(address: SocketAddr, offer: &str) -> Request {
    let mut request = format!("ws://{address}/events")
        .into_client_request()
        .assured("the bound fixture publishes a WebSocket endpoint");
    request.headers_mut().insert(
        "sec-websocket-protocol",
        offer.parse().assured("the offer is a header value"),
    );
    request
}

#[nervix_primitives::test]
async fn a_subprotocol_peer_reports_each_offer_and_answers_every_connection_as_scripted() {
    timeout(PEER_WAIT, async {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .assured("the fixture binds");
        let address = listener.local_addr().assured("the listener has an address");
        let mut peer =
            WebsocketPeer::subprotocol(listener, SubprotocolAnswer::Select("v1.events".into()));

        let stream = TcpStream::connect(address)
            .await
            .assured("the client reaches the listening fixture");
        let (mut socket, response) =
            tokio_tungstenite::client_async(offering(address, "v2.events,v1.events"), stream)
                .await
                .assured("the peer selects a subprotocol the client offered");
        assert_eq!(
            response
                .headers()
                .get("sec-websocket-protocol")
                .assured("the peer answers with its selection"),
            "v1.events"
        );
        let PeerEvent::Upgraded { offer } = peer.next().await else {
            panic!("the peer reports the completed upgrade");
        };
        assert_eq!(offer.as_deref(), Some("v2.events,v1.events"));
        let payload = socket
            .next()
            .await
            .assured("the peer sends its payload over a kept connection")
            .assured("the payload is a valid frame");
        assert!(matches!(payload, Message::Text(ref text) if text == "{\"user_id\":42}"));
        socket.close(None).await.assured("the client sends Close");

        // A client that did not offer the selection refuses it; the next connection is answered
        // the same way.
        let stream = TcpStream::connect(address)
            .await
            .assured("the client reaches the listening fixture again");
        let refused = tokio_tungstenite::client_async(offering(address, "v2.events"), stream).await;
        assert!(matches!(
            refused,
            Err(WebsocketError::Protocol(
                ProtocolError::SecWebSocketSubProtocolError(SubProtocolError::InvalidSubProtocol)
            ))
        ));
        let PeerEvent::Upgraded { offer } = peer.next().await else {
            panic!("the peer reports the refused upgrade too");
        };
        assert_eq!(offer.as_deref(), Some("v2.events"));
        peer.stop().await;
    })
    .await
    .assured("the fixture qualifies within its existing budget");
}

#[nervix_primitives::test]
async fn a_ping_peer_orders_each_ping_around_the_signaling_it_holds() {
    timeout(PEER_WAIT, async {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .assured("the fixture binds");
        let address = listener.local_addr().assured("the listener has an address");
        let mut peer = WebsocketPeer::ping_cadence(listener, Duration::from_millis(200));
        let request = format!("ws://{address}/events")
            .into_client_request()
            .assured("the bound fixture publishes a WebSocket endpoint");
        let stream = TcpStream::connect(address)
            .await
            .assured("the client reaches the listening fixture");
        let (mut socket, _) = tokio_tungstenite::client_async(request, stream)
            .await
            .assured("the peer upgrades the connection");
        assert!(matches!(peer.next().await, PeerEvent::Connected(_)));

        socket
            .send(Message::Text("{\"op\":\"subscribe\"}".into()))
            .await
            .assured("the client subscribes");
        socket
            .send(Message::Ping(Vec::new()))
            .await
            .assured("the client pings while signaling is held");
        assert!(
            matches!(peer.next().await, PeerEvent::ReceivedPing(_)),
            "a ping during the held handshake is reported before signaling completes"
        );
        let PeerEvent::SignalingCompleted(completed) = peer.next().await else {
            panic!("the peer answers signaling once its hold elapses");
        };
        let mut texts = Vec::new();
        while texts.len() < 2 {
            nervix_primitives::task::consume_budget().await;
            match socket
                .next()
                .await
                .assured("the peer answers signaling and sends its payload")
                .assured("the peer sends valid frames")
            {
                Message::Text(text) => texts.push(text),
                Message::Pong(_) => {}
                other => panic!("unexpected frame from the ping peer: {other:?}"),
            }
        }
        assert_eq!(texts, ["{\"ready\":true}", "{\"user_id\":42}"]);
        socket
            .send(Message::Ping(Vec::new()))
            .await
            .assured("the client pings after signaling");
        let PeerEvent::ReceivedPing(arrived) = peer.next().await else {
            panic!("the peer reports a ping after signaling");
        };
        assert!(arrived >= completed);
        peer.stop().await;
    })
    .await
    .assured("the fixture qualifies within its existing budget");
}
