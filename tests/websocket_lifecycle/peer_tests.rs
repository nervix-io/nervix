//! Real-transport qualification of the scenario-local WebSocket peer.
//!
//! Layer: test harness.
//! - **Owns.** Cancelled openings, replacement payload delivery and established-reset assertions.
//! - **Depends on.** The shared WebSocket peer, execution primitives and the public protocol.
//! - **Must not know.** Scenario state, connector internals or runtime ownership handles.

use std::{
    io,
    net::{Ipv4Addr, SocketAddr},
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
        Error as WebsocketError, Message, client::IntoClientRequest as _, error::ProtocolError,
    },
};

use crate::websocket_peer::{PEER_WAIT, PeerEvent, WebsocketPeer};

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
