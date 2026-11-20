//! WebSocket endpoint bounds observed over a real upgraded connection.
//!
//! Layer: test harness.
//! - **Owns.** WebSocket peers that exceed node limits or stop sending bytes.
//! - **Depends on.** The public listener, its WebSocket protocol, and the scenario cluster.
//! - **Must not know.** The listener's I/O wrappers or the endpoint's session implementation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "checks node bounds through public endpoint connections"
    )
)]

use std::{io, time::Duration};

use cucumber::then;
use futures_util::{SinkExt as _, StreamExt as _};
use meticulous::ResultExt as _;
use nervix_models::parse_duration_text;
use nervix_primitives::time::{Instant, timeout};
use tokio_tungstenite::tungstenite::{
    Error, Message,
    error::ProtocolError,
    protocol::frame::{
        Frame,
        coding::{Data, OpCode},
    },
};

use super::EndpointWebsocket;
use crate::{ScenarioWorld, expand_placeholders};

const WAIT: Duration = Duration::from_secs(30);

#[then(
    expr = "a second WebSocket on host {string} path {string} closes without a Close frame for an \
            oversized {word}"
)]
async fn oversized_session(world: &mut ScenarioWorld, host: String, path: String, limit: String) {
    let mut peer = connect(world, &host, &path).await;
    match limit.as_str() {
        // One byte over the 16-byte frame cap and well below the 32-byte message cap.
        "frame" => peer
            .send(Message::Binary(vec![b'x'; 17]))
            .await
            .assured("the peer sends the oversized frame"),
        // All three frame payloads fit, but together they exceed the message cap by one byte.
        "message" => {
            let frames = [
                Frame::message(vec![b'x'; 16], OpCode::Data(Data::Binary), false),
                Frame::message(vec![b'x'; 16], OpCode::Data(Data::Continue), false),
                Frame::message(vec![b'x'], OpCode::Data(Data::Continue), true),
            ];
            for frame in frames {
                peer.send(Message::Frame(frame))
                    .await
                    .assured("the peer sends a fragment within the frame cap");
            }
        }
        other => panic!("expected a frame or message bound, got {other}"),
    }
    expect_transport_end(&mut peer).await;
}

#[then(
    expr = "a WebSocket on host {string} path {string} closes without a Close frame after \
            {string} of silence"
)]
async fn idle_session(world: &mut ScenarioWorld, host: String, path: String, idle: String) {
    let idle = parse_duration_text(&idle).assured("a valid listener idle duration");
    let mut peer = connect(world, &host, &path).await;
    let progress = Instant::now();
    let ping = vec![1];
    peer.send(Message::Ping(ping.clone()))
        .await
        .assured("a fresh WebSocket sends traffic");
    let pong = timeout(WAIT, peer.next())
        .await
        .assured("the endpoint responds to the ping");
    assert!(matches!(pong, Some(Ok(Message::Pong(payload))) if payload == ping));

    expect_transport_end(&mut peer).await;
    assert!(
        progress.elapsed() >= idle,
        "the endpoint closed before its idle bound of {idle:?}"
    );
}

async fn connect(world: &ScenarioWorld, host: &str, path: &str) -> EndpointWebsocket {
    let host = expand_placeholders(world, host);
    timeout(
        WAIT,
        world
            .cluster()
            .open_endpoint_websocket("node-1", &host, path),
    )
    .await
    .assured("the endpoint accepts the upgrade within the fixture budget")
    .assured("the endpoint accepts the upgrade")
}

async fn expect_transport_end(peer: &mut EndpointWebsocket) {
    let end = timeout(WAIT, peer.next())
        .await
        .assured("the WebSocket session ends within the fixture budget");
    match end {
        None | Some(Err(Error::Protocol(ProtocolError::ResetWithoutClosingHandshake))) => {}
        Some(Err(Error::Io(error)))
            if matches!(
                error.kind(),
                io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionAborted
            ) => {}
        other => {
            panic!("expected the transport to end without a WebSocket Close frame, got {other:?}")
        }
    }
}
