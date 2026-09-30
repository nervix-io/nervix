//! Public endpoint connection lifetimes.
//!
//! Layer: test harness.
//! - **Owns.** One scenario's WebSocket connection across endpoint source replacement.
//! - **Depends on.** The cluster's public HTTP listener and NSPL scenario steps.
//! - **Must not know.** Runtime route tables or source binding internals.

use std::time::Duration;

use cucumber::{then, when};
use futures_util::{SinkExt, StreamExt};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::{net::TcpStream, time::timeout};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

use crate::{ScenarioWorld, docstring, expand_placeholders};

pub(crate) type EndpointWebsocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[when(expr = "an endpoint websocket is opened on node {string} host {string} path {string}")]
async fn open(world: &mut ScenarioWorld, node: String, host: String, path: String) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    world.endpoint_websocket = Some(
        world
            .cluster()
            .open_endpoint_websocket(&node, &host, &path)
            .await
            .assured("the running endpoint accepts the scenario connection"),
    );
}

#[when("a payload is sent on the endpoint websocket")]
async fn send(world: &mut ScenarioWorld, #[step] step: &cucumber::gherkin::Step) {
    let payload = expand_placeholders(world, docstring(step));
    world
        .endpoint_websocket
        .as_mut()
        .assured("the scenario opened its endpoint websocket")
        .send(Message::Text(payload))
        .await
        .assured("the endpoint connection remains open until intake");
}

#[then("the endpoint websocket closes with retry code 1013")]
async fn closes(world: &mut ScenarioWorld) {
    let websocket = world
        .endpoint_websocket
        .as_mut()
        .assured("the scenario opened its endpoint websocket");
    let message = timeout(Duration::from_secs(60), websocket.next())
        .await
        .assured("a stopped binding must close the retained endpoint connection")
        .assured("the peer sends a close frame")
        .assured("the peer sends a valid websocket frame");
    let Message::Close(Some(frame)) = message else {
        panic!("expected an endpoint retry close frame, received {message:?}");
    };
    assert_eq!(u16::from(frame.code), 1013);
}
