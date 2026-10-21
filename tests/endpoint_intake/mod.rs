//! Public endpoint connection lifetimes and acknowledgements.
//!
//! Layer: test harness.
//! - **Owns.** One scenario's named HTTP requests and WebSocket connection, including source
//!   replacement and signaling replies.
//! - **Depends on.** The cluster's public HTTP listener and Plexus scenario steps.
//! - **Must not know.** Runtime route tables or source binding internals.

pub(crate) mod requests;

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

#[then(expr = "the endpoint websocket acknowledges with {string} cause {string}")]
async fn acknowledges(world: &mut ScenarioWorld, outcome: String, cause: String) {
    assert_eq!(
        receive_json(world).await,
        serde_json::json!({ "outcome": outcome, "cause": cause })
    );
}

#[then("the endpoint websocket receives a JSON frame")]
async fn receives_json(world: &mut ScenarioWorld, #[step] step: &cucumber::gherkin::Step) {
    let expected = expand_placeholders(world, docstring(step));
    let expected: serde_json::Value =
        serde_json::from_str(&expected).expect("the expected signaling frame is valid JSON");
    assert_eq!(receive_json(world).await, expected);
}

async fn receive_json(world: &mut ScenarioWorld) -> serde_json::Value {
    let websocket = world
        .endpoint_websocket
        .as_mut()
        .assured("the scenario opened its endpoint websocket");
    let message = timeout(Duration::from_secs(60), websocket.next())
        .await
        .unwrap_or_else(|error| panic!("the endpoint did not send its signaling frame: {error}"))
        .assured("the endpoint keeps its connection open while signaling")
        .unwrap_or_else(|error| panic!("invalid endpoint signaling frame: {error}"));
    let Message::Text(payload) = message else {
        panic!("a JSON signaling protocol must send a text frame: {message:?}");
    };
    serde_json::from_str(&payload)
        .unwrap_or_else(|error| panic!("invalid JSON endpoint signaling frame: {error}"))
}
