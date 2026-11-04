//! Public-browser request-window observations.
//!
//! Layer: test harness.
//! - **Owns.** Bounded real WebSocket frame observation and request/reply accounting.
//! - **Depends on.** Playwright, the public session codec and the scenario's browser.
//! - **Must not know.** Console state, request queues or server admission implementation.

use std::{collections::BTreeSet, time::Duration};

use bytes::Bytes;
use cucumber::{then, when};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{
    ClientFrame, ClientMessage, ClientRequest, MAX_IN_FLIGHT_REQUESTS, RequestId, ServerFrame,
    ServerMessage, SessionLimits, VerifiedFrame,
};
use serde::Deserialize;

use super::ScenarioWorld;

#[derive(Deserialize)]
#[serde(tag = "direction", rename_all = "lowercase")]
enum ObservedFrame {
    Sent { bytes: Vec<u8> },
    Received { bytes: Vec<u8> },
}

impl ObservedFrame {
    fn request(&self) -> Option<ClientMessage> {
        let Self::Sent { bytes } = self else {
            return None;
        };
        let frame = VerifiedFrame::<ClientFrame>::verify(
            Bytes::copy_from_slice(bytes),
            &SessionLimits::DEFAULT,
        )
        .assured("the browser sent a valid public session frame");
        Some(ClientMessage::decode(&frame).assured("the browser request decodes"))
    }

    fn terminal_reply(&self) -> Option<RequestId> {
        let Self::Received { bytes } = self else {
            return None;
        };
        let frame = VerifiedFrame::<ServerFrame>::verify(
            Bytes::copy_from_slice(bytes),
            &SessionLimits::DEFAULT,
        )
        .assured("the node sent a valid public session frame");
        match ServerMessage::decode(&frame).assured("the node's frame decodes") {
            ServerMessage::Reply(reply) => Some(reply.request_id),
            ServerMessage::TransferPart(part) => {
                let end = part
                    .offset()
                    .checked_add(part.chunk().len())
                    .assured("the verified transfer part ends within its declared size");
                if end == part.total_bytes() {
                    Some(part.request_id())
                } else {
                    None
                }
            }
            ServerMessage::Event(_) => None,
        }
    }
}

fn page(world: &ScenarioWorld) -> &playwright_rs::Page {
    world
        .browser_page
        .as_ref()
        .assured("the scenario opened its browser")
}

async fn record(world: &ScenarioWorld) -> Vec<ObservedFrame> {
    page(world)
        .evaluate::<(), Vec<ObservedFrame>>(
            "() => window.__nervixRequestWindow.record()",
            None::<&()>,
        )
        .await
        .assured("the bounded frame observer retained the whole observation")
}

pub(super) fn observer_script() -> String {
    let path = serde_json::to_string(nervix_client_wire::websocket::CONSOLE_SESSION_PATH)
        .assured("the public session path is JavaScript text");
    include_str!("observer.js").replace("__NERVIX_SESSION_PATH__", &path)
}

#[when("the web console is opened on the leader node with its request window observed")]
async fn observe(world: &mut ScenarioWorld) {
    super::close_browser(world).await;
    let leader = super::current_leader_node(world).await;
    let url = world
        .cluster()
        .web_console_url(&leader)
        .assured("the started leader serves its console");
    super::open_recorded_web_console_page(
        world,
        &url,
        super::web_console_connections::InitialSession::ObserveRequests,
    )
    .await
    .assured("the request observer starts before the console's scripts");
}

#[then(expr = "the web console has sent {int} observed commands")]
async fn commands_sent(world: &mut ScenarioWorld, expected: usize) {
    let mut seen: usize = 0;
    let result = nervix_primitives::time::timeout(super::WEB_CONSOLE_ASSERTION_TIMEOUT, async {
        loop {
            nervix_primitives::task::consume_budget().await;
            seen = 0;
            for frame in record(world).await {
                if let Some(ClientMessage {
                    request: ClientRequest::Command(_),
                    ..
                }) = frame.request()
                {
                    seen = seen
                        .checked_add(1)
                        .assured("the observer retains a bounded number of frames");
                }
            }
            if seen >= expected {
                return;
            }
            nervix_primitives::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "the browser sent {seen} commands, expected {expected}"
    );
    assert_eq!(seen, expected);
}

#[when("the web console finishes its scheduled input callbacks")]
async fn finish_callbacks(world: &mut ScenarioWorld) {
    page(world)
        .evaluate::<(), ()>("() => window.__nervixRequestWindow.settle()", None::<&()>)
        .await
        .assured("the input's scheduled callbacks and their continuations completed");
}

#[then("the observed web console session stays within its request window")]
async fn window_is_bounded(world: &mut ScenarioWorld) {
    let mut outstanding = BTreeSet::new();
    let mut peak = 0;
    for frame in record(world).await {
        if let Some(request) = frame.request() {
            assert!(
                outstanding.insert(request.request_id),
                "a request identity was reused"
            );
            peak = peak.max(outstanding.len());
            assert!(
                outstanding.len() <= MAX_IN_FLIGHT_REQUESTS,
                "the browser sent {} requests before their terminal replies",
                outstanding.len()
            );
        } else if let Some(request_id) = frame.terminal_reply() {
            outstanding.remove(&request_id);
        }
    }
    assert_eq!(
        peak, MAX_IN_FLIGHT_REQUESTS,
        "the regression must fill the real request window"
    );
}
