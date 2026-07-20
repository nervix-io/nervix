use std::{fmt::Display, time::Duration};

use futures_util::{SinkExt, StreamExt};
use nervix_models::CreateSignalingProtocol;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    time,
};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Error as WebSocketError, Message},
};
use triomphe::Arc;

#[derive(Debug)]
pub(crate) enum WebsocketSignalingError {
    InvalidTimeout(String),
    Send(WebSocketError),
    Receive(WebSocketError),
    Timeout(Duration),
    Closed,
}

impl Display for WebsocketSignalingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidTimeout(reason) => write!(f, "{reason}"),
            Self::Send(error) => write!(f, "failed to send signaling body: {error}"),
            Self::Receive(error) => write!(f, "failed to receive signaling body: {error}"),
            Self::Timeout(timeout) => write!(f, "timed out after {timeout:?}"),
            Self::Closed => f.write_str("websocket closed before signaling completed"),
        }
    }
}

impl std::error::Error for WebsocketSignalingError {}

pub(crate) struct WebsocketSignalingSession {
    protocol: Arc<CreateSignalingProtocol>,
    timeout: Duration,
}

impl WebsocketSignalingSession {
    pub(crate) fn new(
        protocol: Arc<CreateSignalingProtocol>,
    ) -> Result<Self, WebsocketSignalingError> {
        let timeout = humantime::parse_duration(&protocol.on_connect.timeout).map_err(|error| {
            WebsocketSignalingError::InvalidTimeout(format!(
                "invalid signaling protocol timeout '{}': {error}",
                protocol.on_connect.timeout
            ))
        })?;
        Ok(Self { protocol, timeout })
    }

    pub(crate) async fn run<S>(
        &self,
        websocket: &mut WebSocketStream<S>,
    ) -> Result<Vec<Vec<u8>>, WebsocketSignalingError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        for body in &self.protocol.on_connect.send_bodies {
            websocket
                .send(Message::Text(body.clone()))
                .await
                .map_err(WebsocketSignalingError::Send)?;
        }

        time::timeout(self.timeout, self.wait_for_bodies(websocket))
            .await
            .map_err(|_| WebsocketSignalingError::Timeout(self.timeout))?
    }

    async fn wait_for_bodies<S>(
        &self,
        websocket: &mut WebSocketStream<S>,
    ) -> Result<Vec<Vec<u8>>, WebsocketSignalingError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let mut pending_wait_bodies = self.protocol.on_connect.wait_bodies.clone();
        let mut buffered_payloads = Vec::new();

        while !pending_wait_bodies.is_empty() {
            tokio::task::consume_budget().await;
            let Some(message) = websocket.next().await else {
                return Err(WebsocketSignalingError::Closed);
            };
            let message = message.map_err(WebsocketSignalingError::Receive)?;
            match message {
                Message::Text(text) => {
                    let text = text.to_string();
                    if let Some(index) = matching_wait_body(&pending_wait_bodies, &text) {
                        pending_wait_bodies.remove(index);
                    } else {
                        buffered_payloads.push(text.into_bytes());
                    }
                }
                Message::Binary(bytes) => {
                    if let Ok(text) = std::str::from_utf8(bytes.as_ref())
                        && let Some(index) = matching_wait_body(&pending_wait_bodies, text)
                    {
                        pending_wait_bodies.remove(index);
                        continue;
                    }
                    buffered_payloads.push(bytes.to_vec());
                }
                Message::Ping(payload) => {
                    websocket
                        .send(Message::Pong(payload))
                        .await
                        .map_err(WebsocketSignalingError::Send)?;
                }
                Message::Close(_) => return Err(WebsocketSignalingError::Closed),
                Message::Pong(_) | Message::Frame(_) => {}
            }
        }

        Ok(buffered_payloads)
    }
}

fn matching_wait_body(expected: &[String], actual: &str) -> Option<usize> {
    expected
        .iter()
        .position(|expected| signaling_body_matches(expected, actual))
}

fn signaling_body_matches(expected: &str, actual: &str) -> bool {
    if expected == actual {
        return true;
    }

    let Ok(expected_json) = serde_json::from_str::<serde_json::Value>(expected) else {
        return false;
    };
    let Ok(actual_json) = serde_json::from_str::<serde_json::Value>(actual) else {
        return false;
    };
    expected_json == actual_json
}
