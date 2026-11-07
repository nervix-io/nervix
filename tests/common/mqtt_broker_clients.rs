//! The test MQTT broker's own record of the clients connected to it.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** Reading one client's record from the broker's HTTP management API: the identity it
//!   connected with, its username, the keep-alive the broker holds it to, and whether it is
//!   connected.
//! - **Depends on.** `reqwest`, `serde`, `url` and the test environment's API address and
//!   credential.
//! - **Must not know.** Nervix, or why a client connected.

use std::io;

use nervix_test_environment::{DependencyEndpoints, MQTT_API_ADDR, MQTT_API_KEY, MQTT_API_SECRET};
use serde::Deserialize;

/// What the broker records of one client it knows.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct MqttBrokerClient {
    /// The client identity the client connected with.
    #[serde(rename = "clientid")]
    pub(crate) client_id: String,
    /// The username the client connected with, if it wrote one.
    pub(crate) username: Option<String>,
    /// The keep-alive, in seconds, the broker holds the client to.
    #[serde(rename = "keepalive")]
    pub(crate) keep_alive_seconds: u64,
    /// Whether the client's connection is open.
    pub(crate) connected: bool,
}

/// The broker's record of the client whose identity is `client_id`, or none while the broker knows
/// no client of that identity.
pub(crate) async fn mqtt_broker_client(
    dependencies: &DependencyEndpoints,
    client_id: &str,
) -> io::Result<Option<MqttBrokerClient>> {
    let api = dependencies.get(MQTT_API_ADDR)?;
    let mut url = url::Url::parse(api).map_err(io::Error::other)?;
    url.path_segments_mut()
        .map_err(|()| io::Error::other(format!("the MQTT API address '{api}' has no path")))?
        .extend(["api", "v5", "clients", client_id]);
    let response = reqwest::Client::new()
        .get(url)
        .basic_auth(MQTT_API_KEY, Some(MQTT_API_SECRET))
        .send()
        .await
        .map_err(io::Error::other)?;
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    let response = response.error_for_status().map_err(io::Error::other)?;
    let client = response
        .json::<MqttBrokerClient>()
        .await
        .map_err(io::Error::other)?;
    Ok(Some(client))
}
