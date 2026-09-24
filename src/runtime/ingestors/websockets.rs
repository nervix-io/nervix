//! WebSocket client source runtime composition.
//!
//! Layer: data plane.
//!
//! - **Owns.** Composing a validated WebSocket connector plan, with the signaling protocol its
//!   client names, with host-owned intake.
//! - **Depends on.** The WebSocket connector, the connector source contract, and pre-resolved
//!   runtime execution handles.
//! - **Must not know.** WebSocket transport internals, NSPL parsing, registry validation, or
//!   placement computation.

use nervix_connector_websockets::{WebsocketSource, WebsocketSourcePlan};

use super::{
    super::*,
    source::{BrokerSourceStart, SourceStart},
};

impl WebsocketsIngestorStartPlan {
    pub(super) async fn compose(
        self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
    ) -> Result<SourceStart, RuntimeError> {
        let WebsocketsIngestorStartPlan {
            client,
            mode,
            signaling_protocol,
        } = self;
        let resolved = runtime
            .resolve_client_config(&ingestor.domain, client.mount.as_ref(), &client.config)
            .map_err(|error| ingestor.start_failure(error.to_string()))?;
        let signaling_protocol = match signaling_protocol {
            Some(name) => {
                let Some(protocol) = runtime.signaling_protocol(&ingestor.domain, &name).await
                else {
                    return Err(ingestor
                        .start_failure(format!("missing signaling protocol '{}'", name.as_str())));
                };
                Some(protocol)
            }
            None => None,
        };
        let connector = WebsocketSourcePlan::new(resolved.entries, signaling_protocol)
            .map_err(|error| ingestor.start_failure(error.to_string()))?;
        BrokerSourceStart {
            connector,
            instances: NonZeroU64::MIN,
            acknowledgement: mode.acknowledgement(),
            buffered_intake: true,
            flush_each_intake: true,
            client_mounts: resolved.mounts.into_iter().collect(),
            connector_label: "websockets",
        }
        .open::<WebsocketSource>(ingestor)
        .await
    }
}
