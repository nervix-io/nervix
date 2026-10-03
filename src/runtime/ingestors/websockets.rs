//! WebSocket client source runtime composition.
//!
//! Layer: data plane.
//!
//! - **Owns.** Composing a validated WebSocket connector plan, with the signaling protocol its
//!   client names, with host-owned intake.
//! - **Depends on.** The WebSocket connector, the connector source contract, the node resolver,
//!   and pre-resolved runtime execution handles.
//! - **Must not know.** WebSocket transport internals, NSPL parsing, registry validation, or
//!   placement computation.

use error_stack::ResultExt as _;
use nervix_connector_websockets::{WebsocketSource, WebsocketSourcePlan};

use super::{
    super::*,
    IngestorStartError, SourceStartError,
    source::{BrokerSourceStart, SourceStart},
};

impl WebsocketsIngestorStartPlan {
    pub(super) async fn compose(
        self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
    ) -> error_stack::Result<SourceStart, IngestorStartError> {
        let WebsocketsIngestorStartPlan {
            client,
            mode,
            signaling_protocol,
        } = self;
        let resolved = runtime
            .resolve_client_config(&ingestor.domain, client.mount.as_ref(), &client.config)
            .change_context_lazy(|| ingestor.initialize_failure())?;
        let signaling_protocol = match signaling_protocol {
            Some(name) => {
                let Some(protocol) = runtime.signaling_protocol(&ingestor.domain, &name).await
                else {
                    return Err(ingestor.source_start_failure(
                        SourceStartError::SignalingProtocolMissing { protocol: name },
                    ));
                };
                Some(protocol)
            }
            None => None,
        };
        let Some(dns) = runtime.dns() else {
            return Err(ingestor.source_start_failure(SourceStartError::NodeDnsUnavailable));
        };
        let connector = WebsocketSourcePlan::new(resolved.entries, signaling_protocol, dns.clone())
            .change_context_lazy(|| ingestor.initialize_failure())?;
        BrokerSourceStart {
            connector,
            instances: NonZeroU64::MIN,
            acknowledgement: mode.acknowledgement(),
            buffered_intake: true,
            flush_each_intake: true,
            // The client answers the server's pings only while the loop reads, so a held loop would
            // let a server that pings close the connection and lose what it sent until the source
            // reconnects.
            unacknowledged_admission: QueueAdmission::RefuseWhenFull,
            client_mounts: resolved.mounts.into_iter().collect(),
            connector_label: "websockets",
        }
        .open::<WebsocketSource>(ingestor)
        .await
    }
}
