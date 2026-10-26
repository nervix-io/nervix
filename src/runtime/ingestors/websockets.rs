//! WebSocket client ingestor runtime composition.
//!
//! Layer: data plane.
//!
//! - **Owns.** Composing a validated WebSocket connector plan, with the signaling protocol its
//!   client names, with host-owned intake.
//! - **Depends on.** The WebSocket connector, the connector ingestor contract, the node resolver,
//!   and pre-resolved runtime execution handles.
//! - **Must not know.** WebSocket transport internals, Plexus parsing, registry validation, or
//!   placement computation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use error_stack::ResultExt as _;
use nervix_connector::IngestorConnector as _;
use nervix_connector_websockets::{WebsocketsConnectionIngestor, WebsocketsConnectionIngestorPlan};

use super::{
    super::*,
    IngestorConnectorStartError, IngestorStartError,
    connector::{BrokerIngestorStart, IngestorStart},
};

const _: () = assert!(matches!(
    WebsocketsConnectionIngestor::KIND,
    nervix_models::IngestorEntityKind::Websockets(
        nervix_models::WebsocketsIngestorEntity::Connection
    )
));

impl WebsocketsConnectionIngestorStartPlan {
    pub(super) async fn compose(
        self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
    ) -> error_stack::Result<IngestorStart, IngestorStartError> {
        let WebsocketsConnectionIngestorStartPlan {
            client,
            headers,
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
                    return Err(ingestor.connector_start_failure(
                        IngestorConnectorStartError::SignalingProtocolMissing { protocol: name },
                    ));
                };
                Some(protocol)
            }
            None => None,
        };
        let Some(dns) = runtime.dns() else {
            return Err(
                ingestor.connector_start_failure(IngestorConnectorStartError::NodeDnsUnavailable)
            );
        };
        let connector = WebsocketsConnectionIngestorPlan::new(
            resolved.entries,
            headers,
            signaling_protocol,
            dns.clone(),
        )
        .change_context_lazy(|| ingestor.initialize_failure())?;
        BrokerIngestorStart {
            connector,
            instances: NonZeroU64::MIN,
            acknowledgement: mode.acknowledgement(),
            buffered_intake: true,
            flush_each_intake: true,
            // The client answers the server's pings only while the loop reads, so a held loop would
            // let a server that pings close the connection and lose what it sent until the ingestor
            // reconnects.
            unacknowledged_admission: QueueAdmission::RefuseWhenFull,
            client_mounts: resolved.mounts.into_iter().collect(),
        }
        .open::<WebsocketsConnectionIngestor>(ingestor)
        .await
    }
}
