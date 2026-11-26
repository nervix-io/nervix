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
use nervix_connector_websockets::{
    WebsocketsConnectionIngestor, WebsocketsConnectionIngestorPlan, WebsocketsSignalingProtocol,
};

use super::{
    super::*,
    IngestorConnectorStartError, IngestorStartError,
    connector::{BrokerIngestorStart, IngestorStart},
    connector_host::IngestorEntityServices,
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
        // The protocol an ingestor names reaches its connector through its host handle, as the
        // shared value the domain compiled; the plan says nothing of it.
        let mut entity = IngestorEntityServices::default();
        if let Some(name) = signaling_protocol {
            let Some(protocol) = runtime.signaling_protocol(&ingestor.domain, &name).await else {
                return Err(ingestor.connector_start_failure(
                    IngestorConnectorStartError::SignalingProtocolMissing { protocol: name },
                ));
            };
            // Typed as the connector asks for it: its handle answers by exactly this type.
            let shared: WebsocketsSignalingProtocol = protocol;
            entity.signaling_protocol = Some(Box::new(shared));
        }
        // The connector takes the resolver from its handle when it opens; a node without one
        // refuses the start here, before anything of the connector runs.
        if runtime.dns().is_none() {
            return Err(
                ingestor.connector_start_failure(IngestorConnectorStartError::NodeDnsUnavailable)
            );
        }
        let connector = WebsocketsConnectionIngestorPlan::new(resolved.entries, headers)
            .change_context_lazy(|| ingestor.initialize_failure())?;
        BrokerIngestorStart {
            connector,
            host: runtime.ingestor_connector_host(ingestor, entity),
            instances: NonZeroU64::MIN,
            acknowledgement: mode.acknowledgement(),
            buffered_intake: true,
            flush_each_intake: true,
            client_mounts: resolved.mounts.into_iter().collect(),
        }
        .open::<WebsocketsConnectionIngestor>(runtime, ingestor)
        .await
    }
}
