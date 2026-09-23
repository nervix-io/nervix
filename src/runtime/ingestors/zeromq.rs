//! ZeroMQ source runtime composition.
//!
//! Layer: data plane.
//!
//! - **Owns.** Composing the ZeroMQ connector plan with host-owned intake, whose quiesce either
//!   leaves the socket unread or buffers and drops what it keeps reading.
//! - **Depends on.** The connector source contract, the ZeroMQ connector, and pre-resolved
//!   runtime execution handles.
//! - **Must not know.** The ZeroMQ driver, socket lifecycle, NSPL parsing, registry validation,
//!   or placement computation.

mod source;

use source::{ZeroMqSource, ZeroMqSourcePlan};

use super::{
    super::*,
    source::{BrokerSourceStart, DeclaredSourceAcknowledgement},
};

pub(in crate::runtime) struct ZeroMqIngestor;

impl ZeroMqIngestor {
    pub(in crate::runtime) async fn start(
        runtime: &Runtime,
        plan: ZeroMqIngestorStartPlan,
    ) -> Result<(), RuntimeError> {
        let ZeroMqIngestorStartPlan {
            ingestor,
            client,
            mode,
        } = plan;
        runtime
            .start_broker_source::<ZeroMqSource>(BrokerSourceStart {
                ingestor: &ingestor,
                connector: ZeroMqSourcePlan::new(client.config),
                instances: NonZeroU64::MIN,
                acknowledgement: DeclaredSourceAcknowledgement::from(&mode),
                buffered_intake: true,
                flush_each_intake: false,
                client_mounts: Vec::new(),
                connector_label: "zeromq",
            })
            .await
    }
}
