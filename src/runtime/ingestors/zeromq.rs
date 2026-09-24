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

use nervix_connector_zeromq::{ZeroMqSource, ZeroMqSourcePlan};

use super::{
    super::*,
    source::{BrokerSourceStart, SourceStart},
};

impl ZeroMqIngestorStartPlan {
    pub(super) async fn compose(
        self,
        ingestor: &IngestorSpec,
    ) -> Result<SourceStart, RuntimeError> {
        let ZeroMqIngestorStartPlan { client, mode } = self;
        BrokerSourceStart {
            connector: ZeroMqSourcePlan::new(client.config),
            instances: NonZeroU64::MIN,
            acknowledgement: mode.acknowledgement(),
            buffered_intake: true,
            flush_each_intake: false,
            client_mounts: Vec::new(),
            connector_label: "zeromq",
        }
        .open::<ZeroMqSource>(ingestor)
        .await
    }
}
