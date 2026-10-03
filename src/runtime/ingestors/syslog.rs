//! Syslog source runtime composition.
//!
//! Layer: data plane.
//!
//! - **Owns.** Composing the Syslog connector plan, bound to this node's listener address, with
//!   host-owned intake.
//! - **Depends on.** The connector source contract, the Syslog connector, and pre-resolved runtime
//!   execution handles.
//! - **Must not know.** Syslog socket drivers, framing, TLS configuration, NSPL parsing, registry
//!   validation, or placement computation.

use error_stack::ResultExt as _;
use nervix_connector_syslog::{SyslogSource, SyslogSourcePlan};
use nervix_models::IngestAcknowledgement;

use super::{
    super::*,
    IngestorStartError,
    source::{BrokerSourceStart, SourceStart},
};

impl SyslogIngestorStartPlan {
    pub(super) async fn compose(
        self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
    ) -> error_stack::Result<SourceStart, IngestorStartError> {
        let SyslogIngestorStartPlan { client } = self;
        let resolved = runtime
            .resolve_client_config(&ingestor.domain, client.mount.as_ref(), &client.config)
            .change_context_lazy(|| ingestor.initialize_failure())?;
        let connector = SyslogSourcePlan::new(resolved.entries, |configured| {
            runtime.syslog_ingestor_bind_addr(configured)
        })
        .change_context_lazy(|| ingestor.initialize_failure())?;
        BrokerSourceStart {
            connector,
            instances: NonZeroU64::MIN,
            acknowledgement: IngestAcknowledgement::Unacknowledged,
            buffered_intake: true,
            flush_each_intake: false,
            // The listener stays bound while the loop waits: the kernel drops UDP datagrams beyond
            // its receive buffer, and TCP and TLS senders wait once the listener's bounded frame
            // queue is full.
            unacknowledged_admission: QueueAdmission::WaitForPlace,
            client_mounts: resolved.mounts.into_iter().collect(),
            connector_label: "syslog",
        }
        .open::<SyslogSource>(ingestor)
        .await
    }
}
