//! Syslog ingestor runtime composition.
//!
//! Layer: data plane.
//!
//! - **Owns.** Composing the Syslog connector plan, bound to this node's listener address, with
//!   host-owned intake.
//! - **Depends on.** The connector ingestor contract, the Syslog connector, pre-resolved runtime
//!   execution handles, and a test-only listener fault channel supplied by the harness.
//! - **Must not know.** Syslog socket drivers, framing, TLS configuration, Plexus parsing, registry
//!   validation, or placement computation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use error_stack::ResultExt as _;
use nervix_connector::IngestorConnector as _;
use nervix_connector_syslog::{SyslogListenerIngestor, SyslogListenerIngestorPlan};
use nervix_models::IngestAcknowledgement;

use super::{
    super::*,
    IngestorStartError,
    connector::{BrokerIngestorStart, IngestorStart},
};

const _: () = assert!(matches!(
    SyslogListenerIngestor::KIND,
    nervix_models::IngestorEntityKind::Syslog(nervix_models::SyslogIngestorEntity::Listener)
));

impl SyslogListenerIngestorStartPlan {
    pub(super) async fn compose(
        self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
    ) -> error_stack::Result<IngestorStart, IngestorStartError> {
        let SyslogListenerIngestorStartPlan { client } = self;
        let resolved = runtime
            .resolve_client_config(&ingestor.domain, client.mount.as_ref(), &client.config)
            .change_context_lazy(|| ingestor.initialize_failure())?;
        let connector = SyslogListenerIngestorPlan::new(resolved.entries, |configured| {
            runtime.syslog_ingestor_bind_addr(configured)
        })
        .change_context_lazy(|| ingestor.initialize_failure())?;
        #[cfg(feature = "testing")]
        let connector =
            connector.with_listener_faults(runtime.inner.fault_injection.syslog_listener_faults());
        BrokerIngestorStart {
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
        }
        .open::<SyslogListenerIngestor>(ingestor)
        .await
    }
}
