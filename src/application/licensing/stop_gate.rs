//! The licensing stop gate's effects outside consensus: the leader's stops and gate notices.
//!
//! Layer: control plane.
//! - **Owns.** Stopping every running domain with the licensing cause while the committed gate is
//!   closed, and announcing the gate's closing and lifting to this node's sessions.
//! - **Depends on.** Committed licensing and domain observations, the domain stop path and the
//!   session event bus.
//! - **Must not know.** Signed documents, service requests, policy grants or graph execution.

use error_stack::{Report, ResultExt as _};
use nervix_client_wire::NoticeKind;
use nervix_models::{DomainName, DomainStatus, DomainStopCause, LicensingStopReason};
use nervix_primitives::{sync::CancellationToken, time::sleep};
use thiserror::Error;

use crate::application::session_service::SessionServiceImpl;

#[derive(Debug, Error)]
enum LicensingStopError {
    #[error("the licensing stop of domain '{domain}' was not committed")]
    Commit { domain: DomainName },
    #[error("the licensing stop of domain '{domain}' did not complete across the cluster")]
    Application { domain: DomainName },
}

/// Runs on every node. Each node announces the gate's transitions to its own sessions, because
/// the gate is a committed fact every node applies at the same index; only the leader stops
/// domains, through the ordinary stop command carrying the licensing cause.
pub(in crate::application) async fn run(service: SessionServiceImpl, shutdown: CancellationToken) {
    let observer = service.inner.consensus.observer();
    let mut applied = observer.subscribe_applied();
    let mut topology = observer.subscribe_topology();
    // The gate a node finds when it starts is its baseline; only later transitions are announced.
    let mut announced = service.committed_licensing_stop();
    loop {
        nervix_primitives::task::consume_budget().await;
        let gate = service.committed_licensing_stop();
        if gate != announced {
            announce(&service, gate);
            announced = gate;
        }
        let leads = service
            .inner
            .consensus
            .current_leader_tenure()
            .is_some_and(|tenure| tenure.leader_id() == observer.local_node_id());
        let mut retry = false;
        if leads && let Some(reason) = gate {
            retry = !service.stop_running_domains_for_licensing(reason).await;
        }
        let retry_pause = async {
            if retry {
                sleep(service.cluster_parameters().licensing_request_timeout()).await;
            } else {
                std::future::pending::<()>().await;
            }
        };
        nervix_primitives::select! {
            _ = shutdown.cancelled() => break,
            changed = applied.changed() => if changed.is_err() { break; },
            changed = topology.changed() => if !changed { break; },
            _ = retry_pause => {},
        }
    }
}

fn announce(service: &SessionServiceImpl, gate: Option<LicensingStopReason>) {
    match gate {
        Some(reason) => {
            tracing::info!(%reason, "licensing stop gate closed");
            service.inner.events.relay_error(
                NoticeKind::Licensing,
                format!(
                    "Processing stopped: {reason}. Data and administrative access remain; \
                     activate a new license, then START the stopped domains."
                ),
            );
        }
        None => {
            tracing::info!("licensing stop gate lifted");
            service.inner.events.relay_info(
                NoticeKind::Licensing,
                "Processing may resume: a new activation lifted the licensing stop. Domains it \
                 stopped stay stopped until START."
                    .to_owned(),
            );
        }
    }
}

impl SessionServiceImpl {
    /// The committed stop gate (specification §10.7), read from consensus rather than from the
    /// published policy, so it is current for the applied index that woke the caller.
    pub(in crate::application) fn committed_licensing_stop(&self) -> Option<LicensingStopReason> {
        let snapshot = self.inner.consensus.observer().licensing_snapshot();
        let initialized = snapshot.initialized?;
        initialized.record.processing_stop()
    }

    /// Stops every domain that is not stopped, and returns whether each stop completed. A domain
    /// another execution's mutation lease holds refuses the stop until that execution ends, as it
    /// refuses an operator's `STOP`; the gate already refuses every start meanwhile.
    async fn stop_running_domains_for_licensing(&self, reason: LicensingStopReason) -> bool {
        let domains = self.inner.consensus.observer().current_domains().await;
        let mut complete = true;
        for (domain, state) in domains {
            nervix_primitives::task::consume_budget().await;
            if state.status == DomainStatus::Stopped {
                continue;
            }
            // The stop would be refused while the lease is held; its release is an applied
            // change, which wakes the caller to stop the domain then.
            if self
                .inner
                .consensus
                .current_domain_mutation(&domain)
                .await
                .is_some()
            {
                complete = false;
                tracing::debug!(
                    domain = domain.as_str(),
                    "licensing stop waits for the domain's mutation lease"
                );
                continue;
            }
            match self.stop_domain_for_licensing(&domain, reason).await {
                Ok(()) => {
                    tracing::info!(domain = domain.as_str(), %reason, "domain stopped by licensing");
                }
                Err(error) => {
                    complete = false;
                    tracing::warn!(
                        domain = domain.as_str(),
                        error = %format_args!("{error:#}"),
                        "licensing stop of a running domain did not complete; it is retried"
                    );
                }
            }
        }
        complete
    }

    async fn stop_domain_for_licensing(
        &self,
        domain: &DomainName,
        reason: LicensingStopReason,
    ) -> Result<(), Report<LicensingStopError>> {
        self.inner
            .consensus
            .stop_domain(domain.clone(), None, DomainStopCause::Licensing(reason))
            .await
            .change_context_lazy(|| LicensingStopError::Commit {
                domain: domain.clone(),
            })?;
        self.apply_current_cluster_state()
            .await
            .change_context_lazy(|| LicensingStopError::Application {
                domain: domain.clone(),
            })?;
        Ok(())
    }
}
