//! A node's answers to the leader's stop drain and stop report requests.
//!
//! Layer: control plane.
//! - **Owns.** Accepting the stop's requests only from the current leader, and answering them
//!   from this node's runtime.
//! - **Depends on.** The stop wire contracts, consensus leadership and the runtime's stop view and
//!   cut ledger.
//! - **Must not know.** How the stop's owner decides from the views.

use error_stack::Report;
use nervix_interconnect::{
    HandlerRegistrationError, RemoteOperationFailure, RemoteOperationSubject, Transport,
    domain_stop::{StopDrainRequest, StopDrainStatus, StopReportAnswer, StopReportRequest},
};
use nervix_models::{ClusterNodeName, DomainName};

use crate::application::session_service::SessionServiceImpl;

fn not_from_the_leader(domain: &DomainName) -> RemoteOperationFailure {
    RemoteOperationFailure::failed(
        RemoteOperationSubject::domain(domain),
        "the stop's owner is not the current leader".to_string(),
    )
}

impl SessionServiceImpl {
    pub(in crate::application) fn register_domain_stop_handlers(
        &self,
        interconnect: &Transport,
    ) -> Result<(), Report<HandlerRegistrationError>> {
        let drain = self.clone();
        interconnect.register_handler::<StopDrainRequest, _, _>(move |context, request| {
            let service = drain.clone();
            let peer = context.peer_node_id().clone();
            async move { service.handle_stop_drain_request(&peer, request).await }
        })?;
        let report = self.clone();
        interconnect.register_handler::<StopReportRequest, _, _>(move |context, request| {
            let service = report.clone();
            let peer = context.peer_node_id().clone();
            async move { service.handle_stop_report_request(&peer, request).await }
        })?;
        Ok(())
    }

    /// Takes the drain step the leader asks and reports the stopping domain as this node reads
    /// it. A node that has not applied the stopping record yet still runs its intake, and reports
    /// its ingestors as admitting, so the drain cannot rest on its work.
    pub(super) async fn handle_stop_drain_request(
        &self,
        peer: &ClusterNodeName,
        request: StopDrainRequest,
    ) -> Result<StopDrainStatus, RemoteOperationFailure> {
        if self.inner.consensus.current_leader().await.as_ref() != Some(peer) {
            return Err(not_from_the_leader(&request.domain));
        }
        Ok(self
            .inner
            .runtime
            .stop_drain_status(&request.domain, request.action))
    }

    pub(super) async fn handle_stop_report_request(
        &self,
        peer: &ClusterNodeName,
        request: StopReportRequest,
    ) -> Result<StopReportAnswer, RemoteOperationFailure> {
        if self.inner.consensus.current_leader().await.as_ref() != Some(peer) {
            return Err(not_from_the_leader(&request.domain));
        }
        Ok(self
            .inner
            .runtime
            .stop_report(&request.domain, request.start_version))
    }
}
