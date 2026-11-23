//! A `STOP` statement's request and its answer.
//!
//! Layer: control plane.
//! - **Owns.** Making an operator's stop request, waiting until its stop ends or its `CANCEL`
//!   withdraws it, and answering with how the stop ended and what it cut.
//! - **Depends on.** The committed domain record and the stop's owner.
//! - **Must not know.** How the stop drains or what its teardown cuts.

use nervix_consensus::CommandExecution;
use nervix_models::{
    CommandExecutionReference, DomainName, DomainStatus, OperatorStopRequest, StopDomain,
    StopRequest, StopRequestId, WaitLimit,
};

use super::answer::{cancelled_answer, stopped_answer};
use crate::application::{
    command_result::CommandResult,
    domain_clock::current_timestamp,
    model_mutation::{command_error, command_ok},
    session_service::SessionServiceImpl,
};

/// What became of an operator's stop request, as the committed record tells it.
enum StopResolution {
    /// The stop still holds the request.
    Waiting,
    /// The stop that held the request ended, and its report is committed. Boxed, so the states
    /// a waiting request reads on every applied revision stay small.
    Ended(Box<CommandResult>),
    /// The request was withdrawn: its `CANCEL` acted.
    Withdrawn,
    /// The domain no longer exists.
    Missing,
}

impl SessionServiceImpl {
    /// Executes an admitted `STOP` of `domain`, which was at `start_version` when the command was
    /// admitted. The request begins the domain's stop or joins the stop it is already in, and the
    /// statement answers once that stop ends, or once the request's `CANCEL` withdrew it.
    pub(in crate::application) async fn execute_stop(
        &self,
        execution: &CommandExecution,
        stop: StopDomain,
        start_version: u64,
    ) -> CommandResult {
        let Some(domain) = execution.domain().cloned() else {
            return command_error("no active domain selected".to_string());
        };
        let reference = execution.reference.clone();
        let Some(current) = self.inner.consensus.current_domain(&domain).await else {
            return command_error(format!("domain '{}' does not exist", domain.as_str()));
        };
        // The domain stopped, and started again, since this command was admitted: the stop that
        // served this request ended long ago, before a leader change resumed this execution.
        if current.start_version != start_version {
            return command_ok(format!(
                "stopped domain '{}'; it has been started again since",
                domain.as_str()
            ));
        }
        let own = StopRequestId::Operator(reference.clone());
        let resolved_already = current
            .stop_ending()
            .is_some_and(|ending| ending.resolved.contains(&own));
        let held_already = current
            .stopping()
            .is_some_and(|stopping| stopping.holds(&own));
        if !resolved_already && !held_already {
            if current.status == DomainStatus::Stopped {
                return command_error(format!("domain '{}' is already stopped", domain.as_str()));
            }
            let requested_at = execution.admitted_at().unwrap_or_else(current_timestamp);
            let request = StopRequest::Operator {
                reference: reference.clone(),
                request: OperatorStopRequest {
                    requested_at,
                    limit: stop.wait_limit,
                },
            };
            if let Err(error) = self
                .inner
                .consensus
                .request_domain_stop(domain.clone(), request, None)
                .await
            {
                return self
                    .consensus_report_response(
                        &error,
                        format!("failed to stop domain '{}'", domain.as_str()),
                    )
                    .await;
            }
            if let Some(state) = self.inner.consensus.current_domain(&domain).await
                && state.status == DomainStatus::Stopped
                && !state
                    .stop_ending()
                    .is_some_and(|ending| ending.resolved.contains(&own))
            {
                return command_error(format!("domain '{}' is already stopped", domain.as_str()));
            }
            if let Err(error) = self.apply_current_cluster_state().await {
                tracing::warn!(
                    domain = domain.as_str(),
                    error = %format_args!("{error:#}"),
                    "the stopping domain's intake stop did not complete everywhere yet"
                );
            }
        }
        self.wait_for_stop_request(&domain, &reference, start_version, stop.wait_limit)
            .await
    }

    /// Waits until the stop that holds operator request `reference` of `domain` ends, or until
    /// the request is withdrawn, and answers.
    async fn wait_for_stop_request(
        &self,
        domain: &DomainName,
        reference: &CommandExecutionReference,
        start_version: u64,
        limit: Option<WaitLimit>,
    ) -> CommandResult {
        let observer = self.inner.consensus.observer();
        let mut applied = observer.subscribe_applied();
        let own = StopRequestId::Operator(reference.clone());
        loop {
            nervix_primitives::task::consume_budget().await;
            match self.stop_resolution(domain, &own, start_version).await {
                StopResolution::Waiting => {}
                StopResolution::Ended(result) => return *result,
                StopResolution::Missing => {
                    return command_error(format!(
                        "domain '{}' was dropped while it stopped",
                        domain.as_str()
                    ));
                }
                StopResolution::Withdrawn => {
                    let cancelled = self.inner.domain_stops.take_cancelled(reference);
                    let after = match limit {
                        Some(limit) => limit.after(),
                        None => std::time::Duration::ZERO,
                    };
                    return command_error(cancelled_answer(
                        domain,
                        after,
                        cancelled.as_ref().map(|cancelled| &cancelled.waits),
                    ));
                }
            }
            if applied.changed().await.is_err() {
                return command_error(format!(
                    "the stop of domain '{}' lost its node before it ended",
                    domain.as_str()
                ));
            }
        }
    }

    async fn stop_resolution(
        &self,
        domain: &DomainName,
        own: &StopRequestId,
        start_version: u64,
    ) -> StopResolution {
        let Some(state) = self.inner.consensus.current_domain(domain).await else {
            return StopResolution::Missing;
        };
        if state.start_version != start_version {
            return StopResolution::Ended(Box::new(command_ok(format!(
                "stopped domain '{}'; it has been started again since",
                domain.as_str()
            ))));
        }
        if let Some(stopping) = state.stopping() {
            if stopping.holds(own) {
                return StopResolution::Waiting;
            }
            return StopResolution::Withdrawn;
        }
        if let Some(ending) = state.stop_ending()
            && ending.resolved.contains(own)
        {
            // The answer names what the stop cut, so it waits for the report beside the ending.
            if ending.report.is_none() {
                return StopResolution::Waiting;
            }
            return StopResolution::Ended(Box::new(command_ok(stopped_answer(
                domain, ending, own,
            ))));
        }
        StopResolution::Withdrawn
    }
}
