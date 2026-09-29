//! Admission and serialized installation of ownership-sensitive runtime execution.
//!
//! Layer: control plane.
//!
//! - **Owns.** The process-local linearizable catch-up proof and ordering of coherent runtime-state
//!   installation and activation after that proof.
//! - **Depends on.** Consensus observation, Tokio synchronization, and process shutdown.
//! - **Must not know.** Runtime graph internals, connector implementations, or scheduling policy.

use std::{sync::OnceLock, time::Duration};

use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_consensus::{ConsensusRuntimeState, Observer};
use nervix_models::{ClusterNodeName, ClusterSchedule};
use tokio::sync::{Mutex, MutexGuard};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{
    registry::PlannedClusterRevision,
    runtime::{Runtime, RuntimeError},
};

const ADMISSION_RETRY_INTERVAL: Duration = Duration::from_millis(500);

/// The process-start barrier and runtime-installation sequencer shared by every installation path.
pub(in crate::application) struct RuntimeAdmission {
    committed_log_index: OnceLock<u64>,
    attempt: Mutex<()>,
    installation: Mutex<()>,
    /// The last schedule this node applied successfully. A failed revision compares against the
    /// same predecessor when it is retried, including after a local WASM reset application.
    applied_schedule: Mutex<Option<(u64, ClusterSchedule)>>,
}

impl RuntimeAdmission {
    pub(in crate::application) fn new() -> Self {
        Self {
            committed_log_index: OnceLock::new(),
            attempt: Mutex::new(()),
            installation: Mutex::new(()),
            applied_schedule: Mutex::new(None),
        }
    }

    pub(in crate::application) async fn apply_planned_cluster_state(
        &self,
        runtime: &Runtime,
        local_node_id: &ClusterNodeName,
        state: &ConsensusRuntimeState,
    ) -> error_stack::Result<(), RuntimeError> {
        let mut applied = self.applied_schedule.lock().await;
        if applied
            .as_ref()
            .is_some_and(|(revision, _)| state.revision <= *revision)
        {
            return Ok(());
        }
        let planned = PlannedClusterRevision::between(
            applied.as_ref().map(|(_, schedule)| schedule),
            &state.schedule,
        )
        .map_err(|error| {
            Report::new(RuntimeError::BuildDomainExecution {
                domain: "cluster".to_string(),
                reason: format!("failed to plan committed schedule revision: {error:#}"),
            })
        })?;
        runtime
            .apply_planned_cluster_state(
                local_node_id,
                state.revision,
                &state.domains,
                &state.domain_clock_authorities,
                planned,
            )
            .await
            .map_err(Report::new)?;
        *applied = Some((state.revision, state.schedule.clone()));
        Ok(())
    }

    /// Serialize each local runtime installation or activation decision.
    pub(in crate::application) async fn begin_installation(
        &self,
        shutdown: &CancellationToken,
    ) -> Option<MutexGuard<'_, ()>> {
        let installation = self.installation.lock();
        tokio::pin!(installation);
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => None,
            installation = &mut installation => Some(installation),
        }
    }

    /// Return state captured after admission, retrying independently of replicated value changes.
    pub(in crate::application) async fn runtime_state(
        &self,
        consensus: &Observer,
        shutdown: &CancellationToken,
    ) -> Option<ConsensusRuntimeState> {
        if shutdown.is_cancelled() {
            return None;
        }
        if self.committed_log_index.get().is_some() {
            return Some(consensus.current_runtime_state().await);
        }

        let attempt = self.attempt.lock();
        tokio::pin!(attempt);
        let _attempt = tokio::select! {
            biased;
            _ = shutdown.cancelled() => return None,
            attempt = &mut attempt => attempt,
        };
        if shutdown.is_cancelled() {
            return None;
        }
        if self.committed_log_index.get().is_some() {
            return Some(consensus.current_runtime_state().await);
        }

        let mut reported_wait = false;
        loop {
            tokio::task::consume_budget().await;
            let admission = tokio::select! {
                biased;
                _ = shutdown.cancelled() => return None,
                admission = consensus.admitted_runtime_state() => admission,
            };
            match admission {
                Ok(admission) => {
                    let committed_log_index = admission.committed_log_index();
                    self.committed_log_index.set(committed_log_index).assured(
                        "the serialized admission attempt rechecked the unset proof after locking",
                    );
                    info!(
                        committed_log_index,
                        "runtime execution admitted after linearizable consensus catch-up"
                    );
                    return Some(admission.into_runtime_state());
                }
                Err(error) => {
                    if !reported_wait {
                        warn!(
                            error = %error,
                            "runtime execution is waiting for linearizable consensus catch-up"
                        );
                        reported_wait = true;
                    }
                    tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => return None,
                        _ = tokio::time::sleep(ADMISSION_RETRY_INTERVAL) => {}
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use nervix_models::{
        CreateRelay, DomainSchedule, Model, RelayBranching, ScheduledNode, SchemaFingerprint,
    };
    use nonzero_ext::nonzero;

    use super::*;

    #[tokio::test]
    async fn stale_state_keeps_the_last_applied_schedule_as_the_next_predecessor() {
        let admission = RuntimeAdmission::new();
        let runtime = Runtime::new();
        let local = ClusterNodeName::parse("node-1").assured("the fixture node name is valid");
        let current = ConsensusRuntimeState {
            revision: 2,
            schedule: ClusterSchedule::default(),
            domains: BTreeMap::new(),
            domain_clock_authorities: BTreeMap::new(),
        };
        admission
            .apply_planned_cluster_state(&runtime, &local, &current)
            .await
            .assured("the empty current revision applies");

        let stale = ConsensusRuntimeState {
            revision: 1,
            schedule: ClusterSchedule::from_iter([DomainSchedule::new(
                nervix_models::DomainName::parse("testing")
                    .assured("the fixture domain name is valid"),
                vec![ScheduledNode::new(
                    Model::Relay(CreateRelay {
                        name: nervix_models::RelayName::parse("events")
                            .assured("the fixture relay name is valid"),
                        schema: nervix_models::SchemaName::parse("missing_schema")
                            .assured("the fixture schema name is valid"),
                        buffer: nonzero!(2usize),
                        branching: RelayBranching::unbranched(),
                        materialized_state: None,
                    }),
                    SchemaFingerprint::from_digest([1; 32]),
                )],
                Vec::new(),
            )]),
            domains: BTreeMap::new(),
            domain_clock_authorities: BTreeMap::new(),
        };
        admission
            .apply_planned_cluster_state(&runtime, &local, &stale)
            .await
            .assured("a stale state does not plan or replace its predecessor");
        let applied = admission.applied_schedule.lock().await;
        assert_eq!(applied.as_ref().map(|(revision, _)| *revision), Some(2));
    }
}
