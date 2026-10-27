//! Durable administrative changes of the cluster's acceptance policy.
//!
//! Layer: edges.
//! - **Owns.** Validating captured transitions and awaiting their usable runtime publication.
//! - **Depends on.** Consensus command progress, pure registry decisions and runtime readiness.
//! - **Must not know.** Parsing or wire framing.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns session administration of cluster policy")
)]

use nervix_consensus::{CommandExecution, ConsensusConflict};
use nervix_models::{
    ClusterParameterCatchUp, ClusterParameterPublicationError, ClusterParameterRecord,
    ClusterParameterUpdates, ClusterParameterValues,
};
use thiserror::Error;

/// A policy-dependent request cannot use a record before its live authority is established.
#[derive(Debug, Error)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "refuses requests until live policy is known and caught up"
    )
)]
pub(in crate::application) enum ClusterParameterAdmissionError {
    #[error("the node is waiting for a live cluster parameter publication")]
    AwaitingPublication,
    #[error("the node is catching up with its live cluster parameter publication")]
    AwaitingCatchUp,
    #[error("the node is completing initialization with its live cluster parameters")]
    AwaitingInitialization,
    #[error(transparent)]
    Publication(#[from] ClusterParameterPublicationError),
}

pub(in crate::application) async fn announce_parameter_installation(
    events: &super::session_service::SessionEvents,
    cluster: &crate::cluster::ClusterHandle,
    consensus: &nervix_consensus::Observer,
    announced: &mut Option<std::num::NonZeroU64>,
) {
    let Some(node) = cluster.live_node(consensus.local_node_id()).await else {
        return;
    };
    let Some(publication) = node.parameters else {
        return;
    };
    let Some(record) = publication.running() else {
        return;
    };
    if announced.is_some_and(|revision| revision >= record.revision) {
        return;
    }
    tracing::info!(revision = %record.revision, "cluster parameters committed and installed");
    events.relay_info(format!(
        "cluster parameters committed at revision {}",
        record.revision
    ));
    *announced = Some(record.revision);
}

use super::{
    command_result::CommandResult,
    model_mutation::{command_error, command_ok},
    session_service::SessionServiceImpl,
};

impl SessionServiceImpl {
    pub(in crate::application) fn caught_up_cluster_parameters(
        &self,
    ) -> Result<ClusterParameterValues, ClusterParameterAdmissionError> {
        let construction = self
            .inner
            .runtime
            .construction_parameters()
            .ok_or(ClusterParameterAdmissionError::AwaitingPublication)?;
        let applied = self
            .inner
            .consensus
            .cluster_parameters()
            .ok_or(ClusterParameterAdmissionError::AwaitingCatchUp)?;
        match construction.catch_up(applied)? {
            ClusterParameterCatchUp::Pending => {
                Err(ClusterParameterAdmissionError::AwaitingCatchUp)
            }
            ClusterParameterCatchUp::Ready => Ok(applied.values),
        }
    }

    pub(in crate::application) fn available_cluster_parameters(
        &self,
    ) -> Result<ClusterParameterValues, ClusterParameterAdmissionError> {
        let parameters = self.caught_up_cluster_parameters()?;
        if !self.inner.initialization.is_ready() {
            return Err(ClusterParameterAdmissionError::AwaitingInitialization);
        }
        Ok(parameters)
    }

    pub(in crate::application) fn cluster_parameters(
        &self,
    ) -> nervix_models::ClusterParameterValues {
        use meticulous::ResultExt as _;
        self.caught_up_cluster_parameters()
            .assured("policy-dependent work starts only after live parameter initialization")
    }

    pub(in crate::application) fn command_execution_policy(
        &self,
    ) -> super::command_execution::CommandExecutionPolicy {
        use meticulous::ResultExt as _;
        let parameters = self.cluster_parameters();
        super::command_execution::CommandExecutionPolicy::resolve(
            Some(parameters.command_retry_validity()),
            Some(
                usize::try_from(parameters.command_execution_capacity())
                    .assured("the committed count fits a supported target"),
            ),
        )
        .assured("committed parameters have passed their definitions' validation")
    }

    pub(in crate::application) async fn alter_cluster_parameters(
        &self,
        execution: &CommandExecution,
        changes: &ClusterParameterUpdates,
        applied: Option<ClusterParameterRecord>,
    ) -> CommandResult {
        if applied.is_none() {
            loop {
                nervix_primitives::task::consume_budget().await;
                if self.inner.drain_support_shutdown.is_cancelled() {
                    return command_error(
                        "node stopped while applying cluster parameters".to_string(),
                    );
                }
                let Some(inputs) = self.inner.consensus.cluster_parameter_planning_inputs() else {
                    return command_error(ConsensusConflict::ClusterParametersUnseeded.to_string());
                };
                let next = match inputs.current.values.changed(changes) {
                    Ok(next) => next,
                    Err(error) => return command_error(format!("{error:#}")),
                };
                if let Err(error) =
                    crate::registry::validate_resource_parameters(&inputs.resources, &next)
                {
                    return command_error(format!("{error:#}"));
                }
                for (domain, schedule) in &inputs.schedule {
                    if let Err(error) = crate::registry::validate_model_parameters(
                        domain,
                        schedule.nodes.values().map(|node| node.config.as_ref()),
                        &next,
                    ) {
                        return command_error(format!("{error:#}"));
                    }
                }
                match self
                    .inner
                    .consensus
                    .alter_cluster_parameters(execution.reference.clone(), inputs)
                    .await
                {
                    Ok(()) => break,
                    Err(error)
                        if matches!(
                            error.downcast_ref::<ConsensusConflict>(),
                            Some(ConsensusConflict::ClusterParameterInputsChanged)
                        ) =>
                    {
                        continue;
                    }
                    Err(error) => {
                        return self
                            .consensus_error_response(error.current_context(), format!("{error:#}"))
                            .await;
                    }
                }
            }
        }
        let revision = self.inner.consensus.current_runtime_revision().await;
        if let Err(error) = self.apply_current_cluster_state().await {
            return command_error(format!("{error:#}"));
        }
        if let Err(error) = self.wait_for_runtime_revision(revision).await {
            return command_error(format!("{error:#}"));
        }
        command_ok("cluster parameters changed".to_string())
    }
}

pub(in crate::application) async fn describe_parameters(
    cluster: &crate::cluster::ClusterHandle,
    consensus: &nervix_consensus::Observer,
) -> Vec<String> {
    let mut lines = Vec::new();
    if let Some(record) = consensus.cluster_parameters() {
        for value in record.values.described() {
            lines.push(format!(
                "{} committed={} revision={}",
                value.parameter, value.value, record.revision
            ));
        }
    }
    for (node, state) in cluster.gossip_state().await.latest_nodes_by_id() {
        if let Some(record) = state
            .parameters
            .and_then(|publication| publication.running())
        {
            for value in record.values.described() {
                lines.push(format!(
                    "node={node} {} running={} revision={}",
                    value.parameter, value.value, record.revision
                ));
            }
        } else {
            lines.push(format!(
                "node={node} cluster parameters running=not installed"
            ));
        }
    }
    lines
}
