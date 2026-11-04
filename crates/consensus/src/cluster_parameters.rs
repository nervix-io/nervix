//! Atomic publication and durable command progress for cluster parameters.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Comparing captured policy and model inputs, and publishing one parameter revision.
//! - **Depends on.** Typed vocabulary values and the replicated command and schedule records.
//! - **Must not know.** Model validation, sessions, parsing or runtime installation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns replicated parameter publication")
)]

use super::*;

/// The complete policy and model snapshot a parameter transition was validated against.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ClusterParameterPlanningInputs {
    pub current: ClusterParameterRecord,
    pub schedule: BTreeMap<DomainName, DomainSchedule>,
    pub domains: BTreeMap<DomainName, DomainState>,
    pub resources: Vec<nervix_models::ResourceVersion>,
}

fn inputs(state: &StateMachineData) -> Option<ClusterParameterPlanningInputs> {
    state
        .parameters
        .map(|current| ClusterParameterPlanningInputs {
            current,
            schedule: ClusterSchedule::from(&state.schedule).domains,
            domains: (&state.domains).into(),
            resources: state.resources.versions.values().cloned().collect(),
        })
}

pub(super) fn seed(
    state: &mut StateMachineData,
    values: ClusterParameterValues,
) -> AppliedConsensusCommand {
    if state.parameters.is_some() {
        return AppliedConsensusCommand::applied(StateMachineChanges::default());
    }
    state.parameters = Some(ClusterParameterRecord {
        revision: std::num::NonZeroU64::MIN,
        values,
    });
    AppliedConsensusCommand::applied(StateMachineChanges {
        schedule_changed: true,
        ..Default::default()
    })
}

pub(super) fn apply(
    state: &mut StateMachineData,
    reference: &nervix_models::CommandExecutionReference,
    expected: &ClusterParameterPlanningInputs,
) -> AppliedConsensusCommand {
    let Some(mut execution) = state.command_executions.get(reference).cloned() else {
        return AppliedConsensusCommand::conflict(
            ConsensusConflict::ClusterParameterExecutionMissing,
        );
    };
    let CommandExecutionState::Applying { effect, .. } = &mut execution.state else {
        return AppliedConsensusCommand::conflict(
            ConsensusConflict::ClusterParameterExecutionMissing,
        );
    };
    let CommandExecutionEffect::AlterCluster { changes, applied } = effect.as_mut() else {
        return AppliedConsensusCommand::conflict(
            ConsensusConflict::ClusterParameterExecutionMissing,
        );
    };
    // Progress and the publication are one replicated write. Recovery never reapplies an earlier
    // change over a newer command's values.
    if applied.is_some() {
        return AppliedConsensusCommand::applied(StateMachineChanges::default());
    }
    if inputs(state).as_ref() != Some(expected) {
        return AppliedConsensusCommand::conflict(ConsensusConflict::ClusterParameterInputsChanged);
    }
    let values = match expected.current.values.changed(changes) {
        Ok(values) => values,
        Err(error) => {
            return AppliedConsensusCommand::conflict(ConsensusConflict::InvalidClusterParameter {
                issue: error.current_context().clone(),
            });
        }
    };
    let record = ClusterParameterRecord {
        revision: expected
            .current
            .revision
            .checked_add(1)
            .assured("a cluster cannot apply 2^64 parameter changes"),
        values,
    };
    *applied = Some(Box::new(record));
    state.command_executions.replace(execution);
    state.parameters = Some(record);
    AppliedConsensusCommand::applied(StateMachineChanges {
        schedule_changed: true,
        ..Default::default()
    })
}

impl Observer {
    pub fn cluster_parameters(&self) -> Option<ClusterParameterRecord> {
        self.inner.store.inner.state().parameters
    }

    pub fn cluster_parameter_planning_inputs(&self) -> Option<ClusterParameterPlanningInputs> {
        inputs(&self.inner.store.inner.state())
    }
}

impl Proposer {
    pub async fn seed_cluster_parameters(
        &self,
        values: ClusterParameterValues,
    ) -> error_stack::Result<(), ConsensusError> {
        let response = self
            .inner
            .client_write(ConsensusCommand::SeedClusterParameters { values })
            .await?;
        Self::applied_response(response.data)
    }

    pub async fn alter_cluster_parameters(
        &self,
        reference: nervix_models::CommandExecutionReference,
        inputs: ClusterParameterPlanningInputs,
    ) -> error_stack::Result<(), ConsensusError> {
        let response = self
            .inner
            .client_write(ConsensusCommand::AlterClusterParameters {
                reference,
                inputs: Box::new(inputs),
            })
            .await?;
        Self::applied_response(response.data)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use nervix_models::{
        ClusterParameterChange, ClusterParameterUpdates, CommandExecutionReference, Timestamp,
    };

    use super::*;

    fn seeded() -> StateMachineData {
        let mut state = StateMachineData::default();
        assert_eq!(
            seed(&mut state, ClusterParameterValues::default()).response,
            ConsensusResponse::Applied
        );
        state
    }

    fn admit(state: &mut StateMachineData, name: &str, seconds: u64) -> CommandExecutionReference {
        let reference =
            CommandExecutionReference::parse(name).assured("the test execution reference is valid");
        let changes = ClusterParameterUpdates::try_from(vec![
            ClusterParameterChange::NodeUnavailabilityTimeout(Duration::from_secs(seconds)),
        ])
        .assured("the command assigns one key");
        let execution = CommandExecution::applying(
            reference.clone(),
            UserName::parse("operator").assured("the fixture user is valid"),
            None,
            [3; 32],
            Timestamp::from_unix_nanos(1),
            CommandExecutionEffect::AlterCluster {
                changes,
                applied: None,
            },
        );
        let policy = CommandExecutionAdmissionPolicy::at(
            Timestamp::from_unix_nanos(1),
            Duration::from_secs(60),
            16,
        );
        state.command_executions.insert_admitted(execution, &policy);
        reference
    }

    #[test]
    fn recovery_of_a_published_command_keeps_a_later_commands_values() {
        let mut state = seeded();
        let first = admit(&mut state, "first.1", 15);
        let captured = inputs(&state).assured("the cluster is seeded");
        assert_eq!(
            apply(&mut state, &first, &captured).response,
            ConsensusResponse::Applied
        );
        let first_publication = state.parameters.assured("the publication exists");
        let second = admit(&mut state, "second.1", 20);
        let latest = inputs(&state).assured("the cluster is seeded");
        assert_eq!(
            apply(&mut state, &second, &latest).response,
            ConsensusResponse::Applied
        );
        assert_eq!(
            apply(&mut state, &first, &captured).response,
            ConsensusResponse::Applied
        );
        let current = state.parameters.assured("the publication exists");
        assert_eq!(
            current.values.node_unavailability_timeout(),
            Duration::from_secs(20)
        );
        assert!(current.revision > first_publication.revision);
        assert!(
            matches!(state.command_executions.get(&first).and_then(CommandExecution::effect), Some(CommandExecutionEffect::AlterCluster { applied: Some(record), .. }) if **record == first_publication)
        );
    }

    #[test]
    fn a_changed_model_snapshot_refuses_the_entire_parameter_publication() {
        let mut state = seeded();
        let reference = admit(&mut state, "change.1", 15);
        let captured = inputs(&state).assured("the cluster is seeded");
        let before = state.parameters;
        let domain = DomainName::parse("orders").assured("the fixture domain is valid");
        state
            .schedule
            .domains
            .insert(domain.clone(), DomainSchedule::new(domain, [], Vec::new()));
        assert_eq!(
            apply(&mut state, &reference, &captured).response,
            ConsensusResponse::Conflict(ConsensusConflict::ClusterParameterInputsChanged)
        );
        assert_eq!(state.parameters, before);
        assert!(matches!(
            state
                .command_executions
                .get(&reference)
                .and_then(CommandExecution::effect),
            Some(CommandExecutionEffect::AlterCluster { applied: None, .. })
        ));
    }

    #[test]
    fn a_parameter_publication_invalidates_captured_model_inputs() {
        let mut state = seeded();
        let domain = DomainName::parse("orders").assured("the fixture domain is valid");
        let model_inputs = state.domain_planning_inputs(&domain);
        let reference = admit(&mut state, "change.1", 15);
        let captured = inputs(&state).assured("the cluster is seeded");
        assert_eq!(
            apply(&mut state, &reference, &captured).response,
            ConsensusResponse::Applied
        );
        assert_ne!(state.domain_planning_inputs(&domain), model_inputs);
    }

    #[test]
    fn seeding_is_one_publication_and_never_replaces_committed_values() {
        let mut state = seeded();
        let before = state.parameters;
        let values = ClusterParameterValues::resolve(nervix_models::ClusterParameterSeedValues {
            node_unavailability_timeout: Some(Duration::from_secs(27)),
            ..Default::default()
        })
        .assured("the proposed seed is valid");
        assert_eq!(
            seed(&mut state, values).response,
            ConsensusResponse::Applied
        );
        assert_eq!(state.parameters, before);
    }
}
