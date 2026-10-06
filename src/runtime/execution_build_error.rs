//! Why one step of building, rebuilding or changing a domain's execution on this node failed.
//!
//! Layer: data plane.
//!
//! - **Owns.** The typed step failures beneath [`RuntimeError::BuildDomainExecution`].
//! - **Depends on.** The vocabulary its variants name and the reports of the steps it keeps beneath.
//! - **Must not know.** How a caller reports or recovers from a failed build.

use std::fmt;

use super::*;

/// One step of building or changing a domain's execution on this node that failed. It sits beneath
/// [`RuntimeError::BuildDomainExecution`], which names the domain, and keeps the failure of the
/// step, when the step has one, beneath itself.
#[derive(Debug, Error)]
pub(crate) enum ExecutionBuildError {
    #[error("the domain execution is not installed while {step}")]
    ExecutionUnavailable { step: ExecutionStep },
    #[error(
        "the desired execution revision has no plan for {} '{}'",
        .node.kind.as_str(),
        .node.identifier.as_str()
    )]
    MissingDesiredNode { node: NodeRef },
    #[error(
        "the installed execution revision has no plan for {} '{}'",
        .node.kind.as_str(),
        .node.identifier.as_str()
    )]
    MissingInstalledNode { node: NodeRef },
    #[error(
        "the processor plan binder returned no plan for {} '{}'",
        .node.kind.as_str(),
        .node.identifier.as_str()
    )]
    UnboundProcessorPlan { node: NodeRef },
    #[error("relay '{relay}' has no schema on this node")]
    MissingRelaySchema { relay: RelayName },
    #[error("relay '{relay}' has no runtime boundary on this node")]
    MissingRelayBoundary { relay: RelayName },
    #[error(
        "{} '{}' reads relay '{relay}', which the domain does not activate",
        .node.kind.as_str(),
        .node.identifier.as_str()
    )]
    MissingInputRelay { node: NodeRef, relay: RelayName },
    #[error("lookup '{lookup}' decodes with codec '{codec}', which the domain did not compile")]
    MissingLookupCodec {
        lookup: LookupName,
        codec: CodecName,
    },
    #[error("materialized relay '{relay}' has no state on this node")]
    MissingMaterializedState { relay: RelayName },
    #[error("materialized relay '{relay}' lacks authoritative state access")]
    MaterializedStateAuthority { relay: RelayName },
    #[error("materialized relay '{relay}' lacks replica installation access")]
    MaterializedStateInstaller { relay: RelayName },
    #[error(
        "materialized relay '{relay}' is no longer a replica while refreshing its ownership \
         handoff snapshot"
    )]
    PromotedReplicaInstaller { relay: RelayName },
    #[error("Kafka ingestor '{ingestor}' lacks authoritative offset access")]
    KafkaOffsetAuthority { ingestor: IngestorName },
    #[error("Kafka ingestor '{ingestor}' lacks replica installation access")]
    KafkaOffsetInstaller { ingestor: IngestorName },
    #[error("the local cluster node is not known while transitioning relay '{relay}'")]
    LocalNodeUnknown { relay: RelayName },
    #[error("relay dispatch gate fence did not complete before the local node swap deadline")]
    GateFenceTimeout,
    #[error(
        "failed to activate forced recovery state for {} '{}'",
        .node.kind.as_str(),
        .node.identifier.as_str()
    )]
    ActivateForcedRecoveryState { node: NodeRef },
    #[error(
        "failed to activate prepared state for {} '{}'",
        .node.kind.as_str(),
        .node.identifier.as_str()
    )]
    ActivateHandoffState { node: NodeRef },
    #[error(
        "failed to place the runtime state of {} '{}'",
        .node.kind.as_str(),
        .node.identifier.as_str()
    )]
    PlaceState { node: NodeRef },
    #[error(
        "failed to assign the {} state of {} '{}' on this node",
        .state.as_str(),
        .node.kind.as_str(),
        .node.identifier.as_str()
    )]
    AssignState {
        node: NodeRef,
        state: RuntimeStateKind,
    },
    #[error(
        "failed to start the branch state replica of {} '{}'",
        .node.kind.as_str(),
        .node.identifier.as_str()
    )]
    StartStateReplica { node: NodeRef },
    #[error(
        "failed to purge the stored state of {} '{}'",
        .node.kind.as_str(),
        .node.identifier.as_str()
    )]
    PurgeNodeState { node: NodeRef },
    #[error("failed to purge the runtime state the schedule no longer names")]
    PurgeStaleState,
    #[error("failed to prepare the restored state of materialized relay '{relay}'")]
    PrepareMaterializedRestore { relay: RelayName },
    #[error("failed to refresh promoted materialized relay replica '{relay}'")]
    RefreshPromotedReplica { relay: RelayName },
    #[error("failed to drain relay '{relay}' before its reassignment")]
    DrainRelay { relay: RelayName },
    #[error("failed to stop the state task of relay '{relay}'")]
    StopRelayStateTask { relay: RelayName },
    #[error("failed to share the branch presence of relay '{relay}'")]
    RelayBranchPresence { relay: RelayName },
    #[error("failed to bind the domain clock")]
    BindDomainClock,
    #[error("failed to compile domain UDFs")]
    CompileUdfs,
    #[error("failed to prepare WASM processor '{processor}'")]
    PrepareWasmModule { processor: ModelName },
    #[error("failed to load the protobuf descriptors of codec '{codec}'")]
    CodecDescriptors { codec: CodecName },
    #[error("failed to load the protobuf descriptors of signaling protocol '{protocol}'")]
    SignalingDescriptors { protocol: SignalingProtocolName },
    #[error("failed to load lookup '{lookup}'")]
    LoadLookup { lookup: LookupName },
    #[error("failed to bind published processor plans")]
    BindProcessorPlans,
    #[error("failed to bind message-error routes")]
    BindMessageErrorRoutes,
    #[error("failed to start the reingestors of the domain")]
    StartReingestors,
    #[error(
        "failed to hand off the branches of {} '{}'",
        .node.kind.as_str(),
        .node.identifier.as_str()
    )]
    ProcessorHandoff { node: NodeRef },
    /// The task of an emitter the schedule swaps did not stop, and keeps running. The stop report
    /// is beneath. `drain` is the task's own description of a drain it failed, which that report
    /// holds only as a printable attachment, so a rendered chain shows it here.
    #[error(
        "failed to stop emitter '{emitter}' for its swap{}",
        DrainDescription(.drain.as_deref())
    )]
    StopEmitter {
        emitter: EmitterName,
        drain: Option<String>,
    },
    #[error("failed to reconfigure the flush policy of emitter '{emitter}'")]
    ReconfigureEmitterFlush { emitter: EmitterName },
    #[cfg(feature = "testing")]
    #[error("injected entity-level schedule apply failure")]
    InjectedSwapFailure,
}

/// The step of a schedule change that found no installed domain execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
pub(crate) enum ExecutionStep {
    #[strum(serialize = "reassigning scheduled nodes")]
    ScheduleReassignment,
    #[strum(serialize = "transitioning a relay")]
    RelayTransition,
    #[strum(serialize = "swapping an emitter")]
    EmitterSwap,
    #[strum(serialize = "swapping a reingestor")]
    ReingestorSwap,
    #[strum(serialize = "swapping a generator")]
    GeneratorSwap,
    #[strum(serialize = "swapping a processor")]
    ProcessorSwap,
    #[strum(serialize = "binding message-error routes")]
    MessageErrorBinding,
    #[strum(serialize = "binding processor plans")]
    ProcessorPlanBinding,
    #[strum(serialize = "publishing processor plans")]
    ProcessorPlanPublication,
}

/// The drain description an emitter's failed stop carries, as it reads after the stop failure.
struct DrainDescription<'a>(Option<&'a str>);

impl fmt::Display for DrainDescription<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(description) => write!(formatter, ": {description}"),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::test_fixtures::{domain, named};

    #[test]
    fn a_failed_emitter_stop_ends_with_the_drain_description_it_carries() {
        let drained = ExecutionBuildError::StopEmitter {
            emitter: named("audit"),
            drain: Some("transport drain failed".to_string()),
        };
        assert_eq!(
            drained.to_string(),
            "failed to stop emitter 'audit' for its swap: transport drain failed"
        );

        let unanswered = ExecutionBuildError::StopEmitter {
            emitter: named("audit"),
            drain: None,
        };
        assert_eq!(
            unanswered.to_string(),
            "failed to stop emitter 'audit' for its swap"
        );
    }

    #[test]
    fn a_failed_step_renders_beneath_the_domain_it_builds() {
        let gone = Report::new(ExecutionBuildError::ExecutionUnavailable {
            step: ExecutionStep::EmitterSwap,
        })
        .change_context(RuntimeError::build_domain_execution(&domain("edge")));
        assert_eq!(
            format!("{gone:#}"),
            "failed to build domain execution for 'edge': the domain execution is not installed \
             while swapping an emitter"
        );

        let missing = Report::new(ExecutionBuildError::MissingDesiredNode {
            node: NodeRef::new(ModelKind::Deduplicator, named::<ModelName>("dedupe")),
        })
        .change_context(RuntimeError::build_domain_execution(&domain("edge")));
        assert!(matches!(
            missing.current_context(),
            RuntimeError::BuildDomainExecution { domain: failed } if failed.as_str() == "edge"
        ));
        assert_eq!(
            format!("{missing:#}"),
            "failed to build domain execution for 'edge': the desired execution revision has no \
             plan for deduplicator 'dedupe'"
        );
    }
}
