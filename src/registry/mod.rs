//! The decisions a cluster makes about Models, before anything runs.
//!
//! Layer: decisions.
//!
//! - **Owns.** The durable model store, validation of domains, references, schemas, branches,
//!   capabilities and execution contracts, transaction mutation planning, placement and relocation,
//!   the active execution graph, and the assignment of nodes to cluster members.
//! - **Depends on.** The vocabulary, the dataflow-graph description, the VM and the UDF host to
//!   type-check what it validates, and `fjall` for storage.
//! - **Must not know.** How a validated node runs. No Tokio task, no Arrow batch, no connector and
//!   no branch-local state belongs here, and a decision must be computable without a cluster.
//!
mod domain_activation_plan;
mod domain_state;
mod entity_gate;
mod entrypoint_plan;
mod error;
mod graph;
mod ingestor_plan;
mod mutation;
mod placement;
mod processor_plan;
mod reingestor_plan;
mod relocation;
mod resource_plan;
mod schedule_delta;
mod scheduler;
mod storage;
#[cfg(test)]
mod test_fixtures;
mod transaction;
mod validation;

pub(crate) use domain_activation_plan::{
    DomainActivationPlan, DomainActivationPlanError, PlannedCodec, PlannedCodecWireFormat,
    PlannedRelayRetention, PlannedSignalingProtocol,
};
pub(crate) use entity_gate::{
    EntityGatePlan, entity_pause_relays_for_schedule, gate_boundary,
    ownership_handoff_relays_for_schedule, scheduled_impact_coverage,
};
pub(crate) use entrypoint_plan::{
    BranchInstanceAckBoundary, EntrypointPlanError, EntrypointPlans, LoweredConstruction,
    LoweredFilter, PlannedEntryRoute, PlannedRouteBranch,
};
/// What the decisions layer exposes. Everything else this module and its submodules declare is
/// `pub(in crate::registry)` or narrower, so the control plane reaches the registry only through
/// the names below.
pub(crate) use error::RegistryError;
pub(crate) use graph::{ActiveGraph, EdgeKind};
pub(crate) use ingestor_plan::{
    EndpointIngestorStartPlan, HttpIngestorStartPlan, IngestorSpec, IngestorStartPlan,
    KafkaDomainOffsetPlacement, KafkaIngestorStartPlan, KafkaOffsetPlan, MqttIngestorStartPlan,
    NatsIngestorStartPlan, PrometheusIngestorStartPlan, PulsarIngestorStartPlan,
    RabbitMqIngestorStartPlan, RedisPubSubIngestorStartPlan, SourceStartPlan, SqsIngestorStartPlan,
    SyslogIngestorStartPlan, WebsocketsIngestorStartPlan, ZeroMqIngestorStartPlan,
};
pub(crate) use mutation::{PlannedMutations, RegistryMutation};
pub(crate) use placement::{
    PlacementEndpointPairPlan, PlacementPlan, PlacementRequireGroupPlan, PlacementRulePlan,
};
pub(crate) use processor_plan::{
    BranchedNodeSpecs, BranchedProcessorNodeSpec, BranchedProcessorOperationSpec,
    BranchedProcessorOutputSpec, BranchedProcessorOutputsSpec, BranchedProcessorSpec,
    branched_node_specs_from_scheduled_nodes,
};
#[cfg(test)]
pub(crate) use processor_plan::{PlannedModel, branched_node_specs_from_models};
pub(crate) use reingestor_plan::{ReingestorInputPlan, ReingestorPlan};
pub(crate) use relocation::{
    RelocationCoverage, RelocationMemberReason, RelocationPlanError, RelocationUnit,
};
pub(crate) use resource_plan::{
    GeneratorExecutionPlan, GeneratorRoutePlan, LookupResourcePlan, ResourceExecutionPlans,
    WasmModulePlan, udf_program,
};
pub(crate) use schedule_delta::ScheduleDelta;
#[cfg(feature = "testing")]
pub use scheduler::SchedulerMode;
pub(crate) use storage::{Registry, RuntimeChanges};
pub(crate) use transaction::{
    PlannedTransaction, PlannedTransactionStep, PlannedTransactionStepKind,
    TransactionPlanningError, TransactionPlanningSnapshot, TransactionScheduleDecision,
};
