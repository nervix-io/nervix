//! The wire form of the transaction impact report.
//!
//! The report is the vocabulary's own type, so this module only maps it. Sets are sent in the
//! canonical order the vocabulary keeps them in and are refused in any other order, so decoding
//! and re-encoding a report reproduces it exactly. The report's pause summary is derived from its
//! execution steps and is never sent.

use error_stack::Report;
use flatbuffers::{ForwardsUOffset, Vector, WIPOffset};
use meticulous::OptionExt as _;
use nervix_models::{
    ActivationAction, ActivationImpact, ActualExecutionStepImpact, ActualQuiescence,
    AffectedTopology, AttributedGateBoundary, AttributedImpactNode, BranchKeyFingerprint,
    BranchName, CanonicalImpactSet, ClusterNodeName, ConcreteBranchCoverage, ConfigurationImpact,
    ConfigurationTransition, DomainLifecycleAction, DomainLifecycleImpact, DomainName,
    ExecutionStepImpactReport, ExecutionStepOutcome, ForceFlushImpact, ImpactAttribution,
    ImpactDiagnostic, ImpactDiagnosticKind, ImpactEdgeKind, ImpactEffects, ImpactGateBoundary,
    ImpactNodeCoverage, ImpactPlanningBasis, ImpactReportCompleteness, ImpactTopology,
    ImpactTopologyEdge, ModelChangeAspect, OperationImpactReason, OperationImpactReport,
    OwnershipMoveImpact, PauseRequirement, PlannedExecutionStepImpact, QuiesceSubgraph,
    QuiescenceOutcome, RebuildImpact, RebuildReason, RelayName, RequestedResourceVersion,
    ResourceBindingImpact, ResourceCatalogAction, ResourceCatalogImpact, ResourceName, StatePurge,
    StateResetImpact, TransactionImpactReport, TransactionOperation, TransactionOperationNumber,
    TransactionOperationRange, TransactionPosition,
};

use crate::{
    codec::{
        Decoder, EncodedUnion, Encoder, WireDecodeError, WireEncodeError, wire_enum, wire_size,
    },
    common::{decode_node_ref, encode_node_ref},
    transaction::{decode_operation_number, encode_operation_number},
    wire,
};

wire_enum!(ALL_IMPACT_DIAGNOSTIC_KINDS: ImpactDiagnosticKind => wire::ImpactDiagnosticKind {
    Planning,
    Topology,
    Quiescence,
    Ownership,
    Activation,
    Application,
    Recovery,
});

wire_enum!(ALL_IMPACT_EDGE_KINDS: ImpactEdgeKind => wire::ImpactEdgeKind {
    ConfigurationDependency,
    Dataflow,
    MessageError,
    CorrelationTimeout,
    MaterializedState,
});

wire_enum!(ALL_RESOURCE_CATALOG_ACTIONS: ResourceCatalogAction => wire::ResourceCatalogAction {
    Create,
});

wire_enum!(ALL_DOMAIN_LIFECYCLE_ACTIONS: DomainLifecycleAction => wire::DomainLifecycleAction {
    Start,
    Stop,
});

wire_enum!(ALL_ACTIVATION_ACTIONS: ActivationAction => wire::ActivationAction {
    Activate,
    Deactivate,
    RefreshHttpsListener,
});

wire_enum!(ALL_REBUILD_REASONS: RebuildReason => wire::RebuildReason {
    Configuration,
    Ownership,
    Recovery,
});

wire_enum!(ALL_STATE_PURGES: StatePurge => wire::StatePurge {
    DeduplicatorKeyspace,
    ReordererBuffer,
    WindowAccumulator,
    CorrelationBuffer,
    InferencerWarmState,
    WasmGuestState,
});

wire_enum!(ALL_MODEL_CHANGE_ASPECTS: ModelChangeAspect => wire::ModelChangeAspect {
    RelayCapacity,
    RelaySchema,
    RelayBranching,
    RelayMaterializedState,
    ProcessorFilter,
    ProcessorInputWhere,
    ProcessorRouteConstruction,
    ProcessorRouteFlushPolicy,
    ProcessorCollectPolicy,
    ProcessorMessageErrorPolicy,
    ProcessorErrorRouteTargets,
    ProcessorInputs,
    ProcessorRoutes,
    ProcessorMode,
    ProcessorBranching,
    ProcessorMaterializedState,
    DeduplicatorKeyspace,
    DeduplicatorMaxTime,
    ReordererOrdering,
    ReordererMaxTime,
    EmitterInput,
    EmitterInputWhere,
    EmitterSink,
    EmitterClient,
    EmitterCodec,
    EmitterCollectPolicy,
    EmitterMode,
    EmitterPublishingMode,
    EmitterFlushPolicy,
    EmitterBatchPolicy,
    EmitterConstruction,
    EmitterErrorPolicies,
    EmitterMaterializedState,
    IngestorSource,
    IngestorCodec,
    IngestorTimestamp,
    IngestorFilter,
    IngestorRoutes,
    IngestorGeneralError,
    ReingestorInputs,
    ReingestorRoutes,
    ReingestorMode,
    ReingestorFilter,
    ReingestorMaterializedState,
    GeneratorMaterializedState,
    GeneratorCadence,
    GeneratorBranching,
    GeneratorRoutes,
    CorrelatorCorrelation,
    CorrelatorMatchPolicy,
    CorrelatorMaxTime,
    CorrelatorTimeoutPolicy,
    WindowBounds,
    InferencerBinding,
    InferencerTensors,
    WasmBinding,
    WasmLimits,
    WasmGlobalError,
    WasmRejectedState,
    SchemaDefinition,
    WireSchemaDefinition,
    CodecDefinition,
    ClientConfig,
    VhostHostnames,
    VhostTls,
    VhostTlsVersion,
    EndpointDefinition,
    SignalingProtocolDefinition,
    LookupDefinition,
    BranchSchema,
    BranchLifecycle,
    UdfDefinition,
    PlacementDefinition,
    EntityReplaced,
    EntityCreated,
    EntityDropped,
});

/// Encodes a complete transaction impact report.
pub(crate) fn encode_report<'fbb>(
    encoder: &mut Encoder<'fbb>,
    report: &TransactionImpactReport,
) -> Result<WIPOffset<wire::TransactionImpactReport<'fbb>>, Report<WireEncodeError>> {
    let domain = encoder.text("TransactionImpactReport.domain", report.domain().as_str())?;
    let planning_basis = encoder.fingerprint(
        "TransactionImpactReport.planning_basis",
        report.planning_basis().fingerprint(),
    )?;
    let completeness = encode_completeness(encoder, report.completeness())?;
    let operations = encoder.table_vector(
        "TransactionImpactReport.operations",
        report.operations(),
        encode_operation_report,
    )?;
    let execution_steps = encoder.table_vector(
        "TransactionImpactReport.execution_steps",
        report.execution_steps(),
        encode_execution_step,
    )?;
    Ok(wire::TransactionImpactReport::create(
        encoder.fbb(),
        &wire::TransactionImpactReportArgs {
            domain: Some(domain),
            position: wire_size(report.position().accepted_operations()),
            planning_basis: Some(planning_basis),
            completeness_type: completeness.discriminant,
            completeness: Some(completeness.value),
            operations: Some(operations),
            execution_steps: Some(execution_steps),
        },
    ))
}

/// Decodes a complete transaction impact report and checks it against the vocabulary's rules.
pub(crate) fn decode_report(
    decoder: Decoder<'_>,
    report: wire::TransactionImpactReport<'_>,
) -> Result<TransactionImpactReport, Report<WireDecodeError>> {
    let domain: DomainName = decoder.name("TransactionImpactReport.domain", report.domain())?;
    let position = decoder.size("TransactionImpactReport.position", report.position())?;
    let planning_basis = decoder.fingerprint(
        "TransactionImpactReport.planning_basis",
        report.planning_basis(),
    )?;
    let completeness = decode_completeness(
        decoder,
        "TransactionImpactReport.completeness",
        report.completeness_as_impact_incomplete(),
        report.completeness_type(),
    )?;
    let operations = decoder.table_vector(
        "TransactionImpactReport.operations",
        report.operations(),
        |operation| decode_operation_report(decoder, operation),
    )?;
    let execution_steps = decoder.table_vector(
        "TransactionImpactReport.execution_steps",
        report.execution_steps(),
        |step| decode_execution_step(decoder, step),
    )?;
    match TransactionImpactReport::new(
        domain,
        TransactionPosition::new(position),
        ImpactPlanningBasis::new(planning_basis),
        completeness,
        operations,
        execution_steps,
    ) {
        Ok(report) => Ok(report),
        Err(error) => Err(error.change_context(WireDecodeError::InvalidValue {
            field: "TransactionImpactReport",
            kind: "transaction impact report",
        })),
    }
}

fn encode_operation_report<'fbb>(
    report: &OperationImpactReport,
    encoder: &mut Encoder<'fbb>,
) -> Result<WIPOffset<wire::OperationReport<'fbb>>, Report<WireEncodeError>> {
    let operation = encode_transaction_operation(encoder, &report.operation)?;
    let execution_step = encode_operation_range(report.execution_step);
    let completeness = encode_completeness(encoder, &report.completeness)?;
    let reasons =
        encoder.table_vector("OperationReport.reasons", &report.reasons, encode_reason)?;
    let contribution = encode_effects(encoder, &report.contribution)?;
    Ok(wire::OperationReport::create(
        encoder.fbb(),
        &wire::OperationReportArgs {
            number: encode_operation_number(report.number),
            operation_type: operation.discriminant,
            operation: Some(operation.value),
            execution_step: Some(&execution_step),
            completeness_type: completeness.discriminant,
            completeness: Some(completeness.value),
            reasons: Some(reasons),
            contribution: Some(contribution),
        },
    ))
}

fn decode_operation_report(
    decoder: Decoder<'_>,
    report: wire::OperationReport<'_>,
) -> Result<OperationImpactReport, Report<WireDecodeError>> {
    let number = decode_operation_number(decoder, "OperationReport.number", report.number())?;
    let operation = decode_transaction_operation(decoder, report)?;
    let execution_step = decode_operation_range(
        decoder,
        "OperationReport.execution_step",
        report.execution_step(),
    )?;
    let completeness = decode_completeness(
        decoder,
        "OperationReport.completeness",
        report.completeness_as_impact_incomplete(),
        report.completeness_type(),
    )?;
    let reasons = decoder.table_vector("OperationReport.reasons", report.reasons(), |reason| {
        decode_reason(decoder, reason)
    })?;
    let contribution = decode_effects(decoder, report.contribution())?;
    Ok(OperationImpactReport {
        number,
        operation,
        execution_step,
        completeness,
        reasons,
        contribution,
    })
}

fn encode_execution_step<'fbb>(
    step: &ExecutionStepImpactReport,
    encoder: &mut Encoder<'fbb>,
) -> Result<WIPOffset<wire::ExecutionStepReport<'fbb>>, Report<WireEncodeError>> {
    let operations = encode_operation_range(step.operations());
    let planned = encode_planned_step(encoder, step.planned())?;
    let actual = encode_actual_step(encoder, step.actual())?;
    Ok(wire::ExecutionStepReport::create(
        encoder.fbb(),
        &wire::ExecutionStepReportArgs {
            operations: Some(&operations),
            planned: Some(planned),
            actual: Some(actual),
        },
    ))
}

fn decode_execution_step(
    decoder: Decoder<'_>,
    step: wire::ExecutionStepReport<'_>,
) -> Result<ExecutionStepImpactReport, Report<WireDecodeError>> {
    let operations =
        decode_operation_range(decoder, "ExecutionStepReport.operations", step.operations())?;
    let planned = decode_planned_step(decoder, step.planned())?;
    let actual = decode_actual_step(decoder, step.actual())?;
    Ok(ExecutionStepImpactReport::new(operations, planned, actual))
}

fn encode_planned_step<'fbb>(
    encoder: &mut Encoder<'fbb>,
    planned: &PlannedExecutionStepImpact,
) -> Result<WIPOffset<wire::PlannedStepImpact<'fbb>>, Report<WireEncodeError>> {
    let completeness = encode_completeness(encoder, &planned.completeness)?;
    let pause = encode_pause(encoder, &planned.pause)?;
    let effects = encode_effects(encoder, &planned.effects)?;
    Ok(wire::PlannedStepImpact::create(
        encoder.fbb(),
        &wire::PlannedStepImpactArgs {
            completeness_type: completeness.discriminant,
            completeness: Some(completeness.value),
            pause_type: pause.discriminant,
            pause: Some(pause.value),
            effects: Some(effects),
        },
    ))
}

fn decode_planned_step(
    decoder: Decoder<'_>,
    planned: wire::PlannedStepImpact<'_>,
) -> Result<PlannedExecutionStepImpact, Report<WireDecodeError>> {
    let completeness = decode_completeness(
        decoder,
        "PlannedStepImpact.completeness",
        planned.completeness_as_impact_incomplete(),
        planned.completeness_type(),
    )?;
    let pause = PauseTable {
        discriminant: planned.pause_type(),
        subgraph: planned.pause_as_subgraph_pause(),
        domain: planned.pause_as_domain_pause(),
    };
    let pause = pause.decode(decoder, "PlannedStepImpact.pause")?;
    let effects = decode_effects(decoder, planned.effects())?;
    Ok(PlannedExecutionStepImpact {
        completeness,
        pause,
        effects,
    })
}

fn encode_actual_step<'fbb>(
    encoder: &mut Encoder<'fbb>,
    actual: &ActualExecutionStepImpact,
) -> Result<WIPOffset<wire::ActualStepImpact<'fbb>>, Report<WireEncodeError>> {
    let outcome = encode_step_outcome(encoder, &actual.outcome)?;
    let quiescence = encoder.table_vector(
        "ActualStepImpact.quiescence",
        &actual.quiescence,
        encode_actual_quiescence,
    )?;
    let effects = encode_effects(encoder, &actual.effects)?;
    Ok(wire::ActualStepImpact::create(
        encoder.fbb(),
        &wire::ActualStepImpactArgs {
            outcome_type: outcome.discriminant,
            outcome: Some(outcome.value),
            quiescence: Some(quiescence),
            effects: Some(effects),
        },
    ))
}

fn decode_actual_step(
    decoder: Decoder<'_>,
    actual: wire::ActualStepImpact<'_>,
) -> Result<ActualExecutionStepImpact, Report<WireDecodeError>> {
    let outcome = decode_step_outcome(decoder, actual)?;
    let quiescence = decoder.table_vector(
        "ActualStepImpact.quiescence",
        actual.quiescence(),
        |quiescence| decode_actual_quiescence(decoder, quiescence),
    )?;
    let effects = decode_effects(decoder, actual.effects())?;
    Ok(ActualExecutionStepImpact {
        outcome,
        quiescence,
        effects,
    })
}

fn encode_step_outcome(
    encoder: &mut Encoder<'_>,
    outcome: &ExecutionStepOutcome,
) -> Result<EncodedUnion<wire::ExecutionStepOutcome>, Report<WireEncodeError>> {
    let union = match outcome {
        ExecutionStepOutcome::Unattempted => EncodedUnion::new(
            wire::ExecutionStepOutcome::StepUnattempted,
            wire::StepUnattempted::create(encoder.fbb(), &wire::StepUnattemptedArgs {}),
        ),
        ExecutionStepOutcome::Applying => EncodedUnion::new(
            wire::ExecutionStepOutcome::StepApplying,
            wire::StepApplying::create(encoder.fbb(), &wire::StepApplyingArgs {}),
        ),
        ExecutionStepOutcome::Applied => EncodedUnion::new(
            wire::ExecutionStepOutcome::StepApplied,
            wire::StepApplied::create(encoder.fbb(), &wire::StepAppliedArgs {}),
        ),
        ExecutionStepOutcome::Failed { diagnostic } => {
            let diagnostic = encode_diagnostic(diagnostic, encoder)?;
            let failed = wire::StepFailed::create(
                encoder.fbb(),
                &wire::StepFailedArgs {
                    diagnostic: Some(diagnostic),
                },
            );
            EncodedUnion::new(wire::ExecutionStepOutcome::StepFailed, failed)
        }
    };
    Ok(union)
}

fn decode_step_outcome(
    decoder: Decoder<'_>,
    actual: wire::ActualStepImpact<'_>,
) -> Result<ExecutionStepOutcome, Report<WireDecodeError>> {
    if let Some(failed) = actual.outcome_as_step_failed() {
        let diagnostic = decode_diagnostic(decoder, failed.diagnostic())?;
        return Ok(ExecutionStepOutcome::Failed { diagnostic });
    }
    match actual.outcome_type() {
        wire::ExecutionStepOutcome::StepUnattempted => Ok(ExecutionStepOutcome::Unattempted),
        wire::ExecutionStepOutcome::StepApplying => Ok(ExecutionStepOutcome::Applying),
        wire::ExecutionStepOutcome::StepApplied => Ok(ExecutionStepOutcome::Applied),
        undeclared => Err(decoder.unknown_union("ActualStepImpact.outcome", undeclared.0)),
    }
}

fn encode_actual_quiescence<'fbb>(
    quiescence: &ActualQuiescence,
    encoder: &mut Encoder<'fbb>,
) -> Result<WIPOffset<wire::ActualQuiescence<'fbb>>, Report<WireEncodeError>> {
    let requirement = encode_pause(encoder, &quiescence.requirement)?;
    let outcomes = encoder.table_vector(
        "ActualQuiescence.outcomes",
        &quiescence.outcomes,
        encode_quiescence_outcome,
    )?;
    Ok(wire::ActualQuiescence::create(
        encoder.fbb(),
        &wire::ActualQuiescenceArgs {
            requirement_type: requirement.discriminant,
            requirement: Some(requirement.value),
            outcomes: Some(outcomes),
        },
    ))
}

fn decode_actual_quiescence(
    decoder: Decoder<'_>,
    quiescence: wire::ActualQuiescence<'_>,
) -> Result<ActualQuiescence, Report<WireDecodeError>> {
    let requirement = PauseTable {
        discriminant: quiescence.requirement_type(),
        subgraph: quiescence.requirement_as_subgraph_pause(),
        domain: quiescence.requirement_as_domain_pause(),
    };
    let requirement = requirement.decode(decoder, "ActualQuiescence.requirement")?;
    let outcomes = decoder.table_vector(
        "ActualQuiescence.outcomes",
        quiescence.outcomes(),
        |outcome| decode_quiescence_outcome(decoder, outcome),
    )?;
    Ok(ActualQuiescence {
        requirement,
        outcomes,
    })
}

fn encode_quiescence_outcome<'fbb>(
    outcome: &QuiescenceOutcome,
    encoder: &mut Encoder<'fbb>,
) -> Result<WIPOffset<wire::QuiescenceOutcomeEntry<'fbb>>, Report<WireEncodeError>> {
    let union = match outcome {
        QuiescenceOutcome::Requested => EncodedUnion::new(
            wire::QuiescenceOutcome::QuiescenceRequested,
            wire::QuiescenceRequested::create(encoder.fbb(), &wire::QuiescenceRequestedArgs {}),
        ),
        QuiescenceOutcome::Confirmed => EncodedUnion::new(
            wire::QuiescenceOutcome::QuiescenceConfirmed,
            wire::QuiescenceConfirmed::create(encoder.fbb(), &wire::QuiescenceConfirmedArgs {}),
        ),
        QuiescenceOutcome::Failed { diagnostic } => {
            let diagnostic = encode_diagnostic(diagnostic, encoder)?;
            let failed = wire::QuiescenceFailed::create(
                encoder.fbb(),
                &wire::QuiescenceFailedArgs {
                    diagnostic: Some(diagnostic),
                },
            );
            EncodedUnion::new(wire::QuiescenceOutcome::QuiescenceFailed, failed)
        }
        QuiescenceOutcome::Uncertain { diagnostic } => {
            let diagnostic = encode_diagnostic(diagnostic, encoder)?;
            let uncertain = wire::QuiescenceUncertain::create(
                encoder.fbb(),
                &wire::QuiescenceUncertainArgs {
                    diagnostic: Some(diagnostic),
                },
            );
            EncodedUnion::new(wire::QuiescenceOutcome::QuiescenceUncertain, uncertain)
        }
        QuiescenceOutcome::Released => EncodedUnion::new(
            wire::QuiescenceOutcome::QuiescenceReleased,
            wire::QuiescenceReleased::create(encoder.fbb(), &wire::QuiescenceReleasedArgs {}),
        ),
    };
    Ok(wire::QuiescenceOutcomeEntry::create(
        encoder.fbb(),
        &wire::QuiescenceOutcomeEntryArgs {
            outcome_type: union.discriminant,
            outcome: Some(union.value),
        },
    ))
}

fn decode_quiescence_outcome(
    decoder: Decoder<'_>,
    entry: wire::QuiescenceOutcomeEntry<'_>,
) -> Result<QuiescenceOutcome, Report<WireDecodeError>> {
    if let Some(failed) = entry.outcome_as_quiescence_failed() {
        let diagnostic = decode_diagnostic(decoder, failed.diagnostic())?;
        return Ok(QuiescenceOutcome::Failed { diagnostic });
    }
    if let Some(uncertain) = entry.outcome_as_quiescence_uncertain() {
        let diagnostic = decode_diagnostic(decoder, uncertain.diagnostic())?;
        return Ok(QuiescenceOutcome::Uncertain { diagnostic });
    }
    match entry.outcome_type() {
        wire::QuiescenceOutcome::QuiescenceRequested => Ok(QuiescenceOutcome::Requested),
        wire::QuiescenceOutcome::QuiescenceConfirmed => Ok(QuiescenceOutcome::Confirmed),
        wire::QuiescenceOutcome::QuiescenceReleased => Ok(QuiescenceOutcome::Released),
        undeclared => Err(decoder.unknown_union("QuiescenceOutcomeEntry.outcome", undeclared.0)),
    }
}

fn encode_effects<'fbb>(
    encoder: &mut Encoder<'fbb>,
    effects: &ImpactEffects,
) -> Result<WIPOffset<wire::ImpactEffects<'fbb>>, Report<WireEncodeError>> {
    let changed_configuration = encoder.table_vector(
        "ImpactEffects.changed_configuration",
        effects.changed_configuration.as_slice(),
        encode_configuration_impact,
    )?;
    let topology = encode_affected_topology(encoder, &effects.topology)?;
    let ownership_moves = encoder.table_vector(
        "ImpactEffects.ownership_moves",
        effects.ownership_moves.as_slice(),
        encode_ownership_move,
    )?;
    let lifecycle = encoder.table_vector(
        "ImpactEffects.lifecycle",
        effects.lifecycle.as_slice(),
        encode_lifecycle_impact,
    )?;
    let activations = encoder.table_vector(
        "ImpactEffects.activations",
        effects.activations.as_slice(),
        encode_activation,
    )?;
    let rebuilds = encoder.table_vector(
        "ImpactEffects.rebuilds",
        effects.rebuilds.as_slice(),
        encode_rebuild,
    )?;
    let state_resets = encoder.table_vector(
        "ImpactEffects.state_resets",
        effects.state_resets.as_slice(),
        encode_state_reset,
    )?;
    let force_flushes = encoder.table_vector(
        "ImpactEffects.force_flushes",
        effects.force_flushes.as_slice(),
        encode_force_flush,
    )?;
    let resource_catalog = encoder.table_vector(
        "ImpactEffects.resource_catalog",
        effects.resource_catalog.as_slice(),
        encode_resource_catalog_impact,
    )?;
    let resource_bindings = encoder.table_vector(
        "ImpactEffects.resource_bindings",
        effects.resource_bindings.as_slice(),
        encode_resource_binding_impact,
    )?;
    Ok(wire::ImpactEffects::create(
        encoder.fbb(),
        &wire::ImpactEffectsArgs {
            changed_configuration: Some(changed_configuration),
            topology: Some(topology),
            ownership_moves: Some(ownership_moves),
            lifecycle: Some(lifecycle),
            activations: Some(activations),
            rebuilds: Some(rebuilds),
            state_resets: Some(state_resets),
            force_flushes: Some(force_flushes),
            resource_catalog: Some(resource_catalog),
            resource_bindings: Some(resource_bindings),
        },
    ))
}

fn decode_effects(
    decoder: Decoder<'_>,
    effects: wire::ImpactEffects<'_>,
) -> Result<ImpactEffects, Report<WireDecodeError>> {
    let changed_configuration = decode_set(
        decoder,
        "ImpactEffects.changed_configuration",
        effects.changed_configuration(),
        |impact| decode_configuration_impact(decoder, impact),
    )?;
    let topology = decode_affected_topology(decoder, effects.topology())?;
    let ownership_moves = decode_set(
        decoder,
        "ImpactEffects.ownership_moves",
        effects.ownership_moves(),
        |impact| decode_ownership_move(decoder, impact),
    )?;
    let lifecycle = decode_set(
        decoder,
        "ImpactEffects.lifecycle",
        effects.lifecycle(),
        |impact| decode_lifecycle_impact(decoder, impact),
    )?;
    let activations = decode_set(
        decoder,
        "ImpactEffects.activations",
        effects.activations(),
        |impact| decode_activation(decoder, impact),
    )?;
    let rebuilds = decode_set(
        decoder,
        "ImpactEffects.rebuilds",
        effects.rebuilds(),
        |impact| decode_rebuild(decoder, impact),
    )?;
    let state_resets = decode_set(
        decoder,
        "ImpactEffects.state_resets",
        effects.state_resets(),
        |impact| decode_state_reset(decoder, impact),
    )?;
    let force_flushes = decode_set(
        decoder,
        "ImpactEffects.force_flushes",
        effects.force_flushes(),
        |impact| decode_force_flush(decoder, impact),
    )?;
    let resource_catalog = decode_set(
        decoder,
        "ImpactEffects.resource_catalog",
        effects.resource_catalog(),
        |impact| decode_resource_catalog_impact(decoder, impact),
    )?;
    let resource_bindings = decode_set(
        decoder,
        "ImpactEffects.resource_bindings",
        effects.resource_bindings(),
        |impact| decode_resource_binding_impact(decoder, impact),
    )?;
    Ok(ImpactEffects {
        changed_configuration,
        topology,
        ownership_moves,
        lifecycle,
        activations,
        rebuilds,
        state_resets,
        force_flushes,
        resource_catalog,
        resource_bindings,
    })
}

fn encode_affected_topology<'fbb>(
    encoder: &mut Encoder<'fbb>,
    topology: &AffectedTopology,
) -> Result<WIPOffset<wire::AffectedTopology<'fbb>>, Report<WireEncodeError>> {
    let before = encode_topology(encoder, &topology.before)?;
    let after = encode_topology(encoder, &topology.after)?;
    Ok(wire::AffectedTopology::create(
        encoder.fbb(),
        &wire::AffectedTopologyArgs {
            before: Some(before),
            after: Some(after),
        },
    ))
}

fn decode_affected_topology(
    decoder: Decoder<'_>,
    topology: wire::AffectedTopology<'_>,
) -> Result<AffectedTopology, Report<WireDecodeError>> {
    let before = decode_topology(decoder, topology.before())?;
    let after = decode_topology(decoder, topology.after())?;
    Ok(AffectedTopology { before, after })
}

fn encode_topology<'fbb>(
    encoder: &mut Encoder<'fbb>,
    topology: &ImpactTopology,
) -> Result<WIPOffset<wire::ImpactTopology<'fbb>>, Report<WireEncodeError>> {
    let nodes = encoder.table_vector(
        "ImpactTopology.nodes",
        topology.nodes.as_slice(),
        encode_attributed_node,
    )?;
    let edges = encoder.table_vector(
        "ImpactTopology.edges",
        topology.edges.as_slice(),
        encode_edge,
    )?;
    Ok(wire::ImpactTopology::create(
        encoder.fbb(),
        &wire::ImpactTopologyArgs {
            nodes: Some(nodes),
            edges: Some(edges),
        },
    ))
}

fn decode_topology(
    decoder: Decoder<'_>,
    topology: wire::ImpactTopology<'_>,
) -> Result<ImpactTopology, Report<WireDecodeError>> {
    let nodes = decode_set(decoder, "ImpactTopology.nodes", topology.nodes(), |node| {
        decode_attributed_node(decoder, node)
    })?;
    let edges = decode_set(decoder, "ImpactTopology.edges", topology.edges(), |edge| {
        decode_edge(decoder, edge)
    })?;
    Ok(ImpactTopology { nodes, edges })
}

fn encode_edge<'fbb>(
    edge: &ImpactTopologyEdge,
    encoder: &mut Encoder<'fbb>,
) -> Result<WIPOffset<wire::TopologyEdge<'fbb>>, Report<WireEncodeError>> {
    let source = encode_node_coverage(encoder, &edge.source)?;
    let target = encode_node_coverage(encoder, &edge.target)?;
    let operations = encode_attribution(encoder, "TopologyEdge.operations", &edge.attribution)?;
    Ok(wire::TopologyEdge::create(
        encoder.fbb(),
        &wire::TopologyEdgeArgs {
            source: Some(source),
            target: Some(target),
            kind: Some(edge.kind.into()),
            operations: Some(operations),
        },
    ))
}

fn decode_edge(
    decoder: Decoder<'_>,
    edge: wire::TopologyEdge<'_>,
) -> Result<ImpactTopologyEdge, Report<WireDecodeError>> {
    let source = decode_node_coverage(decoder, edge.source())?;
    let target = decode_node_coverage(decoder, edge.target())?;
    let kind = decoder.required_enumeration("TopologyEdge.kind", edge.kind())?;
    let attribution = decode_attribution(decoder, "TopologyEdge.operations", edge.operations())?;
    Ok(ImpactTopologyEdge {
        source,
        target,
        kind,
        attribution,
    })
}

fn encode_configuration_impact<'fbb>(
    impact: &ConfigurationImpact,
    encoder: &mut Encoder<'fbb>,
) -> Result<WIPOffset<wire::ConfigurationImpact<'fbb>>, Report<WireEncodeError>> {
    let transition = match &impact.transition {
        ConfigurationTransition::Created { node } => {
            let node = encode_node_ref(encoder, node)?;
            let created = wire::ConfigurationCreated::create(
                encoder.fbb(),
                &wire::ConfigurationCreatedArgs { node: Some(node) },
            );
            EncodedUnion::new(wire::ConfigurationTransition::ConfigurationCreated, created)
        }
        ConfigurationTransition::Changed { node } => {
            let node = encode_node_ref(encoder, node)?;
            let changed = wire::ConfigurationChanged::create(
                encoder.fbb(),
                &wire::ConfigurationChangedArgs { node: Some(node) },
            );
            EncodedUnion::new(wire::ConfigurationTransition::ConfigurationChanged, changed)
        }
        ConfigurationTransition::Dropped { node } => {
            let node = encode_node_ref(encoder, node)?;
            let dropped = wire::ConfigurationDropped::create(
                encoder.fbb(),
                &wire::ConfigurationDroppedArgs { node: Some(node) },
            );
            EncodedUnion::new(wire::ConfigurationTransition::ConfigurationDropped, dropped)
        }
    };
    let operations = encode_attribution(
        encoder,
        "ConfigurationImpact.operations",
        &impact.attribution,
    )?;
    Ok(wire::ConfigurationImpact::create(
        encoder.fbb(),
        &wire::ConfigurationImpactArgs {
            transition_type: transition.discriminant,
            transition: Some(transition.value),
            operations: Some(operations),
        },
    ))
}

fn decode_configuration_impact(
    decoder: Decoder<'_>,
    impact: wire::ConfigurationImpact<'_>,
) -> Result<ConfigurationImpact, Report<WireDecodeError>> {
    let transition = if let Some(created) = impact.transition_as_configuration_created() {
        ConfigurationTransition::Created {
            node: decode_node_ref(decoder, created.node())?,
        }
    } else if let Some(changed) = impact.transition_as_configuration_changed() {
        ConfigurationTransition::Changed {
            node: decode_node_ref(decoder, changed.node())?,
        }
    } else if let Some(dropped) = impact.transition_as_configuration_dropped() {
        ConfigurationTransition::Dropped {
            node: decode_node_ref(decoder, dropped.node())?,
        }
    } else {
        return Err(
            decoder.unknown_union("ConfigurationImpact.transition", impact.transition_type().0)
        );
    };
    let attribution = decode_attribution(
        decoder,
        "ConfigurationImpact.operations",
        impact.operations(),
    )?;
    Ok(ConfigurationImpact {
        transition,
        attribution,
    })
}

fn encode_ownership_move<'fbb>(
    impact: &OwnershipMoveImpact,
    encoder: &mut Encoder<'fbb>,
) -> Result<WIPOffset<wire::OwnershipMoveImpact<'fbb>>, Report<WireEncodeError>> {
    let node = encode_node_coverage(encoder, &impact.node)?;
    let source = encoder.text("OwnershipMoveImpact.source", impact.source.as_str())?;
    let destination = encoder.text(
        "OwnershipMoveImpact.destination",
        impact.destination.as_str(),
    )?;
    let operations = encode_attribution(
        encoder,
        "OwnershipMoveImpact.operations",
        &impact.attribution,
    )?;
    Ok(wire::OwnershipMoveImpact::create(
        encoder.fbb(),
        &wire::OwnershipMoveImpactArgs {
            node: Some(node),
            source: Some(source),
            destination: Some(destination),
            operations: Some(operations),
        },
    ))
}

fn decode_ownership_move(
    decoder: Decoder<'_>,
    impact: wire::OwnershipMoveImpact<'_>,
) -> Result<OwnershipMoveImpact, Report<WireDecodeError>> {
    let node = decode_node_coverage(decoder, impact.node())?;
    let source: ClusterNodeName = decoder.name("OwnershipMoveImpact.source", impact.source())?;
    let destination: ClusterNodeName =
        decoder.name("OwnershipMoveImpact.destination", impact.destination())?;
    let attribution = decode_attribution(
        decoder,
        "OwnershipMoveImpact.operations",
        impact.operations(),
    )?;
    Ok(OwnershipMoveImpact {
        node,
        source,
        destination,
        attribution,
    })
}

fn encode_lifecycle_impact<'fbb>(
    impact: &DomainLifecycleImpact,
    encoder: &mut Encoder<'fbb>,
) -> Result<WIPOffset<wire::DomainLifecycleImpact<'fbb>>, Report<WireEncodeError>> {
    let domain = encoder.text("DomainLifecycleImpact.domain", impact.domain.as_str())?;
    let operations = encode_attribution(
        encoder,
        "DomainLifecycleImpact.operations",
        &impact.attribution,
    )?;
    Ok(wire::DomainLifecycleImpact::create(
        encoder.fbb(),
        &wire::DomainLifecycleImpactArgs {
            domain: Some(domain),
            action: Some(impact.action.into()),
            operations: Some(operations),
        },
    ))
}

fn decode_lifecycle_impact(
    decoder: Decoder<'_>,
    impact: wire::DomainLifecycleImpact<'_>,
) -> Result<DomainLifecycleImpact, Report<WireDecodeError>> {
    let domain = decoder.name("DomainLifecycleImpact.domain", impact.domain())?;
    let action = decoder.required_enumeration("DomainLifecycleImpact.action", impact.action())?;
    let attribution = decode_attribution(
        decoder,
        "DomainLifecycleImpact.operations",
        impact.operations(),
    )?;
    Ok(DomainLifecycleImpact {
        domain,
        action,
        attribution,
    })
}

fn encode_activation<'fbb>(
    impact: &ActivationImpact,
    encoder: &mut Encoder<'fbb>,
) -> Result<WIPOffset<wire::ActivationImpact<'fbb>>, Report<WireEncodeError>> {
    let node = encode_node_coverage(encoder, &impact.node)?;
    let operations =
        encode_attribution(encoder, "ActivationImpact.operations", &impact.attribution)?;
    Ok(wire::ActivationImpact::create(
        encoder.fbb(),
        &wire::ActivationImpactArgs {
            node: Some(node),
            action: Some(impact.action.into()),
            operations: Some(operations),
        },
    ))
}

fn decode_activation(
    decoder: Decoder<'_>,
    impact: wire::ActivationImpact<'_>,
) -> Result<ActivationImpact, Report<WireDecodeError>> {
    let node = decode_node_coverage(decoder, impact.node())?;
    let action = decoder.required_enumeration("ActivationImpact.action", impact.action())?;
    let attribution =
        decode_attribution(decoder, "ActivationImpact.operations", impact.operations())?;
    Ok(ActivationImpact {
        node,
        action,
        attribution,
    })
}

fn encode_rebuild<'fbb>(
    impact: &RebuildImpact,
    encoder: &mut Encoder<'fbb>,
) -> Result<WIPOffset<wire::RebuildImpact<'fbb>>, Report<WireEncodeError>> {
    let node = encode_node_coverage(encoder, &impact.node)?;
    let operations = encode_attribution(encoder, "RebuildImpact.operations", &impact.attribution)?;
    Ok(wire::RebuildImpact::create(
        encoder.fbb(),
        &wire::RebuildImpactArgs {
            node: Some(node),
            reason: Some(impact.reason.into()),
            operations: Some(operations),
        },
    ))
}

fn decode_rebuild(
    decoder: Decoder<'_>,
    impact: wire::RebuildImpact<'_>,
) -> Result<RebuildImpact, Report<WireDecodeError>> {
    let node = decode_node_coverage(decoder, impact.node())?;
    let reason = decoder.required_enumeration("RebuildImpact.reason", impact.reason())?;
    let attribution = decode_attribution(decoder, "RebuildImpact.operations", impact.operations())?;
    Ok(RebuildImpact {
        node,
        reason,
        attribution,
    })
}

fn encode_state_reset<'fbb>(
    impact: &StateResetImpact,
    encoder: &mut Encoder<'fbb>,
) -> Result<WIPOffset<wire::StateResetImpact<'fbb>>, Report<WireEncodeError>> {
    let node = encode_node_coverage(encoder, &impact.node)?;
    let operations =
        encode_attribution(encoder, "StateResetImpact.operations", &impact.attribution)?;
    Ok(wire::StateResetImpact::create(
        encoder.fbb(),
        &wire::StateResetImpactArgs {
            node: Some(node),
            state: Some(impact.state.into()),
            operations: Some(operations),
        },
    ))
}

fn decode_state_reset(
    decoder: Decoder<'_>,
    impact: wire::StateResetImpact<'_>,
) -> Result<StateResetImpact, Report<WireDecodeError>> {
    let node = decode_node_coverage(decoder, impact.node())?;
    let state = decoder.required_enumeration("StateResetImpact.state", impact.state())?;
    let attribution =
        decode_attribution(decoder, "StateResetImpact.operations", impact.operations())?;
    Ok(StateResetImpact {
        node,
        state,
        attribution,
    })
}

fn encode_force_flush<'fbb>(
    impact: &ForceFlushImpact,
    encoder: &mut Encoder<'fbb>,
) -> Result<WIPOffset<wire::ForceFlushImpact<'fbb>>, Report<WireEncodeError>> {
    let node = encode_node_coverage(encoder, &impact.node)?;
    let operations =
        encode_attribution(encoder, "ForceFlushImpact.operations", &impact.attribution)?;
    Ok(wire::ForceFlushImpact::create(
        encoder.fbb(),
        &wire::ForceFlushImpactArgs {
            node: Some(node),
            operations: Some(operations),
        },
    ))
}

fn decode_force_flush(
    decoder: Decoder<'_>,
    impact: wire::ForceFlushImpact<'_>,
) -> Result<ForceFlushImpact, Report<WireDecodeError>> {
    let node = decode_node_coverage(decoder, impact.node())?;
    let attribution =
        decode_attribution(decoder, "ForceFlushImpact.operations", impact.operations())?;
    Ok(ForceFlushImpact { node, attribution })
}

fn encode_resource_catalog_impact<'fbb>(
    impact: &ResourceCatalogImpact,
    encoder: &mut Encoder<'fbb>,
) -> Result<WIPOffset<wire::ResourceCatalogImpact<'fbb>>, Report<WireEncodeError>> {
    let resource = encoder.text("ResourceCatalogImpact.resource", impact.resource.as_str())?;
    let operations = encode_attribution(
        encoder,
        "ResourceCatalogImpact.operations",
        &impact.attribution,
    )?;
    Ok(wire::ResourceCatalogImpact::create(
        encoder.fbb(),
        &wire::ResourceCatalogImpactArgs {
            resource: Some(resource),
            action: Some(impact.action.into()),
            operations: Some(operations),
        },
    ))
}

fn decode_resource_catalog_impact(
    decoder: Decoder<'_>,
    impact: wire::ResourceCatalogImpact<'_>,
) -> Result<ResourceCatalogImpact, Report<WireDecodeError>> {
    let resource: ResourceName =
        decoder.name("ResourceCatalogImpact.resource", impact.resource())?;
    let action = decoder.required_enumeration("ResourceCatalogImpact.action", impact.action())?;
    let attribution = decode_attribution(
        decoder,
        "ResourceCatalogImpact.operations",
        impact.operations(),
    )?;
    Ok(ResourceCatalogImpact {
        resource,
        action,
        attribution,
    })
}

fn encode_resource_binding_impact<'fbb>(
    impact: &ResourceBindingImpact,
    encoder: &mut Encoder<'fbb>,
) -> Result<WIPOffset<wire::ResourceBindingImpact<'fbb>>, Report<WireEncodeError>> {
    let node = encode_node_ref(encoder, &impact.node)?;
    let resource = encoder.text("ResourceBindingImpact.resource", impact.resource.as_str())?;
    let requested = match impact.requested {
        RequestedResourceVersion::Number(version) => EncodedUnion::new(
            wire::RequestedResourceVersion::ResourceVersionNumber,
            wire::ResourceVersionNumber::create(
                encoder.fbb(),
                &wire::ResourceVersionNumberArgs { version },
            ),
        ),
        RequestedResourceVersion::Latest => EncodedUnion::new(
            wire::RequestedResourceVersion::LatestResourceVersion,
            wire::LatestResourceVersion::create(encoder.fbb(), &wire::LatestResourceVersionArgs {}),
        ),
    };
    let operations = encode_attribution(
        encoder,
        "ResourceBindingImpact.operations",
        &impact.attribution,
    )?;
    Ok(wire::ResourceBindingImpact::create(
        encoder.fbb(),
        &wire::ResourceBindingImpactArgs {
            node: Some(node),
            resource: Some(resource),
            requested_type: requested.discriminant,
            requested: Some(requested.value),
            version: impact.version,
            operations: Some(operations),
        },
    ))
}

fn decode_resource_binding_impact(
    decoder: Decoder<'_>,
    impact: wire::ResourceBindingImpact<'_>,
) -> Result<ResourceBindingImpact, Report<WireDecodeError>> {
    let node = decode_node_ref(decoder, impact.node())?;
    let resource: ResourceName =
        decoder.name("ResourceBindingImpact.resource", impact.resource())?;
    let requested = match impact.requested_type() {
        wire::RequestedResourceVersion::ResourceVersionNumber => {
            let number = union_member(impact.requested_as_resource_version_number());
            RequestedResourceVersion::Number(number.version())
        }
        wire::RequestedResourceVersion::LatestResourceVersion => RequestedResourceVersion::Latest,
        undeclared => {
            return Err(decoder.unknown_union("ResourceBindingImpact.requested", undeclared.0));
        }
    };
    let attribution = decode_attribution(
        decoder,
        "ResourceBindingImpact.operations",
        impact.operations(),
    )?;
    Ok(ResourceBindingImpact {
        node,
        resource,
        requested,
        version: impact.version(),
        attribution,
    })
}

fn encode_transaction_operation(
    encoder: &mut Encoder<'_>,
    operation: &TransactionOperation,
) -> Result<EncodedUnion<wire::TransactionOperation>, Report<WireEncodeError>> {
    let union = match operation {
        TransactionOperation::CreateConfiguration { domain, node } => {
            let domain = encoder.text("CreateConfigurationOperation.domain", domain.as_str())?;
            let node = encode_node_ref(encoder, node)?;
            let operation = wire::CreateConfigurationOperation::create(
                encoder.fbb(),
                &wire::CreateConfigurationOperationArgs {
                    domain: Some(domain),
                    node: Some(node),
                },
            );
            EncodedUnion::new(
                wire::TransactionOperation::CreateConfigurationOperation,
                operation,
            )
        }
        TransactionOperation::AlterConfiguration { domain, node } => {
            let domain = encoder.text("AlterConfigurationOperation.domain", domain.as_str())?;
            let node = encode_node_ref(encoder, node)?;
            let operation = wire::AlterConfigurationOperation::create(
                encoder.fbb(),
                &wire::AlterConfigurationOperationArgs {
                    domain: Some(domain),
                    node: Some(node),
                },
            );
            EncodedUnion::new(
                wire::TransactionOperation::AlterConfigurationOperation,
                operation,
            )
        }
        TransactionOperation::DropConfiguration { domain, node } => {
            let domain = encoder.text("DropConfigurationOperation.domain", domain.as_str())?;
            let node = encode_node_ref(encoder, node)?;
            let operation = wire::DropConfigurationOperation::create(
                encoder.fbb(),
                &wire::DropConfigurationOperationArgs {
                    domain: Some(domain),
                    node: Some(node),
                },
            );
            EncodedUnion::new(
                wire::TransactionOperation::DropConfigurationOperation,
                operation,
            )
        }
        TransactionOperation::AlterDomain { domain } => {
            let domain = encoder.text("AlterDomainOperation.domain", domain.as_str())?;
            let operation = wire::AlterDomainOperation::create(
                encoder.fbb(),
                &wire::AlterDomainOperationArgs {
                    domain: Some(domain),
                },
            );
            EncodedUnion::new(wire::TransactionOperation::AlterDomainOperation, operation)
        }
        TransactionOperation::StartDomain { domain } => {
            let domain = encoder.text("StartDomainOperation.domain", domain.as_str())?;
            let operation = wire::StartDomainOperation::create(
                encoder.fbb(),
                &wire::StartDomainOperationArgs {
                    domain: Some(domain),
                },
            );
            EncodedUnion::new(wire::TransactionOperation::StartDomainOperation, operation)
        }
        TransactionOperation::StopDomain { domain } => {
            let domain = encoder.text("StopDomainOperation.domain", domain.as_str())?;
            let operation = wire::StopDomainOperation::create(
                encoder.fbb(),
                &wire::StopDomainOperationArgs {
                    domain: Some(domain),
                },
            );
            EncodedUnion::new(wire::TransactionOperation::StopDomainOperation, operation)
        }
        TransactionOperation::CreateResource { domain, resource } => {
            let domain = encoder.text("CreateResourceOperation.domain", domain.as_str())?;
            let resource = encoder.text("CreateResourceOperation.resource", resource.as_str())?;
            let operation = wire::CreateResourceOperation::create(
                encoder.fbb(),
                &wire::CreateResourceOperationArgs {
                    domain: Some(domain),
                    resource: Some(resource),
                },
            );
            EncodedUnion::new(
                wire::TransactionOperation::CreateResourceOperation,
                operation,
            )
        }
        TransactionOperation::RebindResource {
            domain,
            resource,
            requested,
            version,
        } => {
            let domain = encoder.text("RebindResourceOperation.domain", domain.as_str())?;
            let resource = encoder.text("RebindResourceOperation.resource", resource.as_str())?;
            let requested = match requested {
                RequestedResourceVersion::Number(version) => EncodedUnion::new(
                    wire::RequestedResourceVersion::ResourceVersionNumber,
                    wire::ResourceVersionNumber::create(
                        encoder.fbb(),
                        &wire::ResourceVersionNumberArgs { version: *version },
                    ),
                ),
                RequestedResourceVersion::Latest => EncodedUnion::new(
                    wire::RequestedResourceVersion::LatestResourceVersion,
                    wire::LatestResourceVersion::create(
                        encoder.fbb(),
                        &wire::LatestResourceVersionArgs {},
                    ),
                ),
            };
            let operation = wire::RebindResourceOperation::create(
                encoder.fbb(),
                &wire::RebindResourceOperationArgs {
                    domain: Some(domain),
                    resource: Some(resource),
                    requested_type: requested.discriminant,
                    requested: Some(requested.value),
                    version: *version,
                },
            );
            EncodedUnion::new(
                wire::TransactionOperation::RebindResourceOperation,
                operation,
            )
        }
        TransactionOperation::ResetWasmState { domain, processor } => {
            let domain = encoder.text("ResetWasmStateOperation.domain", domain.as_str())?;
            let processor =
                encoder.text("ResetWasmStateOperation.processor", processor.as_str())?;
            let operation = wire::ResetWasmStateOperation::create(
                encoder.fbb(),
                &wire::ResetWasmStateOperationArgs {
                    domain: Some(domain),
                    processor: Some(processor),
                },
            );
            EncodedUnion::new(
                wire::TransactionOperation::ResetWasmStateOperation,
                operation,
            )
        }
    };
    Ok(union)
}

fn decode_transaction_operation(
    decoder: Decoder<'_>,
    report: wire::OperationReport<'_>,
) -> Result<TransactionOperation, Report<WireDecodeError>> {
    if let Some(operation) = report.operation_as_create_configuration_operation() {
        return Ok(TransactionOperation::CreateConfiguration {
            domain: decoder.name("CreateConfigurationOperation.domain", operation.domain())?,
            node: decode_node_ref(decoder, operation.node())?,
        });
    }
    if let Some(operation) = report.operation_as_alter_configuration_operation() {
        return Ok(TransactionOperation::AlterConfiguration {
            domain: decoder.name("AlterConfigurationOperation.domain", operation.domain())?,
            node: decode_node_ref(decoder, operation.node())?,
        });
    }
    if let Some(operation) = report.operation_as_drop_configuration_operation() {
        return Ok(TransactionOperation::DropConfiguration {
            domain: decoder.name("DropConfigurationOperation.domain", operation.domain())?,
            node: decode_node_ref(decoder, operation.node())?,
        });
    }
    if let Some(operation) = report.operation_as_alter_domain_operation() {
        return Ok(TransactionOperation::AlterDomain {
            domain: decoder.name("AlterDomainOperation.domain", operation.domain())?,
        });
    }
    if let Some(operation) = report.operation_as_start_domain_operation() {
        return Ok(TransactionOperation::StartDomain {
            domain: decoder.name("StartDomainOperation.domain", operation.domain())?,
        });
    }
    if let Some(operation) = report.operation_as_stop_domain_operation() {
        return Ok(TransactionOperation::StopDomain {
            domain: decoder.name("StopDomainOperation.domain", operation.domain())?,
        });
    }
    if let Some(operation) = report.operation_as_create_resource_operation() {
        return Ok(TransactionOperation::CreateResource {
            domain: decoder.name("CreateResourceOperation.domain", operation.domain())?,
            resource: decoder.name("CreateResourceOperation.resource", operation.resource())?,
        });
    }
    if let Some(operation) = report.operation_as_rebind_resource_operation() {
        let requested = match operation.requested_type() {
            wire::RequestedResourceVersion::ResourceVersionNumber => {
                let number = union_member(operation.requested_as_resource_version_number());
                RequestedResourceVersion::Number(number.version())
            }
            wire::RequestedResourceVersion::LatestResourceVersion => {
                RequestedResourceVersion::Latest
            }
            undeclared => {
                return Err(
                    decoder.unknown_union("RebindResourceOperation.requested", undeclared.0)
                );
            }
        };
        return Ok(TransactionOperation::RebindResource {
            domain: decoder.name("RebindResourceOperation.domain", operation.domain())?,
            resource: decoder.name("RebindResourceOperation.resource", operation.resource())?,
            requested,
            version: operation.version(),
        });
    }
    if let Some(operation) = report.operation_as_reset_wasm_state_operation() {
        return Ok(TransactionOperation::ResetWasmState {
            domain: decoder.name("ResetWasmStateOperation.domain", operation.domain())?,
            processor: decoder.name("ResetWasmStateOperation.processor", operation.processor())?,
        });
    }
    Err(decoder.unknown_union("OperationReport.operation", report.operation_type().0))
}

fn encode_reason<'fbb>(
    reason: &OperationImpactReason,
    encoder: &mut Encoder<'fbb>,
) -> Result<WIPOffset<wire::OperationReason<'fbb>>, Report<WireEncodeError>> {
    let union = match reason {
        OperationImpactReason::Configuration { node, aspect } => {
            let node = encode_node_ref(encoder, node)?;
            let configuration = wire::ConfigurationReason::create(
                encoder.fbb(),
                &wire::ConfigurationReasonArgs {
                    node: Some(node),
                    aspect: Some((*aspect).into()),
                },
            );
            EncodedUnion::new(
                wire::OperationImpactReason::ConfigurationReason,
                configuration,
            )
        }
        OperationImpactReason::DomainPlacement => EncodedUnion::new(
            wire::OperationImpactReason::DomainPlacementReason,
            wire::DomainPlacementReason::create(encoder.fbb(), &wire::DomainPlacementReasonArgs {}),
        ),
        OperationImpactReason::DomainStart => EncodedUnion::new(
            wire::OperationImpactReason::DomainStartReason,
            wire::DomainStartReason::create(encoder.fbb(), &wire::DomainStartReasonArgs {}),
        ),
        OperationImpactReason::DomainStop => EncodedUnion::new(
            wire::OperationImpactReason::DomainStopReason,
            wire::DomainStopReason::create(encoder.fbb(), &wire::DomainStopReasonArgs {}),
        ),
        OperationImpactReason::ResourceCatalog { resource } => {
            let resource = encoder.text("ResourceCatalogReason.resource", resource.as_str())?;
            let catalog = wire::ResourceCatalogReason::create(
                encoder.fbb(),
                &wire::ResourceCatalogReasonArgs {
                    resource: Some(resource),
                },
            );
            EncodedUnion::new(wire::OperationImpactReason::ResourceCatalogReason, catalog)
        }
        OperationImpactReason::ResourceRebinding {
            node,
            resource,
            from_version,
            to_version,
        } => {
            let node = encode_node_ref(encoder, node)?;
            let resource = encoder.text("ResourceRebindingReason.resource", resource.as_str())?;
            let rebinding = wire::ResourceRebindingReason::create(
                encoder.fbb(),
                &wire::ResourceRebindingReasonArgs {
                    node: Some(node),
                    resource: Some(resource),
                    previous_version: *from_version,
                    target_version: *to_version,
                },
            );
            EncodedUnion::new(
                wire::OperationImpactReason::ResourceRebindingReason,
                rebinding,
            )
        }
        OperationImpactReason::WasmStateReset { node } => {
            let node = encode_node_ref(encoder, node)?;
            let reset = wire::WasmStateResetReason::create(
                encoder.fbb(),
                &wire::WasmStateResetReasonArgs { node: Some(node) },
            );
            EncodedUnion::new(wire::OperationImpactReason::WasmStateResetReason, reset)
        }
    };
    Ok(wire::OperationReason::create(
        encoder.fbb(),
        &wire::OperationReasonArgs {
            reason_type: union.discriminant,
            reason: Some(union.value),
        },
    ))
}

fn decode_reason(
    decoder: Decoder<'_>,
    reason: wire::OperationReason<'_>,
) -> Result<OperationImpactReason, Report<WireDecodeError>> {
    if let Some(configuration) = reason.reason_as_configuration_reason() {
        return Ok(OperationImpactReason::Configuration {
            node: decode_node_ref(decoder, configuration.node())?,
            aspect: decoder
                .required_enumeration("ConfigurationReason.aspect", configuration.aspect())?,
        });
    }
    if let Some(catalog) = reason.reason_as_resource_catalog_reason() {
        return Ok(OperationImpactReason::ResourceCatalog {
            resource: decoder.name("ResourceCatalogReason.resource", catalog.resource())?,
        });
    }
    if let Some(rebinding) = reason.reason_as_resource_rebinding_reason() {
        return Ok(OperationImpactReason::ResourceRebinding {
            node: decode_node_ref(decoder, rebinding.node())?,
            resource: decoder.name("ResourceRebindingReason.resource", rebinding.resource())?,
            from_version: rebinding.previous_version(),
            to_version: rebinding.target_version(),
        });
    }
    if let Some(reset) = reason.reason_as_wasm_state_reset_reason() {
        return Ok(OperationImpactReason::WasmStateReset {
            node: decode_node_ref(decoder, reset.node())?,
        });
    }
    match reason.reason_type() {
        wire::OperationImpactReason::DomainPlacementReason => {
            Ok(OperationImpactReason::DomainPlacement)
        }
        wire::OperationImpactReason::DomainStartReason => Ok(OperationImpactReason::DomainStart),
        wire::OperationImpactReason::DomainStopReason => Ok(OperationImpactReason::DomainStop),
        undeclared => Err(decoder.unknown_union("OperationReason.reason", undeclared.0)),
    }
}

/// The union slots a pause requirement is read from, which differ between the tables that hold
/// one.
struct PauseTable<'a> {
    discriminant: wire::PauseRequirement,
    subgraph: Option<wire::SubgraphPause<'a>>,
    domain: Option<wire::DomainPause<'a>>,
}

fn encode_pause(
    encoder: &mut Encoder<'_>,
    pause: &PauseRequirement,
) -> Result<EncodedUnion<wire::PauseRequirement>, Report<WireEncodeError>> {
    let union = match pause {
        PauseRequirement::NoPause => EncodedUnion::new(
            wire::PauseRequirement::NoPause,
            wire::NoPause::create(encoder.fbb(), &wire::NoPauseArgs {}),
        ),
        PauseRequirement::Subgraph { scope } => {
            let scope = encode_subgraph(encoder, scope)?;
            let subgraph = wire::SubgraphPause::create(
                encoder.fbb(),
                &wire::SubgraphPauseArgs { scope: Some(scope) },
            );
            EncodedUnion::new(wire::PauseRequirement::SubgraphPause, subgraph)
        }
        PauseRequirement::Domain { domain } => {
            let domain = encoder.text("DomainPause.domain", domain.as_str())?;
            let domain = wire::DomainPause::create(
                encoder.fbb(),
                &wire::DomainPauseArgs {
                    domain: Some(domain),
                },
            );
            EncodedUnion::new(wire::PauseRequirement::DomainPause, domain)
        }
    };
    Ok(union)
}

impl PauseTable<'_> {
    fn decode(
        self,
        decoder: Decoder<'_>,
        field: &'static str,
    ) -> Result<PauseRequirement, Report<WireDecodeError>> {
        if let Some(subgraph) = self.subgraph {
            let scope = decode_subgraph(decoder, subgraph.scope())?;
            return Ok(PauseRequirement::Subgraph { scope });
        }
        if let Some(domain) = self.domain {
            let domain = decoder.name("DomainPause.domain", domain.domain())?;
            return Ok(PauseRequirement::Domain { domain });
        }
        match self.discriminant {
            wire::PauseRequirement::NoPause => Ok(PauseRequirement::NoPause),
            undeclared => Err(decoder.unknown_union(field, undeclared.0)),
        }
    }
}

fn encode_subgraph<'fbb>(
    encoder: &mut Encoder<'fbb>,
    subgraph: &QuiesceSubgraph,
) -> Result<WIPOffset<wire::QuiesceSubgraph<'fbb>>, Report<WireEncodeError>> {
    let domain = encoder.text("QuiesceSubgraph.domain", subgraph.domain().as_str())?;
    let nodes = encoder.table_vector(
        "QuiesceSubgraph.nodes",
        subgraph.nodes(),
        encode_attributed_node,
    )?;
    let gate_boundaries = encoder.table_vector(
        "QuiesceSubgraph.gate_boundaries",
        subgraph.gate_boundaries(),
        encode_attributed_gate,
    )?;
    Ok(wire::QuiesceSubgraph::create(
        encoder.fbb(),
        &wire::QuiesceSubgraphArgs {
            domain: Some(domain),
            nodes: Some(nodes),
            gate_boundaries: Some(gate_boundaries),
        },
    ))
}

fn decode_subgraph(
    decoder: Decoder<'_>,
    subgraph: wire::QuiesceSubgraph<'_>,
) -> Result<QuiesceSubgraph, Report<WireDecodeError>> {
    let domain = decoder.name("QuiesceSubgraph.domain", subgraph.domain())?;
    let nodes = decoder.table_vector("QuiesceSubgraph.nodes", subgraph.nodes(), |node| {
        decode_attributed_node(decoder, node)
    })?;
    ensure_ascending(
        "QuiesceSubgraph.nodes",
        nodes.iter().map(|node| &node.coverage),
    )?;
    let gate_boundaries = decoder.table_vector(
        "QuiesceSubgraph.gate_boundaries",
        subgraph.gate_boundaries(),
        |gate| decode_attributed_gate(decoder, gate),
    )?;
    ensure_ascending(
        "QuiesceSubgraph.gate_boundaries",
        gate_boundaries.iter().map(|gate| &gate.boundary),
    )?;
    Ok(QuiesceSubgraph::new(domain, nodes, gate_boundaries))
}

fn encode_attributed_node<'fbb>(
    node: &AttributedImpactNode,
    encoder: &mut Encoder<'fbb>,
) -> Result<WIPOffset<wire::AttributedNode<'fbb>>, Report<WireEncodeError>> {
    let coverage = encode_node_coverage(encoder, &node.coverage)?;
    let operations = encode_attribution(encoder, "AttributedNode.operations", &node.attribution)?;
    Ok(wire::AttributedNode::create(
        encoder.fbb(),
        &wire::AttributedNodeArgs {
            coverage: Some(coverage),
            operations: Some(operations),
        },
    ))
}

fn decode_attributed_node(
    decoder: Decoder<'_>,
    node: wire::AttributedNode<'_>,
) -> Result<AttributedImpactNode, Report<WireDecodeError>> {
    let coverage = decode_node_coverage(decoder, node.coverage())?;
    let attribution = decode_attribution(decoder, "AttributedNode.operations", node.operations())?;
    Ok(AttributedImpactNode {
        coverage,
        attribution,
    })
}

fn encode_attributed_gate<'fbb>(
    gate: &AttributedGateBoundary,
    encoder: &mut Encoder<'fbb>,
) -> Result<WIPOffset<wire::AttributedGateBoundary<'fbb>>, Report<WireEncodeError>> {
    let relay = encoder.text("GateBoundary.relay", gate.boundary.relay.as_str())?;
    let branches = encode_branch_coverage(encoder, &gate.boundary.branches)?;
    let boundary = wire::GateBoundary::create(
        encoder.fbb(),
        &wire::GateBoundaryArgs {
            relay: Some(relay),
            branches_type: branches.discriminant,
            branches: Some(branches.value),
        },
    );
    let operations = encode_attribution(
        encoder,
        "AttributedGateBoundary.operations",
        &gate.attribution,
    )?;
    Ok(wire::AttributedGateBoundary::create(
        encoder.fbb(),
        &wire::AttributedGateBoundaryArgs {
            boundary: Some(boundary),
            operations: Some(operations),
        },
    ))
}

fn decode_attributed_gate(
    decoder: Decoder<'_>,
    gate: wire::AttributedGateBoundary<'_>,
) -> Result<AttributedGateBoundary, Report<WireDecodeError>> {
    let boundary = gate.boundary();
    let relay: RelayName = decoder.name("GateBoundary.relay", boundary.relay())?;
    let branches = match boundary.branches_type() {
        wire::BranchCoverage::AllBranchCoverage => BranchCoverageMember::All,
        wire::BranchCoverage::UnbranchedCoverage => BranchCoverageMember::Unbranched,
        wire::BranchCoverage::DeclaredBranchCoverage => BranchCoverageMember::Declared(
            union_member(boundary.branches_as_declared_branch_coverage()),
        ),
        wire::BranchCoverage::SelectedBranchCoverage => BranchCoverageMember::Selected(
            union_member(boundary.branches_as_selected_branch_coverage()),
        ),
        undeclared => {
            return Err(decoder.unknown_union("GateBoundary.branches", undeclared.0));
        }
    };
    let branches = branches.decode(decoder)?;
    let attribution = decode_attribution(
        decoder,
        "AttributedGateBoundary.operations",
        gate.operations(),
    )?;
    Ok(AttributedGateBoundary {
        boundary: ImpactGateBoundary { relay, branches },
        attribution,
    })
}

fn encode_node_coverage<'fbb>(
    encoder: &mut Encoder<'fbb>,
    coverage: &ImpactNodeCoverage,
) -> Result<WIPOffset<wire::NodeCoverage<'fbb>>, Report<WireEncodeError>> {
    let node = encode_node_ref(encoder, &coverage.node)?;
    let branches = match &coverage.branches {
        Some(branches) => encode_branch_coverage(encoder, branches)?,
        None => EncodedUnion::new(
            wire::NodeBranchCoverage::ConfigurationCoverage,
            wire::ConfigurationCoverage::create(encoder.fbb(), &wire::ConfigurationCoverageArgs {}),
        ),
    };
    Ok(wire::NodeCoverage::create(
        encoder.fbb(),
        &wire::NodeCoverageArgs {
            node: Some(node),
            branches_type: branches.discriminant,
            branches: Some(branches.value),
        },
    ))
}

fn decode_node_coverage(
    decoder: Decoder<'_>,
    coverage: wire::NodeCoverage<'_>,
) -> Result<ImpactNodeCoverage, Report<WireDecodeError>> {
    let node = decode_node_ref(decoder, coverage.node())?;
    let branches = match coverage.branches_type() {
        wire::NodeBranchCoverage::ConfigurationCoverage => {
            return Ok(ImpactNodeCoverage::configuration(node));
        }
        wire::NodeBranchCoverage::AllBranchCoverage => BranchCoverageMember::All,
        wire::NodeBranchCoverage::UnbranchedCoverage => BranchCoverageMember::Unbranched,
        wire::NodeBranchCoverage::DeclaredBranchCoverage => BranchCoverageMember::Declared(
            union_member(coverage.branches_as_declared_branch_coverage()),
        ),
        wire::NodeBranchCoverage::SelectedBranchCoverage => BranchCoverageMember::Selected(
            union_member(coverage.branches_as_selected_branch_coverage()),
        ),
        undeclared => {
            return Err(decoder.unknown_union("NodeCoverage.branches", undeclared.0));
        }
    };
    let branches = branches.decode(decoder)?;
    Ok(ImpactNodeCoverage::execution(node, branches))
}

/// The branch coverage a union holds, read through the accessors of the table that holds it.
enum BranchCoverageMember<'a> {
    All,
    Unbranched,
    Declared(wire::DeclaredBranchCoverage<'a>),
    Selected(wire::SelectedBranchCoverage<'a>),
}

/// The discriminants a branch coverage is stored under, which differ between the unions that
/// hold one.
trait BranchCoverageUnion {
    const ALL: Self;
    const UNBRANCHED: Self;
    const DECLARED: Self;
    const SELECTED: Self;
}

impl BranchCoverageUnion for wire::BranchCoverage {
    const ALL: Self = Self::AllBranchCoverage;
    const UNBRANCHED: Self = Self::UnbranchedCoverage;
    const DECLARED: Self = Self::DeclaredBranchCoverage;
    const SELECTED: Self = Self::SelectedBranchCoverage;
}

impl BranchCoverageUnion for wire::NodeBranchCoverage {
    const ALL: Self = Self::AllBranchCoverage;
    const UNBRANCHED: Self = Self::UnbranchedCoverage;
    const DECLARED: Self = Self::DeclaredBranchCoverage;
    const SELECTED: Self = Self::SelectedBranchCoverage;
}

/// Reads the union member a matched discriminant names.
fn union_member<T>(member: Option<T>) -> T {
    member.assured("a union's discriminant names the member its accessor reads")
}

fn encode_branch_coverage<D: BranchCoverageUnion>(
    encoder: &mut Encoder<'_>,
    coverage: &ConcreteBranchCoverage,
) -> Result<EncodedUnion<D>, Report<WireEncodeError>> {
    let union = match coverage {
        ConcreteBranchCoverage::All => EncodedUnion::new(
            D::ALL,
            wire::AllBranchCoverage::create(encoder.fbb(), &wire::AllBranchCoverageArgs {}),
        ),
        ConcreteBranchCoverage::Unbranched => EncodedUnion::new(
            D::UNBRANCHED,
            wire::UnbranchedCoverage::create(encoder.fbb(), &wire::UnbranchedCoverageArgs {}),
        ),
        ConcreteBranchCoverage::AllOfBranch { branch } => {
            let branch = encoder.text("DeclaredBranchCoverage.branch", branch.as_str())?;
            let declared = wire::DeclaredBranchCoverage::create(
                encoder.fbb(),
                &wire::DeclaredBranchCoverageArgs {
                    branch: Some(branch),
                },
            );
            EncodedUnion::new(D::DECLARED, declared)
        }
        ConcreteBranchCoverage::Selected { branch, keys } => {
            let branch = encoder.text("SelectedBranchCoverage.branch", branch.as_str())?;
            let keys = encoder.table_vector(
                "SelectedBranchCoverage.keys",
                keys.as_slice(),
                |key, encoder| {
                    encoder.fingerprint("SelectedBranchCoverage.keys", key.fingerprint())
                },
            )?;
            let selected = wire::SelectedBranchCoverage::create(
                encoder.fbb(),
                &wire::SelectedBranchCoverageArgs {
                    branch: Some(branch),
                    keys: Some(keys),
                },
            );
            EncodedUnion::new(D::SELECTED, selected)
        }
    };
    Ok(union)
}

impl BranchCoverageMember<'_> {
    fn decode(
        self,
        decoder: Decoder<'_>,
    ) -> Result<ConcreteBranchCoverage, Report<WireDecodeError>> {
        match self {
            Self::All => Ok(ConcreteBranchCoverage::All),
            Self::Unbranched => Ok(ConcreteBranchCoverage::Unbranched),
            Self::Declared(declared) => {
                let branch: BranchName =
                    decoder.name("DeclaredBranchCoverage.branch", declared.branch())?;
                Ok(ConcreteBranchCoverage::AllOfBranch { branch })
            }
            Self::Selected(selected) => {
                let branch: BranchName =
                    decoder.name("SelectedBranchCoverage.branch", selected.branch())?;
                let fingerprints = decoder.table_vector(
                    "SelectedBranchCoverage.keys",
                    selected.keys(),
                    |key| {
                        let fingerprint =
                            decoder.fingerprint("SelectedBranchCoverage.keys", key)?;
                        Ok(BranchKeyFingerprint::new(fingerprint))
                    },
                )?;
                ensure_ascending("SelectedBranchCoverage.keys", fingerprints.iter())?;
                match ConcreteBranchCoverage::selected(branch, fingerprints) {
                    Ok(coverage) => Ok(coverage),
                    Err(error) => Err(error.change_context(WireDecodeError::EmptyCollection {
                        field: "SelectedBranchCoverage.keys",
                    })),
                }
            }
        }
    }
}

fn encode_completeness(
    encoder: &mut Encoder<'_>,
    completeness: &ImpactReportCompleteness,
) -> Result<EncodedUnion<wire::ImpactCompleteness>, Report<WireEncodeError>> {
    let union = match completeness {
        ImpactReportCompleteness::Complete => EncodedUnion::new(
            wire::ImpactCompleteness::ImpactComplete,
            wire::ImpactComplete::create(encoder.fbb(), &wire::ImpactCompleteArgs {}),
        ),
        ImpactReportCompleteness::Incomplete { diagnostics } => {
            let diagnostics = encoder.table_vector(
                "ImpactIncomplete.diagnostics",
                diagnostics,
                encode_diagnostic,
            )?;
            let incomplete = wire::ImpactIncomplete::create(
                encoder.fbb(),
                &wire::ImpactIncompleteArgs {
                    diagnostics: Some(diagnostics),
                },
            );
            EncodedUnion::new(wire::ImpactCompleteness::ImpactIncomplete, incomplete)
        }
    };
    Ok(union)
}

fn decode_completeness(
    decoder: Decoder<'_>,
    field: &'static str,
    incomplete: Option<wire::ImpactIncomplete<'_>>,
    discriminant: wire::ImpactCompleteness,
) -> Result<ImpactReportCompleteness, Report<WireDecodeError>> {
    if let Some(incomplete) = incomplete {
        let diagnostics = decoder.table_vector(
            "ImpactIncomplete.diagnostics",
            incomplete.diagnostics(),
            |diagnostic| decode_diagnostic(decoder, diagnostic),
        )?;
        return match ImpactReportCompleteness::incomplete(diagnostics) {
            Ok(completeness) => Ok(completeness),
            Err(error) => Err(error.change_context(WireDecodeError::EmptyCollection {
                field: "ImpactIncomplete.diagnostics",
            })),
        };
    }
    match discriminant {
        wire::ImpactCompleteness::ImpactComplete => Ok(ImpactReportCompleteness::Complete),
        undeclared => Err(decoder.unknown_union(field, undeclared.0)),
    }
}

fn encode_diagnostic<'fbb>(
    diagnostic: &ImpactDiagnostic,
    encoder: &mut Encoder<'fbb>,
) -> Result<WIPOffset<wire::ImpactDiagnostic<'fbb>>, Report<WireEncodeError>> {
    let message = encoder.text("ImpactDiagnostic.message", &diagnostic.message)?;
    let operation = diagnostic.operation.map(encode_operation_number);
    Ok(wire::ImpactDiagnostic::create(
        encoder.fbb(),
        &wire::ImpactDiagnosticArgs {
            kind: Some(diagnostic.kind.into()),
            operation,
            message: Some(message),
        },
    ))
}

fn decode_diagnostic(
    decoder: Decoder<'_>,
    diagnostic: wire::ImpactDiagnostic<'_>,
) -> Result<ImpactDiagnostic, Report<WireDecodeError>> {
    let kind = decoder.required_enumeration("ImpactDiagnostic.kind", diagnostic.kind())?;
    let operation = match diagnostic.operation() {
        Some(operation) => Some(decode_operation_number(
            decoder,
            "ImpactDiagnostic.operation",
            operation,
        )?),
        None => None,
    };
    let message = decoder.text("ImpactDiagnostic.message", diagnostic.message())?;
    Ok(ImpactDiagnostic {
        kind,
        operation,
        message,
    })
}

fn encode_attribution<'fbb>(
    encoder: &mut Encoder<'fbb>,
    field: &'static str,
    attribution: &ImpactAttribution,
) -> Result<WIPOffset<Vector<'fbb, u64>>, Report<WireEncodeError>> {
    let operations = attribution
        .operations()
        .iter()
        .map(|operation| encode_operation_number(*operation))
        .collect::<Vec<_>>();
    encoder.scalars(field, &operations)
}

fn decode_attribution(
    decoder: Decoder<'_>,
    field: &'static str,
    operations: Vector<'_, u64>,
) -> Result<ImpactAttribution, Report<WireDecodeError>> {
    decoder.entries(field, operations.len())?;
    let mut numbers = Vec::with_capacity(operations.len());
    for operation in operations.iter() {
        numbers.push(decode_operation_number(decoder, field, operation)?);
    }
    ensure_ascending(field, numbers.iter())?;
    match ImpactAttribution::new(numbers) {
        Ok(attribution) => Ok(attribution),
        Err(error) => Err(error.change_context(WireDecodeError::EmptyCollection { field })),
    }
}

fn encode_operation_range(range: TransactionOperationRange) -> wire::OperationRange {
    wire::OperationRange::new(
        encode_operation_number(range.first()),
        encode_operation_number(range.last()),
    )
}

fn decode_operation_range(
    decoder: Decoder<'_>,
    field: &'static str,
    range: &wire::OperationRange,
) -> Result<TransactionOperationRange, Report<WireDecodeError>> {
    let first: TransactionOperationNumber = decode_operation_number(decoder, field, range.first())?;
    let last = decode_operation_number(decoder, field, range.last())?;
    match TransactionOperationRange::new(first, last) {
        Ok(range) => Ok(range),
        Err(error) => Err(error.change_context(WireDecodeError::InvalidValue {
            field,
            kind: "operation range",
        })),
    }
}

/// Decodes a vector that must hold a canonical set.
fn decode_set<'a, W, T>(
    decoder: Decoder<'_>,
    field: &'static str,
    tables: Vector<'a, ForwardsUOffset<W>>,
    decode: impl FnMut(W) -> Result<T, Report<WireDecodeError>>,
) -> Result<CanonicalImpactSet<T>, Report<WireDecodeError>>
where
    W: flatbuffers::Follow<'a, Inner = W> + 'a,
    T: Ord,
{
    let values = decoder.table_vector(field, tables, decode)?;
    ensure_ascending(field, values.iter())?;
    Ok(CanonicalImpactSet::new(values))
}

/// Refuses keys that are not strictly ascending, which is how a canonical set is sent.
fn ensure_ascending<'v, T>(
    field: &'static str,
    keys: impl Iterator<Item = &'v T>,
) -> Result<(), Report<WireDecodeError>>
where
    T: Ord + 'v,
{
    let mut previous = None;
    for key in keys {
        if let Some(previous) = previous
            && previous >= key
        {
            return Err(Report::new(WireDecodeError::NonCanonicalSet { field }));
        }
        previous = Some(key);
    }
    Ok(())
}
