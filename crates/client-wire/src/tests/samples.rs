//! Representative values of every message family, at the edges of their ranges.

use std::num::NonZeroU32;

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    ActivationAction, ActivationImpact, ActualExecutionStepImpact, ActualQuiescence,
    AffectedTopology, AttributedGateBoundary, AttributedImpactNode, BranchKeyFingerprint,
    CanonicalImpactSet, ConcreteBranchCoverage, ConfigurationImpact, ConfigurationTransition,
    DomainLifecycleAction, DomainLifecycleImpact, ExecutionStepImpactReport, ExecutionStepOutcome,
    ForceFlushImpact, ImpactAttribution, ImpactDiagnostic, ImpactDiagnosticKind, ImpactEdgeKind,
    ImpactEffects, ImpactGateBoundary, ImpactNodeCoverage, ImpactPlanningBasis,
    ImpactReportCompleteness, ImpactTopology, ImpactTopologyEdge, ModelChangeAspect, ModelKind,
    ModelName, NodeRef, OperationImpactReason, OperationImpactReport, OwnershipMoveImpact,
    ParseAsType, PauseRequirement, PlannedExecutionStepImpact, QuiesceSubgraph, QuiescenceOutcome,
    RebuildImpact, RebuildReason, RequestedResourceVersion, ResourceBindingImpact,
    ResourceCatalogAction, ResourceCatalogImpact, SchemaField, StatePurge, StateResetImpact,
    Timestamp, TransactionImpactReport, TransactionInspectionTarget, TransactionOperation,
    TransactionOperationAdmission, TransactionOperationRange, TransactionPosition,
    TransactionPreviewIdentity,
};
use url::Url;

use super::fixtures::{name, non_zero, operation, reference, request};
use crate::{
    AttachTransactionRequest, CancelRequest, CellWriter, ClientMessage, ClientRequest,
    CommandDisposition, CommandOutcome, CommandRequest, Diagnostic, EncodedFrame,
    InspectTransactionRequest, LeaderEndpoints, LeaderRedirect, OutcomeOrigin, RowBranch,
    RowSchema, SelectDomainRequest, ServerFrame, SessionLimits, SourceSpan, StatementDisposition,
    StatementOutcome, SubscribeRequest, SubscriptionHandle, SubscriptionRowsEncoder,
    SubscriptionType, SuggestRequest, TransactionState, TransactionStatus, UnsubscribeRequest,
    WireEncodeError,
};

pub(crate) fn leader() -> LeaderEndpoints {
    LeaderEndpoints {
        node: name("node-2"),
        grpc_uri: Some(Url::parse("https://node-2.cluster.local:7443").assured("a literal URI")),
        web_console_uri: Some(
            Url::parse("http://node-2.cluster.local:17420/console/").assured("a literal URI"),
        ),
    }
}

pub(crate) fn preview(position: usize) -> TransactionPreviewIdentity {
    TransactionPreviewIdentity {
        transaction_id: "0192d4e4-7b36-7c3e-9f00-5b2d8c3a1e44".to_string(),
        position: TransactionPosition::new(position),
        planning_basis: ImpactPlanningBasis::new([0xA5; 32]),
    }
}

pub(crate) fn transaction(state: TransactionState) -> TransactionStatus {
    TransactionStatus::new(
        "0192d4e4-7b36-7c3e-9f00-5b2d8c3a1e44".to_string(),
        name("tenant"),
        state,
        TransactionPosition::new(3),
        2,
    )
    .assured("two applied operations of three accepted is a consistent status")
}

pub(crate) fn diagnostics() -> Vec<Diagnostic> {
    vec![
        Diagnostic {
            message: "unexpected token 'RELAY'".to_string(),
            span: Some(SourceSpan::new(7, 12).assured("an ordered span")),
        },
        Diagnostic {
            message: "an empty span marks a position".to_string(),
            span: Some(SourceSpan::new(u32::MAX, u32::MAX).assured("an ordered span")),
        },
        Diagnostic {
            message: String::new(),
            span: None,
        },
    ]
}

pub(crate) fn client_messages() -> Vec<ClientMessage> {
    let requests = vec![
        ClientRequest::Command(CommandRequest {
            query: "CREATE SCHEMA orders ( id U64 ); CREATE RELAY orders SCHEMA orders UNBRANCHED;"
                .to_string(),
            domain: Some(name("tenant")),
            execution_reference: reference("0192d4e4-7b36-7c3e.9f00_5b2d8c3a1e44"),
            expected_transaction_position: Some(TransactionPosition::new(0)),
            expected_preview: None,
        }),
        ClientRequest::Command(CommandRequest {
            query: "COMMIT;".to_string(),
            domain: None,
            execution_reference: reference(&"r".repeat(128)),
            expected_transaction_position: Some(TransactionPosition::new(usize::MAX)),
            expected_preview: Some(preview(usize::MAX)),
        }),
        ClientRequest::Command(CommandRequest {
            query: String::new(),
            domain: None,
            execution_reference: reference("x"),
            expected_transaction_position: None,
            expected_preview: None,
        }),
        ClientRequest::Suggest(
            SuggestRequest::new(
                "SHOW RELAY «ünïcode» ".to_string(),
                13,
                Some(name("tenant")),
            )
            .assured("byte 13 starts a character"),
        ),
        ClientRequest::Suggest(
            SuggestRequest::new("ü".to_string(), 2, None).assured("the end is a boundary"),
        ),
        ClientRequest::ListDomains,
        ClientRequest::SelectDomain(SelectDomainRequest {
            domain: name("tenant"),
        }),
        ClientRequest::AttachTransaction(AttachTransactionRequest {
            transaction_id: "0192d4e4-7b36-7c3e-9f00-5b2d8c3a1e44".to_string(),
        }),
        ClientRequest::InspectTransaction(InspectTransactionRequest {
            target: TransactionInspectionTarget::Attached,
            operation: None,
        }),
        ClientRequest::InspectTransaction(InspectTransactionRequest {
            target: TransactionInspectionTarget::Transaction {
                transaction_id: "0192d4e4-7b36-7c3e-9f00-5b2d8c3a1e44".to_string(),
            },
            operation: Some(operation(usize::MAX)),
        }),
        ClientRequest::InspectTransaction(InspectTransactionRequest {
            target: TransactionInspectionTarget::Attached,
            operation: Some(operation(1)),
        }),
        ClientRequest::Subscribe(SubscribeRequest {
            domain: name("tenant"),
            statement: "CREATE SUBSCRIPTION live FROM orders;".to_string(),
            subscription_type: SubscriptionType::Row,
        }),
        ClientRequest::Unsubscribe(UnsubscribeRequest {
            subscription: name("live"),
        }),
        ClientRequest::Cancel(CancelRequest {
            target: request(u64::MAX),
        }),
    ];
    requests
        .into_iter()
        .zip(1_u64..)
        .map(|(request_body, id)| ClientMessage {
            request_id: request(id),
            request: request_body,
        })
        .collect()
}

pub(crate) fn command_outcome(disposition: CommandDisposition) -> CommandOutcome {
    CommandOutcome {
        execution_reference: reference("0192d4e4-7b36-7c3e-9f00-5b2d8c3a1e44"),
        origin: OutcomeOrigin::Recovered,
        disposition,
        message: "committed transaction; quiesce level: ENTITY_PAUSE".to_string(),
        diagnostics: diagnostics(),
        statements: vec![
            StatementOutcome {
                disposition: StatementDisposition::Completed {
                    already_existed: true,
                },
                message: "schema 'orders' already exists".to_string(),
                diagnostics: Vec::new(),
            },
            StatementOutcome {
                disposition: StatementDisposition::Failed,
                message: "relay 'orders' references a missing schema".to_string(),
                diagnostics: diagnostics(),
            },
            StatementOutcome {
                disposition: StatementDisposition::NotLeader(LeaderRedirect { leader: None }),
                message: String::new(),
                diagnostics: Vec::new(),
            },
        ],
        transaction: Some(transaction(TransactionState::Failed {
            failing_operation: operation(usize::MAX),
            error: "relay 'orders' references a missing schema".to_string(),
        })),
        transaction_admission: Some(TransactionOperationAdmission {
            operation: operation(1),
            preview: preview(1),
        }),
    }
}

pub(crate) fn command_dispositions() -> Vec<CommandDisposition> {
    vec![
        CommandDisposition::Completed {
            already_existed: false,
        },
        CommandDisposition::Completed {
            already_existed: true,
        },
        CommandDisposition::Failed,
        CommandDisposition::NotLeader(LeaderRedirect {
            leader: Some(leader()),
        }),
        CommandDisposition::NotLeader(LeaderRedirect {
            leader: Some(LeaderEndpoints {
                node: name("node-3"),
                grpc_uri: None,
                web_console_uri: None,
            }),
        }),
        CommandDisposition::NotLeader(LeaderRedirect { leader: None }),
        CommandDisposition::TransactionDetached {
            transaction_id: "0192d4e4".to_string(),
        },
        CommandDisposition::TransactionTakenOver {
            transaction_id: "0192d4e4".to_string(),
        },
        CommandDisposition::OutcomeUnknown(crate::UnknownOutcomeCause::LeadershipLost),
        CommandDisposition::OutcomeUnknown(crate::UnknownOutcomeCause::StillApplying),
        CommandDisposition::OutcomeUnknown(crate::UnknownOutcomeCause::NotYetAuthoritative),
        CommandDisposition::ExecutionReferenceConflict(crate::ExecutionReferenceConflict::Content),
        CommandDisposition::ExecutionReferenceConflict(crate::ExecutionReferenceConflict::Domain),
        CommandDisposition::ExecutionReferenceConflict(crate::ExecutionReferenceConflict::Owner),
        CommandDisposition::ExecutionReferenceConflict(crate::ExecutionReferenceConflict::Position),
        CommandDisposition::ExecutionReferenceExpired,
        CommandDisposition::PreviewStale {
            expected: preview(2),
            current: preview(0),
        },
    ]
}

pub(crate) fn transaction_states() -> Vec<TransactionState> {
    vec![
        TransactionState::Open,
        TransactionState::Committing,
        TransactionState::Committed,
        TransactionState::Failed {
            failing_operation: operation(1),
            error: String::new(),
        },
        TransactionState::Reverted,
        TransactionState::Expired,
    ]
}

fn node(kind: ModelKind, identifier: &str) -> NodeRef {
    NodeRef::new(kind, name::<ModelName>(identifier))
}

fn attribution(first: usize, last: usize) -> ImpactAttribution {
    ImpactAttribution::for_range(
        TransactionOperationRange::new(operation(first), operation(last))
            .assured("the sample ranges are ordered"),
    )
}

fn range(first: usize, last: usize) -> TransactionOperationRange {
    TransactionOperationRange::new(operation(first), operation(last))
        .assured("the sample ranges are ordered")
}

fn diagnostic(kind: ImpactDiagnosticKind, number: Option<usize>) -> ImpactDiagnostic {
    ImpactDiagnostic {
        kind,
        operation: number.map(operation),
        message: format!("{} diagnostic", kind.as_ref()),
    }
}

fn selected_keys() -> ConcreteBranchCoverage {
    ConcreteBranchCoverage::selected(
        name("tenants"),
        [
            BranchKeyFingerprint::new([0xFF; 32]),
            BranchKeyFingerprint::new([0x00; 32]),
        ],
    )
    .assured("two keys are a non-empty selection")
}

fn full_effects() -> ImpactEffects {
    let relay = node(ModelKind::Relay, "events");
    let junction = node(ModelKind::Junction, "normalize");
    let relay_execution =
        ImpactNodeCoverage::execution(relay.clone(), ConcreteBranchCoverage::Unbranched);
    let junction_execution = ImpactNodeCoverage::execution(
        junction.clone(),
        ConcreteBranchCoverage::AllOfBranch {
            branch: name("tenants"),
        },
    );
    let schema = ImpactNodeCoverage::configuration(node(ModelKind::Schema, "orders"));
    let emitter = ImpactNodeCoverage::all_executions(node(ModelKind::Emitter, "sink"));
    let selected =
        ImpactNodeCoverage::execution(node(ModelKind::Deduplicator, "dedupe"), selected_keys());
    let before = ImpactTopology {
        nodes: CanonicalImpactSet::new([
            AttributedImpactNode {
                coverage: relay_execution.clone(),
                attribution: attribution(1, 1),
            },
            AttributedImpactNode {
                coverage: schema.clone(),
                attribution: attribution(1, 2),
            },
        ]),
        edges: CanonicalImpactSet::new([ImpactTopologyEdge {
            source: schema.clone(),
            target: relay_execution.clone(),
            kind: ImpactEdgeKind::ConfigurationDependency,
            attribution: attribution(1, 1),
        }]),
    };
    let edge_kinds = [
        ImpactEdgeKind::Dataflow,
        ImpactEdgeKind::MessageError,
        ImpactEdgeKind::CorrelationTimeout,
        ImpactEdgeKind::MaterializedState,
    ];
    let after = ImpactTopology {
        nodes: CanonicalImpactSet::new([
            AttributedImpactNode {
                coverage: relay_execution.clone(),
                attribution: attribution(1, 2),
            },
            AttributedImpactNode {
                coverage: junction_execution.clone(),
                attribution: attribution(2, 2),
            },
            AttributedImpactNode {
                coverage: selected.clone(),
                attribution: attribution(2, 2),
            },
        ]),
        edges: edge_kinds
            .into_iter()
            .map(|kind| ImpactTopologyEdge {
                source: relay_execution.clone(),
                target: junction_execution.clone(),
                kind,
                attribution: attribution(2, 2),
            })
            .collect(),
    };
    ImpactEffects {
        changed_configuration: CanonicalImpactSet::new([
            ConfigurationImpact {
                transition: ConfigurationTransition::Created {
                    node: relay.clone(),
                },
                attribution: attribution(1, 1),
            },
            ConfigurationImpact {
                transition: ConfigurationTransition::Changed {
                    node: junction.clone(),
                },
                attribution: attribution(2, 2),
            },
            ConfigurationImpact {
                transition: ConfigurationTransition::Dropped {
                    node: node(ModelKind::Emitter, "sink"),
                },
                attribution: attribution(1, 2),
            },
        ]),
        topology: AffectedTopology { before, after },
        ownership_moves: CanonicalImpactSet::new([OwnershipMoveImpact {
            node: junction_execution.clone(),
            source: name("node-1"),
            destination: name("node-2"),
            attribution: attribution(2, 2),
        }]),
        lifecycle: CanonicalImpactSet::new([
            DomainLifecycleImpact {
                domain: name("tenant"),
                action: DomainLifecycleAction::Start,
                attribution: attribution(1, 1),
            },
            DomainLifecycleImpact {
                domain: name("tenant"),
                action: DomainLifecycleAction::Stop,
                attribution: attribution(1, 1),
            },
        ]),
        activations: CanonicalImpactSet::new([
            ActivationImpact {
                node: emitter.clone(),
                action: ActivationAction::Activate,
                attribution: attribution(1, 1),
            },
            ActivationImpact {
                node: emitter.clone(),
                action: ActivationAction::Deactivate,
                attribution: attribution(2, 2),
            },
            ActivationImpact {
                node: ImpactNodeCoverage::configuration(node(ModelKind::Vhost, "edge")),
                action: ActivationAction::RefreshHttpsListener,
                attribution: attribution(1, 2),
            },
        ]),
        rebuilds: [
            RebuildReason::Configuration,
            RebuildReason::Ownership,
            RebuildReason::Recovery,
        ]
        .into_iter()
        .map(|reason| RebuildImpact {
            node: relay_execution.clone(),
            reason,
            attribution: attribution(1, 2),
        })
        .collect(),
        state_resets: [
            StatePurge::DeduplicatorKeyspace,
            StatePurge::ReordererBuffer,
            StatePurge::WindowAccumulator,
            StatePurge::CorrelationBuffer,
            StatePurge::InferencerWarmState,
            StatePurge::WasmGuestState,
        ]
        .into_iter()
        .map(|state| StateResetImpact {
            node: selected.clone(),
            state,
            attribution: attribution(2, 2),
        })
        .collect(),
        force_flushes: CanonicalImpactSet::new([ForceFlushImpact {
            node: emitter,
            attribution: attribution(1, 2),
        }]),
        resource_catalog: CanonicalImpactSet::new([ResourceCatalogImpact {
            resource: name("model"),
            action: ResourceCatalogAction::Create,
            attribution: attribution(1, 1),
        }]),
        resource_bindings: CanonicalImpactSet::new([
            ResourceBindingImpact {
                node: node(ModelKind::WasmProcessor, "guest"),
                resource: name("guest_bundle"),
                requested: RequestedResourceVersion::Number(u64::MAX),
                version: u64::MAX,
                attribution: attribution(1, 1),
            },
            ResourceBindingImpact {
                node: node(ModelKind::Inferencer, "score"),
                resource: name("model"),
                requested: RequestedResourceVersion::Latest,
                version: 1,
                attribution: attribution(2, 2),
            },
        ]),
    }
}

fn subgraph_pause() -> PauseRequirement {
    let relay_execution = ImpactNodeCoverage::execution(
        node(ModelKind::Relay, "events"),
        ConcreteBranchCoverage::Unbranched,
    );
    PauseRequirement::Subgraph {
        scope: QuiesceSubgraph::new(
            name("tenant"),
            [
                AttributedImpactNode {
                    coverage: relay_execution,
                    attribution: attribution(1, 2),
                },
                AttributedImpactNode {
                    coverage: ImpactNodeCoverage::all_executions(node(
                        ModelKind::Junction,
                        "normalize",
                    )),
                    attribution: attribution(2, 2),
                },
            ],
            [
                AttributedGateBoundary {
                    boundary: ImpactGateBoundary {
                        relay: name("events"),
                        branches: selected_keys(),
                    },
                    attribution: attribution(1, 1),
                },
                AttributedGateBoundary {
                    boundary: ImpactGateBoundary {
                        relay: name("orders"),
                        branches: ConcreteBranchCoverage::All,
                    },
                    attribution: attribution(1, 2),
                },
            ],
        ),
    }
}

fn operation_report(
    number: usize,
    operation_value: TransactionOperation,
    step: TransactionOperationRange,
    reasons: Vec<OperationImpactReason>,
) -> OperationImpactReport {
    let completeness = if number == 2 {
        ImpactReportCompleteness::incomplete(vec![diagnostic(
            ImpactDiagnosticKind::Planning,
            Some(2),
        )])
        .assured("one diagnostic makes an incomplete report")
    } else {
        ImpactReportCompleteness::Complete
    };
    let contribution = if number == 1 {
        full_effects()
    } else {
        ImpactEffects::default()
    };
    OperationImpactReport {
        number: operation(number),
        operation: operation_value,
        execution_step: step,
        completeness,
        reasons,
        contribution,
    }
}

/// A report that holds every union member and every set of the impact vocabulary.
pub(crate) fn impact_report() -> TransactionImpactReport {
    let tenant = || name("tenant");
    let first_step = range(1, 2);
    let operations = vec![
        operation_report(
            1,
            TransactionOperation::CreateConfiguration {
                domain: tenant(),
                node: node(ModelKind::Relay, "events"),
            },
            first_step,
            vec![OperationImpactReason::Configuration {
                node: node(ModelKind::Relay, "events"),
                aspect: ModelChangeAspect::EntityCreated,
            }],
        ),
        operation_report(
            2,
            TransactionOperation::AlterConfiguration {
                domain: tenant(),
                node: node(ModelKind::Junction, "normalize"),
            },
            first_step,
            vec![
                OperationImpactReason::Configuration {
                    node: node(ModelKind::Junction, "normalize"),
                    aspect: ModelChangeAspect::ProcessorRoutes,
                },
                OperationImpactReason::Configuration {
                    node: node(ModelKind::Junction, "normalize"),
                    aspect: ModelChangeAspect::ProcessorFilter,
                },
            ],
        ),
        operation_report(
            3,
            TransactionOperation::DropConfiguration {
                domain: tenant(),
                node: node(ModelKind::Emitter, "sink"),
            },
            range(3, 3),
            vec![OperationImpactReason::Configuration {
                node: node(ModelKind::Emitter, "sink"),
                aspect: ModelChangeAspect::EntityDropped,
            }],
        ),
        operation_report(
            4,
            TransactionOperation::AlterDomain { domain: tenant() },
            range(4, 4),
            vec![OperationImpactReason::DomainPlacement],
        ),
        operation_report(
            5,
            TransactionOperation::StartDomain { domain: tenant() },
            range(5, 8),
            vec![OperationImpactReason::DomainStart],
        ),
        operation_report(
            6,
            TransactionOperation::StopDomain { domain: tenant() },
            range(5, 8),
            vec![OperationImpactReason::DomainStop],
        ),
        operation_report(
            7,
            TransactionOperation::CreateResource {
                domain: tenant(),
                resource: name("model"),
            },
            range(5, 8),
            vec![OperationImpactReason::ResourceCatalog {
                resource: name("model"),
            }],
        ),
        operation_report(
            8,
            TransactionOperation::RebindResource {
                domain: tenant(),
                resource: name("model"),
                requested: RequestedResourceVersion::Latest,
                version: 3,
            },
            range(5, 8),
            vec![OperationImpactReason::ResourceRebinding {
                node: node(ModelKind::Inferencer, "score"),
                resource: name("model"),
                from_version: 2,
                to_version: 3,
            }],
        ),
    ];
    let failure = diagnostic(ImpactDiagnosticKind::Application, Some(1));
    let execution_steps = vec![
        ExecutionStepImpactReport::new(
            first_step,
            PlannedExecutionStepImpact {
                completeness: ImpactReportCompleteness::Complete,
                pause: subgraph_pause(),
                effects: full_effects(),
            },
            ActualExecutionStepImpact {
                outcome: ExecutionStepOutcome::Failed {
                    diagnostic: failure.clone(),
                },
                quiescence: vec![
                    ActualQuiescence {
                        requirement: subgraph_pause(),
                        outcomes: vec![
                            QuiescenceOutcome::Requested,
                            QuiescenceOutcome::Uncertain {
                                diagnostic: diagnostic(ImpactDiagnosticKind::Quiescence, None),
                            },
                        ],
                    },
                    ActualQuiescence {
                        requirement: PauseRequirement::Domain { domain: tenant() },
                        outcomes: vec![
                            QuiescenceOutcome::Requested,
                            QuiescenceOutcome::Confirmed,
                            QuiescenceOutcome::Failed {
                                diagnostic: diagnostic(ImpactDiagnosticKind::Recovery, Some(2)),
                            },
                            QuiescenceOutcome::Released,
                        ],
                    },
                ],
                effects: full_effects(),
            },
        ),
        ExecutionStepImpactReport::new(
            range(3, 3),
            PlannedExecutionStepImpact {
                completeness: ImpactReportCompleteness::Complete,
                pause: PauseRequirement::NoPause,
                effects: ImpactEffects::default(),
            },
            ActualExecutionStepImpact {
                outcome: ExecutionStepOutcome::Applied,
                quiescence: Vec::new(),
                effects: ImpactEffects::default(),
            },
        ),
        ExecutionStepImpactReport::new(
            range(4, 4),
            PlannedExecutionStepImpact {
                completeness: ImpactReportCompleteness::Complete,
                pause: PauseRequirement::Domain { domain: tenant() },
                effects: ImpactEffects::default(),
            },
            ActualExecutionStepImpact::applying(),
        ),
        ExecutionStepImpactReport::new(
            range(5, 8),
            PlannedExecutionStepImpact {
                completeness: ImpactReportCompleteness::incomplete(vec![
                    diagnostic(ImpactDiagnosticKind::Topology, None),
                    diagnostic(ImpactDiagnosticKind::Ownership, Some(7)),
                    diagnostic(ImpactDiagnosticKind::Activation, Some(5)),
                ])
                .assured("three diagnostics make an incomplete report"),
                pause: PauseRequirement::NoPause,
                effects: ImpactEffects::default(),
            },
            ActualExecutionStepImpact::unattempted(),
        ),
    ];
    TransactionImpactReport::new(
        tenant(),
        TransactionPosition::new(8),
        ImpactPlanningBasis::new([0x5A; 32]),
        ImpactReportCompleteness::incomplete(vec![failure])
            .assured("one diagnostic makes an incomplete report"),
        operations,
        execution_steps,
    )
    .assured("the sample numbers its operations consecutively and partitions them into steps")
}

/// A schema whose fields hold every type, nullability and sensitivity, on a branched relay.
pub(crate) fn row_schema() -> RowSchema {
    let field = |raw: &str, ty: ParseAsType| SchemaField {
        name: name(raw),
        ty,
        optional: false,
        sensitive: false,
    };
    let array = |element: ParseAsType, len: u32| ParseAsType::Array {
        element: Box::new(element),
        len: NonZeroU32::new(len).assured("the sample length is non-zero"),
    };
    RowSchema {
        fields: vec![
            field("u8", ParseAsType::U8),
            field("i8", ParseAsType::I8),
            field("u16", ParseAsType::U16),
            field("i16", ParseAsType::I16),
            field("u32", ParseAsType::U32),
            field("i32", ParseAsType::I32),
            field("u64", ParseAsType::U64),
            field("i64", ParseAsType::I64),
            field("f32", ParseAsType::F32),
            field("f64", ParseAsType::F64),
            field("flag", ParseAsType::Bool),
            field("text", ParseAsType::String),
            field("seen_at", ParseAsType::Datetime),
            SchemaField {
                optional: true,
                ..field("note", ParseAsType::String)
            },
            SchemaField {
                sensitive: true,
                ..field("secret", ParseAsType::String)
            },
            SchemaField {
                optional: true,
                sensitive: true,
                ..field("maybe_secret", ParseAsType::I64)
            },
            field("triple", array(ParseAsType::I32, 3)),
            field(
                "pairs",
                ParseAsType::Vec {
                    element: Box::new(array(ParseAsType::U8, 2)),
                },
            ),
        ],
        branch: Some(
            RowBranch::new(
                name("tenants"),
                vec![
                    field("tenant", ParseAsType::String),
                    SchemaField {
                        optional: true,
                        ..field("region", ParseAsType::U16)
                    },
                    SchemaField {
                        sensitive: true,
                        ..field("token", ParseAsType::String)
                    },
                ],
            )
            .assured("the sample branch has key fields"),
        ),
    }
}

pub(crate) fn subscription() -> SubscriptionHandle {
    SubscriptionHandle {
        name: name("live"),
        generation: non_zero(u64::MAX),
    }
}

/// The branch key of [`row_schema`]: a value, a null and a redaction.
fn write_branch_key(key: &mut CellWriter<'_, 'static>) -> Result<(), Report<WireEncodeError>> {
    key.push_string("acme")?;
    key.push_null()?;
    key.push_redacted()?;
    Ok(())
}

/// A row of [`row_schema`] holding the smallest value of every type.
fn write_minimum_row(cells: &mut CellWriter<'_, 'static>) -> Result<(), Report<WireEncodeError>> {
    cells.push_u8(u8::MIN)?;
    cells.push_i8(i8::MIN)?;
    cells.push_u16(u16::MIN)?;
    cells.push_i16(i16::MIN)?;
    cells.push_u32(u32::MIN)?;
    cells.push_i32(i32::MIN)?;
    cells.push_u64(u64::MIN)?;
    cells.push_i64(i64::MIN)?;
    cells.push_f32(-0.0)?;
    cells.push_f64(f64::NEG_INFINITY)?;
    cells.push_bool(false)?;
    cells.push_string("")?;
    cells.push_datetime(Timestamp::from_unix_nanos(i64::MIN))?;
    cells.push_null()?;
    cells.push_redacted()?;
    cells.push_redacted()?;
    cells.push_list(|elements| {
        elements.push_i32(i32::MIN)?;
        elements.push_i32(0)?;
        elements.push_i32(i32::MAX)?;
        Ok(())
    })?;
    cells.push_list(|_| Ok(()))?;
    Ok(())
}

/// A row of [`row_schema`] holding the largest and most unusual value of every type.
fn write_maximum_row(cells: &mut CellWriter<'_, 'static>) -> Result<(), Report<WireEncodeError>> {
    cells.push_u8(u8::MAX)?;
    cells.push_i8(i8::MAX)?;
    cells.push_u16(u16::MAX)?;
    cells.push_i16(i16::MAX)?;
    cells.push_u32(u32::MAX)?;
    cells.push_i32(i32::MAX)?;
    cells.push_u64(u64::MAX)?;
    cells.push_i64(i64::MAX)?;
    cells.push_f32(f32::from_bits(0x7FC0_0001))?;
    cells.push_f64(f64::from_bits(0x0000_0000_0000_0001))?;
    cells.push_bool(true)?;
    cells.push_string("ünïcødé 🦀 \u{0}")?;
    cells.push_datetime(Timestamp::from_unix_nanos(i64::MAX))?;
    cells.push_string("present")?;
    cells.push_redacted()?;
    cells.push_redacted()?;
    cells.push_list(|elements| {
        elements.push_i32(1)?;
        elements.push_i32(2)?;
        elements.push_i32(3)?;
        Ok(())
    })?;
    cells.push_list(|pairs| {
        pairs.push_list(|pair| {
            pair.push_u8(0)?;
            pair.push_u8(255)?;
            Ok(())
        })?;
        pairs.push_list(|pair| {
            pair.push_u8(7)?;
            pair.push_u8(8)?;
            Ok(())
        })
    })?;
    Ok(())
}

/// A batch of [`row_schema`] rows on the `acme` branch.
pub(crate) fn rows_frame(limits: &SessionLimits) -> EncodedFrame<ServerFrame> {
    let mut batch = SubscriptionRowsEncoder::branched(subscription(), limits, write_branch_key)
        .assured("the sample branch key fits the limits");
    batch
        .push_row(write_minimum_row)
        .assured("the sample row fits the limits");
    batch
        .push_row(write_maximum_row)
        .assured("the sample row fits the limits");
    batch.finish().assured("the sample batch fits the limits")
}
