//! The transaction impact report on the wire: canonical sets, the vocabulary's own rules, and
//! undeclared members, checked against reports the typed encoder would never produce.

use bytes::Bytes;
use flatbuffers::{FlatBufferBuilder, UnionWIPOffset, WIPOffset};

use super::fixtures::{decode_error, finish_raw, raw_server};
use crate::{ServerMessage, WireDecodeError, wire};

type Builder = FlatBufferBuilder<'static>;

/// The parts of a one-operation report a test varies.
struct RawReport {
    position: u64,
    operation_number: u64,
    step: (u64, u64),
    operation_type: wire::TransactionOperation,
    node_kind: Option<wire::ModelKind>,
    aspect: Option<wire::ModelChangeAspect>,
    incomplete_diagnostics: Option<usize>,
    /// The attribution of each changed-configuration entry, in order.
    configuration_attributions: Vec<Vec<u64>>,
    /// The selected branch keys of a topology node, when the node selects keys.
    selected_keys: Option<Vec<[u8; 32]>>,
    /// The bytes of the report's planning basis fingerprint.
    planning_basis: Vec<u8>,
    /// The discriminant a node's coverage is stored under, when a test overrides the one its
    /// value has.
    coverage_type: Option<wire::NodeBranchCoverage>,
    /// The discriminant of a resource binding's requested version, when the contribution binds a
    /// resource.
    requested_version_type: Option<wire::RequestedResourceVersion>,
    /// The attributions of subgraph pause nodes sharing one coverage, when the step pauses one.
    subgraph_attributions: Option<Vec<Vec<u64>>>,
}

impl Default for RawReport {
    fn default() -> Self {
        Self {
            position: 1,
            operation_number: 1,
            step: (1, 1),
            operation_type: wire::TransactionOperation::CreateConfigurationOperation,
            node_kind: Some(wire::ModelKind::Relay),
            aspect: Some(wire::ModelChangeAspect::EntityCreated),
            incomplete_diagnostics: None,
            configuration_attributions: vec![vec![1]],
            selected_keys: None,
            planning_basis: vec![7; 32],
            coverage_type: None,
            requested_version_type: None,
            subgraph_attributions: None,
        }
    }
}

impl RawReport {
    fn node(&self, builder: &mut Builder) -> WIPOffset<wire::NodeRef<'static>> {
        let name = builder.create_string("events");
        wire::NodeRef::create(
            builder,
            &wire::NodeRefArgs {
                kind: self.node_kind,
                name: Some(name),
            },
        )
    }

    fn coverage(&self, builder: &mut Builder) -> WIPOffset<wire::NodeCoverage<'static>> {
        let node = self.node(builder);
        let (branches_type, branches) = match &self.selected_keys {
            Some(keys) => {
                let branch = builder.create_string("tenants");
                let fingerprints = keys
                    .iter()
                    .map(|key| fingerprint(builder, key))
                    .collect::<Vec<_>>();
                let keys = builder.create_vector(&fingerprints);
                let selected = wire::SelectedBranchCoverage::create(
                    builder,
                    &wire::SelectedBranchCoverageArgs {
                        branch: Some(branch),
                        keys: Some(keys),
                    },
                );
                (
                    wire::NodeBranchCoverage::SelectedBranchCoverage,
                    selected.as_union_value(),
                )
            }
            None => {
                let configuration = wire::ConfigurationCoverage::create(
                    builder,
                    &wire::ConfigurationCoverageArgs {},
                );
                (
                    wire::NodeBranchCoverage::ConfigurationCoverage,
                    configuration.as_union_value(),
                )
            }
        };
        let branches_type = self.coverage_type.unwrap_or(branches_type);
        // A NONE discriminant equals the field's default, which the builder writes only when
        // forced to.
        builder.force_defaults(true);
        let coverage = wire::NodeCoverage::create(
            builder,
            &wire::NodeCoverageArgs {
                node: Some(node),
                branches_type,
                branches: Some(branches),
            },
        );
        builder.force_defaults(false);
        coverage
    }

    fn effects(
        &self,
        builder: &mut Builder,
        contribution: bool,
    ) -> WIPOffset<wire::ImpactEffects<'static>> {
        let mut changed = Vec::new();
        if contribution {
            for operations in &self.configuration_attributions {
                let node = self.node(builder);
                let created = wire::ConfigurationCreated::create(
                    builder,
                    &wire::ConfigurationCreatedArgs { node: Some(node) },
                );
                let operations = builder.create_vector(operations);
                changed.push(wire::ConfigurationImpact::create(
                    builder,
                    &wire::ConfigurationImpactArgs {
                        transition_type: wire::ConfigurationTransition::ConfigurationCreated,
                        transition: Some(created.as_union_value()),
                        operations: Some(operations),
                    },
                ));
            }
        }
        let changed_configuration = builder.create_vector(&changed);
        let before_nodes = if contribution && self.selected_keys.is_some() {
            let coverage = self.coverage(builder);
            let operations = builder.create_vector(&[1_u64]);
            vec![wire::AttributedNode::create(
                builder,
                &wire::AttributedNodeArgs {
                    coverage: Some(coverage),
                    operations: Some(operations),
                },
            )]
        } else {
            Vec::new()
        };
        let before = topology(builder, &before_nodes);
        let after = topology(builder, &[]);
        let topology = wire::AffectedTopology::create(
            builder,
            &wire::AffectedTopologyArgs {
                before: Some(before),
                after: Some(after),
            },
        );
        let ownership_moves = builder.create_vector::<WIPOffset<wire::OwnershipMoveImpact>>(&[]);
        let lifecycle = builder.create_vector::<WIPOffset<wire::DomainLifecycleImpact>>(&[]);
        let activations = builder.create_vector::<WIPOffset<wire::ActivationImpact>>(&[]);
        let rebuilds = builder.create_vector::<WIPOffset<wire::RebuildImpact>>(&[]);
        let state_resets = builder.create_vector::<WIPOffset<wire::StateResetImpact>>(&[]);
        let force_flushes = builder.create_vector::<WIPOffset<wire::ForceFlushImpact>>(&[]);
        let resource_catalog = builder.create_vector::<WIPOffset<wire::ResourceCatalogImpact>>(&[]);
        let mut bindings = Vec::new();
        if contribution && let Some(requested_type) = self.requested_version_type {
            let node = self.node(builder);
            let resource = builder.create_string("guest_bundle");
            let latest =
                wire::LatestResourceVersion::create(builder, &wire::LatestResourceVersionArgs {});
            let operations = builder.create_vector(&[1_u64]);
            // A NONE discriminant equals the field's default, which the builder writes only when
            // forced to.
            builder.force_defaults(true);
            bindings.push(wire::ResourceBindingImpact::create(
                builder,
                &wire::ResourceBindingImpactArgs {
                    node: Some(node),
                    resource: Some(resource),
                    requested_type,
                    requested: Some(latest.as_union_value()),
                    version: 1,
                    operations: Some(operations),
                },
            ));
            builder.force_defaults(false);
        }
        let resource_bindings = builder.create_vector(&bindings);
        wire::ImpactEffects::create(
            builder,
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
        )
    }

    fn completeness(
        &self,
        builder: &mut Builder,
    ) -> (wire::ImpactCompleteness, WIPOffset<UnionWIPOffset>) {
        match self.incomplete_diagnostics {
            Some(count) => {
                let mut diagnostics = Vec::new();
                for _ in 0..count {
                    let message = builder.create_string("unresolved");
                    diagnostics.push(wire::ImpactDiagnostic::create(
                        builder,
                        &wire::ImpactDiagnosticArgs {
                            kind: Some(wire::ImpactDiagnosticKind::Topology),
                            operation: None,
                            message: Some(message),
                        },
                    ));
                }
                let diagnostics = builder.create_vector(&diagnostics);
                let incomplete = wire::ImpactIncomplete::create(
                    builder,
                    &wire::ImpactIncompleteArgs {
                        diagnostics: Some(diagnostics),
                    },
                );
                (
                    wire::ImpactCompleteness::ImpactIncomplete,
                    incomplete.as_union_value(),
                )
            }
            None => {
                let complete = wire::ImpactComplete::create(builder, &wire::ImpactCompleteArgs {});
                (
                    wire::ImpactCompleteness::ImpactComplete,
                    complete.as_union_value(),
                )
            }
        }
    }

    fn pause(&self, builder: &mut Builder) -> (wire::PauseRequirement, WIPOffset<UnionWIPOffset>) {
        let Some(attributions) = &self.subgraph_attributions else {
            let none = wire::NoPause::create(builder, &wire::NoPauseArgs {});
            return (wire::PauseRequirement::NoPause, none.as_union_value());
        };
        let mut nodes = Vec::new();
        for operations in attributions {
            let coverage = self.coverage(builder);
            let operations = builder.create_vector(operations);
            nodes.push(wire::AttributedNode::create(
                builder,
                &wire::AttributedNodeArgs {
                    coverage: Some(coverage),
                    operations: Some(operations),
                },
            ));
        }
        let nodes = builder.create_vector(&nodes);
        let gates = builder.create_vector::<WIPOffset<wire::AttributedGateBoundary>>(&[]);
        let domain = builder.create_string("tenant");
        let scope = wire::QuiesceSubgraph::create(
            builder,
            &wire::QuiesceSubgraphArgs {
                domain: Some(domain),
                nodes: Some(nodes),
                gate_boundaries: Some(gates),
            },
        );
        let pause =
            wire::SubgraphPause::create(builder, &wire::SubgraphPauseArgs { scope: Some(scope) });
        (
            wire::PauseRequirement::SubgraphPause,
            pause.as_union_value(),
        )
    }

    fn operation(&self, builder: &mut Builder) -> WIPOffset<wire::OperationReport<'static>> {
        let domain = builder.create_string("tenant");
        let node = self.node(builder);
        let operation = wire::CreateConfigurationOperation::create(
            builder,
            &wire::CreateConfigurationOperationArgs {
                domain: Some(domain),
                node: Some(node),
            },
        );
        let (completeness_type, completeness) = self.completeness(builder);
        let reason_node = self.node(builder);
        let configuration = wire::ConfigurationReason::create(
            builder,
            &wire::ConfigurationReasonArgs {
                node: Some(reason_node),
                aspect: self.aspect,
            },
        );
        let reason = wire::OperationReason::create(
            builder,
            &wire::OperationReasonArgs {
                reason_type: wire::OperationImpactReason::ConfigurationReason,
                reason: Some(configuration.as_union_value()),
            },
        );
        let reasons = builder.create_vector(&[reason]);
        let contribution = self.effects(builder, true);
        let step = wire::OperationRange::new(self.step.0, self.step.1);
        wire::OperationReport::create(
            builder,
            &wire::OperationReportArgs {
                number: self.operation_number,
                operation_type: self.operation_type,
                operation: Some(operation.as_union_value()),
                execution_step: Some(&step),
                completeness_type,
                completeness: Some(completeness),
                reasons: Some(reasons),
                contribution: Some(contribution),
            },
        )
    }

    fn step(&self, builder: &mut Builder) -> WIPOffset<wire::ExecutionStepReport<'static>> {
        let complete = wire::ImpactComplete::create(builder, &wire::ImpactCompleteArgs {});
        let (pause_type, pause) = self.pause(builder);
        let planned_effects = self.effects(builder, false);
        let planned = wire::PlannedStepImpact::create(
            builder,
            &wire::PlannedStepImpactArgs {
                completeness_type: wire::ImpactCompleteness::ImpactComplete,
                completeness: Some(complete.as_union_value()),
                pause_type,
                pause: Some(pause),
                effects: Some(planned_effects),
            },
        );
        let unattempted = wire::StepUnattempted::create(builder, &wire::StepUnattemptedArgs {});
        let quiescence = builder.create_vector::<WIPOffset<wire::ActualQuiescence>>(&[]);
        let actual_effects = self.effects(builder, false);
        let actual = wire::ActualStepImpact::create(
            builder,
            &wire::ActualStepImpactArgs {
                outcome_type: wire::ExecutionStepOutcome::StepUnattempted,
                outcome: Some(unattempted.as_union_value()),
                quiescence: Some(quiescence),
                effects: Some(actual_effects),
            },
        );
        let operations = wire::OperationRange::new(self.step.0, self.step.1);
        wire::ExecutionStepReport::create(
            builder,
            &wire::ExecutionStepReportArgs {
                operations: Some(&operations),
                planned: Some(planned),
                actual: Some(actual),
            },
        )
    }

    fn frame(&self) -> Bytes {
        let mut builder = FlatBufferBuilder::new();
        let operation = self.operation(&mut builder);
        let operations = builder.create_vector(&[operation]);
        let step = self.step(&mut builder);
        let steps = builder.create_vector(&[step]);
        let domain = builder.create_string("tenant");
        let complete = wire::ImpactComplete::create(&mut builder, &wire::ImpactCompleteArgs {});
        let basis = fingerprint(&mut builder, &self.planning_basis);
        let report = wire::TransactionImpactReport::create(
            &mut builder,
            &wire::TransactionImpactReportArgs {
                domain: Some(domain),
                position: self.position,
                planning_basis: Some(basis),
                completeness_type: wire::ImpactCompleteness::ImpactComplete,
                completeness: Some(complete.as_union_value()),
                operations: Some(operations),
                execution_steps: Some(steps),
            },
        );
        let transaction = transaction_status(&mut builder);
        let inspected = wire::TransactionInspected::create(
            &mut builder,
            &wire::TransactionInspectedArgs {
                transaction: Some(transaction),
                operation: None,
                report: Some(report),
            },
        );
        let outcome = wire::InspectionOutcome::create(
            &mut builder,
            &wire::InspectionOutcomeArgs {
                disposition_type: wire::InspectionDisposition::TransactionInspected,
                disposition: Some(inspected.as_union_value()),
            },
        );
        let reply = wire::Reply::create(
            &mut builder,
            &wire::ReplyArgs {
                request_id: 1,
                body_type: wire::ReplyBody::InspectionOutcome,
                body: Some(outcome.as_union_value()),
            },
        );
        let root = wire::ServerMessage::create(
            &mut builder,
            &wire::ServerMessageArgs {
                body_type: wire::ServerBody::Reply,
                body: Some(reply.as_union_value()),
            },
        );
        finish_raw(builder, root, "NXSM")
    }

    fn decode_error(&self) -> WireDecodeError {
        decode_error(ServerMessage::decode(&raw_server(self.frame())))
    }
}

fn topology(
    builder: &mut Builder,
    nodes: &[WIPOffset<wire::AttributedNode<'static>>],
) -> WIPOffset<wire::ImpactTopology<'static>> {
    let nodes = builder.create_vector(nodes);
    let edges = builder.create_vector::<WIPOffset<wire::TopologyEdge>>(&[]);
    wire::ImpactTopology::create(
        builder,
        &wire::ImpactTopologyArgs {
            nodes: Some(nodes),
            edges: Some(edges),
        },
    )
}

fn fingerprint(builder: &mut Builder, bytes: &[u8]) -> WIPOffset<wire::Fingerprint<'static>> {
    let bytes = builder.create_vector(bytes);
    wire::Fingerprint::create(builder, &wire::FingerprintArgs { bytes: Some(bytes) })
}

fn transaction_status(builder: &mut Builder) -> WIPOffset<wire::TransactionStatus<'static>> {
    let id = builder.create_string("id");
    let domain = builder.create_string("tenant");
    let open = wire::TransactionOpen::create(builder, &wire::TransactionOpenArgs {});
    wire::TransactionStatus::create(
        builder,
        &wire::TransactionStatusArgs {
            transaction_id: Some(id),
            domain: Some(domain),
            state_type: wire::TransactionState::TransactionOpen,
            state: Some(open.as_union_value()),
            accepted_operations: 1,
            applied_operations: 0,
        },
    )
}

#[test]
fn the_raw_report_baseline_decodes() {
    let frame = raw_server(RawReport::default().frame());
    assert!(ServerMessage::decode(&frame).is_ok());
    let frame = raw_server(
        RawReport {
            selected_keys: Some(vec![[1; 32], [2; 32]]),
            subgraph_attributions: Some(vec![vec![1]]),
            incomplete_diagnostics: Some(1),
            ..RawReport::default()
        }
        .frame(),
    );
    assert!(ServerMessage::decode(&frame).is_ok());
}

#[test]
fn attributions_are_non_empty_ascending_sets_of_operation_numbers() {
    let field = "ConfigurationImpact.operations";
    for operations in [vec![1, 1], vec![2, 1]] {
        let report = RawReport {
            configuration_attributions: vec![operations],
            ..RawReport::default()
        };
        assert_eq!(
            report.decode_error(),
            WireDecodeError::NonCanonicalSet { field }
        );
    }
    let report = RawReport {
        configuration_attributions: vec![Vec::new()],
        ..RawReport::default()
    };
    assert_eq!(
        report.decode_error(),
        WireDecodeError::EmptyCollection { field }
    );
    let report = RawReport {
        configuration_attributions: vec![vec![0]],
        ..RawReport::default()
    };
    assert_eq!(report.decode_error(), WireDecodeError::ZeroValue { field });
}

#[test]
fn effect_sets_are_sent_in_canonical_order() {
    let field = "ImpactEffects.changed_configuration";
    for attributions in [vec![vec![2], vec![1]], vec![vec![1], vec![1]]] {
        let report = RawReport {
            configuration_attributions: attributions,
            ..RawReport::default()
        };
        assert_eq!(
            report.decode_error(),
            WireDecodeError::NonCanonicalSet { field }
        );
    }
}

#[test]
fn subgraph_nodes_never_share_a_coverage() {
    let report = RawReport {
        subgraph_attributions: Some(vec![vec![1], vec![1]]),
        ..RawReport::default()
    };
    assert_eq!(
        report.decode_error(),
        WireDecodeError::NonCanonicalSet {
            field: "QuiesceSubgraph.nodes",
        }
    );
}

#[test]
fn selected_branch_keys_are_a_non_empty_ascending_set() {
    let field = "SelectedBranchCoverage.keys";
    let report = RawReport {
        selected_keys: Some(Vec::new()),
        ..RawReport::default()
    };
    assert_eq!(
        report.decode_error(),
        WireDecodeError::EmptyCollection { field }
    );
    for keys in [vec![[2; 32], [1; 32]], vec![[1; 32], [1; 32]]] {
        let report = RawReport {
            selected_keys: Some(keys),
            ..RawReport::default()
        };
        assert_eq!(
            report.decode_error(),
            WireDecodeError::NonCanonicalSet { field }
        );
    }
}

#[test]
fn a_fingerprint_holds_exactly_32_bytes() {
    for planning_basis in [vec![7; 31], vec![7; 33], Vec::new()] {
        let report = RawReport {
            planning_basis,
            ..RawReport::default()
        };
        assert_eq!(
            report.decode_error(),
            WireDecodeError::InvalidValue {
                field: "TransactionImpactReport.planning_basis",
                kind: "32-byte fingerprint",
            }
        );
    }
}

#[test]
fn a_report_must_follow_the_vocabulary_rules() {
    let report = RawReport {
        position: 2,
        ..RawReport::default()
    };
    assert_eq!(
        report.decode_error(),
        WireDecodeError::InvalidValue {
            field: "TransactionImpactReport",
            kind: "transaction impact report",
        }
    );
    let report = RawReport {
        operation_number: 2,
        ..RawReport::default()
    };
    assert!(matches!(
        report.decode_error(),
        WireDecodeError::InvalidValue {
            field: "TransactionImpactReport",
            ..
        }
    ));
    let report = RawReport {
        step: (2, 1),
        ..RawReport::default()
    };
    assert_eq!(
        report.decode_error(),
        WireDecodeError::InvalidValue {
            field: "OperationReport.execution_step",
            kind: "operation range",
        }
    );
    let report = RawReport {
        incomplete_diagnostics: Some(0),
        ..RawReport::default()
    };
    assert_eq!(
        report.decode_error(),
        WireDecodeError::EmptyCollection {
            field: "ImpactIncomplete.diagnostics",
        }
    );
}

#[test]
fn undeclared_report_members_are_refused() {
    let report = RawReport {
        operation_type: wire::TransactionOperation(42),
        ..RawReport::default()
    };
    assert_eq!(
        report.decode_error(),
        WireDecodeError::UnknownUnionVariant {
            field: "OperationReport.operation",
            discriminant: 42,
        }
    );
    let report = RawReport {
        node_kind: Some(wire::ModelKind(25)),
        ..RawReport::default()
    };
    assert_eq!(
        report.decode_error(),
        WireDecodeError::UnknownEnumValue {
            field: "NodeRef.kind",
            value: 25,
        }
    );
    let report = RawReport {
        node_kind: None,
        ..RawReport::default()
    };
    assert_eq!(
        report.decode_error(),
        WireDecodeError::MissingField {
            field: "NodeRef.kind",
        }
    );
    let report = RawReport {
        aspect: Some(wire::ModelChangeAspect(76)),
        ..RawReport::default()
    };
    assert_eq!(
        report.decode_error(),
        WireDecodeError::UnknownEnumValue {
            field: "ConfigurationReason.aspect",
            value: 76,
        }
    );
    let report = RawReport {
        requested_version_type: Some(wire::RequestedResourceVersion::LatestResourceVersion),
        ..RawReport::default()
    };
    assert!(ServerMessage::decode(&raw_server(report.frame())).is_ok());
    for discriminant in [
        wire::RequestedResourceVersion::NONE,
        wire::RequestedResourceVersion(3),
    ] {
        let report = RawReport {
            requested_version_type: Some(discriminant),
            ..RawReport::default()
        };
        assert_eq!(
            report.decode_error(),
            WireDecodeError::UnknownUnionVariant {
                field: "ResourceBindingImpact.requested",
                discriminant: discriminant.0,
            }
        );
    }
    // NONE is never a valid discriminant, even with a value present, which the verifier leaves
    // unchecked.
    for discriminant in [wire::NodeBranchCoverage::NONE, wire::NodeBranchCoverage(6)] {
        let report = RawReport {
            coverage_type: Some(discriminant),
            subgraph_attributions: Some(vec![vec![1]]),
            ..RawReport::default()
        };
        assert_eq!(
            report.decode_error(),
            WireDecodeError::UnknownUnionVariant {
                field: "NodeCoverage.branches",
                discriminant: discriminant.0,
            }
        );
    }
}
