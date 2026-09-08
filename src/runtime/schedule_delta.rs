use nervix_models::{
    DomainSchedule, DynamicModelUpdate, ModelKind, NodeRef, QuiesceLevel, ScheduledNode,
};
use sorted_vec::SortedSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ScheduleDelta {
    Unchanged,
    Dynamic(Vec<DynamicModelUpdate>),
    EntitySwap {
        entities: Vec<NodeRef>,
        reassignments: Vec<NodeRef>,
        dynamic_updates: Vec<DynamicModelUpdate>,
    },
    Rebuild,
}

impl ScheduleDelta {
    pub(super) fn classify(existing: &DomainSchedule, desired: &DomainSchedule) -> Self {
        if existing == desired {
            return Self::Unchanged;
        }
        if existing.domain != desired.domain || existing.nodes.len() != desired.nodes.len() {
            return Self::Rebuild;
        }

        let mut updates = Vec::new();
        let mut entities = Vec::new();
        let mut reassignments = Vec::new();
        for (identity, desired_node) in &desired.nodes {
            let Some(existing_node) = existing.nodes.get(identity) else {
                return Self::Rebuild;
            };
            if !existing_node.has_same_assignment_as(desired_node) {
                reassignments.push(desired_node.identity());
            }
            let aspects = existing_node
                .config
                .change_aspects_against(&desired_node.config);
            let level = aspects.quiesce_level();
            let emitter_schema_fingerprint_may_change =
                desired_node.kind() == ModelKind::Emitter && level == QuiesceLevel::EntityPause;
            let model_derived_residue_may_change = matches!(
                desired_node.kind(),
                ModelKind::Reingestor | ModelKind::Generator
            ) && level == QuiesceLevel::EntityPause;
            let ingestor_schedule_residue_may_change =
                desired_node.kind() == ModelKind::Ingestor && level == QuiesceLevel::EntityPause;
            if !Self::same_schedule_residue(
                existing_node,
                desired_node,
                emitter_schema_fingerprint_may_change,
                model_derived_residue_may_change,
                ingestor_schedule_residue_may_change,
            ) {
                return Self::Rebuild;
            }
            if aspects.is_empty() {
                continue;
            }
            match level {
                QuiesceLevel::Dynamic if aspects.is_control_plane_only() => {}
                QuiesceLevel::Dynamic => {
                    if aspects.dynamic_updates().is_empty() {
                        return Self::Rebuild;
                    }
                    updates.extend(aspects.dynamic_updates().iter().cloned());
                }
                QuiesceLevel::EntityPause => {
                    entities.push(desired_node.identity());
                    updates.extend(aspects.dynamic_updates().iter().cloned());
                }
                QuiesceLevel::DomainPause => return Self::Rebuild,
            }
        }

        if entities.is_empty() && reassignments.is_empty() {
            // The schedules still differ somewhere the runtime does not execute, such as a
            // placement definition. Publishing it keeps the stored schedule truthful.
            return Self::Dynamic(updates);
        }
        Self::EntitySwap {
            entities: SortedSet::from_unsorted(entities).into_vec(),
            reassignments: SortedSet::from_unsorted(reassignments).into_vec(),
            dynamic_updates: updates,
        }
    }

    fn same_schedule_residue(
        existing: &ScheduledNode,
        desired: &ScheduledNode,
        allow_schema_fingerprint_change: bool,
        allow_model_derived_residue_change: bool,
        allow_ingestor_schedule_residue_change: bool,
    ) -> bool {
        let ScheduledNode {
            identifier: existing_identifier,
            effective_branching: existing_effective_branching,
            effective_branching_schema: existing_effective_branching_schema,
            schema_fingerprint: existing_schema_fingerprint,
            kafka_partition_schedule: existing_kafka_partition_schedule,
            ..
        } = existing;
        let ScheduledNode {
            identifier: desired_identifier,
            effective_branching: desired_effective_branching,
            effective_branching_schema: desired_effective_branching_schema,
            schema_fingerprint: desired_schema_fingerprint,
            kafka_partition_schedule: desired_kafka_partition_schedule,
            ..
        } = desired;

        if existing_identifier != desired_identifier || existing.kind() != desired.kind() {
            return false;
        }
        if allow_ingestor_schedule_residue_change {
            return true;
        }

        (allow_model_derived_residue_change
            || (existing_effective_branching == desired_effective_branching
                && existing_effective_branching_schema == desired_effective_branching_schema))
            && (allow_schema_fingerprint_change
                || allow_model_derived_residue_change
                || existing_schema_fingerprint == desired_schema_fingerprint)
            && existing_kafka_partition_schedule == desired_kafka_partition_schedule
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use nervix_models::{
        AckMode, BranchSelection, ClusterNodeName, CreateEmitter, CreateIngestor, CreateJunction,
        CreatePlacement, CreateRelay, DomainName, DomainSchedule, DynamicModelUpdate, EmitSink,
        EmitterPublishingMode, EndpointIngestMode, ErrorPolicies, Expression, FlushPolicy,
        GeneralErrorPolicy, IngestSource, Literal, Model, ModelKind, NodeRef, OutputBranch,
        PlacementPolicy, ProcessorInputs, ProcessorOutput, ProcessorOutputs, RelayBranching,
        RetryPolicy, RouteConstruction, ScheduledNode,
    };
    use nonzero_ext::nonzero;

    use super::ScheduleDelta;

    fn named<N>(raw: &str) -> N
    where
        N: for<'a> TryFrom<&'a str>,
        for<'a> <N as TryFrom<&'a str>>::Error: std::fmt::Debug,
    {
        N::try_from(raw).expect("valid name")
    }

    fn push_scheduled(schedule: &mut DomainSchedule, node: ScheduledNode) {
        schedule.nodes.insert(node.identity(), node);
    }

    fn first_scheduled(schedule: DomainSchedule) -> ScheduledNode {
        schedule
            .nodes
            .into_values()
            .next()
            .expect("fixture schedule must contain a node")
    }

    fn schedule(capacity: NonZeroUsize) -> DomainSchedule {
        DomainSchedule::new(
            DomainName::parse("testing").expect("valid domain"),
            vec![
                ScheduledNode::new(Model::Relay(CreateRelay {
                    name: named("events"),
                    schema: named("event"),
                    buffer: capacity,
                    branching: RelayBranching::unbranched(),
                    materialized_state: None,
                }))
                .with_effective_branching(Some(Vec::new()), None)
                .with_schema_fingerprint([1; 32])
                .placed_on(
                    Some(ClusterNodeName::parse("node-1").expect("valid name")),
                    vec![ClusterNodeName::parse("node-1").expect("valid name")],
                ),
            ],
            Vec::new(),
        )
    }

    fn ingestor_schedule(endpoint: &str) -> DomainSchedule {
        DomainSchedule::new(
            DomainName::parse("testing").expect("valid domain"),
            vec![
                ScheduledNode::new(Model::Ingestor(CreateIngestor {
                    name: named("event_source"),
                    output_routes: ProcessorOutputs::new(vec![ProcessorOutput {
                        relay: named("events"),
                        construction: RouteConstruction::default(),
                        flush_policy: Some(FlushPolicy::Immediate),
                        message_error_policy: nervix_models::MessageErrorPolicy::Log,
                        branch: Some(OutputBranch::Unbranched),
                    }]),
                    decode_using_codec: named("event_codec"),
                    timestamp_source: None,
                    source: IngestSource::Endpoint {
                        endpoint: named(endpoint),
                        mode: EndpointIngestMode::NoAckSequential,
                        quiesce: nervix_models::IngestQuiesceMode::EndpointBuffer {
                            max_size: "1MiB".to_string(),
                        },
                    },
                    general_error_policy: GeneralErrorPolicy::Log,
                    filter_where: None,
                }))
                .with_schema_fingerprint([1; 32])
                .placed_on(
                    Some(ClusterNodeName::parse("node-1").expect("valid name")),
                    vec![ClusterNodeName::parse("node-1").expect("valid name")],
                ),
            ],
            Vec::new(),
        )
    }

    #[test]
    fn unchanged_schedule_has_no_apply_work() {
        let existing = schedule(nonzero!(1usize));
        assert_eq!(
            ScheduleDelta::classify(&existing, &existing),
            ScheduleDelta::Unchanged
        );
    }

    #[test]
    fn relay_capacity_is_a_dynamic_delta() {
        let existing = schedule(nonzero!(1usize));
        let desired = schedule(nonzero!(5usize));
        assert_eq!(
            ScheduleDelta::classify(&existing, &desired),
            ScheduleDelta::Dynamic(vec![DynamicModelUpdate::RelayCapacity {
                relay: named("events"),
                capacity: nonzero!(5usize),
            }])
        );
    }

    #[test]
    fn schema_and_schedule_residue_changes_rebuild() {
        let existing = schedule(nonzero!(1usize));
        let mut schema_change = schedule(nonzero!(1usize));
        let Model::Relay(relay) = schema_change.nodes[0].config.as_mut() else {
            panic!("test node should contain a relay");
        };
        relay.schema = named("event_v2");
        assert_eq!(
            ScheduleDelta::classify(&existing, &schema_change),
            ScheduleDelta::Rebuild
        );

        let mut fingerprint_change = schedule(nonzero!(1usize));
        fingerprint_change.nodes[0].schema_fingerprint = [2; 32];
        assert_eq!(
            ScheduleDelta::classify(&existing, &fingerprint_change),
            ScheduleDelta::Rebuild
        );
    }

    #[test]
    fn junction_filter_is_dynamic_and_attachment_changes_swap_the_entity() {
        let junction = CreateJunction {
            name: named("route_events"),
            from: ProcessorInputs::single(named("incoming")),
            output_routes: ProcessorOutputs::new(vec![ProcessorOutput::with_flush_policy(
                named("outgoing"),
                FlushPolicy::Each {
                    interval: "100ms".to_string(),
                    max_batch_size: "1MiB".to_string(),
                },
            )]),
            branched_by: BranchSelection::unbranched(),
            mode: AckMode::Attached,
            filter_where: None,
            materialized_state: Vec::new(),
        };
        let existing = DomainSchedule::new(
            DomainName::parse("testing").expect("valid domain"),
            vec![
                ScheduledNode::new(Model::Junction(junction.clone()))
                    .with_effective_branching(Some(Vec::new()), None)
                    .with_schema_fingerprint([1; 32])
                    .placed_on(
                        Some(ClusterNodeName::parse("node-1").expect("valid name")),
                        vec![ClusterNodeName::parse("node-1").expect("valid name")],
                    ),
            ],
            Vec::new(),
        );
        let mut dynamic_config = junction;
        dynamic_config.filter_where = Some(Expression::Literal(Literal::Bool(true)));
        let mut desired = existing.clone();
        *desired.nodes[0].config = Model::Junction(dynamic_config);
        assert_eq!(
            ScheduleDelta::classify(&existing, &desired),
            ScheduleDelta::Dynamic(vec![DynamicModelUpdate::Processor {
                kind: ModelKind::Junction,
                processor: named("route_events"),
            }])
        );

        let mut structural = existing.clone();
        let Model::Junction(config) = structural.nodes[0].config.as_mut() else {
            panic!("test node should contain a junction");
        };
        config.mode = AckMode::Detached;
        assert_eq!(
            ScheduleDelta::classify(&existing, &structural),
            ScheduleDelta::EntitySwap {
                entities: vec![NodeRef {
                    kind: ModelKind::Junction,
                    identifier: named("route_events"),
                }],
                reassignments: Vec::new(),
                dynamic_updates: Vec::new(),
            }
        );
    }

    #[test]
    fn ingestor_changes_swap_even_when_schedule_placement_residue_changes() {
        let existing = ingestor_schedule("ingress_a");
        let mut desired = ingestor_schedule("ingress_b");
        desired.nodes[0].schema_fingerprint = [2; 32];
        desired.nodes[0].primary_node = Some(ClusterNodeName::parse("node-2").expect("valid name"));
        desired.nodes[0].assigned_nodes =
            vec![ClusterNodeName::parse("node-2").expect("valid name")];

        let ScheduleDelta::EntitySwap {
            entities,
            reassignments,
            dynamic_updates,
        } = ScheduleDelta::classify(&existing, &desired)
        else {
            panic!("ingestor changes should use entity swap");
        };
        assert_eq!(entities.len(), 1);
        assert_eq!(entities[0].kind, ModelKind::Ingestor);
        assert_eq!(entities[0].identifier, named("event_source"));
        assert_eq!(reassignments, entities);
        assert!(dynamic_updates.is_empty());
    }

    #[test]
    fn emitter_flush_is_dynamic_and_client_changes_swap_despite_fingerprint_changes() {
        let emitter = CreateEmitter {
            name: named("event_sink"),
            from: nervix_models::ProcessorInputs::single(named("events")),
            encode_using_codec: Some(named("event_codec")),
            sink: Box::new(EmitSink::ZeroMq {
                client: named("sink_a"),
            }),
            flush_policy: FlushPolicy::Each {
                interval: "30s".to_string(),
                max_batch_size: "1MiB".to_string(),
            },
            error_policies: ErrorPolicies::handled_by_log(),
            publishing_mode: EmitterPublishingMode::NoAck {
                retry_policy: RetryPolicy {
                    backoff: "250ms".to_string(),
                    max_backoff: "30s".to_string(),
                },
            },
            mode: AckMode::Attached,
            construction: RouteConstruction::default(),
            materialized_state: Vec::new(),
        };
        let existing = DomainSchedule::new(
            DomainName::parse("testing").expect("valid domain"),
            vec![
                ScheduledNode::new(Model::Emitter(emitter.clone()))
                    .with_effective_branching(Some(Vec::new()), None)
                    .with_schema_fingerprint([1; 32])
                    .placed_on(
                        Some(ClusterNodeName::parse("node-1").expect("valid name")),
                        vec![ClusterNodeName::parse("node-1").expect("valid name")],
                    ),
            ],
            Vec::new(),
        );

        let mut dynamic_emitter = emitter.clone();
        dynamic_emitter.flush_policy = FlushPolicy::Immediate;
        let mut dynamic = existing.clone();
        *dynamic.nodes[0].config = Model::Emitter(dynamic_emitter.clone());
        assert_eq!(
            ScheduleDelta::classify(&existing, &dynamic),
            ScheduleDelta::Dynamic(vec![DynamicModelUpdate::Emitter {
                emitter: named("event_sink"),
                config: Box::new(dynamic_emitter),
            }])
        );

        let mut swapped_emitter = emitter;
        swapped_emitter.sink = Box::new(EmitSink::ZeroMq {
            client: named("sink_b"),
        });
        let mut swapped = existing.clone();
        *swapped.nodes[0].config = Model::Emitter(swapped_emitter);
        swapped.nodes[0].schema_fingerprint = [2; 32];
        assert_eq!(
            ScheduleDelta::classify(&existing, &swapped),
            ScheduleDelta::EntitySwap {
                entities: vec![NodeRef {
                    kind: ModelKind::Emitter,
                    identifier: named("event_sink"),
                }],
                reassignments: Vec::new(),
                dynamic_updates: Vec::new(),
            }
        );
    }

    #[test]
    fn owner_change_alone_reassigns_without_a_rebuild() {
        let existing = ingestor_schedule("ingress_a");
        let mut desired = ingestor_schedule("ingress_a");
        desired.nodes[0].primary_node = Some(ClusterNodeName::parse("node-2").expect("valid name"));
        desired.nodes[0].assigned_nodes =
            vec![ClusterNodeName::parse("node-2").expect("valid name")];

        assert_eq!(
            ScheduleDelta::classify(&existing, &desired),
            ScheduleDelta::EntitySwap {
                entities: Vec::new(),
                reassignments: vec![NodeRef {
                    kind: ModelKind::Ingestor,
                    identifier: named("event_source"),
                }],
                dynamic_updates: Vec::new(),
            }
        );
    }

    #[test]
    fn replica_set_change_alone_reassigns_without_a_rebuild() {
        let existing = ingestor_schedule("ingress_a");
        let mut desired = ingestor_schedule("ingress_a");
        desired.nodes[0].assigned_nodes = vec![
            ClusterNodeName::parse("node-1").expect("valid name"),
            ClusterNodeName::parse("node-3").expect("valid name"),
        ];

        assert_eq!(
            ScheduleDelta::classify(&existing, &desired),
            ScheduleDelta::EntitySwap {
                entities: Vec::new(),
                reassignments: vec![NodeRef {
                    kind: ModelKind::Ingestor,
                    identifier: named("event_source"),
                }],
                dynamic_updates: Vec::new(),
            }
        );
    }

    #[test]
    fn a_placement_policy_change_only_applies_its_reassignments() {
        let placement_node = |policy: PlacementPolicy| {
            ScheduledNode::new(Model::Placement(
                CreatePlacement::new(
                    named("keep_local"),
                    vec![named("event_source")],
                    vec![named("events")],
                    policy,
                    Some(nonzero!(1u64)),
                )
                .expect("valid placement"),
            ))
            .with_schema_fingerprint([1; 32])
        };
        let mut existing = ingestor_schedule("ingress_a");
        push_scheduled(
            &mut existing,
            placement_node(PlacementPolicy::PreferColocation),
        );
        let mut desired = ingestor_schedule("ingress_a");
        desired.nodes[0].primary_node = Some(ClusterNodeName::parse("node-2").expect("valid name"));
        desired.nodes[0].assigned_nodes =
            vec![ClusterNodeName::parse("node-2").expect("valid name")];
        push_scheduled(
            &mut desired,
            placement_node(PlacementPolicy::RequireColocation),
        );

        assert_eq!(
            ScheduleDelta::classify(&existing, &desired),
            ScheduleDelta::EntitySwap {
                entities: Vec::new(),
                reassignments: vec![NodeRef {
                    kind: ModelKind::Ingestor,
                    identifier: named("event_source"),
                }],
                dynamic_updates: Vec::new(),
            }
        );
    }

    #[test]
    fn a_model_change_and_an_unrelated_move_apply_together() {
        let mut existing = schedule(nonzero!(1usize));
        push_scheduled(
            &mut existing,
            first_scheduled(ingestor_schedule("ingress_a")),
        );
        let mut desired = schedule(nonzero!(5usize));
        let mut moved_ingestor = first_scheduled(ingestor_schedule("ingress_a"));
        moved_ingestor.primary_node = Some(ClusterNodeName::parse("node-3").expect("valid name"));
        moved_ingestor.assigned_nodes = vec![ClusterNodeName::parse("node-3").expect("valid name")];
        push_scheduled(&mut desired, moved_ingestor);

        assert_eq!(
            ScheduleDelta::classify(&existing, &desired),
            ScheduleDelta::EntitySwap {
                entities: Vec::new(),
                reassignments: vec![NodeRef {
                    kind: ModelKind::Ingestor,
                    identifier: named("event_source"),
                }],
                dynamic_updates: vec![DynamicModelUpdate::RelayCapacity {
                    relay: named("events"),
                    capacity: nonzero!(5usize),
                }],
            }
        );
    }
}
