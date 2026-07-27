use nervix_models::{DomainSchedule, DynamicModelUpdate, ModelKind, QuiesceLevel, ScheduledNode};

use crate::registry::RegistryEntity;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ScheduleDelta {
    Unchanged,
    Dynamic(Vec<DynamicModelUpdate>),
    EntitySwap {
        entities: Vec<RegistryEntity>,
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
        for desired_node in &desired.nodes {
            let Some(existing_node) = existing.nodes.iter().find(|existing_node| {
                existing_node.kind == desired_node.kind
                    && existing_node.identifier == desired_node.identifier
            }) else {
                return Self::Rebuild;
            };
            let aspects = existing_node
                .config
                .change_aspects_against(&desired_node.config);
            let level = aspects.quiesce_level();
            let emitter_schema_fingerprint_may_change =
                desired_node.kind == ModelKind::Emitter && level == QuiesceLevel::EntityPause;
            if !Self::same_schedule_residue(
                existing_node,
                desired_node,
                emitter_schema_fingerprint_may_change,
            ) {
                return Self::Rebuild;
            }
            if aspects.is_empty() {
                continue;
            }
            match level {
                QuiesceLevel::Dynamic => {
                    if aspects.dynamic_updates().is_empty() {
                        return Self::Rebuild;
                    }
                    updates.extend(aspects.dynamic_updates().iter().cloned());
                }
                QuiesceLevel::EntityPause => {
                    entities.push(RegistryEntity {
                        kind: desired_node.kind,
                        identifier: desired_node.identifier.clone(),
                    });
                    updates.extend(aspects.dynamic_updates().iter().cloned());
                }
                QuiesceLevel::DomainPause => return Self::Rebuild,
            }
        }

        if !entities.is_empty() {
            entities.sort_by(|left, right| {
                left.kind
                    .as_str()
                    .cmp(right.kind.as_str())
                    .then_with(|| left.identifier.as_str().cmp(right.identifier.as_str()))
            });
            Self::EntitySwap {
                entities,
                dynamic_updates: updates,
            }
        } else if updates.is_empty() {
            Self::Unchanged
        } else {
            Self::Dynamic(updates)
        }
    }

    fn same_schedule_residue(
        existing: &ScheduledNode,
        desired: &ScheduledNode,
        allow_schema_fingerprint_change: bool,
    ) -> bool {
        let ScheduledNode {
            identifier: existing_identifier,
            kind: existing_kind,
            config: _,
            effective_branching: existing_effective_branching,
            effective_branching_schema: existing_effective_branching_schema,
            schema_fingerprint: existing_schema_fingerprint,
            kafka_partition_schedule: existing_kafka_partition_schedule,
            primary_node: existing_primary_node,
            assigned_nodes: existing_assigned_nodes,
        } = existing;
        let ScheduledNode {
            identifier: desired_identifier,
            kind: desired_kind,
            config: _,
            effective_branching: desired_effective_branching,
            effective_branching_schema: desired_effective_branching_schema,
            schema_fingerprint: desired_schema_fingerprint,
            kafka_partition_schedule: desired_kafka_partition_schedule,
            primary_node: desired_primary_node,
            assigned_nodes: desired_assigned_nodes,
        } = desired;

        existing_identifier == desired_identifier
            && existing_kind == desired_kind
            && existing_effective_branching == desired_effective_branching
            && existing_effective_branching_schema == desired_effective_branching_schema
            && (allow_schema_fingerprint_change
                || existing_schema_fingerprint == desired_schema_fingerprint)
            && existing_kafka_partition_schedule == desired_kafka_partition_schedule
            && existing_primary_node == desired_primary_node
            && existing_assigned_nodes == desired_assigned_nodes
    }
}

#[cfg(test)]
mod tests {
    use nervix_models::{
        AckMode, BranchSelection, CreateEmitter, CreateJunction, CreateRelay, Domain,
        DomainSchedule, DynamicModelUpdate, EmitSink, ErrorPolicies, Expression, Identifier,
        Literal, Model, ModelKind, ProcessorInputs, ProcessorOutput, ProcessorOutputs,
        RelayBranching, RouteConstruction, ScheduledNode,
    };

    use super::ScheduleDelta;

    fn identifier(raw: &str) -> Identifier {
        Identifier::parse(raw).expect("valid identifier")
    }

    fn schedule(capacity: usize) -> DomainSchedule {
        DomainSchedule {
            domain: Domain::parse("testing").expect("valid domain"),
            nodes: vec![ScheduledNode {
                identifier: identifier("events"),
                kind: ModelKind::Relay,
                config: Box::new(Model::Relay(CreateRelay {
                    name: identifier("events"),
                    schema: identifier("event"),
                    buffer: capacity,
                    branching: RelayBranching::unbranched(),
                    materialized_state: None,
                })),
                effective_branching: Some(Vec::new()),
                effective_branching_schema: None,
                schema_fingerprint: [1; 32],
                kafka_partition_schedule: None,
                primary_node: Some("node-1".to_string()),
                assigned_nodes: vec!["node-1".to_string()],
            }],
        }
    }

    #[test]
    fn unchanged_schedule_has_no_apply_work() {
        let existing = schedule(1);
        assert_eq!(
            ScheduleDelta::classify(&existing, &existing),
            ScheduleDelta::Unchanged
        );
    }

    #[test]
    fn relay_capacity_is_a_dynamic_delta() {
        let existing = schedule(1);
        let desired = schedule(5);
        assert_eq!(
            ScheduleDelta::classify(&existing, &desired),
            ScheduleDelta::Dynamic(vec![DynamicModelUpdate::RelayCapacity {
                relay: identifier("events"),
                capacity: 5,
            }])
        );
    }

    #[test]
    fn schema_and_schedule_residue_changes_rebuild() {
        let existing = schedule(1);
        let mut schema_change = schedule(1);
        let Model::Relay(relay) = schema_change.nodes[0].config.as_mut() else {
            panic!("test node should contain a relay");
        };
        relay.schema = identifier("event_v2");
        assert_eq!(
            ScheduleDelta::classify(&existing, &schema_change),
            ScheduleDelta::Rebuild
        );

        let mut fingerprint_change = schedule(1);
        fingerprint_change.nodes[0].schema_fingerprint = [2; 32];
        assert_eq!(
            ScheduleDelta::classify(&existing, &fingerprint_change),
            ScheduleDelta::Rebuild
        );
    }

    #[test]
    fn junction_filter_is_dynamic_and_attachment_changes_swap_the_entity() {
        let junction = CreateJunction {
            name: identifier("route_events"),
            from: ProcessorInputs::single(identifier("incoming")),
            output_routes: ProcessorOutputs::new(vec![ProcessorOutput::with_flush_policy(
                identifier("outgoing"),
                "100ms".to_string(),
                Some("1MiB".to_string()),
            )]),
            branched_by: BranchSelection::unbranched(),
            mode: AckMode::Attached,
            filter_where: None,
            materialized_state: Vec::new(),
        };
        let existing = DomainSchedule {
            domain: Domain::parse("testing").expect("valid domain"),
            nodes: vec![ScheduledNode {
                identifier: identifier("route_events"),
                kind: ModelKind::Junction,
                config: Box::new(Model::Junction(junction.clone())),
                effective_branching: Some(Vec::new()),
                effective_branching_schema: None,
                schema_fingerprint: [1; 32],
                kafka_partition_schedule: None,
                primary_node: Some("node-1".to_string()),
                assigned_nodes: vec!["node-1".to_string()],
            }],
        };
        let mut dynamic_config = junction.clone();
        dynamic_config.filter_where = Some(Expression::Literal(Literal::Bool(true)));
        let mut desired = existing.clone();
        desired.nodes[0].config = Box::new(Model::Junction(dynamic_config.clone()));
        assert_eq!(
            ScheduleDelta::classify(&existing, &desired),
            ScheduleDelta::Dynamic(vec![DynamicModelUpdate::Junction {
                junction: identifier("route_events"),
                config: dynamic_config,
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
                entities: vec![crate::registry::RegistryEntity {
                    kind: ModelKind::Junction,
                    identifier: identifier("route_events"),
                }],
                dynamic_updates: Vec::new(),
            }
        );
    }

    #[test]
    fn emitter_flush_is_dynamic_and_client_changes_swap_despite_fingerprint_changes() {
        let emitter = CreateEmitter {
            name: identifier("event_sink"),
            from_relay: identifier("events"),
            collect_policy: None,
            encode_using_codec: Some(identifier("event_codec")),
            sink: EmitSink::ZeroMq {
                client: identifier("sink_a"),
            },
            flush_each: "30s".to_string(),
            max_batch_size: Some("1MiB".to_string()),
            error_policies: ErrorPolicies::handled_by_log(),
            mode: AckMode::Attached,
            construction: RouteConstruction::default(),
            materialized_state: Vec::new(),
        };
        let existing = DomainSchedule {
            domain: Domain::parse("testing").expect("valid domain"),
            nodes: vec![ScheduledNode {
                identifier: emitter.name.clone(),
                kind: ModelKind::Emitter,
                config: Box::new(Model::Emitter(emitter.clone())),
                effective_branching: Some(Vec::new()),
                effective_branching_schema: None,
                schema_fingerprint: [1; 32],
                kafka_partition_schedule: None,
                primary_node: Some("node-1".to_string()),
                assigned_nodes: vec!["node-1".to_string()],
            }],
        };

        let mut dynamic_emitter = emitter.clone();
        dynamic_emitter.flush_each = "IMMEDIATE".to_string();
        dynamic_emitter.max_batch_size = None;
        let mut dynamic = existing.clone();
        dynamic.nodes[0].config = Box::new(Model::Emitter(dynamic_emitter.clone()));
        assert_eq!(
            ScheduleDelta::classify(&existing, &dynamic),
            ScheduleDelta::Dynamic(vec![DynamicModelUpdate::Emitter {
                emitter: identifier("event_sink"),
                config: dynamic_emitter,
            }])
        );

        let mut swapped_emitter = emitter;
        swapped_emitter.sink = EmitSink::ZeroMq {
            client: identifier("sink_b"),
        };
        let mut swapped = existing.clone();
        swapped.nodes[0].config = Box::new(Model::Emitter(swapped_emitter));
        swapped.nodes[0].schema_fingerprint = [2; 32];
        assert_eq!(
            ScheduleDelta::classify(&existing, &swapped),
            ScheduleDelta::EntitySwap {
                entities: vec![crate::registry::RegistryEntity {
                    kind: ModelKind::Emitter,
                    identifier: identifier("event_sink"),
                }],
                dynamic_updates: Vec::new(),
            }
        );
    }
}
