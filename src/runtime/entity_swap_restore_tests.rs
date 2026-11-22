//! The restore of a failed entity swap, at its owner.
//!
//! Layer: test harness.
//! - **Owns.** Tests that a swap failing while it replaces any entity of a mixed swap puts back
//!   every entity it had begun to replace, with the tasks, routing, consumer registrations and
//!   placement the domain ran before, and that state only the previous execution reads survives.
//! - **Depends on.** The production entity swap and restore, the swap fault injection and the
//!   runtime's test fixtures.
//! - **Must not know.** Control-plane orchestration or connector behavior.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::collections::BTreeMap;

use nervix_models::{
    BranchSelection, CreateDeduplicator, CreateGenerator, CreateReingestor, CreateRelay,
    CreateSchema, DomainSchedule, MaterializedRelayState, MessageErrorPolicy, OutputBranch,
    ParseAsType, ProcessorInputs, ProcessorOutput, ProcessorOutputs, RelayBranching, SchemaField,
    StatePurge,
};
use nonzero_ext::nonzero;

use super::*;

fn local_node() -> ClusterNodeName {
    ClusterNodeName::parse("node-1").assured("the test node is an identifier-shaped literal")
}

fn relay(name: &str, materialized_state: Option<MaterializedRelayState>) -> ScheduledNode {
    scheduled_model(nervix_models::Model::Relay(CreateRelay {
        labels: Default::default(),
        name: named(name),
        schema: named("payload"),
        buffer: nonzero!(4usize),
        branching: RelayBranching::unbranched(),
        materialized_state,
    }))
}

/// A deduplicator, a reingestor behind it and a generator over a materialized relay, all on
/// relays of one schema.
fn mixed_schedule(domain: &DomainName) -> DomainSchedule {
    let nodes = vec![
        scheduled_model(nervix_models::Model::Schema(CreateSchema {
            labels: Default::default(),
            name: named("payload"),
            fields: vec![SchemaField {
                name: named("tenant"),
                ty: ParseAsType::String,
                optional: false,
                sensitive: false,
            }],
        })),
        relay("incoming", None),
        relay("unique", None),
        relay("outgoing", None),
        relay("generated", None),
        relay(
            "notifications",
            Some(MaterializedRelayState::LastByTimestamp),
        ),
        scheduled_model(nervix_models::Model::Deduplicator(CreateDeduplicator {
            labels: Default::default(),
            name: named("dedupe"),
            from: ProcessorInputs::single(named("incoming")),
            output_routes: with_inherit_all(ProcessorOutputs::single(named("unique")))
                .with_flush_policy(FlushPolicy::Immediate),
            branched_by: BranchSelection::unbranched(),
            deduplicate_on: vec![expression("input.tenant")],
            max_time: "10m".to_string(),
            mode: AckMode::Attached,
            filter_where: None,
            materialized_state: Vec::new(),
        })),
        scheduled_model(nervix_models::Model::Reingestor(CreateReingestor {
            labels: Default::default(),
            name: named("repartition"),
            from: ProcessorInputs::single(named("unique")),
            output_routes: with_inherit_all(ProcessorOutputs::single(named("outgoing")))
                .with_flush_policy(FlushPolicy::Immediate)
                .with_branch(OutputBranch::Unbranched),
            mode: AckMode::Attached,
            materialized_state: Vec::new(),
            filter_where: None,
        })),
        scheduled_model(nervix_models::Model::Generator(CreateGenerator {
            labels: Default::default(),
            name: named("synth"),
            materialized_relay: named("notifications"),
            branched_by: BranchSelection::unbranched(),
            each: "1h".parse().assured("the fixture cadence is positive"),
            output_routes: ProcessorOutputs::new(vec![ProcessorOutput {
                relay: named("generated"),
                construction: nervix_plexus::parse_route_construction(
                    "SET tenant = relay_state.notifications.tenant",
                )
                .assured("the fixture route is valid Plexus"),
                flush_policy: Some(FlushPolicy::Immediate),
                message_error_policy: MessageErrorPolicy::Log,
                branch: None,
            }]),
        })),
    ];
    DomainSchedule::new(domain.clone(), nodes, Vec::new())
}

fn entity(kind: ModelKind, name: &str) -> NodeRef {
    NodeRef::new(kind, named::<ModelName>(name))
}

/// Every entity of the mixed schedule a swap replaces.
fn mixed_entities() -> Vec<NodeRef> {
    let entities = vec![
        entity(ModelKind::Relay, "notifications"),
        entity(ModelKind::Deduplicator, "dedupe"),
        entity(ModelKind::Reingestor, "repartition"),
        entity(ModelKind::Generator, "synth"),
    ];
    SortedSet::from_unsorted(entities).into_vec()
}

fn mixed_swap(state_purges: BTreeMap<NodeRef, Vec<StatePurge>>) -> EntitySwapExecution {
    EntitySwapExecution {
        entities: mixed_entities(),
        reassignments: Vec::new(),
        dynamic_updates: Vec::new(),
        state_purges,
        gate_relays: Vec::new(),
        forced_recovery_relays: Vec::new(),
    }
}

/// A running domain execution of the mixed schedule on a one-node loopback cluster.
async fn running_mixed_domain() -> (Runtime, DomainName) {
    let runtime = Runtime::default();
    attach_loopback_cluster(&runtime, &local_node()).await;
    let domain = domain("default");
    runtime.sync_domains(&BTreeMap::from([(
        domain.clone(),
        unpaced_domain_state(domain.as_str()),
    )]));
    runtime
        .rebuild_domain_from_schedule(&local_node(), &domain, Some(mixed_schedule(&domain)), true)
        .await
        .assured("the mixed fixture builds a running domain");
    (runtime, domain)
}

/// The consumers one relay counts for its single unit, by acknowledgement mode.
#[derive(Debug, PartialEq, Eq)]
struct CountedConsumers {
    relay: RelayName,
    attached: usize,
    detached: usize,
}

/// What a domain's execution runs on this node: the parts a restore must put back.
#[derive(Debug, PartialEq, Eq)]
struct RunningParts {
    processor_units: BTreeSet<NodeUnitRef>,
    emitter_units: BTreeSet<NodeUnitRef>,
    generator_units: BTreeSet<NodeUnitRef>,
    reingestor_tasks: BTreeMap<NodeRef, usize>,
    branched_entrypoints: BTreeMap<ModelName, usize>,
    placement_units: BTreeSet<NodeUnitRef>,
    relay_state_units: Vec<RelayUnitRef>,
    relay_owner_units: Vec<RelayUnitRef>,
    consumers: Vec<CountedConsumers>,
}

fn running_parts(runtime: &Runtime, domain: &DomainName) -> RunningParts {
    let execution = runtime
        .inner
        .executions
        .get(domain)
        .assured("the test domain keeps its execution installed");
    let mut reingestor_tasks = BTreeMap::new();
    for (node, tasks) in &execution.reingestor_tasks {
        reingestor_tasks.insert(node.clone(), tasks.len());
    }
    let mut branched_entrypoints = BTreeMap::new();
    for (name, runtimes) in &execution.branched_entrypoints {
        branched_entrypoints.insert(name.clone(), runtimes.len());
    }
    let published = execution.routing.published_snapshot();
    let mut consumers = Vec::new();
    for relay in [
        "incoming",
        "unique",
        "outgoing",
        "generated",
        "notifications",
    ] {
        let relay = named::<RelayName>(relay);
        let services = published
            .relay_services
            .get(&relay)
            .assured("the fixture relay has its boundary");
        consumers.push(CountedConsumers {
            attached: services.counted_unit_runtime_consumers(AckMode::Attached, Unit::SINGLE),
            detached: services.counted_unit_runtime_consumers(AckMode::Detached, Unit::SINGLE),
            relay,
        });
    }
    RunningParts {
        processor_units: execution.node_tasks.keys().cloned().collect(),
        emitter_units: execution.emitter_tasks.keys().cloned().collect(),
        generator_units: execution.generator_tasks.keys().cloned().collect(),
        reingestor_tasks,
        branched_entrypoints,
        placement_units: execution.placement_tasks.keys().cloned().collect(),
        relay_state_units: execution.relay_state_tasks.keys().cloned().collect(),
        relay_owner_units: execution.relay_owner_tasks.keys().cloned().collect(),
        consumers,
    }
}

/// The revision and published routing of a domain's execution.
struct PublishedExecution {
    revision: Arc<ExecutionRevision>,
    routing: StdArc<DomainRoutingSnapshot>,
}

fn published_execution(runtime: &Runtime, domain: &DomainName) -> PublishedExecution {
    let execution = runtime
        .inner
        .executions
        .get(domain)
        .assured("the test domain keeps its execution installed");
    PublishedExecution {
        revision: execution.revision.clone(),
        routing: execution.routing.published_snapshot(),
    }
}

#[nervix_primitives::test]
async fn a_swap_failing_at_any_entity_puts_back_every_entity_it_replaced() {
    let (runtime, domain) = running_mixed_domain().await;
    let before = running_parts(&runtime, &domain);
    let published = published_execution(&runtime, &domain);
    assert!(
        before.processor_units.len() == 1
            && before.generator_units.len() == 1
            && before.reingestor_tasks.len() == 1
            && !before.relay_state_units.is_empty(),
        "the fixture runs every kind the swap replaces: {before:?}"
    );

    for failing in mixed_entities() {
        nervix_primitives::task::consume_budget().await;
        runtime
            .inner
            .fault_injection
            .fail_entity_swaps_while_replacing_on(&local_node(), &domain, failing.clone());
        let desired = ExecutionRevision::from_schedule(&mixed_schedule(&domain))
            .assured("the mixed fixture schedules a complete revision");
        let error = runtime
            .swap_scheduled_nodes(&domain, desired, &mixed_swap(BTreeMap::new()))
            .await
            .expect_err("the armed entity fails the swap");
        runtime
            .inner
            .fault_injection
            .clear_entity_swap_failures_on(&local_node(), &domain);
        assert_eq!(
            format!("{error:#}"),
            format!(
                "failed to build domain execution for '{domain}': injected entity-level schedule \
                 apply failure"
            ),
            "a swap failing at {failing:?} reports that failure alone: everything it replaced \
             came back"
        );
        assert_eq!(
            running_parts(&runtime, &domain),
            before,
            "a swap failing at {failing:?} puts back every task, placement and registration"
        );
        let after = published_execution(&runtime, &domain);
        assert!(
            Arc::ptr_eq(&after.revision, &published.revision),
            "a swap failing at {failing:?} keeps the installed revision"
        );
        assert!(
            StdArc::ptr_eq(&after.routing, &published.routing),
            "a swap failing at {failing:?} publishes no routing"
        );
        let staged = runtime
            .inner
            .executions
            .get(&domain)
            .assured("the test domain keeps its execution installed")
            .routing
            .staged();
        assert!(
            StdArc::ptr_eq(&staged, &published.routing),
            "a swap failing at {failing:?} leaves nothing staged for a later publication"
        );
    }

    let desired = ExecutionRevision::from_schedule(&mixed_schedule(&domain))
        .assured("the mixed fixture schedules a complete revision");
    runtime
        .swap_scheduled_nodes(&domain, desired.clone(), &mixed_swap(BTreeMap::new()))
        .await
        .expect("the swap applies once nothing fails it");
    assert_eq!(
        running_parts(&runtime, &domain),
        before,
        "the swap that applies runs the same parts as the execution it replaced"
    );
    assert!(Arc::ptr_eq(
        &published_execution(&runtime, &domain).revision,
        &desired
    ));
}

#[nervix_primitives::test]
async fn a_failed_swap_keeps_the_state_its_previous_deduplicator_reads() {
    let (runtime, domain) = running_mixed_domain().await;
    let deduplicator = entity(ModelKind::Deduplicator, "dedupe");
    let placement = runtime
        .state_placement_for_unit(
            &domain,
            RuntimeStateKind::Deduplicator,
            ModelKind::Deduplicator,
            &deduplicator.identifier,
            Unit::SINGLE,
            None,
        )
        .assured("the deduplicator's unit has a state placement");
    runtime
        .replicated_deduplicator_state(placement.clone())
        .assured("the previous deduplicator's keyspace is held in memory");
    let purges = BTreeMap::from([(deduplicator.clone(), vec![StatePurge::DeduplicatorKeyspace])]);

    for failing in mixed_entities() {
        nervix_primitives::task::consume_budget().await;
        runtime
            .inner
            .fault_injection
            .fail_entity_swaps_while_replacing_on(&local_node(), &domain, failing.clone());
        let desired = ExecutionRevision::from_schedule(&mixed_schedule(&domain))
            .assured("the mixed fixture schedules a complete revision");
        let error = runtime
            .swap_scheduled_nodes(&domain, desired, &mixed_swap(purges.clone()))
            .await
            .expect_err("the armed entity fails the swap");
        assert!(
            format!("{error:#}").ends_with("injected entity-level schedule apply failure"),
            "a swap failing at {failing:?} reports that failure alone: {error:#}"
        );
        runtime
            .inner
            .fault_injection
            .clear_entity_swap_failures_on(&local_node(), &domain);
        assert!(
            runtime
                .inner
                .replicated_deduplicator_states
                .contains_key(&placement),
            "a swap failing at {failing:?} purges nothing the previous deduplicator reads"
        );
    }

    let desired = ExecutionRevision::from_schedule(&mixed_schedule(&domain))
        .assured("the mixed fixture schedules a complete revision");
    runtime
        .swap_scheduled_nodes(&domain, desired, &mixed_swap(purges))
        .await
        .expect("the swap applies once nothing fails it");
    assert!(
        !runtime
            .inner
            .replicated_deduplicator_states
            .contains_key(&placement),
        "the swap that applies purges the previous key's keyspace"
    );
}
