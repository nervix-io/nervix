//! Committed cluster policy fences every schedule derived from a captured domain snapshot.
//!
//! Layer: test harness.
//! - **Owns.** Deterministic publication-order regressions at the replicated planning boundary.
//! - **Depends on.** Current consensus commands, parameter records and schedule contracts.
//! - **Must not know.** Runtime installation, session timing or historical stored shapes.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "tests replicated planning input admission")
)]

use super::*;

fn seeded_state() -> StateMachineData {
    let mut state = StateMachineData::default();
    let seeded = apply_consensus_command(
        &mut state,
        &ConsensusCommand::SeedClusterParameters {
            values: ClusterParameterValues::default(),
        },
    );
    assert_eq!(seeded.response, ConsensusResponse::Applied);
    state
}

fn publish_timeout(state: &mut StateMachineData, seconds: u64) {
    let current = state
        .parameters
        .assured("every caller starts from seeded_state");
    let reference = nervix_models::CommandExecutionReference::parse(format!(
        "018bcfe5-6800-7000-8000-{:012x}",
        current.revision.get()
    ))
    .assured("the bounded fixture revision fills a valid UUIDv7 suffix");
    let at = nervix_models::Timestamp::from_unix_nanos(1_700_000_010_000_000_000);
    let changes = nervix_models::ClusterParameterUpdates::try_from(vec![
        nervix_models::ClusterParameterChange::NodeUnavailabilityTimeout(Duration::from_secs(
            seconds,
        )),
    ])
    .assured("one positive timeout assignment satisfies the update rules");
    let execution = CommandExecution::applying(
        reference.clone(),
        UserName::parse("operator").assured("operator is an identifier-shaped literal"),
        None,
        [3; 32],
        at,
        CommandExecutionEffect::AlterCluster {
            changes,
            applied: None,
        },
    );
    let admitted = apply_consensus_command(
        state,
        &ConsensusCommand::AdmitCommandExecution {
            execution: Box::new(execution),
            mutation_domains: BTreeSet::new(),
            policy: CommandExecutionAdmissionPolicy::at(at, Duration::from_secs(60), 16),
        },
    );
    assert_eq!(admitted.response, ConsensusResponse::Applied);
    let inputs = ClusterParameterPlanningInputs {
        current,
        schedule: ClusterSchedule::from(&state.schedule).domains,
        domains: (&state.domains).into(),
        resources: state.resources.versions.values().cloned().collect(),
    };
    let published = apply_consensus_command(
        state,
        &ConsensusCommand::AlterClusterParameters {
            reference,
            inputs: Box::new(inputs),
        },
    );
    assert_eq!(published.response, ConsensusResponse::Applied);
    assert!(
        state
            .parameters
            .verified("the successful parameter apply above installed its record")
            .revision
            > current.revision
    );
}

fn schedule_commands(inputs: &DomainPlanningInputs) -> Vec<ConsensusCommand> {
    let schedule = DomainSchedule::new(inputs.domain().clone(), [], Vec::new());
    vec![
        ConsensusCommand::ReplaceDomainSchedule {
            inputs: Box::new(inputs.clone()),
            schedule: Some(Box::new(schedule.clone())),
            mutation: None,
        },
        ConsensusCommand::UpdatePartitionTopology {
            tenure: LeaderTenure {
                leader_id: ClusterNodeName::parse("node-1")
                    .assured("valid literal cluster node name"),
                term: 0,
            },
            inputs: Box::new(inputs.clone()),
            schedule: Box::new(schedule.clone()),
        },
        ConsensusCommand::ApplyAutomaticDomainSchedule {
            fence: AutomaticScheduleFence {
                leader_tenure: LeaderTenure {
                    leader_id: ClusterNodeName::parse("node-1")
                        .assured("node-1 is a valid literal cluster node name"),
                    term: 0,
                },
            },
            inputs: Box::new(inputs.clone()),
            schedule: Some(Box::new(schedule.clone())),
        },
    ]
}

#[test]
fn planning_parameter_fence_policy_change_refuses_each_schedule_writer() {
    let mut state = seeded_state();
    let domain = DomainName::parse("orders").assured("orders is an identifier-shaped literal");
    let captured = state.domain_planning_inputs(&domain);
    publish_timeout(&mut state, 20);
    let before = state.clone();
    for command in schedule_commands(&captured) {
        let mut attempted = state.clone();
        let applied = apply_consensus_command(&mut attempted, &command);
        assert!(
            matches!(
                &applied.response,
                ConsensusResponse::Conflict(ConsensusConflict::Reason(reason))
                    if reason == "domain 'orders' cluster parameters changed"
            ),
            "a plan captured before the parameter publication must conflict: {:?}",
            applied.response
        );
        assert_eq!(attempted, before);
        assert!(!applied.schedule_changed && !applied.domains_changed);
    }
}

#[test]
fn planning_parameter_fence_returning_policy_does_not_revive_a_captured_plan() {
    let mut state = seeded_state();
    let domain = DomainName::parse("orders").assured("orders is an identifier-shaped literal");
    let captured = state.domain_planning_inputs(&domain);
    let captured_record = captured
        .parameters()
        .copied()
        .assured("seeded_state installs the parameter record before capture");
    publish_timeout(&mut state, 20);
    publish_timeout(&mut state, 10);
    let current = state
        .parameters
        .verified("both successful parameter publications retained the seeded record");
    assert_eq!(current.values, captured_record.values);
    assert!(current.revision > captured_record.revision);
    let rejection = validate_domain_planning_inputs(&state, &captured)
        .expect_err("a later parameter revision cannot reuse a captured planning identity");
    assert!(matches!(
        rejection.current_context(),
        DomainPlanningInputConflict::Parameters(changed) if changed == &domain
    ));
}

#[test]
fn planning_parameter_fence_preserves_existing_conflict_precedence() {
    let mut state = seeded_state();
    let domain = DomainName::parse("orders").assured("orders is an identifier-shaped literal");
    let captured = state.domain_planning_inputs(&domain);
    publish_timeout(&mut state, 20);
    let changed = Box::new(DomainState {
        id: domain.clone(),
        config: nervix_models::DomainConfig {
            pace: nervix_models::DomainPace::Unpaced,
        },
        status: nervix_models::DomainStatus::Stopped,
        start_version: 0,
        last_start: nervix_models::DomainStartPoint::Resume,
        clock: None,
    });
    let applied = apply_consensus_command(
        &mut state,
        &ConsensusCommand::PutDomain {
            domain_quota: nervix_licensing_vocabulary::Quota::Unlimited,
            domain: changed,
            mutation: None,
        },
    );
    assert_eq!(applied.response, ConsensusResponse::Applied);
    let rejection = validate_domain_planning_inputs(&state, &captured)
        .expect_err("the domain and policy both changed");
    assert!(matches!(
        rejection.current_context(),
        DomainPlanningInputConflict::Domain(changed) if changed == &domain
    ));
}

#[test]
fn planning_parameter_fence_current_inputs_still_publish_each_schedule_writer() {
    let mut state = seeded_state();
    let domain = DomainName::parse("orders").assured("orders is an identifier-shaped literal");
    publish_timeout(&mut state, 20);
    let current = state.domain_planning_inputs(&domain);
    for command in schedule_commands(&current) {
        let mut attempted = state.clone();
        let applied = apply_consensus_command(&mut attempted, &command);
        assert_eq!(applied.response, ConsensusResponse::Applied);
        assert!(applied.schedule_changed);
        assert_eq!(
            attempted.schedule.domains.get(&domain).map(|s| &s.domain),
            Some(&domain)
        );
    }
}

#[cfg(feature = "shuttle")]
#[test]
fn shuttle_partition_observation_and_instance_assignment_publish_one_epoch() {
    use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};

    use nervix_models::{
        ExternalPartitionSet, InheritedPartitionLayout, PartitionEpochNumber,
        PartitionSourceFingerprint, ResolvedBranchPartitions, SourcePartitionObservation, Unit,
    };
    use nervix_primitives::{
        sync::{Arc, blocking::Mutex},
        thread,
    };

    fn assignment(count: u16) -> DomainSchedule {
        let domain = DomainName::parse("orders").assured("valid fixture domain");
        let branch = nervix_models::BranchName::parse("by_tenant").assured("valid fixture branch");
        let source = nervix_models::IngestorName::parse("input").assured("valid fixture ingestor");
        let mut values =
            nervix_arbitrary::Arbitrary::new(&[], nervix_arbitrary::Domain::Vocabulary);
        let model = values
            .model_of(nervix_arbitrary::ModelVariant::Ingestor)
            .try_map_resource_versions(|_, _| Ok::<_, ()>(1_u64))
            .assured("the fixture pins its resources");
        let nervix_models::Model::Ingestor(mut intake) = model else {
            unreachable!("the requested fixture is an ingestor")
        };
        intake.name = source.clone();
        intake.input = values.ingestor_input_of(nervix_models::IngestorEntityKind::Kafka(
            nervix_models::KafkaIngestorEntity::Topic,
        ));
        let nervix_models::IngestorInput::Transport(input) = &mut intake.input else {
            unreachable!("Kafka is a transport")
        };
        let nervix_models::IngestorEntity::Kafka(nervix_models::KafkaIngestor::Topic(topic)) =
            &mut input.source
        else {
            unreachable!("the fixture selects Kafka")
        };
        topic.instances = nervix_models::IngestorInstances::PerPartition;
        let units = (0..u64::from(count))
            .map(|index| Unit::try_instance(index).assured("the bounded instance fits"))
            .collect::<Vec<_>>();
        let node = nervix_models::ScheduledNode::new(
            nervix_models::Model::Ingestor(intake),
            nervix_models::SchemaFingerprint::from_digest([1; 32]),
        )
        .with_units(&units);
        let mut schedule = DomainSchedule::new(domain, [node], Vec::new());
        let count = NonZeroU16::new(count).assured("fixture counts are positive");
        schedule.partition_topology.sources.insert(
            source,
            SourcePartitionObservation {
                configuration: PartitionSourceFingerprint::from_digest([2; 32]),
                partitions: Some(ExternalPartitionSet::Partitioned(NonZeroU32::from(count))),
                epoch: u64::from(count.get()),
            },
        );
        schedule
            .partition_topology
            .branches
            .insert(branch.clone(), InheritedPartitionLayout::Identity(count));
        schedule.partition_epochs.insert(
            branch,
            nervix_models::BranchPartitionEpoch {
                number: PartitionEpochNumber::new(
                    NonZeroU64::new(u64::from(count.get())).assured("positive fixture epoch"),
                ),
                partitions: ResolvedBranchPartitions::Inherited(count),
                transition: None,
            },
        );
        schedule
    }

    fn assert_coherent(state: &StateMachineData, domain: &DomainName) {
        let schedule = state
            .schedule
            .domains
            .get(domain)
            .assured("the initial schedule stays installed");
        let count = schedule
            .partition_topology
            .branches
            .values()
            .next()
            .assured("the fixture declares a layout")
            .partitions()
            .count()
            .get();
        assert_eq!(schedule.nodes[0].units.len(), usize::from(count));
        assert_eq!(
            schedule
                .partition_epochs
                .values()
                .next()
                .assured("the fixture has an epoch")
                .number
                .get(),
            u64::from(count)
        );
        assert_eq!(
            schedule
                .partition_topology
                .sources
                .values()
                .next()
                .assured("the fixture has a source")
                .partitions
                .assured("the source is observed")
                .count()
                .get(),
            u32::from(count)
        );
    }

    nervix_model_harness::shuttle::check_dfs(
        || {
            let mut state = seeded_state();
            let initial = assignment(8);
            let domain = initial.domain.clone();
            state.replace_domain_schedule(&domain, Some(&initial));
            let captured = state.domain_planning_inputs(&domain);
            let state = Arc::new(Mutex::new(state));
            let observer = thread::spawn({
                let state = state.clone();
                let captured = captured.clone();
                move || {
                    apply_consensus_command_at(
                        &mut state.lock(),
                        &ConsensusCommand::UpdatePartitionTopology {
                            tenure: LeaderTenure {
                                leader_id: ClusterNodeName::parse("node-1")
                                    .assured("valid fixture node"),
                                term: 7,
                            },
                            inputs: Box::new(captured),
                            schedule: Box::new(assignment(12)),
                        },
                        AppliedEntryContext { leader_term: 7 },
                    )
                    .response
                }
            });
            let reconfiguration = thread::spawn({
                let state = state.clone();
                move || {
                    apply_consensus_command_at(
                        &mut state.lock(),
                        &ConsensusCommand::ReplaceDomainSchedule {
                            inputs: Box::new(captured),
                            schedule: Some(Box::new(assignment(16))),
                            mutation: None,
                        },
                        AppliedEntryContext { leader_term: 7 },
                    )
                    .response
                }
            });
            for _ in 0..3 {
                assert_coherent(&state.lock(), &domain);
                thread::yield_now();
            }
            let observed = observer.join().assured("the observation task completes");
            let changed = reconfiguration
                .join()
                .assured("the assignment task completes");
            assert_ne!(
                observed == ConsensusResponse::Applied,
                changed == ConsensusResponse::Applied,
                "exactly one plan can publish from one captured revision"
            );
            assert_coherent(&state.lock(), &domain);
        },
        Some(256),
    );
}
