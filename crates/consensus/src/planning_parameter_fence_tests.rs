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
        ConsensusCommand::UpdateKafkaPartitionSchedule {
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
        ConsensusCommand::PutDomainAndSchedule {
            inputs: Box::new(inputs.clone()),
            domain: Box::new(DomainState {
                id: inputs.domain().clone(),
                config: nervix_models::DomainConfig {
                    pace: nervix_models::DomainPace::Unpaced,
                    placement: nervix_models::PlacementPolicy::Neutral,
                },
                status: DomainStatus::Stopped,
                start_version: 0,
                last_start: nervix_models::DomainStartPoint::Resume,
                clock: None,
            }),
            schedule: Some(Box::new(schedule)),
            mutation: None,
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
    let ConsensusCommand::PutDomainAndSchedule {
        domain: changed, ..
    } = schedule_commands(&captured)
        .pop()
        .assured("the four-entry fixture sequence ends with the domain writer")
    else {
        panic!("the last fixture writer changes domain state");
    };
    let applied = apply_consensus_command(
        &mut state,
        &ConsensusCommand::PutDomain {
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
