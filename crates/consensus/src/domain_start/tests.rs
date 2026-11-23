//! Layer: test harness.
//! - **Owns.** Owner checks of the start a `START` command commits through consensus.
//! - **Depends on.** The state machine's apply and the command execution records.
//! - **Must not know.** Sessions, placement or runtime activation.

use std::{collections::BTreeSet, time::Duration};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    ClusterNodeIdentity, ClusterNodeIncarnation, ClusterNodeName, CommandExecutionReference,
    DomainClockState, DomainConfig, DomainName, DomainPace, DomainStartPoint, DomainState,
    DomainStatus, DomainTimeRate, StartDomain, Timestamp, UserName,
};

use super::{DomainStart, DomainStartClock, DomainStartRefusal};
use crate::{
    CommandExecution, CommandExecutionAdmissionPolicy, CommandExecutionEffect, CommittedLeaderIdOf,
    ConsensusCommand, ConsensusConflict, ConsensusResponse, DomainPlanningInputs, LogIdOf,
    StateMachineData, TypeConfig, apply_consensus_command,
};

fn domain() -> DomainName {
    DomainName::parse("tenant").assured("the test domain is an identifier-shaped literal")
}

fn reference(index: u64) -> CommandExecutionReference {
    CommandExecutionReference::parse(format!("018bcfe5-6800-7000-8000-{index:012x}"))
        .assured("the test command reference is a UUIDv7 value")
}

/// A paced pace with a one-second period and no skew.
fn paced() -> DomainPace {
    DomainPace::Paced {
        period: "1s"
            .parse()
            .assured("one second is a positive fixture cadence"),
        skew: "0s"
            .parse()
            .assured("zero nanoseconds is a valid fixture skew"),
    }
}

fn stopped_domain(pace: DomainPace) -> DomainState {
    DomainState {
        labels: nervix_models::EntityLabels::default(),
        id: domain(),
        config: DomainConfig { pace },
        status: DomainStatus::Stopped,
        start_version: 3,
        last_start: DomainStartPoint::Resume,
        clock: None,
        stop: None,
    }
}

/// A state machine holding `state`, at an applied revision of 41.
fn state_with(domain_state: DomainState) -> StateMachineData {
    use openraft::vote::RaftLeaderId as _;

    let mut state = StateMachineData::default();
    state.domains.insert(domain(), domain_state);
    let leader = CommittedLeaderIdOf::<TypeConfig>::new(
        1,
        ClusterNodeName::parse("node-1").assured("the test node name is a valid literal"),
    );
    state.last_applied_log_id = Some(LogIdOf::new(leader, 41));
    state
}

/// Admits the command `index` with `effect`, holding the test domain's mutation lease.
fn admit(state: &mut StateMachineData, index: u64, effect: CommandExecutionEffect) {
    let owner =
        UserName::parse("operator").assured("the test owner is an identifier-shaped literal");
    // Admitted ten seconds after the instant the test references' UUIDv7 prefix names.
    let admitted_at = Timestamp::from_unix_nanos(1_700_000_010_000_000_000);
    let execution = CommandExecution::applying(
        reference(index),
        owner,
        Some(domain()),
        [7; 32],
        admitted_at,
        effect,
    );
    let admitted = apply_consensus_command(
        state,
        &ConsensusCommand::AdmitCommandExecution {
            execution: Box::new(execution),
            mutation_domains: BTreeSet::from([domain()]),
            policy: CommandExecutionAdmissionPolicy::at(admitted_at, Duration::from_secs(60), 16),
        },
    );
    assert_eq!(admitted.response, ConsensusResponse::Applied);
}

fn start_effect() -> CommandExecutionEffect {
    CommandExecutionEffect::StartDomain {
        start: StartDomain {
            start: DomainStartPoint::Resume,
        },
        started_at: None,
    }
}

fn unpaced_start() -> DomainStart {
    DomainStart {
        start: DomainStartPoint::Resume,
        clock: DomainStartClock::Unpaced,
    }
}

fn inputs(state: &StateMachineData) -> DomainPlanningInputs {
    state.domain_planning_inputs(&domain())
}

fn propose(
    state: &mut StateMachineData,
    index: u64,
    inputs: DomainPlanningInputs,
    start: DomainStart,
) -> ConsensusResponse {
    apply_consensus_command(
        state,
        &ConsensusCommand::StartDomain {
            reference: reference(index),
            inputs: Box::new(inputs),
            start: Box::new(start),
        },
    )
    .response
}

fn refusal(response: ConsensusResponse) -> DomainStartRefusal {
    let ConsensusResponse::Conflict(ConsensusConflict::DomainStart { refusal }) = response else {
        panic!("the start is refused as a typed start refusal: {response:?}");
    };
    refusal
}

#[test]
fn a_start_command_starts_its_stopped_domain_once_and_records_the_revision() {
    let mut state = state_with(stopped_domain(DomainPace::Unpaced));
    admit(&mut state, 1, start_effect());
    let captured = inputs(&state);

    let applied = apply_consensus_command(
        &mut state,
        &ConsensusCommand::StartDomain {
            reference: reference(1),
            inputs: Box::new(captured.clone()),
            start: Box::new(unpaced_start()),
        },
    );
    assert_eq!(applied.response, ConsensusResponse::Applied);
    assert!(
        applied.domains_changed,
        "the start changes the domain record"
    );
    let started = state
        .domains
        .get(&domain())
        .verified("the domain exists above");
    assert_eq!(started.status, DomainStatus::Running);
    assert_eq!(started.start_version, 4);
    assert_eq!(started.clock, None);
    assert!(!state.domain_clock_authorities.contains_key(&domain()));
    let execution = state
        .command_executions
        .get(&reference(1))
        .verified("the start command was admitted above");
    assert_eq!(execution.domain_start_revision(), Some(41));

    // A repeated proposal, as after a lost reply, finds the recorded start and applies nothing.
    let repeated = apply_consensus_command(
        &mut state,
        &ConsensusCommand::StartDomain {
            reference: reference(1),
            inputs: Box::new(captured),
            start: Box::new(unpaced_start()),
        },
    );
    assert_eq!(repeated.response, ConsensusResponse::Applied);
    assert!(!repeated.domains_changed);
    assert_eq!(
        state
            .domains
            .get(&domain())
            .verified("the domain exists above")
            .start_version,
        4,
        "the domain started once"
    );
}

#[test]
fn a_paced_start_commits_its_mapping_and_clock_authority() {
    let mut state = state_with(stopped_domain(paced()));
    admit(&mut state, 1, start_effect());
    let mapping = DomainClockState::new(
        Timestamp::from_unix_nanos(100),
        Timestamp::from_unix_nanos(1_000),
        DomainTimeRate::ONE,
    );
    let authority = ClusterNodeIdentity::new(
        ClusterNodeName::parse("node-2").assured("the test node name is a valid literal"),
        ClusterNodeIncarnation::new(9),
    );
    let captured = inputs(&state);

    let response = propose(
        &mut state,
        1,
        captured,
        DomainStart {
            start: DomainStartPoint::At {
                timestamp: Timestamp::from_unix_nanos(1_000),
                time_rate: DomainTimeRate::ONE,
            },
            clock: DomainStartClock::Paced {
                mapping: mapping.clone(),
                authority: authority.clone(),
            },
        },
    );
    assert_eq!(response, ConsensusResponse::Applied);
    let started = state
        .domains
        .get(&domain())
        .verified("the domain exists above");
    assert_eq!(started.clock, Some(mapping));
    let owner = state
        .domain_clock_authorities
        .get(&domain())
        .verified("a paced start assigns its clock authority");
    assert_eq!(owner.owner(), Some(&authority));
}

#[test]
fn a_start_refuses_what_is_no_start_command_of_its_domain() {
    let mut state = state_with(stopped_domain(DomainPace::Unpaced));
    let captured = inputs(&state);
    assert!(matches!(
        refusal(propose(&mut state, 1, captured.clone(), unpaced_start())),
        DomainStartRefusal::NotAStart { .. }
    ));

    admit(
        &mut state,
        2,
        CommandExecutionEffect::StopDomain {
            source: "STOP".to_owned(),
            stop: nervix_models::StopDomain::default(),
            start_version: 3,
        },
    );
    assert!(matches!(
        refusal(propose(&mut state, 2, captured, unpaced_start())),
        DomainStartRefusal::NotAStart { .. }
    ));
    assert_eq!(
        state
            .domains
            .get(&domain())
            .verified("the domain exists above")
            .status,
        DomainStatus::Stopped
    );
}

#[test]
fn a_start_refuses_planning_inputs_that_changed_since_it_placed() {
    let mut state = state_with(stopped_domain(DomainPace::Unpaced));
    admit(&mut state, 1, start_effect());
    let captured = inputs(&state);
    state
        .domains
        .get_mut(&domain())
        .verified("the domain exists above")
        .config
        .pace = paced();

    let response = propose(&mut state, 1, captured, unpaced_start());
    assert!(
        matches!(
            response,
            ConsensusResponse::Conflict(ConsensusConflict::DomainPlanningInputs { .. })
        ),
        "a changed domain record is a typed planning-input conflict: {response:?}"
    );
    let execution = state
        .command_executions
        .get(&reference(1))
        .verified("the start command was admitted above");
    assert_eq!(execution.domain_start_revision(), None);
}

#[test]
fn a_start_refuses_a_domain_that_is_not_stopped_or_a_start_of_another_pace() {
    let mut state = state_with(DomainState {
        status: DomainStatus::Running,
        ..stopped_domain(DomainPace::Unpaced)
    });
    admit(&mut state, 1, start_effect());
    let captured = inputs(&state);
    assert!(matches!(
        refusal(propose(&mut state, 1, captured, unpaced_start())),
        DomainStartRefusal::NotStopped { .. }
    ));

    let mut state = state_with(stopped_domain(paced()));
    admit(&mut state, 1, start_effect());
    let captured = inputs(&state);
    assert!(matches!(
        refusal(propose(&mut state, 1, captured, unpaced_start())),
        DomainStartRefusal::PaceMismatch { .. }
    ));
}

#[test]
fn a_start_refuses_a_domain_another_execution_holds() {
    let mut state = state_with(stopped_domain(DomainPace::Unpaced));
    admit(&mut state, 1, start_effect());
    let captured = inputs(&state);
    // The domain no longer holds the command's lease, as after its fence was recovered.
    assert!(
        state.domain_mutations.contains_key(&domain()),
        "the start command holds the domain's lease"
    );
    state.domain_mutations.remove(&domain());

    let response = propose(&mut state, 1, captured, unpaced_start());
    assert!(
        matches!(
            response,
            ConsensusResponse::Conflict(ConsensusConflict::DomainMutation { .. })
        ),
        "a start whose lease is gone is refused: {response:?}"
    );
}
