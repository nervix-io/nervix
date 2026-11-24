//! Shuttle checks of the order in which a stopping domain's concurrent proposals reach the
//! consensus state machine.
//!
//! Layer: test harness.
//! - **Owns.** The invariant that the clauses of a stop's requests acting together, an operator's
//!   `ENFORCE AFTER` against the licensing stop's forced ending, beside a stale owner's ending, a
//!   completed drain, a `CANCEL` and a late request, commit exactly one ending, which resolves
//!   exactly the requests held when it applied, in every order their entries apply.
//! - **Depends on.** The production command apply, the licensing gate fixture and the Shuttle
//!   runner.
//! - **Must not know.** The leader's stop owner, the interconnect or any node's teardown.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::{collections::BTreeSet, time::Duration};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    ClusterNodeName, CommandExecutionReference, DomainName, DomainStatus, LicensingStopReason,
    OperatorStopRequest, StopEnding, StopRequest, StopRequestId, Timestamp, WaitLimit,
};
use nervix_primitives::{
    sync::{Arc, blocking::Mutex},
    thread,
};
use openraft::{type_config::alias::LeaderIdOf, vote::RaftLeaderIdExt};

use crate::{
    AppliedConsensusCommand, ConsensusCommand, ConsensusResponse, LogIdOf, StateMachineData,
    TypeConfig, apply_consensus_command,
    licensing::stop_gate_tests::{ephemeral_state, pass_expiry},
};

const DOMAIN: &str = "orders";
const ENFORCING: &str = "operator-enforce";
const CANCELLING: &str = "operator-cancel";
const LATE: &str = "operator-late";
const PARTICIPANT_JOINS: &str = "a participant that panics fails the execution before its join";

fn domain() -> DomainName {
    DomainName::parse(DOMAIN).assured("the literal follows the name rule")
}

fn reference(literal: &str) -> CommandExecutionReference {
    CommandExecutionReference::parse(literal).assured("the literal is a valid reference")
}

/// An operator's request under `literal`, made at the instant the fixture's stop began.
fn operator_request(literal: &str, limit: Option<WaitLimit>) -> StopRequest {
    StopRequest::Operator {
        reference: reference(literal),
        request: OperatorStopRequest {
            requested_at: Timestamp::from_unix_nanos(1),
            limit,
        },
    }
}

/// One cluster's consensus state, applying each proposal as the state machine applies the
/// committed entry that carries it: at the next log index, one entry at a time.
struct Fixture {
    state: StateMachineData,
    index: u64,
    /// The requests the stop held when the ending that applied took effect, recorded in the same
    /// apply.
    held_at_ending: Option<BTreeSet<StopRequestId>>,
    /// How many ending proposals changed the domain.
    endings: usize,
}

impl Fixture {
    /// A running domain whose operator asked for `STOP ENFORCE AFTER 0s` and another for
    /// `STOP CANCEL AFTER 0s`, after which the license expired and the licensing stop joined: the
    /// clauses of all three act at once.
    fn new() -> (Self, u64) {
        let mut fixture = Self {
            state: ephemeral_state(),
            index: 0,
            held_at_ending: None,
            endings: 0,
        };
        let mut running = crate::tests::running_domain_state(DOMAIN);
        running.status = DomainStatus::Running;
        let put = fixture.apply(&ConsensusCommand::PutDomain {
            domain: Box::new(running),
            mutation: None,
        });
        assert_eq!(put.response, ConsensusResponse::Applied);
        let enforce = Some(WaitLimit::Enforce {
            after: Duration::ZERO,
        });
        let cancel = Some(WaitLimit::Cancel {
            after: Duration::ZERO,
        });
        fixture.request(operator_request(ENFORCING, enforce));
        fixture.request(operator_request(CANCELLING, cancel));
        let reason: LicensingStopReason = pass_expiry(&mut fixture.state);
        fixture.request(StopRequest::Licensing(reason));
        let stopping = fixture
            .state
            .domains
            .get(&domain())
            .verified("the fixture created the domain")
            .stopping()
            .verified("the first request began the stop")
            .clone();
        assert_eq!(stopping.request_ids().len(), 3, "every request joined");
        (fixture, stopping.began_at_revision)
    }

    fn apply(&mut self, command: &ConsensusCommand) -> AppliedConsensusCommand {
        self.index = self
            .index
            .checked_add(1)
            .assured("a check applies a handful of entries");
        let node = ClusterNodeName::parse("node-1").assured("the literal follows the name rule");
        let leader = <LeaderIdOf<TypeConfig> as RaftLeaderIdExt>::new_committed(1, node);
        self.state.last_applied_log_id = Some(LogIdOf::new(leader, self.index));
        apply_consensus_command(&mut self.state, command)
    }

    fn request(&mut self, request: StopRequest) -> AppliedConsensusCommand {
        self.apply(&ConsensusCommand::RequestDomainStop {
            domain_id: domain(),
            request: Box::new(request),
            mutation: None,
        })
    }

    fn held(&self) -> Option<BTreeSet<StopRequestId>> {
        let state = self.state.domains.get(&domain())?;
        let stopping = state.stopping()?;
        Some(stopping.request_ids().into_iter().collect())
    }

    /// Proposes `ending` for the stop that began at `began_at_revision`, recording what the stop
    /// held if this proposal is the one that ended it.
    fn end(&mut self, began_at_revision: u64, ending: StopEnding) {
        let held = self.held();
        let applied = self.apply(&ConsensusCommand::EndDomainStop {
            domain_id: domain(),
            began_at_revision,
            ending,
        });
        assert_eq!(
            applied.response,
            ConsensusResponse::Applied,
            "an ending that finds the stop over changes nothing and conflicts with nothing"
        );
        if applied.domains_changed {
            self.endings = self
                .endings
                .checked_add(1)
                .assured("a check proposes three endings");
            self.held_at_ending = held;
        }
    }
}

/// A participant that read the stop's fence earlier and proposes `ending` once it is scheduled
/// again, as an owner whose clause acted does, or a previous leader's owner that has not yet seen
/// its tenure end.
fn propose_ending(
    fixture: &Arc<Mutex<Fixture>>,
    began_at_revision: u64,
    ending: StopEnding,
) -> thread::JoinHandle<()> {
    let fixture = fixture.clone();
    thread::spawn(move || {
        thread::yield_now();
        fixture.lock().end(began_at_revision, ending);
    })
}

fn clauses_acting_together_commit_one_ending() {
    let (fixture, began_at_revision) = Fixture::new();
    let fixture = Arc::new(Mutex::new(fixture));
    let enforced_by_operator = propose_ending(
        &fixture,
        began_at_revision,
        StopEnding::Enforced {
            by: StopRequestId::Operator(reference(ENFORCING)),
        },
    );
    let enforced_by_licensing = propose_ending(
        &fixture,
        began_at_revision,
        StopEnding::Enforced {
            by: StopRequestId::Licensing,
        },
    );
    let completed = propose_ending(&fixture, began_at_revision, StopEnding::Completed);
    let cancel = thread::spawn({
        let fixture = fixture.clone();
        move || {
            thread::yield_now();
            let mut fixture = fixture.lock();
            let applied = fixture.apply(&ConsensusCommand::WithdrawDomainStopRequest {
                domain_id: domain(),
                reference: reference(CANCELLING),
            });
            assert_eq!(applied.response, ConsensusResponse::Applied);
            // The other requests still hold the stop, so the withdrawal never calls it off.
            let status = &fixture
                .state
                .domains
                .get(&domain())
                .verified("the fixture created the domain")
                .status;
            assert_ne!(
                status,
                &DomainStatus::Running,
                "a withdrawal called off a held stop"
            );
        }
    });
    let late = thread::spawn({
        let fixture = fixture.clone();
        move || {
            thread::yield_now();
            let applied = fixture.lock().request(operator_request(LATE, None));
            assert_eq!(
                applied.response,
                ConsensusResponse::Applied,
                "a request joins a stopping domain or changes nothing for a stopped one"
            );
        }
    });
    for participant in [
        enforced_by_operator,
        enforced_by_licensing,
        completed,
        cancel,
        late,
    ] {
        participant.join().assured(PARTICIPANT_JOINS);
    }

    let fixture = fixture.lock();
    assert_eq!(fixture.endings, 1, "exactly one ending committed");
    let state = fixture
        .state
        .domains
        .get(&domain())
        .verified("the fixture created the domain");
    assert_eq!(state.status, DomainStatus::Stopped);
    let ending = state
        .stop_ending()
        .verified("the committed ending is recorded");
    let resolved = ending.resolved.iter().cloned().collect::<BTreeSet<_>>();
    let held = fixture
        .held_at_ending
        .clone()
        .verified("the ending that applied recorded what the stop held");
    assert_eq!(
        resolved, held,
        "the ending resolves exactly the requests the stop held when it applied"
    );
    assert!(
        resolved.contains(&StopRequestId::Licensing),
        "nothing withdraws the licensing request"
    );
    assert!(
        resolved.contains(&StopRequestId::Operator(reference(ENFORCING))),
        "the enforcing request is resolved by whichever ending applied"
    );
    if let StopEnding::Enforced { by } = &ending.ending {
        assert!(
            resolved.contains(by),
            "an enforced ending names a request the stop held"
        );
    }
}

/// An operator's `ENFORCE AFTER` and the licensing stop's forced ending acting at the same
/// instant, a stale owner's ending, a completed drain, another operator's `CANCEL` and a late
/// `STOP` reach the state machine in every order: exactly one ending commits, and it resolves
/// exactly the requests the stop held when it applied.
#[test]
fn shuttle_requests_whose_clauses_act_together_commit_exactly_one_ending() {
    nervix_model_harness::shuttle::check_interleavings(clauses_acting_together_commit_one_ending);
}
