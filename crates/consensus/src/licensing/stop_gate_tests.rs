//! The licensing stop gate over committed licensing time and the ephemeral binding.
//!
//! Layer: test harness.
//! - **Owns.** Deterministic gate, refusal, stop-cause and supersession proofs through the
//!   production consensus apply path, and the activated state other consensus owners' tests close
//!   the gate on partway through their own operations.
//! - **Depends on.** Real protocol signers, typed commands and current domain records.
//! - **Must not know.** Clocks, service networking or the leader's stop loop.

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    ActivationKind, DomainStartPoint, DomainStatus, DomainStopCause, LicensingStopReason, Timestamp,
};

use super::{verification_tests::SignedFixture, *};
use crate::{
    ConsensusCommand, ConsensusConflict, ConsensusResponse, StateMachineData,
    apply_consensus_command,
};

const FIRST: u128 = 0xE1;
const SECOND: u128 = 0xE2;

fn activate(
    fixture: &SignedFixture,
    state: &mut StateMachineData,
    activation: u128,
    created: u64,
) -> ConsensusResponse {
    let decision = fixture.ephemeral_decision(activation, created, 600);
    let held = state
        .licensing
        .as_ref()
        .verified("identity installed")
        .trust
        .clone();
    let received = fixture.received(&decision, &held);
    let binding = ActivationBinding::from_verified(&decision, "TESTABCD".to_owned())
        .assured("the fixture decision carries current coordinates");
    let command = ConsensusCommand::ActivateLicense {
        binding: Box::new(binding),
        decision: Box::new(received),
    };
    apply_consensus_command(state, &command).response
}

fn expiry(state: &StateMachineData) -> Timestamp {
    let LicensingMode::Activated(activation) =
        &state.licensing.as_ref().verified("identity installed").mode
    else {
        panic!("the fixture activated the cluster");
    };
    activation
        .binding
        .ephemeral_expires_at
        .verified("an ephemeral binding carries its expiry")
}

fn advance(state: &mut StateMachineData, observed_at: Timestamp) {
    let command = ConsensusCommand::AdvanceLicensingTime { observed_at };
    assert_eq!(
        apply_consensus_command(state, &command).response,
        ConsensusResponse::Applied
    );
}

fn put(state: &mut StateMachineData, name: &str, status: DomainStatus) -> ConsensusResponse {
    let mut domain = crate::tests::running_domain_state(name);
    domain.status = status;
    let command = ConsensusCommand::PutDomain {
        domain: Box::new(domain),
        mutation: None,
        domain_quota: nervix_licensing_vocabulary::Quota::Unlimited,
    };
    apply_consensus_command(state, &command).response
}

fn domain(name: &str) -> nervix_models::DomainName {
    nervix_models::DomainName::parse(name).assured("the literal follows the name rule")
}

fn start(state: &mut StateMachineData, name: &str) -> ConsensusResponse {
    let command = ConsensusCommand::StartDomain {
        domain_id: domain(name),
        start: DomainStartPoint::Resume,
        clock: None,
        authority: None,
        mutation: None,
    };
    apply_consensus_command(state, &command).response
}

fn stop_cause(state: &StateMachineData, name: &str) -> Option<DomainStopCause> {
    state
        .domains
        .get(&domain(name))
        .verified("the fixture created the domain")
        .stop_cause()
}

/// Requests the licensing stop of `name` and ends it once its work is at rest, as the leader's
/// stop owner does, and returns the response to the request.
fn stop_for_licensing(
    state: &mut StateMachineData,
    name: &str,
    reason: LicensingStopReason,
) -> ConsensusResponse {
    let request = ConsensusCommand::RequestDomainStop {
        domain_id: domain(name),
        request: Box::new(nervix_models::StopRequest::Licensing(reason)),
        mutation: None,
    };
    let requested = apply_consensus_command(state, &request);
    let Some(began_at_revision) = state
        .domains
        .get(&domain(name))
        .verified("the fixture created the domain")
        .stopping()
        .map(|stopping| stopping.began_at_revision)
    else {
        return requested.response;
    };
    let end = ConsensusCommand::EndDomainStop {
        domain_id: domain(name),
        began_at_revision,
        ending: nervix_models::StopEnding::Completed,
    };
    assert_eq!(
        apply_consensus_command(state, &end).response,
        ConsensusResponse::Applied
    );
    requested.response
}

/// A cluster whose ephemeral activation is committed and has not expired, so the gate is open.
pub(crate) fn ephemeral_state() -> StateMachineData {
    let fixture = SignedFixture::new();
    let mut state = fixture.state();
    assert_eq!(
        activate(&fixture, &mut state, FIRST, 0),
        ConsensusResponse::Applied
    );
    assert_eq!(state.licensing_stop(), None, "the activation is live");
    state
}

/// Commits licensing time at the ephemeral expiry, which closes the gate, and returns its reason.
pub(crate) fn pass_expiry(state: &mut StateMachineData) -> LicensingStopReason {
    let expires_at = expiry(state);
    advance(state, expires_at);
    let reason = LicensingStopReason::EphemeralExpired {
        expired_at: expires_at,
    };
    assert_eq!(state.licensing_stop(), Some(reason));
    reason
}

#[test]
fn the_gate_closes_exactly_when_committed_time_reaches_an_ephemeral_expiry() {
    let fixture = SignedFixture::new();
    let mut state = fixture.state();
    assert_eq!(
        state.licensing_stop(),
        None,
        "free mode never stops processing"
    );
    assert_eq!(
        activate(&fixture, &mut state, FIRST, 0),
        ConsensusResponse::Applied
    );
    let expires_at = expiry(&state);
    assert_eq!(state.licensing_stop(), None);
    advance(
        &mut state,
        Timestamp::from_unix_nanos(expires_at.unix_nanos() - 1),
    );
    assert_eq!(
        state.licensing_stop(),
        None,
        "the gate is open before the expiry"
    );
    advance(&mut state, expires_at);
    let reason = LicensingStopReason::EphemeralExpired {
        expired_at: expires_at,
    };
    assert_eq!(state.licensing_stop(), Some(reason));
    // The gate reads the committed binding, never the decision's verification.
    state.licensing_verification = Some(Err(LicenseDecisionError::Metadata));
    assert_eq!(state.licensing_stop(), Some(reason));
    assert_eq!(
        reason.to_string(),
        format!("ephemeral activation expired at {expires_at}")
    );
}

#[test]
fn a_persistent_activation_never_closes_the_gate() {
    let fixture = SignedFixture::new();
    let mut state = fixture.state();
    let received = fixture.received(&fixture.decision, &[]);
    let command = ConsensusCommand::ActivateLicense {
        binding: Box::new(fixture.binding_of(ActivationKind::Persistent, &received)),
        decision: Box::new(received),
    };
    assert_eq!(
        apply_consensus_command(&mut state, &command).response,
        ConsensusResponse::Applied
    );
    advance(&mut state, Timestamp::from_unix_nanos(i64::MAX));
    assert_eq!(state.licensing_stop(), None);
}

#[test]
fn a_closed_gate_refuses_every_change_that_runs_a_domain_and_records_its_stops() {
    let fixture = SignedFixture::new();
    let mut state = fixture.state();
    assert_eq!(
        activate(&fixture, &mut state, FIRST, 0),
        ConsensusResponse::Applied
    );
    assert_eq!(
        put(&mut state, "running", DomainStatus::Running),
        ConsensusResponse::Applied
    );
    assert_eq!(
        put(&mut state, "paused", DomainStatus::Paused),
        ConsensusResponse::Applied
    );
    assert_eq!(
        put(&mut state, "idle", DomainStatus::Stopped),
        ConsensusResponse::Applied
    );
    let expires_at = expiry(&state);
    advance(&mut state, expires_at);
    let reason = LicensingStopReason::EphemeralExpired {
        expired_at: expires_at,
    };
    let refused = ConsensusResponse::Conflict(ConsensusConflict::LicensingStop { reason });

    assert_eq!(start(&mut state, "idle"), refused);
    let resume = ConsensusCommand::ResumeDomain {
        domain_id: domain("paused"),
        mutation: None,
    };
    assert_eq!(
        apply_consensus_command(&mut state, &resume).response,
        refused
    );
    assert_eq!(put(&mut state, "idle", DomainStatus::Running), refused);
    let paused = state
        .domains
        .get(&domain("paused"))
        .verified("created above");
    assert_eq!(paused.status, DomainStatus::Paused);
    // A new domain is new usage, which the closed gate admits under no quota.
    let ConsensusResponse::Conflict(ConsensusConflict::LicensingQuota { refusal }) =
        put(&mut state, "created", DomainStatus::Stopped)
    else {
        panic!("a closed gate refuses a new domain record");
    };
    assert_eq!(
        refusal.state,
        nervix_licensing_vocabulary::AdmissionState::Stopped
    );
    // A stopped record may still be written: administration of a stopped domain continues.
    assert_eq!(
        put(&mut state, "idle", DomainStatus::Stopped),
        ConsensusResponse::Applied
    );

    for name in ["running", "paused"] {
        let request = ConsensusCommand::RequestDomainStop {
            domain_id: domain(name),
            request: Box::new(nervix_models::StopRequest::Licensing(reason)),
            mutation: None,
        };
        let applied = apply_consensus_command(&mut state, &request);
        assert_eq!(applied.response, ConsensusResponse::Applied);
        assert!(applied.domains_changed);
        assert_eq!(
            state
                .domains
                .get(&domain(name))
                .verified("created above")
                .status,
            DomainStatus::Stopping,
            "the licensing request begins a stop that waits for the work in motion"
        );
        assert_eq!(
            stop_cause(&state, name),
            Some(DomainStopCause::Licensing(reason))
        );
        let began_at_revision = state
            .domains
            .get(&domain(name))
            .verified("created above")
            .stopping()
            .verified("the domain is stopping, as asserted above")
            .began_at_revision;
        let end = ConsensusCommand::EndDomainStop {
            domain_id: domain(name),
            began_at_revision,
            ending: nervix_models::StopEnding::Completed,
        };
        assert_eq!(
            apply_consensus_command(&mut state, &end).response,
            ConsensusResponse::Applied
        );
        assert_eq!(
            state
                .domains
                .get(&domain(name))
                .verified("created above")
                .status,
            DomainStatus::Stopped
        );
        assert_eq!(
            stop_cause(&state, name),
            Some(DomainStopCause::Licensing(reason))
        );
    }
    assert_eq!(stop_cause(&state, "idle"), None, "no stop committed for it");
    // A later stop request for a stopped domain changes nothing: it keeps the cause of the stop
    // that ended it.
    let repeated = ConsensusCommand::RequestDomainStop {
        domain_id: domain("running"),
        request: Box::new(nervix_models::StopRequest::Operator {
            reference: nervix_models::CommandExecutionReference::parse("later-stop")
                .assured("the fixture reference is a valid literal"),
            request: nervix_models::OperatorStopRequest {
                requested_at: expiry(&state),
                limit: None,
            },
        }),
        mutation: None,
    };
    let applied = apply_consensus_command(&mut state, &repeated);
    assert_eq!(applied.response, ConsensusResponse::Applied);
    assert!(!applied.domains_changed);
    assert_eq!(
        stop_cause(&state, "running"),
        Some(DomainStopCause::Licensing(reason))
    );
}

#[test]
fn only_a_new_activation_after_the_expiry_lifts_the_gate_and_starts_stay_explicit() {
    let fixture = SignedFixture::new();
    let mut state = fixture.state();
    assert_eq!(
        activate(&fixture, &mut state, FIRST, 0),
        ConsensusResponse::Applied
    );
    assert_eq!(
        put(&mut state, "tenant", DomainStatus::Running),
        ConsensusResponse::Applied
    );
    let already = ConsensusResponse::Conflict(ConsensusConflict::LicenseDecision {
        reason: LicenseDecisionError::AlreadyActivated,
    });
    assert_eq!(
        activate(&fixture, &mut state, SECOND, 601),
        already,
        "a live activation is never superseded"
    );
    let expires_at = expiry(&state);
    advance(&mut state, expires_at);
    let reason = LicensingStopReason::EphemeralExpired {
        expired_at: expires_at,
    };
    assert_eq!(
        stop_for_licensing(&mut state, "tenant", reason),
        ConsensusResponse::Applied
    );
    assert_eq!(
        activate(&fixture, &mut state, FIRST, 601),
        already,
        "the expired activation itself cannot be committed again"
    );

    assert_eq!(
        activate(&fixture, &mut state, SECOND, 601),
        ConsensusResponse::Applied
    );
    let LicensingMode::Activated(activation) =
        &state.licensing.as_ref().verified("identity installed").mode
    else {
        panic!("the new activation is committed");
    };
    assert_eq!(
        activation.binding.activation_id,
        uuid::Uuid::from_u128(SECOND)
    );
    assert_eq!(
        state.licensing_stop(),
        None,
        "the new activation lifts the gate"
    );
    assert!(
        state
            .licensing_verification
            .as_ref()
            .verified("the new decision has its derived view")
            .is_ok()
    );
    // Lifting the gate restarts nothing: the licensing stop stays until an explicit START.
    let tenant = state
        .domains
        .get(&domain("tenant"))
        .verified("created above");
    assert_eq!(tenant.status, DomainStatus::Stopped);
    assert_eq!(
        tenant.stop_cause(),
        Some(DomainStopCause::Licensing(reason))
    );
    assert_eq!(start(&mut state, "tenant"), ConsensusResponse::Applied);
    let tenant = state
        .domains
        .get(&domain("tenant"))
        .verified("created above");
    assert_eq!(tenant.status, DomainStatus::Running);
    assert_eq!(tenant.stop_cause(), None);
}

#[test]
fn a_closed_gate_refuses_a_pause_recovery_that_would_resume_the_domain() {
    let mut state = ephemeral_state();
    assert_eq!(
        put(&mut state, "paused", DomainStatus::Paused),
        ConsensusResponse::Applied
    );
    let reference =
        nervix_models::CommandExecutionReference::parse("018bcfe5-6800-7000-8000-000000000001")
            .assured("the fixture reference is a UUIDv7");
    let mutation =
        crate::DomainMutationLease::recorded(crate::DomainMutationOwner::command(reference), 11);
    state
        .domain_mutations
        .insert(domain("paused"), mutation.clone());
    let reason = pass_expiry(&mut state);
    let recovery = ConsensusCommand::RecoverOrphanedModelPause {
        domain_id: domain("paused"),
        mutation: Box::new(mutation),
    };
    let applied = apply_consensus_command(&mut state, &recovery);
    assert_eq!(
        applied.response,
        ConsensusResponse::Conflict(ConsensusConflict::LicensingStop { reason })
    );
    assert!(!applied.domains_changed);
    // The pause stays until the leader's licensing stop ends it; nothing resumes the domain.
    let paused = state
        .domains
        .get(&domain("paused"))
        .verified("created above");
    assert_eq!(paused.status, DomainStatus::Paused);
    assert_eq!(paused.stop_cause(), None);
}

#[test]
fn a_closed_gate_refuses_a_transaction_start_step_planned_before_it_closed() {
    let mut state = ephemeral_state();
    assert_eq!(
        put(&mut state, "tenant", DomainStatus::Stopped),
        ConsensusResponse::Applied
    );
    let owner = nervix_models::UserName::parse("operator")
        .assured("the fixture owner is an identifier-shaped literal");
    let activity = crate::TransactionActivity::from_timeout(
        Timestamp::from_unix_nanos(1),
        std::time::Duration::from_secs(60),
    );
    let mut transaction = crate::ReplicatedTransaction::open(
        "tx".to_owned(),
        domain("tenant"),
        owner.clone(),
        activity,
    );
    transaction
        .queue(
            &owner,
            &domain("tenant"),
            activity,
            1,
            crate::TransactionStatement::test_admitted(crate::TransactionStatementRequest {
                request_reference: nervix_models::CommandExecutionReference::parse("start.0")
                    .assured("the fixture reference is a valid request reference"),
                expected_position: 0,
                source: "START".to_owned(),
                statement: nervix_models::Statement::StartDomain(nervix_models::StartDomain {
                    start: DomainStartPoint::Resume,
                }),
            }),
            crate::TransactionQueueLimits {
                max_statements: 1,
                max_source_bytes: 1024,
            },
        )
        .assured("an open transaction of its owner queues one statement within its limits");
    // Planning admitted the START while the gate was open; the gate closes before its step commits.
    let effect = crate::TransactionStepEffect::StartDomain {
        inputs: Box::new(state.domain_planning_inputs(&domain("tenant"))),
        start: DomainStartPoint::Resume,
        clock: None,
        authority: None,
    };
    let reason = pass_expiry(&mut state);
    let result = crate::TransactionStepResult {
        impact: crate::transaction::test_commit_plan("tx", 1).steps[0]
            .impact
            .clone(),
        result: crate::TransactionCommandResult {
            success: true,
            message: "started".to_owned(),
            diagnostics: Vec::new(),
            already_existed: false,
            admission: None,
        },
    };
    let refused = crate::validate_transaction_step_effect(
        &state,
        &transaction,
        0,
        1,
        &result,
        &effect,
        Some(&crate::TransactionOutcome::Committed),
    )
    .expect_err("a closed gate refuses the START step");
    let crate::TransactionMutationError::LicensingStop {
        id,
        reason: refused_reason,
    } = refused.current_context()
    else {
        panic!("the START step is refused by the licensing stop, not {refused:?}");
    };
    assert_eq!(id, "tx");
    assert_eq!(*refused_reason, reason);
    let tenant = state
        .domains
        .get(&domain("tenant"))
        .verified("created above");
    assert_eq!(tenant.status, DomainStatus::Stopped);
}
