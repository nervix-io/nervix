//! The licensing stop gate over committed licensing time and the ephemeral binding.
//!
//! Layer: test harness.
//! - **Owns.** Deterministic gate, refusal, stop-cause and supersession proofs through the
//!   production consensus apply path.
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
        .stop_cause
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
        let stop = ConsensusCommand::StopDomain {
            domain_id: domain(name),
            mutation: None,
            cause: DomainStopCause::Licensing(reason),
        };
        let applied = apply_consensus_command(&mut state, &stop);
        assert_eq!(applied.response, ConsensusResponse::Applied);
        assert!(applied.domains_changed);
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
    // A later stop of a stopped domain keeps the cause of the stop that ended it.
    let repeated = ConsensusCommand::StopDomain {
        domain_id: domain("running"),
        mutation: None,
        cause: DomainStopCause::Operator,
    };
    assert_eq!(
        apply_consensus_command(&mut state, &repeated).response,
        ConsensusResponse::Applied
    );
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
    let stop = ConsensusCommand::StopDomain {
        domain_id: domain("tenant"),
        mutation: None,
        cause: DomainStopCause::Licensing(reason),
    };
    assert_eq!(
        apply_consensus_command(&mut state, &stop).response,
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
    assert_eq!(tenant.stop_cause, Some(DomainStopCause::Licensing(reason)));
    assert_eq!(start(&mut state, "tenant"), ConsensusResponse::Applied);
    let tenant = state
        .domains
        .get(&domain("tenant"))
        .verified("created above");
    assert_eq!(tenant.status, DomainStatus::Running);
    assert_eq!(tenant.stop_cause, None);
}
