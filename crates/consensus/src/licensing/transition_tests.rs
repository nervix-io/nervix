//! Receipt anchoring, durable grants and signed replacements through the replicated apply owner.
//!
//! Layer: test harness.
//! - **Owns.** Deterministic transition regressions with actual signed decisions.
//! - **Depends on.** Consensus apply, production codecs and the shared signing fixture.
//! - **Must not know.** Physical clocks, networking or node policy publication.

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_licensing_protocol::{
    conditions::{CLUSTER_DOMAINS, ConditionValue, Quota},
    documents::Decision,
    test_fixture::{at, id},
};

use super::{ActivationState, LicensingMode, LicensingRecord, verification_tests::SignedFixture};
use crate::{ConsensusCommand, ConsensusResponse, StateMachineData, apply_consensus_command};

fn activated(fixture: &SignedFixture) -> StateMachineData {
    let mut state = fixture.state();
    let received = fixture.received(&fixture.decision, &[]);
    let command = ConsensusCommand::ActivateLicense {
        binding: Box::new(fixture.binding(&received)),
        decision: Box::new(received),
    };
    assert_eq!(
        apply_consensus_command(&mut state, &command).response,
        ConsensusResponse::Applied
    );
    for name in ["first", "second"] {
        let command = ConsensusCommand::PutDomain {
            domain: Box::new(crate::tests::running_domain_state(name)),
            mutation: None,
            domain_quota: Quota::Unlimited,
        };
        assert_eq!(
            apply_consensus_command(&mut state, &command).response,
            ConsensusResponse::Applied
        );
    }
    state
}

fn active(state: &StateMachineData) -> &ActivationState {
    let record = state.licensing.as_ref().verified("activation was applied");
    let LicensingMode::Activated(activation) = &record.mode else {
        panic!("test retains activation");
    };
    activation
}

fn next(decision: &mut Decision) {
    decision.decision_revision = decision
        .decision_revision
        .checked_add(1)
        .assured("bounded test revision");
    decision.decision_id = id(u128::from(decision.decision_revision));
    decision.in_reply_to = id(u128::from(decision.decision_revision)
        .checked_add(100)
        .assured("bounded request identity"));
}

fn apply_decision(
    state: &mut StateMachineData,
    fixture: &SignedFixture,
    decision: &Decision,
    receipt: i64,
) {
    let record = state.licensing.as_ref().verified("activation was applied");
    let mut received = fixture.received(decision, &record.trust);
    received.received_at =
        nervix_models::Timestamp::try_from(at(receipt)).assured("test coordinate");
    let command = ConsensusCommand::ApplyLicenseDecision {
        decision: Box::new(received),
        expected_revision: active(state).decision.revision,
    };
    assert_eq!(
        apply_consensus_command(state, &command).response,
        ConsensusResponse::Applied
    );
}

fn downgrade(decision: &mut Decision, value: u64) {
    next(decision);
    decision.conditions.conditions_revision = decision
        .conditions
        .conditions_revision
        .checked_add(1)
        .assured("bounded conditions revision");
    decision.conditions.usage.quotas.insert(
        CLUSTER_DOMAINS.to_owned(),
        ConditionValue::Known(Quota::Bounded { value }),
    );
}

#[test]
fn receipt_anchored_grants_survive_refresh_and_storage_and_signed_acknowledgement_replaces_them() {
    let fixture = SignedFixture::new();
    let mut state = activated(&fixture);
    let mut decision = fixture.decision.clone();
    downgrade(&mut decision, 1);
    decision.timing.valid_until = Some(at(500));
    apply_decision(&mut state, &fixture, &decision, 20);
    let grant = active(&state)
        .transitions
        .values()
        .next()
        .verified("reduction affects two domains")
        .clone();
    assert_eq!(grant.transition.started_at, at(20));
    assert_eq!(grant.transition.deadline, at(920));
    assert_eq!(grant.until().into_datetime(), at(500));
    assert!(!grant.acknowledged());

    next(&mut decision);
    decision.timing.valid_until = Some(at(2_000));
    apply_decision(&mut state, &fixture, &decision, 40);
    assert_eq!(active(&state).transitions.values().next(), Some(&grant));
    let record = state
        .licensing
        .as_ref()
        .verified("committed licensing record");
    let bytes = crate::durable_batch::DurableBatch::encode(record.as_ref(), 64 * 1024)
        .assured("bounded current record");
    let restored: LicensingRecord =
        crate::storage_decode(&bytes).assured("current record restores");
    assert_eq!(&restored, record.as_ref());
    restored
        .validate(&fixture.identity.public())
        .assured("restored grant structure validates");
    assert!(
        restored
            .verify(&fixture.identity.public(), &fixture.environment)
            .verified("activated record verifies")
            .is_ok()
    );

    next(&mut decision);
    let mut acknowledged = grant.transition.clone();
    acknowledged.started_at = at(25);
    acknowledged.deadline = at(1_000);
    decision.transitions = vec![acknowledged.clone()];
    apply_decision(&mut state, &fixture, &decision, 50);
    let accepted = active(&state)
        .transitions
        .values()
        .next()
        .verified("signed acknowledgement retained");
    assert_eq!(accepted.transition, acknowledged);
    assert_eq!(accepted.until().into_datetime(), at(1_000));
    assert!(accepted.acknowledged());
    assert_eq!(
        accepted.authorization,
        super::transitions::TransitionAuthorization::Acknowledged
    );
}

#[test]
fn further_reduction_preserves_the_earlier_deadline_and_term_tick_applies_actual_free_policy() {
    let fixture = SignedFixture::new();
    let mut state = activated(&fixture);
    let mut decision = fixture.decision.clone();
    downgrade(&mut decision, 1);
    apply_decision(&mut state, &fixture, &decision, 20);
    let first = active(&state).transitions.reported();
    downgrade(&mut decision, 0);
    apply_decision(&mut state, &fixture, &decision, 120);
    let transitions = active(&state).transitions.reported();
    assert_eq!(transitions.len(), 2);
    assert!(transitions.contains(&first[0]));
    let effective = nervix_licensing_protocol::impact::EffectiveUsage {
        conditions: &decision.conditions.usage,
        transitions: &transitions,
    };
    assert_eq!(
        effective
            .quota(CLUSTER_DOMAINS, at(919))
            .assured("checked allowances"),
        Quota::Bounded { value: 2 }
    );
    assert_eq!(
        effective
            .quota(CLUSTER_DOMAINS, at(920))
            .assured("first deadline"),
        Quota::Bounded { value: 1 }
    );
    assert_eq!(
        effective
            .quota(CLUSTER_DOMAINS, at(1_020))
            .assured("last deadline"),
        Quota::Bounded { value: 0 }
    );
    let tick = ConsensusCommand::AdvanceLicensingTime {
        observed_at: nervix_models::Timestamp::try_from(decision.timing.term_ends_at)
            .assured("signed term fits"),
    };
    assert_eq!(
        apply_consensus_command(&mut state, &tick).response,
        ConsensusResponse::Applied
    );
    assert!(
        active(&state).transitions.reported().is_empty(),
        "the actual unlimited free policy needs no grant"
    );
    assert_eq!(
        state.domains.len(),
        2,
        "licensing never tears down existing domains"
    );
    assert!(
        super::admit_domain(&state, Quota::Unlimited, false).is_ok(),
        "the term-end free policy also governs committed growth"
    );
}

#[test]
fn a_captured_restore_allowance_expires_even_while_the_decision_is_fresh() {
    let fixture = SignedFixture::new();
    let mut state = activated(&fixture);
    let mut decision = fixture.decision.clone();
    downgrade(&mut decision, 1);
    decision.timing.valid_until = Some(at(2_000));
    apply_decision(&mut state, &fixture, &decision, 20);
    let name = "second"
        .parse::<nervix_models::DomainName>()
        .assured("fixed domain name");
    state.domains.remove(&name);
    let captured = Quota::Bounded { value: 2 };
    assert!(super::admit_domain(&state, captured, true).is_ok());
    let tick = ConsensusCommand::AdvanceLicensingTime {
        observed_at: nervix_models::Timestamp::try_from(at(920)).assured("test coordinate fits"),
    };
    assert_eq!(
        apply_consensus_command(&mut state, &tick).response,
        ConsensusResponse::Applied
    );
    assert!(
        super::admit_domain(&state, captured, true).is_err(),
        "a fresh decision cannot keep a captured grant alive past its deadline"
    );
}

#[test]
fn acknowledged_recovery_remains_capped_at_the_durable_boundary_after_lapse() {
    let fixture = SignedFixture::new();
    let mut state = activated(&fixture);
    let mut decision = fixture.decision.clone();
    downgrade(&mut decision, 1);
    decision.timing.valid_until = Some(at(500));
    apply_decision(&mut state, &fixture, &decision, 20);
    decision.transitions = active(&state).transitions.reported();
    next(&mut decision);
    apply_decision(&mut state, &fixture, &decision, 40);
    let tick = |seconds| ConsensusCommand::AdvanceLicensingTime {
        observed_at: nervix_models::Timestamp::try_from(at(seconds))
            .assured("test coordinate fits"),
    };
    assert_eq!(
        apply_consensus_command(&mut state, &tick(600)).response,
        ConsensusResponse::Applied
    );
    // The owner sees a missing archived domain during recovery; no domain deletion syntax exists.
    let name = "second"
        .parse::<nervix_models::DomainName>()
        .assured("fixed domain name");
    let removed = state
        .domains
        .get(&name)
        .cloned()
        .verified("fixture has two domains");
    state.domains.remove(&name);
    assert!(super::admit_domain(&state, Quota::Bounded { value: 2 }, true).is_ok());
    assert!(super::admit_domain(&state, Quota::Unlimited, true).is_ok());
    let growth = super::admit_domain(&state, Quota::Unlimited, false)
        .err()
        .verified("lapse refuses ordinary growth");
    assert_eq!(
        growth.current_context().state,
        nervix_licensing_vocabulary::AdmissionState::Lapsed
    );
    let too_small = super::admit_domain(&state, Quota::Bounded { value: 1 }, true)
        .err()
        .verified("the captured quota still limits recovery");
    assert_eq!(
        too_small.current_context().state,
        nervix_licensing_vocabulary::AdmissionState::OverLimit
    );
    state.domains.insert(name, removed);
    assert!(
        super::admit_domain(&state, Quota::Unlimited, true).is_err(),
        "recovery cannot expand the live grant"
    );
    assert_eq!(
        apply_consensus_command(&mut state, &tick(920)).response,
        ConsensusResponse::Applied
    );
    assert!(
        super::admit_domain(&state, Quota::Unlimited, true).is_err(),
        "the deadline ends the recovery permission"
    );
}
