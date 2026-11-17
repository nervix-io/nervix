//! Durable licensing operations through the production state machine.
//!
//! Layer: test harness.
//! - **Owns.** Atomic key/decision installation, stale reply fences and release grant lifetimes.
//! - **Depends on.** Real protocol signatures, current records and production archive codecs.
//! - **Must not know.** Network scheduling, application policy or physical clocks.

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_licensing_protocol::{
    documents::{AffectedQuota, Transition, TransitionReason, TransitionTrigger},
    test_fixture,
};
use uuid::Uuid;

use super::{verification_tests::SignedFixture, *};
use crate::{
    ConsensusCommand, ConsensusResponse, StateMachineData, apply_consensus_command,
    cluster_identity::tests::{rotation_candidate, rotation_request},
};

fn applied(state: &mut StateMachineData, command: ConsensusCommand) {
    assert_eq!(
        apply_consensus_command(state, &command).response,
        ConsensusResponse::Applied,
        "{command}"
    );
}

fn activated(fixture: &SignedFixture) -> StateMachineData {
    let mut state = fixture.state();
    let decision = fixture.received(&fixture.decision, &[]);
    let binding = ActivationBinding::from_verified(&fixture.decision, "TESTABCD".to_owned())
        .assured("fixture binding");
    applied(
        &mut state,
        ConsensusCommand::ActivateLicense {
            binding: Box::new(binding),
            decision: Box::new(decision),
        },
    );
    state
}

fn record(state: &StateMachineData) -> &LicensingRecord {
    state.licensing.as_ref().verified("licensing installed")
}

fn binding(state: &StateMachineData) -> ActivationBinding {
    let LicensingMode::Activated(activation) = &record(state).mode else {
        panic!("fixture activated");
    };
    activation.binding.clone()
}

fn admit_licensing_effect(
    state: &mut StateMachineData,
    reference: nervix_models::CommandExecutionReference,
    effect: crate::CommandExecutionEffect,
) {
    let at = nervix_models::Timestamp::from_unix_nanos(1);
    let execution = crate::CommandExecution::applying(
        reference,
        nervix_models::UserName::parse("operator").assured("fixture user"),
        None,
        [3; 32],
        at,
        effect,
    );
    state.command_executions.insert_admitted(
        execution,
        &crate::CommandExecutionAdmissionPolicy::at(at, std::time::Duration::from_secs(60), 32),
    );
}

fn finish_licensing_command(
    state: &mut StateMachineData,
    reference: &nervix_models::CommandExecutionReference,
    disposition: crate::CommandExecutionDisposition,
) {
    applied(
        state,
        ConsensusCommand::FinishCommandExecution {
            reference: reference.clone(),
            owner: nervix_models::UserName::parse("operator").assured("fixture user"),
            request_digest: [3; 32],
            at: nervix_models::Timestamp::from_unix_nanos(2),
            result: Box::new(crate::CommandExecutionResult {
                disposition,
                message: "fixture terminal result".into(),
                diagnostics: Vec::new(),
                statements: Vec::new(),
                transaction: None,
                transaction_admission: None,
                backup: None,
                restore: None,
            }),
        },
    );
}

#[test]
fn pending_rotation_keeps_its_execution_applying_until_a_terminal_operation_outcome() {
    let fixture = SignedFixture::new();
    let mut state = activated(&fixture);
    let identity = state
        .cluster_identity
        .as_ref()
        .verified("identity")
        .as_ref()
        .clone();
    let request_id = test_fixture::id(721);
    let reference = crate::cluster_identity::tests::reference(request_id);
    admit_licensing_effect(
        &mut state,
        reference.clone(),
        crate::CommandExecutionEffect::RotateLicenseClusterKey {
            identity: Box::new(identity.public()),
            outcome: None,
        },
    );
    applied(
        &mut state,
        ConsensusCommand::BeginClusterKeyRotation {
            identity: Box::new(identity.public()),
            pending_key: Box::new(rotation_candidate(&identity, request_id)),
        },
    );
    for disposition in [
        crate::CommandExecutionDisposition::Failed,
        crate::CommandExecutionDisposition::Completed {
            already_existed: false,
        },
    ] {
        finish_licensing_command(&mut state, &reference, disposition);
        assert!(
            state
                .command_executions
                .get(&reference)
                .verified("admitted execution")
                .is_applying(),
            "a pending rotation has no definitive command outcome"
        );
    }
    applied(
        &mut state,
        ConsensusCommand::AbandonClusterKeyRotation {
            reference: reference.clone(),
            request_id,
            code: nervix_licensing_vocabulary::ProblemCode::ProofInvalid,
        },
    );
    finish_licensing_command(
        &mut state,
        &reference,
        crate::CommandExecutionDisposition::Failed,
    );
    assert!(matches!(
        &state.command_executions.get(&reference).verified("retained outcome").state,
        crate::CommandExecutionState::Finished { result, .. }
            if result.disposition == crate::CommandExecutionDisposition::Failed
    ));
}

#[test]
fn committed_rotation_outcomes_survive_later_keys_before_response_storage() {
    let fixture = SignedFixture::new();
    let mut state = fixture.state();
    let mut first = None;
    for id in [701, 702, 703] {
        let identity = state
            .cluster_identity
            .as_ref()
            .verified("identity installed")
            .as_ref()
            .clone();
        let pending = rotation_candidate(&identity, test_fixture::id(id));
        let reference = crate::cluster_identity::tests::reference(test_fixture::id(id));
        admit_licensing_effect(
            &mut state,
            reference.clone(),
            crate::CommandExecutionEffect::RotateLicenseClusterKey {
                identity: Box::new(identity.public()),
                outcome: None,
            },
        );
        let command = ConsensusCommand::BeginClusterKeyRotation {
            identity: Box::new(identity.public()),
            pending_key: Box::new(pending),
        };
        applied(&mut state, command.clone());
        assert_eq!(
            state.licensing_command_outcome(&reference),
            Some(crate::LicensingCommandOutcome::Applied)
        );
        if first.is_none() {
            first = Some((reference, command));
        }
    }
    let (reference, command) = first.verified("three commands ran");
    let saved = state.cluster_identity.clone();
    applied(&mut state, command);
    assert_eq!(state.cluster_identity, saved);
    assert_eq!(
        saved
            .as_ref()
            .verified("identity")
            .public()
            .key_generation
            .get(),
        4
    );
    let execution = state
        .command_executions
        .get(&reference)
        .verified("original applying execution retained");
    let bytes = crate::durable_batch::DurableBatch::encode(execution, 65536)
        .assured("bounded command fits");
    let restored: crate::CommandExecution =
        crate::storage_decode(&bytes).assured("complete command progress decodes");
    assert_eq!(&restored, execution);
}

#[test]
fn committed_rotation_defers_a_stale_failure_until_its_success_is_recovered() {
    let fixture = SignedFixture::new();
    let mut state = fixture.state();
    let identity = state
        .cluster_identity
        .as_ref()
        .verified("identity")
        .as_ref()
        .clone();
    let request_id = test_fixture::id(722);
    let reference = crate::cluster_identity::tests::reference(request_id);
    admit_licensing_effect(
        &mut state,
        reference.clone(),
        crate::CommandExecutionEffect::RotateLicenseClusterKey {
            identity: Box::new(identity.public()),
            outcome: None,
        },
    );
    applied(
        &mut state,
        ConsensusCommand::BeginClusterKeyRotation {
            identity: Box::new(identity.public()),
            pending_key: Box::new(rotation_candidate(&identity, request_id)),
        },
    );
    let before = state.command_executions.get(&reference).cloned();
    finish_licensing_command(
        &mut state,
        &reference,
        crate::CommandExecutionDisposition::Failed,
    );
    assert_eq!(
        state.command_executions.get(&reference),
        before.as_ref(),
        "a stale failure cannot overwrite the committed rotation"
    );
    finish_licensing_command(
        &mut state,
        &reference,
        crate::CommandExecutionDisposition::Completed {
            already_existed: false,
        },
    );
    assert!(matches!(
        &state.command_executions.get(&reference).verified("retained outcome").state,
        crate::CommandExecutionState::Finished { result, .. }
            if result.disposition == crate::CommandExecutionDisposition::Completed { already_existed: false }
    ));
    assert_eq!(
        state
            .cluster_identity
            .as_ref()
            .verified("identity")
            .public()
            .key_generation
            .get(),
        2
    );
}

#[test]
fn abandoned_rotation_defers_a_stale_success_until_its_refusal_is_recovered() {
    let fixture = SignedFixture::new();
    let mut state = activated(&fixture);
    let identity = state
        .cluster_identity
        .as_ref()
        .verified("identity")
        .as_ref()
        .clone();
    let request_id = test_fixture::id(723);
    let reference = crate::cluster_identity::tests::reference(request_id);
    admit_licensing_effect(
        &mut state,
        reference.clone(),
        crate::CommandExecutionEffect::RotateLicenseClusterKey {
            identity: Box::new(identity.public()),
            outcome: None,
        },
    );
    applied(
        &mut state,
        ConsensusCommand::BeginClusterKeyRotation {
            identity: Box::new(identity.public()),
            pending_key: Box::new(rotation_candidate(&identity, request_id)),
        },
    );
    applied(
        &mut state,
        ConsensusCommand::AbandonClusterKeyRotation {
            reference: reference.clone(),
            request_id,
            code: nervix_licensing_vocabulary::ProblemCode::ProofInvalid,
        },
    );
    let before = state.command_executions.get(&reference).cloned();
    finish_licensing_command(
        &mut state,
        &reference,
        crate::CommandExecutionDisposition::Completed {
            already_existed: false,
        },
    );
    assert_eq!(
        state.command_executions.get(&reference),
        before.as_ref(),
        "a stale success cannot overwrite the committed refusal"
    );
    finish_licensing_command(
        &mut state,
        &reference,
        crate::CommandExecutionDisposition::Failed,
    );
    assert!(
        !state
            .command_executions
            .get(&reference)
            .verified("retained outcome")
            .is_applying()
    );
}

#[test]
fn committed_release_defers_a_stale_failure_until_its_success_is_recovered() {
    let fixture = SignedFixture::new();
    let mut state = activated(&fixture);
    let binding = binding(&state);
    let request_id = test_fixture::id(724);
    let reference = crate::cluster_identity::tests::reference(request_id);
    admit_licensing_effect(
        &mut state,
        reference.clone(),
        crate::CommandExecutionEffect::ReleaseLicense {
            binding: Box::new(binding.clone()),
            outcome: None,
        },
    );
    let released_at = record(&state).licensing_time;
    applied(
        &mut state,
        ConsensusCommand::ReleaseLicense {
            reference: reference.clone(),
            binding: Box::new(binding),
            request_id,
            released_at,
        },
    );
    let before = state.command_executions.get(&reference).cloned();
    finish_licensing_command(
        &mut state,
        &reference,
        crate::CommandExecutionDisposition::Failed,
    );
    assert_eq!(
        state.command_executions.get(&reference),
        before.as_ref(),
        "a stale failure cannot overwrite the committed release"
    );
    finish_licensing_command(
        &mut state,
        &reference,
        crate::CommandExecutionDisposition::Completed {
            already_existed: false,
        },
    );
    assert!(
        !state
            .command_executions
            .get(&reference)
            .verified("retained outcome")
            .is_applying()
    );
    let LicensingMode::Released(release) = &record(&state).mode else {
        panic!("release remains committed")
    };
    assert!(
        release.awaiting(),
        "command completion does not await remote capacity confirmation"
    );
}

#[test]
fn abandoned_rotation_retains_its_refusal_without_touching_the_next_pending_key() {
    let fixture = SignedFixture::new();
    let mut state = activated(&fixture);
    let original = state
        .cluster_identity
        .as_ref()
        .verified("identity")
        .as_ref()
        .clone();
    let request_id = test_fixture::id(711);
    let pending = rotation_candidate(&original, request_id);
    let reference = crate::cluster_identity::tests::reference(request_id);
    admit_licensing_effect(
        &mut state,
        reference.clone(),
        crate::CommandExecutionEffect::RotateLicenseClusterKey {
            identity: Box::new(original.public()),
            outcome: None,
        },
    );
    let command = ConsensusCommand::BeginClusterKeyRotation {
        identity: Box::new(original.public()),
        pending_key: Box::new(pending),
    };
    applied(&mut state, command.clone());
    applied(
        &mut state,
        ConsensusCommand::AbandonClusterKeyRotation {
            reference: reference.clone(),
            request_id,
            code: nervix_licensing_vocabulary::ProblemCode::ProofInvalid,
        },
    );
    let later = rotation_candidate(&original, test_fixture::id(712));
    applied(
        &mut state,
        ConsensusCommand::BeginClusterKeyRotation {
            identity: Box::new(original.public()),
            pending_key: Box::new(later),
        },
    );
    let saved = state.cluster_identity.clone();
    applied(&mut state, command);
    assert_eq!(state.cluster_identity, saved);
    assert_eq!(
        state.licensing_command_outcome(&reference),
        Some(crate::LicensingCommandOutcome::Refused {
            code: nervix_licensing_vocabulary::ProblemCode::ProofInvalid
        })
    );
}

fn start_rotation(
    fixture: &SignedFixture,
    state: &mut StateMachineData,
    request_id: Uuid,
) -> crate::ClusterKeyRotation {
    let identity = state
        .cluster_identity
        .as_ref()
        .verified("identity installed")
        .as_ref()
        .clone();
    let pending = rotation_candidate(&identity, request_id);
    applied(
        state,
        ConsensusCommand::BeginClusterKeyRotation {
            identity: Box::new(identity.public()),
            pending_key: Box::new(pending.clone()),
        },
    );
    let prepared = rotation_request(
        &identity,
        &pending,
        fixture.decision.activation.activation_id,
        fixture.decision.decision_revision,
        fixture.decision.conditions.conditions_revision,
        record(state).licensing_time,
        fixture.decision.service.clone(),
    );
    applied(
        state,
        ConsensusCommand::PrepareClusterKeyRotationRequest {
            request_id,
            request: Box::new(prepared),
        },
    );
    state
        .cluster_identity
        .as_ref()
        .verified("identity installed")
        .pending_rotation()
        .verified("pending rotation retained")
}

#[test]
fn rotated_key_and_decision_commit_atomically_without_moving_any_deadline() {
    let mut fixture = SignedFixture::new();
    fixture.decision.activation.kind = nervix_models::ActivationKind::Ephemeral;
    fixture.decision.activation.ephemeral_expires_at = Some(test_fixture::at(3600));
    fixture.decision.timing.ephemeral_expires_at = Some(test_fixture::at(3600));
    let mut state = activated(&fixture);
    let request_id = test_fixture::id(101);
    let rotation = start_rotation(&fixture, &mut state, request_id);
    assert_eq!(
        state
            .cluster_identity
            .as_ref()
            .verified("identity")
            .public()
            .key_generation
            .get(),
        1
    );
    let mut decision = fixture.decision.clone();
    decision.decision_revision += 1;
    decision.decision_id = test_fixture::id(102);
    decision.issued_at = nervix_models::Timestamp::try_from(fixture.decision.timing.refresh_after)
        .assured("fixture time")
        .checked_add(std::time::Duration::from_secs(1))
        .assured("bounded receipt")
        .into_datetime();
    decision.in_reply_to = request_id;
    decision.activation.generation = 2;
    decision.activation.cluster_key_kid = rotation.next_identity.kid;
    let received = fixture.received(&decision, &record(&state).trust);
    let before = record(&state).clone();
    let wrong = ConsensusCommand::ApplyLicenseDecision {
        decision: Box::new(received.clone()),
        expected_revision: fixture.decision.decision_revision,
    };
    assert!(matches!(
        apply_consensus_command(&mut state, &wrong).response,
        ConsensusResponse::Conflict(_)
    ));
    assert_eq!(record(&state), &before);
    let mut shifted = decision.clone();
    shifted.timing.refresh_after = nervix_models::Timestamp::try_from(shifted.timing.refresh_after)
        .assured("fixture time")
        .checked_add(std::time::Duration::from_secs(1))
        .assured("bounded deadline")
        .into_datetime();
    let bad = fixture.received(&shifted, &record(&state).trust);
    assert!(matches!(
        apply_consensus_command(
            &mut state,
            &ConsensusCommand::CompleteClusterKeyRotation {
                request_id,
                decision: Box::new(bad)
            }
        )
        .response,
        ConsensusResponse::Conflict(_)
    ));
    assert_eq!(record(&state), &before);
    applied(
        &mut state,
        ConsensusCommand::CompleteClusterKeyRotation {
            request_id,
            decision: Box::new(received),
        },
    );
    let identity = state
        .cluster_identity
        .as_ref()
        .verified("identity retained")
        .public();
    assert_eq!(identity.key_generation.get(), 2);
    assert_eq!(identity.cluster_uid, fixture.identity.public().cluster_uid);
    assert!(
        state
            .cluster_identity
            .as_ref()
            .verified("identity")
            .pending_rotation()
            .is_none()
    );
    let verified = state
        .licensing_verification
        .as_ref()
        .verified("view")
        .as_ref()
        .assured("rotated decision verifies");
    assert_eq!(verified.decision.timing, fixture.decision.timing);
    assert_eq!(
        verified.decision.activation.ephemeral_expires_at,
        fixture.decision.activation.ephemeral_expires_at
    );
    let encoded =
        crate::durable_batch::DurableBatch::encode(record(&state), 65536).assured("record fits");
    let restored: LicensingRecord = crate::storage_decode(&encoded).assured("record decodes");
    assert_eq!(
        restored.verify(&identity, &fixture.environment),
        state.licensing_verification
    );
}

#[test]
fn release_commits_before_confirmation_and_keeps_its_signer_across_local_rotations() {
    let fixture = SignedFixture::new();
    let mut state = activated(&fixture);
    let original = state
        .cluster_identity
        .as_ref()
        .verified("identity")
        .public();
    let binding = binding(&state);
    let request_id = test_fixture::id(201);
    let released_at = record(&state).licensing_time;
    applied(
        &mut state,
        ConsensusCommand::ReleaseLicense {
            reference: crate::cluster_identity::tests::reference(test_fixture::id(1)),
            binding: Box::new(binding.clone()),
            request_id,
            released_at,
        },
    );
    let LicensingMode::Released(release) = &record(&state).mode else {
        panic!("release committed");
    };
    assert!(release.awaiting());
    assert_eq!(release.grants_until, None);
    let mut common = fixture.signing.common();
    common.cluster_uid = release.identity.cluster_uid;
    common.key_generation = release.identity.key_generation.get();
    common.request_id = release.request_id;
    common.sent_at = release.released_at.into_datetime();
    common.aud = fixture.decision.service.clone();
    let request = nervix_licensing_protocol::requests::Request::Release {
        common,
        activation_id: release.activation.binding.activation_id,
        state: nervix_licensing_protocol::requests::CommittedState {
            applied_decision_revision: release.activation.decision.revision,
            applied_conditions_revision: release.activation.decision.conditions_revision,
            licensing_time: release.released_at.into_datetime(),
        },
        grants_until: release.released_at.into_datetime(),
        reason: "operator_release".to_owned(),
    };
    let signer = nervix_licensing_protocol::document::DocumentSigner::from_pkcs8(
        &fixture.identity.retain_release_key().private_pkcs8.0,
    )
    .assured("release fixture signer");
    let prepared = PreparedLicensingRequest::new(
        signer
            .sign(&request, test_fixture::limits())
            .assured("bounded release request signs"),
        released_at
            .checked_add(std::time::Duration::from_secs(1))
            .assured("bounded challenge"),
        test_fixture::limits(),
    )
    .assured("bounded request retained");
    applied(
        &mut state,
        ConsensusCommand::PrepareLicenseReleaseRequest {
            request_id,
            request: Box::new(prepared),
        },
    );
    for id in [202, 203] {
        let identity = state
            .cluster_identity
            .as_ref()
            .verified("identity")
            .as_ref()
            .clone();
        let pending = rotation_candidate(&identity, test_fixture::id(id));
        applied(
            &mut state,
            ConsensusCommand::BeginClusterKeyRotation {
                identity: Box::new(identity.public()),
                pending_key: Box::new(pending),
            },
        );
    }
    let LicensingMode::Released(release) = &record(&state).mode else {
        panic!("confirmation still pending");
    };
    assert_eq!(release.identity, original);
    assert!(release.signing_key().is_some());
    record(&state)
        .validate(
            &state
                .cluster_identity
                .as_ref()
                .verified("identity")
                .public(),
        )
        .assured("retained release validates after rotations");
    applied(
        &mut state,
        ConsensusCommand::ConfirmRelease {
            request_id,
            decision: None,
        },
    );
    assert!(
        matches!(record(&state).mode, LicensingMode::Free { last_release: Some(id) } if id == request_id)
    );
    applied(
        &mut state,
        ConsensusCommand::ConfirmRelease {
            request_id,
            decision: None,
        },
    );
    applied(
        &mut state,
        ConsensusCommand::ReleaseLicense {
            reference: crate::cluster_identity::tests::reference(test_fixture::id(1)),
            binding: Box::new(binding),
            request_id,
            released_at,
        },
    );
}

#[test]
fn release_does_not_keep_the_activation_offline_admission_gate() {
    let mut fixture = SignedFixture::new();
    fixture.decision.timing.valid_until = Some(test_fixture::at(600));
    let mut state = activated(&fixture);
    let binding = binding(&state);
    let released_at = record(&state).licensing_time;
    applied(
        &mut state,
        ConsensusCommand::ReleaseLicense {
            reference: crate::cluster_identity::tests::reference(test_fixture::id(801)),
            binding: Box::new(binding),
            request_id: test_fixture::id(802),
            released_at,
        },
    );
    let until = fixture
        .decision
        .timing
        .valid_until
        .verified("bounded fixture allowance");
    applied(
        &mut state,
        ConsensusCommand::AdvanceLicensingTime {
            observed_at: nervix_models::Timestamp::try_from(until).assured("fixture deadline fits"),
        },
    );
    assert!(matches!(&record(&state).mode, LicensingMode::Released(release) if release.awaiting()));
    super::admit_domain(&state, nervix_licensing_vocabulary::Quota::Unlimited, false)
        .assured("the committed release admits new free-policy usage past the former deadline");
}

#[test]
fn expired_release_is_a_typed_conflict_and_leaves_every_record_intact() {
    let mut fixture = SignedFixture::new();
    fixture.decision.activation.kind = nervix_models::ActivationKind::Ephemeral;
    fixture.decision.activation.ephemeral_expires_at = Some(test_fixture::at(3600));
    fixture.decision.timing.ephemeral_expires_at = Some(test_fixture::at(3600));
    let mut state = activated(&fixture);
    let binding = binding(&state);
    let before = state.licensing.clone();
    let result = apply_consensus_command(
        &mut state,
        &ConsensusCommand::ReleaseLicense {
            reference: crate::cluster_identity::tests::reference(test_fixture::id(1)),
            binding: Box::new(binding),
            request_id: test_fixture::id(301),
            released_at: nervix_models::Timestamp::try_from(test_fixture::at(3600))
                .assured("fixture time"),
        },
    );
    assert_eq!(
        result.response,
        ConsensusResponse::Conflict(crate::ConsensusConflict::LicenseDecision {
            reason: LicenseDecisionError::Expired
        })
    );
    assert_eq!(state.licensing, before);
    let report = crate::Proposer::applied_response(result.response)
        .expect_err("expiry remains a typed refusal at the proposer");
    assert_eq!(
        report.downcast_ref::<LicenseDecisionError>(),
        Some(&LicenseDecisionError::Expired)
    );
    assert!(
        crate::ConsensusError::report_message(&report)
            .contains("expired ephemeral activation cannot be released")
    );
}

#[test]
fn admission_bindings_fence_late_release_and_rotation_commands() {
    let fixture = SignedFixture::new();
    let mut state = activated(&fixture);
    let original = state
        .cluster_identity
        .as_ref()
        .verified("identity")
        .as_ref()
        .clone();
    let mut unrelated = binding(&state);
    unrelated.activation_id = test_fixture::id(401);
    let before = state.licensing.clone();
    let released_at = record(&state).licensing_time;
    let result = apply_consensus_command(
        &mut state,
        &ConsensusCommand::ReleaseLicense {
            reference: crate::cluster_identity::tests::reference(test_fixture::id(1)),
            binding: Box::new(unrelated),
            request_id: test_fixture::id(402),
            released_at,
        },
    );
    assert_eq!(
        result.response,
        ConsensusResponse::Conflict(crate::ConsensusConflict::LicenseDecision {
            reason: LicenseDecisionError::Binding
        })
    );
    assert_eq!(state.licensing, before);
    let mut wrong = original.public();
    wrong.key_generation = std::num::NonZeroU64::new(2).verified("nonzero fixture");
    let result = apply_consensus_command(
        &mut state,
        &ConsensusCommand::BeginClusterKeyRotation {
            identity: Box::new(wrong),
            pending_key: Box::new(rotation_candidate(&original, test_fixture::id(403))),
        },
    );
    assert!(matches!(
        result.response,
        ConsensusResponse::Conflict(crate::ConsensusConflict::ClusterKeyRotation {
            reason: crate::ClusterIdentityError::StaleRotation
        })
    ));
    assert_eq!(state.cluster_identity.as_deref(), Some(&original));
}

#[test]
fn release_confirmation_keeps_existing_grants_until_their_original_deadline() {
    use nervix_licensing_protocol::conditions::{CLUSTER_DOMAINS, ConditionValue, Quota};
    let fixture = SignedFixture::new();
    let mut state = activated(&fixture);
    let mut decision = fixture.decision.clone();
    decision.decision_revision += 1;
    decision.conditions.conditions_revision += 1;
    decision.conditions.usage.quotas.insert(
        CLUSTER_DOMAINS.to_owned(),
        ConditionValue::Known(Quota::Bounded { value: 1 }),
    );
    let deadline = nervix_models::Timestamp::try_from(decision.issued_at)
        .assured("fixture time")
        .checked_add(std::time::Duration::from_secs(300))
        .assured("bounded grace");
    decision.transitions.push(Transition {
        transition_id: test_fixture::id(501),
        from_conditions_revision: fixture.decision.conditions.conditions_revision,
        to_conditions_revision: decision.conditions.conditions_revision,
        started_at: decision.issued_at,
        deadline: deadline.into_datetime(),
        trigger: TransitionTrigger::Decision {
            decision_id: decision.decision_id,
        },
        reason: TransitionReason::PlanChanged,
        affected: vec![AffectedQuota {
            condition: CLUSTER_DOMAINS.to_owned(),
            previous: Quota::Unlimited,
            target: Quota::Bounded { value: 1 },
            usage: 4,
        }],
    });
    let received = fixture.received(&decision, &record(&state).trust);
    applied(
        &mut state,
        ConsensusCommand::ApplyLicenseDecision {
            decision: Box::new(received),
            expected_revision: fixture.decision.decision_revision,
        },
    );
    let binding = binding(&state);
    let released_at = record(&state).licensing_time;
    let request_id = test_fixture::id(502);
    applied(
        &mut state,
        ConsensusCommand::ReleaseLicense {
            reference: crate::cluster_identity::tests::reference(test_fixture::id(1)),
            binding: Box::new(binding),
            request_id,
            released_at,
        },
    );
    let mut confirmation = decision.clone();
    confirmation.decision_id = test_fixture::id(503);
    confirmation.decision_revision += 1;
    confirmation.in_reply_to = request_id;
    confirmation.status = nervix_licensing_protocol::documents::DecisionStatus::Released;
    confirmation.conditions.conditions_revision += 1;
    confirmation.conditions.usage = nervix_licensing_protocol::conditions::FREE_POLICY.usage();
    let before_confirmation = state.licensing.clone();
    let mut extended = confirmation.clone();
    extended.transitions[0].deadline = deadline
        .checked_add(std::time::Duration::from_secs(1))
        .assured("fixture extension fits")
        .into_datetime();
    let received = fixture.received(&extended, &record(&state).trust);
    let rejected = apply_consensus_command(
        &mut state,
        &ConsensusCommand::ConfirmRelease {
            request_id,
            decision: Some(Box::new(received)),
        },
    );
    assert_eq!(
        rejected.response,
        ConsensusResponse::Conflict(crate::ConsensusConflict::LicenseDecision {
            reason: LicenseDecisionError::Binding,
        })
    );
    assert_eq!(state.licensing, before_confirmation);
    let received = fixture.received(&confirmation, &record(&state).trust);
    applied(
        &mut state,
        ConsensusCommand::ConfirmRelease {
            request_id,
            decision: Some(Box::new(received)),
        },
    );
    let LicensingMode::Released(release) = &record(&state).mode else {
        panic!("a grant outlives confirmation");
    };
    assert!(!release.awaiting());
    assert_eq!(release.grants_until, Some(deadline));
    assert_eq!(release.transitions.values(), decision.transitions);
    let encoded = crate::durable_batch::DurableBatch::encode(record(&state), 65536)
        .assured("bounded released record fits");
    let restored: LicensingRecord =
        crate::storage_decode(&encoded).assured("released record decodes");
    assert_eq!(&restored, record(&state));
    assert!(
        restored
            .verify(&fixture.identity.public(), &fixture.environment)
            .verified("decision retained")
            .is_ok()
    );
    applied(
        &mut state,
        ConsensusCommand::AdvanceLicensingTime {
            observed_at: deadline,
        },
    );
    assert!(matches!(record(&state).mode, LicensingMode::Free { .. }));
}

#[test]
fn new_activation_supersedes_release_and_late_confirmation_cannot_replace_it() {
    let fixture = SignedFixture::new();
    let mut state = activated(&fixture);
    let binding = binding(&state);
    let released_at = record(&state).licensing_time;
    let request_id = test_fixture::id(601);
    let reference = crate::cluster_identity::tests::reference(test_fixture::id(1));
    admit_licensing_effect(
        &mut state,
        reference.clone(),
        crate::CommandExecutionEffect::ReleaseLicense {
            binding: Box::new(binding.clone()),
            outcome: None,
        },
    );
    let release = ConsensusCommand::ReleaseLicense {
        reference,
        binding: Box::new(binding),
        request_id,
        released_at,
    };
    applied(&mut state, release.clone());
    let mut decision = fixture.decision.clone();
    decision.activation.activation_id = test_fixture::id(602);
    decision.decision_revision = 1;
    decision.decision_id = test_fixture::id(603);
    decision.in_reply_to = test_fixture::id(604);
    let received = fixture.received(&decision, &record(&state).trust);
    let binding = ActivationBinding::from_verified(&decision, "TESTABCD".to_owned())
        .assured("new independent binding");
    applied(
        &mut state,
        ConsensusCommand::ActivateLicense {
            binding: Box::new(binding),
            decision: Box::new(received),
        },
    );
    let current = state.licensing.clone();
    applied(&mut state, release);
    assert_eq!(state.licensing, current);
    assert!(matches!(
        apply_consensus_command(
            &mut state,
            &ConsensusCommand::ConfirmRelease {
                request_id,
                decision: None
            }
        )
        .response,
        ConsensusResponse::Conflict(_)
    ));
    assert_eq!(state.licensing, current);
}
