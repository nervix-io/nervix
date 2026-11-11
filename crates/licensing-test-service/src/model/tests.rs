//! The fixture must exercise real trust, binding and retry identity before it tests a cluster.

use nervix_licensing_protocol::trust::{DecisionVerification, TrustState};

use super::*;

fn fixture() -> Model {
    let now = DateTime::parse_from_rfc3339("2026-11-10T12:00:00Z")
        .assured("fixed fixture timestamp")
        .with_timezone(&Utc);
    Model::new(
        Url::parse("https://127.0.0.1:12345").assured("fixture URL"),
        now,
    )
    .assured("fixture model builds")
}

#[nervix_primitives::test]
async fn transition_acknowledgement_clamps_both_receipt_bounds_and_preserves_an_extension() {
    for (claimed_offset, accepted_offset) in [(-3_600, -300), (3_600, 300)] {
        let mut model = fixture();
        let cluster = signer().assured("cluster signer");
        let mut common = common(&mut model).await;
        let activation = model
            .create(
                &common,
                &model.license_key.clone(),
                &ActivationKind::Persistent,
                cluster.public_key(),
            )
            .assured("fixture activation");
        model.reduce_domain_quota_with_grace();
        common.request_id = id();
        model
            .refresh(&common, &activation, &[], None)
            .assured("signed reduction");
        let held = model
            .activations
            .get_mut(&activation)
            .verified("activation retained");
        let mut reported = held.decision.transitions[0].clone();
        held.decision.transitions.clear();
        reported.started_at = model.now + TimeDelta::seconds(claimed_offset);
        reported.deadline = reported.started_at + TimeDelta::seconds(900);
        common.request_id = id();
        model
            .refresh(&common, &activation, &[reported.clone()], None)
            .assured("acknowledgement");
        let accepted = model
            .activations
            .get(&activation)
            .verified("activation retained")
            .decision
            .transitions[0]
            .clone();
        assert_eq!(accepted.transition_id, reported.transition_id);
        assert_eq!(
            accepted.started_at,
            model.now + TimeDelta::seconds(accepted_offset)
        );
        assert_eq!(
            accepted.deadline,
            accepted.started_at + TimeDelta::seconds(300)
        );
        model.extend_transitions(std::time::Duration::from_secs(600));
        common.request_id = id();
        model
            .refresh(&common, &activation, &[reported], None)
            .assured("refresh preserves administrator extension");
        let extended = &model
            .activations
            .get(&activation)
            .verified("activation retained")
            .decision
            .transitions[0];
        assert_eq!(extended.transition_id, accepted.transition_id);
        assert_eq!(extended.started_at, accepted.started_at);
        assert_eq!(
            extended.deadline,
            accepted.deadline + TimeDelta::seconds(600)
        );
    }
}

async fn bytes(response: Response) -> Vec<u8> {
    axum::body::to_bytes(response.into_body(), crate::MAX_BODY_BYTES)
        .await
        .assured("fixture response is bounded")
        .to_vec()
}

async fn common(model: &mut Model) -> RequestCommon {
    let cluster_uid = id();
    let response = model.challenge(
        &serde_json::to_vec(&ChallengeRequest { cluster_uid })
            .assured("challenge request serializes"),
    );
    let challenge: ChallengeResponse =
        serde_json::from_slice(&bytes(response).await).assured("typed challenge response");
    RequestCommon {
        protocol: 1,
        request_id: id(),
        challenge: challenge.challenge,
        sent_at: model.now,
        aud: model.address.clone(),
        cluster_uid,
        display_name: "scenario".to_owned(),
        client: ClientDescription {
            product: ClientProduct::NervixServer,
            version: "test".to_owned(),
        },
        key_generation: 1,
        trust_held: TrustHeld {
            working_keys: Vec::new(),
            revocation_serials: Vec::new(),
            succession_serials: Vec::new(),
        },
    }
}

#[nervix_primitives::test]
async fn a_lost_creation_response_reuses_one_signed_decision_and_one_capacity_slot() {
    let mut model = fixture();
    let cluster = signer().assured("cluster signer");
    let request = Request::Create {
        common: common(&mut model).await,
        license_key: model.license_key.clone(),
        kind: ActivationKind::Persistent,
        cluster_key: cluster.public_key().clone(),
    };
    let compact = cluster
        .sign(&request, limits())
        .assured("cluster signs its creation");
    model.script(Fault::LoseResponse);
    let (first, fault) = model.answer("/v1/activations", compact.as_bytes());
    assert!(matches!(fault, Some(Fault::LoseResponse)));
    assert_eq!(first.status(), 201);
    let original = bytes(first).await;
    let (retry, _) = model.answer("/v1/activations", compact.as_bytes());
    assert_eq!(bytes(retry).await, original);
    assert_eq!(model.activation_count(), 1);
    assert_eq!(model.requests, vec![request.clone(), request.clone()]);
    let answer: DecisionResponse = serde_json::from_slice(&original).assured("service response");
    let trust = TrustState::build(&model.bundle, &answer.trust, limits())
        .assured("real two-root signature chain");
    let decision = trust
        .verify_decision(&answer.decision, DecisionVerification::Apply, limits())
        .assured("real working-key signature");
    assert_eq!(
        decision.decision().activation.cluster_uid,
        request.common().cluster_uid
    );
    assert_eq!(
        decision.decision().activation.cluster_key_kid,
        *cluster.key_id()
    );
    assert_eq!(decision.decision().in_reply_to, request.common().request_id);
}

#[nervix_primitives::test]
async fn response_signature_corruption_preserves_the_valid_saved_replay() {
    let mut model = fixture();
    let cluster = signer().assured("cluster signer");
    let request = Request::Create {
        common: common(&mut model).await,
        license_key: model.license_key.clone(),
        kind: ActivationKind::Persistent,
        cluster_key: cluster.public_key().clone(),
    };
    let compact = cluster.sign(&request, limits()).assured("signed request");
    model.script(Fault::CorruptResponseSignature);
    let (first, _) = model.answer("/v1/activations", compact.as_bytes());
    let first: DecisionResponse =
        serde_json::from_slice(&bytes(first).await).assured("typed response");
    let trust = TrustState::build(&model.bundle, &first.trust, limits())
        .assured("unchanged real trust chain");
    assert!(
        trust
            .verify_decision(&first.decision, DecisionVerification::Apply, limits())
            .is_err(),
        "the first transmitted signature must be corrupt"
    );
    let (retry, _) = model.answer("/v1/activations", compact.as_bytes());
    let saved = bytes(retry).await;
    let retry: DecisionResponse = serde_json::from_slice(&saved).assured("typed replay");
    let verified = trust
        .verify_decision(&retry.decision, DecisionVerification::Apply, limits())
        .assured("the saved decision remains correctly signed");
    assert_eq!(verified.decision().in_reply_to, request.common().request_id);
    let (repeated, _) = model.answer("/v1/activations", compact.as_bytes());
    assert_eq!(bytes(repeated).await, saved);
    assert_eq!(model.activation_count(), 1);
    assert_eq!(
        model.requests,
        vec![request.clone(), request.clone(), request]
    );
}

#[nervix_primitives::test]
async fn a_signed_tampering_script_is_distinct_from_a_broken_signature() {
    for fault in [
        Tamper::Cluster,
        Tamper::Service,
        Tamper::Signature,
        Tamper::UnauthorizedKey,
        Tamper::RevokedKey,
    ] {
        let mut model = fixture();
        let cluster = signer().assured("cluster signer");
        let request = Request::Create {
            common: common(&mut model).await,
            license_key: model.license_key.clone(),
            kind: ActivationKind::Persistent,
            cluster_key: cluster.public_key().clone(),
        };
        model.script(Fault::Tamper(fault));
        let compact = cluster.sign(&request, limits()).assured("signed request");
        let (response, _) = model.answer("/v1/activations", compact.as_bytes());
        let answer: DecisionResponse =
            serde_json::from_slice(&bytes(response).await).assured("typed response");
        let trust =
            TrustState::build(&model.bundle, &answer.trust, limits()).assured("real trust chain");
        let result = trust.verify_decision(&answer.decision, DecisionVerification::Apply, limits());
        match fault {
            Tamper::Signature | Tamper::UnauthorizedKey | Tamper::RevokedKey => {
                assert!(result.is_err())
            }
            Tamper::Cluster => assert_ne!(
                result
                    .assured("properly signed wrong binding")
                    .decision()
                    .activation
                    .cluster_uid,
                request.common().cluster_uid
            ),
            Tamper::Service => assert_ne!(
                result
                    .assured("properly signed wrong service")
                    .decision()
                    .service,
                model.address
            ),
            _ => unreachable!(),
        }
    }
}

#[nervix_primitives::test]
async fn late_release_reissues_refresh_fields_without_moving_fixed_deadlines() {
    let mut model = fixture();
    let cluster = signer().assured("cluster signer");
    let mut plan = model.conditions();
    plan.operating.durations.insert(
        OFFLINE_ALLOWANCE.to_owned(),
        ConditionValue::Known(ConditionDuration::Bounded { seconds: 2_400 }),
    );
    model.plan(plan);
    let mut common = common(&mut model).await;
    let activation_id = model
        .create(
            &common,
            &model.license_key.clone(),
            &ActivationKind::Persistent,
            cluster.public_key(),
        )
        .assured("activation is admitted");
    model.reduce_domain_quota_with_grace();
    common.request_id = id();
    model
        .refresh(&common, &activation_id, &[], None)
        .assured("quota reduction issues a grant");
    let before = model
        .activations
        .get(&activation_id)
        .verified("activation retained")
        .decision
        .clone();
    assert!(!before.transitions.is_empty());
    let after_offline = before
        .timing
        .valid_until
        .verified("bounded allowance")
        .checked_add_signed(TimeDelta::seconds(1))
        .verified("fixture deadline fits");
    model.set_time(after_offline);
    common.request_id = id();
    common.sent_at = model.now;
    model
        .release(&common, activation_id, model.now)
        .assured("release is admitted");
    let after = &model
        .activations
        .get(&activation_id)
        .verified("activation retained")
        .decision;
    assert_eq!(after.status, DecisionStatus::Released);
    assert!(after.issued_at <= after.timing.refresh_after);
    assert!(after.timing.refresh_after <= after.timing.overdue_at);
    assert!(after.timing.overdue_at <= after.timing.valid_until.verified("bounded allowance"));
    assert_eq!(after.activation, before.activation);
    assert_eq!(after.license, before.license);
    assert_eq!(after.timing.term_ends_at, before.timing.term_ends_at);
    assert_eq!(
        after.timing.ephemeral_expires_at,
        before.timing.ephemeral_expires_at
    );
    assert!(
        after.transitions.is_empty(),
        "elapsed grants are not acknowledged as current release grants"
    );
}
