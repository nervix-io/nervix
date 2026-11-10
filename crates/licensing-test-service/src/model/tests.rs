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

async fn bytes(response: Response) -> Vec<u8> {
    axum::body::to_bytes(response.into_body(), crate::MAX_BODY_BYTES)
        .await
        .assured("fixture response is bounded")
        .to_vec()
}

async fn common(model: &mut Model) -> RequestCommon {
    common_for(model, id()).await
}

async fn common_for(model: &mut Model, cluster_uid: Uuid) -> RequestCommon {
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
async fn an_expired_ephemeral_activation_releases_its_cluster_and_its_capacity() {
    let mut model = fixture();
    let mut plan = model.conditions();
    plan.operating.quotas.insert(
        EPHEMERAL_ACTIVATIONS.to_owned(),
        ConditionValue::Known(Quota::Bounded { value: 1 }),
    );
    model.plan(plan);
    let cluster = signer().assured("cluster signer");
    let expired_cluster = id();
    async fn create(
        model: &mut Model,
        cluster: &DocumentSigner,
        cluster_uid: Uuid,
    ) -> (u16, Vec<u8>) {
        let request = Request::Create {
            common: common_for(model, cluster_uid).await,
            license_key: model.license_key.clone(),
            kind: ActivationKind::Ephemeral,
            cluster_key: cluster.public_key().clone(),
        };
        let compact = cluster.sign(&request, limits()).assured("signed creation");
        let (response, _) = model.answer("/v1/activations", compact.as_bytes());
        let status = response.status().as_u16();
        (status, bytes(response).await)
    }
    let code = |body: &[u8]| {
        serde_json::from_slice::<ProblemDetails>(body)
            .assured("a typed problem")
            .kind
    };

    let (status, body) = create(&mut model, &cluster, expired_cluster).await;
    assert_eq!(status, 201);
    let first: DecisionResponse = serde_json::from_slice(&body).assured("service response");
    let first = TrustState::build(&model.bundle, &first.trust, limits())
        .assured("real trust chain")
        .verify_decision(&first.decision, DecisionVerification::Apply, limits())
        .assured("real working-key signature")
        .decision()
        .activation
        .clone();
    let expires_at = first
        .ephemeral_expires_at
        .verified("an ephemeral activation carries its expiry");
    let (_, body) = create(&mut model, &cluster, expired_cluster).await;
    assert!(matches!(code(&body), ProblemKind::AlreadyActivated { .. }));
    let (_, body) = create(&mut model, &cluster, id()).await;
    assert_eq!(code(&body), ProblemKind::QuotaExhausted);
    assert_eq!(model.activation_expired(first.activation_id), Some(false));

    model.set_time(expires_at);
    assert_eq!(model.activation_expired(first.activation_id), Some(true));
    assert_eq!(model.occupied_activations(ActivationKind::Ephemeral), 0);
    // A refresh answers status expired, and its emptied usage takes the activation's next
    // conditions revision, so the cluster can order it after the decision it holds.
    let mut refreshed = Vec::new();
    for _ in 0..2 {
        let request = Request::Refresh {
            common: common_for(&mut model, expired_cluster).await,
            activation_id: first.activation_id,
            state: RefreshState::Adoption,
            transitions: Vec::new(),
        };
        let compact = cluster.sign(&request, limits()).assured("signed refresh");
        let path = format!("/v1/activations/{}/refresh", first.activation_id);
        let (response, _) = model.answer(&path, compact.as_bytes());
        assert_eq!(response.status().as_u16(), 200);
        let response: DecisionResponse =
            serde_json::from_slice(&bytes(response).await).assured("service response");
        let decision = TrustState::build(&model.bundle, &response.trust, limits())
            .assured("real trust chain")
            .verify_decision(&response.decision, DecisionVerification::Apply, limits())
            .assured("real working-key signature")
            .decision()
            .clone();
        assert_eq!(decision.status, DecisionStatus::Expired);
        assert!(decision.conditions.usage.quotas.is_empty());
        refreshed.push(decision.conditions.conditions_revision);
    }
    let expired_revision = model
        .conditions_revision
        .checked_add(1)
        .assured("the fixture's conditions revision is small");
    assert_eq!(
        refreshed,
        vec![expired_revision, expired_revision],
        "the expired conditions take the next revision once and keep it"
    );
    let (status, body) = create(&mut model, &cluster, expired_cluster).await;
    assert_eq!(status, 201, "the expired cluster activates again");
    let second: DecisionResponse = serde_json::from_slice(&body).assured("service response");
    let second = TrustState::build(&model.bundle, &second.trust, limits())
        .assured("real trust chain")
        .verify_decision(&second.decision, DecisionVerification::Apply, limits())
        .assured("real working-key signature")
        .decision()
        .activation
        .clone();
    assert_ne!(second.activation_id, first.activation_id);
    assert!(second.ephemeral_expires_at > Some(expires_at));
    assert_eq!(model.occupied_activations(ActivationKind::Ephemeral), 1);
}
