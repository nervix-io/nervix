//! Positive and negative checks of every trust and decision verification rule.

use std::collections::BTreeSet;

use meticulous::{OptionExt as _, ResultExt as _};

use crate::{
    conditions::*, document::*, documents::*, requests::*, test_fixture::*, trust::*, validation::*,
};

fn validate(
    fixture: &Fixture,
    decision: &Decision,
    binding: &Activation,
    previous: Option<&Decision>,
) -> Result<DecisionDisposition, error_stack::Report<DecisionRejection>> {
    let trust = fixture.trust();
    let service = fixture.decision().service;
    let revisions = BTreeSet::from([4, 5]);
    let context = DecisionContext {
        service: &service,
        binding,
        operation: DecisionOperation::Apply,
        request_id: id(10),
        committed: previous,
        known_conditions_revisions: &revisions,
    };
    let validated = trust.validate_decision(
        &fixture.sign(decision),
        DecisionVerification::Apply,
        context,
        limits(),
    )?;
    Ok(validated.disposition())
}

#[test]
fn decision_rules_accept_current_values_and_idempotent_retries() {
    let fixture = Fixture::new();
    let decision = fixture.decision();
    assert_eq!(
        validate(&fixture, &decision, &decision.activation, None).assured("valid decision"),
        DecisionDisposition::Apply {
            unknown_conditions: 0
        }
    );
    assert_eq!(
        validate(&fixture, &decision, &decision.activation, Some(&decision))
            .assured("identical revision is a no-op"),
        DecisionDisposition::AlreadyApplied
    );
}

#[test]
fn decision_rules_refuse_protocol_service_binding_revision_timing_and_content() {
    let fixture = Fixture::new();
    let original = fixture.decision();
    let mut decision = original.clone();
    decision.protocol = u8::MAX;
    assert!(matches!(
        validate(&fixture, &decision, &original.activation, None)
            .err()
            .assured("unsupported protocol")
            .current_context(),
        DecisionRejection::Protocol { .. }
    ));
    decision = original.clone();
    decision
        .service
        .set_host(Some("another-deployment.example"))
        .assured("valid host");
    assert!(matches!(
        validate(&fixture, &decision, &original.activation, None)
            .err()
            .assured("different service")
            .current_context(),
        DecisionRejection::Service
    ));
    decision = original.clone();
    decision.activation.cluster_uid = id(999);
    assert!(matches!(
        validate(&fixture, &decision, &original.activation, None)
            .err()
            .assured("different cluster")
            .current_context(),
        DecisionRejection::Binding {
            field: BindingField::Cluster
        }
    ));
    decision = original.clone();
    decision.decision_revision = 1;
    assert!(matches!(
        validate(&fixture, &decision, &original.activation, Some(&original))
            .err()
            .assured("revision rollback")
            .current_context(),
        DecisionRejection::Revision {
            field: RevisionField::Decision
        }
    ));
    decision = original.clone();
    decision.timing.overdue_at = at(-1);
    assert!(matches!(
        validate(&fixture, &decision, &original.activation, None)
            .err()
            .assured("inverted timing")
            .current_context(),
        DecisionRejection::Timing { .. }
    ));
    decision = original.clone();
    decision.conditions.usage.quotas.remove(CLUSTER_NODES);
    assert!(matches!(
        validate(&fixture, &decision, &original.activation, None)
            .err()
            .assured("incomplete known conditions")
            .current_context(),
        DecisionRejection::Content
    ));
}

#[test]
fn envelope_rule_refuses_an_unauthorized_signer() {
    let fixture = Fixture::new();
    let foreign = signer();
    let decision = fixture.decision();
    let compact = foreign
        .sign(&decision, limits())
        .assured("foreign signer can sign its own payload");
    assert!(matches!(
        fixture
            .trust()
            .verify_decision(&compact, DecisionVerification::Apply, limits())
            .err()
            .assured("no authorization for foreign key")
            .current_context(),
        TrustError::UnknownSigningKey
    ));
}

#[test]
fn working_key_validity_is_inclusive_and_checked_at_the_signed_issue_time() {
    let fixture = Fixture::new();
    let mut authorization = fixture.authorization();
    authorization.not_before = now();
    authorization.not_after = now();
    let documents = TrustDocuments {
        documents: vec![SignedTrustDocument::Signatures(
            fixture.root_sign(&authorization),
        )],
    };
    let trust = TrustState::build(&fixture.embedded(), &documents, limits())
        .assured("valid zero-width authorization window");
    let mut decision = fixture.decision();
    assert!(
        trust
            .verify_decision(
                &fixture.sign(&decision),
                DecisionVerification::Apply,
                limits()
            )
            .is_ok()
    );
    decision.issued_at = at(1);
    assert!(matches!(
        trust
            .verify_decision(
                &fixture.sign(&decision),
                DecisionVerification::Apply,
                limits()
            )
            .err()
            .assured("issue time outside authorization")
            .current_context(),
        TrustError::NotAuthorized
    ));
    authorization.uses.clear();
    let documents = TrustDocuments {
        documents: vec![SignedTrustDocument::Signatures(
            fixture.root_sign(&authorization),
        )],
    };
    let trust = TrustState::build(&fixture.embedded(), &documents, limits())
        .assured("an empty use grants nothing");
    assert!(
        trust
            .verify_decision(
                &fixture.sign(&fixture.decision()),
                DecisionVerification::Apply,
                limits()
            )
            .is_err()
    );
}

#[test]
fn revocation_is_forward_looking_and_never_invalidates_a_reloaded_signature() {
    let fixture = Fixture::new();
    let mut documents = fixture.documents();
    let revocation = WorkingKeyRevocation {
        doc: RevocationKind::WorkingKeyRevocation,
        protocol: 1,
        serial: 7,
        issued_at: now(),
        root_epoch: 0,
        kid: fixture.working.key_id().clone(),
        revoked_at: at(5_000),
        reason: "fixture ceremony".to_owned(),
    };
    documents.documents.push(SignedTrustDocument::Signatures(
        fixture.root_sign(&revocation),
    ));
    let trust =
        TrustState::build(&fixture.embedded(), &documents, limits()).assured("verified revocation");
    let compact = fixture.sign(&fixture.decision());
    assert!(matches!(
        trust
            .verify_decision(&compact, DecisionVerification::Apply, limits())
            .err()
            .assured(
                "new decisions from revoked keys are refused even before informational revoked_at"
            )
            .current_context(),
        TrustError::SigningKeyRevoked
    ));
    let retained = trust
        .verify_decision(&compact, DecisionVerification::Reload, limits())
        .assured("committed signature remains verifiable");
    assert!(retained.signing_key_revoked());
    assert_eq!(retained.compact(), compact);
    assert_eq!(trust.original_documents(), &documents);
}

#[test]
fn distinct_root_threshold_and_payload_identity_are_required() {
    let fixture = Fixture::new();
    let authorization = fixture.authorization();
    let mut set = fixture.root_sign(&authorization);
    set.signatures.pop();
    let documents = TrustDocuments {
        documents: vec![SignedTrustDocument::Signatures(set.clone())],
    };
    assert!(TrustState::build(&fixture.embedded(), &documents, limits()).is_err());
    set.signatures.push(set.signatures[0].clone());
    assert!(set.parse(DocumentType::WorkingKey, limits()).is_err());
    let mut changed = authorization;
    changed.serial = 9;
    let set = SignatureSet {
        signatures: vec![
            fixture.roots[0]
                .sign(&changed, limits())
                .assured("current payload"),
            fixture.roots[1]
                .sign(&fixture.authorization(), limits())
                .assured("different current payload"),
        ],
    };
    assert!(matches!(
        set.parse(DocumentType::WorkingKey, limits())
            .err()
            .assured("members must have equal payloads")
            .current_context(),
        DocumentError::PayloadMismatch
    ));
}

#[test]
fn verified_succession_adds_only_the_next_epoch() {
    let fixture = Fixture::new();
    let next = [signer(), signer(), signer()];
    let succession = RootSuccession {
        doc: SuccessionKind::RootSuccession,
        protocol: 1,
        serial: 4,
        issued_at: at(-200),
        from_epoch: 0,
        to_epoch: 1,
        roots: next
            .iter()
            .map(|signer| signer.public_key().clone())
            .collect(),
        threshold: std::num::NonZeroUsize::new(2).assured("nonzero threshold"),
        effective_at: at(-100),
    };
    let mut authorization = fixture.authorization();
    authorization.root_epoch = 1;
    let set = SignatureSet {
        signatures: vec![
            next[0]
                .sign(&authorization, limits())
                .assured("authorized next root"),
            next[1]
                .sign(&authorization, limits())
                .assured("authorized next root"),
        ],
    };
    let documents = TrustDocuments {
        documents: vec![
            SignedTrustDocument::Signatures(set),
            SignedTrustDocument::Signatures(fixture.root_sign(&succession)),
        ],
    };
    let trust = TrustState::build(&fixture.embedded(), &documents, limits())
        .assured("input order does not replace the trust chain");
    assert!(
        trust
            .verify_decision(
                &fixture.sign(&fixture.decision()),
                DecisionVerification::Apply,
                limits()
            )
            .is_ok()
    );
    let mut gap = succession;
    gap.to_epoch = 2;
    let documents = TrustDocuments {
        documents: vec![SignedTrustDocument::Signatures(fixture.root_sign(&gap))],
    };
    assert!(matches!(
        TrustState::build(&fixture.embedded(), &documents, limits())
            .err()
            .assured("succession may not skip an epoch")
            .current_context(),
        TrustError::EpochGap
    ));
}

#[test]
fn unknown_condition_values_are_retained_and_never_reinterpret_known_names() {
    let fixture = Fixture::new();
    let mut decision = fixture.decision();
    let extension =
        serde_json::json!({"future": [null, {"precise": -0.0}], "kind": "future_quota"});
    decision.conditions.usage.quotas.insert(
        "future.quota".to_owned(),
        ConditionValue::Extension(extension.clone()),
    );
    assert_eq!(
        validate(&fixture, &decision, &decision.activation, None)
            .assured("unknown name is ignored"),
        DecisionDisposition::Apply {
            unknown_conditions: 1
        }
    );
    signed_round_trip(&fixture.working, &decision);
    decision.conditions.usage.quotas.insert(
        CLUSTER_NODES.to_owned(),
        ConditionValue::Extension(extension),
    );
    assert!(validate(&fixture, &decision, &decision.activation, None).is_err());
}

#[test]
fn every_binding_field_and_pending_rotation_are_checked_against_committed_identity() {
    let fixture = Fixture::new();
    let original = fixture.decision();
    struct BindingMutation {
        field: BindingField,
        mutate: fn(&mut Decision),
    }
    let mutations = [
        BindingMutation {
            field: BindingField::Activation,
            mutate: |d| d.activation.activation_id = id(99),
        },
        BindingMutation {
            field: BindingField::Kind,
            mutate: |d| d.activation.kind = ActivationKind::Ephemeral,
        },
        BindingMutation {
            field: BindingField::Generation,
            mutate: |d| d.activation.generation = 2,
        },
        BindingMutation {
            field: BindingField::Expiry,
            mutate: |d| d.activation.ephemeral_expires_at = Some(at(100)),
        },
        BindingMutation {
            field: BindingField::Creation,
            mutate: |d| d.activation.created_at = at(-200),
        },
        BindingMutation {
            field: BindingField::Request,
            mutate: |d| d.in_reply_to = id(99),
        },
    ];
    for BindingMutation { field, mutate } in mutations {
        let mut changed = original.clone();
        mutate(&mut changed);
        let error = validate(&fixture, &changed, &original.activation, None)
            .err()
            .assured("binding mutation");
        assert!(
            matches!(error.current_context(), DecisionRejection::Binding { field: rejected } if *rejected == field)
        );
    }
    let mut changed = original.clone();
    changed.activation.cluster_key_kid = fixture.working.key_id().clone();
    assert!(validate(&fixture, &changed, &original.activation, None).is_err());
    changed.activation.generation = 2;
    let service = original.service.clone();
    let revisions = BTreeSet::from([5]);
    let context = DecisionContext {
        service: &service,
        binding: &original.activation,
        operation: DecisionOperation::CompleteClusterKeyRotation {
            pending_key: fixture.working.key_id(),
        },
        request_id: id(10),
        committed: Some(&original),
        known_conditions_revisions: &revisions,
    };
    changed.decision_revision = 3;
    assert!(
        fixture
            .trust()
            .validate_decision(
                &fixture.sign(&changed),
                DecisionVerification::Apply,
                context,
                limits()
            )
            .is_ok()
    );
    changed.activation.generation = 3;
    let context = DecisionContext {
        service: &service,
        binding: &original.activation,
        operation: DecisionOperation::CompleteClusterKeyRotation {
            pending_key: fixture.working.key_id(),
        },
        request_id: id(10),
        committed: Some(&original),
        known_conditions_revisions: &revisions,
    };
    assert!(
        fixture
            .trust()
            .validate_decision(
                &fixture.sign(&changed),
                DecisionVerification::Apply,
                context,
                limits()
            )
            .is_err()
    );
}

#[test]
fn conditions_revisions_issue_time_and_equal_revision_identity_are_monotone() {
    let fixture = Fixture::new();
    let original = fixture.decision();
    let mut changed = original.clone();
    changed.decision_revision = 3;
    changed.conditions.conditions_revision = 4;
    assert!(matches!(
        validate(&fixture, &changed, &original.activation, Some(&original))
            .err()
            .assured("conditions rollback")
            .current_context(),
        DecisionRejection::Revision {
            field: RevisionField::Conditions
        }
    ));
    changed = original.clone();
    changed.decision_revision = 3;
    changed.issued_at = at(-1);
    assert!(matches!(
        validate(&fixture, &changed, &original.activation, Some(&original))
            .err()
            .assured("issue-time rollback")
            .current_context(),
        DecisionRejection::Revision {
            field: RevisionField::IssuedAt
        }
    ));
    changed = original.clone();
    changed.decision_id = id(99);
    assert!(matches!(
        validate(&fixture, &changed, &original.activation, Some(&original))
            .err()
            .assured("same revision with different decision")
            .current_context(),
        DecisionRejection::Revision {
            field: RevisionField::ReusedDecision
        }
    ));
    changed = original.clone();
    changed.decision_revision = 3;
    changed.conditions.usage.quotas.insert(
        CLUSTER_NODES.to_owned(),
        ConditionValue::Known(Quota::Bounded { value: 3 }),
    );
    assert!(matches!(
        validate(&fixture, &changed, &original.activation, Some(&original))
            .err()
            .assured("same condition revision with changed values")
            .current_context(),
        DecisionRejection::Revision {
            field: RevisionField::ReusedConditions
        }
    ));
    changed.conditions.conditions_revision = 6;
    assert!(validate(&fixture, &changed, &original.activation, Some(&original)).is_ok());
}

#[test]
fn bounded_timing_and_firm_ephemeral_status_are_validated() {
    let fixture = Fixture::new();
    let mut decision = fixture.decision();
    decision.timing.valid_until = Some(at(180));
    assert!(validate(&fixture, &decision, &decision.activation, None).is_ok());
    decision.timing.valid_until = Some(at(179));
    assert!(validate(&fixture, &decision, &decision.activation, None).is_err());
    decision = fixture.decision();
    decision.activation.kind = ActivationKind::Ephemeral;
    decision.activation.ephemeral_expires_at = Some(now());
    decision.timing.ephemeral_expires_at = Some(now());
    assert!(validate(&fixture, &decision, &decision.activation, None).is_err());
    decision.status = DecisionStatus::Expired;
    decision.conditions.usage = empty_conditions().usage;
    assert!(validate(&fixture, &decision, &decision.activation, None).is_ok());
    decision.activation.ephemeral_expires_at = Some(at(1));
    decision.timing.ephemeral_expires_at = Some(at(1));
    assert!(validate(&fixture, &decision, &decision.activation, None).is_err());
}

#[test]
fn acknowledged_transitions_require_known_revisions_and_an_ordered_deadline() {
    let fixture = Fixture::new();
    let mut decision = fixture.decision();
    let transition = Transition {
        transition_id: id(90),
        from_conditions_revision: 4,
        to_conditions_revision: 5,
        started_at: now(),
        deadline: at(100),
        trigger: TransitionTrigger::Tick {
            decision_id: decision.decision_id,
        },
        reason: TransitionReason::PlanChanged,
        affected: vec![AffectedQuota {
            condition: CLUSTER_NODES.to_owned(),
            previous: Quota::Unlimited,
            target: Quota::Bounded { value: 2 },
            usage: 3,
        }],
    };
    decision.transitions.push(transition.clone());
    assert!(validate(&fixture, &decision, &decision.activation, None).is_ok());
    round_trip(&transition);
    decision.transitions[0].from_conditions_revision = 999;
    assert!(validate(&fixture, &decision, &decision.activation, None).is_err());
    decision.transitions[0] = transition.clone();
    decision.transitions[0].deadline = at(-1);
    assert!(validate(&fixture, &decision, &decision.activation, None).is_err());
    decision.transitions[0] = transition.clone();
    decision.transitions.push(transition);
    assert!(validate(&fixture, &decision, &decision.activation, None).is_err());
}

#[test]
fn embedded_epochs_protocol_and_signed_authorization_shape_are_checked() {
    let fixture = Fixture::new();
    let mutations: [fn(&mut WorkingKeyAuthorization); 3] = [
        |a| a.protocol = 2,
        |a| a.root_epoch = 99,
        |a| a.not_after = at(-20_000),
    ];
    for mutate in mutations {
        let mut authorization = fixture.authorization();
        mutate(&mut authorization);
        let documents = TrustDocuments {
            documents: vec![SignedTrustDocument::Signatures(
                fixture.root_sign(&authorization),
            )],
        };
        assert!(TrustState::build(&fixture.embedded(), &documents, limits()).is_err());
    }
    let documents = fixture.documents();
    let trust = TrustState::build(&fixture.embedded(), &documents, limits())
        .assured("trusted embedded epoch");
    assert_eq!(trust.original_documents(), &documents);
    let mut tampered = documents;
    let SignedTrustDocument::Signatures(set) = &mut tampered.documents[0] else {
        panic!("fixture uses a signature set");
    };
    let mut framing: jose_jws::Flattened = set.signatures[0].parse().assured("maintained framing");
    framing.signature.signature = vec![0; 64].into();
    set.signatures[0] = framing.to_string();
    assert!(TrustState::build(&fixture.embedded(), &tampered, limits()).is_err());
}

#[test]
fn nonactive_statuses_carry_free_usage_and_preserve_operating_conditions() {
    let fixture = Fixture::new();
    for status in [
        DecisionStatus::Suspended,
        DecisionStatus::Revoked,
        DecisionStatus::Released,
        DecisionStatus::Transferred,
        DecisionStatus::TermEnded,
    ] {
        let mut decision = fixture.decision();
        decision.status = status;
        if status == DecisionStatus::TermEnded {
            decision.license.term_ends_at = now();
            decision.timing.term_ends_at = now();
        }
        assert!(validate(&fixture, &decision, &decision.activation, None).is_ok());
        decision.conditions.usage.quotas.insert(
            CLUSTER_NODES.to_owned(),
            ConditionValue::Known(Quota::Bounded { value: 0 }),
        );
        assert!(validate(&fixture, &decision, &decision.activation, None).is_err());
    }
}

fn sign_request(signer: &DocumentSigner, request: &Request) -> String {
    signer
        .sign(request, limits())
        .assured("bounded current request")
}

fn flip_signature_bit(compact: &str) -> String {
    let mut framing: jose_jws::Flattened = compact.parse().assured("maintained framing");
    let mut signature = framing.signature.signature.as_ref().to_vec();
    signature[0] ^= 1;
    framing.signature.signature = signature.into();
    framing.to_string()
}

fn verification_failure(compact: &str, signer: RequestSigner<'_>) -> RequestVerificationError {
    let failure = Request::verify(compact, signer, limits())
        .err()
        .assured("the request must be refused");
    *failure.current_context()
}

#[test]
fn self_signed_creation_verifies_only_under_its_carried_cluster_key() {
    let fixture = Fixture::new();
    let mut requests = fixture.requests();
    let refresh = requests.remove(2);
    let creation = requests.remove(0);
    let compact = sign_request(&fixture.cluster, &creation);
    assert_eq!(
        Request::verify(&compact, RequestSigner::CarriedClusterKey, limits())
            .assured("a creation signed by the key it carries"),
        creation
    );
    // A creation carrying one key and signed by another proves possession of neither.
    let mut foreign = creation.clone();
    let Request::Create { cluster_key, .. } = &mut foreign else {
        panic!("the fixture's first operation is a creation");
    };
    *cluster_key = fixture.working.public_key().clone();
    assert!(matches!(
        verification_failure(
            &sign_request(&fixture.cluster, &foreign),
            RequestSigner::CarriedClusterKey
        ),
        RequestVerificationError::Signer
    ));
    assert!(matches!(
        verification_failure(
            &flip_signature_bit(&compact),
            RequestSigner::CarriedClusterKey
        ),
        RequestVerificationError::Signature
    ));
    // Only a creation carries its own signer.
    assert!(matches!(
        verification_failure(
            &sign_request(&fixture.cluster, &refresh),
            RequestSigner::CarriedClusterKey
        ),
        RequestVerificationError::Payload
    ));
    assert!(matches!(
        verification_failure("no compact serialization", RequestSigner::CarriedClusterKey),
        RequestVerificationError::Envelope
    ));
    // A decision is not a request, whoever signed it.
    assert!(matches!(
        verification_failure(
            &fixture.sign(&fixture.decision()),
            RequestSigner::CarriedClusterKey
        ),
        RequestVerificationError::Envelope
    ));
}

#[test]
fn bound_requests_verify_only_under_the_bound_key_and_report_payload_failures_apart() {
    let fixture = Fixture::new();
    let refresh = fixture.requests().remove(2);
    let compact = sign_request(&fixture.cluster, &refresh);
    let bound = RequestSigner::Bound(fixture.cluster.public_key());
    assert_eq!(
        Request::verify(&compact, bound, limits()).assured("a refresh signed by its binding"),
        refresh
    );
    assert!(matches!(
        verification_failure(&compact, RequestSigner::Bound(fixture.working.public_key())),
        RequestVerificationError::Signer
    ));
    assert!(matches!(
        verification_failure(&flip_signature_bit(&compact), bound),
        RequestVerificationError::Signature
    ));
    // A valid signature over a payload that is no current request.
    let private = nervix_crypto::ecdsa::EcdsaP256KeyPair::generate_pkcs8()
        .assured("AWS-LC generates a fixture key");
    let signer = DocumentSigner::from_pkcs8(private.as_ref())
        .assured("the generated key has the P-256 PKCS#8 profile");
    let raw = nervix_crypto::jws::SigningKey::from_pkcs8(private.as_ref())
        .assured("the generated key imports");
    let mut payload = serde_json::to_value(&refresh).assured("current request");
    payload["unexpected"] = serde_json::json!(true);
    let extended = raw
        .sign(
            DocumentType::Request.as_str(),
            signer.key_id().as_ref(),
            &payload,
        )
        .assured("the maintained signer signs any JSON payload");
    let failure = Request::verify(
        &extended,
        RequestSigner::Bound(signer.public_key()),
        limits(),
    )
    .err()
    .assured("an extra member is refused");
    assert!(matches!(
        failure.current_context(),
        RequestVerificationError::Payload
    ));
    assert!(matches!(
        failure.downcast_ref::<DocumentError>(),
        Some(DocumentError::Payload)
    ));
    let array = raw
        .sign(
            DocumentType::Request.as_str(),
            signer.key_id().as_ref(),
            &serde_json::json!([1, 2]),
        )
        .assured("the maintained signer signs any JSON payload");
    assert!(matches!(
        verification_failure(&array, RequestSigner::Bound(signer.public_key())),
        RequestVerificationError::Payload
    ));
}

struct SuppliedTrust {
    documents: TrustDocuments,
    authorization: WorkingKeyAuthorization,
    revocation: WorkingKeyRevocation,
    succession: RootSuccession,
    next_roots: Vec<crate::keys::PublicKey>,
}

/// A revocation of another key, the working key's authorization and a succession to epoch 1,
/// supplied in that order, which is not the order the state verifies them in.
fn supplied_trust(fixture: &Fixture) -> SuppliedTrust {
    let next = [signer(), signer(), signer()];
    let next_roots: Vec<crate::keys::PublicKey> = next
        .iter()
        .map(|signer| signer.public_key().clone())
        .collect();
    let revocation = WorkingKeyRevocation {
        doc: RevocationKind::WorkingKeyRevocation,
        protocol: 1,
        serial: 2,
        issued_at: at(-50),
        root_epoch: 0,
        kid: fixture.cluster.key_id().clone(),
        revoked_at: at(-50),
        reason: "retired by the ceremony".to_owned(),
    };
    let succession = RootSuccession {
        doc: SuccessionKind::RootSuccession,
        protocol: 1,
        serial: 3,
        issued_at: at(-40),
        from_epoch: 0,
        to_epoch: 1,
        roots: next_roots.clone(),
        threshold: std::num::NonZeroUsize::new(2).assured("nonzero threshold"),
        effective_at: at(-30),
    };
    let authorization = fixture.authorization();
    let documents = TrustDocuments {
        documents: vec![
            SignedTrustDocument::Signatures(fixture.root_sign(&revocation)),
            SignedTrustDocument::Signatures(fixture.root_sign(&authorization)),
            SignedTrustDocument::Signatures(fixture.root_sign(&succession)),
        ],
    };
    SuppliedTrust {
        documents,
        authorization,
        revocation,
        succession,
        next_roots,
    }
}

#[test]
fn verified_trust_documents_state_their_payloads_in_supplied_order() {
    let fixture = Fixture::new();
    let supplied = supplied_trust(&fixture);
    let trust = TrustState::build(&fixture.embedded(), &supplied.documents, limits())
        .assured("every supplied document verifies under epoch 0");
    let mut signed = Vec::new();
    let mut contents = Vec::new();
    for document in trust.documents() {
        signed.push(document.signed().clone());
        contents.push(document.content().clone());
    }
    assert_eq!(signed, supplied.documents.documents);
    assert_eq!(
        contents,
        vec![
            TrustDocumentContent::Revocation(supplied.revocation.clone()),
            TrustDocumentContent::Authorization(supplied.authorization.clone()),
            TrustDocumentContent::Succession(supplied.succession.clone()),
        ]
    );
    let facts: Vec<(DocumentType, u64, u64)> = contents
        .iter()
        .map(|content| {
            (
                content.document_type(),
                content.serial(),
                content.root_epoch(),
            )
        })
        .collect();
    assert_eq!(
        facts,
        vec![
            (DocumentType::Revocation, 2, 0),
            (DocumentType::WorkingKey, 1, 0),
            (DocumentType::Succession, 3, 0),
        ]
    );
    let epochs: Vec<RootSet> = trust.root_sets().cloned().collect();
    let mut expected = fixture.embedded().epochs;
    expected.push(RootSet {
        epoch: 1,
        roots: supplied.next_roots.clone(),
        threshold: supplied.succession.threshold,
    });
    assert_eq!(epochs, expected);
    let working = fixture.working.key_id();
    assert_eq!(
        trust.authorization(working, now()),
        Some(&supplied.authorization)
    );
    assert_eq!(trust.authorization(working, at(-10_001)), None);
    assert_eq!(trust.authorization(fixture.cluster.key_id(), now()), None);
    assert!(trust.is_revoked(fixture.cluster.key_id()));
    assert!(!trust.is_revoked(working));
}

#[test]
fn missing_documents_are_what_the_holder_lacks_for_the_signing_key() {
    let fixture = Fixture::new();
    let supplied = supplied_trust(&fixture);
    let trust = TrustState::build(&fixture.embedded(), &supplied.documents, limits())
        .assured("every supplied document verifies under epoch 0");
    let working = fixture.working.key_id();
    let nothing = TrustHeld {
        working_keys: Vec::new(),
        revocation_serials: Vec::new(),
        succession_serials: Vec::new(),
    };
    assert_eq!(trust.missing(&nothing, working), supplied.documents);
    let everything = TrustHeld {
        working_keys: vec![working.clone()],
        revocation_serials: vec![2],
        succession_serials: vec![3],
    };
    assert_eq!(
        trust.missing(&everything, working),
        TrustDocuments {
            documents: Vec::new()
        }
    );
    // Another key's decision needs no authorization of the working key.
    let documents = &supplied.documents.documents;
    assert_eq!(
        trust.missing(&nothing, fixture.cluster.key_id()),
        TrustDocuments {
            documents: vec![documents[0].clone(), documents[2].clone()]
        }
    );
    // A succession the build embeds is lacking too, and it comes first.
    let mut embedded = fixture.embedded();
    let SignedTrustDocument::Signatures(succession) = documents[2].clone() else {
        panic!("the supplied succession is a signature set");
    };
    embedded.successions.push(succession);
    let authorization_only = TrustDocuments {
        documents: vec![documents[1].clone()],
    };
    let trust = TrustState::build(&embedded, &authorization_only, limits())
        .assured("the embedded succession and the authorization verify");
    assert_eq!(
        trust.missing(&nothing, working),
        TrustDocuments {
            documents: vec![documents[2].clone(), documents[1].clone()]
        }
    );
    assert_eq!(trust.root_sets().count(), 2);
}

#[test]
fn publication_lists_root_sets_and_every_signed_document_by_kind() {
    let fixture = Fixture::new();
    let supplied = supplied_trust(&fixture);
    let trust = TrustState::build(&fixture.embedded(), &supplied.documents, limits())
        .assured("every supplied document verifies under epoch 0");
    let publication = trust.publication();
    let documents = &supplied.documents.documents;
    let mut epochs = fixture.embedded().epochs;
    epochs.push(RootSet {
        epoch: 1,
        roots: supplied.next_roots.clone(),
        threshold: supplied.succession.threshold,
    });
    assert_eq!(
        publication,
        TrustBundleResponse {
            epochs,
            successions: vec![documents[2].clone()],
            authorizations: vec![documents[1].clone()],
            revocations: vec![documents[0].clone()],
        }
    );
    round_trip(&publication);
    // A receiver verifies the signed members against its own anchors and derives the same facts.
    let received = TrustState::build(&fixture.embedded(), &publication.documents(), limits())
        .assured("the published members verify under the receiver's anchors");
    assert_eq!(
        received.authorization(fixture.working.key_id(), now()),
        Some(&supplied.authorization)
    );
    assert!(received.is_revoked(fixture.cluster.key_id()));
    assert_eq!(received.root_sets().count(), 2);
}

#[test]
fn a_serial_conflicts_only_between_documents_with_different_signed_bytes() {
    let fixture = Fixture::new();
    let authorization =
        SignedTrustDocument::Signatures(fixture.root_sign(&fixture.authorization()));
    // The same signed document twice is one document, not a conflict.
    let repeated = TrustDocuments {
        documents: vec![authorization.clone(), authorization.clone()],
    };
    let trust = TrustState::build(&fixture.embedded(), &repeated, limits())
        .assured("an identical repeated document verifies");
    assert_eq!(trust.documents().len(), 2);
    // Another document of the same type under the same serial is refused.
    let mut renewed = fixture.authorization();
    renewed.not_after = at(20_000);
    let conflicting = TrustDocuments {
        documents: vec![
            authorization.clone(),
            SignedTrustDocument::Signatures(fixture.root_sign(&renewed)),
        ],
    };
    assert!(matches!(
        TrustState::build(&fixture.embedded(), &conflicting, limits())
            .err()
            .assured("one serial cannot name two documents")
            .current_context(),
        TrustError::SerialConflict
    ));
    // A document of another type may share the serial.
    let revocation = WorkingKeyRevocation {
        doc: RevocationKind::WorkingKeyRevocation,
        protocol: 1,
        serial: fixture.authorization().serial,
        issued_at: at(-50),
        root_epoch: 0,
        kid: fixture.cluster.key_id().clone(),
        revoked_at: at(-50),
        reason: "another type".to_owned(),
    };
    let distinct_types = TrustDocuments {
        documents: vec![
            authorization,
            SignedTrustDocument::Signatures(fixture.root_sign(&revocation)),
        ],
    };
    assert!(TrustState::build(&fixture.embedded(), &distinct_types, limits()).is_ok());
}
