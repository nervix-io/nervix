//! Positive and negative checks of every trust and decision verification rule.

use std::collections::BTreeSet;

use meticulous::{OptionExt as _, ResultExt as _};

use crate::{conditions::*, document::*, documents::*, test_fixture::*, trust::*, validation::*};

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
