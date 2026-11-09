//! Bounded complete-value round trips of every current protocol representation.

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use meticulous::{OptionExt as _, ResultExt as _};

use crate::{conditions::*, documents::*, problems::*, requests::*, test_fixture::*};

pub fn number(input: &[u8]) -> u64 {
    let mut bytes = [0; 8];
    let length = input.len().min(bytes.len());
    bytes[..length].copy_from_slice(&input[..length]);
    u64::from_le_bytes(bytes)
}

pub fn text(input: &[u8]) -> String {
    format!("λ\n\"{}", URL_SAFE_NO_PAD.encode(input))
}

pub fn problem_kinds(value: u64) -> Vec<ProblemKind> {
    vec![
        ProblemKind::InvalidRequest,
        ProblemKind::SignatureInvalid,
        ProblemKind::ChallengeInvalid,
        ProblemKind::LicenseKeyInvalid,
        ProblemKind::LicenseKeyRevoked,
        ProblemKind::LicenseNotActive,
        ProblemKind::KindNotAllowed,
        ProblemKind::QuotaExhausted,
        ProblemKind::ProtocolUnsupported,
        ProblemKind::ActivationUnknown,
        ProblemKind::BindingMismatch,
        ProblemKind::GenerationMismatch,
        ProblemKind::RequestConflict,
        ProblemKind::ProofInvalid,
        ProblemKind::AlreadyActivated {
            activation_id: id(u128::from(value)),
        },
        ProblemKind::RateLimited {
            retry_after_seconds: value,
        },
        ProblemKind::Unavailable {
            retry_after_seconds: value,
        },
        ProblemKind::Internal {
            retry_after_seconds: Some(value),
        },
        ProblemKind::Internal {
            retry_after_seconds: None,
        },
    ]
}

#[test]
fn bolero_protocol_json_roundtrip() {
    let fixture = Fixture::new();
    bolero::check!()
        .with_iterations(128)
        .with_max_len(256)
        .for_each(|input| {
            let value = number(input);
            let text = text(input);
            let mut decision = fixture.decision();
            decision.decision_revision = value;
            decision.license.plan.plan_code = text.clone();
            decision.license.assignment_revision = value;
            decision.conditions.usage.quotas.insert(
                CLUSTER_NODES.to_owned(),
                ConditionValue::Known(Quota::Bounded { value }),
            );
            decision.conditions.usage.quotas.insert(
                "future.quota".to_owned(),
                ConditionValue::Extension(
                    serde_json::json!({"tag":text,"nested":[null, -0.0, value]}),
                ),
            );
            for status in [
                DecisionStatus::Active,
                DecisionStatus::TermEnded,
                DecisionStatus::Suspended,
                DecisionStatus::Revoked,
                DecisionStatus::Released,
                DecisionStatus::Transferred,
                DecisionStatus::Expired,
            ] {
                let mut current = decision.clone();
                current.status = status;
                if value.is_multiple_of(2) {
                    current.timing.valid_until = Some(at(180));
                    current.conditions.operating.durations.insert(
                        OFFLINE_ALLOWANCE.to_owned(),
                        ConditionValue::Known(ConditionDuration::Bounded { seconds: 180 }),
                    );
                }
                match status {
                    DecisionStatus::Active => {}
                    DecisionStatus::Expired => {
                        current.activation.kind = ActivationKind::Ephemeral;
                        current.activation.ephemeral_expires_at = Some(now());
                        current.timing.ephemeral_expires_at = Some(now());
                        current.conditions.usage = empty_conditions().usage;
                    }
                    DecisionStatus::TermEnded => {
                        current.license.term_ends_at = now();
                        current.timing.term_ends_at = now();
                        current.conditions.usage = FREE_POLICY.usage();
                    }
                    _ => current.conditions.usage = FREE_POLICY.usage(),
                }
                signed_round_trip(&fixture.working, &current);
            }
            let mut authorization = fixture.authorization();
            authorization.serial = value;
            signed_round_trip(&fixture.roots[0], &authorization);
            round_trip(&fixture.root_sign(&authorization));
            let revocation = WorkingKeyRevocation {
                doc: RevocationKind::WorkingKeyRevocation,
                protocol: 1,
                serial: value,
                issued_at: now(),
                root_epoch: 0,
                kid: fixture.working.key_id().clone(),
                revoked_at: now(),
                reason: text.clone(),
            };
            signed_round_trip(&fixture.roots[0], &revocation);
            let succession = RootSuccession {
                doc: SuccessionKind::RootSuccession,
                protocol: 1,
                serial: value,
                issued_at: now(),
                from_epoch: 0,
                to_epoch: 1,
                roots: fixture.embedded().epochs[0].roots.clone(),
                threshold: std::num::NonZeroUsize::new(2).assured("fixture threshold"),
                effective_at: now(),
            };
            signed_round_trip(&fixture.roots[0], &succession);
            for mut request in fixture.requests() {
                let common = request.common_mut();
                common.display_name = text.clone();
                common.key_generation = value.max(1);
                common.trust_held.revocation_serials = vec![value];
                signed_round_trip(&fixture.cluster, &request);
            }
            let ticket = StreamTicket {
                common: fixture.common(),
                activation_id: id(u128::from(value)),
                state: fixture.committed(),
            };
            signed_round_trip(&fixture.cluster, &ticket);
            let proof = KeyRotationProof {
                challenge: Secret::new(text.clone()),
                cluster_uid: id(u128::from(value)),
                activation_id: id(1),
                new_kid: fixture.working.key_id().clone(),
            };
            signed_round_trip(&fixture.working, &proof);
            for trigger in [
                TransitionTrigger::Decision {
                    decision_id: id(u128::from(value)),
                },
                TransitionTrigger::Tick {
                    decision_id: id(u128::from(value)),
                },
                TransitionTrigger::Release {
                    request_id: id(u128::from(value)),
                },
            ] {
                let transition = fixture_transition(trigger);
                round_trip(&transition);
                let mut request = fixture.requests().remove(2);
                let Request::Refresh { transitions, .. } = &mut request else {
                    panic!("fixture's third operation is committed refresh");
                };
                transitions.push(transition);
                signed_round_trip(&fixture.cluster, &request);
            }
            for reason in [
                TransitionReason::PlanChanged,
                TransitionReason::TermEnded,
                TransitionReason::Suspended,
                TransitionReason::Revoked,
                TransitionReason::Released,
                TransitionReason::Transferred,
                TransitionReason::OfflineLapse,
            ] {
                round_trip(&reason);
            }
            round_trip(&DecisionResponse {
                decision: fixture.sign(&decision),
                trust: fixture.documents(),
                server_time: now(),
            });
            let single_root = fixture.roots[2]
                .sign(&authorization, limits())
                .assured("bounded current root document");
            round_trip(&TrustBundleResponse {
                epochs: fixture.embedded().epochs,
                successions: vec![SignedTrustDocument::Signatures(
                    fixture.root_sign(&succession),
                )],
                authorizations: vec![
                    SignedTrustDocument::Signatures(fixture.root_sign(&authorization)),
                    SignedTrustDocument::Compact(single_root),
                ],
                revocations: vec![SignedTrustDocument::Signatures(
                    fixture.root_sign(&revocation),
                )],
            });
            round_trip(&fixture.embedded());
            round_trip(&ChallengeRequest {
                cluster_uid: id(u128::from(value)),
            });
            round_trip(&ChallengeResponse {
                challenge: Secret::new(text.clone()),
                expires_at: now(),
            });
            round_trip(&plan());
            for capability in [Capability::Enabled, Capability::Disabled] {
                round_trip(&capability);
            }
            for quota in [Quota::Unlimited, Quota::Bounded { value }] {
                round_trip(&quota);
            }
            for duration in [
                ConditionDuration::Unbounded,
                ConditionDuration::Bounded { seconds: value },
            ] {
                round_trip(&duration);
            }
            for kind in problem_kinds(value) {
                round_trip(&ProblemDetails {
                    title: text.clone(),
                    kind,
                    detail: text.clone(),
                    request_id: Some(id(u128::from(value))),
                });
            }
        });
}

#[test]
fn problem_catalog_refuses_contradictory_metadata_and_required_data() {
    for kind in problem_kinds(7) {
        let problem = ProblemDetails {
            title: kind.code().title().to_owned(),
            kind,
            detail: "fixture detail".to_owned(),
            request_id: None,
        };
        let mut value = serde_json::to_value(&problem).assured("problem encodes");
        value["status"] = serde_json::json!(200);
        assert!(serde_json::from_value::<ProblemDetails>(value).is_err());
    }
    let problem = ProblemDetails {
        title: "Cluster already activated".to_owned(),
        kind: ProblemKind::AlreadyActivated {
            activation_id: id(1),
        },
        detail: String::new(),
        request_id: None,
    };
    let mut value = serde_json::to_value(&problem).assured("problem encodes");
    value
        .as_object_mut()
        .assured("problem object")
        .remove("activation_id");
    assert!(serde_json::from_value::<ProblemDetails>(value).is_err());
}
