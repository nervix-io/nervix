//! Complete current licensing records through their production archive and singleton owners.
//!
//! Layer: test harness.
//! - **Owns.** Bounded record round trips and atomic first-install observations.
//! - **Depends on.** Current licensing records, vocabulary generators and production codecs.
//! - **Must not know.** Policy publication, sessions, clocks or network scheduling.

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_arbitrary::{Arbitrary, Domain};

use super::*;
use crate::{StateMachineData, cluster_identity, durable_batch::DurableBatch};

#[test]
fn committed_domain_admission_serializes_competing_creates_and_preserves_updates() {
    use nervix_licensing_vocabulary::{LicensingRefusal, Quota, QuotaName};

    use crate::{
        ConsensusCommand, ConsensusConflict, ConsensusResponse, Proposer, apply_consensus_command,
    };

    let quota = Quota::Bounded { value: 1 };
    let command = |name: &str| ConsensusCommand::PutDomain {
        domain: Box::new(crate::tests::running_domain_state(name)),
        mutation: None,
        domain_quota: quota,
    };
    let mut state = StateMachineData::default();
    // Both planners saw zero domains. Only the first durable effect may consume the last slot.
    assert!(super::admit_domain(&state, quota, false).is_ok());
    assert!(super::admit_domain(&state, quota, false).is_ok());
    assert_eq!(
        apply_consensus_command(&mut state, &command("first")).response,
        ConsensusResponse::Applied
    );
    let result = apply_consensus_command(&mut state, &command("second"));
    let refusal = LicensingRefusal {
        state: nervix_licensing_vocabulary::AdmissionState::OverLimit,
        condition: QuotaName::ClusterDomains,
        limit: 1,
        current_usage: 1,
        requested: 1,
    };
    assert_eq!(
        result.response,
        ConsensusResponse::Conflict(ConsensusConflict::LicensingQuota { refusal })
    );
    assert!(!result.domains_changed);
    assert_eq!(state.domains.len(), 1);
    let report = Proposer::applied_response(result.response).unwrap_err();
    assert_eq!(report.downcast_ref::<LicensingRefusal>(), Some(&refusal));
    let rendered = format!("{report:#}");
    assert_eq!(
        rendered
            .matches("licensing quota 'cluster.domains'")
            .count(),
        1
    );
    assert_eq!(crate::ConsensusError::report_message(&report), rendered);
    // A lifecycle update does not occupy another slot, even at the limit.
    assert_eq!(
        apply_consensus_command(&mut state, &command("first")).response,
        ConsensusResponse::Applied
    );
    assert_eq!(state.domains.len(), 1);
}

#[test]
fn identity_installation_installs_and_retains_one_free_record() {
    let first = cluster_identity::tests::candidate(0x11);
    let mut state = StateMachineData::default();
    cluster_identity::install(&mut state, &first);
    let record = state
        .licensing
        .clone()
        .verified("identity installation includes licensing");
    assert_eq!(
        record.as_ref(),
        &LicensingRecord::free(first.public().created_at)
    );
    cluster_identity::install(&mut state, &cluster_identity::tests::candidate(0x22));
    assert!(Arc::ptr_eq(
        &record,
        state
            .licensing
            .as_ref()
            .verified("the record remains installed")
    ));
    assert_eq!(state.runtime_revision, 0);
}

#[test]
fn bolero_licensing_record_preserves_time_mode_and_complete_trust_documents() {
    bolero::check!()
        .with_iterations(128)
        .with_max_len(512)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let mut record = LicensingRecord::free(arbitrary.timestamp());
            if arbitrary.entropy().flag() {
                record.mode = LicensingMode::Free {
                    last_release: Some(uuid::Uuid::from_bytes(std::array::from_fn(|_| {
                        arbitrary.entropy().byte()
                    }))),
                };
            }
            for _ in 0..arbitrary.entropy().count(3) {
                let typ = match arbitrary.entropy().byte() % 3 {
                    0 => DocumentType::WorkingKey,
                    1 => DocumentType::Revocation,
                    _ => DocumentType::Succession,
                };
                record.trust.push(StoredTrustDocument {
                    bytes: arbitrary.non_empty_string().into_bytes(),
                    typ,
                    serial: arbitrary.entropy().any_u64(),
                    kid: if typ == DocumentType::Succession {
                        None
                    } else {
                        Some(
                            cluster_identity::tests::candidate(arbitrary.entropy().byte())
                                .public()
                                .kid,
                        )
                    },
                });
            }
            if arbitrary.entropy().flag() {
                let binding = activation_binding(&mut arbitrary);
                record.mode = LicensingMode::Activated(Box::new(ActivationState {
                    binding,
                    transitions: generated_committed_transitions(&mut arbitrary),
                    decision: StoredDecision {
                        bytes: arbitrary.non_empty_string(),
                        revision: arbitrary.entropy().between(1..=u64::MAX),
                        conditions_revision: arbitrary.entropy().between(1..=u64::MAX),
                        transition_conditions_revisions: (0..arbitrary.entropy().count(3))
                            .map(|_| arbitrary.entropy().between(1..=u64::MAX))
                            .collect::<std::collections::BTreeSet<_>>()
                            .into_iter()
                            .collect(),
                        operation: if arbitrary.entropy().flag() {
                            StoredDecisionOperation::Apply
                        } else {
                            StoredDecisionOperation::ClusterKeyRotation
                        },
                        issued_at: arbitrary.timestamp(),
                        received_at: arbitrary.timestamp(),
                        request_id: uuid::Uuid::from_bytes(std::array::from_fn(|_| {
                            arbitrary.entropy().byte()
                        })),
                    },
                }));
            }
            if let LicensingMode::Activated(activation) = &record.mode {
                record.licensing_time = record
                    .licensing_time
                    .max(activation.binding.created_at)
                    .max(activation.decision.issued_at)
                    .max(activation.decision.received_at);
            }
            if let LicensingMode::Activated(activation) = &record.mode
                && arbitrary.entropy().flag()
            {
                let identity = cluster_identity::tests::candidate(0x11);
                record.licensing_time = record.licensing_time.max(identity.public().created_at);
                let request_id =
                    uuid::Uuid::from_bytes(std::array::from_fn(|_| arbitrary.entropy().byte()));
                let values = generated_transitions(&mut arbitrary);
                let grants_until = values
                    .iter()
                    .map(|value| {
                        Timestamp::try_from(value.deadline).assured("generated deadline fits")
                    })
                    .max();
                let transitions = StoredLicenseTransitions::new(
                    &values,
                    nervix_licensing_protocol::test_fixture::limits(),
                )
                .assured("bounded transitions fit");
                assert_eq!(transitions.values(), values.as_slice());
                record.mode = LicensingMode::Released(Box::new(ReleaseState {
                    reference: crate::cluster_identity::tests::reference(request_id),
                    activation: activation.as_ref().clone(),
                    identity: identity.public(),
                    request_id,
                    released_at: record.licensing_time,
                    grants_until,
                    transitions,
                    confirmation: if arbitrary.entropy().flag() {
                        ReleaseConfirmation::Confirmed
                    } else {
                        ReleaseConfirmation::Awaiting(Box::new(AwaitingLicenseRelease {
                            key: identity.retain_release_key(),
                            prepared: if arbitrary.entropy().flag() {
                                Some(prepared_request(&mut arbitrary))
                            } else {
                                None
                            },
                        }))
                    },
                }));
            }
            let encoded = DurableBatch::encode(&record, 64 * 1024)
                .assured("three bounded opaque trust documents fit the record budget");
            let restored: LicensingRecord = crate::storage_decode(&encoded)
                .assured("the current complete licensing record decodes");
            assert_eq!(restored, record);
            let json = serde_json::to_vec(&record).assured("the current record serializes");
            let restored: LicensingRecord =
                serde_json::from_slice(&json).assured("the complete JSON record decodes");
            assert_eq!(restored, record);
        });
}

fn generated_committed_transitions(arbitrary: &mut Arbitrary<'_>) -> CommittedLicenseTransitions {
    let mut committed = CommittedLicenseTransitions::default();
    for transition in generated_transitions(arbitrary) {
        if arbitrary.entropy().flag() {
            committed = committed
                .acknowledge(
                    &[transition],
                    nervix_licensing_protocol::test_fixture::limits(),
                )
                .assured("bounded generated acknowledgement");
        } else {
            let until = if arbitrary.entropy().flag() {
                Some(arbitrary.timestamp())
            } else {
                None
            };
            committed.insert_pending(transition, until);
        }
    }
    let nodes = (0..arbitrary.entropy().count(3))
        .map(|_| arbitrary.rule_name())
        .collect();
    committed.protect_nodes(&nodes);
    committed
}

pub(crate) fn generated_transitions(
    arbitrary: &mut Arbitrary<'_>,
) -> Vec<nervix_licensing_protocol::documents::Transition> {
    use nervix_licensing_protocol::{
        conditions::{CLUSTER_DOMAINS, Quota},
        documents::*,
    };
    let mut transitions = Vec::new();
    for index in 0..arbitrary.entropy().count(3) {
        let started_at = arbitrary
            .timestamp()
            .min(Timestamp::from_unix_nanos(i64::MAX - 1));
        let deadline = started_at
            .checked_add(std::time::Duration::from_nanos(1))
            .assured("generator leaves one nanosecond");
        let request_id =
            uuid::Uuid::from_bytes(std::array::from_fn(|_| arbitrary.entropy().byte()));
        let mut identity = *request_id.as_bytes();
        identity[15] = u8::try_from(index).assured("three transitions fit u8");
        transitions.push(Transition {
            transition_id: uuid::Uuid::from_bytes(identity),
            from_conditions_revision: arbitrary.entropy().between(1..=u64::MAX),
            to_conditions_revision: arbitrary.entropy().between(1..=u64::MAX),
            started_at: started_at.into_datetime(),
            deadline: deadline.into_datetime(),
            trigger: TransitionTrigger::Release { request_id },
            reason: TransitionReason::Released,
            affected: vec![AffectedQuota {
                condition: CLUSTER_DOMAINS.to_owned(),
                previous: Quota::Unlimited,
                target: Quota::Bounded { value: 0 },
                usage: arbitrary.entropy().between(1..=u64::MAX),
            }],
        });
    }
    transitions
}

pub(crate) fn activation_binding(arbitrary: &mut Arbitrary<'_>) -> ActivationBinding {
    let kind = if arbitrary.entropy().flag() {
        nervix_licensing_vocabulary::ActivationKind::Persistent
    } else {
        nervix_licensing_vocabulary::ActivationKind::Ephemeral
    };
    let latest = Timestamp::from_unix_nanos(i64::MAX - 1);
    let created_at = arbitrary.timestamp().min(latest);
    let ephemeral_expires_at = match kind {
        nervix_licensing_vocabulary::ActivationKind::Persistent => None,
        nervix_licensing_vocabulary::ActivationKind::Ephemeral => Some(
            created_at
                .checked_add(std::time::Duration::from_nanos(1))
                .assured("generator leaves one timestamp nanosecond"),
        ),
    };
    ActivationBinding {
        activation_id: uuid::Uuid::from_bytes(std::array::from_fn(|_| arbitrary.entropy().byte())),
        kind,
        created_at,
        ephemeral_expires_at,
        license_key_id: "TESTABCD".to_owned(),
    }
}

pub(crate) fn received_decision(arbitrary: &mut Arbitrary<'_>) -> ReceivedLicenseDecision {
    ReceivedLicenseDecision {
        bytes: arbitrary.non_empty_string(),
        trust: Vec::new(),
        received_at: arbitrary.timestamp(),
        request_id: uuid::Uuid::from_bytes(std::array::from_fn(|_| arbitrary.entropy().byte())),
    }
}

pub(crate) fn prepared_request(arbitrary: &mut Arbitrary<'_>) -> PreparedLicensingRequest {
    PreparedLicensingRequest::new(
        arbitrary.non_empty_string(),
        arbitrary.timestamp(),
        nervix_licensing_protocol::test_fixture::limits(),
    )
    .assured("the generated request fits the document envelope")
}
