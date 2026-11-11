//! The public licensing observation from one immutable policy cut.
//!
//! Layer: edges.
//! - **Owns.** Projecting licensing facts into the shared inspection envelope and Row sections.
//! - **Depends on.** The policy handle, Arrow columns, the shared Row encoder and inspection vocabulary.
//! - **Must not know.** Policy evaluation, quota admission, storage or service requests.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns public licensing inspection")
)]

use arrow_array::{ArrayRef, RecordBatch, StringArray, TimestampNanosecondArray, UInt64Array};
use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{
    RowSchema, SessionLimits, StateInspection, StateInspectionEntity, StateInspectionPage,
    StateInspectionResult, StateInspectionSection, StateInspectionUnit, StateRowsEncoder,
    WireEncodeError,
};
use nervix_crypto::digest::Sha256;
use nervix_models::{
    InspectionFormat, SchemaField, StateInspectionCounts, StateInspectionOrigin,
    StateInspectionRejection, StateInspectionStage, Unit,
};
use nervix_primitives::sync::StdArc;

use super::{
    command_result::{CommandResult, RenderedStateInspection},
    model_mutation::{command_error, command_ok},
    session_service::SessionServiceImpl,
};
use crate::{
    client_row::ArrowRowBatch,
    licensing::{Quota, QuotaName},
    runtime_schema::{batch_payload_bytes, parse_as_type_from_arrow},
};

impl SessionServiceImpl {
    fn append_activated_license(
        &self,
        columns: &mut Vec<(&'static str, ArrayRef)>,
        initialized: &nervix_consensus::ClusterLicensingState,
        policy: &crate::licensing::LicensingPolicy,
        observation: nervix_models::LicensingConnectivity,
        reported: &str,
    ) {
        let activation = match &initialized.record.mode {
            nervix_consensus::LicensingMode::Free { .. } => return,
            nervix_consensus::LicensingMode::Activated(activation) => activation.as_ref(),
            nervix_consensus::LicensingMode::Released(release) => {
                columns.push((
                    "release",
                    StdArc::new(StringArray::from(vec![if release.awaiting() {
                        "awaiting service confirmation"
                    } else {
                        "confirmed"
                    }])),
                ));
                columns.push((
                    "released_at",
                    StdArc::new(TimestampNanosecondArray::from(vec![
                        release.released_at.unix_nanos(),
                    ])),
                ));
                if let Some(until) = release.grants_until {
                    columns.push((
                        "grants_until",
                        StdArc::new(TimestampNanosecondArray::from(vec![until.unix_nanos()])),
                    ));
                }
                &release.activation
            }
        };

        let environment = self.inner.licensing_test_support.environment(
            self.inner
                .runtime
                .executor()
                .limits()
                .management_event_bytes
                .as_u64(),
        );
        let verification = match &initialized.verification {
            Some(Ok(verified)) if verified.signing_key_revoked => "verified (signing key revoked)",
            Some(Ok(_)) => "verified",
            Some(Err(_)) | None => "unverifiable",
        };
        let kind = match activation.binding.kind {
            nervix_models::ActivationKind::Persistent => "persistent",
            nervix_models::ActivationKind::Ephemeral => "ephemeral",
        };
        let binding = &activation.binding;
        let mut activation_text = format!(
            "{} {kind} created {}",
            binding.activation_id, binding.created_at
        );
        if let Some(expiry) = binding.ephemeral_expires_at {
            columns.push((
                "ephemeral_expires_at",
                StdArc::new(TimestampNanosecondArray::from(vec![expiry.unix_nanos()])),
            ));
            activation_text.push_str(&format!(" expires {expiry}"));
        }
        let mut facts = vec![
            ("service", environment.service().to_string()),
            ("activation", activation_text),
            ("license_key", binding.license_key_id.clone()),
            (
                "decision",
                format!(
                    "revision {} conditions {} issued {} received {} {verification}",
                    activation.decision.revision,
                    activation.decision.conditions_revision,
                    activation.decision.issued_at,
                    activation.decision.received_at
                ),
            ),
            ("verification", verification.to_owned()),
        ];
        if let Some(Ok(verified)) = &initialized.verification {
            let decision = &verified.decision;
            for (name, at) in [
                ("refresh_after", Some(decision.timing.refresh_after)),
                ("overdue_at", Some(decision.timing.overdue_at)),
                ("valid_until", decision.timing.valid_until),
            ] {
                if let Some(at) = at {
                    columns.push((
                        name,
                        StdArc::new(TimestampNanosecondArray::from(vec![
                            nervix_models::Timestamp::try_from(at)
                                .assured("verified timing fits Timestamp")
                                .unix_nanos(),
                        ])),
                    ));
                }
            }

            facts.push((
                "license",
                format!(
                    "{} organization {} plan {} v{} term {} .. {}",
                    decision.license.license_id,
                    decision.license.organization_id,
                    decision.license.plan.plan_code,
                    decision.license.plan.version,
                    decision.license.term_starts_at,
                    decision.license.term_ends_at
                ),
            ));
            facts.push((
                "status",
                serde_json::to_value(decision.status)
                    .assured("known status serializes")
                    .as_str()
                    .verified("status is a string")
                    .to_owned(),
            ));
            facts.push((
                "refresh",
                format!(
                    "{} (after {}, overdue {}, valid until {})",
                    policy.display_state(),
                    decision.timing.refresh_after,
                    decision.timing.overdue_at,
                    match decision.timing.valid_until {
                        Some(until) => until.to_string(),
                        None => "unbounded".to_owned(),
                    }
                ),
            ));
        } else {
            facts.push(("warning", "decision_unverifiable".to_owned()));
            if let Some(Err(reason)) = &initialized.verification {
                facts.push(("rejection", reason.to_string()));
            }
        }
        facts.push((
            "connectivity",
            format!(
                "{} (last success {}, last failure {}){reported}",
                match observation.state {
                    nervix_models::LicensingConnectionState::Unknown => "unknown".to_owned(),
                    nervix_models::LicensingConnectionState::Reachable => "reachable".to_owned(),
                    nervix_models::LicensingConnectionState::Unreachable {
                        since,
                        last_failure,
                    } => {
                        format!("unreachable since {since} ({last_failure:?})")
                    }
                    nervix_models::LicensingConnectionState::Refusing {
                        since,
                        last_refusal,
                    } => {
                        format!("refusing {} since {since}", last_refusal.as_str())
                    }
                },
                match observation.last_success {
                    Some(at) => at.to_string(),
                    None => "unknown".to_owned(),
                },
                match observation.last_failure {
                    Some(at) => at.to_string(),
                    None => "unknown".to_owned(),
                },
            ),
        ));
        for (name, value) in facts {
            columns.push((name, StdArc::new(StringArray::from(vec![value]))));
        }
        columns.push((
            "decision_revision",
            StdArc::new(UInt64Array::from(vec![activation.decision.revision])),
        ));
        columns.push((
            "conditions_revision",
            StdArc::new(UInt64Array::from(vec![
                activation.decision.conditions_revision,
            ])),
        ));
    }

    pub(in crate::application) async fn describe_license(&self) -> CommandResult {
        let origin = self.inner.cluster.local_node_identity().await;
        let leader = self.inner.consensus.current_leader_tenure();
        let connectivity = match &leader {
            Some(leader) if leader.leader_id() != self.inner.consensus.local_node_id() => {
                self.inner
                    .cluster
                    .licensing_connectivity(leader.leader_id(), leader.term())
                    .await
            }
            _ => None,
        };
        let local_leader = leader
            .as_ref()
            .is_some_and(|tenure| tenure.leader_id() == self.inner.consensus.local_node_id());
        let connectivity =
            connectivity.filter(|_| self.inner.consensus.current_leader_tenure() == leader);
        let observation = match &connectivity {
            Some(publication) => publication.observation,
            None if local_leader && self.inner.consensus.current_leader_tenure() == leader => {
                self.inner.licensing.connectivity()
            }
            None => nervix_models::LicensingConnectivity::default(),
        };
        let reported = match connectivity.as_ref() {
            Some(publication) => format!(" (reported by {})", publication.leader.node_id()),
            None => String::new(),
        };
        let policy = self.inner.licensing.current();
        let inspected = match (&policy.snapshot.initialized, policy.snapshot.revision) {
            (Some(initialized), Some(revision)) => {
                let identity = &initialized.identity;
                let uid = identity.cluster_uid.to_string();
                let mode = match &initialized.record.mode {
                    nervix_consensus::LicensingMode::Free { .. } => "free",
                    nervix_consensus::LicensingMode::Activated(_) => "activated",
                    nervix_consensus::LicensingMode::Released(_) => "released",
                };
                let processing = match policy.processing_stop() {
                    None => "running",
                    Some(reason) => match reason {},
                };
                let conditions_not_understood = match &initialized.verification {
                    Some(Ok(verified)) => u64::try_from(verified.conditions_not_understood)
                        .assured("bounded condition count fits u64"),
                    Some(Err(_)) | None => 0,
                };
                let mut columns: Vec<(&str, ArrayRef)> = vec![
                    ("mode", StdArc::new(StringArray::from(vec![mode]))),
                    (
                        "policy",
                        StdArc::new(StringArray::from(vec![policy.display_state()])),
                    ),
                    (
                        "cluster_uid",
                        StdArc::new(StringArray::from(vec![uid.as_str()])),
                    ),
                    (
                        "cluster_key",
                        StdArc::new(StringArray::from(vec![identity.kid.as_ref()])),
                    ),
                    (
                        "cluster_key_generation",
                        StdArc::new(UInt64Array::from(vec![identity.key_generation.get()])),
                    ),
                    (
                        "licensing_time",
                        StdArc::new(TimestampNanosecondArray::from(vec![
                            initialized.record.licensing_time.unix_nanos(),
                        ])),
                    ),
                    (
                        "processing",
                        StdArc::new(StringArray::from(vec![processing])),
                    ),
                    (
                        "conditions_not_understood",
                        StdArc::new(UInt64Array::from(vec![conditions_not_understood])),
                    ),
                ];
                self.append_activated_license(
                    &mut columns,
                    initialized,
                    &policy,
                    observation,
                    &reported,
                );
                let summary = RecordBatch::try_from_iter(columns)
                    .assured("fixed licensing columns have one value each");
                let quotas: Vec<_> = QuotaName::ALL
                    .into_iter()
                    .map(|name| policy.quota(name))
                    .collect();
                let columns: Vec<(&str, ArrayRef)> = vec![
                    (
                        "condition",
                        StdArc::new(StringArray::from(
                            QuotaName::ALL.map(QuotaName::as_str).to_vec(),
                        )),
                    ),
                    (
                        "usage",
                        StdArc::new(UInt64Array::from(vec![
                            policy.snapshot.usage.cluster_nodes,
                            policy.snapshot.usage.cluster_domains,
                        ])),
                    ),
                    (
                        "limit_kind",
                        StdArc::new(StringArray::from(
                            quotas
                                .iter()
                                .map(|quota| match quota {
                                    Quota::Unlimited => "unlimited",
                                    Quota::Bounded { .. } => "bounded",
                                })
                                .collect::<Vec<_>>(),
                        )),
                    ),
                    (
                        "limit",
                        StdArc::new(UInt64Array::from(
                            quotas
                                .iter()
                                .map(|quota| match quota {
                                    Quota::Unlimited => None,
                                    Quota::Bounded { value } => Some(*value),
                                })
                                .collect::<Vec<_>>(),
                        )),
                    ),
                ];
                let quotas = RecordBatch::try_from_iter(columns)
                    .assured("fixed quota columns have two values each");
                match (section("summary", &summary), section("quotas", &quotas)) {
                    (Ok(summary), Ok(quotas)) => {
                        let counts = StateInspectionCounts {
                            branches: 0,
                            rows: 3,
                            bytes: summary
                                .1
                                .checked_add(quotas.1)
                                .verified("three fixed licensing rows fit the payload count"),
                        };
                        let sections = vec![summary.0, quotas.0];
                        let mut fingerprint = Sha256::new();
                        fingerprint.update(&revision.to_le_bytes());
                        for section in &sections {
                            fingerprint.update(
                                &u64::try_from(section.name.len())
                                    .assured("fixed section names fit u64")
                                    .to_le_bytes(),
                            );
                            fingerprint.update(section.name.as_bytes());
                            let schema = serde_json::to_vec(&section.schema.fields)
                                .assured("fixed schema fields serialize");
                            fingerprint.update(
                                &u64::try_from(schema.len())
                                    .assured("fixed schema fits u64")
                                    .to_le_bytes(),
                            );
                            fingerprint.update(&schema);
                            for frame in &section.frames {
                                fingerprint.update(
                                    &u64::try_from(frame.bytes().len())
                                        .assured("bounded frames fit u64")
                                        .to_le_bytes(),
                                );
                                fingerprint.update(frame.bytes());
                            }
                        }
                        StateInspectionResult::Page(StateInspectionPage {
                            counts,
                            unit_count: 1,
                            units: vec![StateInspectionUnit {
                                unit: Unit::SINGLE,
                                owner: origin.node_id().clone(),
                                replicas: Vec::new(),
                                origin: StateInspectionOrigin {
                                    node: origin,
                                    fingerprint: fingerprint.finish(),
                                    generation: revision,
                                    revision,
                                    stage: StateInspectionStage::Stored,
                                },
                                counts,
                                sections,
                                wasm: None,
                            }],
                            continuation: None,
                            wasm_binding: None,
                        })
                    }
                    (Err(error), _) | (_, Err(error)) => {
                        tracing::debug!(error = %format_args!("{error:#}"), "licensing inspection encoding refused");
                        StateInspectionResult::Rejected(StateInspectionRejection::ReplyTooLarge)
                    }
                }
            }
            _ => StateInspectionResult::Rejected(StateInspectionRejection::NotReady),
        };
        let state = StateInspection {
            entity: StateInspectionEntity::ClusterLicense,
            result: inspected,
        };
        let text = match state.display_text() {
            Ok(text) => text,
            Err(error) => return command_error(format!("{error:#}")),
        };
        let mut result = if matches!(state.result, StateInspectionResult::Page(_)) {
            command_ok(text)
        } else {
            command_error(text)
        };
        result.state = Some(Box::new(RenderedStateInspection {
            inspection: state,
            format: InspectionFormat::Text,
        }));
        result
    }
}

fn section(
    name: &str,
    batch: &RecordBatch,
) -> Result<(StateInspectionSection, u64), Report<WireEncodeError>> {
    let schema = RowSchema {
        fields: batch
            .schema()
            .fields()
            .iter()
            .map(|field| SchemaField {
                name: field
                    .name()
                    .parse()
                    .assured("fixed licensing field names follow the name rule"),
                ty: parse_as_type_from_arrow(field.data_type())
                    .assured("licensing uses shared scalar types"),
                optional: field.is_nullable(),
                sensitive: false,
            })
            .collect(),
        branch: None,
    };
    let columns = ArrowRowBatch::new(&schema, batch)
        .assured("the Row schema derives from these exact Arrow columns");
    let limits = SessionLimits::DEFAULT;
    let mut encoder = StateRowsEncoder::unbranched(&limits);
    for index in 0..batch.num_rows() {
        encoder.push_row(|writer| columns.write_row(index, writer))?;
    }
    Ok((
        StateInspectionSection {
            name: name.to_string(),
            schema,
            frames: vec![encoder.finish(&limits)?],
            omitted_rows: 0,
            watermarks: None,
        },
        batch_payload_bytes(batch),
    ))
}
