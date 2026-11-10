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
    pub(in crate::application) async fn describe_license(&self) -> CommandResult {
        let origin = self.inner.cluster.local_node_identity().await;
        let policy = self.inner.licensing.current();
        let inspected = match (&policy.snapshot.initialized, policy.snapshot.revision) {
            (Some(initialized), Some(revision)) => {
                let identity = &initialized.identity;
                let uid = identity.cluster_uid.to_string();
                let mode = match initialized.record.mode {
                    nervix_consensus::LicensingMode::Free => "free",
                };
                let processing = match policy.processing_stop() {
                    None => "running",
                    Some(reason) => match reason {},
                };
                let columns: Vec<(&str, ArrayRef)> = vec![
                    ("mode", StdArc::new(StringArray::from(vec![mode]))),
                    (
                        "policy",
                        StdArc::new(StringArray::from(vec![policy.state_name()])),
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
                        StdArc::new(UInt64Array::from(vec![0])),
                    ),
                ];
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
                            }],
                            continuation: None,
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
