//! Bounded typed entity metadata at the session boundary.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Current metadata tables and policy-bound semantic encoding and decoding.
//! - **Depends on.** Vocabulary identities, exact label constructors and session bounds.
//! - **Must not know.** Registry state, Plexus display text, transactions or execution tasks.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns the entity metadata wire boundary")
)]

use error_stack::{Report, ResultExt as _};
use flatbuffers::WIPOffset;
use meticulous::OptionExt as _;
use nervix_models::{
    DomainNodeRef, EntityLabelLimits, EntityLabelPolicy, EntityLabelView, EntityLabels,
    EntityLabelsInspection, LabelAssignment, LabelKey,
};

use crate::{
    codec::{Decoder, EncodedUnion, Encoder, WireDecodeError, WireEncodeError},
    common::{decode_node_ref, encode_node_ref},
    transaction::{decode_preview_identity, encode_preview_identity},
    wire,
};

pub(crate) fn encode_entity_labels<'fbb>(
    value: &EntityLabelsInspection,
    encoder: &mut Encoder<'fbb>,
) -> Result<WIPOffset<wire::EntityLabelsInspection<'fbb>>, Report<WireEncodeError>> {
    encoder.entries("EntityLabelsInspection.labels", value.labels().len())?;
    let domain = encoder.text(
        "EntityLabelsInspection.domain",
        value.entity().domain.as_str(),
    )?;
    let entity = encode_node_ref(encoder, &value.entity().node)?;
    let mut entries = Vec::with_capacity(value.labels().len());
    for (key, value) in value.labels().iter() {
        let key = encoder.text("LabelEntry.key", key.as_str())?;
        let value = encoder.text("LabelEntry.value", value)?;
        entries.push(wire::LabelEntry::create(
            encoder.fbb(),
            &wire::LabelEntryArgs {
                key: Some(key),
                value: Some(value),
            },
        ));
        encoder.within_limit("EntityLabelsInspection.labels")?;
    }
    let labels = encoder.tables("EntityLabelsInspection.labels", &entries)?;
    let limits = wire::EntityLabelLimits::create(
        encoder.fbb(),
        &wire::EntityLabelLimitsArgs {
            count: value.policy().limits.count,
            key_bytes: value.policy().limits.key_bytes,
            value_bytes: value.policy().limits.value_bytes,
            bytes: value.policy().limits.bytes,
        },
    );
    let policy = wire::EntityLabelPolicy::create(
        encoder.fbb(),
        &wire::EntityLabelPolicyArgs {
            revision: value.policy().revision.get(),
            limits: Some(limits),
        },
    );
    let view = match value.view() {
        EntityLabelView::Committed { revision } => EncodedUnion::new(
            wire::EntityLabelView::CommittedEntityLabels,
            wire::CommittedEntityLabels::create(
                encoder.fbb(),
                &wire::CommittedEntityLabelsArgs {
                    revision: *revision,
                },
            ),
        ),
        EntityLabelView::Staged { preview } => {
            let preview = encode_preview_identity(encoder, preview)?;
            EncodedUnion::new(
                wire::EntityLabelView::StagedEntityLabels,
                wire::StagedEntityLabels::create(
                    encoder.fbb(),
                    &wire::StagedEntityLabelsArgs {
                        preview: Some(preview),
                    },
                ),
            )
        }
    };
    Ok(wire::EntityLabelsInspection::create(
        encoder.fbb(),
        &wire::EntityLabelsInspectionArgs {
            domain: Some(domain),
            entity: Some(entity),
            labels: Some(labels),
            policy: Some(policy),
            view_type: view.discriminant,
            view: Some(view.value),
        },
    ))
}

pub(crate) fn decode_entity_labels(
    decoder: Decoder<'_>,
    value: wire::EntityLabelsInspection<'_>,
) -> Result<EntityLabelsInspection, Report<WireDecodeError>> {
    let domain = decoder.name("EntityLabelsInspection.domain", value.domain())?;
    let node = decode_node_ref(decoder, value.entity())?;
    let captured = value.policy();
    let revision = decoder.non_zero("EntityLabelPolicy.revision", captured.revision())?;
    let limits = captured.limits();
    let policy = EntityLabelPolicy {
        revision,
        limits: EntityLabelLimits {
            count: limits.count(),
            key_bytes: limits.key_bytes(),
            value_bytes: limits.value_bytes(),
            bytes: limits.bytes(),
        },
    };
    let entries = value.labels();
    decoder.entries("EntityLabelsInspection.labels", entries.len())?;
    let mut assignments = Vec::with_capacity(entries.len());
    let mut previous: Option<&str> = None;
    for entry in entries {
        let key = decoder.check_text("LabelEntry.key", entry.key())?;
        if previous.is_some_and(|previous| previous >= key) {
            return Err(Report::new(WireDecodeError::NonCanonicalSet {
                field: "EntityLabelsInspection.labels",
            }));
        }
        previous = Some(key);
        let key =
            LabelKey::try_from(key.to_owned()).change_context(WireDecodeError::InvalidValue {
                field: "LabelEntry.key",
                kind: "exact ASCII label key",
            })?;
        let value = decoder.text("LabelEntry.value", entry.value())?;
        assignments.push(LabelAssignment { key, value });
    }
    let labels =
        EntityLabels::try_from(assignments).change_context(WireDecodeError::NonCanonicalSet {
            field: "EntityLabelsInspection.labels",
        })?;
    let view = match value.view_type() {
        wire::EntityLabelView::CommittedEntityLabels => {
            let view = value
                .view_as_committed_entity_labels()
                .assured("the verified union discriminant names the committed view");
            EntityLabelView::Committed {
                revision: view.revision(),
            }
        }
        wire::EntityLabelView::StagedEntityLabels => {
            let view = value
                .view_as_staged_entity_labels()
                .assured("the verified union discriminant names the staged view");
            EntityLabelView::Staged {
                preview: decode_preview_identity(decoder, view.preview())?,
            }
        }
        unknown => return Err(decoder.unknown_union("EntityLabelsInspection.view", unknown.0)),
    };
    EntityLabelsInspection::new(DomainNodeRef::new(domain, node), labels, view, policy)
        .change_context(WireDecodeError::InvalidValue {
            field: "EntityLabelsInspection.labels",
            kind: "labels within captured committed policy",
        })
}

#[cfg(test)]
pub(crate) mod tests;
