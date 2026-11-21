//! Complete metadata views, captured policies and malformed current wire tables.
//!
//! Layer: test harness.
//! - **Owns.** Whole-value metadata round trips and current decoder refusal oracles.
//! - **Depends on.** Production codecs, current tables and bounded metadata generation.
//! - **Must not know.** Registry state, persistence or execution tasks.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns metadata wire invariants")
)]

use std::num::NonZeroU64;

use flatbuffers::FlatBufferBuilder;
use meticulous::ResultExt as _;
use nervix_models::{
    ClusterParameterValues, DomainNodeRef, EntityLabelLimits, EntityLabelPolicy, EntityLabelView,
    EntityLabelsInspection, ImpactPlanningBasis, ModelKind, TransactionPosition,
    TransactionPreviewIdentity,
};

use super::*;
use crate::{
    CommandDisposition, Reply, ReplyBody, SessionLimits,
    tests::{
        fixtures::{name, request, round_trip_reply},
        samples::command_outcome,
    },
};

fn policy() -> EntityLabelPolicy {
    EntityLabelPolicy {
        revision: NonZeroU64::MIN,
        limits: EntityLabelLimits::from(&ClusterParameterValues::default()),
    }
}

pub(crate) fn inspection(staged: bool, revision: u64) -> EntityLabelsInspection {
    owner_inspection(
        EntityLabelOwner::Model(DomainNodeRef::node_in(
            name("sales"),
            ModelKind::WireJsonSchema,
            name::<nervix_models::WireSchemaName>("payload"),
        )),
        staged,
        revision,
    )
}

pub(crate) fn owner_inspection(
    owner: EntityLabelOwner,
    staged: bool,
    revision: u64,
) -> EntityLabelsInspection {
    let labels =
        serde_json::from_str(r#"{"empty":"","owner":" Δ ","team/path":"Mixed Case / east"}"#)
            .assured("the exact fixture keys are valid and distinct");
    let view = if staged {
        EntityLabelView::Staged {
            preview: TransactionPreviewIdentity {
                transaction_id: "metadata-transaction".to_owned(),
                position: TransactionPosition::new(1),
                planning_basis: ImpactPlanningBasis::new([37; 32]),
            },
        }
    } else {
        EntityLabelView::Committed { revision }
    };
    EntityLabelsInspection::new(owner, labels, view, policy())
        .assured("the complete map fits its captured policy")
}

fn round_trip(value: EntityLabelsInspection, message: String) -> EntityLabelsInspection {
    let mut original = command_outcome(CommandDisposition::Completed {
        already_existed: false,
    });
    original.entity_labels = Some(Box::new(value));
    original.message = message;
    let decoded = round_trip_reply(&Reply {
        request_id: request(1),
        body: ReplyBody::Command(Box::new(original.clone())),
    });
    assert_eq!(decoded.body, ReplyBody::Command(Box::new(original)));
    let ReplyBody::Command(outcome) = decoded.body else {
        panic!("the reply is a command outcome");
    };
    *outcome
        .entity_labels
        .assured("the result retains its typed metadata view")
}

#[test]
fn exact_empty_and_unicode_values_keep_both_views_and_every_revision_bit() {
    let owners = [
        inspection(false, 0).entity().clone(),
        EntityLabelOwner::Domain {
            name: name("sales"),
        },
        EntityLabelOwner::Resource(ResourceCatalogKey::new(&name("sales"), &name("bundle"))),
        EntityLabelOwner::User {
            name: name("operator"),
        },
    ];
    for owner in owners {
        for staged in [false, true] {
            if staged && !owner.supports_staged_view() {
                continue;
            }
            for revision in [0, u64::MAX] {
                let expected = owner_inspection(owner.clone(), staged, revision);
                assert_eq!(
                    round_trip(expected.clone(), "metadata in TEXT".to_owned()),
                    expected
                );
                let json =
                    serde_json::to_string(&expected).assured("the typed view is serializable");
                assert_eq!(round_trip(expected.clone(), json.clone()), expected);
                assert_eq!(
                    serde_json::from_str::<EntityLabelsInspection>(&json)
                        .assured("the same captured complete value decodes"),
                    expected
                );
            }
        }
    }
}

#[test]
fn serde_views_validate_the_complete_map_against_their_recorded_policy() {
    let original = inspection(false, 0);
    let mut json = serde_json::to_value(&original).assured("the view is a JSON value");
    json["policy"]["limits"]["count"] = serde_json::json!(0);
    assert!(serde_json::from_value::<EntityLabelsInspection>(json).is_err());
}

#[test]
fn administrative_owners_refuse_a_staged_preview() {
    let mut json = serde_json::to_value(inspection(true, 0)).assured("the staged view encodes");
    for owner in [
        EntityLabelOwner::Domain {
            name: name("sales"),
        },
        EntityLabelOwner::User {
            name: name("operator"),
        },
    ] {
        json["entity"] = serde_json::to_value(owner).assured("the owner encodes");
        let error = serde_json::from_value::<EntityLabelsInspection>(json.clone())
            .expect_err("administrative labels are committed only");
        assert!(error.to_string().contains("no staged transaction view"));
    }
}

fn raw(entries: &[(&str, &str)], limits: EntityLabelLimits) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let domain = builder.create_string("sales");
    let name = builder.create_string("payload");
    let entity = wire::NodeRef::create(
        &mut builder,
        &wire::NodeRefArgs {
            kind: Some(wire::ModelKind::WireJsonSchema),
            name: Some(name),
        },
    );
    let owner = wire::ModelLabelOwner::create(
        &mut builder,
        &wire::ModelLabelOwnerArgs {
            domain: Some(domain),
            model: Some(entity),
        },
    );
    let mut labels = Vec::new();
    for (key, value) in entries {
        let key = builder.create_string(key);
        let value = builder.create_string(value);
        labels.push(wire::LabelEntry::create(
            &mut builder,
            &wire::LabelEntryArgs {
                key: Some(key),
                value: Some(value),
            },
        ));
    }
    let labels = builder.create_vector(&labels);
    let limits = wire::EntityLabelLimits::create(
        &mut builder,
        &wire::EntityLabelLimitsArgs {
            count: limits.count,
            key_bytes: limits.key_bytes,
            value_bytes: limits.value_bytes,
            bytes: limits.bytes,
        },
    );
    let policy = wire::EntityLabelPolicy::create(
        &mut builder,
        &wire::EntityLabelPolicyArgs {
            revision: 1,
            limits: Some(limits),
        },
    );
    let committed = wire::CommittedEntityLabels::create(
        &mut builder,
        &wire::CommittedEntityLabelsArgs { revision: 0 },
    );
    let root = wire::EntityLabelsInspection::create(
        &mut builder,
        &wire::EntityLabelsInspectionArgs {
            owner_type: wire::EntityLabelOwner::ModelLabelOwner,
            owner: Some(owner.as_union_value()),
            labels: Some(labels),
            policy: Some(policy),
            view_type: wire::EntityLabelView::CommittedEntityLabels,
            view: Some(committed.as_union_value()),
        },
    );
    builder.finish(root, None);
    builder.finished_data().to_vec()
}

#[test]
fn current_wire_maps_refuse_invalid_duplicate_unordered_and_over_limit_entries() {
    let limits = policy().limits;
    for entries in [
        vec![("Upper", "value")],
        vec![("owner", "one"), ("owner", "two")],
        vec![("z", "one"), ("a", "two")],
    ] {
        let bytes = raw(&entries, limits);
        let table = flatbuffers::root::<wire::EntityLabelsInspection<'_>>(&bytes)
            .assured("the current table is structurally valid");
        assert!(decode_entity_labels(Decoder::new(&SessionLimits::DEFAULT), table).is_err());
    }
    for limits in [
        EntityLabelLimits { count: 0, ..limits },
        EntityLabelLimits {
            key_bytes: 4,
            ..limits
        },
        EntityLabelLimits {
            value_bytes: 4,
            ..limits
        },
        EntityLabelLimits { bytes: 9, ..limits },
    ] {
        let bytes = raw(&[("owner", "value")], limits);
        let table = flatbuffers::root::<wire::EntityLabelsInspection<'_>>(&bytes)
            .assured("the table carries a current explicit policy");
        assert!(decode_entity_labels(Decoder::new(&SessionLimits::DEFAULT), table).is_err());
    }
}

#[test]
fn bolero_entity_label_views_preserve_the_complete_map_identity_revision_and_policy() {
    bolero::check!().with_type::<[u8; 128]>().for_each(|input| {
        let mut arbitrary =
            nervix_arbitrary::Arbitrary::new(input, nervix_arbitrary::Domain::Vocabulary);
        let labels = arbitrary.entity_labels();
        let target = arbitrary.entity_label_target();
        let domain = name("sales");
        let owner = target
            .resolve(Some(&domain))
            .assured("every relative label target has a selected domain");
        let revision = u64::from_le_bytes(
            input[..8]
                .try_into()
                .assured("the bounded input holds eight revision bytes"),
        );
        let staged = input[0] & 1 == 1 && owner.supports_staged_view();
        let base = inspection(staged, revision);
        // Reconstruct through the production constructor; no mutable field can break the bound.
        let view = EntityLabelsInspection::new(owner, labels, base.view().clone(), base.policy())
            .assured("the bounded map fits the declared captured policy");
        assert_eq!(round_trip(view.clone(), "metadata".to_owned()), view);
    });
}
