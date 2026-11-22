//! Current label syntax, complete-set bounds and immutable mutation contracts.
//!
//! Layer: test harness.
//! - **Owns.** Exact label value, immutable mutation and current decoder oracles.
//! - **Depends on.** The production metadata owner and its maintained serializers.
//! - **Must not know.** Registry state, runtime tasks, sessions or Plexus parsing.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns metadata value contract tests")
)]

use std::collections::BTreeMap;

use meticulous::{OptionExt as _, ResultExt as _};

use super::*;

fn labels(entries: &[(&str, &str)]) -> EntityLabels {
    let entries: Vec<LabelAssignment> = entries
        .iter()
        .map(|(key, value)| LabelAssignment {
            key: LabelKey::try_from((*key).to_owned()).assured("the fixture key is valid"),
            value: (*value).to_owned(),
        })
        .collect();
    EntityLabels::try_from(entries).assured("fixture keys are unique")
}

fn limits() -> EntityLabelLimits {
    EntityLabelLimits::from(&crate::ClusterParameterValues::default())
}

/// An edit setting each `Some` value and removing each `None` key.
fn edit(entries: &[(&str, Option<&str>)]) -> EntityLabelMutation {
    let mut edits = Vec::new();
    for (key, value) in entries {
        let change = match value {
            Some(value) => LabelChange::Set((*value).to_owned()),
            None => LabelChange::Drop,
        };
        edits.push(LabelEdit {
            key: LabelKey::try_from((*key).to_owned()).assured("the fixture key is valid"),
            change,
        });
    }
    EntityLabelMutation::try_from(edits).assured("fixture keys are unique")
}

/// An edit setting every label of `set`.
fn set_all(set: &EntityLabels) -> EntityLabelMutation {
    let mut edits = Vec::new();
    for (key, value) in set.iter() {
        edits.push(LabelEdit {
            key: key.clone(),
            change: LabelChange::Set(value.to_owned()),
        });
    }
    EntityLabelMutation::try_from(edits).assured("a label map has distinct keys")
}

#[test]
fn resolved_branch_schemas_preserve_configuration_across_label_edits() {
    let mut schema = crate::CreateSchema {
        labels: labels(&[("owner", "First")]),
        name: "tenant_key".try_into().assured("the schema name is valid"),
        fields: vec![crate::SchemaField {
            name: "tenant".try_into().assured("the field name is valid"),
            ty: crate::ParseAsType::String,
            optional: false,
            sensitive: true,
        }],
    };
    let branch = "tenants".try_into().assured("the branch name is valid");
    let before = crate::ResolvedBranching::branched(
        branch,
        schema.clone(),
        crate::ResolvedParametrizerPartitions::ONE,
    );
    schema.labels = labels(&[("owner", "Second")]);
    let after = crate::ResolvedBranching::branched(
        before
            .parametrizer()
            .assured("the fixture is branched")
            .clone(),
        schema.clone(),
        crate::ResolvedParametrizerPartitions::ONE,
    );
    assert_eq!(before, after);
    assert!(before.same_key_as(&after));
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&after)
        .assured("the complete resolved branch archives");
    assert_eq!(
        rkyv::from_bytes::<crate::ResolvedBranching, rkyv::rancor::Error>(&bytes)
            .assured("the current resolved branch restores"),
        after,
    );
    let canonical = schema
        .definition()
        .to_canonical_plexus()
        .assured("the key schema renders");
    assert_eq!(
        canonical,
        "CREATE SCHEMA tenant_key (\n  tenant STRING SENSITIVE\n);"
    );
    schema.fields[0].sensitive = false;
    let changed = crate::ResolvedBranching::branched(
        before
            .parametrizer()
            .assured("the fixture is branched")
            .clone(),
        schema,
        crate::ResolvedParametrizerPartitions::ONE,
    );
    assert!(!before.same_key_as(&changed));
}

#[test]
fn keys_accept_exactly_the_declared_ascii_alphabet() {
    for key in ["a", "0", "team/path", "a.b_c-d/0", "9/._-"] {
        let parsed = LabelKey::try_from(key.to_owned()).assured("the key follows the grammar");
        assert_eq!(parsed.as_str(), key);
    }
    for key in [
        "", "Upper", "/team", ".team", "_team", "-team", "a b", "a:b", "δ", "a\0",
    ] {
        assert!(LabelKey::try_from(key.to_owned()).is_err(), "{key:?}");
    }
}

#[test]
fn authored_duplicates_are_refused_even_when_the_values_agree() {
    let assignment = LabelAssignment {
        key: LabelKey::try_from("owner".to_owned()).assured("owner is a valid key"),
        value: "Operations".to_owned(),
    };
    assert!(EntityLabels::try_from(vec![assignment.clone(), assignment]).is_err());
    assert!(serde_json::from_str::<EntityLabels>(r#"{"owner":"one","owner":"one"}"#).is_err());
}

#[test]
fn a_merge_preserves_other_keys_and_exact_values_and_a_drop_is_idempotent() {
    let original = labels(&[
        ("owner", "Operations"),
        ("empty", ""),
        ("team/path", " / Δ "),
    ]);
    let update = edit(&[("owner", Some("Platform")), ("region", Some("East"))]);
    let merged = original
        .updated(&update, &limits())
        .assured("the merged map fits");
    assert_eq!(
        merged,
        labels(&[
            ("owner", "Platform"),
            ("empty", ""),
            ("team/path", " / Δ "),
            ("region", "East")
        ])
    );
    let update = edit(&[("empty", None), ("absent", None)]);
    let dropped = merged
        .updated(&update, &limits())
        .assured("removing keys stays within bounds");
    assert_eq!(
        dropped,
        labels(&[
            ("owner", "Platform"),
            ("team/path", " / Δ "),
            ("region", "East")
        ])
    );
    assert_eq!(
        dropped
            .updated(&update, &limits())
            .assured("the second removal is a no-op"),
        dropped
    );
    assert_eq!(original.get("owner"), Some("Operations"));
    assert_eq!(original.get("empty"), Some(""));
}

#[test]
fn every_limit_counts_the_complete_result_in_utf8_bytes() {
    let policy = EntityLabelLimits {
        count: 2,
        key_bytes: 4,
        value_bytes: 4,
        bytes: 12,
    };
    let original = labels(&[("key1", "one")]);
    original.validate(&policy).assured("seven bytes fit");
    for update in [
        labels(&[("key2", "two")]),
        labels(&[("large", "one")]),
        labels(&[("key1", "three")]),
    ] {
        assert!(original.updated(&set_all(&update), &policy).is_err());
        assert_eq!(original, labels(&[("key1", "one")]));
    }
    let valid = original
        .updated(&edit(&[("k2", Some("two"))]), &policy)
        .assured("the complete result is exactly twelve bytes");
    assert_eq!(valid, labels(&[("key1", "one"), ("k2", "two")]));
    let count_policy = EntityLabelLimits { count: 1, ..policy };
    assert!(valid.validate(&count_policy).is_err());
    let unicode = labels(&[("k", "ΔΔ")]);
    unicode
        .validate(&policy)
        .assured("two delta characters occupy four UTF-8 bytes");
    assert!(labels(&[("k", "ΔΔΔ")]).validate(&policy).is_err());
}

#[test]
fn decoders_preserve_an_empty_value_and_refuse_invalid_keys_and_duplicate_operations() {
    let decoded: EntityLabels = serde_json::from_str(r#"{"empty":"","owner":"  Mixed / Δ  "}"#)
        .assured("the complete current label map decodes");
    assert_eq!(
        decoded,
        labels(&[("empty", ""), ("owner", "  Mixed / Δ  ")])
    );
    assert!(serde_json::from_str::<EntityLabels>(r#"{"Uppercase":"value"}"#).is_err());
    let key = LabelKey::try_from("owner".to_owned()).assured("owner is a valid key");
    let set = LabelEdit {
        key: key.clone(),
        change: LabelChange::Set("one".to_owned()),
    };
    let drop = LabelEdit {
        key,
        change: LabelChange::Drop,
    };
    assert!(EntityLabelMutation::try_from(vec![set, drop.clone()]).is_err());
    assert!(EntityLabelMutation::try_from(vec![drop.clone(), drop]).is_err());
}

#[test]
fn one_edit_sets_and_removes_keys_and_is_checked_as_one_result() {
    let policy = EntityLabelLimits {
        count: 2,
        ..limits()
    };
    let original = labels(&[("one", "1"), ("two", "2")]);
    let swapped = original
        .updated(
            &edit(&[("three", Some("3")), ("two", None), ("absent", None)]),
            &policy,
        )
        .assured("the result keeps two labels");
    assert_eq!(swapped, labels(&[("one", "1"), ("three", "3")]));
    assert!(
        original
            .updated(
                &edit(&[("three", Some("3")), ("four", Some("4")), ("one", None)]),
                &policy
            )
            .is_err()
    );
    assert_eq!(original, labels(&[("one", "1"), ("two", "2")]));
    let unchanged = original
        .updated(&EntityLabelMutation::default(), &policy)
        .assured("an empty edit changes nothing");
    assert_eq!(unchanged, original);
}

#[test]
fn edit_encodings_preserve_every_change_and_refuse_repeated_or_invalid_keys() {
    for source in [r#"{}"#, r#"{"team/a":{"set":""},"team/b":"drop"}"#] {
        let original: EntityLabelMutation =
            serde_json::from_str(source).assured("the authored keys are distinct and valid");
        let json = serde_json::to_string(&original).assured("the complete edit encodes");
        assert_eq!(json, source);
        let archive = rkyv::to_bytes::<rkyv::rancor::Error>(&original)
            .assured("the complete current edit archives");
        assert_eq!(
            rkyv::from_bytes::<EntityLabelMutation, rkyv::rancor::Error>(&archive)
                .assured("every current change restores exactly"),
            original
        );
    }
    assert!(
        serde_json::from_str::<EntityLabelMutation>(r#"{"team/a":"drop","team/a":"drop"}"#)
            .is_err()
    );
    assert!(serde_json::from_str::<EntityLabelMutation>(r#"{"Team/a":"drop"}"#).is_err());
    assert!(serde_json::from_str::<EntityLabelMutation>(r#"{"team/a":"keep"}"#).is_err());
}

#[test]
fn current_archives_refuse_duplicate_and_unordered_keys_in_maps_and_edits() {
    let changes = edit(&[("team/a", Some("first")), ("team/b", None)]);
    let edit_archive = rkyv::to_bytes::<rkyv::rancor::Error>(&changes)
        .assured("the complete current edit archives");
    let edit_position = edit_archive
        .windows(b"team/a".len())
        .position(|bytes| bytes == b"team/a")
        .assured("the archive contains the first current key's literal bytes");
    for replacement in *b"bc" {
        let mut malformed = edit_archive.clone();
        malformed[edit_position + b"team/".len()] = replacement;
        assert!(rkyv::from_bytes::<EntityLabelMutation, rkyv::rancor::Error>(&malformed).is_err());
    }

    let original = labels(&[("team/a", "first"), ("team/b", "second")]);
    let map_archive = rkyv::to_bytes::<rkyv::rancor::Error>(&original)
        .assured("the complete current map archives");
    let map_position = map_archive
        .windows(b"team/a".len())
        .position(|bytes| bytes == b"team/a")
        .assured("the archive contains the first current key's literal bytes");
    for replacement in *b"bc" {
        let mut malformed = map_archive.clone();
        malformed[map_position + b"team/".len()] = replacement;
        assert!(rkyv::from_bytes::<EntityLabels, rkyv::rancor::Error>(&malformed).is_err());
    }
}

#[test]
fn bolero_label_maps_preserve_every_key_and_exact_value() {
    bolero::check!().with_type::<[u8; 128]>().for_each(|input| {
        let mut entries = Vec::new();
        for index in 0..usize::from(input[0] % 8) {
            let key = format!("team/key-{index}");
            let value = match input[index + 1] % 6 {
                0 => String::new(),
                1 => " 'quoted' \"value\" ".to_owned(),
                2 => "Δ🙂\n\r\0".to_owned(),
                3 => "$s$verbatim$s$".to_owned(),
                4 => "Mixed Case / path".to_owned(),
                _ => input[index + 16].to_string(),
            };
            entries.push(LabelAssignment {
                key: LabelKey::try_from(key).assured("the generated key has the declared alphabet"),
                value,
            });
        }
        let original = EntityLabels::try_from(entries).assured("the generated keys are unique");
        original
            .validate(&limits())
            .assured("eight bounded values fit the default limits");
        let json = serde_json::to_vec(&original).assured("a typed string map encodes");
        assert_eq!(
            serde_json::from_slice::<EntityLabels>(&json).assured("the complete map decodes"),
            original
        );
        let archive = rkyv::to_bytes::<rkyv::rancor::Error>(&original)
            .assured("a bounded current map archives");
        assert_eq!(
            rkyv::from_bytes::<EntityLabels, rkyv::rancor::Error>(&archive)
                .assured("the complete map restores"),
            original
        );

        // The same keys as one edit: every other one removed, the rest set to its value.
        let mut edits = Vec::new();
        for (index, (key, value)) in original.iter().enumerate() {
            let change = if input[index + 32] % 2 == 0 {
                LabelChange::Drop
            } else {
                LabelChange::Set(value.to_owned())
            };
            edits.push(LabelEdit {
                key: key.clone(),
                change,
            });
        }
        let changes = EntityLabelMutation::try_from(edits).assured("the map's keys are unique");
        let json = serde_json::to_vec(&changes).assured("a typed edit encodes");
        assert_eq!(
            serde_json::from_slice::<EntityLabelMutation>(&json)
                .assured("the complete edit decodes"),
            changes
        );
        let archive = rkyv::to_bytes::<rkyv::rancor::Error>(&changes)
            .assured("a bounded current edit archives");
        assert_eq!(
            rkyv::from_bytes::<EntityLabelMutation, rkyv::rancor::Error>(&archive)
                .assured("the complete edit restores"),
            changes
        );
    });
}

#[test]
fn bolero_label_mutations_match_an_independent_map_after_every_operation() {
    bolero::check!().with_type::<[u8; 128]>().for_each(|input| {
        let mut actual = EntityLabels::default();
        let mut expected = BTreeMap::<String, String>::new();
        for operation in input.as_chunks::<4>().0 {
            // One edit names up to two of the four keys, each set or removed.
            let first = usize::from(operation[0] % 4);
            let second = (first + 1 + usize::from(operation[1] % 3)) % 4;
            let mut edits = Vec::new();
            let mut names = vec![first];
            if operation[1] % 2 == 0 {
                names.push(second);
            }
            for (index, name) in names.into_iter().enumerate() {
                let key = format!("key-{name}");
                let change = if (operation[2] >> index) % 2 == 0 {
                    let value = format!("Δ {} / {}", operation[3], index);
                    expected.insert(key.clone(), value.clone());
                    LabelChange::Set(value)
                } else {
                    expected.remove(&key);
                    LabelChange::Drop
                };
                edits.push(LabelEdit {
                    key: LabelKey::try_from(key).assured("the generated key is valid"),
                    change,
                });
            }
            let mutation =
                EntityLabelMutation::try_from(edits).assured("one edit names distinct keys");
            actual = actual
                .updated(&mutation, &limits())
                .assured("four small keys fit the default limits");
            let complete = actual
                .iter()
                .map(|(key, value)| (key.as_str().to_owned(), value.to_owned()))
                .collect::<BTreeMap<_, _>>();
            assert_eq!(complete, expected);
        }
    });
}

#[test]
fn bolero_label_decoders_reject_invalid_current_keys_and_duplicate_assignments() {
    bolero::check!().with_type::<[u8; 32]>().for_each(|input| {
        let value = format!("Δ {} / {}", input[0], input[1]);
        let original = labels(&[("team/path", &value)]);
        let archive = rkyv::to_bytes::<rkyv::rancor::Error>(&original)
            .assured("the current complete map archives");
        let mut malformed = archive.clone();
        let key_position = malformed
            .windows(b"team/path".len())
            .position(|bytes| bytes == b"team/path")
            .assured("the archive contains its current literal key bytes");
        malformed[key_position] = b'T';
        assert!(rkyv::from_bytes::<EntityLabels, rkyv::rancor::Error>(&malformed).is_err());

        let encoded = serde_json::to_string(&value).assured("a string value encodes");
        let duplicate = format!("{{\"team/path\":{encoded},\"team/path\":{encoded}}}");
        assert!(serde_json::from_str::<EntityLabels>(&duplicate).is_err());
        let invalid = format!("{{\"Team/path\":{encoded}}}");
        assert!(serde_json::from_str::<EntityLabels>(&invalid).is_err());

        // An edit's decoders refuse the same keys, and a key both set and removed.
        let set_and_drop =
            format!("{{\"team/path\":{{\"set\":{encoded}}},\"team/path\":\"drop\"}}");
        assert!(serde_json::from_str::<EntityLabelMutation>(&set_and_drop).is_err());
        let invalid_edit = format!("{{\"Team/path\":{{\"set\":{encoded}}}}}");
        assert!(serde_json::from_str::<EntityLabelMutation>(&invalid_edit).is_err());
        let changes = edit(&[("team/path", Some(&value))]);
        let edit_archive = rkyv::to_bytes::<rkyv::rancor::Error>(&changes)
            .assured("the current complete edit archives");
        let mut malformed_edit = edit_archive.clone();
        let edit_key = malformed_edit
            .windows(b"team/path".len())
            .position(|bytes| bytes == b"team/path")
            .assured("the archive contains its current literal key bytes");
        malformed_edit[edit_key] = b'T';
        assert!(
            rkyv::from_bytes::<EntityLabelMutation, rkyv::rancor::Error>(&malformed_edit).is_err()
        );
        assert_eq!(
            rkyv::from_bytes::<EntityLabelMutation, rkyv::rancor::Error>(&edit_archive)
                .assured("validation did not change the complete valid edit archive"),
            changes
        );
        assert_eq!(
            rkyv::from_bytes::<EntityLabels, rkyv::rancor::Error>(&archive)
                .assured("validation did not change the complete valid archive"),
            original
        );
    });
}

fn generated_model(variant: nervix_arbitrary::ModelVariant) -> crate::Model {
    // The generator's dev-dependency sees the library build; decode the complete current value
    // into the unit harness's instance of the same vocabulary before exercising its owner.
    let generated =
        nervix_arbitrary::Arbitrary::new(&[37; 2048], nervix_arbitrary::Domain::Vocabulary)
            .model_of(variant)
            .try_map_resource_versions(|_, _| Ok::<u64, std::convert::Infallible>(1))
            .assured("the fixture pins every resource version to one");
    let archive = rkyv::to_bytes::<rkyv::rancor::Error>(&generated)
        .assured("the complete current generated model archives");
    rkyv::from_bytes::<crate::Model, rkyv::rancor::Error>(&archive)
        .assured("the unit harness reads the complete current vocabulary shape")
}

#[test]
fn every_model_keeps_configuration_classification_independent_of_metadata() {
    for variant in nervix_arbitrary::ModelVariant::ALL {
        let mut original = generated_model(variant);
        *original.labels_mut() = labels(&[("owner", "Operations")]);
        let mut candidate = original.clone();
        *candidate.labels_mut() = labels(&[("owner", "Platform"), ("empty", "")]);
        assert_eq!(
            original.execution_configuration(),
            candidate.execution_configuration(),
            "{variant:?}"
        );
        assert!(matches!(
            original.execution_configuration(),
            std::borrow::Cow::Owned(_)
        ));
        let changes = original.change_aspects_against(&candidate);
        assert_eq!(
            changes.aspects(),
            &[crate::ModelChangeAspect::EntityLabels],
            "{variant:?}"
        );
        assert!(changes.dynamic_updates().is_empty(), "{variant:?}");
        assert!(changes.state_purges().is_empty(), "{variant:?}");
        assert!(
            changes
                .aspects()
                .iter()
                .all(|aspect| aspect.is_control_plane_only()),
            "{variant:?}"
        );
        assert_eq!(candidate.labels().get("owner"), Some("Platform"));
    }
}

#[test]
fn an_empty_metadata_projection_borrows_the_existing_model() {
    let mut model = generated_model(nervix_arbitrary::ModelVariant::Schema);
    *model.labels_mut() = EntityLabels::default();
    assert!(matches!(
        model.execution_configuration(),
        std::borrow::Cow::Borrowed(_)
    ));
}
