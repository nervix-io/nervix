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
    let update = EntityLabelMutation::Set(labels(&[("owner", "Platform"), ("region", "East")]));
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
    let keys = vec![
        LabelKey::try_from("empty".to_owned()).assured("empty is a valid key"),
        LabelKey::try_from("absent".to_owned()).assured("absent is a valid key"),
    ];
    let update =
        EntityLabelMutation::Drop(EntityLabelKeys::try_from(keys).assured("the keys differ"));
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
        assert!(
            original
                .updated(&EntityLabelMutation::Set(update), &policy)
                .is_err()
        );
        assert_eq!(original, labels(&[("key1", "one")]));
    }
    let valid = original
        .updated(&EntityLabelMutation::Set(labels(&[("k2", "two")])), &policy)
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
    assert!(EntityLabelKeys::try_from(vec![key.clone(), key]).is_err());
}

#[test]
fn key_set_encodings_preserve_complete_sets_and_refuse_duplicate_operations() {
    for source in [r#"[]"#, r#"["team/a","team/b"]"#] {
        let original: EntityLabelKeys =
            serde_json::from_str(source).assured("the authored keys are distinct and valid");
        let json = serde_json::to_string(&original).assured("the complete key set encodes");
        assert_eq!(json, source);
        let archive = rkyv::to_bytes::<rkyv::rancor::Error>(&original)
            .assured("the complete current key set archives");
        assert_eq!(
            rkyv::from_bytes::<EntityLabelKeys, rkyv::rancor::Error>(&archive)
                .assured("every current key restores exactly"),
            original
        );
    }
    assert!(serde_json::from_str::<EntityLabelKeys>(r#"["team/a","team/a"]"#).is_err());
    assert!(serde_json::from_str::<EntityLabelKeys>(r#"["Team/a"]"#).is_err());
}

#[test]
fn current_archives_refuse_duplicate_and_unordered_keys_in_maps_and_key_sets() {
    let keys: EntityLabelKeys = serde_json::from_str(r#"["team/a","team/b"]"#)
        .assured("the two current keys are distinct and ordered");
    let key_archive = rkyv::to_bytes::<rkyv::rancor::Error>(&keys)
        .assured("the complete current key set archives");
    let key_position = key_archive
        .windows(b"team/a".len())
        .position(|bytes| bytes == b"team/a")
        .assured("the archive contains the first current key's literal bytes");
    for replacement in *b"bc" {
        let mut malformed = key_archive.clone();
        malformed[key_position + b"team/".len()] = replacement;
        assert!(rkyv::from_bytes::<EntityLabelKeys, rkyv::rancor::Error>(&malformed).is_err());
    }

    let original = labels(&[("team/a", "first"), ("team/b", "second")]);
    let map_archive = rkyv::to_bytes::<rkyv::rancor::Error>(&original)
        .assured("the complete current map archives");
    let map_position = map_archive
        .windows(b"team/a".len())
        .position(|bytes| bytes == b"team/a")
        .assured("the archive contains the first current key's literal bytes");
    for replacement in [b'b', b'c'] {
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
    });
}

#[test]
fn bolero_label_mutations_match_an_independent_map_after_every_operation() {
    bolero::check!().with_type::<[u8; 128]>().for_each(|input| {
        let mut actual = EntityLabels::default();
        let mut expected = BTreeMap::<String, String>::new();
        for operation in input.chunks_exact(4) {
            let key = format!("key-{}", operation[0] % 4);
            let mutation = if operation[1] % 2 == 0 {
                let value = format!("Δ {} / {}", operation[2], operation[3]);
                expected.insert(key.clone(), value.clone());
                EntityLabelMutation::Set(labels(&[(&key, &value)]))
            } else {
                expected.remove(&key);
                let keys = vec![LabelKey::try_from(key).assured("the generated key is valid")];
                EntityLabelMutation::Drop(
                    EntityLabelKeys::try_from(keys).assured("one key is unique"),
                )
            };
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
