//! The label drafts lowered to their creation sets and their one `SET LABELS` edit.
//!
//! Layer: test harness.
//! - **Owns.** Owner tests and the registered property of the console's label drafts.
//! - **Depends on.** The drafts, the vocabulary's label values and canonical rendering.
//! - **Must not know.** Browser rendering, request dispatch or the server.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the label draft owner tests")
)]

use std::{collections::BTreeMap, num::NonZeroU64};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    AdministrativeLabelTarget, DomainName, EntityLabelError, EntityLabelLimits, EntityLabelOwner,
    EntityLabelPolicy, EntityLabelTarget, EntityLabelView, EntityLabels, EntityLabelsInspection,
    LabelAssignment, LabelChange, LabelKey, ModelKind, ModelName, NodeRef,
};

use super::labels_draft::{
    AddedLabel, CreationLabelsDraft, LabelConflict, LabelEditDraft, LabelRead, LabelRow,
    LabelTarget, LabelTargetKind, LabelsDraftError,
};

fn domain() -> DomainName {
    DomainName::parse("orders").assured("the fixture names a valid domain")
}

fn schema_target() -> LabelTarget {
    let name = ModelName::parse("readings").assured("the fixture names a valid schema");
    LabelTarget::new(
        Some(domain()),
        EntityLabelTarget::Model(NodeRef::new(ModelKind::Schema, name)),
    )
    .assured("a Model target with its domain is complete")
}

fn labels(entries: &[(&str, &str)]) -> EntityLabels {
    let mut assignments = Vec::new();
    for (key, value) in entries {
        assignments.push(LabelAssignment {
            key: LabelKey::try_from((*key).to_owned()).assured("the fixture keys are valid"),
            value: (*value).to_owned(),
        });
    }
    EntityLabels::try_from(assignments).assured("the fixture keys are distinct")
}

fn limits(count: u64) -> EntityLabelLimits {
    EntityLabelLimits {
        count,
        key_bytes: 128,
        value_bytes: 1024,
        bytes: 16384,
    }
}

fn inspection(target: &LabelTarget, read: EntityLabels, count: u64) -> EntityLabelsInspection {
    EntityLabelsInspection::new(
        target
            .owner()
            .assured("the target is resolved in its domain"),
        read,
        EntityLabelView::Committed { revision: 7 },
        EntityLabelPolicy {
            revision: NonZeroU64::MIN,
            limits: limits(count),
        },
    )
    .assured("the fixture set fits its limits")
}

/// A draft that read `read` for the schema target.
fn read_draft(read: &[(&str, &str)]) -> LabelEditDraft {
    let target = schema_target();
    let mut draft = LabelEditDraft::default();
    let sequence = draft.begin_read(target.clone());
    draft.read_completed(sequence, inspection(&target, labels(read), 64));
    draft
}

fn reread(draft: &mut LabelEditDraft, read: &[(&str, &str)]) {
    let target = draft
        .target()
        .cloned()
        .assured("the draft has read a target");
    let sequence = draft.begin_read(target.clone());
    draft.read_completed(sequence, inspection(&target, labels(read), 64));
}

fn statement_text(draft: &LabelEditDraft) -> String {
    draft
        .statement()
        .assured("the draft has a complete edit")
        .to_canonical_plexus()
        .assured("a label edit renders")
}

fn error(result: error_stack::Result<impl std::fmt::Debug, LabelsDraftError>) -> LabelsDraftError {
    match result {
        Ok(value) => panic!("expected a refusal, got {value:?}"),
        Err(report) => report.current_context().clone(),
    }
}

#[test]
fn creation_rows_keep_exact_keys_and_values_in_one_set() {
    let draft = CreationLabelsDraft {
        rows: vec![
            LabelRow {
                key: "team/path".to_owned(),
                value: " Δ 'quoted'\nvalue ".to_owned(),
            },
            LabelRow {
                key: "empty".to_owned(),
                value: String::new(),
            },
        ],
    };
    let built = draft.build().assured("both rows are valid");
    assert_eq!(
        built,
        labels(&[("team/path", " Δ 'quoted'\nvalue "), ("empty", "")])
    );
    assert_eq!(
        CreationLabelsDraft::from_labels(&built).build().ok(),
        Some(built)
    );
    assert_eq!(
        CreationLabelsDraft::default().build().ok(),
        Some(EntityLabels::default())
    );
}

#[test]
fn creation_rows_name_the_label_the_operator_fixes() {
    let missing = CreationLabelsDraft {
        rows: vec![LabelRow::default()],
    };
    assert_eq!(
        error(missing.build()),
        LabelsDraftError::MissingKey { label: 1 }
    );
    let invalid = CreationLabelsDraft {
        rows: vec![
            LabelRow {
                key: "owner".to_owned(),
                value: "a".to_owned(),
            },
            LabelRow {
                key: " Owner".to_owned(),
                value: "b".to_owned(),
            },
        ],
    };
    assert_eq!(
        error(invalid.build()),
        LabelsDraftError::Label {
            label: 2,
            issue: EntityLabelError::InvalidKey {
                key: " Owner".to_owned()
            }
        }
    );
    let duplicate = CreationLabelsDraft {
        rows: vec![
            LabelRow {
                key: "owner".to_owned(),
                value: "a".to_owned(),
            },
            LabelRow {
                key: "owner".to_owned(),
                value: "a".to_owned(),
            },
        ],
    };
    assert_eq!(
        error(duplicate.build()),
        LabelsDraftError::Labels(EntityLabelError::DuplicateKey {
            key: "owner".to_owned()
        })
    );
}

#[test]
fn one_edit_sets_changed_and_added_keys_and_drops_removed_ones() {
    let mut draft = read_draft(&[("owner", "Operations"), ("team", "orders"), ("same", "x")]);
    draft.set_value("owner", "Platform".to_owned());
    draft.set_value("same", "x".to_owned());
    draft.remove("team");
    draft.added.push(AddedLabel {
        row: LabelRow {
            key: "region".to_owned(),
            value: String::new(),
        },
        exists: None,
    });
    let mutation = draft.edit().assured("the edit is complete");
    let mut changes = BTreeMap::new();
    for (key, change) in mutation.iter() {
        changes.insert(key.as_str().to_owned(), change.clone());
    }
    assert_eq!(
        changes,
        BTreeMap::from([
            ("owner".to_owned(), LabelChange::Set("Platform".to_owned())),
            ("region".to_owned(), LabelChange::Set(String::new())),
            ("team".to_owned(), LabelChange::Drop),
        ])
    );
    assert_eq!(
        statement_text(&draft),
        "SET LABELS ON SCHEMA readings { 'owner' = 'Platform', 'region' = '', 'team' DROP };"
    );
}

#[test]
fn an_edit_that_changes_nothing_or_repeats_a_read_key_is_refused() {
    let mut draft = read_draft(&[("owner", "Operations")]);
    assert_eq!(error(draft.edit()), LabelsDraftError::Unchanged);
    draft.set_value("owner", "Operations".to_owned());
    assert_eq!(error(draft.edit()), LabelsDraftError::Unchanged);
    draft.added.push(AddedLabel {
        row: LabelRow {
            key: "owner".to_owned(),
            value: "again".to_owned(),
        },
        exists: None,
    });
    assert_eq!(
        error(draft.edit()),
        LabelsDraftError::AlreadyLabelled {
            key: "owner".to_owned()
        }
    );
    draft.added.clear();
    for key in ["extra", "extra"] {
        draft.added.push(AddedLabel {
            row: LabelRow {
                key: key.to_owned(),
                value: String::new(),
            },
            exists: None,
        });
    }
    assert_eq!(
        error(draft.edit()),
        LabelsDraftError::Labels(EntityLabelError::DuplicateKey {
            key: "extra".to_owned()
        })
    );
    assert_eq!(
        error(LabelEditDraft::default().edit()),
        LabelsDraftError::NotRead
    );
}

#[test]
fn the_complete_result_is_checked_against_the_limits_the_read_captured() {
    let target = schema_target();
    let mut draft = LabelEditDraft::default();
    let sequence = draft.begin_read(target.clone());
    draft.read_completed(
        sequence,
        inspection(&target, labels(&[("one", "1"), ("two", "2")]), 2),
    );
    draft.added.push(AddedLabel {
        row: LabelRow {
            key: "three".to_owned(),
            value: "3".to_owned(),
        },
        exists: None,
    });
    assert!(matches!(
        error(draft.edit()),
        LabelsDraftError::Labels(EntityLabelError::Limit {
            actual: 3,
            maximum: 2,
            ..
        })
    ));
    // Removing a label in the same edit makes room for the new one.
    draft.remove("two");
    let mutation = draft.edit().assured("two labels fit the limit");
    assert_eq!(mutation.len(), 2);
}

#[test]
fn a_reread_marks_changes_another_session_overtook_and_keeps_untouched_labels_current() {
    let mut draft = read_draft(&[("owner", "Operations"), ("team", "orders"), ("kept", "a")]);
    draft.set_value("owner", "Platform".to_owned());
    draft.set_value("team", "returns".to_owned());
    reread(
        &mut draft,
        &[("owner", "Security"), ("kept", "b"), ("fresh", "new")],
    );
    let read = draft.labels();
    assert_eq!(
        read.get("owner").and_then(|label| label.conflict.clone()),
        Some(LabelConflict::Changed {
            now: "Security".to_owned()
        })
    );
    assert_eq!(
        read.get("team").and_then(|label| label.conflict.clone()),
        Some(LabelConflict::Removed)
    );
    assert_eq!(read.get("kept").map(|label| label.read.as_str()), Some("b"));
    assert_eq!(
        read.get("fresh").map(|label| label.read.as_str()),
        Some("new")
    );
    assert!(draft.has_conflict());
    assert_eq!(error(draft.edit()), LabelsDraftError::Conflicted);

    draft.keep_change("owner");
    draft.keep_change("team");
    assert!(!draft.has_conflict());
    assert_eq!(
        statement_text(&draft),
        "SET LABELS ON SCHEMA readings { 'owner' = 'Platform', 'team' = 'returns' };"
    );
}

#[test]
fn using_the_current_value_drops_the_overtaken_change() {
    let mut draft = read_draft(&[("owner", "Operations"), ("team", "orders")]);
    draft.set_value("owner", "Platform".to_owned());
    draft.remove("team");
    draft.added.push(AddedLabel {
        row: LabelRow {
            key: "region".to_owned(),
            value: "east".to_owned(),
        },
        exists: None,
    });
    reread(&mut draft, &[("owner", "Security"), ("region", "west")]);
    // A removal another session already made is no conflict.
    assert!(draft.labels().get("team").is_none());
    assert_eq!(
        draft.added.first().and_then(|added| added.exists.clone()),
        Some("west".to_owned())
    );
    draft.use_current("owner");
    draft.resolve_added(0, false);
    assert!(!draft.has_conflict());
    assert_eq!(error(draft.edit()), LabelsDraftError::Unchanged);

    reread(&mut draft, &[("owner", "Security"), ("region", "west")]);
    draft.added.push(AddedLabel {
        row: LabelRow {
            key: "owner".to_owned(),
            value: "Mine".to_owned(),
        },
        exists: None,
    });
    assert!(matches!(
        error(draft.edit()),
        LabelsDraftError::AlreadyLabelled { .. }
    ));
}

#[test]
fn a_kept_value_for_a_key_another_session_added_becomes_an_edit_of_that_label() {
    let mut draft = read_draft(&[]);
    draft.added.push(AddedLabel {
        row: LabelRow {
            key: "owner".to_owned(),
            value: "Mine".to_owned(),
        },
        exists: None,
    });
    reread(&mut draft, &[("owner", "Theirs")]);
    draft.resolve_added(0, true);
    assert_eq!(
        statement_text(&draft),
        "SET LABELS ON SCHEMA readings { 'owner' = 'Mine' };"
    );
}

#[test]
fn only_the_latest_read_of_the_captured_entity_fills_the_draft() {
    let target = schema_target();
    let mut draft = LabelEditDraft::default();
    let first = draft.begin_read(target.clone());
    let second = draft.begin_read(target.clone());
    draft.read_completed(first, inspection(&target, labels(&[("old", "1")]), 64));
    assert!(matches!(draft.read(), LabelRead::Reading { sequence } if *sequence == second));

    let other_domain = DomainName::parse("returns").assured("the fixture names a valid domain");
    let elsewhere = LabelTarget::new(Some(other_domain), target.entity().clone())
        .assured("a Model target with its domain is complete");
    draft.read_completed(second, inspection(&elsewhere, labels(&[]), 64));
    assert!(matches!(draft.read(), LabelRead::Refused(_)));

    let third = draft.begin_read(target.clone());
    draft.read_refused(third, "missing".to_owned());
    assert_eq!(draft.read(), &LabelRead::Refused("missing".to_owned()));
}

#[test]
fn a_saved_edit_clears_the_changes_and_the_next_read_shows_the_result() {
    let mut draft = read_draft(&[("owner", "Operations"), ("team", "orders")]);
    draft.set_value("owner", "Platform".to_owned());
    draft.remove("team");
    draft.saved();
    assert!(!draft.has_conflict());
    reread(&mut draft, &[("owner", "Platform")]);
    assert_eq!(
        draft.labels().get("owner").map(|label| label.read.as_str()),
        Some("Platform")
    );
    assert!(draft.labels().get("team").is_none());
    assert_eq!(error(draft.edit()), LabelsDraftError::Unchanged);
}

#[test]
fn targets_need_a_domain_only_where_their_kind_resolves_in_one() {
    let mut draft = LabelEditDraft::choosing(
        LabelTargetKind::Model(ModelKind::Relay),
        "orders".to_owned(),
    );
    assert_eq!(
        error(draft.chosen_target(None)),
        LabelsDraftError::TargetDomainRequired
    );
    let target = draft
        .chosen_target(Some(domain()))
        .assured("the choice is complete");
    assert_eq!(target.domain(), Some(&domain()));
    assert_eq!(
        target.show_statement(),
        "SHOW LABELS ON RELAY orders FORMAT TEXT;"
    );

    draft.chosen_kind = Some(LabelTargetKind::User);
    draft.chosen_name = "auditor".to_owned();
    let user = draft
        .chosen_target(Some(domain()))
        .assured("a user target ignores the selected domain");
    assert_eq!(user.domain(), None);
    assert_eq!(
        user.owner(),
        Some(EntityLabelOwner::User {
            name: nervix_models::UserName::parse("auditor").assured("the fixture names a user")
        })
    );
    assert!(matches!(
        user.entity(),
        EntityLabelTarget::Administrative(AdministrativeLabelTarget::User(_))
    ));

    draft.chosen_name = "not a name".to_owned();
    assert_eq!(
        error(draft.chosen_target(None)),
        LabelsDraftError::TargetName { kind: "user" }
    );
    draft.chosen_kind = None;
    assert_eq!(
        error(draft.chosen_target(None)),
        LabelsDraftError::TargetKindRequired
    );
    for kind in LabelTargetKind::all() {
        assert_eq!(LabelTargetKind::from_key(&kind.key()), Some(kind));
    }
}

/// The values the property draws, including empty, quoted, Unicode and NUL ones.
fn generated_value(byte: u8) -> String {
    match byte % 6 {
        0 => String::new(),
        1 => " 'quoted' \"value\" ".to_owned(),
        2 => "Δ🙂\n\r\0".to_owned(),
        3 => "$s$verbatim$s$".to_owned(),
        4 => "Mixed Case / path".to_owned(),
        _ => byte.to_string(),
    }
}

#[test]
fn bolero_label_drafts_lower_to_the_exact_set_and_edit_the_operator_wrote() {
    bolero::check!().with_type::<[u8; 64]>().for_each(|input| {
        // A creation draft lowers to exactly the rows written.
        let count = usize::from(input[0] % 6);
        let mut rows = Vec::new();
        let mut expected_created = BTreeMap::new();
        for index in 0..count {
            let key = format!("created/key-{index}");
            let value = generated_value(input[index + 1]);
            expected_created.insert(key.clone(), value.clone());
            rows.push(LabelRow { key, value });
        }
        let created = CreationLabelsDraft { rows }
            .build()
            .assured("generated rows have distinct valid keys");
        let mut created_map = BTreeMap::new();
        for (key, value) in created.iter() {
            created_map.insert(key.as_str().to_owned(), value.to_owned());
        }
        assert_eq!(created_map, expected_created);

        // An edit applied to the set it read yields exactly the set the operator intended.
        let mut base_entries = Vec::new();
        for index in 0..usize::from(input[8] % 6) {
            base_entries.push((
                format!("read/key-{index}"),
                generated_value(input[index + 9]),
            ));
        }
        let mut base_refs = Vec::new();
        for (key, value) in &base_entries {
            base_refs.push((key.as_str(), value.as_str()));
        }
        let mut draft = read_draft(&base_refs);
        let mut intended = BTreeMap::new();
        for (index, (key, value)) in base_entries.iter().enumerate() {
            match input[index + 20] % 3 {
                0 => {
                    intended.insert(key.clone(), value.clone());
                }
                1 => {
                    let changed = generated_value(input[index + 30]);
                    draft.set_value(key, changed.clone());
                    intended.insert(key.clone(), changed);
                }
                _ => draft.remove(key),
            }
        }
        for index in 0..usize::from(input[40] % 4) {
            let key = format!("added/key-{index}");
            let value = generated_value(input[index + 41]);
            intended.insert(key.clone(), value.clone());
            draft.added.push(AddedLabel {
                row: LabelRow { key, value },
                exists: None,
            });
        }
        let read = labels(&base_refs);
        let mut read_map = BTreeMap::new();
        for (key, value) in read.iter() {
            read_map.insert(key.as_str().to_owned(), value.to_owned());
        }
        match draft.edit() {
            Ok(mutation) => {
                let result = read
                    .updated(&mutation, &limits(64))
                    .assured("the draft checked the result against these limits");
                let mut result_map = BTreeMap::new();
                for (key, value) in result.iter() {
                    result_map.insert(key.as_str().to_owned(), value.to_owned());
                }
                assert_eq!(result_map, intended);
                assert_ne!(result_map, read_map);
            }
            Err(report) => {
                assert_eq!(report.current_context(), &LabelsDraftError::Unchanged);
                assert_eq!(intended, read_map);
            }
        }
    });
}
