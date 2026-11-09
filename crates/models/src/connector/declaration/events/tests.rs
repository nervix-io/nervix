//! Layer: test harness.
//! - **Owns.** Complete current event projections and declaration uniqueness checks.
//! - **Depends on.** Vocabulary declarations and their production JSON encoding.
//! - **Must not know.** Runtime metric registries, drivers or event recording.

use std::collections::BTreeSet;

use meticulous::ResultExt as _;

use super::*;

#[test]
fn every_entity_declares_unique_event_identities() {
    let declarations = crate::IngestorEntityKind::all()
        .map(|kind| kind.capabilities().events)
        .chain(crate::EmitterEntityKind::all().map(|kind| kind.capabilities().events));
    for declared in declarations {
        let unique: BTreeSet<_> = declared.as_slice().iter().collect();
        assert_eq!(unique.len(), declared.as_slice().len());
        for event in declared.as_slice() {
            assert!(declared.contains(*event));
        }
        assert_complete_projection(declared);
    }
}

#[test]
fn bolero_declared_events_project_the_complete_current_set() {
    bolero::check!()
        .with_iterations(128)
        .with_max_len(32)
        .for_each(|bytes: &[u8]| {
            let declarations: Vec<_> = crate::IngestorEntityKind::all()
                .map(|kind| kind.capabilities().events)
                .chain(crate::EmitterEntityKind::all().map(|kind| kind.capabilities().events))
                .collect();
            let index = usize::from(bytes.first().copied().unwrap_or(0)) % declarations.len();
            let declared = declarations[index];
            assert_complete_projection(declared);
        });
}

fn assert_complete_projection(declared: DeclaredEvents) {
    let expected: Vec<_> = declared
        .as_slice()
        .iter()
        .map(|event| match event {
            ConnectorEvent::PublishedWithoutSubscribers => "published_without_subscribers",
        })
        .collect();
    let projected = serde_json::to_value(declared)
        .assured("a current event declaration has a complete JSON projection");
    assert_eq!(projected, serde_json::json!(expected));
    let displayed: Vec<_> = declared
        .as_slice()
        .iter()
        .map(ToString::to_string)
        .collect();
    assert_eq!(displayed, expected);
}
