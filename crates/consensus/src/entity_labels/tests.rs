//! Coherent metadata snapshot tests over the production publication reader.
//!
//! Layer: test harness.
//! - **Owns.** Exact map/revision/policy coherence and retained-read independence.
//! - **Depends on.** The production capture function and the selected publication lock.
//! - **Must not know.** Sessions, display parsing, transport or execution tasks.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns metadata publication invariants")
)]

use std::num::NonZeroU64;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    ClusterParameterChange, ClusterParameterRecord, ClusterParameterUpdates,
    ClusterParameterValues, CreateSchema, DomainSchedule, Model, ModelKind, ScheduledNode,
    SchemaFingerprint, SchemaName,
};

use super::*;

fn entity() -> DomainNodeRef {
    DomainNodeRef::node_in(
        DomainName::parse("metadata").assured("fixture domain is valid"),
        ModelKind::Schema,
        SchemaName::parse("event").assured("fixture schema is valid"),
    )
}

fn publication(
    owner: &str,
    revision: u64,
    policy_revision: NonZeroU64,
    maximum: u64,
) -> StateMachineData {
    use openraft::vote::RaftLeaderId as _;
    let entity = entity();
    let labels = serde_json::from_value(serde_json::json!({ "owner": owner }))
        .assured("the map has one valid key");
    let model = Model::Schema(CreateSchema {
        name: SchemaName::parse("event").assured("fixture schema is valid"),
        fields: Vec::new(),
        labels,
    });
    let scheduled = ScheduledNode::new(model, SchemaFingerprint::from_digest([0; 32]));
    let schedule = DomainSchedule::new(entity.domain.clone(), [scheduled], Vec::new());
    let parameters = ClusterParameterValues::default()
        .changed(
            &ClusterParameterUpdates::try_from(vec![ClusterParameterChange::EntityLabelsMaxCount(
                maximum,
            )])
            .assured("the update names a single declared parameter"),
        )
        .assured("the count is within the declared policy range");
    let mut state = StateMachineData::default();
    state.schedule.domains.insert(entity.domain, schedule);
    state.parameters = Some(ClusterParameterRecord {
        revision: policy_revision,
        values: parameters,
    });
    state.last_applied_log_id = Some(crate::LogIdOf::new(
        openraft::type_config::alias::CommittedLeaderIdOf::<crate::TypeConfig>::new(
            1,
            nervix_models::ClusterNodeName::parse("node").assured("fixture node is valid"),
        ),
        revision,
    ));
    state
}

fn first() -> StateMachineData {
    publication("Operations", 11, NonZeroU64::MIN, 20)
}
fn second() -> StateMachineData {
    publication(
        "Platform",
        22,
        NonZeroU64::new(2).assured("two is positive"),
        21,
    )
}

fn assert_coherent(view: &EntityLabelsInspection) {
    let EntityLabelView::Committed { revision } = *view.view() else {
        panic!("the production applied reader returns a committed view");
    };
    let tuple = (
        view.labels().get("owner"),
        revision,
        view.policy().revision.get(),
        view.policy().limits.count,
    );
    assert!(
        matches!(
            tuple,
            (Some("Operations"), 11, 1, 20) | (Some("Platform"), 22, 2, 21)
        ),
        "mixed publication: {tuple:?}"
    );
    assert_eq!(*view.entity(), entity());
}

#[test]
fn a_retained_view_keeps_its_exact_revision_and_policy_after_publication() {
    let applied = RwLock::new(first());
    let before = capture_labels(&applied, &entity()).assured("the entity exists");
    *applied.write() = second();
    let after = capture_labels(&applied, &entity()).assured("the replacement exists");
    assert_coherent(&before);
    assert_coherent(&after);
    assert_eq!(before.labels().get("owner"), Some("Operations"));
    assert_eq!(after.labels().get("owner"), Some("Platform"));
}

#[cfg(feature = "shuttle")]
#[test]
fn shuttle_entity_label_reads_hold_one_map_revision_and_committed_policy() {
    use nervix_primitives::thread;
    nervix_model_harness::shuttle::check_interleavings(|| {
        let applied = Arc::new(RwLock::new(first()));
        let writer = thread::spawn({
            let applied = applied.clone();
            move || {
                let replacement = second();
                *applied.write() = replacement;
            }
        });
        let reader = thread::spawn({
            let applied = applied.clone();
            move || {
                let view = capture_labels(&applied, &entity())
                    .assured("the entity exists in both publications");
                thread::yield_now();
                assert_coherent(&view);
                view
            }
        });
        let retained = reader.join().assured("the reader finishes without panic");
        writer.join().assured("the writer finishes without panic");
        assert_coherent(&retained);
        let final_view = capture_labels(&applied, &entity())
            .assured("the final publication contains the entity");
        assert_eq!(final_view.labels().get("owner"), Some("Platform"));
        assert_coherent(&final_view);
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn shuttle_entity_label_reads_remain_coherent_across_drop_and_recreation() {
    use nervix_primitives::thread;
    nervix_model_harness::shuttle::check_interleavings(|| {
        let applied = Arc::new(RwLock::new(first()));
        let before = capture_labels(&applied, &entity()).assured("the initial entity exists");
        let writer = thread::spawn({
            let applied = applied.clone();
            move || {
                let mut dropped = first();
                dropped.schedule = Default::default();
                *applied.write() = dropped;
                thread::yield_now();
                *applied.write() = second();
            }
        });
        let reader = thread::spawn({
            let applied = applied.clone();
            move || match capture_labels(&applied, &entity()) {
                Ok(view) => {
                    thread::yield_now();
                    assert_coherent(&view);
                }
                Err(error) => assert!(
                    matches!(error.current_context(), EntityLabelReadError::UnknownEntity { entity: absent } if absent == &entity())
                ),
            }
        });
        reader.join().assured("the reader finishes without panic");
        writer.join().assured("the writer finishes without panic");
        assert_eq!(before.labels().get("owner"), Some("Operations"));
        let recreated = capture_labels(&applied, &entity()).assured("the recreated entity exists");
        assert_eq!(recreated.labels().get("owner"), Some("Platform"));
        assert_coherent(&recreated);
    });
}
