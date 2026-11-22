//! Coherent metadata snapshot tests over the production publication reader.
//!
//! Layer: test harness.
//! - **Owns.** Exact map/revision/policy coherence, the staged view's control snapshot and
//!   retained-read independence.
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
    ClusterParameterValues, CreateSchema, DomainConfig, DomainNodeRef, DomainPace, DomainSchedule,
    DomainStartPoint, DomainStatus, Model, ModelKind, ScheduledNode, SchemaFingerprint, SchemaName,
    Timestamp, UserName,
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
    let labels: EntityLabels = serde_json::from_value(serde_json::json!({ "owner": owner }))
        .assured("the map has one valid key");
    let model = Model::Schema(CreateSchema {
        name: SchemaName::parse("event").assured("fixture schema is valid"),
        fields: Vec::new(),
        labels: labels.clone(),
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
    state
        .schedule
        .domains
        .insert(entity.domain.clone(), schedule);
    state.domains.insert(
        entity.domain.clone(),
        DomainState {
            id: entity.domain.clone(),
            config: DomainConfig {
                pace: DomainPace::Unpaced,
            },
            status: DomainStatus::Running,
            start_version: 1,
            last_start: DomainStartPoint::Resume,
            clock: None,
            stop: None,
            labels: labels.clone(),
        },
    );
    state
        .resources
        .ensure_catalog(&entity.domain, &resource(), &labels);
    let user = user();
    state.users.insert(
        user.clone(),
        crate::UserRecord {
            name: user,
            password_credential: nervix_models::PasswordCredential::from_parts(
                210000, [11; 16], [17; 64],
            )
            .assured("the synthetic credential has a bounded cost"),
            labels,
        },
    );
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

fn resource() -> ResourceName {
    ResourceName::parse("table").assured("fixture resource is valid")
}

fn user() -> UserName {
    UserName::parse("reader").assured("fixture user is valid")
}

fn owners() -> [EntityLabelOwner; 4] {
    let model = entity();
    [
        EntityLabelOwner::Domain {
            name: model.domain.clone(),
        },
        EntityLabelOwner::Resource(ResourceCatalogKey::new(&model.domain, &resource())),
        EntityLabelOwner::User { name: user() },
        EntityLabelOwner::Model(model),
    ]
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
}

#[test]
fn a_retained_view_keeps_its_exact_revision_and_policy_after_publication() {
    let applied = RwLock::new(first());
    let before =
        owners().map(|owner| capture_labels(&applied, &owner).assured("the entity exists"));
    *applied.write() = second();
    for (before, owner) in before.into_iter().zip(owners()) {
        let after = capture_labels(&applied, &owner).assured("the replacement exists");
        assert_eq!(before.entity(), &owner);
        assert_eq!(after.entity(), &owner);
        assert_coherent(&before);
        assert_coherent(&after);
        assert_eq!(before.labels().get("owner"), Some("Operations"));
        assert_eq!(after.labels().get("owner"), Some("Platform"));
    }
}

#[test]
fn replaying_a_user_label_edit_preserves_later_labels_and_the_credential() {
    use crate::{CommandExecution, CommandExecutionAdmissionPolicy, ConsensusResponse};

    let mut state = first();
    let user = user();
    let original = state
        .users
        .get(&user)
        .assured("the fixture has a user")
        .password_credential
        .clone();
    let admitted_at = Timestamp::from_unix_nanos(1);
    let policy =
        CommandExecutionAdmissionPolicy::at(admitted_at, std::time::Duration::from_secs(60), 5);
    let mut preexisting = None;
    for (ordinal, value) in [(1, "Finance"), (2, "Operations")] {
        let reference =
            CommandExecutionReference::parse(format!("018bcfe5-6800-7000-8000-{ordinal:012x}"))
                .assured("the fixture reference is a valid UUIDv7");
        let labels = serde_json::from_value(serde_json::json!({ "owner": value }))
            .assured("the fixture label is valid");
        state.command_executions.insert_admitted(
            CommandExecution::applying(
                reference.clone(),
                user.clone(),
                None,
                [0; 32],
                admitted_at,
                CommandExecutionEffect::EditAdministrativeLabels {
                    target: AdministrativeLabelTarget::User(user.clone()),
                    mutation: EntityLabelMutation::Set(labels),
                    outcome: None,
                },
            ),
            &policy,
        );
        let outcome = apply_administrative_labels(&mut state, &reference);
        assert_eq!(outcome.response, ConsensusResponse::Applied);
        if ordinal == 1 {
            preexisting = Some(reference);
        }
    }
    let outcome = apply_administrative_labels(
        &mut state,
        &preexisting.assured("the first edit was admitted"),
    );
    assert_eq!(outcome.response, ConsensusResponse::Applied);
    let record = state.users.get(&user).assured("the user still exists");
    assert_eq!(record.labels.get("owner"), Some("Operations"));
    assert_eq!(record.password_credential, original);
}

/// The staged view plans its candidate against the control snapshot a commit is checked against,
/// so a licensing stop gate closed by committed time reaches the preview as it reaches the commit.
#[test]
fn a_staged_label_view_captures_the_committed_licensing_stop_gate() {
    let domain = DomainName::parse("tenant").assured("fixture domain is valid");
    let owner = nervix_models::UserName::parse("operator").assured("fixture owner is valid");
    let mut state = crate::licensing::stop_gate_tests::ephemeral_state();
    state.parameters = Some(ClusterParameterRecord {
        revision: NonZeroU64::MIN,
        values: ClusterParameterValues::default(),
    });
    let activity = crate::TransactionActivity::from_timeout(
        nervix_models::Timestamp::from_unix_nanos(1),
        std::time::Duration::from_secs(60),
    );
    state.transactions.insert(
        "tx".to_owned(),
        ReplicatedTransaction::open("tx".to_owned(), domain.clone(), owner, activity),
    );
    let applied = RwLock::new(state.clone());
    let open = capture_transaction(&applied, &domain, "tx").assured("the transaction is open");
    assert_eq!(open.control.licensing_stop, None);

    let reason = crate::licensing::stop_gate_tests::pass_expiry(&mut state);
    *applied.write() = state;
    let closed = capture_transaction(&applied, &domain, "tx").assured("the transaction is open");
    assert_eq!(closed.control.licensing_stop, Some(reason));
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
                owners().map(|owner| {
                    let view = capture_labels(&applied, &owner)
                        .assured("the entity exists in both publications");
                    thread::yield_now();
                    assert_eq!(view.entity(), &owner);
                    assert_coherent(&view);
                    view
                })
            }
        });
        let retained = reader.join().assured("the reader finishes without panic");
        writer.join().assured("the writer finishes without panic");
        for (view, owner) in retained.into_iter().zip(owners()) {
            assert_coherent(&view);
            let final_view = capture_labels(&applied, &owner)
                .assured("the final publication contains the entity");
            assert_eq!(final_view.labels().get("owner"), Some("Platform"));
            assert_coherent(&final_view);
        }
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn shuttle_entity_label_reads_remain_coherent_across_drop_and_recreation() {
    use nervix_primitives::thread;
    nervix_model_harness::shuttle::check_interleavings(|| {
        let applied = Arc::new(RwLock::new(first()));
        let before = capture_labels(&applied, &EntityLabelOwner::Model(entity()))
            .assured("the initial entity exists");
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
            move || match capture_labels(&applied, &EntityLabelOwner::Model(entity())) {
                Ok(view) => {
                    thread::yield_now();
                    assert_coherent(&view);
                }
                Err(error) => assert!(
                    matches!(error.current_context(), EntityLabelReadError::UnknownEntity { entity: absent } if absent == &EntityLabelOwner::Model(entity()))
                ),
            }
        });
        reader.join().assured("the reader finishes without panic");
        writer.join().assured("the writer finishes without panic");
        assert_eq!(before.labels().get("owner"), Some("Operations"));
        let recreated = capture_labels(&applied, &EntityLabelOwner::Model(entity()))
            .assured("the recreated entity exists");
        assert_eq!(recreated.labels().get("owner"), Some("Platform"));
        assert_coherent(&recreated);
    });
}
