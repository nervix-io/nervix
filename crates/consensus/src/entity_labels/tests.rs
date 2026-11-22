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

/// What one applied revision published for the owner a log follows.
#[cfg(feature = "shuttle")]
struct PublishedRevision {
    revision: u64,
    labels: Option<EntityLabels>,
}

/// One label-changing application of a command execution reference, in log order.
#[cfg(feature = "shuttle")]
struct LabelApplication {
    reference: CommandExecutionReference,
    changed_labels: bool,
}

#[cfg(feature = "shuttle")]
#[derive(Default)]
struct AppliedHistory {
    revisions: Vec<PublishedRevision>,
    applications: Vec<LabelApplication>,
}

/// The store's application of committed entries over the production state machine: one entry at
/// a time in log order, each computed on a copy of the published state and swapped in whole, as
/// the store publishes it. A reader therefore holds one applied revision, never a state between
/// two. The history records what every revision published for one owner.
#[cfg(feature = "shuttle")]
struct AppliedLog {
    published: RwLock<StateMachineData>,
    owner: EntityLabelOwner,
    history: nervix_primitives::sync::blocking::Mutex<AppliedHistory>,
}

#[cfg(feature = "shuttle")]
impl AppliedLog {
    fn new(initial: StateMachineData, owner: EntityLabelOwner) -> Self {
        let mut history = AppliedHistory::default();
        history.revisions.push(PublishedRevision {
            revision: revision(&initial),
            labels: Self::labels_of(&initial, &owner),
        });
        Self {
            published: RwLock::new(initial),
            owner,
            history: nervix_primitives::sync::blocking::Mutex::new(history),
        }
    }

    fn labels_of(state: &StateMachineData, owner: &EntityLabelOwner) -> Option<EntityLabels> {
        match inspect_labels(state, owner) {
            Ok(view) => Some(view.labels().clone()),
            Err(_) => None,
        }
    }

    /// Applies `command` as the next committed entry and publishes the resulting state.
    fn apply(&self, command: &ConsensusCommand) -> AppliedConsensusCommand {
        let mut history = self.history.lock();
        let mut next = self.published.read().clone();
        let before = Self::labels_of(&next, &self.owner);
        let outcome = crate::apply_consensus_command(&mut next, command);
        let previous = history
            .revisions
            .last()
            .verified("the history starts with the initial revision")
            .revision;
        let revision = previous
            .checked_add(1)
            .assured("a model run applies a handful of entries");
        next.last_applied_log_id = Some(log_id(revision));
        let after = Self::labels_of(&next, &self.owner);
        if let ConsensusCommand::EditAdministrativeLabels { reference } = command {
            history.applications.push(LabelApplication {
                reference: reference.clone(),
                changed_labels: before != after,
            });
        }
        history.revisions.push(PublishedRevision {
            revision,
            labels: after,
        });
        *self.published.write() = next;
        outcome
    }

    /// Plans against the published state and commits through the state machine's planning-input
    /// check, planning again after a conflict, as a transaction planner does.
    fn replace_schedule_until_applied(
        &self,
        plan: impl Fn(&DomainPlanningInputs) -> DomainSchedule,
    ) {
        // The other proposer of these checks commits at most once, so a second attempt always
        // plans against the state it left.
        for _ in 0..2 {
            let inputs = self
                .published
                .read()
                .domain_planning_inputs(&entity().domain);
            let schedule = plan(&inputs);
            let outcome = self.apply(&ConsensusCommand::ReplaceDomainSchedule {
                inputs: Box::new(inputs),
                schedule: Some(Box::new(schedule)),
                mutation: None,
            });
            if outcome.response == crate::ConsensusResponse::Applied {
                return;
            }
        }
        panic!("a schedule replacement is applied once nothing else commits beside it");
    }

    /// A capture is one published revision exactly: its labels are the ones that revision
    /// published, and an absent owner was absent at some published revision.
    fn assert_published(
        &self,
        capture: &Result<EntityLabelsInspection, Report<EntityLabelReadError>>,
    ) {
        let history = self.history.lock();
        match capture {
            Ok(view) => {
                let EntityLabelView::Committed { revision } = *view.view() else {
                    panic!("the production applied reader returns a committed view");
                };
                let published = history
                    .revisions
                    .iter()
                    .find(|published| published.revision == revision)
                    .verified("a reader captures a revision the log published");
                assert_eq!(
                    published.labels.as_ref(),
                    Some(view.labels()),
                    "the capture mixes revision {revision} with another"
                );
            }
            Err(error) => {
                assert!(matches!(
                    error.current_context(),
                    EntityLabelReadError::UnknownEntity { .. }
                ));
                assert!(
                    history
                        .revisions
                        .iter()
                        .any(|published| published.labels.is_none()),
                    "the owner is reported absent although every revision published it"
                );
            }
        }
    }
}

#[cfg(feature = "shuttle")]
fn log_id(revision: u64) -> crate::LogIdOf {
    use openraft::vote::RaftLeaderId as _;
    crate::LogIdOf::new(
        openraft::type_config::alias::CommittedLeaderIdOf::<crate::TypeConfig>::new(
            1,
            nervix_models::ClusterNodeName::parse("node").assured("fixture node is valid"),
        ),
        revision,
    )
}

#[cfg(feature = "shuttle")]
fn label_map(entries: serde_json::Value) -> EntityLabels {
    serde_json::from_value(entries).assured("the fixture label map has valid keys and values")
}

#[cfg(feature = "shuttle")]
fn user_edit_reference(ordinal: u64) -> CommandExecutionReference {
    CommandExecutionReference::parse(format!("018bcfe5-6800-7000-8000-{ordinal:012x}"))
        .assured("the fixture reference is a valid UUIDv7")
}

/// Admits a user label edit as the session service does before it applies the edit, at the instant
/// the fixture references' UUIDv7 time names (2023-11-14T22:13:20Z), which admission accepts.
#[cfg(feature = "shuttle")]
fn admit_user_edit(log: &AppliedLog, reference: &CommandExecutionReference, labels: EntityLabels) {
    let admitted_at = Timestamp::from_unix_nanos(1_700_000_000_000_000_000);
    let execution = crate::CommandExecution::applying(
        reference.clone(),
        user(),
        None,
        [0; 32],
        admitted_at,
        CommandExecutionEffect::EditAdministrativeLabels {
            target: AdministrativeLabelTarget::User(user()),
            mutation: EntityLabelMutation::Set(labels),
            outcome: None,
        },
    );
    let admitted = log.apply(&ConsensusCommand::AdmitCommandExecution {
        execution: Box::new(execution),
        mutation_domains: std::collections::BTreeSet::new(),
        policy: crate::CommandExecutionAdmissionPolicy::at(
            admitted_at,
            std::time::Duration::from_secs(60),
            5,
        ),
    });
    assert_eq!(admitted.response, crate::ConsensusResponse::Applied);
}

/// Two editors of one user's labels race each other and a reader, and the first editor repeats
/// its exact request once its answer was lost. Each edit changes the labels exactly once, a replay
/// returns the recorded outcome without undoing a later edit, neither edit loses the other's keys,
/// and every capture is one published revision.
#[cfg(feature = "shuttle")]
#[test]
fn shuttle_two_user_label_edits_each_apply_once_and_a_late_replay_keeps_the_later_edit() {
    use nervix_primitives::thread;
    nervix_model_harness::shuttle::check_interleavings(|| {
        let owner = EntityLabelOwner::User { name: user() };
        let log = Arc::new(AppliedLog::new(first(), owner.clone()));
        let platform = user_edit_reference(1);
        let finance = user_edit_reference(2);
        let replaying_editor = thread::spawn({
            let log = log.clone();
            let reference = platform.clone();
            move || {
                admit_user_edit(
                    &log,
                    &reference,
                    label_map(serde_json::json!({ "owner": "Platform", "first": "1" })),
                );
                let edit = ConsensusCommand::EditAdministrativeLabels {
                    reference: reference.clone(),
                };
                assert_eq!(log.apply(&edit).response, crate::ConsensusResponse::Applied);
                thread::yield_now();
                assert_eq!(log.apply(&edit).response, crate::ConsensusResponse::Applied);
            }
        });
        let editor = thread::spawn({
            let log = log.clone();
            let reference = finance.clone();
            move || {
                admit_user_edit(
                    &log,
                    &reference,
                    label_map(serde_json::json!({ "owner": "Finance", "second": "2" })),
                );
                let edit = ConsensusCommand::EditAdministrativeLabels { reference };
                assert_eq!(log.apply(&edit).response, crate::ConsensusResponse::Applied);
            }
        });
        let reader = thread::spawn({
            let log = log.clone();
            let owner = owner.clone();
            move || {
                for _ in 0..2 {
                    let capture = capture_labels(&log.published, &owner);
                    log.assert_published(&capture);
                    thread::yield_now();
                }
            }
        });
        replaying_editor
            .join()
            .assured("the replaying editor finishes without panic");
        editor.join().assured("the editor finishes without panic");
        reader.join().assured("the reader finishes without panic");

        let history = log.history.lock();
        for reference in [&platform, &finance] {
            let changes = history
                .applications
                .iter()
                .filter(|application| {
                    &application.reference == reference && application.changed_labels
                })
                .count();
            assert_eq!(changes, 1, "an edit changes the labels exactly once");
        }
        let last_change = history
            .applications
            .iter()
            .rev()
            .find(|application| application.changed_labels)
            .verified("both edits changed the labels");
        let expected_owner = if last_change.reference == platform {
            "Platform"
        } else {
            "Finance"
        };
        drop(history);
        let committed = capture_labels(&log.published, &owner)
            .assured("the edited user exists in every publication");
        assert_eq!(committed.labels().get("owner"), Some(expected_owner));
        assert_eq!(committed.labels().get("first"), Some("1"));
        assert_eq!(committed.labels().get("second"), Some("2"));
    });
}

/// How the label editor's one commit ended.
#[cfg(feature = "shuttle")]
enum ModelEditOutcome {
    /// The editor found no Model to edit.
    Absent,
    /// The state machine refused the commit because its planning inputs had changed.
    Refused,
    /// The commit applied the edit to the Model whose labels it planned against.
    Applied { planned_against: EntityLabels },
}

/// The candidate schedule that sets `labels` on the Model `inputs` schedule, if they schedule it.
#[cfg(feature = "shuttle")]
fn relabelled(
    inputs: &DomainPlanningInputs,
    labels: EntityLabels,
) -> Option<(DomainSchedule, EntityLabels)> {
    let mut schedule = inputs.schedule()?.clone();
    let node = schedule.nodes.get_mut(&entity().node)?;
    let planned_against = node.config.labels().clone();
    let limits = EntityLabelLimits::from(&inputs.parameters()?.values);
    let edited = planned_against
        .updated(&EntityLabelMutation::Set(labels), &limits)
        .assured("the fixture edit fits the committed limits");
    *node.config.labels_mut() = edited;
    Some((schedule, planned_against))
}

/// A label edit planned against a Model races the Model's drop and its recreation with other
/// labels, and a reader. The state machine's planning-input check refuses an edit planned before
/// the drop once the drop committed, so the recreated Model never carries the dropped Model's
/// labels: it holds exactly its own, plus the edit only when the edit was planned against it.
#[cfg(feature = "shuttle")]
#[test]
fn shuttle_a_model_label_edit_planned_before_a_drop_never_reaches_the_recreated_model() {
    use nervix_models::{CreateSchema, Model, ScheduledNode, SchemaFingerprint, SchemaName};
    use nervix_primitives::thread;
    nervix_model_harness::shuttle::check_interleavings(|| {
        let owner = EntityLabelOwner::Model(entity());
        let log = Arc::new(AppliedLog::new(first(), owner.clone()));
        let editor = thread::spawn({
            let log = log.clone();
            move || {
                let inputs = log
                    .published
                    .read()
                    .domain_planning_inputs(&entity().domain);
                let edit = label_map(serde_json::json!({ "edit": "planned" }));
                let Some((schedule, planned_against)) = relabelled(&inputs, edit) else {
                    return ModelEditOutcome::Absent;
                };
                let outcome = log.apply(&ConsensusCommand::ReplaceDomainSchedule {
                    inputs: Box::new(inputs),
                    schedule: Some(Box::new(schedule)),
                    mutation: None,
                });
                match outcome.response {
                    crate::ConsensusResponse::Applied => {
                        ModelEditOutcome::Applied { planned_against }
                    }
                    crate::ConsensusResponse::Conflict(_) => ModelEditOutcome::Refused,
                    other => panic!("a schedule replacement applies or conflicts: {other:?}"),
                }
            }
        });
        let dropper = thread::spawn({
            let log = log.clone();
            move || {
                log.replace_schedule_until_applied(|inputs| {
                    let mut schedule = inputs
                        .schedule()
                        .verified("the dropped Model's domain keeps its schedule")
                        .clone();
                    schedule.nodes.shift_remove(&entity().node);
                    schedule
                });
                thread::yield_now();
                log.replace_schedule_until_applied(|inputs| {
                    let mut schedule = inputs
                        .schedule()
                        .verified("the recreated Model's domain keeps its schedule")
                        .clone();
                    let model = Model::Schema(CreateSchema {
                        name: SchemaName::parse("event").assured("fixture schema is valid"),
                        fields: Vec::new(),
                        labels: label_map(serde_json::json!({ "owner": "Platform" })),
                    });
                    schedule.nodes.insert(
                        entity().node,
                        ScheduledNode::new(model, SchemaFingerprint::from_digest([1; 32])),
                    );
                    schedule
                });
            }
        });
        let reader = thread::spawn({
            let log = log.clone();
            let owner = owner.clone();
            move || {
                for _ in 0..2 {
                    let capture = capture_labels(&log.published, &owner);
                    log.assert_published(&capture);
                    thread::yield_now();
                }
            }
        });
        let edited = editor.join().assured("the editor finishes without panic");
        dropper.join().assured("the dropper finishes without panic");
        reader.join().assured("the reader finishes without panic");

        let recreated = capture_labels(&log.published, &owner)
            .assured("the recreated Model exists in the final publication");
        let recreated_with_edit =
            label_map(serde_json::json!({ "owner": "Platform", "edit": "planned" }));
        let recreated_alone = label_map(serde_json::json!({ "owner": "Platform" }));
        match edited {
            ModelEditOutcome::Applied { planned_against }
                if planned_against.get("owner") == Some("Platform") =>
            {
                assert_eq!(recreated.labels(), &recreated_with_edit);
            }
            ModelEditOutcome::Applied { .. }
            | ModelEditOutcome::Refused
            | ModelEditOutcome::Absent => {
                assert_eq!(recreated.labels(), &recreated_alone);
            }
        }
    });
}
