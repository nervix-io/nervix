//! Browser drafts for entity labels: the labels a create form adds, and an edit of the labels an
//! existing entity has.
//!
//! Layer: edges.
//!
//! - **Owns.** Incomplete label rows, the captured target of an edit, the labels the edit last
//!   read, the operator's change to each of them, the conflicts a later read reveals, and the one
//!   `SET LABELS` edit a completed draft lowers to.
//! - **Depends on.** The vocabulary's label values, edits, targets and typed inspection.
//! - **Must not know.** How the inspection is requested, how the edit is dispatched, or how the
//!   server validates and stores labels.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::collections::BTreeMap;

use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    AdministrativeLabelTarget, DomainName, EntityLabelError, EntityLabelMutation, EntityLabelOwner,
    EntityLabelTarget, EntityLabelView, EntityLabels, EntityLabelsInspection, LabelAssignment,
    LabelChange, LabelEdit, LabelKey, ModelKind, ModelName, MutateEntityLabels, NodeRef,
    ResourceName, ShowLabels, Statement, UserName,
};
use strum::IntoEnumIterator as _;
use thiserror::Error;

/// Why a label draft cannot complete. Every variant names the label or input the operator fixes.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum LabelsDraftError {
    #[error("Label {label} needs a key")]
    MissingKey { label: usize },
    #[error("Label {label}: {issue}")]
    Label {
        label: usize,
        issue: EntityLabelError,
    },
    #[error("New label {label} needs a key")]
    MissingNewKey { label: usize },
    #[error("New label {label}: {issue}")]
    NewLabel {
        label: usize,
        issue: EntityLabelError,
    },
    #[error("'{key}' is already labelled: change its value in the list above")]
    AlreadyLabelled { key: String },
    #[error("{0}")]
    Labels(EntityLabelError),
    #[error("Show the entity's labels before saving")]
    NotRead,
    #[error("Nothing to save: no label was changed")]
    Unchanged,
    #[error("Another session changed labels you edited: keep your change or use the current value")]
    Conflicted,
    #[error("Choose the kind of entity whose labels to edit")]
    TargetKindRequired,
    #[error("The {kind} name is invalid")]
    TargetName { kind: &'static str },
    #[error("Select a domain before editing the labels of a domain entity")]
    TargetDomainRequired,
    #[error("The server returned the labels of another entity")]
    WrongEntity,
}

/// One label row an operator fills. Neither the key nor the value is trimmed: both are exact.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct LabelRow {
    pub(crate) key: String,
    pub(crate) value: String,
}

/// The labels a create form adds to the entity it creates, in the order the operator wrote them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CreationLabelsDraft {
    pub(crate) rows: Vec<LabelRow>,
}

impl CreationLabelsDraft {
    /// The complete set the creation statement carries. The server checks it against the
    /// committed limits; the form shows that refusal like any other.
    pub(crate) fn build(&self) -> error_stack::Result<EntityLabels, LabelsDraftError> {
        let mut assignments = Vec::with_capacity(self.rows.len());
        for (index, row) in self.rows.iter().enumerate() {
            let label = position(index);
            if row.key.is_empty() {
                return Err(Report::new(LabelsDraftError::MissingKey { label }));
            }
            let key = match LabelKey::try_from(row.key.clone()) {
                Ok(key) => key,
                Err(error) => {
                    let issue = error.current_context().clone();
                    return Err(error.change_context(LabelsDraftError::Label { label, issue }));
                }
            };
            assignments.push(LabelAssignment {
                key,
                value: row.value.clone(),
            });
        }
        EntityLabels::try_from(assignments).map_err(whole_set)
    }

    /// The draft that fills the form a completed creation was read from.
    #[cfg(test)]
    pub(crate) fn from_labels(labels: &EntityLabels) -> Self {
        let mut rows = Vec::with_capacity(labels.len());
        for (key, value) in labels.iter() {
            rows.push(LabelRow {
                key: key.as_str().to_owned(),
                value: value.to_owned(),
            });
        }
        Self { rows }
    }
}

/// The 1-based position the form shows for the label at `index`.
fn position(index: usize) -> usize {
    index
        .checked_add(1)
        .assured("an index of an in-memory row list is below usize::MAX")
}

/// Carries the vocabulary's refusal of the whole set, a repeated key or a limit, as the form's.
fn whole_set(error: Report<EntityLabelError>) -> Report<LabelsDraftError> {
    let context = LabelsDraftError::Labels(error.current_context().clone());
    error.change_context(context)
}

/// The kinds of entity whose labels the editor addresses: the administrative owners, the logical
/// resource, and every domain Model kind the vocabulary declares.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LabelTargetKind {
    Domain,
    User,
    Resource,
    Model(ModelKind),
}

impl LabelTargetKind {
    /// Every kind the editor's target choice offers, administrative owners first.
    pub(crate) fn all() -> Vec<Self> {
        let mut kinds = vec![Self::Domain, Self::User, Self::Resource];
        for kind in ModelKind::iter() {
            kinds.push(Self::Model(kind));
        }
        kinds
    }

    /// The kind of an addressed target.
    pub(crate) fn of(target: &EntityLabelTarget) -> Self {
        match target {
            EntityLabelTarget::Model(model) => Self::Model(model.kind),
            EntityLabelTarget::Resource(_) => Self::Resource,
            EntityLabelTarget::Administrative(AdministrativeLabelTarget::Domain(_)) => Self::Domain,
            EntityLabelTarget::Administrative(AdministrativeLabelTarget::User(_)) => Self::User,
        }
    }

    /// The keyword phrase Plexus addresses the kind with, which the target choice shows.
    pub(crate) fn phrase(self) -> &'static str {
        match self {
            Self::Domain => "DOMAIN",
            Self::User => "USER",
            Self::Resource => "RESOURCE",
            Self::Model(kind) => kind.keyword_phrase(),
        }
    }

    /// The value a choice control and a contextual action carry for the kind.
    pub(crate) fn key(self) -> String {
        self.phrase().to_ascii_lowercase().replace(' ', "-")
    }

    /// The kind a choice control's value names.
    pub(crate) fn from_key(key: &str) -> Option<Self> {
        let kinds = Self::all();
        // Bounded by the fixed number of label target kinds the vocabulary declares.
        kinds.into_iter().find(|kind| kind.key() == key)
    }

    /// Whether the kind's entities resolve in a domain: Models and resources do, while a domain or
    /// a user is named by its own identity.
    pub(crate) fn domain_scoped(self) -> bool {
        matches!(self, Self::Resource | Self::Model(_))
    }

    fn name_label(self) -> &'static str {
        match self {
            Self::Domain => "domain",
            Self::User => "user",
            Self::Resource => "resource",
            Self::Model(_) => "entity",
        }
    }

    /// The target `name` addresses in this kind.
    fn target(self, name: &str) -> error_stack::Result<EntityLabelTarget, LabelsDraftError> {
        let invalid = || LabelsDraftError::TargetName {
            kind: self.name_label(),
        };
        let target = match self {
            Self::Domain => EntityLabelTarget::Administrative(AdministrativeLabelTarget::Domain(
                DomainName::parse(name).change_context_lazy(invalid)?,
            )),
            Self::User => EntityLabelTarget::Administrative(AdministrativeLabelTarget::User(
                UserName::parse(name).change_context_lazy(invalid)?,
            )),
            Self::Resource => {
                EntityLabelTarget::Resource(ResourceName::parse(name).change_context_lazy(invalid)?)
            }
            Self::Model(kind) => EntityLabelTarget::Model(NodeRef::new(
                kind,
                ModelName::parse(name).change_context_lazy(invalid)?,
            )),
        };
        Ok(target)
    }
}

/// The entity an edit addresses, captured when the editor opened or when its target was chosen.
/// A Model or resource keeps the domain it resolves in, whatever domain the console selects later.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LabelTarget {
    domain: Option<DomainName>,
    entity: EntityLabelTarget,
}

impl LabelTarget {
    /// The target of `entity`, resolved in `domain` when its kind resolves in one.
    pub(crate) fn new(
        domain: Option<DomainName>,
        entity: EntityLabelTarget,
    ) -> error_stack::Result<Self, LabelsDraftError> {
        if entity.requires_selected_domain() {
            let Some(domain) = domain else {
                return Err(Report::new(LabelsDraftError::TargetDomainRequired));
            };
            return Ok(Self {
                domain: Some(domain),
                entity,
            });
        }
        Ok(Self {
            domain: None,
            entity,
        })
    }

    /// The domain a command about the target selects, for a target that resolves in one.
    pub(crate) fn domain(&self) -> Option<&DomainName> {
        self.domain.as_ref()
    }

    pub(crate) fn entity(&self) -> &EntityLabelTarget {
        &self.entity
    }

    /// The fully addressed owner, as the inspection names it.
    pub(crate) fn owner(&self) -> Option<EntityLabelOwner> {
        self.entity.resolve(self.domain.as_ref())
    }

    /// The canonical statement that reads the target's labels.
    pub(crate) fn show_statement(&self) -> String {
        Statement::ShowLabels(ShowLabels {
            entity: self.entity.clone(),
            format: Default::default(),
        })
        .to_canonical_plexus()
        .assured("a label inspection of a typed target renders without a fallible clause")
    }
}

/// What the operator does to one label the last read returned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReadLabelChange {
    /// Leaves the label as it was read.
    Keep,
    /// Sets the label to this exact value. A value equal to the read one changes nothing.
    Value(String),
    /// Removes the label.
    Drop,
}

/// What a later read found for a label the operator had changed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LabelConflict {
    /// Another change set the label to this value.
    Changed { now: String },
    /// Another change removed the label.
    Removed,
}

/// One label the last read returned, with the operator's change to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReadLabel {
    /// The value of the read the change was made against.
    pub(crate) read: String,
    pub(crate) change: ReadLabelChange,
    /// Set when a later read found another value under the operator's change; saving waits until
    /// the operator decides.
    pub(crate) conflict: Option<LabelConflict>,
}

impl ReadLabel {
    fn kept(read: String) -> Self {
        Self {
            read,
            change: ReadLabelChange::Keep,
            conflict: None,
        }
    }

    /// The value the operator sees in the label's field.
    pub(crate) fn shown_value(&self) -> &str {
        match &self.change {
            ReadLabelChange::Value(value) => value,
            ReadLabelChange::Keep | ReadLabelChange::Drop => &self.read,
        }
    }

    /// The change the edit carries for the label, if it changes it.
    fn edit(&self) -> Option<LabelChange> {
        match &self.change {
            ReadLabelChange::Keep => None,
            ReadLabelChange::Value(value) if *value == self.read => None,
            ReadLabelChange::Value(value) => Some(LabelChange::Set(value.clone())),
            ReadLabelChange::Drop => Some(LabelChange::Drop),
        }
    }
}

/// A label the operator adds, with what a later read found under its key.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct AddedLabel {
    pub(crate) row: LabelRow,
    /// The value another change gave this key after the operator started adding it.
    pub(crate) exists: Option<String>,
}

/// The state of the editor's read of its target's labels.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LabelRead {
    /// No target has been read yet.
    Idle,
    /// The read numbered `sequence` is on its way; an answer to any earlier read is ignored.
    Reading { sequence: u64 },
    /// The typed result of the latest read.
    Read(Box<EntityLabelsInspection>),
    /// The latest read was refused; the message says why.
    Refused(String),
}

/// An edit of the labels of one existing entity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LabelEditDraft {
    /// The kind the target choice shows.
    pub(crate) chosen_kind: Option<LabelTargetKind>,
    /// The name the target choice shows.
    pub(crate) chosen_name: String,
    /// The target the labels below belong to.
    target: Option<LabelTarget>,
    read: LabelRead,
    /// The labels of the latest read, by key, with the operator's change to each.
    labels: BTreeMap<LabelKey, ReadLabel>,
    /// The labels the operator adds, in the order written.
    pub(crate) added: Vec<AddedLabel>,
    /// The number the next read carries.
    next_read: u64,
}

impl Default for LabelEditDraft {
    fn default() -> Self {
        Self {
            chosen_kind: None,
            chosen_name: String::new(),
            target: None,
            read: LabelRead::Idle,
            labels: BTreeMap::new(),
            added: Vec::new(),
            next_read: 0,
        }
    }
}

impl LabelEditDraft {
    /// A choice of target that reads nothing until the operator asks, prefilled with `kind` and
    /// `name`.
    pub(crate) fn choosing(kind: LabelTargetKind, name: String) -> Self {
        Self {
            chosen_kind: Some(kind),
            chosen_name: name,
            ..Self::default()
        }
    }

    pub(crate) fn target(&self) -> Option<&LabelTarget> {
        self.target.as_ref()
    }

    pub(crate) fn read(&self) -> &LabelRead {
        &self.read
    }

    pub(crate) fn labels(&self) -> &BTreeMap<LabelKey, ReadLabel> {
        &self.labels
    }

    /// The target the choice names, resolved in `domain` when its kind resolves in one.
    pub(crate) fn chosen_target(
        &self,
        domain: Option<DomainName>,
    ) -> error_stack::Result<LabelTarget, LabelsDraftError> {
        let kind = self
            .chosen_kind
            .ok_or_else(|| Report::new(LabelsDraftError::TargetKindRequired))?;
        let entity = kind.target(&self.chosen_name)?;
        LabelTarget::new(domain, entity)
    }

    /// Starts reading `target` and returns the number of the read. A different target starts a
    /// new edit; the same target keeps the operator's changes, which the read is compared with
    /// when it arrives.
    pub(crate) fn begin_read(&mut self, target: LabelTarget) -> u64 {
        if self.target.as_ref() != Some(&target) {
            self.labels.clear();
            self.added.clear();
        }
        self.chosen_kind = Some(LabelTargetKind::of(target.entity()));
        self.chosen_name = target_name(target.entity());
        self.target = Some(target);
        let sequence = self.next_read;
        self.next_read = sequence
            .checked_add(1)
            .assured("one page issues fewer than u64::MAX label reads");
        self.read = LabelRead::Reading { sequence };
        sequence
    }

    /// Takes the typed answer to the read numbered `sequence`. An answer to an earlier read is
    /// ignored, and an answer about another entity than the target is refused.
    pub(crate) fn read_completed(&mut self, sequence: u64, inspection: EntityLabelsInspection) {
        if self.read != (LabelRead::Reading { sequence }) {
            return;
        }
        let expected = match &self.target {
            Some(target) => target.owner(),
            None => None,
        };
        if expected.as_ref() != Some(inspection.entity()) {
            self.read = LabelRead::Refused(LabelsDraftError::WrongEntity.to_string());
            return;
        }
        let read = inspection.labels().clone();
        self.reconcile(&read);
        self.read = LabelRead::Read(Box::new(inspection));
    }

    /// Takes the refusal of the read numbered `sequence`.
    pub(crate) fn read_refused(&mut self, sequence: u64, reason: String) {
        if self.read != (LabelRead::Reading { sequence }) {
            return;
        }
        self.read = LabelRead::Refused(reason);
    }

    /// A save the server completed or queued leaves nothing to save. The labels stay as shown
    /// until the next read reports the result.
    pub(crate) fn saved(&mut self) {
        for label in self.labels.values_mut() {
            label.change = ReadLabelChange::Keep;
            label.conflict = None;
        }
        self.added.clear();
    }

    /// Compares a new read with the operator's changes. A label the operator left alone follows
    /// the read; a label the operator changed keeps the change and is marked when the read found
    /// another value, so the change never silently overwrites what another session wrote.
    fn reconcile(&mut self, read: &EntityLabels) {
        let previous = std::mem::take(&mut self.labels);
        for (key, label) in previous {
            let now = read.get(key.as_str());
            let changed = label.edit().is_some();
            let sets = matches!(label.change, ReadLabelChange::Value(_));
            let reconciled = match now {
                Some(now) if now == label.read => Some(label),
                Some(now) if !changed => Some(ReadLabel::kept(now.to_owned())),
                Some(now) => Some(ReadLabel {
                    conflict: Some(LabelConflict::Changed {
                        now: now.to_owned(),
                    }),
                    ..label
                }),
                // A label someone else removed is gone unless the operator was setting it.
                None if changed && sets => Some(ReadLabel {
                    conflict: Some(LabelConflict::Removed),
                    ..label
                }),
                None => None,
            };
            if let Some(reconciled) = reconciled {
                self.labels.insert(key, reconciled);
            }
        }
        for (key, value) in read.iter() {
            if !self.labels.contains_key(key.as_str()) {
                self.labels
                    .insert(key.clone(), ReadLabel::kept(value.to_owned()));
            }
        }
        for added in &mut self.added {
            added.exists = read.get(&added.row.key).map(str::to_owned);
        }
    }

    /// Whether the operator has changed anything a save would send or a reread would compare.
    pub(crate) fn has_changes(&self) -> bool {
        let changed = self
            .labels
            .values()
            .any(|label| label.edit().is_some() || label.conflict.is_some());
        changed || !self.added.is_empty()
    }

    /// Whether any label waits for the operator to resolve what another session changed.
    pub(crate) fn has_conflict(&self) -> bool {
        let read_conflict = self.labels.values().any(|label| label.conflict.is_some());
        let added_conflict = self.added.iter().any(|added| added.exists.is_some());
        read_conflict || added_conflict
    }

    pub(crate) fn set_value(&mut self, key: &str, value: String) {
        if let Some(label) = self.labels.get_mut(key) {
            label.change = ReadLabelChange::Value(value);
        }
    }

    pub(crate) fn remove(&mut self, key: &str) {
        if let Some(label) = self.labels.get_mut(key) {
            label.change = ReadLabelChange::Drop;
        }
    }

    pub(crate) fn restore(&mut self, key: &str) {
        if let Some(label) = self.labels.get_mut(key) {
            label.change = ReadLabelChange::Keep;
        }
    }

    /// Keeps the operator's change to a label another session changed, now made against the
    /// value that session wrote.
    pub(crate) fn keep_change(&mut self, key: &str) {
        let Some(label) = self.labels.get_mut(key) else {
            return;
        };
        match label.conflict.take() {
            Some(LabelConflict::Changed { now }) => label.read = now,
            Some(LabelConflict::Removed) => {
                let value = label.shown_value().to_owned();
                self.labels.remove(key);
                self.added.push(AddedLabel {
                    row: LabelRow {
                        key: key.to_owned(),
                        value,
                    },
                    exists: None,
                });
            }
            None => {}
        }
    }

    /// Drops the operator's change to a label another session changed, taking that value.
    pub(crate) fn use_current(&mut self, key: &str) {
        let Some(label) = self.labels.get_mut(key) else {
            return;
        };
        match label.conflict.take() {
            Some(LabelConflict::Changed { now }) => *label = ReadLabel::kept(now),
            Some(LabelConflict::Removed) => {
                self.labels.remove(key);
            }
            None => {}
        }
    }

    /// Turns a label being added under a key another session gave a value into an edit of that
    /// label: `keep` keeps the operator's value, otherwise the current one stays.
    pub(crate) fn resolve_added(&mut self, index: usize, keep: bool) {
        let Some(added) = self.added.get(index) else {
            return;
        };
        let Some(now) = added.exists.clone() else {
            return;
        };
        let Ok(key) = LabelKey::try_from(added.row.key.clone()) else {
            return;
        };
        let change = if keep {
            ReadLabelChange::Value(added.row.value.clone())
        } else {
            ReadLabelChange::Keep
        };
        self.added.remove(index);
        self.labels.insert(
            key,
            ReadLabel {
                read: now,
                change,
                conflict: None,
            },
        );
    }

    /// The one edit the draft saves: the labels it sets or changes and the labels it removes,
    /// checked as a complete result against the limits its read captured.
    pub(crate) fn edit(&self) -> error_stack::Result<EntityLabelMutation, LabelsDraftError> {
        let LabelRead::Read(inspection) = &self.read else {
            return Err(Report::new(LabelsDraftError::NotRead));
        };
        if self.has_conflict() {
            return Err(Report::new(LabelsDraftError::Conflicted));
        }
        let mut edits = Vec::new();
        for (key, label) in &self.labels {
            if let Some(change) = label.edit() {
                edits.push(LabelEdit {
                    key: key.clone(),
                    change,
                });
            }
        }
        for (index, added) in self.added.iter().enumerate() {
            let label = position(index);
            if added.row.key.is_empty() {
                return Err(Report::new(LabelsDraftError::MissingNewKey { label }));
            }
            let key = match LabelKey::try_from(added.row.key.clone()) {
                Ok(key) => key,
                Err(error) => {
                    let issue = error.current_context().clone();
                    return Err(error.change_context(LabelsDraftError::NewLabel { label, issue }));
                }
            };
            if self.labels.contains_key(key.as_str()) {
                return Err(Report::new(LabelsDraftError::AlreadyLabelled {
                    key: key.as_str().to_owned(),
                }));
            }
            edits.push(LabelEdit {
                key,
                change: LabelChange::Set(added.row.value.clone()),
            });
        }
        let mutation = EntityLabelMutation::try_from(edits).map_err(whole_set)?;
        if mutation.is_empty() {
            return Err(Report::new(LabelsDraftError::Unchanged));
        }
        inspection
            .labels()
            .updated(&mutation, &inspection.policy().limits)
            .map_err(whole_set)?;
        Ok(mutation)
    }

    /// The canonical statement the draft saves, for the target its labels were read from.
    pub(crate) fn statement(&self) -> error_stack::Result<Statement, LabelsDraftError> {
        let target = self
            .target
            .as_ref()
            .ok_or_else(|| Report::new(LabelsDraftError::NotRead))?;
        let mutation = self.edit()?;
        Ok(Statement::MutateEntityLabels(MutateEntityLabels {
            entity: target.entity().clone(),
            mutation,
        }))
    }

    /// The view the latest read describes: the committed revision or a staged transaction.
    pub(crate) fn view(&self) -> Option<&EntityLabelView> {
        match &self.read {
            LabelRead::Read(inspection) => Some(inspection.view()),
            LabelRead::Idle | LabelRead::Reading { .. } | LabelRead::Refused(_) => None,
        }
    }
}

/// The name an addressed target carries, as the target choice shows it.
fn target_name(target: &EntityLabelTarget) -> String {
    match target {
        EntityLabelTarget::Model(model) => model.identifier.as_str().to_owned(),
        EntityLabelTarget::Resource(resource) => resource.as_str().to_owned(),
        EntityLabelTarget::Administrative(AdministrativeLabelTarget::Domain(domain)) => {
            domain.as_str().to_owned()
        }
        EntityLabelTarget::Administrative(AdministrativeLabelTarget::User(user)) => {
            user.as_str().to_owned()
        }
    }
}
