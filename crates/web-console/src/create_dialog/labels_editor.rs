//! Label controls of the create dialog: the labels a create form adds to the entity it creates, and
//! the editor of the labels an existing entity has.
//!
//! Layer: edges.
//!
//! - **Owns.** Browser presentation of label rows, the editor's target choice, the labels its read
//!   returned with each label's change and conflict, and the creation labels each form keeps.
//! - **Depends on.** The label drafts, the create dialog's signals and its durable submission.
//! - **Must not know.** How a read is requested or answered, or how the server stores labels.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use error_stack::Report;
use leptos::prelude::*;
use nervix_models::{DomainName, EntityLabelView, EntityLabels, EntityLabelsInspection, LabelKey};

use super::{
    CommandDispatch, CreateDispatch, CreateDraftError, CreateKind, CreateSignals, CreateSubmission,
    event_target_textarea_value, event_target_value,
    labels_draft::{
        CreationLabelsDraft, LabelConflict, LabelEditDraft, LabelRead, LabelRow, LabelTarget,
        LabelTargetKind, LabelsDraftError, ReadLabelChange,
    },
};

/// An entity the console knows by name, offered as a target the editor can choose.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KnownLabelTarget {
    pub(crate) kind: LabelTargetKind,
    pub(crate) name: String,
}

/// The answer to the console's read of a target's labels.
pub(crate) enum LabelReadResponse {
    /// The typed inspection the read returned.
    Inspection(Box<EntityLabelsInspection>),
    /// The read was refused or its answer carried no inspection; the message says why.
    Refused(String),
}

impl CreateSignals {
    /// The row at `index` of the creation labels of the open form.
    fn creation_label(self, index: usize) -> LabelRow {
        let Some(kind) = self.open.get() else {
            return LabelRow::default();
        };
        self.creation_labels.with(|drafts| {
            let Some(draft) = drafts.get(&kind) else {
                return LabelRow::default();
            };
            match draft.rows.get(index) {
                Some(row) => row.clone(),
                None => LabelRow::default(),
            }
        })
    }

    /// How many label rows the open form shows.
    fn creation_label_count(self) -> usize {
        let Some(kind) = self.open.get() else {
            return 0;
        };
        self.creation_labels.with(|drafts| match drafts.get(&kind) {
            Some(draft) => draft.rows.len(),
            None => 0,
        })
    }

    /// Changes the creation labels of the open form, which edits its draft.
    fn edit_creation_labels(self, change: impl FnOnce(&mut CreationLabelsDraft)) {
        let Some(kind) = self.open.get_untracked() else {
            return;
        };
        self.creation_labels
            .update(|drafts| change(drafts.entry(kind).or_default()));
        self.edit();
    }

    /// The labels the form of `kind` adds to the entity it creates.
    pub(super) fn creation_labels_of(
        self,
        kind: CreateKind,
    ) -> error_stack::Result<EntityLabels, CreateDraftError> {
        self.creation_labels
            .with_untracked(|drafts| match drafts.get(&kind) {
                Some(draft) => draft.build().map_err(super::draft_error),
                None => Ok(EntityLabels::default()),
            })
    }

    /// Opens the label editor on `target` and returns the number of the read of its labels. A
    /// Model or resource the operator chooses later resolves in `scope`.
    pub(crate) fn open_label_edit(
        self,
        scope: Option<DomainName>,
        target: LabelTarget,
        return_focus: &str,
    ) -> Option<u64> {
        if self.reopen_pending(return_focus) {
            return None;
        }
        let kind = CreateKind::EntityLabels;
        self.captured_scopes.update(|scopes| {
            scopes.insert(kind, scope.clone());
        });
        self.open(kind, scope, return_focus);
        let mut sequence = 0;
        self.label_edit
            .update(|draft| sequence = draft.begin_read(target));
        self.advance_revision();
        Some(sequence)
    }

    /// Reopens a label edit the operator left with unsaved changes, on the entity and in the
    /// domain it was captured for, and returns whether there was one to reopen.
    pub(crate) fn reopen_label_edit(self, return_focus: &str) -> bool {
        if self.reopen_pending(return_focus) {
            return true;
        }
        let retained = self
            .label_edit
            .with_untracked(|draft| draft.target().is_some() && draft.has_changes());
        if !retained {
            return false;
        }
        let kind = CreateKind::EntityLabels;
        let scope = match self.captured_scopes.get_untracked().get(&kind) {
            Some(scope) => scope.clone(),
            None => None,
        };
        self.open(kind, scope, return_focus);
        true
    }

    /// Opens the label editor on the choice of a target in `domain`, prefilled with `kind` and
    /// `name`. Nothing is read until the operator asks.
    pub(crate) fn open_label_choice(
        self,
        domain: Option<DomainName>,
        kind: LabelTargetKind,
        name: String,
        return_focus: &str,
    ) {
        if self.reopen_pending(return_focus) {
            return;
        }
        let create_kind = CreateKind::EntityLabels;
        self.captured_scopes.update(|scopes| {
            scopes.insert(create_kind, domain.clone());
        });
        self.open(create_kind, domain, return_focus);
        self.label_edit.set(LabelEditDraft::choosing(kind, name));
        self.advance_revision();
    }

    /// Starts a read of `target`'s labels in the open editor and returns its number.
    pub(crate) fn begin_label_read(self, target: LabelTarget) -> u64 {
        let mut sequence = 0;
        self.label_edit
            .update(|draft| sequence = draft.begin_read(target));
        self.advance_revision();
        sequence
    }

    /// Takes the answer to the read numbered `sequence`.
    pub(crate) fn label_read_answered(self, sequence: u64, response: LabelReadResponse) {
        self.label_edit.update(|draft| match response {
            LabelReadResponse::Inspection(inspection) => {
                draft.read_completed(sequence, *inspection)
            }
            LabelReadResponse::Refused(reason) => draft.read_refused(sequence, reason),
        });
        // The canonical preview depends on what the read returned.
        self.advance_revision();
    }

    /// Records that the server completed or queued the editor's save and returns the target to
    /// read again, so the editor shows the result.
    pub(crate) fn label_edit_saved(self) -> Option<LabelTarget> {
        let mut target = None;
        self.label_edit.update(|draft| {
            draft.saved();
            target = draft.target().cloned();
        });
        self.advance_revision();
        target
    }

    /// The editor's save, for the target its labels were read from.
    pub(super) fn label_edit_submission(
        self,
    ) -> error_stack::Result<CreateSubmission, CreateDraftError> {
        self.label_edit.with_untracked(|draft| {
            let target = draft
                .target()
                .ok_or_else(|| Report::new(CreateDraftError::Labels(LabelsDraftError::NotRead)))?;
            let statement = draft.statement().map_err(super::draft_error)?;
            let query = {
                let rendered = statement.to_canonical_plexus();
                match rendered {
                    Ok(query) => query,
                    Err(error) => {
                        return Err(error.change_context(CreateDraftError::CanonicalPlexus));
                    }
                }
            };
            Ok(CreateSubmission {
                kind: CreateKind::EntityLabels,
                presentation: query.clone(),
                dispatch: CreateDispatch::Command(CommandDispatch {
                    query,
                    domain: target.domain().cloned(),
                    resource: None,
                    created_domain: None,
                }),
            })
        })
    }
}

/// The labels a create form adds to the entity it creates.
#[component]
pub(super) fn CreationLabelsEditor(signals: CreateSignals) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    view! {
        <Show when=move || signals.open.get().is_some_and(CreateKind::takes_labels) fallback=|| ()>
            <fieldset class="create-labels">
                <legend>"Labels"</legend>
                <p class="create-labels-note">"Descriptive keys and values stored with the entity. Labels grant no permissions."</p>
                <For each=move || { (0..signals.creation_label_count()).collect::<Vec<_>>() }
                    key=|index| *index
                    children=move |index| view! {
                        <div class="create-field-row create-label-row" data-row=index.to_string()>
                            <label class="create-field"><span>{format!("Label {} key", index + 1)}</span>
                                <input class="create-label-key" type="text" autocomplete="off" spellcheck="false"
                                    prop:value=move || signals.creation_label(index).key
                                    disabled=pending
                                    on:input=move |event| {
                                        let key = event_target_value(&event);
                                        signals.edit_creation_labels(|draft| {
                                            if let Some(row) = draft.rows.get_mut(index) { row.key = key; }
                                        });
                                    } />
                            </label>
                            <label class="create-field"><span>{format!("Label {} value", index + 1)}</span>
                                <textarea class="create-label-value" rows="1" spellcheck="false"
                                    prop:value=move || signals.creation_label(index).value
                                    disabled=pending
                                    on:input=move |event| {
                                        let value = event_target_textarea_value(&event);
                                        signals.edit_creation_labels(|draft| {
                                            if let Some(row) = draft.rows.get_mut(index) { row.value = value; }
                                        });
                                    }></textarea>
                            </label>
                            <button type="button" class="create-label-remove" disabled=pending
                                aria-label=format!("Remove label {}", index + 1)
                                on:click=move |_| signals.edit_creation_labels(|draft| {
                                    if index < draft.rows.len() { draft.rows.remove(index); }
                                })>"Remove"</button>
                        </div>
                    } />
                <button type="button" class="create-label-add" disabled=pending
                    on:click=move |_| signals.edit_creation_labels(|draft| draft.rows.push(LabelRow::default()))>"Add label"</button>
            </fieldset>
        </Show>
    }
    .into_any()
}

/// The editor of an existing entity's labels: the target, the labels its latest read returned with
/// the operator's change to each, and the labels the operator adds.
#[component]
pub(super) fn LabelEditEditor(
    signals: CreateSignals,
    name_input: NodeRef<leptos::html::Input>,
    read_labels: Callback<LabelTarget>,
    known_targets: Signal<Vec<KnownLabelTarget>>,
) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    let show = move || {
        let domain = signals.captured_domain.get_untracked();
        let target = signals
            .label_edit
            .with_untracked(|draft| draft.chosen_target(domain));
        match target {
            Ok(target) => {
                signals.validation.set(None);
                read_labels.run(target);
            }
            Err(error) => signals
                .validation
                .set(Some(error.current_context().to_string())),
        }
    };
    let reload = move || {
        let target = signals
            .label_edit
            .with_untracked(|draft| draft.target().cloned());
        if let Some(target) = target {
            read_labels.run(target);
        }
    };
    let names = move || {
        let chosen = signals.label_edit.with(|draft| draft.chosen_kind);
        let mut names = Vec::new();
        for known in known_targets.get() {
            if Some(known.kind) == chosen {
                names.push(known.name);
            }
        }
        names
    };
    let reading = move || {
        signals
            .label_edit
            .with(|draft| matches!(draft.read(), LabelRead::Reading { .. }))
    };
    let refusal = move || {
        signals.label_edit.with(|draft| match draft.read() {
            LabelRead::Refused(reason) => Some(reason.clone()),
            LabelRead::Idle | LabelRead::Reading { .. } | LabelRead::Read(_) => None,
        })
    };
    let read = move || {
        signals
            .label_edit
            .with(|draft| matches!(draft.read(), LabelRead::Read(_)))
    };
    let owner = move || {
        signals.label_edit.with(|draft| match draft.target() {
            Some(target) => match target.owner() {
                Some(owner) => owner.to_string(),
                None => String::new(),
            },
            None => String::new(),
        })
    };
    let view_text = move || {
        signals.label_edit.with(|draft| match draft.view() {
            Some(EntityLabelView::Committed { revision }) => {
                format!("Committed · revision {revision}")
            }
            Some(EntityLabelView::Staged { preview }) => format!(
                "Staged in transaction {} · not committed yet",
                preview.transaction_id
            ),
            None => String::new(),
        })
    };
    let keys = move || {
        signals
            .label_edit
            .with(|draft| draft.labels().keys().cloned().collect::<Vec<_>>())
    };
    view! {
        <div class="labels-target">
            <label class="create-field"><span>"Entity kind"</span>
                <select class="labels-target-kind" disabled=pending
                    prop:value=move || match signals.label_edit.get().chosen_kind {
                        Some(kind) => kind.key(),
                        None => String::new(),
                    }
                    on:change=move |event| {
                        let key = event_target_value(&event);
                        signals.label_edit.update(|draft| draft.chosen_kind = LabelTargetKind::from_key(&key));
                        signals.edit();
                    }>
                    <option value="">"Choose a kind"</option>
                    {LabelTargetKind::all()
                        .into_iter()
                        .map(|kind| view! { <option value=kind.key()>{kind.phrase()}</option> })
                        .collect_view()}
                </select>
            </label>
            <label class="create-field"><span>"Name"</span>
                <input node_ref=name_input class="labels-target-name create-name" type="text"
                    autocomplete="off" spellcheck="false" list="labels-target-names" disabled=pending
                    prop:value=move || signals.label_edit.get().chosen_name
                    on:input=move |event| {
                        let name = event_target_value(&event);
                        signals.label_edit.update(|draft| draft.chosen_name = name);
                        signals.edit();
                    } />
            </label>
            <datalist id="labels-target-names">
                {move || names().into_iter().map(|name| view! { <option value=name /> }).collect_view()}
            </datalist>
            <button type="button" class="labels-target-show" disabled=pending
                on:click=move |_| show()>"Show labels"</button>
        </div>
        <Show when=move || !owner().is_empty() fallback=|| ()>
            <p class="labels-owner">{owner}</p>
        </Show>
        <Show when=reading fallback=|| ()>
            <p class="labels-reading" aria-live="polite">"Reading labels…"</p>
        </Show>
        <Show when=move || refusal().is_some() fallback=|| ()>
            <p class="labels-read-error" role="alert">{move || refusal().unwrap_or_default()}</p>
        </Show>
        <Show when=read fallback=|| ()>
            <p class="labels-view" aria-live="polite">{view_text}</p>
            <div class="labels-list" role="list" aria-label="Labels">
                <For each=keys key=|key| key.clone()
                    children=move |label_key| view! { <ReadLabelEntry signals=signals label_key=label_key /> } />
                <Show when=move || signals.label_edit.with(|draft| draft.labels().is_empty()) fallback=|| ()>
                    <p class="labels-empty">"No labels"</p>
                </Show>
            </div>
            <AddedLabelsEditor signals=signals />
            <div class="labels-actions">
                <button type="button" class="labels-new-add" disabled=pending
                    on:click=move |_| {
                        signals.label_edit.update(|draft| draft.added.push(Default::default()));
                        signals.edit();
                    }>"Add label"</button>
                <button type="button" class="labels-reload" disabled=pending
                    on:click=move |_| reload()>"Reload"</button>
            </div>
        </Show>
    }
    .into_any()
}

/// One label the latest read returned: its value, its removal and any conflict with another
/// session's change.
#[component]
fn ReadLabelEntry(signals: CreateSignals, label_key: LabelKey) -> impl IntoView {
    // A stored handle keeps every closure below `Copy`, as the views that re-render them need.
    let key = StoredValue::new(label_key.as_str().to_owned());
    let pending = move || signals.progress.get().is_pending();
    let label = move || {
        key.with_value(|key| {
            signals
                .label_edit
                .with(|draft| draft.labels().get(key.as_str()).cloned())
        })
    };
    let state = move || match label() {
        Some(label) if label.conflict.is_some() => "conflict",
        Some(label) => match label.change {
            ReadLabelChange::Keep => "kept",
            ReadLabelChange::Value(value) if value == label.read => "kept",
            ReadLabelChange::Value(_) => "changed",
            ReadLabelChange::Drop => "removed",
        },
        None => "kept",
    };
    let removed = move || label().is_some_and(|label| label.change == ReadLabelChange::Drop);
    let conflict = move || match label().and_then(|label| label.conflict) {
        Some(LabelConflict::Changed { now }) => {
            Some(format!("Another session changed this label to '{now}'"))
        }
        Some(LabelConflict::Removed) => Some("Another session removed this label".to_owned()),
        None => None,
    };
    let value = move || match label() {
        Some(label) => label.shown_value().to_owned(),
        None => String::new(),
    };
    let set_value = move |event: leptos::ev::Event| {
        let value = event_target_textarea_value(&event);
        key.with_value(|key| {
            signals
                .label_edit
                .update(|draft| draft.set_value(key, value))
        });
        signals.edit();
    };
    let toggle = move |_| {
        let was_removed = removed();
        key.with_value(|key| {
            signals.label_edit.update(|draft| {
                if was_removed {
                    draft.restore(key);
                } else {
                    draft.remove(key);
                }
            });
        });
        signals.edit();
    };
    let keep_change = move |_| {
        key.with_value(|key| signals.label_edit.update(|draft| draft.keep_change(key)));
        signals.edit();
    };
    let use_current = move |_| {
        key.with_value(|key| signals.label_edit.update(|draft| draft.use_current(key)));
        signals.edit();
    };
    let toggle_label = move || {
        let key = key.get_value();
        if removed() {
            format!("Keep label {key}")
        } else {
            format!("Remove label {key}")
        }
    };
    let toggle_text = move || if removed() { "Undo remove" } else { "Remove" };
    let key_text = key.get_value();
    let value_label = format!("Value of label {key_text}");
    let data_key = key_text.clone();
    view! {
        <div class="labels-entry" role="listitem" data-key=data_key data-state=state>
            <span class="labels-entry-key">{key_text}</span>
            <textarea class="labels-entry-value" rows="1" spellcheck="false" aria-label=value_label
                prop:value=value
                disabled=move || pending() || removed()
                on:input=set_value></textarea>
            <button type="button" class="labels-entry-remove" disabled=pending aria-label=toggle_label
                on:click=toggle>{toggle_text}</button>
            <Show when=move || conflict().is_some() fallback=|| ()>
                <p class="labels-entry-conflict" role="alert">{move || conflict().unwrap_or_default()}</p>
                <button type="button" class="labels-entry-keep" disabled=pending on:click=keep_change>"Keep my change"</button>
                <button type="button" class="labels-entry-current" disabled=pending on:click=use_current>"Use current"</button>
            </Show>
        </div>
    }
    .into_any()
}

/// The labels the operator adds to the entity, and any key another session labelled meanwhile.
#[component]
fn AddedLabelsEditor(signals: CreateSignals) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    let added = move |index: usize| {
        signals
            .label_edit
            .with(|draft| match draft.added.get(index) {
                Some(added) => added.clone(),
                None => Default::default(),
            })
    };
    view! {
        <For each=move || { (0..signals.label_edit.with(|draft| draft.added.len())).collect::<Vec<_>>() }
            key=|index| *index
            children=move |index| view! {
                <div class="create-field-row labels-new-row" data-row=index.to_string()>
                    <label class="create-field"><span>{format!("New label {} key", index + 1)}</span>
                        <input class="labels-new-key" type="text" autocomplete="off" spellcheck="false"
                            prop:value=move || added(index).row.key
                            disabled=pending
                            on:input=move |event| {
                                let key = event_target_value(&event);
                                signals.label_edit.update(|draft| {
                                    if let Some(added) = draft.added.get_mut(index) { added.row.key = key; added.exists = None; }
                                });
                                signals.edit();
                            } />
                    </label>
                    <label class="create-field"><span>{format!("New label {} value", index + 1)}</span>
                        <textarea class="labels-new-value" rows="1" spellcheck="false"
                            prop:value=move || added(index).row.value
                            disabled=pending
                            on:input=move |event| {
                                let value = event_target_textarea_value(&event);
                                signals.label_edit.update(|draft| {
                                    if let Some(added) = draft.added.get_mut(index) { added.row.value = value; }
                                });
                                signals.edit();
                            }></textarea>
                    </label>
                    <button type="button" class="labels-new-remove" disabled=pending
                        aria-label=format!("Remove new label {}", index + 1)
                        on:click=move |_| {
                            signals.label_edit.update(|draft| {
                                if index < draft.added.len() { draft.added.remove(index); }
                            });
                            signals.edit();
                        }>"Remove"</button>
                    <Show when=move || added(index).exists.is_some() fallback=|| ()>
                        <p class="labels-new-conflict" role="alert">{move || format!(
                            "Another session labelled this key with '{}'",
                            added(index).exists.unwrap_or_default()
                        )}</p>
                        <button type="button" class="labels-new-keep" disabled=pending
                            on:click=move |_| {
                                signals.label_edit.update(|draft| draft.resolve_added(index, true));
                                signals.edit();
                            }>"Keep my value"</button>
                        <button type="button" class="labels-new-current" disabled=pending
                            on:click=move |_| {
                                signals.label_edit.update(|draft| draft.resolve_added(index, false));
                                signals.edit();
                            }>"Use current"</button>
                    </Show>
                </div>
            } />
    }
    .into_any()
}
