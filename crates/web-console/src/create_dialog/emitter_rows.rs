//! Reusable scalar and ordered-row controls for emitter drafts.
//!
//! Layer: edges.
//! - **Owns.** Browser editing of text, optional literals, mapping order and assignment rows.
//! - **Depends on.** The emitter draft and Leptos signals.
//! - **Must not know.** Connector validation, session transport or external records.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns emitter field controls")
)]

use leptos::prelude::*;

use super::{
    CreateSignals,
    emitter_draft::EmitterDraft,
    emitter_envelope_draft::ValueDraft,
    event_target_checked, event_target_textarea_value, event_target_value,
    ingestor_route_draft::{AssignmentDraft, MessageErrorDraft},
    processor_draft::StatePolicyDraft,
};

#[component]
pub(super) fn EmitterText(
    label: &'static str,
    class_name: &'static str,
    signals: CreateSignals,
    read: fn(&EmitterDraft) -> String,
    write: fn(&mut EmitterDraft, String),
    #[prop(optional)] multiline: bool,
) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    let edit = move |value| {
        signals.emitter.update(|draft| write(draft, value));
        signals.edit();
    };
    view! {
        <label class="create-field"><span>{label}</span>
            <Show when=move || multiline fallback=move || view! {
                <input type="text" class=class_name autocomplete="off" prop:value=move || signals.emitter.with(read)
                    disabled=pending on:input=move |event| edit(event_target_value(&event)) />
            }>
                <textarea class=class_name prop:value=move || signals.emitter.with(read)
                    disabled=pending on:input=move |event| edit(event_target_textarea_value(&event)) />
            </Show>
        </label>
    }
}

#[component]
pub(super) fn EmitterOptionalText(
    label: &'static str,
    class_name: &'static str,
    signals: CreateSignals,
    read: fn(&EmitterDraft) -> Option<String>,
    write: fn(&mut EmitterDraft, Option<String>),
) -> impl IntoView {
    view! {
        <label class="create-check"><input type="checkbox" class=format!("{class_name}-enabled")
            prop:checked=move || signals.emitter.with(read).is_some()
            disabled=move || signals.progress.get().is_pending()
            on:change=move |event| {
                let value = if event_target_checked(&event) { Some(String::new()) } else { None };
                signals.emitter.update(|draft| write(draft, value)); signals.edit();
            } />{label}</label>
        <Show when=move || signals.emitter.with(read).is_some() fallback=|| ()>
            <label class="create-field"><span>{label}</span>
                <textarea class=class_name prop:value=move || signals.emitter.with(read).unwrap_or_default()
                    disabled=move || signals.progress.get().is_pending()
                    on:input=move |event| { let value = event_target_textarea_value(&event); signals.emitter.update(|draft| write(draft, Some(value))); signals.edit(); } />
            </label>
        </Show>
    }
}

pub(super) fn move_item<T>(rows: &mut [T], index: usize, up: bool) {
    let other = if up {
        index.checked_sub(1)
    } else {
        index.checked_add(1)
    };
    if let Some(other) = other
        && index < rows.len()
        && other < rows.len()
    {
        rows.swap(index, other);
    }
}

#[component]
pub(super) fn OrderButtons(
    signals: CreateSignals,
    index: Signal<usize>,
    len: Signal<usize>,
    move_row: Callback<(usize, bool)>,
    remove_row: Callback<usize>,
    #[prop(default = 0)] minimum: usize,
) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    view! {
        <div class="create-row-actions">
            <button type="button" class="create-emitter-row-up" disabled=move || pending() || index.get() == 0
                on:click=move |_| { move_row.run((index.get_untracked(), true)); signals.edit(); }>"Move up"</button>
            <button type="button" class="create-emitter-row-down" disabled=move || pending() || index.get().checked_add(1).is_none_or(|next| next >= len.get())
                on:click=move |_| { move_row.run((index.get_untracked(), false)); signals.edit(); }>"Move down"</button>
            <button type="button" class="create-emitter-row-remove" disabled=move || pending() || len.get() <= minimum
                on:click=move |_| { remove_row.run(index.get_untracked()); signals.edit(); }>"Remove"</button>
        </div>
    }
}

#[derive(Clone, Copy)]
pub(super) enum ValueArea {
    Values,
    Attributes,
    Resource,
    Properties,
}

impl ValueArea {
    fn rows(self, draft: &EmitterDraft) -> &[ValueDraft] {
        match self {
            Self::Values => &draft.destination.values,
            Self::Attributes => &draft.destination.attributes,
            Self::Resource => &draft.destination.resource,
            Self::Properties => &draft.envelope.properties,
        }
    }
    pub(super) fn rows_mut(self, draft: &mut EmitterDraft) -> &mut Vec<ValueDraft> {
        match self {
            Self::Values => &mut draft.destination.values,
            Self::Attributes => &mut draft.destination.attributes,
            Self::Resource => &mut draft.destination.resource,
            Self::Properties => &mut draft.envelope.properties,
        }
    }
}

#[component]
pub(super) fn ValueRows(
    signals: CreateSignals,
    area: ValueArea,
    class_name: &'static str,
) -> impl IntoView {
    view! {
        <div class=class_name>
            <For each={move || (0..signals.emitter.with(|draft| area.rows(draft).len())).collect::<Vec<_>>()} key=|index| *index children=move |index| view! {
                <div class="create-emitter-value-row" data-row=index.to_string()>
                    <ValueName signals=signals area=area index=index />
                    <label class="create-field"><span>"Expression"</span>
                        <textarea class="create-emitter-value-expression" prop:value=move || signals.emitter.with(|draft| match area.rows(draft).get(index) { Some(row) => row.expression.clone(), None => String::new() })
                            disabled=move || signals.progress.get().is_pending()
                            on:input=move |event| { let value = event_target_textarea_value(&event); signals.emitter.update(|draft| { if let Some(row) = area.rows_mut(draft).get_mut(index) { row.expression = value; } }); signals.edit(); } />
                    </label>
                    <OrderButtons signals=signals index=Signal::derive(move || index) len=Signal::derive(move || signals.emitter.with(|draft| area.rows(draft).len()))
                        move_row=Callback::new(move |(index, up)| signals.emitter.update(|draft| move_item(area.rows_mut(draft), index, up)))
                        remove_row=Callback::new(move |index| signals.emitter.update(|draft| { let rows = area.rows_mut(draft); if index < rows.len() { rows.remove(index); } })) />
                </div>
            } />
            <button type="button" class="create-emitter-add-value" disabled=move || signals.progress.get().is_pending()
                on:click=move |_| { signals.emitter.update(|draft| area.rows_mut(draft).push(ValueDraft::default())); signals.edit(); }>"Add mapping"</button>
        </div>
    }
}

#[component]
fn ValueName(signals: CreateSignals, area: ValueArea, index: usize) -> impl IntoView {
    let current = move || {
        signals
            .emitter
            .with(|draft| match area.rows(draft).get(index) {
                Some(row) => row.name.clone(),
                None => String::new(),
            })
    };
    let edit = move |value: String| {
        signals.emitter.update(|draft| {
            if let Some(row) = area.rows_mut(draft).get_mut(index) {
                row.name = value;
            }
        });
        signals.edit();
    };
    view! {
        <label class="create-field"><span>"Column or key"</span>
            <Show when=move || matches!(area,ValueArea::Properties) fallback=move || view! {
                <input class="create-emitter-value-name" type="text" prop:value=current disabled=move || signals.progress.get().is_pending() on:input=move |event| edit(event_target_value(&event)) />
            }>
                <select class="create-emitter-value-name" prop:value=current disabled=move || signals.progress.get().is_pending() on:change=move |event| edit(event_target_value(&event))>
                    <option value="">"Select a declared property"</option>
                    <For each={move || match signals.emitter.get().destination.kind {Some(kind) => kind.capabilities().properties.to_vec(),None => Vec::new()}} key=|property| property.name children=|property| view! {<option value=property.name>{format!("{}: {}",property.name,property.ty)}</option>} />
                </select>
            </Show>
        </label>
    }
}

#[derive(Clone, Copy)]
pub(super) enum AssignmentArea {
    Output,
    State,
    Error,
}

impl AssignmentArea {
    fn rows(self, draft: &EmitterDraft) -> &[AssignmentDraft] {
        match self {
            Self::Output => &draft.assignments,
            Self::State => match draft.state.get(draft.active_state) {
                Some(state) => match &state.policy {
                    StatePolicyDraft::Default(assignments) => assignments,
                    _ => &[],
                },
                None => &[],
            },
            Self::Error => match &draft.message_error {
                MessageErrorDraft::SendTo { assignments, .. } => assignments,
                _ => &[],
            },
        }
    }
    fn with_rows(self, draft: &mut EmitterDraft, action: impl FnOnce(&mut Vec<AssignmentDraft>)) {
        match self {
            Self::Output => action(&mut draft.assignments),
            Self::State => {
                if let Some(state) = draft.state.get_mut(draft.active_state)
                    && let StatePolicyDraft::Default(assignments) = &mut state.policy
                {
                    action(assignments);
                }
            }
            Self::Error => {
                if let MessageErrorDraft::SendTo { assignments, .. } = &mut draft.message_error {
                    action(assignments);
                }
            }
        }
    }
}

#[component]
pub(super) fn AssignmentRows(signals: CreateSignals, area: AssignmentArea) -> impl IntoView {
    view! {
        <For each={move || (0..signals.emitter.with(|draft| area.rows(draft).len())).collect::<Vec<_>>()} key=|index| *index children=move |index| view! {
            <div class="create-emitter-assignment-row" data-row=index.to_string()>
                <span>{move || signals.emitter.with(|draft| match area.rows(draft).get(index) { Some(row) => match &row.field { Some(field) => field.name().to_string(), None => String::new() }, None => String::new() })}</span>
                <Show when=move || signals.emitter.with(|draft| match area.rows(draft).get(index) { Some(row) => row.field.as_ref().is_some_and(|field| !field.is_current()), None => false }) fallback=|| ()>
                    <p class="create-reference-invalid">"This field belongs to a changed context. Remove it and select it again."</p>
                </Show>
                <Show when=move || matches!(area,AssignmentArea::Output) fallback=|| ()>
                    <label class="create-field"><span>"Assignment scope"</span><select class="create-emitter-assignment-scope"
                        prop:value=move || signals.emitter.with(|draft| match area.rows(draft).get(index) {Some(row) => match row.scope {nervix_models::AssignmentTargetScope::Bare => "bare",nervix_models::AssignmentTargetScope::Message => "message",nervix_models::AssignmentTargetScope::Output => "output",nervix_models::AssignmentTargetScope::Branch => "branch"}, None => "bare"})
                        disabled=move || signals.progress.get().is_pending()
                        on:change=move |event| {let value = event_target_value(&event); signals.emitter.update(|draft| area.with_rows(draft,|rows| {if let Some(row) = rows.get_mut(index) {row.scope = match value.as_str() {"message" => nervix_models::AssignmentTargetScope::Message,"output" => nervix_models::AssignmentTargetScope::Output,"branch" => nervix_models::AssignmentTargetScope::Branch,_ => nervix_models::AssignmentTargetScope::Bare};}}));signals.edit();}>
                        <option value="bare">"Unqualified"</option><option value="message">"MESSAGE"</option><option value="output">"OUTPUT"</option><option value="branch">"BRANCH"</option>
                    </select></label>
                </Show>
                <label class="create-field"><span>"SET expression"</span><textarea class="create-emitter-assignment-expression"
                    prop:value=move || signals.emitter.with(|draft| match area.rows(draft).get(index) { Some(row) => row.expression.clone(), None => String::new() })
                    disabled=move || signals.progress.get().is_pending()
                    on:input=move |event| { let value = event_target_textarea_value(&event); signals.emitter.update(|draft| area.with_rows(draft, |rows| { if let Some(row) = rows.get_mut(index) { row.expression = value; } })); signals.edit(); } /></label>
                <OrderButtons signals=signals index=Signal::derive(move || index) len=Signal::derive(move || signals.emitter.with(|draft| area.rows(draft).len()))
                    move_row=Callback::new(move |(index, up)| signals.emitter.update(|draft| area.with_rows(draft, |rows| move_item(rows, index, up))))
                    remove_row=Callback::new(move |index| signals.emitter.update(|draft| area.with_rows(draft, |rows| { if index < rows.len() { rows.remove(index); } }))) />
            </div>
        } />
    }
}
