//! Ordered key expressions and the holding time of a visual deduplicator or reorderer.
//!
//! Layer: edges.
//!
//! - **Owns.** Browser editing of the ordered `DEDUPLICATE ON` or `BY` expressions and the
//!   `MAX TIME` text of one keyed processor draft.
//! - **Depends on.** Processor drafts and Leptos signals.
//! - **Must not know.** Key compilation, duration validation, keyspaces or ordering buffers.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use leptos::prelude::*;

use super::{
    CreateSignals, event_target_textarea_value, event_target_value,
    processor_draft::{KeyClause, ProcessorFamily},
    processor_editor::indices,
};

#[component]
pub(super) fn ProcessorKeyEditor(
    family: ProcessorFamily,
    clause: KeyClause,
    signals: CreateSignals,
) -> impl IntoView {
    let draft = signals.processor(family);
    let pending = move || signals.progress.get().is_pending();
    let expression_count = move || {
        let state = draft.get();
        match state.keyed() {
            Some(keyed) => keyed.expressions.len(),
            None => 0,
        }
    };
    let active = move || {
        let state = draft.get();
        match state.keyed() {
            Some(keyed) => keyed.active,
            None => 0,
        }
    };
    let section = view! {
        <section class="create-processor-keys">
            <h3>{format!("Ordered {clause} expressions")}</h3>
            <div class="create-processor-key-list">
                <For each=move || indices(expression_count()) key=|index| *index
                    children=move |index| view! {
                        <button type="button" data-key=index.to_string()
                            class:active=move || active() == index
                            disabled=pending
                            on:click=move |_| {
                                draft.update(|state| {
                                    if let Some(keyed) = state.keyed_mut() { keyed.active = index; }
                                });
                                signals.edit();
                            }>
                            {format!("Expression {}", index + 1)}
                        </button>
                    } />
                <button type="button" class="create-processor-add-key" disabled=pending
                    on:click=move |_| {
                        draft.update(|state| {
                            if let Some(keyed) = state.keyed_mut() { keyed.add(); }
                        });
                        signals.edit();
                    }>"Add expression"</button>
            </div>
            <div class="create-processor-key-actions">
                <button type="button" class="create-processor-key-up"
                    disabled=move || { pending() || active() == 0 }
                    on:click=move |_| {
                        draft.update(|state| {
                            if let Some(keyed) = state.keyed_mut() { keyed.move_active(true); }
                        });
                        signals.edit();
                    }>"Move up"</button>
                <button type="button" class="create-processor-key-down"
                    disabled=move || { pending() || active() + 1 >= expression_count() }
                    on:click=move |_| {
                        draft.update(|state| {
                            if let Some(keyed) = state.keyed_mut() { keyed.move_active(false); }
                        });
                        signals.edit();
                    }>"Move down"</button>
                <button type="button" class="create-processor-remove-key"
                    disabled=move || { pending() || expression_count() <= 1 }
                    on:click=move |_| {
                        draft.update(|state| {
                            if let Some(keyed) = state.keyed_mut() { keyed.remove_active(); }
                        });
                        signals.edit();
                    }>"Remove expression"</button>
            </div>
            <label class="create-field"><span>{format!("{clause} expression")}</span>
                <textarea class="create-processor-key-expression"
                    prop:value=move || {
                        let state = draft.get();
                        let Some(keyed) = state.keyed() else {
                            return String::new();
                        };
                        match keyed.active_expression() {
                            Some(expression) => expression.clone(),
                            None => String::new(),
                        }
                    }
                    disabled=pending
                    on:input=move |event| {
                        let value = event_target_textarea_value(&event);
                        draft.update(|state| {
                            let Some(keyed) = state.keyed_mut() else {
                                return;
                            };
                            if let Some(expression) = keyed.active_expression_mut() {
                                *expression = value;
                            }
                        });
                        signals.edit();
                    } />
            </label>
            <label class="create-field"><span>"MAX TIME duration"</span>
                <input class="create-processor-max-time" type="text" autocomplete="off"
                    prop:value=move || {
                        let state = draft.get();
                        match state.keyed() {
                            Some(keyed) => keyed.max_time.clone(),
                            None => String::new(),
                        }
                    }
                    disabled=pending
                    on:input=move |event| {
                        let value = event_target_value(&event);
                        draft.update(|state| {
                            if let Some(keyed) = state.keyed_mut() { keyed.max_time = value; }
                        });
                        signals.edit();
                    } />
            </label>
        </section>
    };
    // Erased at the section boundary: the processor editor composing this section then holds and
    // names a boxed view, so neither its rendering frames nor the debug names of every form that
    // embeds it repeat this section's complete nested view.
    section.into_any()
}
