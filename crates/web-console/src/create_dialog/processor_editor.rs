//! Shared visual editor for junction, deduplicator, reorderer, window processor, correlator and
//! reingestor creation.
//!
//! Layer: edges.
//!
//! - **Owns.** Browser controls for node-wide processor options and composition of the ordered
//!   input lists, the family's key, window or correlation section, and the state and route
//!   editors for every processor family.
//! - **Depends on.** Processor drafts, typed choice controls, and Leptos signals.
//! - **Must not know.** Registry validation, runtime placement, or session transport internals.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use leptos::prelude::*;
use nervix_models::AckMode;

use super::{
    CreateSignals, RequestSender, event_target_textarea_value, event_target_value,
    processor_correlation_editor::ProcessorCorrelationEditor, processor_draft::ProcessorFamily,
    processor_input_draft::InputList, processor_input_editor::ProcessorInputEditor,
    processor_key_editor::ProcessorKeyEditor, processor_route_editor::ProcessorRouteEditor,
    processor_state_editor::ProcessorStateEditor, processor_window_editor::ProcessorWindowEditor,
};

pub(super) fn indices(len: usize) -> Vec<usize> {
    (0..len).collect()
}

#[component]
pub(super) fn ProcessorEditor(
    family: ProcessorFamily,
    signals: CreateSignals,
    name_input: NodeRef<leptos::html::Input>,
    request_tx: RwSignal<Option<RequestSender>>,
    session_generation: RwSignal<u64>,
) -> impl IntoView {
    let draft = signals.processor(family);
    let pending = move || signals.progress.get().is_pending();
    let keys = family.key_clause().map(|clause| {
        view! { <ProcessorKeyEditor family=family clause=clause signals=signals /> }
    });
    let right_inputs = family.reads_right_side().then(|| {
        view! { <ProcessorInputEditor family=family list=InputList::Right signals=signals request_tx=request_tx session_generation=session_generation /> }
    });
    let filter = family.filters_input().then(|| {
        view! {
            <label class="create-field"><span>"Node FILTER WHERE expression (optional)"</span>
                <textarea class="create-processor-filter"
                    prop:value=move || draft.get().filter
                    disabled=pending
                    on:input=move |event| {
                        draft.update(|state| state.filter = event_target_textarea_value(&event));
                        signals.edit();
                    } />
            </label>
        }
    });
    let stage = match family {
        ProcessorFamily::Window => Some(view! { <ProcessorWindowEditor signals=signals /> }.into_any()),
        ProcessorFamily::Correlator => Some(
            view! { <ProcessorCorrelationEditor signals=signals request_tx=request_tx session_generation=session_generation /> }
                .into_any(),
        ),
        ProcessorFamily::Junction
        | ProcessorFamily::Deduplicator
        | ProcessorFamily::Reorderer
        | ProcessorFamily::Reingestor => None,
    };
    let editor = view! {
        <label class="create-field"><span>{family.name_label()}</span>
            <input node_ref=name_input class="create-name" type="text" autocomplete="off"
                prop:value=move || draft.get().name
                disabled=pending
                on:input=move |event| {
                    draft.update(|state| state.name = event_target_value(&event));
                    signals.edit();
                } />
        </label>
        <fieldset class="create-field create-processor-mode">
            <legend>"Acknowledgement mode"</legend>
            <button type="button" data-mode="attached"
                class:active=move || draft.get().mode == AckMode::Attached
                disabled=pending
                on:click=move |_| { draft.update(|state| state.mode = AckMode::Attached); signals.edit(); }>"ATTACHED"</button>
            <button type="button" data-mode="detached"
                class:active=move || draft.get().mode == AckMode::Detached
                disabled=pending
                on:click=move |_| { draft.update(|state| state.mode = AckMode::Detached); signals.edit(); }>"DETACHED"</button>
        </fieldset>
        <ProcessorInputEditor family=family list=family.primary_input_list() signals=signals request_tx=request_tx session_generation=session_generation />
        {right_inputs}
        {filter}
        {keys}
        {stage}
        <ProcessorStateEditor family=family signals=signals request_tx=request_tx session_generation=session_generation />
        <ProcessorRouteEditor family=family signals=signals request_tx=request_tx session_generation=session_generation />
    };
    // Erased at the editor boundary: the dialog shows this editor in one arm per processor family,
    // and each arm's type would otherwise spell this editor's complete nested view into the
    // dialog's debug names.
    editor.into_any()
}
