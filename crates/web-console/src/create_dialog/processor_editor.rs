//! Shared visual editor for junction and reingestor creation.
//!
//! Layer: edges.
//!
//! - **Owns.** Browser controls for node-wide processor options and composition of ordered input,
//!   state, and route editors for both processor families.
//! - **Depends on.** Processor drafts, typed choice controls, and Leptos signals.
//! - **Must not know.** Registry validation, runtime placement, or session transport internals.

use leptos::prelude::*;
use nervix_models::AckMode;

use super::{
    CreateSignals, RequestSender, event_target_textarea_value, event_target_value,
    processor_draft::ProcessorFamily, processor_input_editor::ProcessorInputEditor,
    processor_route_editor::ProcessorRouteEditor, processor_state_editor::ProcessorStateEditor,
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
    let label = match family {
        ProcessorFamily::Junction => "Junction name",
        ProcessorFamily::Reingestor => "Reingestor name",
    };
    view! {
        <label class="create-field"><span>{label}</span>
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
        <ProcessorInputEditor family=family signals=signals request_tx=request_tx session_generation=session_generation />
        <label class="create-field"><span>"Node FILTER WHERE expression (optional)"</span>
            <textarea class="create-processor-filter"
                prop:value=move || draft.get().filter
                disabled=pending
                on:input=move |event| {
                    draft.update(|state| state.filter = event_target_textarea_value(&event));
                    signals.edit();
                } />
        </label>
        <ProcessorStateEditor family=family signals=signals request_tx=request_tx session_generation=session_generation />
        <ProcessorRouteEditor family=family signals=signals request_tx=request_tx session_generation=session_generation />
    }
}
