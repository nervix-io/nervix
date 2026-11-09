//! Typed field references a window or correlator route inserts into its last output assignment.
//!
//! Layer: edges.
//!
//! - **Owns.** The scope-aware field pickers of set-only processor routes: window input fields
//!   inserted as an aggregate's argument, and the left and right fields of a correlator.
//! - **Depends on.** Processor drafts, typed choice controls and Leptos signals.
//! - **Must not know.** Which functions aggregate, scope validation or route execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use leptos::prelude::*;

use super::{
    ChoiceControl, CreateSignals, RequestSender, choice_group::ChoiceGroup, event_target_value,
    processor_draft::ProcessorFamily,
};

#[component]
pub(super) fn ProcessorRouteReferences(
    family: ProcessorFamily,
    signals: CreateSignals,
    request_tx: RwSignal<Option<RequestSender>>,
    session_generation: RwSignal<u64>,
) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    let references = match family {
        ProcessorFamily::Window => {
            let draft = signals.processor(family);
            view! {
                <fieldset class="create-field create-window-aggregate-references">
                    <legend>"Aggregate a window input field"</legend>
                    <p>"Window input is read only inside an aggregate argument. A chosen input field is inserted as the argument of this aggregate at the end of the last output assignment."</p>
                    <label class="create-field"><span>"Aggregate function, such as COUNT, SUM or MAX"</span>
                        <input class="create-window-aggregate" type="text" autocomplete="off" spellcheck="false"
                            prop:value=move || {
                                let state = draft.get();
                                match state.window() {
                                    Some(window) => window.aggregate.clone(),
                                    None => String::new(),
                                }
                            }
                            disabled=pending
                            on:input=move |event| {
                                let value = event_target_value(&event);
                                draft.update(|state| {
                                    if let Some(window) = state.window_mut() { window.aggregate = value; }
                                });
                                signals.edit();
                            } />
                    </label>
                    <ChoiceGroup class_name="create-window-input-fields" label="Window input fields"
                        control=ChoiceControl::ProcessorAggregateField signals=signals request_tx=request_tx
                        session_generation=session_generation show_detail=true />
                </fieldset>
            }
            .into_any()
        }
        ProcessorFamily::Correlator => view! {
            <fieldset class="create-field create-correlator-route-references">
                <legend>"Left and right fields"</legend>
                <p>"A chosen field is inserted in its left or right scope at the end of the last output assignment."</p>
                <ChoiceGroup class_name="create-route-left-fields" label="Left fields to assign"
                    control=ChoiceControl::ProcessorRouteLeftField signals=signals request_tx=request_tx
                    session_generation=session_generation show_detail=true />
                <ChoiceGroup class_name="create-route-right-fields" label="Right fields to assign"
                    control=ChoiceControl::ProcessorRouteRightField signals=signals request_tx=request_tx
                    session_generation=session_generation show_detail=true />
            </fieldset>
        }
        .into_any(),
        ProcessorFamily::Junction
        | ProcessorFamily::Deduplicator
        | ProcessorFamily::Reorderer
        | ProcessorFamily::Reingestor => ().into_any(),
    };
    references
}
