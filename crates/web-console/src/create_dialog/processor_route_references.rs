//! Typed field references a set-only processor route inserts into its last output assignment.
//!
//! Layer: edges.
//!
//! - **Owns.** The scope-aware field pickers of set-only processor routes: window input fields
//!   inserted as an aggregate's argument, the left and right fields of a correlator, a
//!   generator's materialized state, an inferencer's generated output tensors, a WASM guest's
//!   output fields, and the concrete branch's key fields. None offers the live input, which those
//!   routes cannot read.
//! - **Depends on.** Processor drafts, typed choice controls and Leptos signals.
//! - **Must not know.** Which functions aggregate, scope validation or route execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use leptos::prelude::*;
use nervix_models::FieldName;

use super::{
    ChoiceControl, CreateSignals, RequestSender,
    choice_group::ChoiceGroup,
    event_target_value,
    processor_draft::{NodeBranchDraft, ProcessorFamily},
};

/// The concrete branch's key fields, for a route of a processor declaring a named branch.
#[component]
fn BranchKeyReferences(
    family: ProcessorFamily,
    signals: CreateSignals,
    request_tx: RwSignal<Option<RequestSender>>,
    session_generation: RwSignal<u64>,
) -> impl IntoView {
    let draft = signals.processor(family);
    view! {
        <Show when=move || matches!(draft.get().branching, NodeBranchDraft::Branched(_)) fallback=|| ()>
            <ChoiceGroup class_name="create-processor-branch-key-fields" label="Branch key fields to assign"
                control=ChoiceControl::ProcessorBranchKeyField signals=signals request_tx=request_tx
                session_generation=session_generation show_detail=true />
        </Show>
    }
    .into_any()
}

/// The generated output tensors an inferencer route reads, each by the field its name gives.
#[component]
fn GeneratedTensorReferences(signals: CreateSignals) -> impl IntoView {
    let draft = signals.processor(ProcessorFamily::Inferencer);
    let pending = move || signals.progress.get().is_pending();
    let tensors = move || match draft.get().inferencer() {
        Some(inferencer) => inferencer.generated_fields(),
        None => Vec::new(),
    };
    view! {
        <div class="create-inferencer-generated">
            <For each=tensors key=|field: &FieldName| field.as_str().to_string()
                children=move |field: FieldName| {
                    let tensor = field.as_str().to_string();
                    let label = tensor.clone();
                    view! {
                        <button type="button" data-tensor=label
                            disabled=move || { pending() || !draft.get().has_output_assignment() }
                            on:click=move |_| {
                                let field = field.clone();
                                draft.update(|state| state.insert_generated_reference(field));
                                signals.edit();
                            }>{tensor}</button>
                    }
                } />
        </div>
    }
    .into_any()
}

#[component]
pub(super) fn ProcessorRouteReferences(
    family: ProcessorFamily,
    signals: CreateSignals,
    request_tx: RwSignal<Option<RequestSender>>,
    session_generation: RwSignal<u64>,
) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    match family {
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
        ProcessorFamily::Generator => view! {
            <fieldset class="create-field create-generator-route-references">
                <legend>"Materialized state and branch fields"</legend>
                <p>"A generator route reads the materialized state of its relay and the concrete branch's key, never a live input. A chosen field is inserted at the end of the last output assignment."</p>
                <ChoiceGroup class_name="create-generator-state-fields" label="Materialized state fields to assign"
                    control=ChoiceControl::GeneratorStateField signals=signals request_tx=request_tx
                    session_generation=session_generation show_detail=true />
                <BranchKeyReferences family=family signals=signals request_tx=request_tx session_generation=session_generation />
            </fieldset>
        }
        .into_any(),
        ProcessorFamily::Inferencer => view! {
            <fieldset class="create-field create-inferencer-route-references">
                <legend>"Generated tensors and branch fields"</legend>
                <p>"An inferencer route reads each OUTPUT SCHEMA tensor as a generated field of the same name, declared materialized state and the concrete branch's key, never the live input. A chosen field is inserted at the end of the last output assignment."</p>
                <GeneratedTensorReferences signals=signals />
                <BranchKeyReferences family=family signals=signals request_tx=request_tx session_generation=session_generation />
            </fieldset>
        }
        .into_any(),
        ProcessorFamily::Wasm => view! {
            <fieldset class="create-field create-wasm-route-references">
                <legend>"Guest output and branch fields"</legend>
                <p>"A WASM route reads the columns its guest returns, one aligned to each field of the route's relay, declared materialized state and the concrete branch's key, never the live input. A chosen field is inserted at the end of the last output assignment."</p>
                <ChoiceGroup class_name="create-wasm-guest-fields" label="Guest output fields to assign"
                    control=ChoiceControl::WasmGuestField signals=signals request_tx=request_tx
                    session_generation=session_generation show_detail=true />
                <BranchKeyReferences family=family signals=signals request_tx=request_tx session_generation=session_generation />
            </fieldset>
        }
        .into_any(),
        ProcessorFamily::Junction
        | ProcessorFamily::Deduplicator
        | ProcessorFamily::Reorderer
        | ProcessorFamily::Reingestor => ().into_any(),
    }
}
