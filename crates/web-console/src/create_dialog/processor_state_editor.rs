//! Ordered materialized-state dependencies for visual processors.
//!
//! Layer: edges.
//!
//! - **Owns.** Browser editing of dependency order, REQUIRED policies, and DEFAULT assignments.
//! - **Depends on.** Processor drafts, typed relay and field choices, and ordered SET controls.
//! - **Must not know.** Materialized view storage or branch scheduling.

use leptos::prelude::*;

use super::{
    ChoiceControl, CreateSignals, RequestSender,
    choice_group::ChoiceGroup,
    processor_assignment_editor::{AssignmentArea, ProcessorAssignmentRows},
    processor_draft::{ProcessorFamily, StatePolicyDraft},
    processor_editor::indices,
};

#[component]
pub(super) fn ProcessorStateEditor(
    family: ProcessorFamily,
    signals: CreateSignals,
    request_tx: RwSignal<Option<RequestSender>>,
    session_generation: RwSignal<u64>,
) -> impl IntoView {
    let draft = signals.processor(family);
    let pending = move || signals.progress.get().is_pending();
    view! {
        <section class="create-processor-state">
            <h3>"Ordered materialized dependencies"</h3>
            <p>"Available state binds immediately. REQUIRED SKIP drops a record when state is absent; REQUIRED WAIT holds it until state arrives. DEFAULT constructs a typed record."</p>
            <div class="create-processor-state-list">
                <For each=move || indices(draft.get().state.len()) key=|index| *index
                    children=move |index| view! {
                        <button type="button" data-state=index.to_string()
                            class:active=move || draft.get().active_state == index
                            disabled=pending
                            on:click=move |_| { draft.update(|state| state.active_state = index); signals.edit(); }>
                            {format!("Dependency {}", index + 1)}
                        </button>
                    } />
                <button type="button" class="create-processor-add-state" disabled=pending
                    on:click=move |_| { draft.update(|state| state.add_state()); signals.edit(); }>"Add materialized state"</button>
            </div>
            <Show when=move || !draft.get().state.is_empty() fallback=|| ()>
                <div class="create-processor-state-actions">
                    <button type="button" class="create-processor-state-up"
                        disabled=move || { pending() || draft.get().active_state == 0 }
                        on:click=move |_| { draft.update(|state| state.move_state(true)); signals.edit(); }>"Move up"</button>
                    <button type="button" class="create-processor-state-down"
                        disabled=move || { pending() || draft.get().active_state + 1 >= draft.get().state.len() }
                        on:click=move |_| { draft.update(|state| state.move_state(false)); signals.edit(); }>"Move down"</button>
                    <button type="button" class="create-processor-remove-state" disabled=pending
                        on:click=move |_| { draft.update(|state| state.remove_state()); signals.edit(); }>"Remove dependency"</button>
                </div>
                <ChoiceGroup class_name="create-processor-state-relay" label="Materialized relay"
                    control=ChoiceControl::ProcessorStateRelay signals=signals request_tx=request_tx
                    session_generation=session_generation show_detail=true />
                <fieldset class="create-field create-processor-state-policy">
                    <legend>"When this state is absent"</legend>
                    <button type="button" data-state-policy="skip"
                        class:active=move || draft.get().active_state().is_some_and(|state| state.policy == StatePolicyDraft::RequiredSkip)
                        disabled=pending
                        on:click=move |_| { draft.update(|state| {
                            if let Some(dependency) = state.active_state_mut() { dependency.policy = StatePolicyDraft::RequiredSkip; }
                        }); signals.edit(); }>"REQUIRED SKIP"</button>
                    <button type="button" data-state-policy="wait"
                        class:active=move || draft.get().active_state().is_some_and(|state| state.policy == StatePolicyDraft::RequiredWait)
                        disabled=pending
                        on:click=move |_| { draft.update(|state| {
                            if let Some(dependency) = state.active_state_mut() { dependency.policy = StatePolicyDraft::RequiredWait; }
                        }); signals.edit(); }>"REQUIRED WAIT"</button>
                    <button type="button" data-state-policy="default"
                        class:active=move || draft.get().active_state().is_some_and(|state| matches!(state.policy, StatePolicyDraft::Default(_)))
                        disabled=pending
                        on:click=move |_| { draft.update(|state| {
                            if let Some(dependency) = state.active_state_mut()
                                && !matches!(dependency.policy, StatePolicyDraft::Default(_)) {
                                dependency.policy = StatePolicyDraft::Default(Vec::new());
                            }
                        }); signals.edit(); }>"DEFAULT"</button>
                </fieldset>
                <Show when=move || draft.get().active_state().is_some_and(|state| matches!(state.policy, StatePolicyDraft::Default(_))) fallback=|| ()>
                    <div class="create-processor-state-default">
                        <ChoiceGroup class_name="create-processor-state-fields" label="Add default field"
                            control=ChoiceControl::ProcessorStateField signals=signals request_tx=request_tx
                            session_generation=session_generation show_detail=true />
                        <ProcessorAssignmentRows family=family area=AssignmentArea::State signals=signals />
                    </div>
                </Show>
            </Show>
        </section>
    }
}
