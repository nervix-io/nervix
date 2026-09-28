//! Structured relay controls for the create popup.
//!
//! Layer: edges.
//!
//! - **Owns.** The relay form's name, schema and branch reference controls, its explicit
//!   branching choice, capacity, and materialized-state options.
//! - **Depends on.** Browser draft signals, the model-owned materialized-state variants, and the
//!   shared choice UI.
//! - **Must not know.** Registry internals, statement parsing, or how a command is dispatched.

use futures_channel::mpsc::UnboundedSender;
use leptos::prelude::*;
use nervix_models::MaterializedRelayState;
use strum::IntoEnumIterator as _;

use super::{
    ChoiceControl, ChoiceGroup, ConsoleRequest, CreateSignals, event_target_value,
    relay_draft::RelayDraft,
};

#[component]
pub(super) fn RelayEditor(
    signals: CreateSignals,
    name_input: NodeRef<leptos::html::Input>,
    request_tx: RwSignal<Option<UnboundedSender<ConsoleRequest>>>,
    session_generation: RwSignal<u64>,
) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    view! {
        <label class="create-field">
            <span>"Relay name"</span>
            <input node_ref=name_input class="create-name" type="text" autocomplete="off"
                prop:value=move || signals.relay.get().name
                disabled=pending
                on:input=move |event| {
                    let name = event_target_value(&event);
                    signals.relay.update(|draft| draft.name = name);
                    signals.edit();
                } />
        </label>
        <ChoiceGroup class_name="create-schema-ref" label="Schema"
            control=ChoiceControl::RelaySchema signals=signals request_tx=request_tx
            session_generation=session_generation />
        <Show when=move || signals.relay.get().schema.is_some() fallback=|| ()>
            <p class="create-selected-schema">{move || match signals.relay.get().schema {
                Some(schema) => format!("Selected schema: {}", schema.name()),
                None => String::new(),
            }}</p>
        </Show>
        <Show when=move || signals.relay.get().schema.is_some_and(|schema| !schema.is_current()) fallback=|| ()>
            <p class="create-reference-invalid" role="alert">"The selected schema's domain changed. Select a schema again."</p>
        </Show>
        <fieldset class="create-choice-group create-branching-options">
            <legend>"Branching"</legend>
            <div class="create-choice-buttons">
                <button type="button" data-value="UNBRANCHED"
                    class:active=move || signals.relay.get().branching.is_unbranched()
                    disabled=pending
                    on:click=move |_| {
                        signals.relay.update(RelayDraft::choose_unbranched);
                        signals.edit();
                    }>"UNBRANCHED"</button>
                <button type="button" data-value="BRANCHED BY"
                    class:active=move || signals.relay.get().branching.is_branched()
                    disabled=pending
                    on:click=move |_| {
                        signals.relay.update(RelayDraft::choose_branched);
                        signals.edit();
                    }>"BRANCHED BY"</button>
            </div>
        </fieldset>
        <Show when=move || signals.relay.get().branching.is_branched() fallback=|| ()>
            <ChoiceGroup class_name="create-branch-ref" label="Branch"
                control=ChoiceControl::RelayBranch signals=signals request_tx=request_tx
                session_generation=session_generation />
        </Show>
        <Show when=move || signals.relay.get().branching.selected_branch().is_some() fallback=|| ()>
            <p class="create-selected-branch">{move || {
                let draft = signals.relay.get();
                match draft.branching.selected_branch() {
                    Some(branch) => format!("Selected branch: {}", branch.name()),
                    None => String::new(),
                }
            }}</p>
        </Show>
        <Show when=move || signals.relay.get().branching.selected_branch().is_some_and(|branch| !branch.is_current()) fallback=|| ()>
            <p class="create-reference-invalid" role="alert">"The selected branch's domain changed. Select a branch again."</p>
        </Show>
        <label class="create-field">
            <span>"Capacity · batches the relay owner buffers"</span>
            <input class="create-capacity" type="number" min="1"
                prop:value=move || signals.relay.get().capacity
                disabled=pending
                on:input=move |event| {
                    let capacity = event_target_value(&event);
                    signals.relay.update(|draft| draft.capacity = capacity);
                    signals.edit();
                } />
        </label>
        <fieldset class="create-choice-group create-materialized-options">
            <legend>"Materialized state"</legend>
            <div class="create-choice-buttons">
                <button type="button" data-value="NONE"
                    class:active=move || signals.relay.get().materialized_state.is_none()
                    disabled=pending
                    on:click=move |_| {
                        signals.relay.update(|draft| draft.materialized_state = None);
                        signals.edit();
                    }>"NONE"</button>
                <For each=move || { MaterializedRelayState::iter().collect::<Vec<_>>() }
                    key=|state| state.as_ref().to_string()
                    children=move |state| {
                        let label = state.as_ref().to_string();
                        let active = state.clone();
                        view! {
                            <button type="button" data-value=label.clone()
                                class:active=move || signals.relay.get().materialized_state.as_ref() == Some(&active)
                                disabled=pending
                                on:click=move |_| {
                                    let chosen = state.clone();
                                    signals.relay.update(|draft| draft.materialized_state = Some(chosen));
                                    signals.edit();
                                }>{label.clone()}</button>
                        }
                    } />
            </div>
        </fieldset>
    }
}
