//! Composition of the visual emitter form and its ordered relay inputs.
//!
//! Layer: edges.
//! - **Owns.** Emitter naming, instances, attachment, collection and the composition of its focused
//!   editors.
//! - **Depends on.** Browser drafts, declared entity choices and shared dialog dispatch.
//! - **Must not know.** Emitter tasks, placement or transport clients.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns visual emitter form composition")
)]

use leptos::prelude::*;
use meticulous::OptionExt as _;
use nervix_models::{AckMode, InputCollectPolicy};

use super::{
    ChoiceControl, ChoiceGroup, CreateSignals, RequestSender,
    emitter_construction_editor::EmitterConstructionEditor,
    emitter_destination_editor::EmitterDestinationEditor,
    emitter_policy_editor::{EmitterPolicyEditor, EmitterPublicationEditor},
    emitter_rows::{EmitterText, OrderButtons},
    emitter_state_editor::EmitterStateEditor,
    event_target_checked, event_target_value,
};

#[component]
pub(super) fn EmitterEditor(
    signals: CreateSignals,
    name_input: NodeRef<leptos::html::Input>,
    request_tx: RwSignal<Option<RequestSender>>,
    session_generation: RwSignal<u64>,
) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    view! {
        <label class="create-field"><span>"Emitter name"</span><input node_ref=name_input class="create-name" type="text" autocomplete="off"
            prop:value=move || signals.emitter.get().name disabled=pending
            on:input=move |event| { let name = event_target_value(&event); signals.emitter.update(|draft| draft.name = name); signals.edit(); } /></label>
        <EmitterDestinationEditor signals=signals request_tx=request_tx session_generation=session_generation />
        <section class="create-emitter-inputs"><h3>"Ordered input relays"</h3>
            <p>"Inputs share one payload schema and may have different branches. Materialized and error relays must fit every input."</p>
            <For each={move || (0..signals.emitter.get().inputs.len()).collect::<Vec<_>>()} key=|index| *index children=move |index| view! {
                <button type="button" data-input=index.to_string() class:active=move || signals.emitter.get().active_input == index disabled=pending
                    on:click=move |_| { signals.emitter.update(|draft| draft.active_input = index); signals.edit(); }>{format!("Input {}", index.checked_add(1).assured("an allocated input index is below usize::MAX"))}</button>
            } />
            <button type="button" class="create-emitter-add-input" disabled=pending on:click=move |_| { signals.emitter.update(|draft| draft.add_input()); signals.edit(); }>"Add input"</button>
            <OrderButtons minimum=1 signals=signals index=Signal::derive(move || signals.emitter.get().active_input) len=Signal::derive(move || signals.emitter.get().inputs.len())
                move_row=Callback::new(move |(_, up)| signals.emitter.update(|draft| draft.move_input(up)))
                remove_row=Callback::new(move |_| signals.emitter.update(|draft| draft.remove_input())) />
            <ChoiceGroup class_name="create-emitter-input" label="Input relay" control=ChoiceControl::EmitterInputRelay signals=signals request_tx=request_tx session_generation=session_generation show_detail=true />
            <p>{move || signals.emitter.with(|draft| match draft.inputs.get(draft.active_input).and_then(|input| input.relay.as_ref()) { Some(relay) => format!("Selected input: {}", relay.name()), None => "Choose an input relay".to_string() })}</p>
            <Show when=move || signals.emitter.with(|draft| draft.inputs.iter().any(|input| input.relay.as_ref().is_some_and(|relay| !relay.is_current()))) fallback=|| ()>
                <p class="create-reference-invalid">"An input belongs to a changed context. Select it again."</p>
            </Show>
            <EmitterText label="Input WHERE expression (optional)" class_name="create-emitter-input-where" signals=signals multiline=true
                read=|draft| match draft.inputs.get(draft.active_input) { Some(input) => input.where_clause.clone(), None => String::new() }
                write=|draft, value| { if let Some(input) = draft.inputs.get_mut(draft.active_input) { input.where_clause = value; } } />
            <Show when=move || signals.emitter.with(|draft| draft.destination.kind.is_some_and(|kind| kind.capabilities().instancing.refusal(kind).is_none())) fallback=|| ()>
                <EmitterText label="INSTANCES (a count or PER PARTITION; empty runs one instance)" class_name="create-emitter-instances" signals=signals
                    read=|draft| draft.instances.clone()
                    write=|draft, value| draft.instances = value />
            </Show>
            <fieldset class="create-field create-emitter-attachment"><legend>"Input acknowledgement"</legend>
                <button type="button" data-mode="attached" class:active=move || signals.emitter.get().mode == AckMode::Attached disabled=pending
                    on:click=move |_| { signals.emitter.update(|draft| draft.mode = AckMode::Attached); signals.edit(); }>"ATTACHED"</button>
                <button type="button" data-mode="detached" class:active=move || signals.emitter.get().mode == AckMode::Detached disabled=pending
                    on:click=move |_| { signals.emitter.update(|draft| draft.mode = AckMode::Detached); signals.edit(); }>"DETACHED"</button>
            </fieldset>
            <label class="create-check"><input type="checkbox" class="create-emitter-collect" prop:checked=move || signals.emitter.get().collect.is_some() disabled=pending
                on:change=move |event| { let enabled = event_target_checked(&event); signals.emitter.update(|draft| draft.collect = if enabled { Some(InputCollectPolicy { collect_for: String::new(), max_batch_size: None }) } else { None }); signals.edit(); } />"COLLECT FOR input batches"</label>
            <Show when=move || signals.emitter.get().collect.is_some() fallback=|| ()>
                <EmitterText label="Collection duration" class_name="create-emitter-collect-for" signals=signals
                    read=|draft| match &draft.collect { Some(collect) => collect.collect_for.clone(), None => String::new() }
                    write=|draft, value| { if let Some(collect) = &mut draft.collect { collect.collect_for = value; } } />
                <EmitterText label="Collection maximum batch size (optional)" class_name="create-emitter-collect-size" signals=signals
                    read=|draft| match &draft.collect { Some(collect) => collect.max_batch_size.clone().unwrap_or_default(), None => String::new() }
                    write=|draft, value| { if let Some(collect) = &mut draft.collect { collect.max_batch_size = if value.is_empty() { None } else { Some(value) }; } } />
            </Show>
        </section>
        <EmitterPublicationEditor signals=signals />
        <EmitterStateEditor signals=signals request_tx=request_tx session_generation=session_generation />
        <EmitterConstructionEditor signals=signals request_tx=request_tx session_generation=session_generation />
        <EmitterPolicyEditor signals=signals request_tx=request_tx session_generation=session_generation />
    }
}
