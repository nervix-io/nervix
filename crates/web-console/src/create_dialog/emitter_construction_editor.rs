//! Ordered output construction and header invocations for visual emitters.
//!
//! Layer: edges.
//! - **Owns.** Inheritance, explicit leakage, output assignments, predicates and invocation order.
//! - **Depends on.** Shared construction drafts and typed input/output field choices.
//! - **Must not know.** VM execution or external header encoding.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns emitter construction controls")
)]

use leptos::prelude::*;

use super::{
    ChoiceControl, ChoiceGroup, CreateSignals, RequestSender,
    emitter_draft::BodyChoice,
    emitter_rows::{AssignmentArea, AssignmentRows, EmitterText, OrderButtons, move_item},
    event_target_checked, event_target_textarea_value, event_target_value,
    ingestor_route_draft::{InheritDraft, InvocationDraft},
};

#[component]
pub(super) fn EmitterConstructionEditor(
    signals: CreateSignals,
    request_tx: RwSignal<Option<RequestSender>>,
    session_generation: RwSignal<u64>,
) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    let transforming = move || {
        matches!(
            signals.emitter.get().body,
            Some(BodyChoice::Codec | BodyChoice::Client)
        )
    };
    view! {
        <section class="create-emitter-construction"><h3>"Output construction"</h3>
            <p>"Sensitive fields require explicit leakage before external publication. Field details show sensitivity; final validation checks every expression and output type."</p>
            <fieldset class="create-field create-emitter-inherit"><legend>"Inheritance"</legend>
                <button type="button" data-inherit="none" class:active=move || signals.emitter.get().inherit == InheritDraft::None disabled=pending on:click=move |_| { signals.emitter.update(|draft| draft.inherit.choose("none")); signals.edit(); }>"No inheritance"</button>
                <Show when=transforming fallback=|| ()>
                    <For each={|| [("all", "INHERIT ALL"), ("all-except", "INHERIT ALL EXCEPT"), ("fields", "Selected fields")]} key=|(key, _)| *key children=move |(key, label)| view! {
                        <button type="button" data-inherit=key class:active=move || signals.emitter.get().inherit.key() == key disabled=pending on:click=move |_| { signals.emitter.update(|draft| draft.inherit.choose(key)); signals.edit(); }>{label}</button>
                    } />
                </Show>
            </fieldset>
            <Show when=move || matches!(signals.emitter.get().inherit, InheritDraft::AllExcept(_) | InheritDraft::Fields(_)) fallback=|| ()>
                <ChoiceGroup class_name="create-emitter-inherit-field" label="Add input field" control=ChoiceControl::EmitterInputField signals=signals request_tx=request_tx session_generation=session_generation show_detail=true />
                <For each={move || (0..signals.emitter.get().inherit.field_count()).collect::<Vec<_>>()} key=|index| *index children=move |index| view! {
                    <div class="create-emitter-inherited-field" data-row=index.to_string()>
                        <span>{move || signals.emitter.with(|draft| match draft.inherit.field_name(index) { Some(field) => field.to_string(), None => String::new() })}</span>
                        <Show when=move || !signals.emitter.get().inherit.field_current(index) fallback=|| ()><p class="create-reference-invalid">"Select this field again after the context change."</p></Show>
                        <Show when=move || signals.emitter.get().inherit.leak_at(index).is_some() fallback=|| ()>
                            <label class="create-check"><input type="checkbox" class="create-emitter-leak" prop:checked=move || signals.emitter.get().inherit.leak_at(index).unwrap_or(false) disabled=pending
                                on:change=move |event| { let leak = event_target_checked(&event); signals.emitter.update(|draft| draft.inherit.set_leak(index, leak)); signals.edit(); } />"LEAK SENSITIVE"</label>
                        </Show>
                        <OrderButtons signals=signals index=Signal::derive(move || index) len=Signal::derive(move || signals.emitter.get().inherit.field_count())
                            move_row=Callback::new(move |(index, up)| signals.emitter.update(|draft| draft.inherit.move_field(index, up)))
                            remove_row=Callback::new(move |index| signals.emitter.update(|draft| draft.inherit.remove_field(index))) />
                    </div>
                } />
            </Show>
            <Show when=transforming fallback=|| ()>
                <ChoiceGroup class_name="create-emitter-output-field" label="Add output field" control=ChoiceControl::EmitterOutputField signals=signals request_tx=request_tx session_generation=session_generation show_detail=true />
                <AssignmentRows signals=signals area=AssignmentArea::Output />
            </Show>
            <EmitterText label="Output WHERE expression (optional)" class_name="create-emitter-output-where" signals=signals multiline=true read=|draft| draft.predicate.clone() write=|draft, value| draft.predicate = value />
            <Show when=move || transforming() || signals.emitter.get().destination.kind.is_some_and(|kind| kind.capabilities().writes_headers()) fallback=|| ()>
                <InvocationEditor signals=signals />
            </Show>
        </section>
    }
    .into_any()
}

#[component]
fn InvocationEditor(signals: CreateSignals) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    view! {
        <section class="create-emitter-invocations"><h4>"Ordered invocations"</h4>
            <For each={move || (0..signals.emitter.get().invocations.len()).collect::<Vec<_>>()} key=|index| *index children=move |index| view! {
                <div class="create-emitter-invocation" data-invocation=index.to_string()>
                    <label class="create-field"><span>"INVOKE function"</span><input type="text" class="create-emitter-invocation-function" disabled=pending
                        prop:value=move || signals.emitter.with(|draft| match draft.invocations.get(index) { Some(invocation) => invocation.function.clone(), None => String::new() })
                        on:input=move |event| { let value = event_target_value(&event); signals.emitter.update(|draft| { if let Some(invocation) = draft.invocations.get_mut(index) { invocation.function = value; } }); signals.edit(); } /></label>
                    <div class="create-emitter-invocation-arguments">
                        <For each={move || signals.emitter.with(|draft| match draft.invocations.get(index) { Some(invocation) => (0..invocation.arguments.len()).collect::<Vec<_>>(), None => Vec::new() })} key=|argument| *argument children=move |argument| view! {
                            <div data-argument=argument.to_string()>
                                <label class="create-field"><span>"Argument expression"</span><textarea class="create-emitter-invocation-argument" disabled=pending
                                    prop:value=move || signals.emitter.with(|draft| match draft.invocations.get(index) { Some(invocation) => invocation.arguments.get(argument).cloned().unwrap_or_default(), None => String::new() })
                                    on:input=move |event| { let value = event_target_textarea_value(&event); signals.emitter.update(|draft| { if let Some(invocation) = draft.invocations.get_mut(index) && let Some(target) = invocation.arguments.get_mut(argument) { *target = value; } }); signals.edit(); } /></label>
                                <OrderButtons signals=signals index=Signal::derive(move || argument) len=Signal::derive(move || signals.emitter.with(|draft| match draft.invocations.get(index) { Some(invocation) => invocation.arguments.len(), None => 0 }))
                                    move_row=Callback::new(move |(argument, up)| signals.emitter.update(|draft| { if let Some(invocation) = draft.invocations.get_mut(index) { move_item(&mut invocation.arguments, argument, up); } }))
                                    remove_row=Callback::new(move |argument| signals.emitter.update(|draft| { if let Some(invocation) = draft.invocations.get_mut(index) && argument < invocation.arguments.len() { invocation.arguments.remove(argument); } })) />
                            </div>
                        } />
                    </div>
                    <button type="button" class="create-emitter-add-argument" disabled=pending on:click=move |_| { signals.emitter.update(|draft| { if let Some(invocation) = draft.invocations.get_mut(index) { invocation.arguments.push(String::new()); } }); signals.edit(); }>"Add argument"</button>
                    <OrderButtons signals=signals index=Signal::derive(move || index) len=Signal::derive(move || signals.emitter.get().invocations.len())
                        move_row=Callback::new(move |(index, up)| signals.emitter.update(|draft| move_item(&mut draft.invocations, index, up)))
                        remove_row=Callback::new(move |index| signals.emitter.update(|draft| { if index < draft.invocations.len() { draft.invocations.remove(index); } })) />
                </div>
            } />
            <button type="button" class="create-emitter-add-invocation" disabled=pending on:click=move |_| { signals.emitter.update(|draft| draft.invocations.push(InvocationDraft::default())); signals.edit(); }>"Add invocation"</button>
        </section>
    }
}
