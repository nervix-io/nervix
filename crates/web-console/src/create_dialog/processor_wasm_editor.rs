//! The guest limits and node-wide guest policies of a visual WASM processor.
//!
//! Layer: edges.
//!
//! - **Owns.** Browser editing of a WASM processor's `MAX FUEL` and `MAX MEMORY` limits, its
//!   `ON REJECTED STATE` choice with the consequence the choice has, and its `ON GLOBAL ERROR`
//!   policy.
//! - **Depends on.** The WASM processor draft and Leptos signals.
//! - **Must not know.** Guest compilation, fuel or memory accounting, or state recovery.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use leptos::prelude::*;
use nervix_models::{GeneralErrorPolicy, WasmRejectedStatePolicy};

use super::{
    CreateSignals, event_target_value,
    processor_draft::ProcessorFamily,
    processor_wasm_draft::{WasmDraft, rejected_state_consequence},
};

/// Applies `edit` to the open WASM processor draft as one edit of the form.
fn edit_wasm(signals: CreateSignals, edit: impl FnOnce(&mut WasmDraft)) {
    signals.processor(ProcessorFamily::Wasm).update(|draft| {
        if let Some(wasm) = draft.wasm_mut() {
            edit(wasm);
        }
    });
    signals.edit();
}

/// What `read` reads from the open WASM processor draft, tracked by the view that reads it.
fn read_wasm<T: Default>(signals: CreateSignals, read: impl FnOnce(&WasmDraft) -> T) -> T {
    signals
        .processor(ProcessorFamily::Wasm)
        .with(|draft| match draft.wasm() {
            Some(wasm) => read(wasm),
            None => T::default(),
        })
}

#[component]
pub(super) fn ProcessorWasmLimitsEditor(signals: CreateSignals) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    let section = view! {
        <section class="create-wasm-limits">
            <h3>"Guest limits"</h3>
            <p>"MAX FUEL bounds one logical guest operation and MAX MEMORY each branch instance's linear memory; both are required. The guest owns when its routes emit: guest output and the timeouts it requests set the cadence, so a WASM route declares no flush."</p>
            <label class="create-field"><span>"MAX FUEL, a whole number of fuel units"</span>
                <input class="create-wasm-max-fuel" type="text" inputmode="numeric" autocomplete="off"
                    prop:value=move || read_wasm(signals, |wasm| wasm.max_fuel.clone())
                    disabled=pending
                    on:input=move |event| {
                        let value = event_target_value(&event);
                        edit_wasm(signals, |wasm| wasm.max_fuel = value);
                    } />
            </label>
            <label class="create-field"><span>"MAX MEMORY, a byte size such as 64MiB"</span>
                <input class="create-wasm-max-memory" type="text" autocomplete="off" spellcheck="false"
                    prop:value=move || read_wasm(signals, |wasm| wasm.max_memory.clone())
                    disabled=pending
                    on:input=move |event| {
                        let value = event_target_value(&event);
                        edit_wasm(signals, |wasm| wasm.max_memory = value);
                    } />
            </label>
        </section>
    };
    // Erased at the section boundary: the processor editor composing this section then holds and
    // names a boxed view, so neither its rendering frames nor the debug names of every form that
    // embeds it repeat this section's complete nested view.
    section.into_any()
}

#[component]
pub(super) fn ProcessorWasmPoliciesEditor(signals: CreateSignals) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    let rejected = move || read_wasm(signals, |wasm| wasm.rejected_state);
    let global = move || read_wasm(signals, |wasm| wasm.global_error.clone());
    let section = view! {
        <section class="create-wasm-policies">
            <h3>"Guest state and errors"</h3>
            <fieldset class="create-field create-wasm-rejected-state">
                <legend>"ON REJECTED STATE"</legend>
                <button type="button" data-policy="preserve"
                    class:active=move || rejected() == WasmRejectedStatePolicy::Preserve
                    disabled=pending
                    on:click=move |_| edit_wasm(signals, |wasm| wasm.rejected_state = WasmRejectedStatePolicy::Preserve)>"PRESERVE"</button>
                <button type="button" data-policy="reset"
                    class:active=move || rejected() == WasmRejectedStatePolicy::Reset
                    disabled=pending
                    on:click=move |_| edit_wasm(signals, |wasm| wasm.rejected_state = WasmRejectedStatePolicy::Reset)>"RESET"</button>
            </fieldset>
            <p class="create-wasm-rejected-state-consequence">{move || rejected_state_consequence(rejected())}</p>
            <fieldset class="create-field create-wasm-global-error">
                <legend>"ON GLOBAL ERROR"</legend>
                <button type="button" data-error="log"
                    class:active=move || global() == Some(GeneralErrorPolicy::Log)
                    disabled=pending
                    on:click=move |_| edit_wasm(signals, |wasm| wasm.global_error = Some(GeneralErrorPolicy::Log))>"LOG"</button>
                <button type="button" data-error="ignore"
                    class:active=move || global() == Some(GeneralErrorPolicy::Ignore)
                    disabled=pending
                    on:click=move |_| edit_wasm(signals, |wasm| wasm.global_error = Some(GeneralErrorPolicy::Ignore))>"IGNORE"</button>
            </fieldset>
        </section>
    };
    // Erased at the section boundary, as every section of a create form is.
    section.into_any()
}
