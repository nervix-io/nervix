//! The materialized source and cadence of a visual generator.
//!
//! Layer: edges.
//!
//! - **Owns.** Browser editing of a generator's node-wide branch, the materialized relay it reads
//!   on exactly that branch, and its `EACH` period.
//! - **Depends on.** The generator draft, typed choice controls and Leptos signals.
//! - **Must not know.** Generator scheduling, materialized-state reads or registry validation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use leptos::prelude::*;

use super::{
    ChoiceControl, CreateSignals, RequestSender, choice_group::ChoiceGroup, event_target_value,
    processor_draft::ProcessorFamily, processor_node_branch_editor::ProcessorNodeBranch,
};

#[component]
pub(super) fn ProcessorGeneratorEditor(
    signals: CreateSignals,
    request_tx: RwSignal<Option<RequestSender>>,
    session_generation: RwSignal<u64>,
) -> impl IntoView {
    let draft = signals.processor(ProcessorFamily::Generator);
    let pending = move || signals.progress.get().is_pending();
    let section = view! {
        <section class="create-generator-source">
            <h3>"Materialized state and cadence"</h3>
            <p>"A generator reads the materialized state of one relay on exactly its own branch, and generates for each concrete branch on the domain clock: the first occurrence is immediate and later ones stay on the EACH schedule. Each route releases what it generates on its own declared flush."</p>
            <ProcessorNodeBranch family=ProcessorFamily::Generator signals=signals request_tx=request_tx session_generation=session_generation />
            <ChoiceGroup class_name="create-generator-relay" label="Materialized relay"
                control=ChoiceControl::GeneratorRelay signals=signals request_tx=request_tx
                session_generation=session_generation show_detail=true />
            <Show when=move || match draft.get().generator() {
                Some(generator) => generator.relay_changed(),
                None => false,
            } fallback=|| ()>
                <p class="create-reference-invalid" role="alert">"The selected domain or branch changed. Select the materialized relay again."</p>
            </Show>
            <label class="create-field"><span>"EACH domain-clock period, such as 100ms"</span>
                <input class="create-generator-each" type="text" autocomplete="off" spellcheck="false"
                    prop:value=move || {
                        let state = draft.get();
                        match state.generator() {
                            Some(generator) => generator.each.clone(),
                            None => String::new(),
                        }
                    }
                    disabled=pending
                    on:input=move |event| {
                        let value = event_target_value(&event);
                        draft.update(|state| {
                            if let Some(generator) = state.generator_mut() { generator.each = value; }
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
