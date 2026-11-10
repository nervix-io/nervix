//! The resource, version and file a visual inferencer or WASM processor loads.
//!
//! Layer: edges.
//!
//! - **Owns.** Browser presentation of a processor's resource and completed-version choices, the
//!   file it loads from that version, and the notice that a changed context invalidated them.
//! - **Depends on.** Processor drafts, typed resource choices and Leptos signals.
//! - **Must not know.** Resource storage, version pinning at application, or loading the file.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use leptos::prelude::*;

use super::{
    ChoiceControl, CreateSignals, RequestSender, choice_group::ChoiceGroup, event_target_value,
    processor_draft::ProcessorFamily,
};

/// The static presentation of one family's binding.
struct BindingText {
    heading: &'static str,
    explanation: &'static str,
    file_label: &'static str,
}

/// The presentation of `family`'s binding, for the families that load a resource.
fn binding_text(family: ProcessorFamily) -> Option<BindingText> {
    match family {
        ProcessorFamily::Inferencer => Some(BindingText {
            heading: "Model resource",
            explanation: "The inferencer loads its model from the one version it stores. LATEST \
                          stays the requested version until the statement is applied, which pins \
                          the newest completed version; a later upload does not change the model.",
            file_label: "ONNX model file inside the version",
        }),
        ProcessorFamily::Wasm => Some(BindingText {
            heading: "Guest module",
            explanation: "The processor compiles its guest from the one version it stores. LATEST \
                          stays the requested version until the statement is applied, which pins \
                          the newest completed version; a later upload does not change the guest.",
            file_label: "WASM module file inside the version",
        }),
        ProcessorFamily::Junction
        | ProcessorFamily::Deduplicator
        | ProcessorFamily::Reorderer
        | ProcessorFamily::Window
        | ProcessorFamily::Correlator
        | ProcessorFamily::Reingestor
        | ProcessorFamily::Generator => None,
    }
}

#[component]
pub(super) fn ProcessorBindingEditor(
    family: ProcessorFamily,
    signals: CreateSignals,
    request_tx: RwSignal<Option<RequestSender>>,
    session_generation: RwSignal<u64>,
) -> impl IntoView {
    let Some(text) = binding_text(family) else {
        return ().into_any();
    };
    let draft = signals.processor(family);
    let pending = move || signals.progress.get().is_pending();
    let section = view! {
        <section class="create-processor-binding">
            <h3>{text.heading}</h3>
            <p>{text.explanation}</p>
            <ChoiceGroup class_name="create-processor-resource" label="Resource"
                control=ChoiceControl::ProcessorResource signals=signals request_tx=request_tx
                session_generation=session_generation />
            <ChoiceGroup class_name="create-processor-resource-version" label="Completed version"
                control=ChoiceControl::ProcessorResourceVersion signals=signals request_tx=request_tx
                session_generation=session_generation />
            <Show when=move || match draft.get().binding() {
                Some(binding) => binding.pin.changed(),
                None => false,
            } fallback=|| ()>
                <p class="create-reference-invalid" role="alert">"The selected domain or resource changed. Select the resource and its version again."</p>
            </Show>
            <label class="create-field"><span>{text.file_label}</span>
                <input class="create-processor-file" type="text" autocomplete="off" spellcheck="false"
                    prop:value=move || {
                        let state = draft.get();
                        match state.binding() {
                            Some(binding) => binding.file.clone(),
                            None => String::new(),
                        }
                    }
                    disabled=pending
                    on:input=move |event| {
                        let value = event_target_value(&event);
                        draft.update(|state| {
                            if let Some(binding) = state.binding_mut() { binding.file = value; }
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
