//! Resource-backed hash map controls for the create popup.
//!
//! Layer: edges.
//!
//! - **Owns.** Browser presentation of a hash map's resource, version, codec, key, and file path.
//! - **Depends on.** Browser draft signals and the shared typed choice UI.
//! - **Must not know.** Lookup loading, resource storage, or command execution.

use futures_channel::mpsc::UnboundedSender;
use leptos::prelude::*;

use super::{ChoiceControl, ChoiceGroup, ConsoleRequest, CreateSignals, event_target_value};

#[component]
pub(super) fn HashMapEditor(
    signals: CreateSignals,
    name_input: NodeRef<leptos::html::Input>,
    request_tx: RwSignal<Option<UnboundedSender<ConsoleRequest>>>,
    session_generation: RwSignal<u64>,
) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    view! {
        <label class="create-field">
            <span>"Hash map name"</span>
            <input node_ref=name_input class="create-name" type="text" autocomplete="off"
                prop:value=move || signals.hash_map.get().name
                disabled=pending
                on:input=move |event| {
                    let name = event_target_value(&event);
                    signals.hash_map.update(|draft| draft.name = name);
                    signals.edit();
                } />
        </label>
        <ChoiceGroup class_name="create-hash-resource" label="Resource"
            control=ChoiceControl::HashResource signals=signals request_tx=request_tx
            session_generation=session_generation />
        <ChoiceGroup class_name="create-hash-version" label="Completed version"
            control=ChoiceControl::HashVersion signals=signals request_tx=request_tx
            session_generation=session_generation />
        <ChoiceGroup class_name="create-hash-codec" label="Codec"
            control=ChoiceControl::HashCodec signals=signals request_tx=request_tx
            session_generation=session_generation show_detail=true />
        <ChoiceGroup class_name="create-hash-key" label="Key field"
            control=ChoiceControl::HashKey signals=signals request_tx=request_tx
            session_generation=session_generation show_detail=true />
        <Show when=move || signals.hash_map.get().key_field.is_some() fallback=|| ()>
            <p class="create-selected-key">{move || match signals.hash_map.get().key_field {
                Some(selected) => format!("Selected key: {}", selected.name()),
                None => String::new(),
            }}</p>
        </Show>
        <Show when=move || {
            let draft = signals.hash_map.get();
            draft.pin.resource.as_ref().is_some_and(|selected| !selected.is_current())
                || draft.pin.version.as_ref().is_some_and(|selected| !selected.is_current())
                || draft.codec.as_ref().is_some_and(|selected| !selected.is_current())
                || draft.key_field.as_ref().is_some_and(|selected| !selected.is_current())
        } fallback=|| ()>
            <p class="create-reference-invalid" role="alert">"The selected domain, resource or codec changed. Select the affected references again."</p>
        </Show>
        <label class="create-field">
            <span>"File path inside resource"</span>
            <input class="create-hash-path" type="text" autocomplete="off"
                prop:value=move || signals.hash_map.get().path
                disabled=pending
                on:input=move |event| {
                    let path = event_target_value(&event);
                    signals.hash_map.update(|draft| draft.path = path);
                    signals.edit();
                } />
        </label>
    }
}
