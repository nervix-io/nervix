//! VHOST hostname and TLS resource controls.
//!
//! Layer: edges.
//!
//! - **Owns.** Browser presentation of hostnames and the optional certificate pin.
//! - **Depends on.** VHOST browser drafts and shared typed choices.
//! - **Must not know.** HTTPS listener installation or certificate loading.

use futures_channel::mpsc::UnboundedSender;
use leptos::prelude::*;

use super::{
    ChoiceControl, ChoiceGroup, ConsoleRequest, CreateSignals, event_target_checked,
    event_target_value,
};

#[component]
pub(super) fn VhostEditor(
    signals: CreateSignals,
    name_input: NodeRef<leptos::html::Input>,
    request_tx: RwSignal<Option<UnboundedSender<ConsoleRequest>>>,
    session_generation: RwSignal<u64>,
) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    view! {
        <label class="create-field"><span>"VHOST name"</span>
            <input node_ref=name_input class="create-name" type="text" autocomplete="off"
                prop:value=move || signals.vhost.get().name disabled=pending
                on:input=move |event| {
                    signals.vhost.update(|draft| draft.name = event_target_value(&event));
                    signals.edit();
                } />
        </label>
        <div class="create-vhost-hostnames">
            <p>"Hostnames"</p>
            <For each=move || { (0..signals.vhost.get().hostnames.len()).collect::<Vec<_>>() }
                key=|index| *index
                children=move |index| view! {
                    <div class="create-field-row create-vhost-hostname-row">
                        <label class="create-field"><span>{format!("Hostname {}", index + 1)}</span>
                            <input class="create-vhost-hostname" type="text" autocomplete="off"
                                prop:value=move || match signals.vhost.get().hostnames.get(index) {
                                    Some(value) => value.clone(), None => String::new(),
                                }
                                disabled=pending
                                on:input=move |event| {
                                    let value = event_target_value(&event);
                                    signals.vhost.update(|draft| {
                                        if let Some(hostname) = draft.hostnames.get_mut(index) { *hostname = value; }
                                    });
                                    signals.edit();
                                } />
                        </label>
                        <button type="button" class="create-vhost-hostname-remove" disabled=pending
                            on:click=move |_| {
                                signals.vhost.update(|draft| {
                                    if index < draft.hostnames.len() { draft.hostnames.remove(index); }
                                });
                                signals.edit();
                            }>"Remove"</button>
                    </div>
                } />
            <button type="button" class="create-vhost-hostname-add" disabled=pending
                on:click=move |_| {
                    signals.vhost.update(|draft| draft.hostnames.push(String::new()));
                    signals.edit();
                }>"Add hostname"</button>
        </div>
        <label class="create-check"><input class="create-vhost-tls-enabled" type="checkbox"
            prop:checked=move || signals.vhost.get().tls_enabled disabled=pending
            on:change=move |event| {
                signals.vhost.update(|draft| draft.tls_enabled = event_target_checked(&event));
                signals.edit();
            } /><span>"Serve HTTPS/WSS with a resource certificate"</span></label>
        <Show when=move || signals.vhost.get().tls_enabled fallback=|| ()>
            <ChoiceGroup class_name="create-vhost-resource" label="TLS resource"
                control=ChoiceControl::VhostResource signals=signals request_tx=request_tx
                session_generation=session_generation />
            <ChoiceGroup class_name="create-vhost-version" label="Completed TLS version"
                control=ChoiceControl::VhostVersion signals=signals request_tx=request_tx
                session_generation=session_generation />
        </Show>
    }
}
