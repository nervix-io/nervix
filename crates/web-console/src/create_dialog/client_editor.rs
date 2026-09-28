//! Structured client transport, pool, mount, signaling, and CONFIG controls.
//!
//! Layer: edges.
//!
//! - **Owns.** Browser controls for every offered client transport and its conditional fields.
//! - **Depends on.** Client browser drafts and the shared typed choice component.
//! - **Must not know.** Connector drivers, external credentials, or runtime startup.

use futures_channel::mpsc::UnboundedSender;
use leptos::prelude::*;

use super::{
    ChoiceControl, ChoiceGroup, ConsoleRequest, CreateSignals,
    client_draft::{ClientConfigDraft, ClientTransport},
    event_target_checked, event_target_value,
};

#[component]
pub(super) fn ClientEditor(
    signals: CreateSignals,
    name_input: NodeRef<leptos::html::Input>,
    request_tx: RwSignal<Option<UnboundedSender<ConsoleRequest>>>,
    session_generation: RwSignal<u64>,
) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    view! {
        <label class="create-field">
            <span>"Client name"</span>
            <input node_ref=name_input class="create-name" type="text" autocomplete="off"
                prop:value=move || signals.client.get().name disabled=pending
                on:input=move |event| {
                    signals.client.update(|draft| draft.name = event_target_value(&event));
                    signals.edit();
                } />
        </label>
        <fieldset class="create-choice-group create-client-type">
            <legend>"Transport"</legend>
            <div class="create-choice-buttons">
                <For each=move || ClientTransport::ALL.to_vec()
                    key=|transport| transport.key()
                    children=move |transport| view! {
                        <button type="button" data-type=transport.key()
                            class:active=move || signals.client.get().transport == Some(transport)
                            disabled=pending
                            on:click=move |_| {
                                signals.client.update(|draft| draft.set_transport(transport));
                                signals.edit();
                            }>{transport.label()}</button>
                    } />
            </div>
        </fieldset>
        <Show when=move || signals.client.get().transport.is_some_and(ClientTransport::pooled) fallback=|| ()>
            <div class="create-field-row create-client-pool">
                <label class="create-field"><span>"Pool minimum · required"</span>
                    <input class="create-client-pool-min" type="number" min="0" step="1"
                        prop:value=move || signals.client.get().minimum disabled=pending
                        on:input=move |event| {
                            signals.client.update(|draft| draft.minimum = event_target_value(&event));
                            signals.edit();
                        } />
                </label>
                <label class="create-field"><span>"Pool maximum · required"</span>
                    <input class="create-client-pool-max" type="number" min="1" step="1"
                        prop:value=move || signals.client.get().maximum disabled=pending
                        on:input=move |event| {
                            signals.client.update(|draft| draft.maximum = event_target_value(&event));
                            signals.edit();
                        } />
                </label>
            </div>
        </Show>
        <label class="create-check"><input class="create-client-mount-enabled" type="checkbox"
            prop:checked=move || signals.client.get().mount_enabled disabled=pending
            on:change=move |event| {
                signals.client.update(|draft| draft.mount_enabled = event_target_checked(&event));
                signals.edit();
            } /><span>"Mount an existing resource version"</span></label>
        <Show when=move || signals.client.get().mount_enabled fallback=|| ()>
            <ChoiceGroup class_name="create-client-resource" label="Mount resource"
                control=ChoiceControl::ClientResource signals=signals request_tx=request_tx
                session_generation=session_generation />
            <ChoiceGroup class_name="create-client-version" label="Completed mount version"
                control=ChoiceControl::ClientVersion signals=signals request_tx=request_tx
                session_generation=session_generation />
        </Show>
        <Show when=move || signals.client.get().transport.is_some_and(ClientTransport::websockets) fallback=|| ()>
            <ChoiceGroup class_name="create-client-signaling" label="Signaling protocol · optional"
                control=ChoiceControl::ClientSignaling signals=signals request_tx=request_tx
                session_generation=session_generation />
            <Show when=move || signals.client.get().signaling_protocol.is_some() fallback=|| ()>
                <button type="button" class="create-client-signaling-clear" disabled=pending
                    on:click=move |_| {
                        signals.client.update(|draft| draft.signaling_protocol = None);
                        signals.edit();
                    }>"No signaling protocol"</button>
            </Show>
        </Show>
        <div class="create-client-config">
            <p>"Connector CONFIG · key/value pairs; external services and objects must already exist"</p>
            <For each=move || { (0..signals.client.get().config.len()).collect::<Vec<_>>() }
                key=|index| *index
                children=move |index| view! {
                    <div class="create-client-config-entry create-field-row">
                        <label class="create-field"><span>"Key"</span>
                            <input class="create-client-config-key" type="text" autocomplete="off"
                                prop:value=move || match signals.client.get().config.get(index) {
                                    Some(entry) => entry.key.clone(), None => String::new(),
                                }
                                disabled=pending
                                on:input=move |event| {
                                    let value = event_target_value(&event);
                                    signals.client.update(|draft| {
                                        if let Some(entry) = draft.config.get_mut(index) { entry.key = value; }
                                    });
                                    signals.edit();
                                } />
                        </label>
                        <label class="create-field"><span>"Value"</span>
                            <input class="create-client-config-value"
                                type=move || if signals.client.get().config.get(index)
                                    .is_some_and(ClientConfigDraft::is_secret) { "password" } else { "text" }
                                autocomplete="off"
                                prop:value=move || match signals.client.get().config.get(index) {
                                    Some(entry) => entry.value.clone(), None => String::new(),
                                }
                                disabled=pending
                                on:input=move |event| {
                                    let value = event_target_value(&event);
                                    signals.client.update(|draft| {
                                        if let Some(entry) = draft.config.get_mut(index) { entry.value = value; }
                                    });
                                    signals.edit();
                                } />
                        </label>
                        <label class="create-check"><input class="create-client-config-secret" type="checkbox"
                            prop:checked=move || signals.client.get().config.get(index)
                                .is_some_and(|entry| entry.secret)
                            disabled=pending
                            on:change=move |event| {
                                let secret = event_target_checked(&event);
                                signals.client.update(|draft| {
                                    if let Some(entry) = draft.config.get_mut(index) { entry.secret = secret; }
                                });
                                signals.edit();
                            } /><span>"Secret"</span></label>
                        <button type="button" class="create-client-config-remove" disabled=pending
                            on:click=move |_| {
                                signals.client.update(|draft| {
                                    if index < draft.config.len() { draft.config.remove(index); }
                                });
                                signals.edit();
                            }>"Remove"</button>
                    </div>
                } />
            <button type="button" class="create-client-config-add" disabled=pending
                on:click=move |_| {
                    signals.client.update(|draft| draft.config.push(ClientConfigDraft::default()));
                    signals.edit();
                }>"Add configuration"</button>
        </div>
    }
}
