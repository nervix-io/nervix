//! HTTP and WebSocket endpoint controls.
//!
//! Layer: edges.
//!
//! - **Owns.** Browser presentation of endpoint VHOST, path, type and signaling selections.
//! - **Depends on.** Endpoint browser drafts and shared typed choices.
//! - **Must not know.** Listener routing or WebSocket handshake execution.

use futures_channel::mpsc::UnboundedSender;
use leptos::prelude::*;
use nervix_models::EndpointType;

use super::{ChoiceControl, ChoiceGroup, ConsoleRequest, CreateSignals, event_target_value};

#[component]
pub(super) fn EndpointEditor(
    signals: CreateSignals,
    name_input: NodeRef<leptos::html::Input>,
    request_tx: RwSignal<Option<UnboundedSender<ConsoleRequest>>>,
    session_generation: RwSignal<u64>,
) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    view! {
        <label class="create-field"><span>"Endpoint name"</span>
            <input node_ref=name_input class="create-name" type="text" autocomplete="off"
                prop:value=move || signals.endpoint.get().name disabled=pending
                on:input=move |event| {
                    signals.endpoint.update(|draft| draft.name = event_target_value(&event));
                    signals.edit();
                } />
        </label>
        <ChoiceGroup class_name="create-endpoint-vhost" label="VHOST"
            control=ChoiceControl::EndpointVhost signals=signals request_tx=request_tx
            session_generation=session_generation />
        <label class="create-field"><span>"Path"</span>
            <input class="create-endpoint-path" type="text" autocomplete="off"
                prop:value=move || signals.endpoint.get().path disabled=pending
                on:input=move |event| {
                    signals.endpoint.update(|draft| draft.path = event_target_value(&event));
                    signals.edit();
                } />
        </label>
        <fieldset class="create-choice-group create-endpoint-type">
            <legend>"Endpoint type"</legend>
            <div class="create-choice-buttons">
                <button type="button" data-type="http"
                    class:active=move || signals.endpoint.get().endpoint_type == Some(EndpointType::Http)
                    disabled=pending
                    on:click=move |_| {
                        signals.endpoint.update(|draft| draft.select_type(EndpointType::Http));
                        signals.edit();
                    }>"HTTP"</button>
                <button type="button" data-type="websockets"
                    class:active=move || signals.endpoint.get().endpoint_type == Some(EndpointType::Websockets)
                    disabled=pending
                    on:click=move |_| {
                        signals.endpoint.update(|draft| draft.select_type(EndpointType::Websockets));
                        signals.edit();
                    }>"WEBSOCKETS"</button>
            </div>
        </fieldset>
        <Show when=move || signals.endpoint.get().endpoint_type == Some(EndpointType::Websockets) fallback=|| ()>
            <ChoiceGroup class_name="create-endpoint-signaling" label="Signaling protocol · optional"
                control=ChoiceControl::EndpointSignaling signals=signals request_tx=request_tx
                session_generation=session_generation />
            <Show when=move || signals.endpoint.get().signaling_protocol.is_some() fallback=|| ()>
                <button type="button" class="create-endpoint-signaling-clear" disabled=pending
                    on:click=move |_| {
                        signals.endpoint.update(|draft| draft.signaling_protocol = None);
                        signals.edit();
                    }>"No signaling protocol"</button>
            </Show>
        </Show>
    }
}
