//! Structured field and branch controls for the create popup.
//!
//! Layer: edges.
//!
//! - **Owns.** Ordered field controls, nested type layers, wire type and mode selection, and the
//!   branch's typed schema reference control.
//! - **Depends on.** Browser draft signals, model-owned type variants, and the shared choice UI.
//! - **Must not know.** Registry internals, statement parsing, or how a command is dispatched.

use leptos::prelude::*;
use nervix_models::{AvroType, JsonType, ParseAsType, WireSchemaStrictness};
use strum::IntoEnumIterator as _;

use super::{
    ChoiceControl, ChoiceGroup, CreateSignals, RequestSender, event_target_checked,
    event_target_value,
    schema_draft::{CollectionLayer, SchemaFieldDraft, WireFieldDraft, WireFieldType, WireFormat},
};

#[component]
pub(super) fn SchemaEditor(
    signals: CreateSignals,
    name_input: NodeRef<leptos::html::Input>,
) -> impl IntoView {
    view! {
        <label class="create-field">
            <span>"Schema name"</span>
            <input node_ref=name_input class="create-name" type="text" autocomplete="off"
                prop:value=move || signals.structured.get().schema.name
                disabled=move || signals.progress.get().is_pending()
                on:input=move |event| {
                    let name = event_target_value(&event);
                    signals.structured.update(|drafts| drafts.schema.name = name);
                    signals.edit();
                } />
        </label>
        <div class="create-fields" aria-label="Schema fields">
            <For
                each=move || { (0..signals.structured.get().schema.fields.len()).collect::<Vec<_>>() }
                key=|index| *index
                children=move |index| view! { <SchemaFieldEditor signals=signals index=index /> }
            />
            <button class="create-add-field" type="button"
                disabled=move || signals.progress.get().is_pending()
                on:click=move |_| {
                    signals.structured.update(|drafts| drafts.schema.fields.push(SchemaFieldDraft::default()));
                    signals.edit();
                }>"Add field"</button>
        </div>
    }
}

#[component]
fn SchemaFieldEditor(signals: CreateSignals, index: usize) -> impl IntoView {
    view! {
        <div class="create-field-entry">
            <div class="create-field-entry-head">
                <strong>{format!("Field {}", index + 1)}</strong>
                <div class="create-field-actions">
                    <button type="button" aria-label=format!("Move field {} up", index + 1)
                        disabled=move || signals.progress.get().is_pending() || index == 0
                        on:click=move |_| {
                            if index > 0 {
                                signals.structured.update(|drafts| drafts.schema.move_field_up(index));
                                signals.edit();
                            }
                        }>"↑"</button>
                    <button type="button" aria-label=format!("Move field {} down", index + 1)
                        disabled=move || signals.progress.get().is_pending()
                        on:click=move |_| {
                            if index.checked_add(1).is_some() {
                                signals.structured.update(|drafts| drafts.schema.move_field_down(index));
                                signals.edit();
                            }
                        }>"↓"</button>
                    <button type="button" aria-label=format!("Remove field {}", index + 1)
                        disabled=move || signals.progress.get().is_pending()
                        on:click=move |_| {
                            signals.structured.update(|drafts| drafts.schema.remove_field(index));
                            signals.edit();
                        }>"Remove"</button>
                </div>
            </div>
            <label class="create-field">
                <span>"Field name"</span>
                <input class="create-field-name" type="text" autocomplete="off"
                    prop:value=move || {
                        let drafts = signals.structured.get();
                        match drafts.schema.fields.get(index) {
                            Some(field) => field.name.clone(),
                            None => String::new(),
                        }
                    }
                    disabled=move || signals.progress.get().is_pending()
                    on:input=move |event| {
                        let name = event_target_value(&event);
                        signals.structured.update(|drafts| drafts.schema.set_field_name(index, name));
                        signals.edit();
                    } />
            </label>
            <fieldset class="create-field-type">
                <legend>"Scalar type"</legend>
                <div class="create-type-options">
                    <For
                        each=move || { ParseAsType::scalar_variants().to_vec() }
                        key=|ty| ty.to_string()
                        children=move |ty| {
                            let label = ty.to_string();
                            let selected = ty.clone();
                            view! {
                                <button type="button" data-type=label.clone()
                                    class:active=move || {
                                        let drafts = signals.structured.get();
                                        drafts.schema.fields.get(index).is_some_and(|field| field.ty.scalar.as_ref() == Some(&selected))
                                    }
                                    disabled=move || signals.progress.get().is_pending()
                                    on:click=move |_| {
                                        signals.structured.update(|drafts| drafts.schema.set_field_scalar(index, ty.clone()));
                                        signals.edit();
                                    }>{label.clone()}</button>
                            }
                        }
                    />
                </div>
            </fieldset>
            <div class="create-type-layers">
                <span>"Collection layers · inner to outer"</span>
                <For
                    each=move || {
                        let drafts = signals.structured.get();
                        match drafts.schema.fields.get(index) {
                            Some(field) => (0..field.ty.layers.len()).collect::<Vec<_>>(),
                            None => Vec::new(),
                        }
                    }
                    key=|layer| *layer
                    children=move |layer| view! {
                        <div class="create-type-layer">
                            <span>{move || {
                                let drafts = signals.structured.get();
                                match drafts.schema.fields.get(index).and_then(|field| field.ty.layers.get(layer)) {
                                    Some(CollectionLayer::Vector) => "Vector".to_string(),
                                    Some(CollectionLayer::Array { .. }) => "Fixed array".to_string(),
                                    None => String::new(),
                                }
                            }}</span>
                            <Show when=move || {
                                let drafts = signals.structured.get();
                                matches!(drafts.schema.fields.get(index).and_then(|field| field.ty.layers.get(layer)), Some(CollectionLayer::Array { .. }))
                            } fallback=|| ()>
                                <label class="create-field">
                                    <span>"Length"</span>
                                    <input class="create-array-length" type="number" min="1" max="2147483647"
                                        prop:value=move || {
                                            let drafts = signals.structured.get();
                                            match drafts.schema.fields.get(index).and_then(|field| field.ty.layers.get(layer)) {
                                                Some(CollectionLayer::Array { length }) => length.clone(),
                                                Some(CollectionLayer::Vector) | None => String::new(),
                                            }
                                        }
                                        disabled=move || signals.progress.get().is_pending()
                                        on:input=move |event| {
                                            let value = event_target_value(&event);
                                            signals.structured.update(|drafts| drafts.schema.set_array_length(index, layer, value));
                                            signals.edit();
                                        } />
                                </label>
                            </Show>
                            <button type="button" aria-label=format!("Remove collection layer {}", layer + 1)
                                disabled=move || signals.progress.get().is_pending()
                                on:click=move |_| {
                                    signals.structured.update(|drafts| drafts.schema.remove_field_layer(index, layer));
                                    signals.edit();
                                }>"Remove"</button>
                        </div>
                    }
                />
                <div class="create-type-layer-add">
                    <button class="create-add-vector" type="button" disabled=move || signals.progress.get().is_pending()
                        on:click=move |_| {
                            signals.structured.update(|drafts| drafts.schema.push_field_layer(index, CollectionLayer::Vector));
                            signals.edit();
                        }>"Wrap in vector"</button>
                    <button class="create-add-array" type="button" disabled=move || signals.progress.get().is_pending()
                        on:click=move |_| {
                            signals.structured.update(|drafts| drafts.schema.push_field_layer(index, CollectionLayer::Array { length: String::new() }));
                            signals.edit();
                        }>"Wrap in fixed array"</button>
                </div>
            </div>
            <label class="create-check">
                <input class="create-field-optional" type="checkbox"
                    prop:checked=move || signals.structured.get().schema.fields.get(index).is_some_and(|field| field.optional)
                    disabled=move || signals.progress.get().is_pending()
                    on:change=move |event| {
                        let checked = event_target_checked(&event);
                        signals.structured.update(|drafts| drafts.schema.set_field_optional(index, checked));
                        signals.edit();
                    } />
                <span>"Optional"</span>
            </label>
            <label class="create-check">
                <input class="create-field-sensitive" type="checkbox"
                    prop:checked=move || signals.structured.get().schema.fields.get(index).is_some_and(|field| field.sensitive)
                    disabled=move || signals.progress.get().is_pending()
                    on:change=move |event| {
                        let checked = event_target_checked(&event);
                        signals.structured.update(|drafts| drafts.schema.set_field_sensitive(index, checked));
                        signals.edit();
                    } />
                <span>"Sensitive"</span>
            </label>
        </div>
    }
}

#[component]
pub(super) fn WireSchemaEditor(
    signals: CreateSignals,
    name_input: NodeRef<leptos::html::Input>,
    format: WireFormat,
) -> impl IntoView {
    view! {
        <label class="create-field">
            <span>{format!("Wire {} schema name", format.label())}</span>
            <input node_ref=name_input class="create-name" type="text" autocomplete="off"
                prop:value=move || signals.structured.get().wire(format).name.clone()
                disabled=move || signals.progress.get().is_pending()
                on:input=move |event| {
                    let name = event_target_value(&event);
                    signals.structured.update(|drafts| drafts.wire_mut(format).name = name);
                    signals.edit();
                } />
        </label>
        <fieldset class="create-mode-options">
            <legend>"Unknown fields"</legend>
            <div class="create-choice-buttons">
                <For each=move || { WireSchemaStrictness::iter().collect::<Vec<_>>() } key=|mode| mode.as_ref().to_string()
                    children=move |mode| {
                        let label = mode.as_ref().to_string();
                        view! {
                            <button type="button" data-value=label.clone()
                                class:active=move || signals.structured.get().wire(format).mode == Some(mode)
                                disabled=move || signals.progress.get().is_pending()
                                on:click=move |_| {
                                    signals.structured.update(|drafts| drafts.wire_mut(format).mode = Some(mode));
                                    signals.edit();
                                }>{label.clone()}</button>
                        }
                    } />
            </div>
        </fieldset>
        <div class="create-fields" aria-label=format!("Wire {} fields", format.label())>
            <For
                each=move || { (0..signals.structured.get().wire(format).fields.len()).collect::<Vec<_>>() }
                key=|index| *index
                children=move |index| view! { <WireFieldEditor signals=signals format=format index=index /> }
            />
            <button class="create-add-field" type="button" disabled=move || signals.progress.get().is_pending()
                on:click=move |_| {
                    signals.structured.update(|drafts| drafts.wire_mut(format).fields.push(WireFieldDraft::default()));
                    signals.edit();
                }>"Add field"</button>
        </div>
    }
}

fn wire_types(format: WireFormat) -> Vec<WireFieldType> {
    match format {
        WireFormat::Json | WireFormat::Cbor => JsonType::iter().map(WireFieldType::Json).collect(),
        WireFormat::Avro => AvroType::iter().map(WireFieldType::Avro).collect(),
    }
}

fn wire_type_label(ty: WireFieldType) -> String {
    match ty {
        WireFieldType::Json(ty) => ty.as_ref().to_string(),
        WireFieldType::Avro(ty) => ty.as_ref().to_string(),
    }
}

#[component]
fn WireFieldEditor(signals: CreateSignals, format: WireFormat, index: usize) -> impl IntoView {
    view! {
        <div class="create-field-entry">
            <div class="create-field-entry-head">
                <strong>{format!("Field {}", index + 1)}</strong>
                <div class="create-field-actions">
                    <button type="button" aria-label=format!("Move field {} up", index + 1)
                        disabled=move || signals.progress.get().is_pending() || index == 0
                        on:click=move |_| {
                            if index > 0 {
                                signals.structured.update(|drafts| drafts.wire_mut(format).move_field_up(index));
                                signals.edit();
                            }
                        }>"↑"</button>
                    <button type="button" aria-label=format!("Move field {} down", index + 1)
                        disabled=move || signals.progress.get().is_pending()
                        on:click=move |_| {
                            if index.checked_add(1).is_some() {
                                signals.structured.update(|drafts| drafts.wire_mut(format).move_field_down(index));
                                signals.edit();
                            }
                        }>"↓"</button>
                    <button type="button" aria-label=format!("Remove field {}", index + 1)
                        disabled=move || signals.progress.get().is_pending()
                        on:click=move |_| {
                            signals.structured.update(|drafts| drafts.wire_mut(format).remove_field(index));
                            signals.edit();
                        }>"Remove"</button>
                </div>
            </div>
            <label class="create-field">
                <span>"Field name"</span>
                <input class="create-field-name" type="text" autocomplete="off"
                    prop:value=move || {
                        let drafts = signals.structured.get();
                        match drafts.wire(format).fields.get(index) {
                            Some(field) => field.name.clone(),
                            None => String::new(),
                        }
                    }
                    disabled=move || signals.progress.get().is_pending()
                    on:input=move |event| {
                        let name = event_target_value(&event);
                        signals.structured.update(|drafts| drafts.wire_mut(format).set_field_name(index, name));
                        signals.edit();
                    } />
            </label>
            <fieldset class="create-field-type">
                <legend>"Wire type"</legend>
                <div class="create-type-options">
                    <For each=move || { wire_types(format) } key=|ty| wire_type_label(*ty)
                        children=move |ty| {
                            let label = wire_type_label(ty);
                            view! {
                                <button type="button" data-type=label.clone()
                                    class:active=move || signals.structured.get().wire(format).fields.get(index).is_some_and(|field| field.ty == Some(ty))
                                    disabled=move || signals.progress.get().is_pending()
                                    on:click=move |_| {
                                        signals.structured.update(|drafts| drafts.wire_mut(format).set_field_type(index, ty));
                                        signals.edit();
                                    }>{label.clone()}</button>
                            }
                        } />
                </div>
            </fieldset>
            <label class="create-check">
                <input class="create-field-optional" type="checkbox"
                    prop:checked=move || signals.structured.get().wire(format).fields.get(index).is_some_and(|field| field.optional)
                    disabled=move || signals.progress.get().is_pending()
                    on:change=move |event| {
                        let checked = event_target_checked(&event);
                        signals.structured.update(|drafts| drafts.wire_mut(format).set_field_optional(index, checked));
                        signals.edit();
                    } />
                <span>"Optional"</span>
            </label>
        </div>
    }
}

#[component]
pub(super) fn BranchEditor(
    signals: CreateSignals,
    name_input: NodeRef<leptos::html::Input>,
    request_tx: RwSignal<Option<RequestSender>>,
    session_generation: RwSignal<u64>,
) -> impl IntoView {
    view! {
        <label class="create-field">
            <span>"Branch name"</span>
            <input node_ref=name_input class="create-name" type="text" autocomplete="off"
                prop:value=move || signals.structured.get().branch.name
                disabled=move || signals.progress.get().is_pending()
                on:input=move |event| {
                    let name = event_target_value(&event);
                    signals.structured.update(|drafts| drafts.branch.name = name);
                    signals.edit();
                } />
        </label>
        <ChoiceGroup class_name="create-schema-ref" label="Key schema"
            control=ChoiceControl::BranchSchema signals=signals request_tx=request_tx
            session_generation=session_generation />
        <Show when=move || signals.structured.get().branch.schema.is_some() fallback=|| ()>
            <p class="create-selected-schema">{move || {
                let branch = signals.structured.get().branch;
                match branch.schema {
                    Some(schema) => format!("Selected schema: {}", schema.name()),
                    None => String::new(),
                }
            }}</p>
        </Show>
        <Show when=move || {
            let branch = signals.structured.get().branch;
            branch.schema.is_some_and(|schema| !schema.is_current())
        } fallback=|| ()>
            <p class="create-reference-invalid" role="alert">"The selected schema's domain changed. Select a schema again."</p>
        </Show>
        <label class="create-field">
            <span>"TTL"</span>
            <input class="create-ttl" type="text" autocomplete="off"
                prop:value=move || signals.structured.get().branch.ttl
                disabled=move || signals.progress.get().is_pending()
                on:input=move |event| {
                    let ttl = event_target_value(&event);
                    signals.structured.update(|drafts| drafts.branch.ttl = ttl);
                    signals.edit();
                } />
        </label>
        <label class="create-check">
            <input class="create-limit-instances" type="checkbox"
                prop:checked=move || signals.structured.get().branch.limit_instances
                disabled=move || signals.progress.get().is_pending()
                on:change=move |event| {
                    let checked = event_target_checked(&event);
                    signals.structured.update(|drafts| drafts.branch.limit_instances = checked);
                    signals.edit();
                } />
            <span>"Maximum instances with LRU eviction"</span>
        </label>
        <Show when=move || signals.structured.get().branch.limit_instances fallback=|| ()>
            <label class="create-field">
                <span>"Maximum instances"</span>
                <input class="create-max-instances" type="number" min="1"
                    prop:value=move || signals.structured.get().branch.max_instances
                    disabled=move || signals.progress.get().is_pending()
                    on:input=move |event| {
                        let max = event_target_value(&event);
                        signals.structured.update(|drafts| drafts.branch.max_instances = max);
                        signals.edit();
                    } />
            </label>
        </Show>
    }
}
