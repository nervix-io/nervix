//! Ordered typed arguments and a verbatim Roto source editor for the create popup.
//!
//! Layer: edges.
//!
//! - **Owns.** Browser controls for the current UDF language, exact signature, volatility, and
//!   the source bytes containing both the function body and its tests.
//! - **Depends on.** Browser UDF and shared schema type drafts and model-owned type variants.
//! - **Must not know.** Roto compilation, test execution, or registry validation.

use leptos::prelude::*;
use nervix_models::{ParseAsType, UdfLanguage};

use super::{
    CreateSignals, event_target_checked, event_target_textarea_value, event_target_value,
    schema_draft::{CollectionLayer, SchemaTypeDraft},
    udf_draft::{UdfArgumentDraft, UdfDraft},
};

#[derive(Clone, Copy)]
enum TypeTarget {
    Argument(usize),
    Return,
}

impl TypeTarget {
    fn of(self, draft: &UdfDraft) -> Option<&SchemaTypeDraft> {
        match self {
            Self::Argument(index) => draft.arguments.get(index).map(|argument| &argument.ty),
            Self::Return => Some(&draft.returns),
        }
    }

    fn of_mut(self, draft: &mut UdfDraft) -> Option<&mut SchemaTypeDraft> {
        match self {
            Self::Argument(index) => draft
                .arguments
                .get_mut(index)
                .map(|argument| &mut argument.ty),
            Self::Return => Some(&mut draft.returns),
        }
    }
}

fn change_type(
    signals: CreateSignals,
    target: TypeTarget,
    change: impl FnOnce(&mut SchemaTypeDraft),
) {
    signals.udf.update(|draft| {
        if let Some(ty) = target.of_mut(draft) {
            change(ty);
        }
    });
    signals.edit();
}

#[component]
fn UdfTypeEditor(
    signals: CreateSignals,
    target: TypeTarget,
    class_name: &'static str,
) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    view! {
        <fieldset class=format!("create-field-type {class_name}")>
            <legend>"Type"</legend>
            <div class="create-type-options">
                <For each=move || ParseAsType::scalar_variants().to_vec()
                    key=|ty| ty.to_string()
                    children=move |ty| {
                        let label = ty.to_string();
                        let selected = ty.clone();
                        view! {
                            <button type="button" data-type=label.clone()
                                class:active=move || target.of(&signals.udf.get()).is_some_and(|draft| draft.scalar.as_ref() == Some(&selected))
                                disabled=pending
                                on:click=move |_| change_type(signals, target, |draft| draft.scalar = Some(ty.clone()))>
                                {label.clone()}
                            </button>
                        }
                    } />
            </div>
            <div class="create-type-layers">
                <span>"Collection layers · inner to outer"</span>
                <For each=move || {
                    let draft = signals.udf.get();
                    match target.of(&draft) {
                        Some(ty) => (0..ty.layers.len()).collect::<Vec<_>>(),
                        None => Vec::new(),
                    }
                }
                    key=|index| *index
                    children=move |layer| view! {
                        <div class="create-type-layer">
                            <span>{move || {
                                let draft = signals.udf.get();
                                match target.of(&draft).and_then(|ty| ty.layers.get(layer)) {
                                    Some(CollectionLayer::Vector) => "Vector".to_string(),
                                    Some(CollectionLayer::Array { .. }) => "Fixed array".to_string(),
                                    None => String::new(),
                                }
                            }}</span>
                            <Show when=move || {
                                let draft = signals.udf.get();
                                matches!(target.of(&draft).and_then(|ty| ty.layers.get(layer)), Some(CollectionLayer::Array { .. }))
                            } fallback=|| ()>
                                <label class="create-field">
                                    <span>"Length"</span>
                                    <input class="create-udf-array-length" type="number" min="1" max="2147483647"
                                        prop:value=move || {
                                            let draft = signals.udf.get();
                                            match target.of(&draft).and_then(|ty| ty.layers.get(layer)) {
                                                Some(CollectionLayer::Array { length }) => length.clone(),
                                                Some(CollectionLayer::Vector) | None => String::new(),
                                            }
                                        }
                                        disabled=pending
                                        on:input=move |event| {
                                            let value = event_target_value(&event);
                                            change_type(signals, target, |ty| {
                                                if let Some(CollectionLayer::Array { length }) = ty.layers.get_mut(layer) {
                                                    *length = value;
                                                }
                                            });
                                        } />
                                </label>
                            </Show>
                            <button type="button" aria-label=format!("Remove collection layer {}", layer + 1)
                                disabled=pending
                                on:click=move |_| change_type(signals, target, |ty| {
                                    if layer < ty.layers.len() { ty.layers.remove(layer); }
                                })>"Remove"</button>
                        </div>
                    } />
                <div class="create-type-layer-add">
                    <button class="create-udf-add-vector" type="button" disabled=pending
                        on:click=move |_| change_type(signals, target, |ty| ty.layers.push(CollectionLayer::Vector))>
                        "Wrap in vector"
                    </button>
                    <button class="create-udf-add-array" type="button" disabled=pending
                        on:click=move |_| change_type(signals, target, |ty| {
                            ty.layers.push(CollectionLayer::Array { length: String::new() });
                        })>"Wrap in fixed array"</button>
                </div>
            </div>
        </fieldset>
    }
}

#[component]
pub(super) fn UdfEditor(
    signals: CreateSignals,
    name_input: NodeRef<leptos::html::Input>,
) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    let add_disabled = move || pending() || signals.udf.get().arguments.len() >= 8;
    view! {
        <label class="create-field">
            <span>"UDF name"</span>
            <input node_ref=name_input class="create-name" type="text" autocomplete="off"
                prop:value=move || signals.udf.get().name
                disabled=pending
                on:input=move |event| {
                    let name = event_target_value(&event);
                    signals.udf.update(|draft| draft.name = name);
                    signals.edit();
                } />
        </label>
        <fieldset class="create-choice-group create-udf-language">
            <legend>"Language"</legend>
            <div class="create-choice-buttons">
                <button type="button" data-language="ROTO_0_13"
                    class:active=move || signals.udf.get().language == UdfLanguage::Roto0_13
                    disabled=pending
                    on:click=move |_| {
                        signals.udf.update(|draft| draft.language = UdfLanguage::Roto0_13);
                        signals.edit();
                    }>"ROTO_0_13"</button>
            </div>
        </fieldset>
        <div class="create-fields create-udf-arguments" aria-label="UDF arguments">
            <For each=move || { (0..signals.udf.get().arguments.len()).collect::<Vec<_>>() }
                key=|index| *index
                children=move |index| view! {
                    <div class="create-field-entry create-udf-argument-entry">
                        <div class="create-field-entry-head">
                            <strong>{format!("Argument {}", index + 1)}</strong>
                            <div class="create-field-actions">
                                <button type="button" aria-label=format!("Move argument {} up", index + 1)
                                    disabled=move || pending() || index == 0
                                    on:click=move |_| {
                                        signals.udf.update(|draft| draft.move_argument_up(index));
                                        signals.edit();
                                    }>"↑"</button>
                                <button type="button" aria-label=format!("Move argument {} down", index + 1)
                                    disabled=pending
                                    on:click=move |_| {
                                        signals.udf.update(|draft| draft.move_argument_down(index));
                                        signals.edit();
                                    }>"↓"</button>
                                <button type="button" aria-label=format!("Remove argument {}", index + 1)
                                    disabled=pending
                                    on:click=move |_| {
                                        signals.udf.update(|draft| {
                                            if index < draft.arguments.len() { draft.arguments.remove(index); }
                                        });
                                        signals.edit();
                                    }>"Remove"</button>
                            </div>
                        </div>
                        <label class="create-field">
                            <span>"Argument name"</span>
                            <input class="create-udf-argument-name" type="text" autocomplete="off"
                                prop:value=move || {
                                    let draft = signals.udf.get();
                                    match draft.arguments.get(index) {
                                        Some(argument) => argument.name.clone(),
                                        None => String::new(),
                                    }
                                }
                                disabled=pending
                                on:input=move |event| {
                                    let value = event_target_value(&event);
                                    signals.udf.update(|draft| {
                                        if let Some(argument) = draft.arguments.get_mut(index) { argument.name = value; }
                                    });
                                    signals.edit();
                                } />
                        </label>
                        <UdfTypeEditor signals=signals target=TypeTarget::Argument(index) class_name="create-udf-argument-type" />
                        <label class="create-check">
                            <input class="create-udf-argument-optional" type="checkbox"
                                prop:checked=move || signals.udf.get().arguments.get(index).is_some_and(|argument| argument.optional)
                                disabled=pending
                                on:change=move |event| {
                                    let checked = event_target_checked(&event);
                                    signals.udf.update(|draft| {
                                        if let Some(argument) = draft.arguments.get_mut(index) { argument.optional = checked; }
                                    });
                                    signals.edit();
                                } />
                            <span>"Optional"</span>
                        </label>
                    </div>
                } />
            <button class="create-udf-add-argument" type="button"
                disabled=add_disabled
                on:click=move |_| {
                    signals.udf.update(|draft| draft.arguments.push(UdfArgumentDraft::default()));
                    signals.edit();
                }>"Add argument"</button>
        </div>
        <UdfTypeEditor signals=signals target=TypeTarget::Return class_name="create-udf-return-type" />
        <label class="create-check">
            <input class="create-udf-return-optional" type="checkbox"
                prop:checked=move || signals.udf.get().return_optional
                disabled=pending
                on:change=move |event| {
                    let checked = event_target_checked(&event);
                    signals.udf.update(|draft| draft.return_optional = checked);
                    signals.edit();
                } />
            <span>"Optional result"</span>
        </label>
        <label class="create-check">
            <input class="create-udf-volatile" type="checkbox"
                prop:checked=move || signals.udf.get().volatile
                disabled=pending
                on:change=move |event| {
                    let checked = event_target_checked(&event);
                    signals.udf.update(|draft| draft.volatile = checked);
                    signals.edit();
                } />
            <span>"Volatile"</span>
        </label>
        <label class="create-field">
            <span>"Roto source, including test blocks"</span>
            <textarea class="create-udf-code" spellcheck="false" rows="12"
                prop:value=move || signals.udf.get().code
                disabled=pending
                on:input=move |event| {
                    let code = event_target_textarea_value(&event);
                    signals.udf.update(|draft| draft.code = code);
                    signals.edit();
                }></textarea>
        </label>
    }
}

#[cfg(test)]
mod tests {
    use leptos::prelude::*;
    use nervix_models::ParseAsType;

    use super::{TypeTarget, change_type};
    use crate::create_dialog::{CreateSignals, udf_draft::UdfArgumentDraft};

    #[test]
    fn editing_one_signature_type_does_not_change_the_other_positions() {
        Owner::new().with(|| {
            let signals = CreateSignals::new();
            signals
                .udf
                .update(|draft| draft.arguments.push(UdfArgumentDraft::default()));
            change_type(signals, TypeTarget::Argument(0), |ty| {
                ty.scalar = Some(ParseAsType::I64);
            });
            change_type(signals, TypeTarget::Return, |ty| {
                ty.scalar = Some(ParseAsType::String);
            });
            change_type(signals, TypeTarget::Argument(1), |ty| {
                ty.scalar = Some(ParseAsType::Bool);
            });
            let draft = signals.udf.get_untracked();
            assert_eq!(draft.arguments[0].ty.scalar, Some(ParseAsType::I64));
            assert_eq!(draft.returns.scalar, Some(ParseAsType::String));
            assert_eq!(draft.arguments.len(), 1);
        });
    }
}
