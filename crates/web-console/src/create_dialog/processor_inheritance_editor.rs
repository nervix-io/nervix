//! Ordered INHERIT controls shared by junction and reingestor routes.
//!
//! Layer: edges.
//!
//! - **Owns.** Inheritance mode, selected field order, leakage flags, and field removal in one
//!   browser route draft.
//! - **Depends on.** Shared route drafts and typed input field choices.
//! - **Must not know.** Runtime field projection or registry validation.

use leptos::prelude::*;

use super::{
    ChoiceControl, CreateSignals, RequestSender, choice_group::ChoiceGroup, event_target_checked,
    ingestor_route_draft::InheritDraft, processor_draft::ProcessorFamily,
    processor_editor::indices,
};

fn inherited_count(signals: CreateSignals, family: ProcessorFamily) -> usize {
    let draft = signals.processor(family).get();
    match draft.active_route() {
        Some(route) => match &route.inherit {
            InheritDraft::AllExcept(fields) => fields.len(),
            InheritDraft::Fields(fields) => fields.len(),
            _ => 0,
        },
        None => 0,
    }
}

fn inherited_name(signals: CreateSignals, family: ProcessorFamily, index: usize) -> String {
    let draft = signals.processor(family).get();
    match draft.active_route() {
        Some(route) => match &route.inherit {
            InheritDraft::AllExcept(fields) => {
                fields.get(index).map(|field| field.name().to_string())
            }
            InheritDraft::Fields(fields) => fields
                .get(index)
                .map(|field| field.field.name().to_string()),
            _ => None,
        },
        None => None,
    }
    .unwrap_or_else(|| "Choose field".to_string())
}

#[component]
pub(super) fn ProcessorInheritanceEditor(
    family: ProcessorFamily,
    signals: CreateSignals,
    request_tx: RwSignal<Option<RequestSender>>,
    session_generation: RwSignal<u64>,
) -> impl IntoView {
    let draft = signals.processor(family);
    let pending = move || signals.progress.get().is_pending();
    view! {
        <fieldset class="create-field create-processor-inherit">
            <legend>"Inherit input fields"</legend>
            <For each=|| ["none", "all", "all-except", "fields"].into_iter()
                key=|mode| *mode
                children=move |mode| view! {
                    <button type="button" data-inherit=mode
                        class:active=move || draft.get().active_route().is_some_and(|route| route.inherit.key() == mode)
                        disabled=pending
                        on:click=move |_| { draft.update(|state| {
                            if let Some(route) = state.active_route_mut() { route.inherit.choose(mode); }
                        }); signals.edit(); }>{mode.to_ascii_uppercase()}</button>
                } />
        </fieldset>
        <Show when=move || draft.get().active_route().is_some_and(|route| matches!(route.inherit, InheritDraft::AllExcept(_) | InheritDraft::Fields(_))) fallback=|| ()>
            <ChoiceGroup class_name="create-processor-input-fields" label="Add input field"
                control=ChoiceControl::ProcessorInputField signals=signals request_tx=request_tx
                session_generation=session_generation show_detail=true />
            <For each=move || indices(inherited_count(signals, family))
                key=|index| *index
                children=move |index| view! {
                    <div class="create-processor-inherited-field">
                        <span>{move || inherited_name(signals, family, index)}</span>
                        <Show when=move || draft.get().active_route().is_some_and(|route| matches!(route.inherit, InheritDraft::Fields(_))) fallback=|| ()>
                            <label><input type="checkbox" class="create-processor-leak"
                                prop:checked=move || draft.get().active_route().is_some_and(|route| match &route.inherit {
                                    InheritDraft::Fields(fields) => fields.get(index).is_some_and(|field| field.leak_sensitive),
                                    _ => false,
                                })
                                disabled=pending
                                on:change=move |event| {
                                    let checked = event_target_checked(&event);
                                    draft.update(|state| {
                                        if let Some(route) = state.active_route_mut()
                                            && let InheritDraft::Fields(fields) = &mut route.inherit
                                            && let Some(field) = fields.get_mut(index) {
                                            field.leak_sensitive = checked;
                                        }
                                    });
                                    signals.edit();
                                } />"LEAK SENSITIVE"</label>
                        </Show>
                        <button type="button" class="create-processor-inherit-up"
                            disabled=move || { pending() || index == 0 }
                            on:click=move |_| { draft.update(|state| {
                                if let Some(route) = state.active_route_mut() {
                                    match &mut route.inherit {
                                        InheritDraft::AllExcept(fields) if index > 0 && index < fields.len() => fields.swap(index, index - 1),
                                        InheritDraft::Fields(fields) if index > 0 && index < fields.len() => fields.swap(index, index - 1),
                                        _ => {},
                                    }
                                }
                            }); signals.edit(); }>"Move up"</button>
                        <button type="button" class="create-processor-inherit-down"
                            disabled=move || { pending() || index + 1 >= inherited_count(signals, family) }
                            on:click=move |_| { draft.update(|state| {
                                if let Some(route) = state.active_route_mut() {
                                    match &mut route.inherit {
                                        InheritDraft::AllExcept(fields) if index + 1 < fields.len() => fields.swap(index, index + 1),
                                        InheritDraft::Fields(fields) if index + 1 < fields.len() => fields.swap(index, index + 1),
                                        _ => {},
                                    }
                                }
                            }); signals.edit(); }>"Move down"</button>
                        <button type="button" class="create-processor-remove-inherited-field" disabled=pending
                            on:click=move |_| { draft.update(|state| {
                                if let Some(route) = state.active_route_mut() {
                                    match &mut route.inherit {
                                        InheritDraft::AllExcept(fields) if index < fields.len() => { fields.remove(index); },
                                        InheritDraft::Fields(fields) if index < fields.len() => { fields.remove(index); },
                                        _ => {},
                                    }
                                }
                            }); signals.edit(); }>"Remove field"</button>
                    </div>
                } />
        </Show>
    }
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_models::FieldName;

    use super::*;
    use crate::create_dialog::{
        CreateKind, SelectedReference, ingestor_route_draft::InheritedFieldDraft,
    };

    #[test]
    fn inherited_field_labels_follow_the_selected_route_mode_and_order() {
        Owner::new().with(|| {
            let signals = CreateSignals::new();
            signals.open(CreateKind::Junction, None, "global-create-button");
            signals.junction.update(|draft| {
                draft.routes[0].inherit = InheritDraft::Fields(vec![
                    InheritedFieldDraft {
                        field: SelectedReference::chosen(
                            FieldName::parse("first").assured("valid field"),
                        ),
                        leak_sensitive: false,
                    },
                    InheritedFieldDraft {
                        field: SelectedReference::chosen(
                            FieldName::parse("second").assured("valid field"),
                        ),
                        leak_sensitive: true,
                    },
                ]);
            });
            assert_eq!(inherited_count(signals, ProcessorFamily::Junction), 2);
            assert_eq!(
                inherited_name(signals, ProcessorFamily::Junction, 0),
                "first"
            );
            assert_eq!(
                inherited_name(signals, ProcessorFamily::Junction, 1),
                "second"
            );
            signals.junction.update(|draft| {
                draft.routes[0].inherit = InheritDraft::AllExcept(vec![SelectedReference::chosen(
                    FieldName::parse("second").assured("valid field"),
                )]);
            });
            assert_eq!(inherited_count(signals, ProcessorFamily::Junction), 1);
            assert_eq!(
                inherited_name(signals, ProcessorFamily::Junction, 0),
                "second"
            );
            signals
                .junction
                .update(|draft| draft.routes[0].inherit = InheritDraft::All);
            assert_eq!(inherited_count(signals, ProcessorFamily::Junction), 0);
            assert_eq!(
                inherited_name(signals, ProcessorFamily::Junction, 0),
                "Choose field"
            );
        });
    }
}
