//! Ordered route invocation and argument controls for visual processors.
//!
//! Layer: edges.
//!
//! - **Owns.** Browser order and expression text of a route's invocations and arguments.
//! - **Depends on.** Shared route drafts and Leptos signals.
//! - **Must not know.** Function execution or expression lowering internals.

use leptos::prelude::*;

use super::{
    CreateSignals, event_target_value, ingestor_route_draft::InvocationDraft,
    processor_draft::ProcessorFamily, processor_editor::indices,
};

fn invocations(signals: CreateSignals, family: ProcessorFamily) -> Vec<InvocationDraft> {
    signals
        .processor(family)
        .get()
        .active_route()
        .map(|route| route.invocations.clone())
        .unwrap_or_default()
}

fn with_invocations(
    signals: CreateSignals,
    family: ProcessorFamily,
    action: impl FnOnce(&mut Vec<InvocationDraft>),
) {
    signals.processor(family).update(|draft| {
        if let Some(route) = draft.active_route_mut() {
            action(&mut route.invocations);
        }
    });
    signals.edit();
}

fn argument_count(signals: CreateSignals, family: ProcessorFamily, index: usize) -> usize {
    invocations(signals, family)
        .get(index)
        .map(|item| item.arguments.len())
        .unwrap_or(0)
}

#[component]
pub(super) fn ProcessorInvocationEditor(
    family: ProcessorFamily,
    signals: CreateSignals,
) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    view! {
        <section class="create-processor-invocations"><h4>"Ordered invocations"</h4>
            <For each=move || indices(invocations(signals, family).len())
                key=|index| *index
                children=move |index| view! {
                    <div class="create-processor-invocation">
                        <label class="create-field"><span>"Function"</span>
                            <input class="create-processor-invocation-function" type="text" autocomplete="off"
                                prop:value=move || invocations(signals, family).get(index).map(|item| item.function.clone()).unwrap_or_default()
                                disabled=pending
                                on:input=move |event| {
                                    let value = event_target_value(&event);
                                    with_invocations(signals, family, |items| {
                                        if let Some(item) = items.get_mut(index) { item.function = value; }
                                    });
                                } />
                        </label>
                        <For each=move || indices(argument_count(signals, family, index))
                            key=|argument| *argument
                            children=move |argument| view! {
                                <div class="create-processor-invocation-argument-row">
                                    <input class="create-processor-invocation-argument" type="text" autocomplete="off"
                                        aria-label="Invocation argument expression"
                                        prop:value=move || invocations(signals, family).get(index)
                                            .and_then(|item| item.arguments.get(argument).cloned()).unwrap_or_default()
                                        disabled=pending
                                        on:input=move |event| {
                                            let value = event_target_value(&event);
                                            with_invocations(signals, family, |items| {
                                                if let Some(item) = items.get_mut(index)
                                                    && let Some(arg) = item.arguments.get_mut(argument) { *arg = value; }
                                            });
                                        } />
                                    <button type="button" class="create-processor-argument-up"
                                        disabled=move || { pending() || argument == 0 }
                                        on:click=move |_| with_invocations(signals, family, |items| {
                                            if let Some(item) = items.get_mut(index)
                                                && argument > 0 && argument < item.arguments.len() {
                                                item.arguments.swap(argument, argument - 1);
                                            }
                                        })>"Move up"</button>
                                    <button type="button" class="create-processor-argument-down"
                                        disabled=move || { pending() || invocations(signals, family).get(index)
                                            .is_none_or(|item| argument + 1 >= item.arguments.len()) }
                                        on:click=move |_| with_invocations(signals, family, |items| {
                                            if let Some(item) = items.get_mut(index)
                                                && argument + 1 < item.arguments.len() {
                                                item.arguments.swap(argument, argument + 1);
                                            }
                                        })>"Move down"</button>
                                    <button type="button" class="create-processor-remove-argument" disabled=pending
                                        on:click=move |_| with_invocations(signals, family, |items| {
                                            if let Some(item) = items.get_mut(index)
                                                && argument < item.arguments.len() {
                                                item.arguments.remove(argument);
                                            }
                                        })>"Remove argument"</button>
                                </div>
                            } />
                        <button type="button" class="create-processor-add-argument" disabled=pending
                            on:click=move |_| with_invocations(signals, family, |items| {
                                if let Some(item) = items.get_mut(index) { item.arguments.push(String::new()); }
                            })>"Add argument"</button>
                        <button type="button" class="create-processor-invocation-up"
                            disabled=move || { pending() || index == 0 }
                            on:click=move |_| with_invocations(signals, family, |items| {
                                if index > 0 && index < items.len() { items.swap(index, index - 1); }
                            })>"Move invocation up"</button>
                        <button type="button" class="create-processor-invocation-down"
                            disabled=move || { pending() || index + 1 >= invocations(signals, family).len() }
                            on:click=move |_| with_invocations(signals, family, |items| {
                                if index + 1 < items.len() { items.swap(index, index + 1); }
                            })>"Move invocation down"</button>
                        <button type="button" class="create-processor-remove-invocation" disabled=pending
                            on:click=move |_| with_invocations(signals, family, |items| {
                                if index < items.len() { items.remove(index); }
                            })>"Remove invocation"</button>
                    </div>
                } />
            <button type="button" class="create-processor-add-invocation" disabled=pending
                on:click=move |_| with_invocations(signals, family, |items| items.push(InvocationDraft::default()))>"Add invocation"</button>
        </section>
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::create_dialog::CreateKind;

    #[test]
    fn invocation_helpers_keep_function_and_argument_order_in_the_active_route() {
        Owner::new().with(|| {
            let signals = CreateSignals::new();
            signals.open(CreateKind::Junction, None, "global-create-button");
            with_invocations(signals, ProcessorFamily::Junction, |items| {
                items.push(InvocationDraft {
                    function: "first".into(),
                    arguments: vec!["input.message".into(), "output.result".into()],
                });
                items.push(InvocationDraft {
                    function: "second".into(),
                    arguments: vec!["1".into()],
                });
            });
            assert_eq!(argument_count(signals, ProcessorFamily::Junction, 0), 2);
            assert_eq!(argument_count(signals, ProcessorFamily::Junction, 1), 1);
            with_invocations(signals, ProcessorFamily::Junction, |items| items.swap(0, 1));
            let ordered = invocations(signals, ProcessorFamily::Junction);
            assert_eq!(ordered[0].function, "second");
            assert_eq!(ordered[1].arguments, ["input.message", "output.result"]);
        });
    }
}
