//! The cadence and state allowance of a visual window processor.
//!
//! Layer: edges.
//!
//! - **Owns.** Browser editing of a window's `WIDTH` and `STEP` bounds and its optional
//!   `MAX STATE SIZE` text.
//! - **Depends on.** The window draft and Leptos signals.
//! - **Must not know.** Bound validation, window panes or state accounting.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use leptos::prelude::*;

use super::{
    CreateSignals, event_target_checked, event_target_value,
    processor_draft::ProcessorFamily,
    processor_window_draft::{WindowBoundClause, WindowDraft},
};

/// Applies `edit` to the window draft as one edit of the form.
fn edit_window(signals: CreateSignals, edit: impl FnOnce(&mut WindowDraft)) {
    signals.processor(ProcessorFamily::Window).update(|draft| {
        if let Some(window) = draft.window_mut() {
            edit(window);
        }
    });
    signals.edit();
}

/// The class names of one bound's message-count and duration inputs.
fn bound_classes(clause: WindowBoundClause) -> (&'static str, &'static str) {
    match clause {
        WindowBoundClause::Width => (
            "create-window-width-messages",
            "create-window-width-duration",
        ),
        WindowBoundClause::Step => ("create-window-step-messages", "create-window-step-duration"),
    }
}

#[component]
fn WindowBoundFields(clause: WindowBoundClause, signals: CreateSignals) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    let (messages_class, duration_class) = bound_classes(clause);
    view! {
        <fieldset class="create-field create-window-bound">
            <legend>{clause.to_string()}</legend>
            <label class="create-field"><span>"Message count (optional)"</span>
                <input class=messages_class type="text" inputmode="numeric" autocomplete="off"
                    prop:value=move || {
                        let draft = signals.processor(ProcessorFamily::Window).get();
                        match draft.window() {
                            Some(window) => window.bound(clause).messages.clone(),
                            None => String::new(),
                        }
                    }
                    disabled=pending
                    on:input=move |event| {
                        let value = event_target_value(&event);
                        edit_window(signals, |window| window.bound_mut(clause).messages = value);
                    } />
            </label>
            <label class="create-field"><span>"Duration (optional)"</span>
                <input class=duration_class type="text" autocomplete="off"
                    prop:value=move || {
                        let draft = signals.processor(ProcessorFamily::Window).get();
                        match draft.window() {
                            Some(window) => window.bound(clause).duration.clone(),
                            None => String::new(),
                        }
                    }
                    disabled=pending
                    on:input=move |event| {
                        let value = event_target_value(&event);
                        edit_window(signals, |window| window.bound_mut(clause).duration = value);
                    } />
            </label>
        </fieldset>
    }
}

#[component]
pub(super) fn ProcessorWindowEditor(signals: CreateSignals) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    let capped = move || {
        let draft = signals.processor(ProcessorFamily::Window).get();
        match draft.window() {
            Some(window) => window.state_size.is_some(),
            None => false,
        }
    };
    let section = view! {
        <section class="create-processor-window">
            <h3>"Window cadence"</h3>
            <p>"A window emits on its WIDTH and STEP, never on a flush. Each bound counts messages, measures a duration, or both; a step measures only what its width measures and is never longer."</p>
            <WindowBoundFields clause=WindowBoundClause::Width signals=signals />
            <WindowBoundFields clause=WindowBoundClause::Step signals=signals />
            <label class="create-check"><input type="checkbox" class="create-window-state-limit"
                prop:checked=capped
                disabled=pending
                on:change=move |event| {
                    let checked = event_target_checked(&event);
                    edit_window(signals, |window| {
                        window.state_size = if checked { Some(String::new()) } else { None };
                    });
                } />"MAX STATE SIZE for each branch"</label>
            <Show when=capped fallback=|| ()>
                <label class="create-field"><span>"Maximum state size, a byte size such as 1MiB"</span>
                    <input class="create-window-state-size" type="text" autocomplete="off"
                        prop:value=move || {
                            let draft = signals.processor(ProcessorFamily::Window).get();
                            match draft.window().and_then(|window| window.state_size.clone()) {
                                Some(size) => size,
                                None => String::new(),
                            }
                        }
                        disabled=pending
                        on:input=move |event| {
                            let value = event_target_value(&event);
                            edit_window(signals, |window| {
                                if let Some(size) = &mut window.state_size { *size = value; }
                            });
                        } />
                </label>
            </Show>
        </section>
    };
    // Erased at the section boundary: the processor editor composing this section then holds and
    // names a boxed view, so neither its rendering frames nor the debug names of every form that
    // embeds it repeat this section's complete nested view.
    section.into_any()
}
