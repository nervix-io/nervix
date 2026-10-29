//! Source, codec, timestamp, and node policy controls for visual ingestor creation.
//!
//! Layer: edges.
//!
//! - **Owns.** Browser presentation and editing of one ingestor's source and node-wide settings.
//! - **Depends on.** The ingestor draft, typed choices, and the route editor.
//! - **Must not know.** Source connectors, registry snapshots, or command dispatch internals.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use leptos::prelude::*;
use nervix_models::{
    HttpIngestorEntity, HttpRequestHeader, IngestDeliveryKind, IngestQuiesceKind,
    IngestQuiesceOverflow, IngestorEntityKind, OptionDeclaration, OptionForm, OptionPresence,
    WebsocketsIngestorEntity, WebsocketsUpgradeHeader,
};

use super::{
    ChoiceControl, ChoiceGroup, CreateSignals, RequestSender, event_target_textarea_value,
    event_target_value, ingestor_draft::TimestampDraft, ingestor_route_editor::IngestorRouteEditor,
    ingestor_source_draft::IngestorEntityDraft,
};

#[derive(Clone, Copy)]
enum IngestorField {
    Topic,
    Queue,
    Channel,
    Subject,
    QueueGroup,
    Subscription,
    Query,
    Method,
    MaxBodySize,
    Every,
    Instances,
    ConsumerGroup,
    AckMax,
    BatchTimeout,
    AckTimeout,
    RetryBackoff,
    RetryMax,
    BufferSize,
    RetryAfter,
}

impl IngestorField {
    fn label(self) -> &'static str {
        match self {
            Self::Topic => "Topic or topic filter",
            Self::Queue => "Queue",
            Self::Channel => "Channel",
            Self::Subject => "Subject",
            Self::QueueGroup => "Queue group",
            Self::Subscription => "Subscription",
            Self::Query => "QUERY",
            Self::Method => "METHOD",
            Self::MaxBodySize => "MAX BODY SIZE",
            Self::Every => "Every",
            Self::Instances => "Instances",
            Self::ConsumerGroup => "Consumer group",
            Self::AckMax => "ACK maximum",
            Self::BatchTimeout => "Batch timeout",
            Self::AckTimeout => "ACK timeout",
            Self::RetryBackoff => "Retry backoff",
            Self::RetryMax => "Maximum retry backoff",
            Self::BufferSize => "Maximum buffer size",
            Self::RetryAfter => "Retry after",
        }
    }

    fn class(self) -> &'static str {
        match self {
            Self::Topic => "create-ingestor-topic",
            Self::Queue => "create-ingestor-queue",
            Self::Channel => "create-ingestor-channel",
            Self::Subject => "create-ingestor-subject",
            Self::QueueGroup => "create-ingestor-queue-group",
            Self::Subscription => "create-ingestor-subscription",
            Self::Query => "create-ingestor-query",
            Self::Method => "create-ingestor-method",
            Self::MaxBodySize => "create-ingestor-max-body-size",
            Self::Every => "create-ingestor-every",
            Self::Instances => "create-ingestor-instances",
            Self::ConsumerGroup => "create-ingestor-consumer-group",
            Self::AckMax => "create-ingestor-ack-max",
            Self::BatchTimeout => "create-ingestor-batch-timeout",
            Self::AckTimeout => "create-ingestor-ack-timeout",
            Self::RetryBackoff => "create-ingestor-retry-backoff",
            Self::RetryMax => "create-ingestor-retry-max",
            Self::BufferSize => "create-ingestor-buffer-size",
            Self::RetryAfter => "create-ingestor-retry-after",
        }
    }

    fn value(self, source: &IngestorEntityDraft) -> &str {
        match self {
            Self::Topic => &source.topic,
            Self::Queue => &source.queue,
            Self::Channel => &source.channel,
            Self::Subject => &source.subject,
            Self::QueueGroup => &source.queue_group,
            Self::Subscription => &source.subscription,
            Self::Query => &source.query,
            Self::Method => &source.method,
            Self::MaxBodySize => &source.max_body_size,
            Self::Every => &source.every,
            Self::Instances => &source.instances,
            Self::ConsumerGroup => &source.consumer_group,
            Self::AckMax => &source.ack_max,
            Self::BatchTimeout => &source.batch_timeout,
            Self::AckTimeout => &source.ack_timeout,
            Self::RetryBackoff => &source.retry_backoff,
            Self::RetryMax => &source.retry_max,
            Self::BufferSize => &source.buffer_size,
            Self::RetryAfter => &source.retry_after,
        }
    }

    fn set(self, source: &mut IngestorEntityDraft, value: String) {
        *match self {
            Self::Topic => &mut source.topic,
            Self::Queue => &mut source.queue,
            Self::Channel => &mut source.channel,
            Self::Subject => &mut source.subject,
            Self::QueueGroup => &mut source.queue_group,
            Self::Subscription => &mut source.subscription,
            Self::Query => &mut source.query,
            Self::Method => &mut source.method,
            Self::MaxBodySize => &mut source.max_body_size,
            Self::Every => &mut source.every,
            Self::Instances => &mut source.instances,
            Self::ConsumerGroup => &mut source.consumer_group,
            Self::AckMax => &mut source.ack_max,
            Self::BatchTimeout => &mut source.batch_timeout,
            Self::AckTimeout => &mut source.ack_timeout,
            Self::RetryBackoff => &mut source.retry_backoff,
            Self::RetryMax => &mut source.retry_max,
            Self::BufferSize => &mut source.buffer_size,
            Self::RetryAfter => &mut source.retry_after,
        } = value;
    }
}

#[component]
fn SourceFieldEditor(signals: CreateSignals, field: IngestorField) -> impl IntoView {
    view! {
        <label class="create-field">
            <span>{field.label()}</span>
            <input class=field.class() type="text" autocomplete="off"
                prop:value=move || field.value(&signals.ingestor.get().source).to_string()
                disabled=move || signals.progress.get().is_pending()
                on:input=move |event| {
                    let value = event_target_value(&event);
                    signals.ingestor.update(|draft| field.set(&mut draft.source, value));
                    signals.edit();
                } />
        </label>
    }
}

/// The typed control that stores one declared locator or option.
enum EntityOptionField {
    Scalar(IngestorField),
    RequestHeaders,
    UpgradeHeaders,
}

impl EntityOptionField {
    fn from_keywords(kind: Option<IngestorEntityKind>, keywords: &str) -> Option<Self> {
        let scalar = match keywords {
            "HEADERS" => {
                return match kind {
                    Some(IngestorEntityKind::Http(HttpIngestorEntity::Request)) => {
                        Some(Self::RequestHeaders)
                    }
                    Some(IngestorEntityKind::Websockets(WebsocketsIngestorEntity::Connection)) => {
                        Some(Self::UpgradeHeaders)
                    }
                    _ => None,
                };
            }
            "TOPIC" => IngestorField::Topic,
            "QUEUE" => IngestorField::Queue,
            "CHANNEL" => IngestorField::Channel,
            "SUBJECT" => IngestorField::Subject,
            "QUEUE GROUP" => IngestorField::QueueGroup,
            "SUBSCRIPTION" => IngestorField::Subscription,
            "QUERY" => IngestorField::Query,
            "METHOD" => IngestorField::Method,
            "MAX BODY SIZE" => IngestorField::MaxBodySize,
            "EVERY" => IngestorField::Every,
            "INSTANCES" => IngestorField::Instances,
            _ => return None,
        };
        Some(Self::Scalar(scalar))
    }
}

#[component]
fn UpgradeHeadersEditor(signals: CreateSignals) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    view! {
        <fieldset class="create-field create-ingestor-upgrade-headers" data-option="HEADERS">
            <legend>"HEADERS · optional literal upgrade fields"</legend>
            <For each=move || { (0..signals.ingestor.get().source.upgrade_headers.len()).collect::<Vec<_>>() }
                key=|index| *index
                children=move |index| view! {
                    <div class="create-field-row">
                        <label class="create-field"><span>"Name"</span>
                            <input class="create-ingestor-upgrade-header-name" type="text" autocomplete="off"
                                disabled=pending
                                prop:value=move || match signals.ingestor.get().source.upgrade_headers.get(index) {
                                    Some(header) => header.name.clone(), None => String::new(),
                                }
                                on:input=move |event| {
                                    let value = event_target_value(&event);
                                    signals.ingestor.update(|draft| {
                                        if let Some(header) = draft.source.upgrade_headers.get_mut(index) { header.name = value; }
                                    });
                                    signals.edit();
                                } />
                        </label>
                        <label class="create-field"><span>"Value"</span>
                            <input class="create-ingestor-upgrade-header-value" type="text" autocomplete="off"
                                disabled=pending
                                prop:value=move || match signals.ingestor.get().source.upgrade_headers.get(index) {
                                    Some(header) => header.value.clone(), None => String::new(),
                                }
                                on:input=move |event| {
                                    let value = event_target_value(&event);
                                    signals.ingestor.update(|draft| {
                                        if let Some(header) = draft.source.upgrade_headers.get_mut(index) { header.value = value; }
                                    });
                                    signals.edit();
                                } />
                        </label>
                        <button type="button" class="create-ingestor-upgrade-header-remove" disabled=pending
                            on:click=move |_| {
                                signals.ingestor.update(|draft| {
                                    if index < draft.source.upgrade_headers.len() { draft.source.upgrade_headers.remove(index); }
                                });
                                signals.edit();
                            }>"Remove"</button>
                    </div>
                } />
            <button type="button" class="create-ingestor-upgrade-headers-add" disabled=pending
                on:click=move |_| {
                    signals.ingestor.update(|draft| draft.source.upgrade_headers.push(WebsocketsUpgradeHeader {
                        name: String::new(), value: String::new(),
                    }));
                    signals.edit();
                }>"Add upgrade header"</button>
        </fieldset>
    }
}

#[component]
fn RequestHeadersEditor(signals: CreateSignals) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    view! {
        <fieldset class="create-field create-ingestor-request-headers" data-option="HEADERS">
            <legend>"HEADERS · optional literal request fields"</legend>
            <For each=move || { (0..signals.ingestor.get().source.request_headers.len()).collect::<Vec<_>>() }
                key=|index| *index
                children=move |index| view! {
                    <div class="create-field-row">
                        <label class="create-field"><span>"Name"</span>
                            <input class="create-ingestor-request-header-name" type="text" autocomplete="off"
                                disabled=pending
                                prop:value=move || match signals.ingestor.get().source.request_headers.get(index) {
                                    Some(header) => header.name.clone(), None => String::new(),
                                }
                                on:input=move |event| {
                                    let value = event_target_value(&event);
                                    signals.ingestor.update(|draft| {
                                        if let Some(header) = draft.source.request_headers.get_mut(index) { header.name = value; }
                                    });
                                    signals.edit();
                                } />
                        </label>
                        <label class="create-field"><span>"Value"</span>
                            <input class="create-ingestor-request-header-value" type="text" autocomplete="off"
                                disabled=pending
                                prop:value=move || match signals.ingestor.get().source.request_headers.get(index) {
                                    Some(header) => header.value.clone(), None => String::new(),
                                }
                                on:input=move |event| {
                                    let value = event_target_value(&event);
                                    signals.ingestor.update(|draft| {
                                        if let Some(header) = draft.source.request_headers.get_mut(index) { header.value = value; }
                                    });
                                    signals.edit();
                                } />
                        </label>
                        <button type="button" class="create-ingestor-request-header-remove" disabled=pending
                            on:click=move |_| {
                                signals.ingestor.update(|draft| {
                                    if index < draft.source.request_headers.len() { draft.source.request_headers.remove(index); }
                                });
                                signals.edit();
                            }>"Remove"</button>
                    </div>
                } />
            <button type="button" class="create-ingestor-request-headers-add" disabled=pending
                on:click=move |_| {
                    signals.ingestor.update(|draft| draft.source.request_headers.push(HttpRequestHeader {
                        name: String::new(), value: String::new(),
                    }));
                    signals.edit();
                }>"Add request header"</button>
        </fieldset>
    }
}

#[component]
fn EntityValueEditor(
    signals: CreateSignals,
    keywords: &'static str,
    default: Option<&'static str>,
) -> impl IntoView {
    match EntityOptionField::from_keywords(signals.ingestor.get().source.kind, keywords) {
        Some(EntityOptionField::Scalar(field)) => view! {
            <label class="create-field" data-option=keywords>
                <span>{keywords}</span>
                <input class=field.class() type="text" autocomplete="off" placeholder=default.unwrap_or("")
                    prop:value=move || field.value(&signals.ingestor.get().source).to_string()
                    disabled=move || signals.progress.get().is_pending()
                    on:input=move |event| {
                        let value = event_target_value(&event);
                        signals.ingestor.update(|draft| field.set(&mut draft.source, value));
                        signals.edit();
                    } />
            </label>
        }.into_any(),
        Some(EntityOptionField::UpgradeHeaders) => view! { <UpgradeHeadersEditor signals=signals /> }.into_any(),
        Some(EntityOptionField::RequestHeaders) => view! { <RequestHeadersEditor signals=signals /> }.into_any(),
        None => ().into_any(),
    }
}

#[component]
fn EntityOptionEditor(signals: CreateSignals, option: OptionDeclaration) -> impl IntoView {
    let keywords = option.keywords;
    let default = match option.presence {
        OptionPresence::Required => None,
        OptionPresence::Optional { default } => default,
    };
    let choices = match option.form {
        OptionForm::Value { .. } => &[][..],
        OptionForm::Choice(choices) => choices,
    };
    let class = match keywords {
        "OFFSET BY" => "create-ingestor-offset",
        "SUBSCRIPTION" => "create-ingestor-subscription-ownership",
        "SESSION" => "create-ingestor-mqtt-session",
        "QOS" => "create-ingestor-mqtt-qos",
        _ => "create-ingestor-option",
    };
    view! {
        <EntityValueEditor signals=signals keywords=keywords default=default />
        <Show when=move || !choices.is_empty() fallback=|| ()>
            <fieldset class=class data-option=keywords>
                <legend>{keywords}</legend>
                <For each=move || choices.to_vec() key=|choice| choice.name
                    children=move |choice| {
                        let key = choice.name.to_ascii_lowercase().replace(' ', "-");
                        // Stable selectors identify the bound field; labels and choices come from the declaration.
                        let ownership = key.strip_prefix("owned-by-").unwrap_or(&key).to_string();
                        view! {
                            <button type="button" data-choice=choice.name
                                data-offset=(keywords == "OFFSET BY").then_some(key.clone())
                                data-ownership=(keywords == "SUBSCRIPTION").then_some(ownership)
                                data-session=(keywords == "SESSION").then_some(key.clone())
                                data-qos=(keywords == "QOS").then_some(key)
                                class:active=move || signals.ingestor.get().source.chosen_option(keywords) == Some(choice.name)
                                disabled=move || signals.progress.get().is_pending()
                                on:click=move |_| {
                                    signals.ingestor.update(|draft| draft.source.choose_option(keywords, choice.name));
                                    signals.edit();
                                }>{choice.name}</button>
                        }
                    } />
            </fieldset>
        </Show>
        <Show when=move || keywords == "OFFSET BY" && signals.ingestor.get().source.offset == Some(super::ingestor_source_draft::OffsetChoice::ConsumerGroup) fallback=|| ()>
            <SourceFieldEditor signals=signals field=IngestorField::ConsumerGroup />
        </Show>
    }
}

#[component]
pub(super) fn IngestorEditor(
    signals: CreateSignals,
    name_input: NodeRef<leptos::html::Input>,
    request_tx: RwSignal<Option<RequestSender>>,
    session_generation: RwSignal<u64>,
) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    let entity_kind = move || signals.ingestor.get().source.kind;
    let acknowledged = move || {
        signals
            .ingestor
            .get()
            .source
            .delivery
            .is_some_and(IngestDeliveryKind::acknowledged)
    };
    view! {
        <label class="create-field">
            <span>"Ingestor name"</span>
            <input node_ref=name_input class="create-name" type="text" autocomplete="off"
                prop:value=move || signals.ingestor.get().name
                disabled=pending
                on:input=move |event| {
                    let value = event_target_value(&event);
                    signals.ingestor.update(|draft| draft.name = value);
                    signals.edit();
                } />
        </label>
        <fieldset class="create-field create-ingestor-source">
            <legend>"Source type"</legend>
            <For each=IngestorEntityKind::modeled
                key=|kind| kind.form_key()
                children=move |kind| view! {
                    <button type="button" data-source=kind.form_key()
                        class:active=move || entity_kind() == Some(kind)
                        disabled=pending
                        on:click=move |_| {
                            signals.ingestor.update(|draft| draft.source.choose_kind(kind));
                            signals.edit();
                        }>{kind.phrase()}</button>
                } />
        </fieldset>
        <Show when=move || entity_kind().is_some() fallback=|| ()>
            <ChoiceGroup class_name="create-ingestor-source-ref" label="Source client or endpoint"
                control=ChoiceControl::IngestSourceRef signals=signals request_tx=request_tx
                session_generation=session_generation show_detail=true />
        </Show>
        <For each={move || entity_kind().and_then(|kind| kind.capabilities().locator).into_iter().collect::<Vec<_>>()}
            key={move |locator| (entity_kind(), locator.keywords)}
            children=move |locator| view! {
                <EntityValueEditor signals=signals keywords=locator.keywords default=None />
            } />
        <For each={move || match entity_kind() {
                Some(kind) => kind.capabilities().options.to_vec(),
                None => Vec::new(),
            }}
            key={move |option| (entity_kind(), option.keywords)}
            children=move |option| view! { <EntityOptionEditor signals=signals option=option /> } />
        <Show when=move || !signals.ingestor.get().source.available_modes().is_empty() fallback=|| ()>
            <fieldset class="create-field create-ingestor-mode">
                <legend>"Delivery mode"</legend>
                <For each=move || signals.ingestor.get().source.available_modes().to_vec()
                    key=|mode| mode.form_key()
                    children=move |mode| view! {
                        <button type="button" data-mode=mode.form_key()
                            class:active=move || {
                                let source = &signals.ingestor.get().source;
                                source.delivery == Some(mode)
                                    || (source.available_modes().len() == 1
                                        && source.available_modes().first() == Some(&mode))
                            }
                            disabled=move || pending() || !signals.ingestor.get().source.mode_enabled(mode)
                            on:click=move |_| {
                                signals.ingestor.update(|draft| draft.source.delivery = Some(mode));
                                signals.edit();
                            }>{mode.phrase()}</button>
                    } />
            </fieldset>
        </Show>
        <Show when=move || matches!(signals.ingestor.get().source.delivery, Some(IngestDeliveryKind::AckParallel | IngestDeliveryKind::AckRequest)) fallback=|| ()>
            <SourceFieldEditor signals=signals field=IngestorField::AckMax />
        </Show>
        <Show when=move || signals.ingestor.get().source.delivery == Some(IngestDeliveryKind::AckParallel) fallback=|| ()>
            <SourceFieldEditor signals=signals field=IngestorField::BatchTimeout />
        </Show>
        <Show when=acknowledged fallback=|| ()>
            <SourceFieldEditor signals=signals field=IngestorField::AckTimeout />
            <Show when=move || signals.ingestor.get().source.delivery != Some(IngestDeliveryKind::AckRequest) fallback=|| ()>
                <SourceFieldEditor signals=signals field=IngestorField::RetryBackoff />
                <SourceFieldEditor signals=signals field=IngestorField::RetryMax />
            </Show>
        </Show>
        <Show when=move || !signals.ingestor.get().source.available_quiesce().is_empty() fallback=|| ()>
            <fieldset class="create-field create-ingestor-quiesce">
                <legend>"On quiesce"</legend>
                <For each=move || signals.ingestor.get().source.available_quiesce().to_vec()
                    key=|choice| *choice
                    children=move |choice| view! {
                        <button type="button" data-quiesce=choice.form_key()
                            class:active=move || signals.ingestor.get().source.quiesce == Some(choice)
                            disabled=move || pending() || !signals.ingestor.get().source.quiesce_enabled(choice)
                            on:click=move |_| {
                                signals.ingestor.update(|draft| draft.source.quiesce = Some(choice));
                                signals.edit();
                            }>{choice.keyword()}</button>
                    } />
            </fieldset>
        </Show>
        <Show when=move || matches!(signals.ingestor.get().source.quiesce, Some(IngestQuiesceKind::Buffer | IngestQuiesceKind::EndpointBuffer)) fallback=|| ()>
            <SourceFieldEditor signals=signals field=IngestorField::BufferSize />
            <Show when=move || !signals.ingestor.get().source.reads_endpoint() fallback=|| ()>
                <fieldset class="create-field create-ingestor-overflow">
                    <legend>"On overflow"</legend>
                    <button type="button" data-overflow="oldest" class:active=move || signals.ingestor.get().source.overflow == Some(IngestQuiesceOverflow::DropOldest)
                        disabled=pending on:click=move |_| { signals.ingestor.update(|draft| draft.source.overflow = Some(IngestQuiesceOverflow::DropOldest)); signals.edit(); }>"DROP OLDEST"</button>
                    <button type="button" data-overflow="newest" class:active=move || signals.ingestor.get().source.overflow == Some(IngestQuiesceOverflow::DropNewest)
                        disabled=pending on:click=move |_| { signals.ingestor.update(|draft| draft.source.overflow = Some(IngestQuiesceOverflow::DropNewest)); signals.edit(); }>"DROP NEWEST"</button>
                </fieldset>
            </Show>
        </Show>
        <Show when=move || signals.ingestor.get().source.quiesce == Some(IngestQuiesceKind::Reject) fallback=|| ()>
            <SourceFieldEditor signals=signals field=IngestorField::RetryAfter />
        </Show>
        <ChoiceGroup class_name="create-ingestor-codec" label="Decoding codec"
            control=ChoiceControl::IngestCodec signals=signals request_tx=request_tx
            session_generation=session_generation show_detail=true />
        <fieldset class="create-field create-ingestor-timestamp">
            <legend>"Timestamp policy"</legend>
            <button type="button" data-timestamp="omit" class:active=move || signals.ingestor.get().timestamp == TimestampDraft::Omit
                disabled=pending on:click=move |_| { signals.ingestor.update(|draft| draft.timestamp = TimestampDraft::Omit); signals.edit(); }>"Use source timestamp"</button>
            <button type="button" data-timestamp="now" class:active=move || signals.ingestor.get().timestamp == TimestampDraft::Now
                disabled=pending on:click=move |_| { signals.ingestor.update(|draft| draft.timestamp = TimestampDraft::Now); signals.edit(); }>"NOW"</button>
            <button type="button" data-timestamp="at" class:active=move || matches!(signals.ingestor.get().timestamp, TimestampDraft::At(_))
                disabled=pending on:click=move |_| { signals.ingestor.update(|draft| draft.timestamp = TimestampDraft::At(None)); signals.edit(); }>"AT decoded field"</button>
        </fieldset>
        <Show when=move || matches!(signals.ingestor.get().timestamp, TimestampDraft::At(_)) fallback=|| ()>
            <ChoiceGroup class_name="create-ingestor-timestamp-field" label="Datetime field"
                control=ChoiceControl::IngestTimestampField signals=signals request_tx=request_tx
                session_generation=session_generation show_detail=true />
        </Show>
        <label class="create-field">
            <span>"Arrival filter expression (optional)"</span>
            <textarea class="create-ingestor-filter" prop:value=move || signals.ingestor.get().filter
                disabled=pending on:input=move |event| {
                    let value = event_target_textarea_value(&event);
                    signals.ingestor.update(|draft| draft.filter = value);
                    signals.edit();
                } />
        </label>
        <IngestorRouteEditor signals=signals request_tx=request_tx session_generation=session_generation />
        <fieldset class="create-field create-ingestor-general-error">
            <legend>"On general error"</legend>
            <button type="button" data-error="ignore" class:active=move || signals.ingestor.get().general_error == Some(nervix_models::GeneralErrorPolicy::Ignore)
                disabled=pending on:click=move |_| { signals.ingestor.update(|draft| draft.general_error = Some(nervix_models::GeneralErrorPolicy::Ignore)); signals.edit(); }>"IGNORE"</button>
            <button type="button" data-error="log" class:active=move || signals.ingestor.get().general_error == Some(nervix_models::GeneralErrorPolicy::Log)
                disabled=pending on:click=move |_| { signals.ingestor.update(|draft| draft.general_error = Some(nervix_models::GeneralErrorPolicy::Log)); signals.edit(); }>"LOG"</button>
        </fieldset>
        <Show when=move || {
            let draft = signals.ingestor.get();
            draft.source.reference.as_ref().is_some_and(|selected| !selected.is_current())
                || draft.codec.as_ref().is_some_and(|selected| !selected.is_current())
        } fallback=|| ()>
            <p class="create-reference-invalid" role="alert">"The selected domain, source or codec changed. Select affected references again."</p>
        </Show>
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    //! Layer: test harness.
    //! - **Owns.** Coverage of the binding from each declared value to its draft control.
    //! - **Depends on.** Current entity declarations and the editor's field adapters.
    //! - **Must not know.** Browser sessions, connector drivers or registry state.

    use nervix_models::{IngestorEntityKind, OptionForm};

    use super::EntityOptionField;

    #[test]
    fn every_declared_transport_value_has_an_editor_binding() {
        for kind in IngestorEntityKind::modeled() {
            let declaration = kind.capabilities();
            if let Some(locator) = declaration.locator {
                assert!(
                    EntityOptionField::from_keywords(Some(kind), locator.keywords).is_some(),
                    "{} {}",
                    kind.phrase(),
                    locator.keywords
                );
            }
            for option in declaration.options {
                match option.form {
                    OptionForm::Value { .. } => assert!(
                        EntityOptionField::from_keywords(Some(kind), option.keywords).is_some(),
                        "{} {}",
                        kind.phrase(),
                        option.keywords
                    ),
                    OptionForm::Choice(choices) => assert!(!choices.is_empty()),
                }
            }
        }
    }
}
