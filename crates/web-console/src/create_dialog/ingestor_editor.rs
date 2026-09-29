//! Source, codec, timestamp, and node policy controls for visual ingestor creation.
//!
//! Layer: edges.
//!
//! - **Owns.** Browser presentation and editing of one ingestor's source and node-wide settings.
//! - **Depends on.** The ingestor draft, typed choices, and the route editor.
//! - **Must not know.** Source connectors, registry snapshots, or command dispatch internals.

use leptos::prelude::*;
use nervix_models::{IngestQuiesceOverflow, IngestSourceKind, MqttQos, MqttSession};

use super::{
    ChoiceControl, ChoiceGroup, CreateSignals, RequestSender, event_target_textarea_value,
    event_target_value,
    ingestor_draft::TimestampDraft,
    ingestor_route_editor::IngestorRouteEditor,
    ingestor_source_draft::{DeliveryChoice, IngestSourceDraft, QuiesceChoice},
};

#[derive(Clone, Copy)]
enum SourceField {
    Topic,
    Queue,
    Channel,
    Subject,
    QueueGroup,
    Subscription,
    Query,
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

impl SourceField {
    fn label(self) -> &'static str {
        match self {
            Self::Topic => "Topic or topic filter",
            Self::Queue => "Queue",
            Self::Channel => "Channel",
            Self::Subject => "Subject",
            Self::QueueGroup => "Queue group",
            Self::Subscription => "Subscription",
            Self::Query => "Prometheus query",
            Self::Every => "Every",
            Self::Instances => "Instances",
            Self::ConsumerGroup => "Consumer group",
            Self::AckMax => "ACK parallel maximum",
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

    fn value(self, source: &IngestSourceDraft) -> &str {
        match self {
            Self::Topic => &source.topic,
            Self::Queue => &source.queue,
            Self::Channel => &source.channel,
            Self::Subject => &source.subject,
            Self::QueueGroup => &source.queue_group,
            Self::Subscription => &source.subscription,
            Self::Query => &source.query,
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

    fn set(self, source: &mut IngestSourceDraft, value: String) {
        *match self {
            Self::Topic => &mut source.topic,
            Self::Queue => &mut source.queue,
            Self::Channel => &mut source.channel,
            Self::Subject => &mut source.subject,
            Self::QueueGroup => &mut source.queue_group,
            Self::Subscription => &mut source.subscription,
            Self::Query => &mut source.query,
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
fn SourceFieldEditor(signals: CreateSignals, field: SourceField) -> impl IntoView {
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

#[component]
pub(super) fn IngestorEditor(
    signals: CreateSignals,
    name_input: NodeRef<leptos::html::Input>,
    request_tx: RwSignal<Option<RequestSender>>,
    session_generation: RwSignal<u64>,
) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    let source_kind = move || signals.ingestor.get().source.kind;
    let acknowledged = move || {
        let source = &signals.ingestor.get().source;
        matches!(
            source.kind,
            Some(IngestSourceKind::RabbitMq | IngestSourceKind::Sqs)
        ) || source.delivery.is_some_and(DeliveryChoice::acknowledged)
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
            <For each=|| IngestSourceKind::ALL.into_iter()
                key=|kind| kind.form_key()
                children=move |kind| view! {
                    <button type="button" data-source=kind.form_key()
                        class:active=move || source_kind() == Some(kind)
                        disabled=pending
                        on:click=move |_| {
                            signals.ingestor.update(|draft| draft.source.choose_kind(kind));
                            signals.edit();
                        }>{kind.form_label()}</button>
                } />
        </fieldset>
        <Show when=move || source_kind().is_some() fallback=|| ()>
            <ChoiceGroup class_name="create-ingestor-source-ref" label="Source client or endpoint"
                control=ChoiceControl::IngestSourceRef signals=signals request_tx=request_tx
                session_generation=session_generation show_detail=true />
        </Show>
        <Show when=move || matches!(source_kind(), Some(IngestSourceKind::Kafka | IngestSourceKind::Pulsar | IngestSourceKind::Mqtt)) fallback=|| ()>
            <SourceFieldEditor signals=signals field=SourceField::Topic />
        </Show>
        <Show when=move || matches!(source_kind(), Some(IngestSourceKind::RabbitMq | IngestSourceKind::Sqs)) fallback=|| ()>
            <SourceFieldEditor signals=signals field=SourceField::Queue />
        </Show>
        <Show when=move || source_kind() == Some(IngestSourceKind::RedisPubSub) fallback=|| ()>
            <SourceFieldEditor signals=signals field=SourceField::Channel />
        </Show>
        <Show when=move || source_kind() == Some(IngestSourceKind::Nats) fallback=|| ()>
            <SourceFieldEditor signals=signals field=SourceField::Subject />
            <SourceFieldEditor signals=signals field=SourceField::QueueGroup />
        </Show>
        <Show when=move || source_kind() == Some(IngestSourceKind::Pulsar) fallback=|| ()>
            <SourceFieldEditor signals=signals field=SourceField::Subscription />
        </Show>
        <Show when=move || source_kind() == Some(IngestSourceKind::Prometheus) fallback=|| ()>
            <SourceFieldEditor signals=signals field=SourceField::Query />
        </Show>
        <Show when=move || matches!(source_kind(), Some(IngestSourceKind::Http | IngestSourceKind::Prometheus)) fallback=|| ()>
            <SourceFieldEditor signals=signals field=SourceField::Every />
        </Show>
        <Show when=move || matches!(source_kind(), Some(IngestSourceKind::Kafka | IngestSourceKind::Pulsar | IngestSourceKind::Mqtt | IngestSourceKind::Nats | IngestSourceKind::RabbitMq | IngestSourceKind::Sqs)) fallback=|| ()>
            <SourceFieldEditor signals=signals field=SourceField::Instances />
        </Show>
        <Show when=move || source_kind() == Some(IngestSourceKind::Kafka) fallback=|| ()>
            <fieldset class="create-field create-ingestor-offset">
                <legend>"Offset ownership"</legend>
                <button type="button" data-offset="domain"
                    class:active=move || signals.ingestor.get().source.offset == Some(super::ingestor_source_draft::OffsetChoice::Domain)
                    disabled=pending
                    on:click=move |_| {
                        signals.ingestor.update(|draft| draft.source.offset = Some(super::ingestor_source_draft::OffsetChoice::Domain));
                        signals.edit();
                    }>"DOMAIN"</button>
                <button type="button" data-offset="consumer-group"
                    class:active=move || signals.ingestor.get().source.offset == Some(super::ingestor_source_draft::OffsetChoice::ConsumerGroup)
                    disabled=pending
                    on:click=move |_| {
                        signals.ingestor.update(|draft| draft.source.offset = Some(super::ingestor_source_draft::OffsetChoice::ConsumerGroup));
                        signals.edit();
                    }>"CONSUMER GROUP"</button>
            </fieldset>
            <Show when=move || signals.ingestor.get().source.offset == Some(super::ingestor_source_draft::OffsetChoice::ConsumerGroup) fallback=|| ()>
                <SourceFieldEditor signals=signals field=SourceField::ConsumerGroup />
            </Show>
        </Show>
        <Show when=move || !signals.ingestor.get().source.available_modes().is_empty() fallback=|| ()>
            <fieldset class="create-field create-ingestor-mode">
                <legend>"Delivery mode"</legend>
                <For each=move || signals.ingestor.get().source.available_modes().to_vec()
                    key=|mode| mode.key()
                    children=move |mode| view! {
                        <button type="button" data-mode=mode.key()
                            class:active=move || {
                                let source = &signals.ingestor.get().source;
                                source.delivery == Some(mode)
                                    || (source.available_modes().len() == 1
                                        && source.available_modes().first() == Some(&mode))
                            }
                            disabled=move || pending() || !matches!(source_kind(), Some(IngestSourceKind::Kafka | IngestSourceKind::Pulsar | IngestSourceKind::Mqtt))
                            on:click=move |_| {
                                signals.ingestor.update(|draft| draft.source.delivery = Some(mode));
                                signals.edit();
                            }>{mode.label()}</button>
                    } />
            </fieldset>
        </Show>
        <Show when=move || source_kind() == Some(IngestSourceKind::Mqtt) fallback=|| ()>
            <fieldset class="create-field create-ingestor-mqtt-session">
                <legend>"MQTT session"</legend>
                <button type="button" data-session="clean" class:active=move || signals.ingestor.get().source.mqtt_session == Some(MqttSession::Clean)
                    disabled=pending on:click=move |_| { signals.ingestor.update(|draft| draft.source.mqtt_session = Some(MqttSession::Clean)); signals.edit(); }>"CLEAN"</button>
                <button type="button" data-session="persistent" class:active=move || signals.ingestor.get().source.mqtt_session == Some(MqttSession::Persistent)
                    disabled=pending on:click=move |_| { signals.ingestor.update(|draft| draft.source.mqtt_session = Some(MqttSession::Persistent)); signals.edit(); }>"PERSISTENT"</button>
            </fieldset>
            <fieldset class="create-field create-ingestor-mqtt-qos">
                <legend>"MQTT QOS"</legend>
                <button type="button" data-qos="0" class:active=move || signals.ingestor.get().source.mqtt_qos == Some(MqttQos::AtMostOnce)
                    disabled=pending on:click=move |_| { signals.ingestor.update(|draft| draft.source.mqtt_qos = Some(MqttQos::AtMostOnce)); signals.edit(); }>"0"</button>
                <button type="button" data-qos="1" class:active=move || signals.ingestor.get().source.mqtt_qos == Some(MqttQos::AtLeastOnce)
                    disabled=pending on:click=move |_| { signals.ingestor.update(|draft| draft.source.mqtt_qos = Some(MqttQos::AtLeastOnce)); signals.edit(); }>"1"</button>
            </fieldset>
        </Show>
        <Show when=move || signals.ingestor.get().source.delivery == Some(DeliveryChoice::AckParallel) fallback=|| ()>
            <SourceFieldEditor signals=signals field=SourceField::AckMax />
            <SourceFieldEditor signals=signals field=SourceField::BatchTimeout />
        </Show>
        <Show when=acknowledged fallback=|| ()>
            <SourceFieldEditor signals=signals field=SourceField::AckTimeout />
            <SourceFieldEditor signals=signals field=SourceField::RetryBackoff />
            <SourceFieldEditor signals=signals field=SourceField::RetryMax />
        </Show>
        <Show when=move || !signals.ingestor.get().source.available_quiesce().is_empty() fallback=|| ()>
            <fieldset class="create-field create-ingestor-quiesce">
                <legend>"On quiesce"</legend>
                <For each=move || signals.ingestor.get().source.available_quiesce().to_vec()
                    key=|choice| choice.key()
                    children=move |choice| view! {
                        <button type="button" data-quiesce=choice.key()
                            class:active=move || signals.ingestor.get().source.quiesce == Some(choice)
                            disabled=move || pending() || (choice == QuiesceChoice::Suspend
                                && source_kind() == Some(IngestSourceKind::Mqtt)
                                && (signals.ingestor.get().source.mqtt_session != Some(MqttSession::Persistent)
                                    || signals.ingestor.get().source.mqtt_qos != Some(MqttQos::AtLeastOnce)))
                            on:click=move |_| {
                                signals.ingestor.update(|draft| draft.source.quiesce = Some(choice));
                                signals.edit();
                            }>{choice.label()}</button>
                    } />
            </fieldset>
        </Show>
        <Show when=move || signals.ingestor.get().source.quiesce == Some(QuiesceChoice::Buffer) fallback=|| ()>
            <SourceFieldEditor signals=signals field=SourceField::BufferSize />
            <Show when=move || source_kind() != Some(IngestSourceKind::Endpoint) fallback=|| ()>
                <fieldset class="create-field create-ingestor-overflow">
                    <legend>"On overflow"</legend>
                    <button type="button" data-overflow="oldest" class:active=move || signals.ingestor.get().source.overflow == Some(IngestQuiesceOverflow::DropOldest)
                        disabled=pending on:click=move |_| { signals.ingestor.update(|draft| draft.source.overflow = Some(IngestQuiesceOverflow::DropOldest)); signals.edit(); }>"DROP OLDEST"</button>
                    <button type="button" data-overflow="newest" class:active=move || signals.ingestor.get().source.overflow == Some(IngestQuiesceOverflow::DropNewest)
                        disabled=pending on:click=move |_| { signals.ingestor.update(|draft| draft.source.overflow = Some(IngestQuiesceOverflow::DropNewest)); signals.edit(); }>"DROP NEWEST"</button>
                </fieldset>
            </Show>
        </Show>
        <Show when=move || signals.ingestor.get().source.quiesce == Some(QuiesceChoice::Reject) fallback=|| ()>
            <SourceFieldEditor signals=signals field=SourceField::RetryAfter />
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
