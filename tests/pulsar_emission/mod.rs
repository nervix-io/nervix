//! Public checks of what a Pulsar emitter writes beside a message's payload.
//!
//! Outside the layer order: a scenario harness.
//!
//! - **Owns.** Observing a topic through a shared subscription, the broker policy that bounds a
//!   topic's delivery delay, and the assertions on the event time, delivery time and publish time
//!   of the messages a broker delivers.
//! - **Depends on.** The broker observer, the scenario world and the broker's admin API.
//! - **Must not know.** Product runtime state or connector implementation details.

use super::*;

#[given(expr = "Pulsar topic {string} is observed through a shared subscription")]
async fn given_pulsar_topic_is_observed_through_a_shared_subscription(
    world: &mut ScenarioWorld,
    topic: String,
) {
    let topic = expand_placeholders(world, &topic);
    world.broker_observer = Some(
        world
            .cluster()
            .observe_pulsar_shared(&topic)
            .await
            .expect("failed to observe the Pulsar topic through a shared subscription"),
    );
}

#[given(expr = "Pulsar topic {string} allows a delivery delay of at most {string}")]
async fn given_pulsar_topic_allows_a_delivery_delay_of_at_most(
    world: &mut ScenarioWorld,
    topic: String,
    maximum: String,
) {
    let topic = expand_placeholders(world, &topic);
    let maximum = parse_duration_text(&maximum).expect("step bound must be a valid duration");
    world
        .cluster()
        .limit_pulsar_topic_delivery_delay(&topic, maximum)
        .await
        .unwrap_or_else(|error| {
            panic!("failed to bound the delivery delay of Pulsar topic '{topic}': {error}")
        });
}

/// A message's payload bytes together with the event time its producer wrote.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ExpectedPulsarMessage {
    payload: Vec<u8>,
    event_time: Option<u64>,
}

impl BrokerMessageExpectation for ExpectedPulsarMessage {
    fn delivered(message: &BrokerMessage) -> Self {
        let times = message
            .pulsar_times
            .expect("a Pulsar observer delivers every message with its times");
        Self {
            payload: message.bytes.clone(),
            event_time: times.event_time,
        }
    }

    fn describe(&self) -> String {
        format!(
            "{} with event time {}",
            describe_broker_payload(&self.payload),
            describe_event_time(self.event_time)
        )
    }
}

/// An event time as a failure message writes it: the instant in RFC 3339, or that there is none.
fn describe_event_time(event_time: Option<u64>) -> String {
    let Some(unix_millis) = event_time else {
        return "none".to_string();
    };
    let Ok(signed_millis) = i64::try_from(unix_millis) else {
        return format!("Unix millisecond {unix_millis}");
    };
    match chrono::DateTime::from_timestamp_millis(signed_millis) {
        Some(instant) => instant.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        None => format!("Unix millisecond {unix_millis}"),
    }
}

/// An event time cell: absent when empty, otherwise the instant it writes in RFC 3339, which a
/// Pulsar message carries in whole milliseconds.
fn expected_event_time(cell: &str) -> Option<u64> {
    let cell = cell.trim();
    if cell.is_empty() {
        return None;
    }
    let instant = chrono::DateTime::parse_from_rfc3339(cell)
        .unwrap_or_else(|error| panic!("event time cell {cell:?} is not RFC 3339: {error}"));
    assert_eq!(
        instant.timestamp_subsec_nanos() % 1_000_000,
        0,
        "event time cell {cell:?} writes a time finer than the milliseconds a message carries"
    );
    let unix_millis = u64::try_from(instant.timestamp_millis())
        .unwrap_or_else(|error| panic!("event time cell {cell:?} precedes the epoch: {error}"));
    Some(unix_millis)
}

/// Like the message table step, where every row is one exact message: its payload and the event
/// time its producer wrote, in RFC 3339 at the millisecond a Pulsar message carries. An empty cell
/// is a message with no event time.
#[then(expr = "within {string} the observed broker receives exactly these Pulsar messages")]
async fn then_within_duration_the_observed_broker_receives_exactly_these_pulsar_messages(
    world: &mut ScenarioWorld,
    duration: String,
    #[step] step: &Step,
) {
    let table = step
        .table
        .as_ref()
        .expect("the step lists every expected message in a table");
    let mut rows = table.rows.iter();
    let header = rows
        .next()
        .expect("the message table starts with its column names");
    assert_eq!(
        header.as_slice(),
        ["payload", "event time"],
        "the Pulsar message table names its columns payload and event time"
    );
    let mut expected = BTreeMap::<ExpectedPulsarMessage, usize>::new();
    for row in rows {
        let [payload, event_time] = row.as_slice() else {
            panic!("each expected Pulsar message row has two cells, got {row:?}");
        };
        let message = ExpectedPulsarMessage {
            payload: expand_placeholders(world, payload).into_bytes(),
            event_time: expected_event_time(event_time),
        };
        *expected.entry(message).or_insert(0) += 1;
    }
    receive_exactly_these_broker_messages(world, &duration, expected).await;
}

/// The times of the last message a step received.
fn last_pulsar_times(world: &ScenarioWorld) -> PulsarMessageTimes {
    world
        .last_pulsar_times
        .expect("a step must receive a Pulsar message before asserting on its times")
}

/// The event time of `times`, signed like the delivery time it is compared with.
fn event_time_of(times: PulsarMessageTimes) -> i64 {
    let event_time = times
        .event_time
        .unwrap_or_else(|| panic!("the Pulsar message carries no event time: {times:?}"));
    i64::try_from(event_time).expect("an event time of this era fits a signed Unix millisecond")
}

fn duration_millis(duration: &str) -> i64 {
    let duration = parse_duration_text(duration).expect("step bound must be a valid duration");
    i64::try_from(duration.as_millis()).expect("a step's bound fits Unix milliseconds")
}

/// The last message's delivery time is exactly `after` past its event time. A statement that writes
/// `TIMESTAMP now()` reads the batch's execution time as the event time, so this measures the
/// delivery time from that execution time rather than from when the message was sent.
#[then(expr = "the last observed Pulsar message is due {string} after its event time")]
fn then_last_observed_pulsar_message_is_due_after_its_event_time(
    world: &mut ScenarioWorld,
    after: String,
) {
    let times = last_pulsar_times(world);
    let event_time = event_time_of(times);
    let deliver_at = times
        .deliver_at
        .unwrap_or_else(|| panic!("the Pulsar message carries no delivery time: {times:?}"));
    assert_eq!(
        deliver_at.checked_sub(event_time),
        Some(duration_millis(&after)),
        "the Pulsar message is not due {after} after its event time: {times:?}"
    );
}

/// The broker held the last message back: this host, whose clock the broker's container shares,
/// took it at least `after` past its event time. Delay is measured here rather than a silence
/// asserted, so a loaded machine only lengthens it.
#[then(expr = "the last observed Pulsar message arrived at least {string} after its event time")]
fn then_last_observed_pulsar_message_arrived_at_least_after_its_event_time(
    world: &mut ScenarioWorld,
    after: String,
) {
    let times = last_pulsar_times(world);
    let received_at =
        i64::try_from(times.received_at).expect("this host's clock fits a signed Unix millisecond");
    let held = received_at
        .checked_sub(event_time_of(times))
        .expect("both instants are Unix milliseconds of this era");
    assert!(
        held >= duration_millis(&after),
        "the Pulsar message arrived {held} ms after its event time, before {after}: {times:?}"
    );
}

#[then(expr = "the last observed Pulsar message carries no delivery time")]
fn then_last_observed_pulsar_message_carries_no_delivery_time(world: &mut ScenarioWorld) {
    let times = last_pulsar_times(world);
    assert_eq!(
        times.deliver_at, None,
        "the Pulsar message carries a delivery time: {times:?}"
    );
}

/// Reads the copies of one message until the one a later attempt sent: its client published it at
/// least `after` past its event time. An attempt the emitter gave up on may reach the broker too,
/// once its connection resumes, so copies before that one are read past. Every copy carries the
/// event time and delivery time of the first.
#[then(
    expr = "within {string} the observed broker receives this Pulsar message sent again at least \
            {string} after its event time"
)]
async fn then_within_duration_the_observed_broker_receives_this_pulsar_message_sent_again(
    world: &mut ScenarioWorld,
    duration: String,
    after: String,
    #[step] step: &Step,
) {
    let expected_payload = expand_placeholders(world, docstring(step));
    let duration = parse_duration_text(&duration).expect("step duration must be a valid duration");
    let after = duration_millis(&after);
    let deadline = Instant::now() + duration;
    let mut first: Option<PulsarMessageTimes> = None;
    loop {
        nervix_primitives::task::consume_budget().await;
        let remaining = deadline.saturating_duration_since(Instant::now());
        let message = world
            .broker_observer
            .as_mut()
            .expect("a broker observer must exist before assertion")
            .try_next_message(remaining)
            .await
            .expect("failed while waiting for a resent Pulsar message");
        let Some(message) = message else {
            panic!(
                "timed out waiting for a copy of the Pulsar message sent {after} ms past its \
                 event time; the first copy was {first:?}"
            );
        };
        assert!(
            payload_matches_expected(&message.payload, &expected_payload),
            "expected payload fragment {} in a resent Pulsar message, got: {}",
            expected_payload.trim(),
            message.payload
        );
        let times = message
            .pulsar_times
            .expect("a Pulsar observer delivers every message with its times");
        if let Some(first) = first {
            assert_eq!(
                (first.event_time, first.deliver_at),
                (times.event_time, times.deliver_at),
                "a resent copy changed what its message carries"
            );
        }
        first = first.or(Some(times));
        world.last_pulsar_times = Some(times);
        let published_at =
            i64::try_from(times.published_at).expect("a publish time fits a signed millisecond");
        let sent_after = published_at
            .checked_sub(event_time_of(times))
            .expect("both instants are Unix milliseconds of this era");
        if sent_after >= after {
            return;
        }
    }
}
