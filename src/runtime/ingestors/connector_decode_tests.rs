//! Ingestor payloads their ingestor's codec refuses, as the production ingestor host consumes them.
//!
//! Layer: test harness.
//! - **Owns.** Driving the production ingestor host's acknowledged, unacknowledged, poll and retained
//!   intake with payloads the codec refuses beside payloads it decodes, and the delivery,
//!   acknowledgement, report and accounting each path promises for them.
//! - **Depends on.** The production ingestor host, the unfolding domain fixture of the extension
//!   tests, and the runtime's event bus.
//! - **Must not know.** Broker drivers or the ingestor loops that call the host.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use nervix_connector::{IngestorPollMessage, NoIngestHeaders, RetainedIngestHeaders};
use nervix_models::{IngestQuiesceMode, IngestQuiesceOverflow};

use super::{
    extension_tests::{UnfoldingDomain, WAITING_INTAKE, quiesce_control},
    *,
};

/// A payload the fixture's codec refuses: its `user_id` is not an integer.
const REFUSED: &[u8] = br#"{"user_id":"seven"}"#;

/// The intake batch bound that takes each poll in as one batch, so a refused message shares its
/// batch with the messages beside it.
const WHOLE_POLL: NonZeroUsize = NonZeroUsize::MAX;

/// The report the fixture's ingestor logs for a payload its codec refused.
const REJECTION: &str =
    "ingestor 'event_source' general error in domain 'default': decode rejection ";

fn message(payload: &[u8]) -> IngestorIntakeMessage<'_> {
    IngestorIntakeMessage {
        payload,
        metadata: IngestMetadataRow::headers(&NoIngestHeaders),
    }
}

/// The next error the fixture's runtime reports.
async fn next_error(errors: &mut broadcast::Receiver<RuntimeEvent>) -> String {
    let event = nervix_primitives::time::timeout(Duration::from_secs(10), errors.recv())
        .await
        .expect("the runtime reports the rejection")
        .expect("the event bus keeps its subscriber");
    let RuntimeEvent::Error(message) = event;
    message
}

/// Asserts that `report` is the logged rejection of a payload the fixture's codec refused, and
/// that the fixture's ingestor counted it and describes it as its last.
fn assert_rejected(fixture: &UnfoldingDomain, report: &str, rejections: u64) {
    assert!(report.starts_with(REJECTION), "{report}");
    assert!(
        report.contains(": codec 'event_json' refused the payload: "),
        "the rejection names the codec and no transport position for a header source: {report}"
    );
    let described = fixture
        .runtime
        .describe_local_ingestor(&fixture.domain, &fixture.ingestor)
        .expect("the fixture's ingestor describes itself");
    assert_eq!(described.decode_rejections_total, rejections);
    let last = described
        .last_decode_rejection
        .assured("a refused payload is the ingestor's last decode rejection");
    assert!(
        report.ends_with(&last.to_string()),
        "the event reports the record the ingestor describes: {report} / {last}"
    );
    assert_eq!(last.position, None);
    assert_eq!(
        last.fields
            .iter()
            .map(FieldPath::as_str)
            .collect::<Vec<_>>(),
        ["user_id"],
        "the field of the message the unfolding failed in is named at fault"
    );
}

/// An acknowledged batch whose codec refuses one payload delivers the other and is acknowledged,
/// so its ingestor acknowledges the refused payload's position too instead of presenting it again.
#[nervix_primitives::test]
async fn an_acknowledged_batch_consumes_a_payload_its_codec_refuses_and_delivers_the_rest() {
    let mut fixture = UnfoldingDomain::start().await;
    let mut errors = fixture.runtime.events().subscribe();
    let (_shutdown_tx, shutdown) = watch::channel(false);
    let quiesce = fixture.quiesce();
    let mut host = fixture.host(quiesce, shutdown, WAITING_INTAKE).await;

    let decoded = br#"{"user_id":7}"#;
    let mut acknowledgement = host
        .intake_acknowledged(vec![message(REFUSED), message(decoded)])
        .await
        .expect("a payload its codec refuses does not fail its batch");

    let report = next_error(&mut errors).await;
    assert_eq!(
        fixture.delivered_user_ids(1).await,
        vec![Some(RuntimeValue::I64(7))]
    );
    assert_eq!(
        acknowledgement.wait(Duration::from_secs(10)).await,
        IngestorAcknowledgementOutcome::Ack,
        "the batch is acknowledged with its refused payload"
    );
    assert_rejected(&fixture, &report, 1);
    fixture.finish().await;
}

/// A batch whose every payload its codec refuses dispatches nothing and is acknowledged at once.
#[nervix_primitives::test]
async fn an_acknowledged_batch_its_codec_refuses_whole_is_acknowledged() {
    let mut fixture = UnfoldingDomain::start().await;
    let mut errors = fixture.runtime.events().subscribe();
    let (_shutdown_tx, shutdown) = watch::channel(false);
    let quiesce = fixture.quiesce();
    let mut host = fixture.host(quiesce, shutdown, WAITING_INTAKE).await;

    let mut acknowledgement = host
        .intake_acknowledged(vec![message(REFUSED), message(REFUSED)])
        .await
        .expect("payloads their codec refuses do not fail their batch");

    let first = next_error(&mut errors).await;
    let second = next_error(&mut errors).await;
    assert_eq!(
        acknowledgement.wait(Duration::from_secs(10)).await,
        IngestorAcknowledgementOutcome::Ack
    );
    assert!(first.starts_with(REJECTION), "{first}");
    assert_rejected(&fixture, &second, 2);
    assert_ne!(
        first, second,
        "each refused payload has a rejection of its own"
    );
    assert_eq!(
        host.collector.len(),
        0,
        "nothing of the batch stays in the group"
    );
    fixture.finish().await;
}

/// An unacknowledged batch whose codec refuses one payload drops it with its rejection and delivers
/// the other.
#[nervix_primitives::test]
async fn an_unacknowledged_batch_drops_a_payload_its_codec_refuses_and_delivers_the_rest() {
    let mut fixture = UnfoldingDomain::start().await;
    let mut errors = fixture.runtime.events().subscribe();
    let (_shutdown_tx, shutdown) = watch::channel(false);
    let quiesce = fixture.quiesce();
    let mut host = fixture.host(quiesce, shutdown, WAITING_INTAKE).await;

    let decoded = br#"{"user_id":8}"#;
    host.intake_unacknowledged(vec![message(decoded), message(REFUSED)])
        .await
        .expect("a payload its codec refuses does not fail its batch");

    let report = next_error(&mut errors).await;
    assert_eq!(
        fixture.delivered_user_ids(1).await,
        vec![Some(RuntimeValue::I64(8))]
    );
    assert_rejected(&fixture, &report, 1);
    assert_eq!(
        host.unfolding_refused.get(),
        0,
        "a codec refusal is not a refusal for room"
    );
    fixture.finish().await;
}

/// An unacknowledged batch whose every payload its codec refuses dispatches nothing.
#[nervix_primitives::test]
async fn an_unacknowledged_batch_its_codec_refuses_whole_dispatches_nothing() {
    let mut fixture = UnfoldingDomain::start().await;
    let mut errors = fixture.runtime.events().subscribe();
    let (_shutdown_tx, shutdown) = watch::channel(false);
    let quiesce = fixture.quiesce();
    let mut host = fixture.host(quiesce, shutdown, WAITING_INTAKE).await;

    host.intake_unacknowledged(vec![message(REFUSED)])
        .await
        .expect("a payload its codec refuses does not fail its batch");

    let report = next_error(&mut errors).await;
    assert_rejected(&fixture, &report, 1);
    assert_eq!(
        host.collector.len(),
        0,
        "nothing of the batch enters the group"
    );
    fixture.finish().await;
}

/// A poll whose every message its codec refuses puts nothing into the ingest group.
#[nervix_primitives::test]
async fn a_poll_its_codec_refuses_whole_enters_nothing() {
    let mut fixture = UnfoldingDomain::start().await;
    let mut errors = fixture.runtime.events().subscribe();
    let (_shutdown_tx, shutdown) = watch::channel(false);
    let quiesce = fixture.quiesce();
    let mut host = fixture.host(quiesce, shutdown, WAITING_INTAKE).await;

    let polled = host
        .intake_poll(
            IngestorPoll {
                messages: vec![IngestorPollMessage {
                    payload: REFUSED.to_vec(),
                    headers: RetainedIngestHeaders::none(),
                    metadata: nervix_connector::RetainedIngestMetadata::none(),
                }],
                failures: Vec::new(),
                observed_at: Timestamp::from_unix_nanos(1),
            },
            WHOLE_POLL,
        )
        .await;

    assert!(
        matches!(polled, Ok(false)),
        "no message of the poll enters the ingest group: {polled:?}"
    );
    let report = next_error(&mut errors).await;
    assert_rejected(&fixture, &report, 1);
    assert_eq!(host.collector.len(), 0);
    fixture.finish().await;
}

/// A poll whose codec refuses one of its messages delivers the others.
#[nervix_primitives::test]
async fn a_poll_consumes_a_message_its_codec_refuses_and_delivers_the_rest() {
    let mut fixture = UnfoldingDomain::start().await;
    let mut errors = fixture.runtime.events().subscribe();
    let (_shutdown_tx, shutdown) = watch::channel(false);
    let quiesce = fixture.quiesce();
    let mut host = fixture.host(quiesce, shutdown, WAITING_INTAKE).await;

    let polled = host
        .intake_poll(
            IngestorPoll {
                messages: vec![
                    IngestorPollMessage {
                        payload: REFUSED.to_vec(),
                        headers: RetainedIngestHeaders::none(),
                        metadata: nervix_connector::RetainedIngestMetadata::none(),
                    },
                    IngestorPollMessage {
                        payload: br#"{"user_id":9}"#.to_vec(),
                        headers: RetainedIngestHeaders::none(),
                        metadata: nervix_connector::RetainedIngestMetadata::none(),
                    },
                ],
                failures: Vec::new(),
                observed_at: Timestamp::from_unix_nanos(1),
            },
            WHOLE_POLL,
        )
        .await;

    assert!(
        matches!(polled, Ok(true)),
        "the poll's other message enters the ingest group: {polled:?}"
    );
    let report = next_error(&mut errors).await;
    host.flush()
        .await
        .expect("the group holding the poll flushes");
    assert_eq!(
        fixture.delivered_user_ids(1).await,
        vec![Some(RuntimeValue::I64(9))]
    );
    assert_rejected(&fixture, &report, 1);
    fixture.finish().await;
}

/// A payload a quiesce buffer retained leaves the buffer as a decode rejection when its codec
/// refuses it as the buffer drains.
#[nervix_primitives::test]
async fn a_retained_payload_its_codec_refuses_leaves_the_buffer_as_a_decode_rejection() {
    let mut fixture = UnfoldingDomain::start().await;
    let mut errors = fixture.runtime.events().subscribe();
    let quiesce = quiesce_control(
        &fixture,
        IngestQuiesceMode::Buffer {
            max_size: "1MiB".to_string(),
            overflow: IngestQuiesceOverflow::DropOldest,
        },
    );
    let (_shutdown_tx, shutdown) = watch::channel(false);
    let mut host = fixture
        .host(quiesce.clone(), shutdown, WAITING_INTAKE)
        .await;

    quiesce.engage(IngestorQuiesceCause::EntityHold);
    host.intake_unacknowledged(vec![message(REFUSED)])
        .await
        .expect("a quiesced intake retains the payload");
    assert_eq!(quiesce.counters().buffered_records, 1);

    quiesce.release(IngestorQuiesceCause::EntityHold);
    let drained = host.replay_buffered().await;
    assert!(
        matches!(drained, Ok(true)),
        "the refused payload leaves the buffer: {drained:?}"
    );
    let report = next_error(&mut errors).await;
    assert_rejected(&fixture, &report, 1);
    assert_eq!(quiesce.counters().buffered_records, 0);
    assert_eq!(quiesce.counters().buffered_bytes, 0);
    fixture.finish().await;
}
