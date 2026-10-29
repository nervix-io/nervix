//! A paced poll's messages, split into intake batches and taken in by the production ingestor host.
//!
//! Layer: test harness.
//! - **Owns.** The intake batches one poll splits into for a bound, and driving the production
//!   ingestor host's poll intake, with the intake a paced ingestor's host has, through polls larger
//!   and smaller than their bound.
//! - **Depends on.** The poll intake's batches, the production ingestor host and the unfolding
//!   domain fixture of the extension tests.
//! - **Must not know.** Connector drivers or the paced loop that calls the host.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use nervix_connector::IngestorPollMessage;

use super::{
    extension_tests::{PACED_INTAKE, UnfoldingDomain},
    poll::PollIntakeBatches,
    *,
};

/// The instant every poll of these tests observed.
fn observed_at() -> Timestamp {
    Timestamp::from_unix_nanos(1)
}

/// The messages `{"user_id":<n>}` for each `n` from one through `count`, in that order.
fn messages_of_users(count: i64) -> Vec<IngestorPollMessage> {
    let mut messages = Vec::new();
    for user_id in 1..=count {
        messages.push(IngestorPollMessage {
            payload: format!(r#"{{"user_id":{user_id}}}"#).into_bytes(),
            headers: RetainedIngestHeaders::none(),
            metadata: nervix_connector::IngestorPollMetadata::Headers,
        });
    }
    messages
}

/// One poll that returned [`messages_of_users`] of `count`.
fn poll_of_users(count: i64) -> IngestorPoll {
    IngestorPoll {
        messages: messages_of_users(count),
        failures: Vec::new(),
        observed_at: observed_at(),
    }
}

/// The payloads of each batch `count` messages split into for `bound`, batch by batch.
fn batches_of(count: i64, bound: usize) -> Vec<Vec<Vec<u8>>> {
    let bound = NonZeroUsize::new(bound).verified("every bound these tests use is nonzero");
    let mut batches = Vec::new();
    for payload in PollIntakeBatches::new(messages_of_users(count), bound, observed_at()) {
        assert_eq!(payload.observed_at(), observed_at());
        batches.push(payload.payloads);
    }
    batches
}

/// The payloads of `range`'s messages, in order.
fn payloads_of(range: std::ops::RangeInclusive<i64>) -> Vec<Vec<u8>> {
    let mut payloads = Vec::new();
    for user_id in range {
        payloads.push(format!(r#"{{"user_id":{user_id}}}"#).into_bytes());
    }
    payloads
}

/// The `user_id`s of `range`, as the fixture's relay delivers them.
fn user_ids(range: std::ops::RangeInclusive<i64>) -> Vec<Option<RuntimeValue>> {
    range
        .map(|user_id| Some(RuntimeValue::I64(user_id)))
        .collect()
}

#[test]
fn a_poll_splits_into_consecutive_batches_no_larger_than_its_bound() {
    assert_eq!(
        batches_of(5, 2),
        [payloads_of(1..=2), payloads_of(3..=4), payloads_of(5..=5)]
    );
    assert_eq!(
        batches_of(2048, 1024),
        [payloads_of(1..=1024), payloads_of(1025..=2048)]
    );
    assert_eq!(
        batches_of(3, 1),
        [payloads_of(1..=1), payloads_of(2..=2), payloads_of(3..=3)]
    );
}

#[test]
fn a_poll_within_its_bound_is_one_batch_and_an_empty_poll_is_none() {
    assert_eq!(batches_of(3, 3), [payloads_of(1..=3)]);
    assert_eq!(batches_of(3, 1024), [payloads_of(1..=3)]);
    assert!(batches_of(0, 2).is_empty());
}

/// A poll larger than its intake batch bound enters the graph one ingest group per batch: the host
/// flushes each batch's group before it takes in the next, and leaves only the last for its caller.
#[nervix_primitives::test]
async fn a_poll_larger_than_its_bound_leaves_only_its_last_batch_for_the_caller() {
    let mut fixture = UnfoldingDomain::start().await;
    let (_shutdown_tx, shutdown) = watch::channel(false);
    let quiesce = fixture.quiesce();
    let mut host = fixture.host(quiesce, shutdown, PACED_INTAKE).await;
    let bound = NonZeroUsize::new(2).verified("two is a nonzero intake batch bound");

    let entered = host.intake_poll(poll_of_users(5), bound).await;
    assert!(
        matches!(entered, Ok(true)),
        "the poll's messages enter their groups: {entered:?}"
    );
    assert_eq!(
        host.collector.len(),
        1,
        "the groups of the first two batches flushed, and the last batch waits in its own"
    );
    host.flush()
        .await
        .expect("the group holding the last batch flushes");

    assert_eq!(fixture.delivered_user_ids(5).await, user_ids(1..=5));
    fixture.finish().await;
}

/// A poll no larger than its bound is one intake batch, which waits whole in one ingest group for
/// its caller's flush.
#[nervix_primitives::test]
async fn a_poll_within_its_bound_waits_whole_in_one_group() {
    let mut fixture = UnfoldingDomain::start().await;
    let (_shutdown_tx, shutdown) = watch::channel(false);
    let quiesce = fixture.quiesce();
    let mut host = fixture.host(quiesce, shutdown, PACED_INTAKE).await;
    let bound = NonZeroUsize::new(3).verified("three is a nonzero intake batch bound");

    let entered = host.intake_poll(poll_of_users(3), bound).await;
    assert!(
        matches!(entered, Ok(true)),
        "the poll's messages enter their group: {entered:?}"
    );
    assert_eq!(host.collector.len(), 3);
    host.flush()
        .await
        .expect("the group holding the poll flushes");

    assert_eq!(fixture.delivered_user_ids(3).await, user_ids(1..=3));
    fixture.finish().await;
}

/// A poll that returned no message enters nothing and leaves no group to flush.
#[nervix_primitives::test]
async fn an_empty_poll_enters_nothing() {
    let mut fixture = UnfoldingDomain::start().await;
    let (_shutdown_tx, shutdown) = watch::channel(false);
    let quiesce = fixture.quiesce();
    let mut host = fixture.host(quiesce, shutdown, PACED_INTAKE).await;

    let entered = host.intake_poll(poll_of_users(0), NonZeroUsize::MIN).await;
    assert!(
        matches!(entered, Ok(false)),
        "an empty poll enters no group: {entered:?}"
    );
    assert_eq!(host.collector.len(), 0);
    fixture.finish().await;
}
