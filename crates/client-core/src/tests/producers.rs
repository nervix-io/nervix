//! Producer tests over a loopback exchange: the test plays the server, answering each open and
//! batch the client sends and ending the exchange under it.
//!
//! Test harness outside the product layer order.
//! - **Owns.** Assertions that an open is answered with an attached producer or a typed refusal,
//!   that only a temporary refusal is sent again, that a cancelled wait keeps its submission and
//!   credit until the outcome is taken, and that a lost exchange leaves sent batches unknown and
//!   ends the producer.
//! - **Depends on.** The client's producer operations and the loopback exchange of the parent
//!   tests.
//! - **Must not know.** A server; nothing here reaches a network.

use std::{
    num::{NonZeroU32, NonZeroU64},
    sync::Arc as StdArc,
    time::Duration,
};

use bytes::Bytes;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{
    ClientRequest, OpenIngestorDisposition, OpenIngestorOutcome, ProducerId, ProducerOpened,
    ReplyBody, RequestId, SubmissionOutcome, SubmitBatchRequest,
};
use nervix_models::{
    AckWindow, ClientAttachmentId, ClientEndpointContract, ClientProducerAdmission,
    ClientProducerDescription, ClientProducerGrant, ClientProducerLimits, ClientProducerPolicy,
    ClientProducerRefusal, ClientSubmissionOutcome, ClientSubmissionRefusal, IngestorName,
};

use super::{DEADLINE, Loopback, domain, field};
use crate::{
    ClientError, Producer, ProducerBatch, ProducerEnd, ProducerOutcome, SubmissionUncertainty,
};

fn ingestor() -> IngestorName {
    IngestorName::parse("orders_in").assured("a literal ingestor name")
}

fn limits(batches: u32) -> ClientProducerLimits {
    ClientProducerLimits {
        batches: NonZeroU32::new(batches).assured("a literal non-zero count"),
        bytes: NonZeroU64::new(4_096).assured("a literal non-zero size"),
    }
}

fn description(batches: u32) -> ClientProducerDescription {
    ClientProducerDescription {
        attachment: ClientAttachmentId::from_u128(7),
        fields: vec![field("id", nervix_models::ParseAsType::String)],
        generation: 1,
        contract: ClientEndpointContract::from_digest([3; 32]),
        policy: ClientProducerPolicy {
            window: AckWindow::Sequential,
            ack_timeout: Duration::from_secs(30),
            retry_backoff: Duration::from_millis(1),
            retry_max_backoff: Duration::from_millis(2),
        },
        grant: ClientProducerGrant {
            batches: NonZeroU32::new(batches).assured("a literal non-zero count"),
            bytes: NonZeroU64::new(4_096).assured("a literal non-zero size"),
            max_batch_bytes: NonZeroU64::new(4_096).assured("a literal non-zero size"),
            max_batch_rows: NonZeroU32::new(16).assured("a literal non-zero count"),
        },
        admission: ClientProducerAdmission::Open,
    }
}

fn batch(body: &'static [u8]) -> ProducerBatch {
    ProducerBatch::from_arrow_ipc(Bytes::from_static(body))
}

fn outcome(outcome: ClientSubmissionOutcome) -> ReplyBody {
    ReplyBody::Submission(SubmissionOutcome {
        outcome,
        message: String::new(),
    })
}

/// Opens a producer granted `batches` batches, answering its open the way the exchange's reader
/// does: the producer is followed before its waiter completes.
async fn open(loopback: &mut Loopback, batches: u32) -> Producer {
    let client = loopback.client.clone();
    let opening = tokio::spawn(async move {
        client
            .open_ingestor(
                domain("tenant"),
                ingestor(),
                vec![field("id", nervix_models::ParseAsType::String)],
                limits(batches),
            )
            .await
    });
    let request = loopback.next_request().await;
    let ClientRequest::OpenIngestor(open) = request.request else {
        panic!("an open is sent as an OpenIngestor request");
    };
    assert_eq!(open.domain, domain("tenant"));
    assert_eq!(open.limits, limits(batches));
    let generation = loopback
        .client
        .inner
        .exchange
        .lock()
        .await
        .generation
        .clone();
    loopback.client.inner.events.sinks.producers.opened(
        &generation,
        ProducerId::opened_by(request.request_id),
        ClientProducerAdmission::Open,
    );
    let opened = OpenIngestorOutcome {
        disposition: OpenIngestorDisposition::Opened(Box::new(ProducerOpened {
            domain: domain("tenant"),
            ingestor: ingestor(),
            description: description(batches),
        })),
        message: "producer attached".to_string(),
    };
    loopback
        .answer(request.request_id, ReplyBody::OpenIngestor(opened))
        .await;
    tokio::time::timeout(DEADLINE, opening)
        .await
        .assured("the open is answered within the deadline")
        .assured("the open task completes")
        .assured("an answered open attaches the producer")
}

/// The next batch the producer sends, with the identity of its request.
async fn next_batch(loopback: &mut Loopback) -> (RequestId, SubmitBatchRequest) {
    let request = loopback.next_request().await;
    let ClientRequest::SubmitBatch(submit) = request.request else {
        panic!("a batch is sent as a SubmitBatch request");
    };
    (request.request_id, submit)
}

#[tokio::test]
async fn a_refused_open_leaves_nothing_attached() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    let client = loopback.client.clone();
    let opening = tokio::spawn(async move {
        client
            .open_ingestor(domain("tenant"), ingestor(), Vec::new(), limits(1))
            .await
    });
    let request = loopback.next_request().await;
    let refused = OpenIngestorOutcome {
        disposition: OpenIngestorDisposition::Refused(ClientProducerRefusal::SchemaMismatch),
        message: "the expected fields differ".to_string(),
    };
    loopback
        .answer(request.request_id, ReplyBody::OpenIngestor(refused))
        .await;
    let error = opening
        .await
        .assured("the open task completes")
        .err()
        .assured("a refused open attaches nothing");
    assert!(matches!(
        error.current_context(),
        ClientError::ProducerRefused {
            refusal: ClientProducerRefusal::SchemaMismatch,
            ..
        }
    ));
}

#[tokio::test]
async fn only_a_temporary_refusal_is_sent_again_and_the_same_bytes_are_sent() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    let producer = StdArc::new(open(&mut loopback, 2).await);

    let sending = tokio::spawn({
        let producer = producer.clone();
        async move { producer.send(batch(b"first")).await }
    });
    let (first_attempt, submitted) = next_batch(&mut loopback).await;
    assert_eq!(submitted.producer, producer.id());
    assert_eq!(submitted.batch, Bytes::from_static(b"first"));
    loopback
        .answer(
            first_attempt,
            outcome(ClientSubmissionOutcome::NotAdmitted(
                ClientSubmissionRefusal::Suspended,
            )),
        )
        .await;
    let (second_attempt, resent) = next_batch(&mut loopback).await;
    assert_ne!(second_attempt, first_attempt, "a resent batch is a new request");
    assert_eq!(resent.batch, Bytes::from_static(b"first"));
    loopback
        .answer(second_attempt, outcome(ClientSubmissionOutcome::Completed))
        .await;
    let completed = sending
        .await
        .assured("the send task completes")
        .assured("an open producer sends the batch");
    assert_eq!(completed, ProducerOutcome::Completed);

    let failing = tokio::spawn({
        let producer = producer.clone();
        async move { producer.send(batch(b"second")).await }
    });
    let (attempt, _) = next_batch(&mut loopback).await;
    loopback
        .answer(
            attempt,
            outcome(ClientSubmissionOutcome::OutcomeUnknown(
                nervix_models::ClientOutcomeUncertainty::Interrupted,
            )),
        )
        .await;
    let unknown = failing
        .await
        .assured("the send task completes")
        .assured("an open producer sends the batch");
    assert!(
        matches!(
            unknown,
            ProducerOutcome::OutcomeUnknown {
                cause: SubmissionUncertainty::Interrupted,
                ..
            }
        ),
        "an unknown outcome is reported, never replayed: {unknown:?}"
    );
}

#[tokio::test]
async fn a_cancelled_wait_keeps_its_submission_and_credit_until_the_outcome_is_taken() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    let producer = StdArc::new(open(&mut loopback, 1).await);

    let id = producer
        .submit(batch(b"held"))
        .await
        .assured("an open producer with credit takes the batch");
    let waiting = tokio::spawn({
        let producer = producer.clone();
        async move { producer.rejoin(id).await }
    });
    waiting.abort();
    let (attempt, _) = next_batch(&mut loopback).await;
    loopback
        .answer(attempt, outcome(ClientSubmissionOutcome::Completed))
        .await;

    // The outcome waits for the application, and so does the credit it holds.
    let blocked = tokio::spawn({
        let producer = producer.clone();
        async move { producer.submit(batch(b"next")).await }
    });
    let pending = tokio::time::timeout(DEADLINE, async {
        loop {
            tokio::task::consume_budget().await;
            let pending = producer.pending_submissions();
            if pending.iter().any(|submission| submission.outcome.is_some()) {
                return pending;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .assured("the outcome arrives within the deadline");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id, id);
    assert_eq!(pending[0].outcome, Some(ProducerOutcome::Completed));
    assert!(
        !blocked.is_finished(),
        "a batch waits for credit while an unread outcome holds it"
    );

    let rejoined = producer
        .rejoin(id)
        .await
        .assured("a held submission is rejoined");
    assert_eq!(rejoined, ProducerOutcome::Completed);
    assert!(producer.pending_submissions().is_empty());
    let next = tokio::time::timeout(DEADLINE, blocked)
        .await
        .assured("taking the outcome returns the credit the next batch waits for")
        .assured("the submit task completes")
        .assured("an open producer takes the batch");
    assert_ne!(next, id);
}

#[tokio::test]
async fn a_lost_exchange_leaves_sent_batches_unknown_and_ends_the_producer() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    let producer = StdArc::new(open(&mut loopback, 2).await);

    let sending = tokio::spawn({
        let producer = producer.clone();
        async move { producer.send(batch(b"in flight")).await }
    });
    let _attempt = next_batch(&mut loopback).await;
    loopback.replace_exchange().await;

    let lost = sending
        .await
        .assured("the send task completes")
        .assured("a sent batch has an outcome");
    assert!(
        matches!(
            lost,
            ProducerOutcome::OutcomeUnknown {
                cause: SubmissionUncertainty::SessionLost,
                ..
            }
        ),
        "a batch sent on a lost exchange is of unknown outcome: {lost:?}"
    );
    assert_eq!(producer.end(), Some(ProducerEnd::SessionLost));
    let refused = producer
        .submit(batch(b"after"))
        .await
        .err()
        .assured("an ended producer takes no batch");
    assert!(matches!(
        refused.current_context(),
        crate::ProducerError::Ended(ProducerEnd::SessionLost)
    ));
}
