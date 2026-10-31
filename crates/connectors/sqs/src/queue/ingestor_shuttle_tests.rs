//! How an SQS read meets the host's cancellation, explored under Shuttle.
//!
//! Layer: test harness.
//! - **Owns.** The invariants a read holds while the host cancels it at any point of its receives
//!   from a service that answers each one, whether it reads one message or gathers a parallel
//!   batch: every message the service took off the queue for a receive reaches the host, a
//!   cancelled read sends at most the one receive it had already decided to send, and each receive
//!   asks for what the batch still needs. And, from a service that never answers, that the read
//!   ends only once the host cancelled it and its budget passed.
//! - **Depends on.** The ingestor's read and its receive seam, an mpsc channel as the service's
//!   requests and oneshot channels as its answers, the model harness's Shuttle runner, and the
//!   task labels and timeout triggers of Shuttle's Tokio.
//! - **Must not know.** The AWS SDK, HTTP, or the host loop that cancels a read.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::{future::Future, num::NonZeroUsize, time::Duration};

use aws_sdk_sqs::types::Message as SqsMessage;
use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{
    IngestorBatch, IngestorBatchRequest, IngestorMessage as _, IngestorReadCancellation,
};
use nervix_model_harness::shuttle::check_interleavings;
use nervix_primitives::{
    sync::{Arc, mpsc, oneshot},
    time::{clear_triggers, trigger_timeouts},
};
use nervix_recovery::Discarded as _;

use super::{
    QueueReceive, ReceiveRequest, SqsQueueIngestor, SqsQueueIngestorError, SqsQueueIngestorResult,
};

const CHECK_TASK_JOINS: &str =
    "a check task that panics fails the execution before its join returns";

/// One receive the read sent: the service answers it through `answer`.
struct Receive {
    answer: oneshot::Sender<Vec<SqsMessage>>,
    /// How many messages the receive asked for.
    count: i32,
    /// Whether the host had already cancelled the read when the read sent this receive.
    after_cancellation: bool,
}

/// A queue whose receives a service task answers, and which records whether each one was sent
/// after the host cancelled the read.
struct ServedQueue {
    receives: mpsc::UnboundedSender<Receive>,
    cancellation: Arc<IngestorReadCancellation>,
}

impl QueueReceive for ServedQueue {
    fn receive(
        &self,
        receive: ReceiveRequest,
    ) -> impl Future<Output = SqsQueueIngestorResult<Vec<SqsMessage>>> + Send {
        let receives = self.receives.clone();
        let cancellation = Arc::clone(&self.cancellation);
        async move {
            let (answer, answered) = oneshot::channel();
            let receive = Receive {
                answer,
                count: receive.count,
                after_cancellation: cancellation.is_cancelled(),
            };
            if receives.send(receive).is_err() {
                panic!("the service takes receives until the read's queue is dropped");
            }
            match answered.await {
                Ok(messages) => Ok(messages),
                Err(_service_gone) => Err(Report::new(SqsQueueIngestorError::Receive)),
            }
        }
    }
}

/// What the service did with the receives it took.
#[derive(Default)]
struct Served {
    /// The ids of the messages the service took off the queue for a receive, in the order it took
    /// them. A message whose answer the read no longer waited for stays invisible until its
    /// visibility timeout.
    taken: Vec<u64>,
    /// The receives the read sent after the host had cancelled it.
    after_cancellation: usize,
    /// How many messages each receive asked for, in the order the read sent them.
    counts: Vec<i32>,
}

/// A message whose body is its id.
fn message(id: u64) -> SqsMessage {
    SqsMessage::builder()
        .body(id.to_string())
        .receipt_handle(format!("receipt-{id}"))
        .build()
}

/// Answers each receive the read sends with the next answer of `script`, and every receive after
/// the script with nothing, until the read's queue is dropped.
async fn serve(mut receives: mpsc::UnboundedReceiver<Receive>, script: &[&[u64]]) -> Served {
    let mut served = Served::default();
    let mut answers = script.iter();
    loop {
        nervix_primitives::task::consume_budget().await;
        let Some(receive) = receives.recv().await else {
            return served;
        };
        if receive.after_cancellation {
            served.after_cancellation = served
                .after_cancellation
                .checked_add(1)
                .assured("a check sends a handful of receives");
        }
        served.counts.push(receive.count);
        let ids: &[u64] = match answers.next() {
            Some(ids) => ids,
            None => &[],
        };
        let mut messages = Vec::with_capacity(ids.len());
        for id in ids {
            messages.push(message(*id));
        }
        // The service takes the messages off the queue before it answers, whether or not the read
        // still waits for the answer.
        served.taken.extend_from_slice(ids);
        receive.answer.send(messages).discarded(
            "a read that abandoned this receive loses what the service took, which `taken` counts",
        );
    }
}

/// The ids of the messages `batch` hands the host.
fn handed_over(batch: IngestorBatch<super::SqsQueueIngestorMessage>) -> Vec<u64> {
    let messages = match batch {
        IngestorBatch::Messages(messages) => messages,
        IngestorBatch::Interrupted { gathered } => gathered,
        IngestorBatch::Oversized { payloads } => {
            panic!("an SQS read drops no message for its size, yet it dropped {payloads}")
        }
        IngestorBatch::ResumeRequired | IngestorBatch::Closed => {
            panic!("a read of a queue whose URL is known neither resumes nor closes")
        }
    };
    let mut ids = Vec::with_capacity(messages.len());
    for message in messages {
        let body = std::str::from_utf8(message.payload()).assured("a check's bodies are ids");
        ids.push(body.parse().assured("a check's bodies are ids"));
    }
    ids
}

/// A host request for one message at a time, as `ACK SEQUENTIAL` reads.
fn one_message() -> IngestorBatchRequest {
    IngestorBatchRequest {
        max_messages: NonZeroUsize::MIN,
        batch_timeout: None,
    }
}

/// A host request for a parallel batch of `max` messages within a window no check outlasts, so
/// the batch ends once it is full or the host cancels it, never by its clock.
fn parallel_batch(max: usize) -> IngestorBatchRequest {
    IngestorBatchRequest {
        max_messages: NonZeroUsize::new(max).assured("a check asks for at least one message"),
        batch_timeout: Some(Duration::from_secs(3_600)),
    }
}

/// A read for `request` of a queue whose service answers its receives with `script`, which the
/// host cancels at any point: before it sends a receive, while a receive waits for its answer,
/// between receives, or after it returned. Returns how many messages each receive asked for.
fn a_cancelled_read_hands_over_every_message_its_receives_took(
    request: IngestorBatchRequest,
    script: &'static [&'static [u64]],
) -> Vec<i32> {
    shuttle::future::block_on(async move {
        let cancellation = Arc::new(IngestorReadCancellation::new());
        let (receives, taken) = mpsc::unbounded_channel();
        let service = nervix_primitives::task::spawn(serve(taken, script));
        let host = nervix_primitives::task::spawn({
            let cancellation = Arc::clone(&cancellation);
            async move {
                cancellation.cancel();
            }
        });
        let queue = ServedQueue {
            receives,
            cancellation: Arc::clone(&cancellation),
        };

        let gathered = SqsQueueIngestor::read_batch(&queue, request, &cancellation)
            .await
            .assured("a service that answers every receive fails none");
        drop(queue);
        host.await.assured(CHECK_TASK_JOINS);
        let served = service.await.assured(CHECK_TASK_JOINS);

        assert!(
            gathered.deferred.is_none(),
            "a service that answers every receive leaves no failure for the next read"
        );
        assert_eq!(
            handed_over(gathered.batch),
            served.taken,
            "every message the service took off the queue for a receive reaches the host"
        );
        assert!(
            served.after_cancellation <= 1,
            "a cancelled read sent {} receives after the cancellation",
            served.after_cancellation
        );
        served.counts
    })
}

/// A read cancelled at any point hands the host every message the receive it had in flight took
/// off the queue, whether that receive is the first or follows one that brought nothing.
#[test]
fn shuttle_a_cancelled_read_hands_over_every_message_its_receives_took() {
    check_interleavings(|| {
        a_cancelled_read_hands_over_every_message_its_receives_took(one_message(), &[&[1]]);
    });
    check_interleavings(|| {
        a_cancelled_read_hands_over_every_message_its_receives_took(one_message(), &[&[], &[1]]);
    });
}

/// A parallel read cancelled at any point while it tops its batch up hands the host every message
/// its receives took, and each of its receives asks only for what the batch still needs: the whole
/// batch until a message arrives, and the rest of it after that.
#[test]
fn shuttle_a_cancelled_parallel_read_hands_over_every_message_its_receives_gathered() {
    check_interleavings(|| {
        let counts = a_cancelled_read_hands_over_every_message_its_receives_took(
            parallel_batch(3),
            &[&[1], &[2, 3]],
        );
        let expected = [3, 2];
        assert!(
            expected.starts_with(&counts),
            "the read asked for {counts:?} messages, not a prefix of {expected:?}"
        );
    });
    check_interleavings(|| {
        let counts = a_cancelled_read_hands_over_every_message_its_receives_took(
            parallel_batch(3),
            &[&[], &[1, 2], &[], &[3]],
        );
        let expected = [3, 3, 1, 1];
        assert!(
            expected.starts_with(&counts),
            "the read asked for {counts:?} messages, not a prefix of {expected:?}"
        );
    });
}

/// Marks the reading task, whose cancelled-receive budget a check's clock expires.
#[derive(Debug, Clone, Copy)]
struct ReadDeadline;

/// A read whose service takes its receive and never answers it, which the host cancels at any
/// point while a clock expires the reading task's deadlines at any point.
fn a_cancelled_read_waits_for_an_unanswered_receive_only_until_its_budget() {
    // Timeout triggers are thread-local rather than per execution, so every execution starts
    // without the trigger an earlier one registered.
    clear_triggers();
    shuttle::future::block_on(async {
        shuttle::current::set_label_for_task(shuttle::current::me(), ReadDeadline);
        let cancellation = Arc::new(IngestorReadCancellation::new());
        let (receives, mut taken) = mpsc::unbounded_channel::<Receive>();
        // The service holds every receive it takes unanswered until the read's queue is dropped.
        let service = nervix_primitives::task::spawn(async move {
            let mut held = Vec::new();
            loop {
                nervix_primitives::task::consume_budget().await;
                let Some(receive) = taken.recv().await else {
                    return held.len();
                };
                held.push(receive);
            }
        });
        let host = nervix_primitives::task::spawn({
            let cancellation = Arc::clone(&cancellation);
            async move {
                cancellation.cancel();
            }
        });
        let clock = nervix_primitives::task::spawn(async {
            trigger_timeouts(|labels| labels.get::<ReadDeadline>().is_some());
        });
        let queue = ServedQueue {
            receives,
            cancellation: Arc::clone(&cancellation),
        };

        let ended = SqsQueueIngestor::read_batch(&queue, one_message(), &cancellation).await;

        assert!(
            cancellation.is_cancelled(),
            "a read whose receive is never answered ended before the host cancelled it"
        );
        drop(queue);
        host.await.assured(CHECK_TASK_JOINS);
        clock.await.assured(CHECK_TASK_JOINS);
        let sent = service.await.assured(CHECK_TASK_JOINS);
        match ended {
            Ok(gathered) => {
                let IngestorBatch::Interrupted { gathered } = gathered.batch else {
                    panic!("a read whose receive is never answered returned a batch");
                };
                assert!(
                    gathered.is_empty(),
                    "a service that never answers brings nothing"
                );
                assert_eq!(
                    sent, 0,
                    "only a read cancelled before it sent a receive ends at once"
                );
            }
            Err(report) => {
                assert_eq!(sent, 1, "a cancelled read sends no receive after its first");
                assert!(
                    matches!(
                        report.downcast_ref::<SqsQueueIngestorError>(),
                        Some(SqsQueueIngestorError::CancelledReceiveUnanswered { .. })
                    ),
                    "{report:?}"
                );
            }
        }
    });
}

/// A read cancelled while a receive the service never answers waits ends once its budget passes,
/// and never before the host cancelled it.
#[test]
fn shuttle_a_cancelled_read_waits_for_an_unanswered_receive_only_until_its_budget() {
    check_interleavings(a_cancelled_read_waits_for_an_unanswered_receive_only_until_its_budget);
}
