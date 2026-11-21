//! Test harness for charged receive-side wire decoding.
//! Owns: held-input admission, release and complete-value regression checks.
//! Depends on: the production wire decoder, current envelopes and the execution budget.
//! Must not know: runtime graph semantics or network scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use futures_util::FutureExt as _;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::{AdmissionError, ChargedBytes, CpuClass, Executor, MemoryClass};
use nervix_recovery::Discarded as _;

use crate::{PoolClass, request::RequestEnvelope};

fn request() -> RequestEnvelope {
    RequestEnvelope {
        class: PoolClass::Bulk,
        request: "fetch_state_snapshot".to_owned(),
        payload: vec![7; 1024],
    }
}

async fn outer_bulk_decode_refuses_behind_a_waiter_that_needs_its_input() {
    let executor = Executor::default();
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&request())
        .assured("the current fixture envelope serializes")
        .to_vec();
    let capacity = executor.memory(MemoryClass::Bulk).capacity_bytes;
    let held = executor
        .try_reserve(MemoryClass::Bulk, capacity - 8192)
        .assured("the fixture leaves room for the encoded input");
    let input = executor
        .try_reserve(MemoryClass::Bulk, 4096)
        .assured("the encoded envelope fits");
    let waiting = executor.reserve(MemoryClass::Bulk, 8192);
    let mut waiting = std::pin::pin!(waiting);
    assert!(waiting.as_mut().now_or_never().is_none());
    let result = super::decode_rkyv::<RequestEnvelope>(
        &executor,
        MemoryClass::Bulk,
        CpuClass::Bulk,
        ChargedBytes::from_owned(bytes, input),
    )
    .now_or_never();
    assert!(
        result.is_some(),
        "decoding must refuse without retaining its input behind the waiter"
    );
    let error = result
        .verified("the decoder returned immediately")
        .err()
        .assured("the queued reservation leaves no decode room");
    assert!(matches!(
        error.downcast_ref::<AdmissionError>(),
        Some(AdmissionError::BudgetExhausted { class: "bulk", .. })
    ));
    drop(
        waiting
            .await
            .assured("releasing the input lets the earlier waiter finish"),
    );
    drop(held);
    assert_eq!(executor.memory(MemoryClass::Bulk).reserved_bytes, 0);
}

async fn nested_bulk_decode_refuses_behind_a_waiter_that_needs_its_envelope() {
    let executor = Executor::default();
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&request())
        .assured("the current fixture envelope serializes")
        .to_vec();
    let capacity = executor.memory(MemoryClass::Bulk).capacity_bytes;
    let held = executor
        .try_reserve(MemoryClass::Bulk, capacity - 8192)
        .assured("the fixture leaves room for the envelope");
    let envelope = executor
        .try_reserve(MemoryClass::Bulk, 4096)
        .assured("the caller's outer envelope fits");
    let waiting = executor.reserve(MemoryClass::Bulk, 8192);
    let mut waiting = std::pin::pin!(waiting);
    assert!(waiting.as_mut().now_or_never().is_none());
    let result = super::decode_rkyv_payload::<RequestEnvelope>(
        &executor,
        MemoryClass::Bulk,
        CpuClass::Bulk,
        bytes,
    )
    .now_or_never();
    assert!(
        result.is_some(),
        "nested decoding must refuse before waiting on its caller's envelope"
    );
    let error = result
        .verified("the decoder returned immediately")
        .err()
        .assured("the queued reservation leaves no decode room");
    assert!(matches!(
        error.downcast_ref::<AdmissionError>(),
        Some(AdmissionError::BudgetExhausted { class: "bulk", .. })
    ));
    drop(envelope);
    drop(
        waiting
            .await
            .assured("releasing the outer envelope lets the waiter finish"),
    );
    drop(held);
    assert_eq!(executor.memory(MemoryClass::Bulk).reserved_bytes, 0);
}

async fn nested_decode_cancellation_keeps_its_own_input_charge() {
    use std::num::NonZeroUsize;

    use nervix_execution::{ExecutionConfig, WorkerCounts};
    use nervix_primitives::sync::oneshot;

    let executor = Executor::new(ExecutionConfig {
        workers: WorkerCounts {
            bulk_cpu: NonZeroUsize::MIN,
            ..WorkerCounts::default()
        },
        ..ExecutionConfig::default()
    })
    .assured("one bulk worker preserves the default memory bounds");
    let (started, has_started) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let occupied = executor.clone();
    let occupying_charge = executor
        .try_reserve(MemoryClass::Bulk, 1)
        .assured("the occupying job holds one byte beside its worker");
    let occupying = nervix_primitives::task::spawn(async move {
        occupied
            .run_cpu(CpuClass::Bulk, occupying_charge, move |_, _| {
                started
                    .send(())
                    .assured("the fixture waits for its occupying job");
                released
                    .blocking_recv()
                    .discarded("sending or dropping the fixture's release frees its worker");
            })
            .await
            .assured("the occupying job completes after release");
    });
    has_started
        .await
        .assured("the sole bulk worker is occupied");
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&request())
        .assured("the current fixture envelope serializes")
        .to_vec();
    let length = u64::try_from(bytes.len()).assured("the bounded fixture length fits");
    let envelope = executor
        .try_reserve(MemoryClass::Bulk, length)
        .assured("the caller owns the outer envelope");
    let mut decoding = Box::pin(super::decode_rkyv_payload::<RequestEnvelope>(
        &executor,
        MemoryClass::Bulk,
        CpuClass::Bulk,
        bytes,
    ));
    assert!(decoding.as_mut().now_or_never().is_none());
    assert_eq!(executor.snapshot().bulk_cpu.pending, 1);
    drop(envelope);
    assert_eq!(
        executor.memory(MemoryClass::Bulk).reserved_bytes,
        length
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(1))
            .assured("two bounded allocations and the occupying byte fit"),
        "the queued decoder owns both input and output after its caller's envelope ends"
    );
    drop(decoding);
    assert_eq!(executor.snapshot().bulk_cpu.pending, 0);
    assert_eq!(executor.memory(MemoryClass::Bulk).reserved_bytes, 1);
    release
        .send(())
        .assured("the occupying job is still waiting");
    occupying
        .await
        .assured("the fixture joins the occupying job");
    assert_eq!(executor.memory(MemoryClass::Bulk).reserved_bytes, 0);
    let (value, charge) = super::decode_rkyv_payload::<RequestEnvelope>(
        &executor,
        MemoryClass::Bulk,
        CpuClass::Bulk,
        rkyv::to_bytes::<rkyv::rancor::Error>(&request())
            .assured("the current fixture serializes")
            .to_vec(),
    )
    .await
    .assured("fresh decode progresses after cancellation")
    .into_parts();
    assert_eq!(value, request());
    drop(value);
    drop(charge);
    assert_eq!(executor.memory(MemoryClass::Bulk).reserved_bytes, 0);
}

/// A request frame its caller still holds is refused at once when its CPU class has no worker and
/// no place in its queue, and it keeps no charge, so a burst beyond the class cannot hold the
/// class's memory. A response frame carries work a handler already did, so its encoding and its
/// decoding wait for a place instead, and both complete once the class frees.
#[cfg(not(feature = "shuttle"))]
#[nervix_primitives::test]
async fn a_full_class_refuses_a_request_frame_and_holds_a_response_frame_back() {
    use std::num::NonZeroUsize;

    use nervix_execution::{
        ExecutionConfig, ExecutionError, PendingJobCounts, QueueAdmission, WorkerCounts,
    };

    use crate::request::ResponseEnvelope;

    let executor = Executor::new(ExecutionConfig {
        workers: WorkerCounts {
            bulk_cpu: NonZeroUsize::MIN,
            pending_jobs: PendingJobCounts::uniform(NonZeroUsize::MIN),
            ..WorkerCounts::default()
        },
        ..ExecutionConfig::default()
    })
    .assured("the test changes only worker counts");
    let (release, held) = nervix_primitives::sync::blocking::mpsc::channel::<()>();
    let occupier = executor.clone();
    let charge = executor
        .try_reserve(MemoryClass::Bulk, 0)
        .assured("a zero charge is always admitted");
    let occupying = nervix_primitives::task::spawn(async move {
        occupier
            .run_cpu(CpuClass::Bulk, charge, move |_, _| {
                held.recv()
                    .expect_err("dropping the sender is how the test releases the worker");
            })
            .await
    });
    while executor.snapshot().cpu_class(CpuClass::Bulk).running == 0 {
        nervix_primitives::task::yield_now().await;
    }
    let queued = executor.clone();
    let charge = executor
        .try_reserve(MemoryClass::Bulk, 0)
        .assured("a zero charge is always admitted");
    let queued = nervix_primitives::task::spawn(async move {
        queued.run_cpu(CpuClass::Bulk, charge, |_, _| ()).await
    });
    while executor.snapshot().cpu_class(CpuClass::Bulk).pending == 0 {
        nervix_primitives::task::yield_now().await;
    }
    let before = executor.snapshot().cpu_class(CpuClass::Bulk);
    let reserved_before = executor.memory(MemoryClass::Bulk).reserved_bytes;
    let charged = |bytes: Vec<u8>| {
        let input = executor
            .try_reserve(
                MemoryClass::Bulk,
                u64::try_from(bytes.len()).assured("the fixture envelope's length fits u64"),
            )
            .assured("the encoded envelope fits");
        ChargedBytes::from_owned(bytes, input)
    };
    let request_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&request())
        .assured("the current fixture envelope serializes")
        .to_vec();

    let refused_encoding = super::encode_rkyv(
        &executor,
        MemoryClass::Bulk,
        CpuClass::Bulk,
        1024 * 1024,
        request(),
    )
    .now_or_never()
    .assured("a full class answers a request frame's encoding at once");
    let refused_decoding = super::decode_rkyv::<RequestEnvelope>(
        &executor,
        MemoryClass::Bulk,
        CpuClass::Bulk,
        charged(request_bytes),
    )
    .now_or_never()
    .assured("a full class answers a request frame's decoding at once");
    for refused in [refused_encoding.map(drop), refused_decoding.map(drop)] {
        let error = refused
            .err()
            .assured("a full class refuses a request frame");
        assert!(
            matches!(
                error.downcast_ref::<ExecutionError>(),
                Some(ExecutionError::QueueFull { .. })
            ),
            "{error:?}"
        );
    }
    assert_eq!(
        executor.memory(MemoryClass::Bulk).reserved_bytes,
        reserved_before,
        "a refused request frame keeps no charge"
    );
    let refused = executor
        .snapshot()
        .cpu_class(CpuClass::Bulk)
        .refused
        .checked_sub(before.refused)
        .assured("the refusal counter only grows");
    assert_eq!(refused, 2, "each request frame is refused once");

    let response = || ResponseEnvelope {
        class: PoolClass::Bulk,
        request: "fetch_state_snapshot".to_owned(),
        result: Ok(vec![7; 1024]),
    };
    let response_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&response())
        .assured("the current fixture envelope serializes")
        .to_vec();
    let mut decoding = std::pin::pin!(super::decode_rkyv_with::<ResponseEnvelope>(
        &executor,
        MemoryClass::Bulk,
        CpuClass::Bulk,
        QueueAdmission::WaitForPlace,
        charged(response_bytes),
    ));
    let mut encoding = std::pin::pin!(super::encode_rkyv_with(
        &executor,
        MemoryClass::Bulk,
        CpuClass::Bulk,
        1024 * 1024,
        QueueAdmission::WaitForPlace,
        response(),
    ));
    assert!(
        decoding.as_mut().now_or_never().is_none(),
        "a full class holds a response frame's decoding back"
    );
    assert!(
        encoding.as_mut().now_or_never().is_none(),
        "a full class holds a response frame's encoding back"
    );

    drop(release);
    let decoded = decoding
        .await
        .assured("the decoding runs once a place frees");
    let encoded = encoding
        .await
        .assured("the encoding runs once a place frees");
    assert_eq!(decoded.value.request, response().request);
    assert_eq!(decoded.value.result, response().result);
    assert!(!encoded.is_empty());
    occupying
        .await
        .assured("the occupying job's supervisor is joined")
        .assured("the occupying job ran");
    queued
        .await
        .assured("the queued job's supervisor is joined")
        .assured("the queued job ran once the worker freed");
    let refused = executor
        .snapshot()
        .cpu_class(CpuClass::Bulk)
        .refused
        .checked_sub(before.refused)
        .assured("the refusal counter only grows");
    assert_eq!(refused, 2, "a full class refuses no response frame");
}

/// An executor whose bulk class has one worker and one place in its wait queue.
fn one_place_executor() -> Executor {
    use std::num::NonZeroUsize;

    use nervix_execution::{ExecutionConfig, PendingJobCounts, WorkerCounts};

    Executor::new(ExecutionConfig {
        workers: WorkerCounts {
            bulk_cpu: NonZeroUsize::MIN,
            pending_jobs: PendingJobCounts::uniform(NonZeroUsize::MIN),
            ..WorkerCounts::default()
        },
        ..ExecutionConfig::default()
    })
    .assured("the test changes only worker counts")
}

/// The bulk class of a [`one_place_executor`] held full: its worker runs a job that waits for the
/// test, and its one place is taken by a job waiting behind it. A job that refuses on a full class
/// is refused, and one that waits for a place waits, until the hold is released.
struct FullBulkClass {
    release: nervix_primitives::sync::oneshot::Sender<()>,
    occupying: nervix_primitives::task::JoinHandle<()>,
    queued: nervix_primitives::task::JoinHandle<()>,
}

impl FullBulkClass {
    async fn fill(executor: &Executor) -> Self {
        let (started, has_started) = nervix_primitives::sync::oneshot::channel();
        let (release, released) = nervix_primitives::sync::oneshot::channel::<()>();
        // Each holding job carries a byte of charge, because a model checker's semaphore cannot
        // grant zero permits.
        let charge = executor
            .try_reserve(MemoryClass::Bulk, 1)
            .assured("the untouched bulk class has room for one byte");
        let occupier = executor.clone();
        let occupying = nervix_primitives::task::spawn(async move {
            occupier
                .run_cpu(CpuClass::Bulk, charge, move |_charge, _cancellation| {
                    started
                        .send(())
                        .assured("the test awaits the occupying job");
                    released
                        .blocking_recv()
                        .assured("the test releases the occupying job before it exits");
                })
                .await
                .assured("the occupying job runs");
        });
        has_started
            .await
            .assured("the occupying job reports once it holds the worker");
        let charge = executor
            .try_reserve(MemoryClass::Bulk, 1)
            .assured("the bulk class has room for one more byte");
        let queuer = executor.clone();
        let queued = nervix_primitives::task::spawn(async move {
            queuer
                .run_cpu(CpuClass::Bulk, charge, |_charge, _cancellation| ())
                .await
                .assured("the queued job runs once the worker frees");
        });
        while executor.snapshot().cpu_class(CpuClass::Bulk).pending == 0 {
            nervix_primitives::task::yield_now().await;
        }
        Self {
            release,
            occupying,
            queued,
        }
    }

    /// Let the occupying job exit, so the queued job takes the worker and frees its place, and
    /// wait for both.
    async fn release(self) {
        self.release
            .send(())
            .assured("the occupying job waits for the test to release it");
        self.occupying
            .await
            .assured("the occupying job's task is joined");
        self.queued.await.assured("the queued job's task is joined");
    }
}

fn answer() -> crate::request::ResponseEnvelope {
    crate::request::ResponseEnvelope {
        class: PoolClass::Bulk,
        request: "fetch_state_snapshot".to_owned(),
        result: Ok(vec![7; 1024]),
    }
}

/// Decodes what [`super::encode_answer`] encoded and checks that it is the complete answer.
async fn assert_complete_answer(executor: &Executor, encoded: ChargedBytes) {
    let decoded = super::decode_rkyv::<crate::request::ResponseEnvelope>(
        executor,
        MemoryClass::Bulk,
        CpuClass::Bulk,
        encoded,
    )
    .await
    .assured("an encoded answer decodes once its class has room");
    let decoded = decoded.into_value();
    assert_eq!(decoded.class, answer().class);
    assert_eq!(decoded.request, answer().request);
    assert_eq!(decoded.result, answer().result);
}

/// An answer's encoding on a full class waits for a place, keeping only its own charge, and the
/// end of its exchange ends the wait with that charge given back. The class refuses nothing on the
/// answer's account, and the answer never takes a place.
#[cfg(not(feature = "shuttle"))]
async fn an_ended_exchange_ends_its_answers_wait_for_a_place() {
    let executor = one_place_executor();
    let full = FullBulkClass::fill(&executor).await;
    let before = executor.snapshot().cpu_class(CpuClass::Bulk);
    let reserved_before = executor.memory(MemoryClass::Bulk).reserved_bytes;
    let (end, ended) = nervix_primitives::sync::oneshot::channel::<()>();
    let mut encoding = std::pin::pin!(super::encode_answer(
        &executor,
        MemoryClass::Bulk,
        CpuClass::Bulk,
        1024 * 1024,
        answer(),
        async move {
            ended.await.assured("the test ends the exchange");
            Ok(())
        },
    ));
    assert!(
        encoding.as_mut().now_or_never().is_none(),
        "a full class holds an answer's encoding back"
    );
    assert!(
        executor.memory(MemoryClass::Bulk).reserved_bytes > reserved_before,
        "the waiting encoding keeps its own charge"
    );
    end.send(()).assured("the encoding waits for its exchange");
    let outcome = encoding
        .await
        .assured("an exchange that ends is not a failure");
    assert!(matches!(outcome, super::AnswerEncoding::Ended));
    assert_eq!(
        executor.memory(MemoryClass::Bulk).reserved_bytes,
        reserved_before,
        "the ended encoding gave its charge back"
    );
    full.release().await;
    let after = executor.snapshot().cpu_class(CpuClass::Bulk);
    assert_eq!(
        after.refused, before.refused,
        "a full class refuses no answer"
    );
    assert_eq!(
        after.admitted, before.admitted,
        "an answer whose exchange ended never took a place"
    );
    assert_eq!(executor.memory(MemoryClass::Bulk).reserved_bytes, 0);
}

/// An answer's encoding on a full class takes the first place that frees and encodes the whole
/// answer, without being refused.
#[cfg(not(feature = "shuttle"))]
async fn an_answer_takes_the_first_place_that_frees() {
    let executor = one_place_executor();
    let full = FullBulkClass::fill(&executor).await;
    let refused_before = executor.snapshot().cpu_class(CpuClass::Bulk).refused;
    let mut encoding = std::pin::pin!(super::encode_answer(
        &executor,
        MemoryClass::Bulk,
        CpuClass::Bulk,
        1024 * 1024,
        answer(),
        std::future::pending(),
    ));
    assert!(
        encoding.as_mut().now_or_never().is_none(),
        "a full class holds an answer's encoding back"
    );
    full.release().await;
    let outcome = encoding
        .await
        .assured("the answer is encoded once a place frees");
    let super::AnswerEncoding::Encoded(encoded) = outcome else {
        panic!("an exchange that never ends cannot end the answer's wait");
    };
    assert_complete_answer(&executor, encoded).await;
    assert_eq!(
        executor.snapshot().cpu_class(CpuClass::Bulk).refused,
        refused_before,
        "a full class refuses no answer"
    );
    assert_eq!(executor.memory(MemoryClass::Bulk).reserved_bytes, 0);
}

#[cfg(not(feature = "shuttle"))]
#[nervix_primitives::test]
async fn an_answer_waiting_for_a_place_on_a_full_class_ends_with_its_exchange() {
    an_ended_exchange_ends_its_answers_wait_for_a_place().await;
}

#[cfg(not(feature = "shuttle"))]
#[nervix_primitives::test]
async fn an_answer_waiting_for_a_place_on_a_full_class_takes_the_first_place_that_frees() {
    an_answer_takes_the_first_place_that_frees().await;
}

/// An answer's decoding on a full class waits for a place, but only until its request's own
/// deadline: then the request fails as timed out, naming the peer and the time it was given, with
/// every charge of the decoding returned and nothing refused. An answer whose place frees before
/// the deadline is decoded whole.
#[cfg(not(feature = "shuttle"))]
#[nervix_primitives::test]
async fn an_answers_decoding_waits_for_a_place_until_its_requests_deadline() {
    use std::time::Duration;

    use nervix_models::ClusterNodeName;
    use nervix_primitives::time::Instant;

    use crate::TransportError;

    let executor = one_place_executor();
    let peer = ClusterNodeName::parse("node-b").assured("the fixture node name is valid");
    let encoded = || {
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&answer())
            .assured("the current fixture envelope serializes")
            .to_vec();
        let input = executor
            .try_reserve(
                MemoryClass::Bulk,
                u64::try_from(bytes.len()).assured("the fixture envelope's length fits u64"),
            )
            .assured("the encoded answer fits");
        ChargedBytes::from_owned(bytes, input)
    };
    let full = FullBulkClass::fill(&executor).await;
    let before = executor.snapshot().cpu_class(CpuClass::Bulk);
    let held = executor.memory(MemoryClass::Bulk).reserved_bytes;
    let timeout = Duration::from_millis(50);
    let timed_out = super::decode_answer::<crate::request::ResponseEnvelope>(
        &executor,
        MemoryClass::Bulk,
        CpuClass::Bulk,
        encoded(),
        super::AnswerDeadline {
            peer: &peer,
            at: Instant::now()
                .checked_add(timeout)
                .assured("a fifty-millisecond deadline fits the monotonic clock"),
            timeout,
        },
    )
    .await;
    let error = timed_out
        .err()
        .assured("the class stays full past the request's deadline");
    match error.current_context() {
        TransportError::RequestTimeout {
            peer: timed_out_peer,
            timeout: reported,
        } => {
            assert_eq!(timed_out_peer, &peer);
            assert_eq!(*reported, timeout);
        }
        other => panic!("a decoding that outlives its deadline times out: {other:?}"),
    }
    assert_eq!(
        executor.memory(MemoryClass::Bulk).reserved_bytes,
        held,
        "a timed-out decoding returns its input and output charges"
    );
    let after = executor.snapshot().cpu_class(CpuClass::Bulk);
    assert_eq!(
        after.refused, before.refused,
        "a full class refuses no answer"
    );

    let mut decoding = std::pin::pin!(super::decode_answer::<crate::request::ResponseEnvelope>(
        &executor,
        MemoryClass::Bulk,
        CpuClass::Bulk,
        encoded(),
        super::AnswerDeadline {
            peer: &peer,
            at: Instant::now()
                .checked_add(Duration::from_secs(600))
                .assured("a ten-minute deadline fits the monotonic clock"),
            timeout: Duration::from_secs(600),
        },
    ));
    assert!(
        decoding.as_mut().now_or_never().is_none(),
        "a full class holds an answer's decoding back"
    );
    full.release().await;
    let decoded = decoding
        .await
        .assured("the answer is decoded once a place frees")
        .into_value();
    assert_eq!(decoded.request, answer().request);
    assert_eq!(decoded.result, answer().result);
    assert_eq!(
        executor.snapshot().cpu_class(CpuClass::Bulk).refused,
        before.refused,
        "a full class refuses no answer"
    );
}

#[cfg(not(feature = "shuttle"))]
#[nervix_primitives::test]
async fn outer_bulk_decode_refusal_releases_its_input() {
    outer_bulk_decode_refuses_behind_a_waiter_that_needs_its_input().await;
}

#[cfg(not(feature = "shuttle"))]
#[nervix_primitives::test]
async fn nested_bulk_decode_refusal_releases_its_envelope() {
    nested_bulk_decode_refuses_behind_a_waiter_that_needs_its_envelope().await;
}

#[cfg(not(feature = "shuttle"))]
#[nervix_primitives::test]
async fn nested_bulk_decode_cancellation_returns_input_output_and_queue_slot() {
    nested_decode_cancellation_keeps_its_own_input_charge().await;
}

/// Invariant: an answer that waits for a place on a full class ends in exactly one of two ways,
/// whichever comes first: the place frees and the whole answer is encoded, or the caller ends the
/// exchange and the encoding gives back its place in line and its charge. While the class stays
/// full the end of the exchange alone ends the wait, so an encoding that ignored it would never
/// finish. In every interleaving of a place freeing with an exchange ending, the class refuses
/// nothing, every job leaves its worker and no charge remains.
#[cfg(feature = "shuttle")]
#[test]
fn shuttle_an_answer_waiting_for_a_place_ends_with_its_exchange_or_takes_the_freed_place() {
    fn answering(
        executor: &Executor,
    ) -> (
        nervix_primitives::sync::oneshot::Sender<()>,
        nervix_primitives::task::JoinHandle<
            Result<super::AnswerEncoding, error_stack::Report<crate::TransportError>>,
        >,
    ) {
        let (end, ended) = nervix_primitives::sync::oneshot::channel::<()>();
        let executor = executor.clone();
        let encoding = nervix_primitives::task::spawn(async move {
            super::encode_answer(
                &executor,
                MemoryClass::Bulk,
                CpuClass::Bulk,
                1024 * 1024,
                answer(),
                async move {
                    ended
                        .await
                        .discarded("an encoding that finished first stops waiting for the end");
                    Ok(())
                },
            )
            .await
        });
        (end, encoding)
    }

    nervix_model_harness::shuttle::check_random_and_pct(|| {
        shuttle::future::block_on(async {
            let executor = one_place_executor();
            let full = FullBulkClass::fill(&executor).await;
            let held = executor.memory(MemoryClass::Bulk).reserved_bytes;

            // The class stays full, so only the end of the exchange can end this wait.
            let (end, encoding) = answering(&executor);
            let ending = nervix_primitives::task::spawn(async move {
                end.send(())
                    .assured("the encoding waits for its exchange while its class stays full");
            });
            let outcome = encoding
                .await
                .assured("the answering task is joined")
                .assured("an exchange that ends is not a failure");
            ending.await.assured("the ending task is joined");
            assert!(
                matches!(outcome, super::AnswerEncoding::Ended),
                "a class that stays full frees no place for the answer"
            );
            assert_eq!(
                executor.memory(MemoryClass::Bulk).reserved_bytes,
                held,
                "the ended encoding gave its charge back"
            );

            // A freed place races the end of the next exchange.
            let (end, encoding) = answering(&executor);
            let ending = nervix_primitives::task::spawn(async move {
                end.send(())
                    .discarded("an encoding that finished first no longer waits for the end");
            });
            full.release().await;
            ending.await.assured("the ending task is joined");
            let outcome = encoding
                .await
                .assured("the answering task is joined")
                .assured("neither a freed place nor an ended exchange is a failure");
            if let super::AnswerEncoding::Encoded(encoded) = outcome {
                assert_complete_answer(&executor, encoded).await;
            }
            let class = executor.snapshot().cpu_class(CpuClass::Bulk);
            assert_eq!(class.refused, 0, "a full class refuses no answer");
            assert_eq!(class.running, 0, "every job left its worker");
            assert_eq!(class.pending, 0, "no job keeps a place");
            assert_eq!(
                executor.memory(MemoryClass::Bulk).reserved_bytes,
                0,
                "no charge remains"
            );
        });
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn shuttle_bulk_receive_admission_releases_held_input_and_cancelled_jobs() {
    nervix_model_harness::shuttle::check_random_and_pct(|| {
        shuttle::future::block_on(async {
            outer_bulk_decode_refuses_behind_a_waiter_that_needs_its_input().await;
            nested_bulk_decode_refuses_behind_a_waiter_that_needs_its_envelope().await;
            nested_decode_cancellation_keeps_its_own_input_charge().await;
        });
    });
}
