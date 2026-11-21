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
