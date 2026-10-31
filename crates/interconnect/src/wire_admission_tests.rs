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
