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
use nervix_execution::{ChargedBytes, CpuClass, Executor, MemoryClass};
use nervix_recovery::Discarded as _;

use crate::{PoolClass, request::RequestEnvelope};

fn request() -> RequestEnvelope {
    RequestEnvelope {
        class: PoolClass::Bulk,
        request: "fetch_state_snapshot".to_owned(),
        payload: vec![7; 1024],
    }
}

/// A body read together with the room its decoding takes decodes within that room: it takes no
/// further charge, so it never waits for room behind a waiter that needs more than the class has
/// left.
async fn a_decode_read_with_its_input_takes_no_charge_behind_a_waiter() {
    let executor = Executor::default();
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&request())
        .assured("the current fixture envelope serializes")
        .to_vec();
    let length = u64::try_from(bytes.len()).assured("the bounded fixture length fits");
    let whole = length
        .checked_mul(2)
        .assured("the fixture and its decoding fit u64");
    let capacity = executor.memory(MemoryClass::Bulk).capacity_bytes;
    let read = executor
        .try_reserve(MemoryClass::Bulk, whole)
        .assured("the body and its decoding fit");
    let (input, decoding) = read
        .split(length)
        .assured("a charge splits at a size it holds");
    let held = executor
        .try_reserve(
            MemoryClass::Bulk,
            capacity
                .checked_sub(whole)
                .assured("the class holds the fixture"),
        )
        .assured("the rest of the class is held");
    let waiting = executor.reserve(MemoryClass::Bulk, 8192);
    let mut waiting = std::pin::pin!(waiting);
    assert!(waiting.as_mut().now_or_never().is_none());
    let decoded = super::decode_rkyv_within::<RequestEnvelope>(
        &executor,
        CpuClass::Bulk,
        ChargedBytes::from_owned(bytes, input),
        decoding,
    )
    .await
    .assured("decoding within the room read with its input waits for nothing");
    assert_eq!(decoded.value, request());
    drop(decoded);
    drop(held);
    drop(
        waiting
            .await
            .assured("the waiter is granted once the class frees"),
    );
    assert_eq!(executor.memory(MemoryClass::Bulk).reserved_bytes, 0);
}

/// A nested payload's decoding waits for its copy and its decoded value as one charge, holding
/// nothing of the class meanwhile, and decodes once room frees.
async fn a_nested_decode_waits_for_its_whole_charge_holding_nothing() {
    let executor = Executor::default();
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&request())
        .assured("the current fixture envelope serializes")
        .to_vec();
    let length = u64::try_from(bytes.len()).assured("the bounded fixture length fits");
    let capacity = executor.memory(MemoryClass::Bulk).capacity_bytes;
    // The class has room for the payload's copy, but not for its decoded value beside it.
    let held = executor
        .try_reserve(
            MemoryClass::Bulk,
            capacity
                .checked_sub(length)
                .assured("the class holds the fixture"),
        )
        .assured("the rest of the class is held");
    let mut decoding = Box::pin(super::decode_rkyv_payload::<RequestEnvelope>(
        &executor,
        MemoryClass::Bulk,
        CpuClass::Bulk,
        bytes,
    ));
    assert!(
        decoding.as_mut().now_or_never().is_none(),
        "a full class holds the nested decoding back instead of refusing it"
    );
    drop(held);
    let (value, charge) = decoding
        .await
        .assured("the nested decoding runs once its class has room")
        .into_parts();
    assert_eq!(value, request());
    assert_eq!(
        charge.bytes(),
        length,
        "the decoded value keeps only its own charge"
    );
    drop(value);
    drop(charge);
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

/// A full CPU class is backpressure for every frame: when its one worker is busy and its one queue
/// place is taken, a request frame's encoding and decoding wait for a place exactly as a response
/// frame's do, and all of them complete once the class frees.
#[cfg(not(feature = "shuttle"))]
#[nervix_primitives::test]
async fn a_full_class_holds_request_and_response_frames_back_until_a_place_frees() {
    use std::num::NonZeroUsize;

    use nervix_execution::{ExecutionConfig, PendingJobCounts, WorkerCounts};

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
    // Each frame is read together with the room its decoding takes.
    let charged = |bytes: Vec<u8>| {
        let length = u64::try_from(bytes.len()).assured("the fixture envelope's length fits u64");
        let read = executor
            .try_reserve(
                MemoryClass::Bulk,
                length
                    .checked_mul(2)
                    .assured("the fixture and its decoding fit u64"),
            )
            .assured("the encoded envelope and its decoding fit");
        let (input, decoding) = read
            .split(length)
            .assured("a charge splits at a size it holds");
        (ChargedBytes::from_owned(bytes, input), decoding)
    };
    let request_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&request())
        .assured("the current fixture envelope serializes")
        .to_vec();
    let response = || ResponseEnvelope {
        class: PoolClass::Bulk,
        request: "fetch_state_snapshot".to_owned(),
        result: Ok(vec![7; 1024]),
    };
    let response_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&response())
        .assured("the current fixture envelope serializes")
        .to_vec();

    let mut request_encoding = std::pin::pin!(super::encode_rkyv(
        &executor,
        MemoryClass::Bulk,
        CpuClass::Bulk,
        1024 * 1024,
        request(),
    ));
    let (request_input, request_room) = charged(request_bytes);
    let mut request_decoding = std::pin::pin!(super::decode_rkyv_within::<RequestEnvelope>(
        &executor,
        CpuClass::Bulk,
        request_input,
        request_room,
    ));
    let (response_input, response_room) = charged(response_bytes);
    let mut response_decoding = std::pin::pin!(super::decode_rkyv_within::<ResponseEnvelope>(
        &executor,
        CpuClass::Bulk,
        response_input,
        response_room,
    ));
    let mut response_encoding = std::pin::pin!(super::encode_rkyv(
        &executor,
        MemoryClass::Bulk,
        CpuClass::Bulk,
        1024 * 1024,
        response(),
    ));
    assert!(
        request_encoding.as_mut().now_or_never().is_none(),
        "a full class holds a request frame's encoding back instead of refusing it"
    );
    assert!(
        request_decoding.as_mut().now_or_never().is_none(),
        "a full class holds a request frame's decoding back instead of refusing it"
    );
    assert!(
        response_decoding.as_mut().now_or_never().is_none(),
        "a full class holds a response frame's decoding back"
    );
    assert!(
        response_encoding.as_mut().now_or_never().is_none(),
        "a full class holds a response frame's encoding back"
    );

    drop(release);
    let encoded_request = request_encoding
        .await
        .assured("the request frame's encoding runs once a place frees");
    let decoded_request = request_decoding
        .await
        .assured("the request frame's decoding runs once a place frees");
    let decoded = response_decoding
        .await
        .assured("the response frame's decoding runs once a place frees");
    let encoded = response_encoding
        .await
        .assured("the response frame's encoding runs once a place frees");
    assert!(!encoded_request.is_empty());
    assert_eq!(decoded_request.value.request, request().request);
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
    drop(decoded_request);
    drop(decoded);
    drop(encoded_request);
    drop(encoded);
    assert_eq!(executor.memory(MemoryClass::Bulk).reserved_bytes, 0);
}

#[cfg(not(feature = "shuttle"))]
#[nervix_primitives::test]
async fn an_outer_decode_takes_no_charge_beyond_the_room_read_with_its_input() {
    a_decode_read_with_its_input_takes_no_charge_behind_a_waiter().await;
}

#[cfg(not(feature = "shuttle"))]
#[nervix_primitives::test]
async fn a_nested_decode_waits_for_its_whole_charge_and_runs_once_room_frees() {
    a_nested_decode_waits_for_its_whole_charge_holding_nothing().await;
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
            a_decode_read_with_its_input_takes_no_charge_behind_a_waiter().await;
            a_nested_decode_waits_for_its_whole_charge_holding_nothing().await;
            nested_decode_cancellation_keeps_its_own_input_charge().await;
        });
    });
}

/// An encoding whose output outgrows its first charge while its class has no room for more is
/// backpressure, not a failed frame: it gives back what it holds, waits for the larger charge, and
/// completes once room frees.
#[cfg(not(feature = "shuttle"))]
#[nervix_primitives::test]
async fn an_encoding_that_outgrows_a_full_class_waits_for_room_and_completes() {
    let executor = Executor::default();
    let capacity = executor.memory(MemoryClass::Bulk).capacity_bytes;
    let before = executor.snapshot().cpu_class(CpuClass::Bulk);
    // The class keeps exactly the first charge an encoding takes, so the encoding starts and then
    // finds no room to grow.
    let held = executor
        .try_reserve(
            MemoryClass::Bulk,
            capacity
                .checked_sub(super::INITIAL_MESSAGE_CHARGE)
                .assured("the bulk class is larger than one message's first charge"),
        )
        .assured("the empty bulk class holds all but one first charge");
    let envelope = RequestEnvelope {
        class: PoolClass::Bulk,
        request: "fetch_state_snapshot".to_owned(),
        payload: vec![7; 64 * 1024],
    };
    let encoder = executor.clone();
    let encoding = nervix_primitives::task::spawn(async move {
        super::encode_rkyv(
            &encoder,
            MemoryClass::Bulk,
            CpuClass::Bulk,
            1024 * 1024,
            envelope,
        )
        .await
    });
    while executor.snapshot().cpu_class(CpuClass::Bulk).completed == before.completed {
        nervix_primitives::task::consume_budget().await;
        nervix_primitives::task::yield_now().await;
    }
    for _ in 0..256 {
        nervix_primitives::task::yield_now().await;
    }
    assert!(
        !encoding.is_finished(),
        "an encoding that found its class full waits for room instead of failing its frame"
    );
    drop(held);
    let encoded = encoding
        .await
        .assured("the encoding task is joined")
        .assured("the encoding completes once its class has room");
    let decoding = executor
        .try_reserve(
            MemoryClass::Bulk,
            u64::try_from(encoded.len()).assured("the encoded envelope's length fits u64"),
        )
        .assured("the class has room to decode the envelope");
    let decoded = super::decode_rkyv_within::<RequestEnvelope>(
        &executor,
        CpuClass::Bulk,
        encoded,
        decoding,
    )
    .await
    .assured("the completed encoding decodes as an envelope");
    assert_eq!(decoded.value.payload, vec![7; 64 * 1024]);
    drop(decoded);
    assert_eq!(executor.memory(MemoryClass::Bulk).reserved_bytes, 0);
}
