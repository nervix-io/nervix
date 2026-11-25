//! One-way control frames that reach a node whose control class is full.
//!
//! Layer: test harness.
//!
//! - **Owns.** The wait of each one-way control frame for a place on the receiving node's control
//!   class: what the frame holds while it waits, and how its exchange ends the wait.
//! - **Depends on.** The interconnect test fixture and the held-class fixture.
//! - **Must not know.** Product runtime graphs or consensus behavior.
#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use super::*;
use crate::answer_admission_tests::{HeldCpuClass, eventually};

/// A placement a replication frame names.
fn placement() -> StatePlacementEnvelope {
    StatePlacementEnvelope {
        domain: DomainName::parse("orders").expect("the fixture domain is valid"),
        state: RuntimeState::KafkaOffset,
        kind: ModelKind::Ingestor,
        identifier: ModelName::parse("source").expect("the fixture ingestor name is valid"),
        unit: nervix_models::Unit::SINGLE,
        branch_key: nervix_models::RemoteBranchKey::Unbranched,
    }
}

/// Connected transports whose sending side has registered the receiving node as its target.
async fn connected() -> ConnectedTransports {
    let transports = bound_transports_with_options(TransportOptions::default()).await;
    transports.transport_a.register_outbound_target(
        transports.node_b.clone(),
        NodeEndpoint::new("localhost", transports.transport_b.local_addr().port()),
    );
    transports
}

/// Send `frame` and wait until the receiving node delivers it to its application.
async fn deliver(transports: &mut ConnectedTransports, frame: &ControlEnvelope) {
    timeout(
        Duration::from_secs(10),
        transports
            .transport_a
            .send(&transports.node_b, Envelope::Control(frame.clone())),
    )
    .await
    .expect("the frame is sent before the test deadline")
    .expect("an idle receiver takes the frame");
    let received = timeout(Duration::from_secs(10), transports.incoming_b.recv())
        .await
        .expect("the frame reaches the application before the test deadline")
        .expect("the application queue stays open");
    let Envelope::Control(received) = received.envelope else {
        panic!("a one-way control frame arrives as a control envelope");
    };
    assert_eq!(received, *frame);
}

/// `frame`, sent to a node whose control class is full, waits for a place to be decoded while it
/// holds only its own charge: its body and the room its decoding takes, in its pool class's memory
/// class. The wait lasts as long as the exchange that carries the frame. A sender that gives up
/// resets the exchange, which ends the wait and returns the charge while the class is still full.
/// A sender that waits has the frame taken in once the class frees, and the charge stays with the
/// frame until the application drops it.
async fn assert_waits_while_its_exchange_lives(frame: ControlEnvelope) {
    for sender_gives_up in [false, true] {
        let mut transports = connected().await;
        // The first frame opens the pool class's connection, so the frame under test is the only
        // work that reaches the held class.
        deliver(&mut transports, &frame).await;

        let class = frame.pool_class();
        let memory = class.memory_class();
        let cpu = class.cpu_class();
        let executor_b = transports.executor_b.clone();
        let body = wire::encode_rkyv(
            &executor_b,
            memory,
            cpu,
            class.control_body_limit(&executor_b),
            frame.clone(),
        )
        .await
        .expect("a test frame fits its class's control body limit");
        let length = u64::try_from(body.len()).expect("a test frame's length fits u64");
        drop(body);
        let held = HeldCpuClass::hold(&executor_b, cpu).await;
        let reserved = executor_b.memory(memory).reserved_bytes;
        let charge = length
            .checked_mul(2)
            .expect("a test frame's body and decoding room fit u64");
        let waiting_charge = reserved
            .checked_add(charge)
            .expect("the receiver's charge while the frame waits fits u64");

        let sender = transports.transport_a.clone();
        let target = transports.node_b.clone();
        let sent = frame.clone();
        let sending = nervix_primitives::task::spawn(async move {
            sender.send(&target, Envelope::Control(sent)).await
        });
        eventually(
            &executor_b,
            "the frame waiting for a place with exactly its body and its decoding room",
            |executor| {
                executor.snapshot().cpu_class(cpu).waiting == 1
                    && executor.memory(memory).reserved_bytes == waiting_charge
            },
        )
        .await;

        if sender_gives_up {
            sending.abort();
            eventually(
                &executor_b,
                "the sender's reset ending the frame's wait and returning its charge",
                |executor| {
                    executor.snapshot().cpu_class(cpu).waiting == 0
                        && executor.memory(memory).reserved_bytes == reserved
                },
            )
            .await;
            held.release().await;
        } else {
            held.release().await;
            timeout(Duration::from_secs(10), sending)
                .await
                .expect("the frame is sent before the test deadline")
                .expect("the sending task is joined")
                .expect("the frame is taken in once the receiver's class frees");
            let received = timeout(Duration::from_secs(10), transports.incoming_b.recv())
                .await
                .expect("the frame reaches the application before the test deadline")
                .expect("the application queue stays open");
            let Envelope::Control(control) = &received.envelope else {
                panic!("a one-way control frame arrives as a control envelope");
            };
            assert_eq!(*control, frame);
            drop(received);
            eventually(
                &executor_b,
                "the frame's charge returning once the application drops it",
                |executor| executor.memory(memory).reserved_bytes == reserved,
            )
            .await;
        }
        transports
            .transport_a
            .shutdown(crate::test_teardown_ceiling())
            .await;
        transports
            .transport_b
            .shutdown(crate::test_teardown_ceiling())
            .await;
    }
}

#[nervix_primitives::test]
async fn a_state_replication_ack_waits_for_a_place_on_its_receivers_full_control_class() {
    assert_waits_while_its_exchange_lives(ControlEnvelope::StateReplicationAck(
        StateReplicationAck {
            placement: placement(),
            lsm: 7,
        },
    ))
    .await;
}

#[nervix_primitives::test]
async fn a_checkpoint_announcement_waits_for_a_place_on_its_receivers_full_control_class() {
    assert_waits_while_its_exchange_lives(ControlEnvelope::StateCheckpointAvailable(
        StateCheckpointAvailable {
            placement: placement(),
            lsm: 7,
        },
    ))
    .await;
}

#[nervix_primitives::test]
async fn a_runtime_error_event_waits_for_a_place_on_its_receivers_full_control_class() {
    assert_waits_while_its_exchange_lives(ControlEnvelope::RuntimeErrorEvent(RuntimeErrorEvent {
        message: "the source lost its connection".to_string(),
        error: None,
    }))
    .await;
}

#[nervix_primitives::test]
async fn a_terminate_frame_waits_for_a_place_on_its_receivers_full_control_class() {
    assert_waits_while_its_exchange_lives(ControlEnvelope::Terminate).await;
}
