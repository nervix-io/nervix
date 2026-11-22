//! A runtime error event through the control envelope's bounded production wire codec.
//!
//! Layer: test harness.
//! - **Owns.** Bounded current runtime error events, with and without the error they report, of
//!   every class and node kind, and their full-value oracle.
//! - **Depends on.** The control envelope, the error vocabulary and the bounded executor.
//! - **Must not know.** Which node published the event or how a session renders it.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::Executor;
use nervix_models::{MessageErrorClass, ModelKind, ModelName, NodeRef, ReportedError};
use strum::IntoEnumIterator as _;

use crate::{ControlEnvelope, PoolClass, RuntimeErrorEvent, wire};

/// The values one generated case varies a runtime error event by.
#[derive(Debug, bolero::TypeGenerator)]
struct RuntimeErrorWireCase {
    message: String,
    node: u16,
}

impl RuntimeErrorWireCase {
    fn node_name(&self) -> ModelName {
        ModelName::parse(&format!("node_{}", self.node))
            .assured("a generated decimal suffix makes a valid node name")
    }

    /// The event without an error, then one event for every class, each reported by a node of
    /// another kind, so every class and every node kind cross the wire.
    fn events(&self) -> Vec<RuntimeErrorEvent> {
        let kinds: Vec<ModelKind> = ModelKind::iter().collect();
        let mut events = vec![RuntimeErrorEvent {
            message: self.message.clone(),
            error: None,
        }];
        let rounds = kinds.len().max(MessageErrorClass::ALL.len());
        for round in 0..rounds {
            let class_index = round
                .checked_rem(MessageErrorClass::ALL.len())
                .assured("the class list is not empty");
            let kind_index = round
                .checked_rem(kinds.len())
                .assured("the model kind list is not empty");
            let kind = *kinds
                .get(kind_index)
                .verified("the remainder above is below the kind count");
            let class = *MessageErrorClass::ALL
                .get(class_index)
                .verified("the remainder above is below the class count");
            events.push(RuntimeErrorEvent {
                message: self.message.clone(),
                error: Some(ReportedError {
                    class,
                    node: NodeRef::new(kind, self.node_name()),
                }),
            });
        }
        events
    }
}

async fn assert_round_trip(executor: &Executor, event: RuntimeErrorEvent) {
    let control = ControlEnvelope::RuntimeErrorEvent(event);
    let class = PoolClass::Management;
    let bytes = wire::encode_rkyv(
        executor,
        class.memory_class(),
        class.cpu_class(),
        class.control_body_limit(executor),
        control.clone(),
    )
    .await
    .assured("a bounded runtime error event fits the control body limit");
    // A receiver decodes within room for the decoded value as large as the body, taken beside the
    // charge the body already holds.
    let length =
        u64::try_from(bytes.len()).assured("a bounded runtime error event's length fits u64");
    let decoding = executor
        .reserve(class.memory_class(), length)
        .await
        .assured("the management class holds a bounded runtime error event's decoding");
    let decoded =
        wire::decode_rkyv_within::<ControlEnvelope>(executor, class.cpu_class(), bytes, decoding)
            .await
            .assured("the production encoder's control envelope decodes");
    let (decoded, charge) = decoded.into_parts();
    assert_eq!(decoded, control);
    drop(charge);
}

/// A runtime error event keeps its complete value through the control envelope's production wire
/// codec: its text, and the class and producing node of the error it reports, for every class and
/// every node kind, and the absence of an error on an event that reports none.
#[test]
fn bolero_runtime_error_events_round_trip() {
    let runtime = nervix_primitives::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .assured("the ordinary test runtime builds");
    bolero::check!()
        .with_iterations(128)
        .with_max_len(512)
        .with_type::<RuntimeErrorWireCase>()
        .for_each(|case| {
            runtime.block_on(async {
                let executor = Executor::default();
                for event in case.events() {
                    nervix_primitives::task::consume_budget().await;
                    assert_round_trip(&executor, event).await;
                }
                let snapshot = executor.snapshot();
                assert_eq!(snapshot.management_memory.reserved_bytes, 0);
                assert_eq!(snapshot.bulk_memory.reserved_bytes, 0);
            });
        });
}
