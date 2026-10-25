//! Production consumer-batch interleavings, outside the product layer order.
//!
//! Layer: test harness.
//! - **Owns.** Exact handover and ordering when delivery, cancellation and the batch deadline race.
//! - **Depends on.** The production reader, modeled channels and the canonical Shuttle runner.
//! - **Must not know.** Sockets or AMQP acknowledgements; Lapin's mock delivery is opaque payload
//!   storage here, and no claim depends on its internal synchronization.

use std::{num::NonZeroUsize, time::Duration};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::IngestorMessage as _;
use nervix_model_harness::shuttle::check_interleavings;
use nervix_primitives::{
    stream::wrappers::ReceiverStream,
    sync::{Arc, mpsc},
    time::{clear_triggers, trigger_timeouts},
};

use super::*;

#[derive(Clone, Copy, Debug)]
struct ReadDeadline;

fn handover(deadline_passes: bool) {
    clear_triggers();
    shuttle::future::block_on(async move {
        shuttle::current::set_label_for_task(shuttle::current::me(), ReadDeadline);
        let cancellation = Arc::new(IngestorReadCancellation::new());
        let (sender, receiver) = mpsc::channel(3);
        let broker = nervix_primitives::task::spawn(async move {
            for id in 1..=3u8 {
                nervix_primitives::task::consume_budget().await;
                let delivery =
                    Delivery::mock(u64::from(id), "".into(), "orders".into(), false, vec![id]);
                sender
                    .send(Ok(delivery))
                    .await
                    .assured("the reader lives until the broker finishes");
            }
            sender
        });
        let cancel = nervix_primitives::task::spawn({
            let cancellation = Arc::clone(&cancellation);
            async move {
                cancellation.cancel();
            }
        });
        let clock = if deadline_passes {
            Some(nervix_primitives::task::spawn(async {
                trigger_timeouts(|labels| labels.get::<ReadDeadline>().is_some());
            }))
        } else {
            None
        };
        let mut reader = RabbitMqBatchReader::new(ReceiverStream::new(receiver));
        let request = IngestorBatchRequest {
            max_messages: NonZeroUsize::new(3).assured("the check requests three deliveries"),
            // Shuttle expires this only through the modeled clock event above.
            batch_timeout: Some(Duration::from_secs(3_600)),
        };
        let batch = reader
            .read(request, &cancellation)
            .await
            .assured("the broker keeps its stream open");
        let (messages, interrupted) = match batch {
            IngestorBatch::Messages(messages) => (messages, false),
            IngestorBatch::Interrupted { gathered } => (gathered, true),
            _ => panic!("a connected consumer returns a batch or an interruption"),
        };
        let mut received: Vec<u8> = messages
            .iter()
            .map(|message| message.payload()[0])
            .collect();
        if received.len() < 3 && !deadline_passes {
            assert!(interrupted, "only cancellation can finish a partial batch");
        }
        cancel
            .await
            .assured("a panicking modeled task fails the check");
        if let Some(clock) = clock {
            clock.await.assured("the clock task cannot fail");
        }
        drop(broker.await.assured("the broker task cannot fail"));
        while let Some(delivery) = reader.stream.next().await {
            nervix_primitives::task::consume_budget().await;
            received.push(delivery.assured("the broker sends only deliveries").data[0]);
        }
        assert_eq!(
            received,
            [1, 2, 3],
            "handover plus the remaining stream preserves every delivery exactly once and in order"
        );
    });
}

#[test]
fn shuttle_a_cancelled_read_hands_over_every_delivery_once_and_in_order() {
    check_interleavings(|| handover(false));
}

#[test]
fn shuttle_a_batch_deadline_racing_cancellation_preserves_every_delivery() {
    check_interleavings(|| handover(true));
}
