//! Interleavings of a link's first answer, idle heartbeat and receiver ending.
//!
//! Layer: test harness.
//! - **Owns.** The complete announced cadence and bounded first-answer ordering under close.
//! - **Depends on.** The production owner's answer-channel constructor and selected channels.
//! - **Must not know.** A second link implementation, transports or endpoint execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns checks of the production opening boundary")
)]

use std::{num::NonZeroU64, time::Duration};

use nervix_model_harness::shuttle::check_random_and_pct;
use nervix_models::{ClientLinkCapabilities, ClientLinkCapabilityValues};

use super::*;

#[test]
fn shuttle_the_link_opening_precedes_a_heartbeat_racing_receiver_close() {
    check_random_and_pct(|| {
        shuttle::future::block_on(async {
            let settings = ClientLinkCapabilities::resolve(ClientLinkCapabilityValues {
                heartbeat_interval: Some(Duration::from_secs(12)),
                producer_answer_capacity: Some(NonZeroU64::MIN),
                ..Default::default()
            })
            .assured("a slow sender and a one-answer queue are valid capabilities");
            let (answers, mut received) = owner_answer_channel(settings);
            let heartbeat = nervix_primitives::task::spawn(async move {
                // Busy answers replace an idle heartbeat. Closing likewise needs no queued heartbeat.
                match answers.try_send(ClientProducerLinkEvent::Heartbeat) {
                    Ok(())
                    | Err(
                        mpsc::error::TrySendError::Full(_) | mpsc::error::TrySendError::Closed(_),
                    ) => {}
                }
            });
            let reader = nervix_primitives::task::spawn(async move {
                let opening = received
                    .try_recv()
                    .assured("the opening was published before the link was exposed");
                let ClientProducerLinkEvent::LinkOpened { heartbeat } = opening else {
                    panic!("no idle event may precede the owner's opening");
                };
                assert_eq!(heartbeat.interval(), Duration::from_secs(12));
                assert_eq!(heartbeat.silence_limit(), Duration::from_secs(60));
                drop(received);
            });
            heartbeat.await.assured("a modeled producer completed");
            reader.await.assured("a modeled reader completed");
        });
    });
}
