//! Public NATS delivery and finish proofs, outside the product layer order.
//!
//! Layer: test harness.
//! - **Owns.** The wait for a client to find the scenario's controlled NATS endpoint down, a domain
//!   stop concurrent with that endpoint forwarding again, and the observation that stopping waited
//!   for it before it reported what the emitter's client held as sent.
//! - **Depends on.** The scenario's cluster, its controlled NATS endpoint, shared command steps and
//!   the shared STOP timing record.
//! - **Must not know.** The NATS emitter's private state or how its driver queues what it sends.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the public scenario boundary declared above")
)]

use std::time::Duration;

use cucumber::{then, when};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::time::Instant;

use super::{
    NodeSession, ScenarioWorld, emitter_finish::StopTiming, execute_plexus_commands_on_leader,
    parse_duration_text,
};
use crate::common::nats_endpoint::NatsEndpoint;

/// How long a scenario waits for a client to find its controlled NATS endpoint down. Generous,
/// because the wait ends at the first connection the endpoint refuses.
const REFUSAL_TIMEOUT: Duration = Duration::from_secs(60);

#[then(expr = "within {string} the controlled NATS endpoint has stopped reading a publish")]
async fn controlled_endpoint_holds_a_publish(world: &mut ScenarioWorld, duration: String) {
    let duration = parse_duration_text(&duration).assured("the step supplies a valid duration");
    nervix_primitives::time::timeout(duration, controlled_endpoint(world).holding_a_publish())
        .await
        .assured("the controlled endpoint observes a held publish within the step's bound");
}

#[when(expr = "the controlled NATS connection is lost during {word}")]
async fn controlled_connection_is_lost(world: &mut ScenarioWorld, operation: String) {
    let reconnect = controlled_endpoint(world).reconnect_after_loss();
    let reconnect = nervix_primitives::time::timeout(REFUSAL_TIMEOUT, reconnect);
    match operation.as_str() {
        "publication" => {
            reconnect
                .await
                .assured("the NATS client attempts to reconnect");
        }
        "STOP" => {
            let stop = async {
                let result = execute_plexus_commands_on_leader(world, "STOP;").await;
                StoppedDomain {
                    result,
                    at: Instant::now(),
                }
            };
            let (stopped, forwarding_at) = tokio::join!(stop, reconnect);
            let _stopped = stopped
                .result
                .assured("STOP resolves the interrupted NATS write");
            let forwarding_at = forwarding_at.assured("the NATS client attempts to reconnect");
            let timing = StopTiming::new(stopped.at, forwarding_at);
            timing.assert_stop_followed_the_fault();
            world.stop_timing = Some(timing);
        }
        _ => panic!("the scenario must interrupt publication or STOP, not {operation:?}"),
    }
}

/// A retried Core NATS write may duplicate records, but every original member must arrive.
#[then(
    expr = "within {string} the observed broker receives every record from 1 to {int} by field \
            {string}"
)]
async fn observed_broker_receives_every_record(
    world: &mut ScenarioWorld,
    duration: String,
    count: u64,
    field: String,
) {
    let duration = parse_duration_text(&duration).assured("the step supplies a valid duration");
    let deadline = Instant::now()
        .checked_add(duration)
        .assured("the scenario's deadline fits the monotonic clock");
    let mut missing = (1..=count).collect::<std::collections::BTreeSet<_>>();
    let observer = world
        .broker_observer
        .as_mut()
        .assured("the scenario configured a broker observer");
    while !missing.is_empty() {
        nervix_primitives::task::consume_budget().await;
        let now = Instant::now();
        let remaining = deadline.checked_duration_since(now).unwrap_or_default();
        let message = observer
            .try_next_message(remaining)
            .await
            .assured("the observer reads NATS messages");
        let Some(message) = message else {
            panic!(
                "the NATS write lost {} of {count} records; first missing: {:?}; {}",
                missing.len(),
                missing.iter().take(16).collect::<Vec<_>>(),
                observer.describe_client_receipts()
            );
        };
        let payload = serde_json::from_str::<serde_json::Value>(&message.payload)
            .assured("the codec emitted a JSON object");
        let sequence = payload
            .get(&field)
            .assured("the output preserves the sequence field")
            .as_u64()
            .assured("the output sequence is an unsigned integer");
        assert!(
            (1..=count).contains(&sequence),
            "unexpected sequence {sequence}"
        );
        missing.remove(&sequence);
    }
}

struct StoppedDomain {
    result: Result<NodeSession, String>,
    at: Instant,
}

fn controlled_endpoint(world: &ScenarioWorld) -> &NatsEndpoint {
    world
        .nats_endpoint
        .as_ref()
        .expect("a NATS endpoint the scenario controls must be configured first")
}

/// A client reconnecting through the endpoint after it went down is refused, so it holds what it
/// sends from now on until the endpoint forwards again.
#[then("the controlled NATS endpoint refuses a client that reconnects")]
async fn controlled_endpoint_refuses_a_reconnection(world: &mut ScenarioWorld) {
    let refused = nervix_primitives::time::timeout(
        REFUSAL_TIMEOUT,
        controlled_endpoint(world).refused_at_least(1),
    )
    .await;
    assert!(
        refused.is_ok(),
        "no client reconnected through the controlled NATS endpoint within {REFUSAL_TIMEOUT:?}"
    );
}

#[when(expr = "STOP runs while the controlled NATS endpoint forwards everything after {string}")]
async fn stop_while_controlled_endpoint_returns(world: &mut ScenarioWorld, delay: String) {
    let delay = parse_duration_text(&delay).expect("the scenario declares a valid fault duration");
    let forward = controlled_endpoint(world).forward_everything_after(delay);
    let stop = async {
        let result = execute_plexus_commands_on_leader(world, "STOP;").await;
        StoppedDomain {
            result,
            at: Instant::now(),
        }
    };
    let (stopped, forwarding_at) = tokio::join!(stop, forward);
    let _stopped = stopped
        .result
        .expect("STOP finishes what the NATS emitter's client held");
    world.stop_timing = Some(StopTiming::new(stopped.at, forwarding_at));
}

#[then("STOP completed after the controlled NATS endpoint forwarded everything")]
fn stop_waited_for_controlled_endpoint(world: &mut ScenarioWorld) {
    world
        .stop_timing
        .as_ref()
        .expect("the preceding action observed STOP and the endpoint forwarding again")
        .assert_stop_followed_the_fault();
}
