//! Public NATS delivery and finish proofs, outside the product layer order.
//!
//! Layer: test harness.
//! - **Owns.** The wait for a client to find the scenario's controlled NATS endpoint down, a domain
//!   stop concurrent with that endpoint forwarding again, and the observation that stopping waited
//!   for it before it reported what the emitter's client held as sent.
//! - **Depends on.** The scenario's cluster, its controlled NATS endpoint and shared command steps.
//! - **Must not know.** The NATS sink's private state or how its driver queues what it sends.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the public scenario boundary declared above")
)]

use std::time::Duration;

use cucumber::{then, when};
use nervix_primitives::time::Instant;

use super::{NodeSession, ScenarioWorld, execute_nspl_commands_on_leader, parse_duration_text};
use crate::common::nats_endpoint::NatsEndpoint;

/// How long a scenario waits for a client to find its controlled NATS endpoint down. Generous,
/// because the wait ends at the first connection the endpoint refuses.
const REFUSAL_TIMEOUT: Duration = Duration::from_secs(60);

/// When a domain stop returned, and when the controlled NATS endpoint started to forward again
/// while it ran.
#[derive(Debug)]
pub(crate) struct NatsStopTiming {
    stopped_at: Instant,
    forwarding_at: Instant,
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
        let result = execute_nspl_commands_on_leader(world, "STOP;").await;
        StoppedDomain {
            result,
            at: Instant::now(),
        }
    };
    let (stopped, forwarding_at) = tokio::join!(stop, forward);
    let _stopped = stopped
        .result
        .expect("STOP finishes what the NATS emitter's client held");
    world.nats_stop_timing = Some(NatsStopTiming {
        stopped_at: stopped.at,
        forwarding_at,
    });
}

#[then("STOP completed after the controlled NATS endpoint forwarded everything")]
fn stop_waited_for_controlled_endpoint(world: &mut ScenarioWorld) {
    let timing = world
        .nats_stop_timing
        .as_ref()
        .expect("the preceding action observed STOP and the endpoint forwarding again");
    assert!(
        timing.stopped_at >= timing.forwarding_at,
        "STOP returned {:?} before the NATS server could receive what the emitter's client held",
        timing
            .forwarding_at
            .checked_duration_since(timing.stopped_at)
    );
}
