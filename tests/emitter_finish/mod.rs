//! Public proofs that STOP finishes an emitter's sink, outside the product layer order.
//!
//! Layer: test harness.
//! - **Owns.** A STOP that runs while a stallable endpoint holds a broker connection paused and
//!   resumes it after a delay, and the observation that STOP completed only once that fault ended.
//! - **Depends on.** The scenario's cluster, its stallable endpoints and the shared command steps.
//! - **Must not know.** Any sink's private state, its driver, or how a finish waits.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the public scenario boundary declared above")
)]

use cucumber::{then, when};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::time::Instant;

use super::{NodeSession, ScenarioWorld, execute_plexus_commands_on_leader, parse_duration_text};

/// When a STOP completed, and when the endpoint it ran beside resumed.
#[derive(Debug)]
pub(crate) struct StopTiming {
    stopped_at: Instant,
    resumed_at: Instant,
}

struct StoppedDomain {
    result: Result<NodeSession, String>,
    at: Instant,
}

/// Runs STOP on the leader while the stallable endpoint `name` stays paused for `delay` more, so a
/// finish that waits for the records its sink's client holds completes only after they can leave.
#[when(expr = "STOP runs while the stallable endpoint {string} resumes after {string}")]
async fn stop_while_endpoint_resumes(world: &mut ScenarioWorld, name: String, delay: String) {
    let delay = parse_duration_text(&delay).assured("the scenario declares a valid fault duration");
    let resume = world
        .stallable_tcp_proxies
        .get(&name)
        .verified("the scenario configured the stallable endpoint it resumes")
        .resume_after(delay);
    let stop = async {
        let result = execute_plexus_commands_on_leader(world, "STOP;").await;
        StoppedDomain {
            result,
            at: Instant::now(),
        }
    };
    let (stopped, resumed_at) = tokio::join!(stop, resume);
    let _stopped = stopped
        .result
        .assured("STOP must finish the records its sink's client holds");
    let resumed_at = resumed_at.assured("the proxy remains alive while the domain stops");
    world.stop_timing = Some(StopTiming {
        stopped_at: stopped.at,
        resumed_at,
    });
}

#[then("STOP completed after the stallable endpoint resumed")]
fn stop_waited_for_endpoint(world: &mut ScenarioWorld) {
    let timing = world
        .stop_timing
        .as_ref()
        .verified("the preceding action observed STOP and the fault's end");
    assert!(
        timing.stopped_at >= timing.resumed_at,
        "STOP returned {:?} before the broker could receive its retained records",
        timing.resumed_at.checked_duration_since(timing.stopped_at)
    );
}
