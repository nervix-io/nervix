//! Causal fault release for the wide subscription row's acknowledged input.
//!
//! Layer: Cucumber harness, outside the product layer order.
//! - **Owns.** The exact forwarding-error observation beside its bounded HTTP publication.
//! - **Depends on.** Raw sessions, the acknowledged publisher and the paced-publication owner.
//! - **Must not know.** Relay admission, HTTP framing or product retry and deadline policy.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "owns the controlled public wide-row intake regression"
    )
)]

use std::{io, time::Duration};

use cucumber::{gherkin::Step, when};
use meticulous::OptionExt as _;
use nervix_primitives::time::Instant;

use crate::{
    ScenarioWorld, append_cucumber_log_line,
    common::{
        paced_publication::PacedPublication,
        raw_session::{TestServerEvent, TestSession},
    },
    docstring, expand_placeholders,
};

struct ForwardingFailureObservation {
    session: TestSession,
    expected: String,
}

struct ForwardingFailure {
    session: TestSession,
    event: TestServerEvent,
}

impl ForwardingFailureObservation {
    async fn observe(mut self, deadline: Instant) -> io::Result<ForwardingFailure> {
        let mut passed_over = Vec::new();
        loop {
            nervix_primitives::task::consume_budget().await;
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                return Err(self.missing_witness(&passed_over));
            };
            let Some(event) = self.session.try_next_server_error(remaining).await? else {
                return Err(self.missing_witness(&passed_over));
            };
            append_cucumber_log_line(&format!(
                "observed runtime server error level={:?} message={}",
                event.level, event.message
            ));
            if event.message.contains(&self.expected) {
                return Ok(ForwardingFailure {
                    session: self.session,
                    event,
                });
            }
            passed_over.push(event.message);
        }
    }

    fn missing_witness(&self, passed_over: &[String]) -> io::Error {
        io::Error::new(
            io::ErrorKind::TimedOut,
            format!(
                "no server error containing {:?} before the publication deadline; passed over: \
                 {passed_over:?}",
                self.expected,
            ),
        )
    }
}

#[when(
    expr = "http payload is acknowledged within \"30s\" by node {string} with host {string} path \
            {string} after releasing paced node {string} on server error {string}"
)]
async fn when_a_forwarding_failure_releases_the_paced_publication(
    world: &mut ScenarioWorld,
    node: String,
    host: String,
    path: String,
    paced_node: String,
    expected: String,
    #[step] step: &Step,
) {
    let wait = Duration::from_secs(30);
    let node = expand_placeholders(world, &node);
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let paced_node = expand_placeholders(world, &paced_node);
    let expected = expand_placeholders(world, &expected);
    let payload = expand_placeholders(world, docstring(step));
    let publisher = world
        .cluster()
        .acknowledged_http_publisher(&node, &host, &path, wait);
    let release = world.cluster().interconnect_pacing_release(&paced_node);
    let session = world
        .active_session
        .take()
        .assured("the scenario opened its server-error session before publishing");
    let observer = ForwardingFailureObservation { session, expected };
    let deadline = Instant::now() + wait;
    let publication = PacedPublication::start(
        async move { publisher.publish_bounded(&payload).await },
        observer.observe(deadline),
        release,
    )
    .await;
    let failure = publication
        .finish()
        .await
        .unwrap_or_else(|error| panic!("paced HTTP publication did not recover: {error}"));
    append_cucumber_log_line(&format!(
        "paced HTTP publication acknowledged: node={node} released_node={paced_node} \
         server_error_level={:?} server_error={}",
        failure.event.level, failure.event.message
    ));
    world.last_server_error = Some(failure.event.message);
    world.active_session = Some(failure.session);
}
