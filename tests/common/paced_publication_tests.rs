//! Causal fault-release regressions for the paced publication fixture.
//!
//! Layer: harness tests, outside the product layer order.
//! - **Owns.** A delayed result collector, an exact failure witness and bounded publication.
//! - **Depends on.** The production fixture owner, controlled channels and the paused test clock.
//! - **Must not know.** Relay admission, HTTP framing or the scenario runner implementation.

use std::{io, time::Duration};

use meticulous::ResultExt as _;
use nervix_primitives::{
    sync::{CancellationToken, oneshot},
    time::{Instant, advance, timeout_at},
};

use super::paced_publication::PacedPublication;

#[nervix_primitives::test(start_paused = true)]
async fn a_forwarding_failure_releases_pacing_before_a_delayed_result_collection() {
    let released = CancellationToken::new();
    let forwarding = released.clone();
    let (first_failure, failure) = oneshot::channel();
    let (acknowledged, completion) = oneshot::channel();
    let started_at = Instant::now();
    let deadline = started_at + Duration::from_secs(30);
    let publication = PacedPublication::start(
        async move {
            let result = timeout_at(deadline, forwarding.cancelled()).await;
            acknowledged
                .send(result.is_ok())
                .assured("the test retains its publication observer");
            result.map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "publication expired"))
        },
        async move {
            failure
                .await
                .assured("the test supplies the exact forwarding failure");
            Ok("failed to forward output to relay 'wide_records'")
        },
        released.clone(),
    )
    .await;

    advance(Duration::from_secs(6)).await;
    first_failure
        .send(())
        .assured("the fixture retains its failure observation");
    // The scenario does not poll finish while the original publication deadline runs.
    let succeeded = completion
        .await
        .assured("the publication reports its result independently of collection");
    assert!(
        succeeded,
        "the witnessed failure must release pacing before the one 30-second publication expires"
    );
    assert!(released.is_cancelled());
    assert_eq!(started_at.elapsed(), Duration::from_secs(6));
    advance(Duration::from_secs(35)).await;
    assert!(Instant::now().duration_since(started_at) > Duration::from_secs(30));
    assert_eq!(
        publication
            .finish()
            .await
            .assured("the publication was acknowledged on time"),
        "failed to forward output to relay 'wide_records'"
    );
}

#[nervix_primitives::test(start_paused = true)]
async fn a_failed_witness_does_not_release_pacing() {
    let released = CancellationToken::new();
    let publication = PacedPublication::start(
        std::future::pending(),
        async { Err::<(), _>(io::Error::other("the forwarding witness ended")) },
        released.clone(),
    )
    .await;
    let Err(error) = publication.finish().await else {
        panic!("the witness failed");
    };
    assert_eq!(error.to_string(), "the forwarding witness ended");
    assert!(!released.is_cancelled());
}

#[nervix_primitives::test(start_paused = true)]
async fn a_publication_failure_is_retained_after_fault_release() {
    let released = CancellationToken::new();
    let forwarding = released.clone();
    let publication = PacedPublication::start(
        async move {
            forwarding.cancelled().await;
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "publication expired",
            ))
        },
        async { Ok(()) },
        released.clone(),
    )
    .await;
    let Err(error) = publication.finish().await else {
        panic!("a forwarding witness does not establish acknowledgement");
    };
    assert!(released.is_cancelled());
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert_eq!(error.to_string(), "publication expired");
}

#[nervix_primitives::test(start_paused = true)]
async fn dropping_a_paced_publication_ends_both_observations() {
    let (publication_held, publication_ended) = oneshot::channel::<()>();
    let (witness_held, witness_ended) = oneshot::channel::<()>();
    let publication = PacedPublication::start(
        async move {
            let _held = publication_held;
            std::future::pending::<io::Result<()>>().await
        },
        async move {
            let _held = witness_held;
            std::future::pending::<io::Result<()>>().await
        },
        CancellationToken::new(),
    )
    .await;
    drop(publication);
    let deadline = Instant::now() + Duration::from_secs(1);
    assert!(
        timeout_at(deadline, publication_ended)
            .await
            .assured("dropping the owner cancels its publication")
            .is_err()
    );
    assert!(
        timeout_at(deadline, witness_ended)
            .await
            .assured("dropping the owner cancels its witness reader")
            .is_err()
    );
}
