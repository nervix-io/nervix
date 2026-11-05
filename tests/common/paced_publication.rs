//! Acknowledged publication and the observation that releases its byte fault.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** The publication, its failure witness, fault release and cancellation together.
//! - **Depends on.** Owned futures, a byte-cadence release token and runtime observation tasks.
//! - **Must not know.** HTTP outcomes, session framing or product timeout policy.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "owns coupled fault observation and publication in tests"
    )
)]

use std::{future::Future, io};

use nervix_primitives::{sync::CancellationToken, time::Instant};

use super::runner_observation::ArrivalObservation;

pub(crate) struct PacedPublication<T> {
    observation: ArrivalObservation<io::Result<T>>,
}

impl<T: Send + 'static> PacedPublication<T> {
    pub(crate) async fn start<P, W>(publication: P, witness: W, release: CancellationToken) -> Self
    where
        P: Future<Output = io::Result<()>> + Send + 'static,
        W: Future<Output = io::Result<T>> + Send + 'static,
    {
        let started_at = Instant::now();
        let observation = ArrivalObservation::start(async move {
            let release_after_witness = async move {
                let witnessed = witness.await?;
                release.cancel();
                tracing::info!(
                    elapsed = ?started_at.elapsed(),
                    "paced publication witnessed its forwarding failure and released the cadence"
                );
                Ok::<T, io::Error>(witnessed)
            };
            let ((), witnessed) = tokio::try_join!(publication, release_after_witness)?;
            tracing::info!(
                elapsed = ?started_at.elapsed(),
                "paced publication was acknowledged"
            );
            Ok(witnessed)
        })
        .await;
        Self { observation }
    }

    pub(crate) async fn finish(self) -> io::Result<T> {
        self.observation.finish().await.value
    }
}
