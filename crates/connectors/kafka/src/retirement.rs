//! Retirement of the Kafka crate's native clients: their destruction, off the async workers.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** A librdkafka client its creator shares with the jobs it admits, the commands charge
//!   of the job that will destroy it, taken before the client exists, and the retirement that
//!   destroys it on the connector storage workers once the last holder lets it go.
//! - **Depends on.** The bounded executor, the primitive boundary, `error-stack` and the recovery
//!   vocabulary.
//! - **Must not know.** Which client it holds or what destroying it waits for, graphs, placement,
//!   registry state, or the host that drives the source or sink.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns the native destruction of Kafka clients")
)]

use std::{marker::PhantomData, ops::Deref};

use arch_into::ArchInto as _;
use error_stack::Report;
use meticulous::OptionExt as _;
use nervix_execution::{
    AdmissionError, Executor, MemoryClass, QueueAdmission, Reservation, StorageClass,
};
use nervix_primitives::sync::Arc;
use nervix_recovery::Reported as _;

/// A librdkafka client whose destruction is native work that waits, so it is never destroyed on
/// the thread that lets it go.
pub(crate) trait NativeClient: Send + Sync + 'static {
    /// What a report calls the job that destroys this client, should that job fail.
    const DESTRUCTION: &'static str;
}

/// The commands charge of the job that will destroy a `C`, taken before the client exists, so
/// retiring the client asks the node for nothing it could refuse.
pub(crate) struct RetirementCharge<C> {
    executor: Executor,
    charge: Reservation,
    client: PhantomData<fn() -> C>,
}

impl<C: NativeClient> RetirementCharge<C> {
    /// Takes the charge of the job that will destroy a `C` on `executor`. That job carries the
    /// client and nothing else.
    pub(crate) fn reserve(executor: Executor) -> Result<Self, Report<AdmissionError>> {
        let charge =
            executor.try_reserve(MemoryClass::Commands, std::mem::size_of::<C>().arch_into())?;
        Ok(Self {
            executor,
            charge,
            client: PhantomData,
        })
    }

    /// Shares `client`, which this charge was taken for, with whoever its creator hands it to.
    pub(crate) fn hold(self, client: C) -> Retiring<C> {
        let Self {
            executor,
            charge,
            client: PhantomData,
        } = self;
        Retiring {
            shared: Arc::new(SharedClient {
                owned: Some(OwnedClient {
                    client,
                    executor,
                    charge,
                }),
            }),
        }
    }
}

/// A handle to a librdkafka client, which the source, partition inspector or sink that created it
/// shares with the jobs it admits.
///
/// Whoever lets the last handle go hands the client to its retirement rather than destroying it on
/// its own thread.
pub(crate) struct Retiring<C: NativeClient> {
    shared: Arc<SharedClient<C>>,
}

impl<C: NativeClient> Clone for Retiring<C> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<C: NativeClient> Deref for Retiring<C> {
    type Target = C;

    fn deref(&self) -> &C {
        let owned = self
            .shared
            .owned
            .as_ref()
            .assured("a client is retired only once no handle to it is left");
        &owned.client
    }
}

/// The client every handle shares. Dropping it, which only the last handle's drop does, retires the
/// client.
struct SharedClient<C: NativeClient> {
    /// Present for as long as a handle exists; the drop that retires the client takes it.
    owned: Option<OwnedClient<C>>,
}

/// A client and what its retirement runs under.
struct OwnedClient<C> {
    client: C,
    executor: Executor,
    /// The commands charge of the job that destroys the client, taken before the client existed.
    charge: Reservation,
}

impl<C: NativeClient> Drop for SharedClient<C> {
    fn drop(&mut self) {
        let owned = self
            .owned
            .take()
            .assured("the shared client is dropped once, and only that drop takes the client");
        owned.retire();
    }
}

impl<C: NativeClient> OwnedClient<C> {
    /// Hands the client to a supervisor that waits for a place among the connector storage workers
    /// and destroys it there.
    ///
    /// Spawning does not wait, so the holder that let the last handle go, an async worker
    /// included, carries on at once. The supervisor owns the client until it is destroyed: no
    /// caller waits for it and nothing cancels it, because the destruction cannot stop part way.
    /// A full queue holds the job until a place frees rather than refusing it, since nothing could
    /// present the client again, and the charge the job runs under was taken before the client
    /// existed. Once a stopping runtime drops the supervisor, a client still waiting for a worker
    /// is destroyed by that stop.
    fn retire(self) {
        let Self {
            client,
            executor,
            charge,
        } = self;
        nervix_primitives::task::spawn(async move {
            let destroyed = executor
                .run_storage_with(
                    StorageClass::Connector,
                    QueueAdmission::WaitForPlace,
                    charge,
                    // Destruction cannot stop part way, so the job never checks its cancellation.
                    move |_charge, _cancellation| drop(client),
                )
                .await;
            destroyed.reported(C::DESTRUCTION);
        });
    }
}
