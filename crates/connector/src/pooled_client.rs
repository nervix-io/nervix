//! The pooled client a host shares among the emitters of one named client.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** What makes a connector's client poolable by the host, the lease one emitter holds on
//!   the node's instance of its client, and the publication of the wait an emitter sits in while
//!   its pool has no connection for it.
//! - **Depends on.** The vocabulary's connector kinds, client configuration entries and pool
//!   bounds, the node's resolver and bounded executor a pool opens through, typed reports, and the
//!   primitive boundary's monotonic clock and shared ownership.
//! - **Must not know.** The registry of shared clients, which graph node waits, how a wait is
//!   shown or measured, or any driver.
//!
//! A pool belongs to one named client in one domain on one node, so the host opens it once and
//! every local emitter of that client borrows from the same instance. The connector crate says how
//! its client opens by implementing [`PooledClient`] for its pool type. The host opens that type,
//! keeps it for as long as an emitter leases it, and hands each emitter its
//! [`PooledClientLease`] through `EmitterHost::pool`. No connector defines an interface for the
//! host to implement.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::{
    any::Any,
    future::{Future, poll_fn},
    pin::pin,
};

use async_trait::async_trait;
use error_stack::Report;
use nervix_dns::DnsResolver;
use nervix_execution::Executor;
use nervix_models::{ClientConfigEntry, ClientPoolBounds, ConnectorKind};
use nervix_primitives::{
    sync::{Arc, StdArc},
    time::Instant,
};

use crate::HostServiceError;

/// What a pooled client opens through on its node.
///
/// The host opens a pool before any emitter of its client exists, so the pool has no emitter's
/// handle to ask and takes these services from the node directly. Like an emitter's own handle,
/// every method hands over one named service that the code calling it declares.
#[cfg_attr(
    nervix_lint,
    nervix::value(
        host_handle,
        reason = "the handle a host gives the pooled client it opens"
    )
)]
pub struct PooledClientHost {
    dns: Option<DnsResolver>,
    executor: Executor,
}

impl PooledClientHost {
    pub fn new(dns: Option<DnsResolver>, executor: Executor) -> Self {
        Self { dns, executor }
    }

    /// The node's resolver, through which a pool dials every host it names.
    #[cfg_attr(
        nervix_lint,
        nervix::host_service(dns, reason = "hands over the node's resolver")
    )]
    pub fn dns(&self) -> Result<DnsResolver, Report<HostServiceError>> {
        match &self.dns {
            Some(dns) => Ok(dns.clone()),
            None => Err(Report::new(HostServiceError::DnsNotInstalled)),
        }
    }

    /// The node's bounded executor, for the synchronous work opening a pool does.
    #[cfg_attr(
        nervix_lint,
        nervix::host_service(executor, reason = "hands over the node's bounded executor")
    )]
    pub fn executor(&self) -> &Executor {
        &self.executor
    }
}

/// A connector's client that the host opens once per named client and shares among its emitters.
#[async_trait]
#[cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "the host opens one shared instance when the first local emitter attaches"
    )
)]
pub trait PooledClient: Send + Sync + Sized + 'static {
    /// The connector whose client statement this pool serves, which keys the host's pooled-client
    /// service and names the pool in the host's diagnostics.
    const CONNECTOR: ConnectorKind;

    /// How this connector's open failure reads beneath the client the host could not open.
    type OpenError: error_stack::Context;

    /// Opens the node's one instance from a client's resolved entries and declared bounds.
    async fn open(
        config: &[ClientConfigEntry],
        bounds: ClientPoolBounds,
        host: &PooledClientHost,
    ) -> Result<Self, Report<Self::OpenError>>;
}

/// The host's publication of one emitter's wait for a pooled connection.
///
/// An emitter blocked on a full pool reports the wait instead of looking idle next to a connection
/// it does not have.
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "a pooled emitter publishes the wait of each checkout its pool does not answer \
                  at once"
    )
)]
pub trait PooledClientWaits: Send + Sync + 'static {
    /// Publishes that the emitter waits for a connection since `since`.
    fn begin(&self, since: Instant);
    /// Records the wait that began at `since` and withdraws its publication.
    fn end(&self, since: Instant);
}

/// One published wait, which ends when it is dropped: on success, failure or cancellation.
pub struct PooledClientWait<K> {
    lease: Arc<PooledClientLeaseInner<K>>,
    since: Instant,
}

impl<K> Drop for PooledClientWait<K> {
    fn drop(&mut self) {
        self.lease.waits.end(self.since);
    }
}

struct PooledClientLeaseInner<K> {
    client: Arc<K>,
    waits: Box<dyn PooledClientWaits>,
    /// The host's interest in the shared instance and in the wait publication, both given back
    /// when the last clone of this lease is dropped.
    _interest: Box<dyn Send + Sync>,
}

/// One emitter's interest in the node's shared instance of its client.
///
/// The lease is held for exactly as long as its emitter can use the client. The instance closes
/// when the last lease on it is dropped, so removing one emitter preserves the pool for the others.
pub struct PooledClientLease<K> {
    inner: Arc<PooledClientLeaseInner<K>>,
}

impl<K> Clone for PooledClientLease<K> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<K: PooledClient> PooledClientLease<K> {
    /// Leases `client` to one emitter, publishing its waits through `waits`. `interest` is
    /// whatever the host gives back when the emitter lets the lease go.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "the host installs one retained lease when it composes a pooled emitter"
        )
    )]
    pub fn new(
        client: Arc<K>,
        waits: impl PooledClientWaits,
        interest: impl Send + Sync + 'static,
    ) -> Self {
        Self {
            inner: Arc::new(PooledClientLeaseInner {
                client,
                waits: Box::new(waits),
                _interest: Box::new(interest),
            }),
        }
    }

    /// The shared client this emitter writes through.
    pub fn client(&self) -> &K {
        &self.inner.client
    }

    /// Publishes this emitter's wait for a connection until the returned record is dropped.
    pub fn begin_wait(&self) -> PooledClientWait<K> {
        let since = Instant::now();
        self.inner.waits.begin(since);
        PooledClientWait {
            lease: self.inner.clone(),
            since,
        }
    }

    /// Awaits one checkout from the pool, publishing the wait once the pool does not answer at
    /// once.
    ///
    /// A checkout that is ready publishes nothing. Cancelling the checkout ends its wait.
    pub async fn checkout<F: Future>(&self, checkout: F) -> F::Output {
        let mut checkout = pin!(checkout);
        let mut waiting = None;
        poll_fn(|context| {
            let answered = checkout.as_mut().poll(context);
            if answered.is_pending() && waiting.is_none() {
                waiting = Some(self.begin_wait());
            }
            answered
        })
        .await
    }

    /// This lease without its client's type, as an emitter host handle carries it.
    pub(crate) fn erased(self) -> ErasedPooledClientLease {
        ErasedPooledClientLease {
            connector: K::CONNECTOR,
            lease: StdArc::new(self),
        }
    }
}

/// A lease whose client type the emitter that asks for it names.
#[derive(Clone)]
pub(crate) struct ErasedPooledClientLease {
    connector: ConnectorKind,
    /// The typed lease behind a trait object, which takes the standard reference count because
    /// the boundary's `Arc` cannot be coerced to one. An emitter never sees it: it takes a clone
    /// of the typed lease it holds.
    lease: StdArc<dyn Any + Send + Sync>,
}

impl ErasedPooledClientLease {
    /// The lease as one on `K`, or the refusal that names both connectors.
    pub(crate) fn typed<K: PooledClient>(
        &self,
    ) -> Result<PooledClientLease<K>, Report<HostServiceError>> {
        match self.lease.downcast_ref::<PooledClientLease<K>>() {
            Some(lease) => Ok(lease.clone()),
            None => Err(Report::new(
                HostServiceError::PooledClientOfOtherConnector {
                    leased: self.connector,
                    requested: K::CONNECTOR,
                },
            )),
        }
    }
}
