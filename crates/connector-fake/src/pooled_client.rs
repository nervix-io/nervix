//! Scripted pooled clients and the wait publication of their leases.
//!
//! Layer: test harness.
//! - **Owns.** Two pool types of different connectors that a host opens through the pooled-client
//!   contract, their scripted open refusals, and observations of the waits a lease publishes.
//! - **Depends on.** The connector contract's pooled-client service, the vocabulary's client
//!   configuration and selected primitives.
//! - **Must not know.** The registry of shared clients, real drivers or server runtime types.

use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_connector::{PooledClient, PooledClientHost, PooledClientWaits};
use nervix_models::{ClientConfigEntry, ClientPoolBounds, ConnectorKind};
use nervix_primitives::{
    sync::{Arc, blocking::Mutex},
    time::Instant,
};
use thiserror::Error;

/// The configuration key whose presence makes a scripted pool refuse to open.
pub const REFUSE_OPEN: &str = "refuse_open";

/// The configuration key whose presence makes a scripted pool take the node's resolver, as a pool
/// that dials by name does.
pub const RESOLVE_THROUGH_NODE: &str = "resolve_through_node";

/// Why a scripted pool did not open.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum FakePooledClientError {
    /// The client's configuration scripted the refusal.
    #[error("the scripted pool refuses to open")]
    Refused,
    /// The pool needs a service its node does not provide; the cause beneath names it.
    #[error("the scripted pool needs a service of its node")]
    HostService,
}

/// What a scripted pool was opened with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FakePooledClientOpening {
    pub config: Vec<ClientConfigEntry>,
    pub bounds: ClientPoolBounds,
    /// Whether the pool took the node's resolver while it opened.
    pub resolved_through_node: bool,
}

impl FakePooledClientOpening {
    /// Opens as `config` scripts: refused, through the node's resolver, or with nothing more.
    fn open(
        config: &[ClientConfigEntry],
        bounds: ClientPoolBounds,
        host: &PooledClientHost,
    ) -> Result<Self, Report<FakePooledClientError>> {
        if config.iter().any(|entry| entry.key == REFUSE_OPEN) {
            return Err(Report::new(FakePooledClientError::Refused));
        }
        let resolved_through_node = config.iter().any(|entry| entry.key == RESOLVE_THROUGH_NODE);
        if resolved_through_node {
            host.dns()
                .change_context(FakePooledClientError::HostService)?;
        }
        Ok(Self {
            config: config.to_vec(),
            bounds,
            resolved_through_node,
        })
    }
}

/// A scripted pool of one connector, which the host opens once per named client.
#[derive(Debug)]
pub struct FakePooledClient {
    pub opening: FakePooledClientOpening,
}

#[async_trait]
impl PooledClient for FakePooledClient {
    const CONNECTOR: ConnectorKind = ConnectorKind::Postgres;

    type OpenError = FakePooledClientError;

    async fn open(
        config: &[ClientConfigEntry],
        bounds: ClientPoolBounds,
        host: &PooledClientHost,
    ) -> Result<Self, Report<Self::OpenError>> {
        Ok(Self {
            opening: FakePooledClientOpening::open(config, bounds, host)?,
        })
    }
}

/// A scripted pool of another connector than [`FakePooledClient`]'s.
#[derive(Debug)]
pub struct OtherFakePooledClient {
    pub opening: FakePooledClientOpening,
}

#[async_trait]
impl PooledClient for OtherFakePooledClient {
    const CONNECTOR: ConnectorKind = ConnectorKind::MySql;

    type OpenError = FakePooledClientError;

    async fn open(
        config: &[ClientConfigEntry],
        bounds: ClientPoolBounds,
        host: &PooledClientHost,
    ) -> Result<Self, Report<Self::OpenError>> {
        Ok(Self {
            opening: FakePooledClientOpening::open(config, bounds, host)?,
        })
    }
}

/// What the leases of one scripted pool published about their waits.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PooledClientWaitObservations {
    /// How many waits began.
    pub begun: usize,
    /// How many waits ended, each with the instant it began at.
    pub ended: usize,
    /// When the wait published now began, absent while no checkout waits.
    pub waiting_since: Option<Instant>,
}

/// A host's wait publication that records every wait a lease begins and ends.
#[derive(Clone, Default)]
pub struct FakePooledClientWaits {
    observations: Arc<Mutex<PooledClientWaitObservations>>,
}

impl FakePooledClientWaits {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            observer,
            reason = "test assertions snapshot the published waits outside the emitter's checkout"
        )
    )]
    pub fn observations(&self) -> PooledClientWaitObservations {
        self.observations.lock().clone()
    }
}

impl PooledClientWaits for FakePooledClientWaits {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            key = "one fake pooled client wait log",
            bound = "one synchronous publication and count; no guard crosses await",
            reason = "the harness records the wait an emitter publishes before its test inspects \
                      it"
        )
    )]
    fn begin(&self, since: Instant) {
        let mut observations = self.observations.lock();
        observations.begun = observations
            .begun
            .checked_add(1)
            .assured("a test begins a bounded number of waits");
        observations.waiting_since = Some(since);
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            key = "one fake pooled client wait log",
            bound = "one synchronous withdrawal and count; no guard crosses await",
            reason = "the harness records that the wait an emitter published ended"
        )
    )]
    fn end(&self, since: Instant) {
        let mut observations = self.observations.lock();
        assert_eq!(
            observations.waiting_since,
            Some(since),
            "a wait ends with the instant it began at"
        );
        observations.ended = observations
            .ended
            .checked_add(1)
            .assured("a test ends a bounded number of waits");
        observations.waiting_since = None;
    }
}
